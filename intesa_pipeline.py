"""Intesa Sanpaolo MREL Instrument Intelligence Pipeline.

Orchestrates:
1. Scrape Intesa retail product website (certificates + bonds)
2. Find institutional bonds via ESMA FIRDS by LEI
3. Classify all instruments and assess MREL eligibility
4. Enrich with FIRDS (issue dates, issued amounts)
5. Enrich with TradingView (outstanding amounts)
6. Store in data/db/intesa.db
7. Export to data/processed/intesa_mrel_instruments.xlsx
"""
from __future__ import annotations

import asyncio
import sqlite3
from datetime import date
from pathlib import Path

import pandas as pd

from scrapers.intesa_products import (
    fetch_all_intesa_products,
    IntesaProduct,
    PRODUCT_CATEGORIES,
)
from scrapers.intesa_institutional import (
    fetch_intesa_institutional_bonds,
    InstitutionalBond,
)
from scrapers.esma_firds import fetch_all_firds_records
from scrapers.tradingview import fetch_bonds_by_isins
from models.instrument import Instrument, InstrumentCategory, CouponType
from models.eligibility import assess_mrel_eligibility

import httpx


# ---------------------------------------------------------------------------
# DB helpers (inlined from pipeline.py to avoid transitive import deps)
# ---------------------------------------------------------------------------

def init_db(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.execute("""
        CREATE TABLE IF NOT EXISTS instruments (
            isin TEXT PRIMARY KEY,
            name TEXT,
            category TEXT,
            issue_date TEXT,
            maturity_date TEXT,
            coupon_type TEXT,
            coupon_rate REAL,
            outstanding_amount REAL,
            currency TEXT,
            crr2_rank INTEGER,
            listing_venue TEXT,
            mrel_eligible INTEGER,
            mrel_layer TEXT,
            eligibility_reason TEXT,
            classification_confidence REAL,
            bail_in_clause INTEGER,
            capital_protected INTEGER,
            capital_protection_pct REAL,
            original_amount REAL,
            underlying_linked INTEGER,
            prospectus_url TEXT,
            source_pdf TEXT
        )
    """)
    conn.commit()
    return conn


def save_instrument(conn: sqlite3.Connection, inst: Instrument, mrel_layer: str = "") -> None:
    conn.execute("""
        INSERT OR REPLACE INTO instruments VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
    """, (
        inst.isin,
        inst.name,
        inst.category.value,
        str(inst.issue_date) if inst.issue_date else None,
        str(inst.maturity_date) if inst.maturity_date else None,
        inst.coupon_type.value,
        inst.coupon_rate,
        inst.outstanding_amount,
        inst.currency,
        inst.crr2_rank,
        inst.listing_venue,
        1 if inst.mrel_eligible else 0 if inst.mrel_eligible is not None else None,
        mrel_layer,
        inst.eligibility_reason,
        inst.classification_confidence,
        1 if inst.bail_in_clause else 0 if inst.bail_in_clause is not None else None,
        1 if inst.capital_protected else 0 if inst.capital_protected is not None else None,
        inst.capital_protection_pct,
        inst.original_amount,
        1 if inst.underlying_linked else 0 if inst.underlying_linked is not None else None,
        inst.prospectus_url,
        inst.source_pdf,
    ))
    conn.commit()


def load_instruments(conn: sqlite3.Connection) -> list[Instrument]:
    cursor = conn.execute("SELECT * FROM instruments")
    columns = [desc[0] for desc in cursor.description]
    instruments = []
    for row in cursor.fetchall():
        data = dict(zip(columns, row))
        try:
            category = InstrumentCategory(data["category"])
        except (ValueError, KeyError):
            category = InstrumentCategory.UNKNOWN
        try:
            coupon_type = CouponType(data["coupon_type"])
        except (ValueError, KeyError):
            coupon_type = CouponType.UNKNOWN
        inst = Instrument(
            isin=data["isin"],
            name=data["name"] or "",
            category=category,
            issue_date=date.fromisoformat(data["issue_date"]) if data["issue_date"] else None,
            maturity_date=date.fromisoformat(data["maturity_date"]) if data["maturity_date"] else None,
            coupon_type=coupon_type,
            coupon_rate=data["coupon_rate"],
            outstanding_amount=data["outstanding_amount"],
            currency=data["currency"] or "EUR",
            crr2_rank=data["crr2_rank"],
            listing_venue=data["listing_venue"],
            mrel_eligible=bool(data["mrel_eligible"]) if data["mrel_eligible"] is not None else None,
            eligibility_reason=data["eligibility_reason"],
            classification_confidence=data["classification_confidence"] or 1.0,
            bail_in_clause=bool(data["bail_in_clause"]) if data["bail_in_clause"] is not None else None,
            capital_protected=bool(data["capital_protected"]) if data["capital_protected"] is not None else None,
            capital_protection_pct=data.get("capital_protection_pct"),
            original_amount=data.get("original_amount"),
            underlying_linked=bool(data["underlying_linked"]) if data["underlying_linked"] is not None else None,
            prospectus_url=data.get("prospectus_url"),
            source_pdf=data.get("source_pdf"),
        )
        instruments.append(inst)
    return instruments

DATA_DIR = Path("data")
DB_DIR = DATA_DIR / "db"
PROCESSED_DIR = DATA_DIR / "processed"

REF_DATES = [date(2025, 6, 30), date(2025, 12, 31)]


# ---------------------------------------------------------------------------
# Classification: map website category → InstrumentCategory
# ---------------------------------------------------------------------------

INTESA_ISSUER_KEYWORDS = {"intesa", "banca imi"}


def _is_intesa_issuer(issuer_name: str | None) -> bool:
    if not issuer_name:
        return False
    lower = issuer_name.lower()
    return any(kw in lower for kw in INTESA_ISSUER_KEYWORDS)


def classify_product(p: IntesaProduct) -> tuple[InstrumentCategory, int, bool]:
    """Classify a retail product into InstrumentCategory.

    Returns (category, crr2_rank, capital_protected).
    """
    code = p.category_code

    if code == "BO":
        # Only Intesa-issued bonds count; third-party bonds distributed on platform are excluded
        if _is_intesa_issuer(p.issuer_name):
            return InstrumentCategory.SENIOR_PREFERRED, 5, False
        else:
            return InstrumentCategory.UNKNOWN, None, False

    if code in ("EP", "DIP"):
        # Capital protected if protection >= 100%
        prot = p.protection_pct or 0
        if prot >= 100.0:
            return InstrumentCategory.STRUCTURED_NOTE_PROTECTED, 5, True
        else:
            return InstrumentCategory.CERTIFICATE, 5, False

    # Conditionally protected and non-protected certificates
    # BN, CC, XP, TW, DICP, BH → all rank 5 certificates (not MREL-eligible)
    return InstrumentCategory.CERTIFICATE, 5, False


def product_to_instrument(p: IntesaProduct) -> Instrument:
    """Convert an IntesaProduct to an Instrument."""
    category, rank, capital_protected = classify_product(p)

    # Determine coupon type from category
    if p.category_code == "BO":
        coupon_type = CouponType.STRUCTURED  # Retail bonds are often structured
    else:
        coupon_type = CouponType.STRUCTURED  # Certificates are structured products

    return Instrument(
        isin=p.isin,
        name=p.name or "",
        category=category,
        issue_date=p.issuance_date,
        maturity_date=p.maturity_date,
        coupon_type=coupon_type,
        outstanding_amount=None,
        currency=p.currency or "EUR",
        crr2_rank=rank,
        listing_venue="EuroTLX/SeDeX",
        capital_protected=capital_protected,
        capital_protection_pct=p.protection_pct,
        underlying_linked=p.category_code != "BO",
        classification_confidence=0.95,
    )


def institutional_to_instrument(b: InstitutionalBond) -> Instrument:
    """Convert an InstitutionalBond to an Instrument."""
    # Map seniority to category
    if b.seniority == "JUND":
        category = InstrumentCategory.AT1
        rank = 2
    elif b.seniority == "SBOD":
        category = InstrumentCategory.TIER2
        rank = 3
    elif b.seniority == "SNDB":
        # SNDB can be SNP (rank 4) or SP (rank 5).
        # Heuristic: if CFI starts with "DB" it's a plain bond → SNP
        # For Intesa, SNDB with large institutional amounts are typically SNP.
        category = InstrumentCategory.SENIOR_NON_PREFERRED
        rank = 4
    else:
        category = InstrumentCategory.UNKNOWN
        rank = None

    return Instrument(
        isin=b.isin,
        name=b.name or "",
        category=category,
        issue_date=b.issue_date,
        maturity_date=b.maturity_date,
        coupon_type=CouponType.FIXED if b.coupon_rate else CouponType.UNKNOWN,
        coupon_rate=b.coupon_rate,
        outstanding_amount=b.issued_amount,
        original_amount=b.issued_amount,
        currency=b.currency or "EUR",
        crr2_rank=rank,
        listing_venue="Institutional",
        capital_protected=False,
        underlying_linked=False,
        classification_confidence=0.90,
    )


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

async def run_pipeline() -> None:
    print("=" * 60)
    print("INTESA SANPAOLO — MREL Instrument Intelligence Pipeline")
    print(f"Reference dates: {', '.join(str(d) for d in REF_DATES)}")
    print("=" * 60)

    # Step 1: Scrape retail products
    print("\n[1/6] Scraping Intesa retail product website...")
    products = await fetch_all_intesa_products()
    print(f"  Total retail products: {len(products)}")

    # Breakdown by category
    from collections import Counter
    cat_counts = Counter(p.category_code for p in products)
    for code, count in sorted(cat_counts.items(), key=lambda x: -x[1]):
        print(f"    {code}: {count}")

    # Step 2: Find institutional bonds via FIRDS
    print("\n[2/6] Querying ESMA FIRDS for institutional bonds (LEI-based)...")
    earliest_ref = min(REF_DATES)
    institutional = await fetch_intesa_institutional_bonds(ref_date=earliest_ref)
    print(f"  Total institutional bonds: {len(institutional)}")

    # Step 3: Convert to Instrument objects and classify
    print("\n[3/6] Classifying instruments...")
    instruments: list[Instrument] = []
    seen_isins: set[str] = set()

    # Institutional bonds first (higher confidence)
    for b in institutional:
        inst = institutional_to_instrument(b)
        instruments.append(inst)
        seen_isins.add(inst.isin)

    # Retail products (skip if already found as institutional)
    retail_skipped = 0
    for p in products:
        if p.isin in seen_isins:
            retail_skipped += 1
            continue
        inst = product_to_instrument(p)
        instruments.append(inst)
        seen_isins.add(inst.isin)

    print(f"  Total instruments: {len(instruments)} ({retail_skipped} duplicates with institutional)")

    # Classification breakdown
    cat_breakdown = Counter(i.category.value for i in instruments)
    for cat, count in sorted(cat_breakdown.items(), key=lambda x: -x[1]):
        print(f"    {cat}: {count}")

    # Step 4: Enrich with FIRDS (issue dates, issued amounts)
    print("\n[4/6] Enriching with ESMA FIRDS (amounts + issue dates)...")
    all_isins = [i.isin for i in instruments]
    firds_records = await fetch_all_firds_records(all_isins, max_concurrent=10, delay=0.15)
    firds_map = {r.isin: r for r in firds_records}

    firds_enriched = 0
    for inst in instruments:
        rec = firds_map.get(inst.isin)
        if not rec:
            continue
        if rec.issued_amount and not inst.original_amount:
            try:
                amt = float(rec.issued_amount)
            except (TypeError, ValueError):
                amt = None
            if amt:
                inst.original_amount = amt
                inst.outstanding_amount = amt  # Baseline; TradingView may overwrite
        if rec.trading_start_date and not inst.issue_date:
            try:
                inst.issue_date = date.fromisoformat(rec.trading_start_date)
            except ValueError:
                pass
        if rec.coupon_rate and not inst.coupon_rate:
            inst.coupon_rate = rec.coupon_rate
        firds_enriched += 1

    print(f"  FIRDS enriched: {firds_enriched}/{len(instruments)}")

    # Step 5: Enrich with TradingView (outstanding amounts)
    print("\n[5/6] Enriching with TradingView (outstanding amounts)...")
    try:
        async with httpx.AsyncClient(
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
                "Content-Type": "application/json",
            },
            timeout=30,
        ) as tv_client:
            tv_records = await fetch_bonds_by_isins(tv_client, all_isins)
        tv_map = {r.isin: r for r in tv_records}

        tv_enriched = 0
        for inst in instruments:
            rec = tv_map.get(inst.isin)
            if rec and rec.outstanding_amount:
                inst.outstanding_amount = rec.outstanding_amount
                tv_enriched += 1

        print(f"  TradingView enriched: {tv_enriched}/{len(instruments)}")
    except Exception as e:
        print(f"  Warning: TradingView enrichment failed: {e}")

    # Step 6: Assess eligibility and store for each ref date
    print("\n[6/6] Assessing MREL eligibility and storing...")

    for ref_date in REF_DATES:
        print(f"\n  --- Reference date: {ref_date} ---")
        db_path = DB_DIR / "intesa.db"
        conn = init_db(db_path)

        eligible_count = 0
        total_eligible_amount = 0.0

        for inst in instruments:
            # Skip instruments matured before ref date
            if inst.maturity_date and inst.maturity_date < ref_date:
                continue

            result = assess_mrel_eligibility(inst, ref_date)
            inst.mrel_eligible = result.eligible
            inst.eligibility_reason = result.reason
            save_instrument(conn, inst, result.mrel_layer)

            if result.eligible:
                eligible_count += 1
                try:
                    total_eligible_amount += float(inst.outstanding_amount or 0)
                except (TypeError, ValueError):
                    pass

        all_stored = load_instruments(conn)
        conn.close()

        print(f"  Stored: {len(all_stored)} instruments")
        print(f"  MREL eligible: {eligible_count}")
        if total_eligible_amount > 0:
            print(f"  Total eligible amount: EUR {total_eligible_amount:,.0f}")

    # Export to Excel (latest ref date)
    latest_ref = max(REF_DATES)
    db_path = DB_DIR / "intesa.db"
    conn = init_db(db_path)
    final_instruments = load_instruments(conn)
    conn.close()

    export_path = PROCESSED_DIR / "intesa_mrel_instruments.xlsx"
    export_path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame([{
        "ISIN": i.isin,
        "Name": i.name,
        "Category": i.category.value,
        "Issue Date": i.issue_date,
        "Maturity Date": i.maturity_date,
        "Coupon Type": i.coupon_type.value,
        "Coupon Rate (%)": i.coupon_rate,
        "Capital Protection (%)": i.capital_protection_pct,
        "Original Amount (EUR)": i.original_amount,
        "Outstanding (EUR)": i.outstanding_amount,
        "Currency": i.currency,
        "CRR2 Rank": i.crr2_rank,
        "MREL Eligible": i.mrel_eligible,
        "MREL Layer": "subordination" if i.crr2_rank and i.crr2_rank <= 4 else "total" if i.mrel_eligible else "excluded",
        "Eligibility Reason": i.eligibility_reason,
        "Listing Venue": i.listing_venue,
        "Confidence": i.classification_confidence,
    } for i in final_instruments])
    df.to_excel(export_path, index=False)
    print(f"\nExported {len(final_instruments)} instruments to {export_path}")

    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    eligible = [i for i in final_instruments if i.mrel_eligible]
    ineligible = [i for i in final_instruments if not i.mrel_eligible]
    print(f"  Total instruments: {len(final_instruments)}")
    print(f"  MREL eligible:    {len(eligible)}")
    print(f"  Not eligible:     {len(ineligible)}")
    if eligible:
        total_elig = sum(float(i.outstanding_amount or 0) for i in eligible)
        print(f"  Eligible amount:  EUR {total_elig:,.0f}")

    print("\nPipeline complete.")


if __name__ == "__main__":
    asyncio.run(run_pipeline())
