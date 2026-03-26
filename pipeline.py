"""MREL Analysis Pipeline — orchestrates scraping, parsing, classification, and storage."""
from __future__ import annotations
import asyncio
import json
import sqlite3
from datetime import date
from pathlib import Path

import pandas as pd

from scrapers.banco_bpm import (
    scrape_all_prospectus_links,
    download_all_final_terms,
    ProspectusLink,
)
from scrapers.borsa_italiana import search_bonds_by_issuer
from scrapers.pillar3 import download_pillar3_files, parse_pillar3_mrel_tables, parse_capital_instruments_xlsx
from parsers.pdf_parser import extract_text
from parsers.prospectus import parse_prospectus
from parsers.classifier import prospectus_to_instrument, classify_instrument
from models.instrument import Instrument, InstrumentCategory
from models.eligibility import assess_mrel_eligibility
from models.mrel_stack import MRELStack

DATA_DIR = Path("data")
RAW_DIR = DATA_DIR / "raw"
DB_DIR = DATA_DIR / "db"
PROCESSED_DIR = DATA_DIR / "processed"

REF_DATE = date(2024, 12, 31)


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
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
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
        1 if inst.underlying_linked else 0 if inst.underlying_linked is not None else None,
        inst.prospectus_url,
        inst.source_pdf,
    ))
    conn.commit()


def load_instruments(conn: sqlite3.Connection) -> list[Instrument]:
    from models.instrument import CouponType
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
            underlying_linked=bool(data["underlying_linked"]) if data["underlying_linked"] is not None else None,
            prospectus_url=data["prospectus_url"],
            source_pdf=data["source_pdf"],
        )
        instruments.append(inst)
    return instruments


async def run_pipeline() -> None:
    print("=" * 60)
    print("MREL ANALYSIS PIPELINE — Banco BPM — Ref Date: 31.12.2024")
    print("=" * 60)

    # Step 1: Scrape prospectus links
    print("\n[1/6] Scraping Banco BPM IR for prospectus links...")
    links = await scrape_all_prospectus_links()
    print(f"  Found {len(links)} total PDF links")
    final_terms = [l for l in links if l.doc_type == "final_terms"]
    print(f"  Of which {len(final_terms)} are Final Terms")

    # Step 2: Download Final Terms PDFs
    print("\n[2/6] Downloading Final Terms PDFs...")
    downloaded = await download_all_final_terms(links, RAW_DIR / "final_terms")
    print(f"  Downloaded {len(downloaded)} Final Terms PDFs")

    # Step 3: Parse and classify
    print("\n[3/6] Parsing prospectuses and classifying instruments...")
    db_path = DB_DIR / "mrel.db"
    conn = init_db(db_path)

    classified_count = 0
    for link, pdf_path in downloaded:
        try:
            text = extract_text(pdf_path)
            if not text.strip():
                continue
            prospectus_data = parse_prospectus(text)
            if not prospectus_data.isin:
                if link.isin:
                    prospectus_data.isin = link.isin
                else:
                    continue

            inst = prospectus_to_instrument(prospectus_data)
            inst.prospectus_url = link.pdf_url
            inst.source_pdf = str(pdf_path)

            result = assess_mrel_eligibility(inst, REF_DATE)
            inst.mrel_eligible = result.eligible
            inst.eligibility_reason = result.reason

            save_instrument(conn, inst, result.mrel_layer)
            classified_count += 1
        except Exception as e:
            print(f"  Error processing {pdf_path.name}: {e}")

    print(f"  Classified {classified_count} instruments")

    # Step 4: Enrich with Borsa Italiana data
    print("\n[4/6] Fetching Borsa Italiana data for outstanding amounts...")
    try:
        borsa_instruments = await search_bonds_by_issuer("BANCO BPM")
        print(f"  Found {len(borsa_instruments)} instruments on Borsa Italiana")

        enriched = 0
        for bi in borsa_instruments:
            if bi.isin and bi.outstanding_amount:
                conn.execute(
                    "UPDATE instruments SET outstanding_amount = ?, listing_venue = ? WHERE isin = ?",
                    (bi.outstanding_amount, bi.market, bi.isin),
                )
                enriched += 1
        conn.commit()
        print(f"  Enriched {enriched} instruments with outstanding amounts")
    except Exception as e:
        print(f"  Warning: Borsa Italiana scraping failed: {e}")

    # Step 5: Download and parse Pillar 3
    print("\n[5/6] Downloading and parsing Pillar 3 data...")
    try:
        pdf_path, xlsx_path = await download_pillar3_files(RAW_DIR / "pillar3")
        aggregates = parse_pillar3_mrel_tables(pdf_path)
        capital_df = parse_capital_instruments_xlsx(xlsx_path)
        print(f"  Pillar 3 aggregates extracted")
        print(f"  Capital instruments XLSX: {len(capital_df)} rows")

        agg_path = PROCESSED_DIR / "pillar3_aggregates.json"
        agg_path.parent.mkdir(parents=True, exist_ok=True)
        import dataclasses
        with open(agg_path, "w") as f:
            json.dump(dataclasses.asdict(aggregates), f, indent=2)
    except Exception as e:
        print(f"  Warning: Pillar 3 processing failed: {e}")

    # Step 6: Compute MREL stack and export
    print("\n[6/6] Computing MREL stack...")
    instruments = load_instruments(conn)
    stack = MRELStack.from_instruments(instruments, REF_DATE)

    print("\n" + "=" * 60)
    print("MREL STACK SUMMARY")
    print("=" * 60)
    for key, val in stack.to_dict().items():
        if val:
            print(f"  {key}: EUR {val:,.0f}")

    export_path = PROCESSED_DIR / "mrel_instruments.xlsx"
    export_path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame([{
        "ISIN": i.isin,
        "Name": i.name,
        "Category": i.category.value,
        "Issue Date": i.issue_date,
        "Maturity Date": i.maturity_date,
        "Coupon Type": i.coupon_type.value,
        "Outstanding (EUR)": i.outstanding_amount,
        "CRR2 Rank": i.crr2_rank,
        "MREL Eligible": i.mrel_eligible,
        "Eligibility Reason": i.eligibility_reason,
        "Confidence": i.classification_confidence,
    } for i in instruments])
    df.to_excel(export_path, index=False)
    print(f"\nExported {len(instruments)} instruments to {export_path}")

    conn.close()
    print("\nPipeline complete.")


if __name__ == "__main__":
    asyncio.run(run_pipeline())
