"""Import missing institutional bonds (AT1, T2, Senior Preferred, SNP) into the MREL database.

Sources:
1. Pillar 3 EU CCA (authoritative for capital instruments: AT1 + T2)
2. ESMA FIRDS (for Senior Preferred/Non-Preferred EMTN bonds)
3. TradingView (for current outstanding amounts)
"""
from __future__ import annotations

import asyncio
import re
import sqlite3
from datetime import date
from pathlib import Path

import pandas as pd
import requests

from scrapers.esma_firds import fetch_all_firds_records
from scrapers.tradingview import fetch_all_tv_bonds

DB_PATH = Path("data/db/mrel.db")
PILLAR3_PATH = Path("data/raw/pillar3/EU_CCA_20241231.xlsx")
LEI = "815600E4E6DCD2D25E30"
FIRDS_URL = "https://registers.esma.europa.eu/solr/esma_registers_firds/select"


def query_esma(q: str, rows: int = 500) -> list[dict]:
    r = requests.get(FIRDS_URL, params={
        "q": q, "wt": "json", "rows": rows,
        "fl": "isin,gnr_full_name,gnr_short_name,bnd_nmnl_value_total,bnd_nmnl_value_curr_code,"
              "bnd_maturity_date,bnd_seniority,gnr_cfi_code,bnd_fixed_rate",
    })
    return r.json().get("response", {}).get("docs", [])


def parse_pillar3() -> list[dict]:
    """Extract capital instruments from Pillar 3 EU CCA XLSX."""
    df = pd.read_excel(PILLAR3_PATH, header=None)

    isins = df.iloc[3, 2:14].tolist()
    # Row 8 = regulatory classification: "Capitale aggiuntivo di classe 1" (AT1) or "Capitale di classe 2" (T2)
    reg_class = df.iloc[8, 2:14].tolist()
    amounts = df.iloc[13, 2:14].tolist()  # Nominal amounts (in millions)
    maturities = df.iloc[19, 2:14].tolist()
    coupons = df.iloc[25, 2:14].tolist()

    instruments = []
    for i, isin in enumerate(isins):
        if not isinstance(isin, str) or not isin.strip():
            continue
        if isin == "IT0005218380":  # CET1 equity — skip
            continue

        cat_raw = str(reg_class[i]).strip().lower() if pd.notna(reg_class[i]) else ""

        # Row 8: "Capitale aggiuntivo di classe 1" = AT1, "Capitale di classe 2" = T2
        if "aggiuntivo" in cat_raw or "classe 1" in cat_raw:
            category = "AT1"
        elif "classe 2" in cat_raw:
            category = "Tier 2"
        else:
            category = "Unknown"

        # Parse amount (in millions)
        amt = None
        if pd.notna(amounts[i]):
            try:
                amt = float(amounts[i]) * 1_000_000
            except (ValueError, TypeError):
                pass

        # Parse maturity
        mat = None
        if pd.notna(maturities[i]):
            mat_str = str(maturities[i])
            if "nan" not in mat_str.lower():
                # Try ISO format from pandas Timestamp
                m = re.match(r"(\d{4})-(\d{2})-(\d{2})", mat_str)
                if m:
                    mat = f"{m.group(1)}-{m.group(2)}-{m.group(3)}"

        # Parse coupon rate from text
        coupon_rate = None
        if pd.notna(coupons[i]):
            ct = str(coupons[i])
            m = re.search(r"(\d+[.,]?\d*)\s*%", ct)
            if m:
                coupon_rate = float(m.group(1).replace(",", "."))

        # Determine coupon type
        coupon_type = "Unknown"
        if pd.notna(coupons[i]):
            ct = str(coupons[i]).lower()
            if "fisso" in ct or "fixed" in ct or "%" in ct:
                coupon_type = "Fixed"
            if "poi" in ct or "then" in ct or "rivedibile" in ct:
                coupon_type = "Step Up"  # Fixed then floating = step-up structure

        instruments.append({
            "isin": isin.strip(),
            "category": category,
            "amount": amt,
            "maturity": mat,
            "coupon_rate": coupon_rate,
            "coupon_type": coupon_type,
            "source": "pillar3",
        })

    return instruments


def find_emtn_bonds(db_isins: set[str]) -> list[dict]:
    """Find EMTN Senior Preferred and other institutional bonds from ESMA FIRDS."""
    all_docs = {}

    # Search by seniority
    for sen in ["JUND", "SBOD"]:
        for doc in query_esma(f"lei:{LEI} AND type_s:parent AND bnd_seniority:{sen}"):
            all_docs.setdefault(doc["isin"], doc)

    # XS-prefix SNDB bonds (international = institutional EMTN)
    for doc in query_esma(f"lei:{LEI} AND type_s:parent AND isin:XS* AND bnd_seniority:SNDB"):
        all_docs.setdefault(doc["isin"], doc)

    # Known institutional ISINs from IR/research
    known = [
        "IT0005549479", "IT0005580136", "IT0005611253", "IT0005696668",
        "IT0005632267", "IT0005675126", "IT0005657850", "IT0005651788",
        "IT0005640203", "IT0005641540",
    ]
    for isin in known:
        if isin not in all_docs:
            docs = query_esma(f"isin:{isin} AND type_s:parent", rows=1)
            if docs:
                all_docs[isin] = docs[0]

    # Convert to instrument dicts, filtering active and not already in DB
    instruments = []
    for isin, doc in all_docs.items():
        if isin in db_isins:
            continue

        mat_str = (doc.get("bnd_maturity_date") or "")[:10]
        if mat_str and mat_str < "2024-12-31" and not mat_str.startswith("9999"):
            continue  # Already matured

        amt = float(doc.get("bnd_nmnl_value_total", "0"))
        if amt < 50_000_000:
            continue  # Too small for institutional

        sen = doc.get("bnd_seniority") or ""
        cfi = doc.get("gnr_cfi_code") or ""
        is_cb = len(cfi) >= 4 and cfi[3] == "S"
        rate = doc.get("bnd_fixed_rate")

        # Classify
        if sen == "JUND" or (mat_str.startswith("9999") and not is_cb):
            category = "AT1"
        elif sen == "SBOD":
            category = "Tier 2"
        elif is_cb:
            category = "Covered Bond"
        elif sen == "SNDB":
            # SNDB = senior non-backed. Need to distinguish SP from SNP.
            # For now classify as Senior Preferred (most EMTN bonds are SP)
            # The 2 known SNP bonds (IT0005640203, IT0005641540) are already in DB
            category = "Senior Preferred"
        else:
            # Unknown seniority — check CFI
            if len(cfi) >= 4 and cfi[3] in ("Q", "O"):
                # Q = callable subordinated, O = callable
                category = "Tier 2"
            else:
                category = "Senior Preferred"

        name = doc.get("gnr_full_name") or doc.get("gnr_short_name") or ""

        # Determine coupon type
        coupon_type = "Unknown"
        if rate:
            coupon_type = "Fixed"
        if cfi and len(cfi) >= 3:
            if cfi[2] == "V":
                coupon_type = "Floating"
            elif cfi[2] == "F":
                coupon_type = "Fixed"

        maturity = mat_str if mat_str and not mat_str.startswith("9999") else None

        instruments.append({
            "isin": isin,
            "name": name[:100],
            "category": category,
            "amount": amt,
            "maturity": maturity,
            "coupon_rate": float(rate) if rate else None,
            "coupon_type": coupon_type,
            "currency": doc.get("bnd_nmnl_value_curr_code") or "EUR",
            "source": "esma_firds",
        })

    return instruments


def insert_instrument(conn: sqlite3.Connection, inst: dict, ref_date: date = date(2024, 12, 31)) -> None:
    """Insert an instrument into the database with eligibility assessment."""
    from models.instrument import Instrument, InstrumentCategory, CouponType
    from models.eligibility import assess_mrel_eligibility

    category = inst["category"]
    crr2_rank_map = {
        "CET1": 1, "AT1": 2, "Tier 2": 3,
        "Senior Non-Preferred": 4, "Senior Preferred": 5,
        "Covered Bond": None,
    }
    crr2_rank = crr2_rank_map.get(category)

    # Assess MREL eligibility
    mat_date = None
    if inst.get("maturity"):
        try:
            mat_date = date.fromisoformat(inst["maturity"])
        except ValueError:
            pass

    try:
        cat_enum = InstrumentCategory(category)
    except ValueError:
        cat_enum = InstrumentCategory.UNKNOWN
    try:
        ct_enum = CouponType(inst.get("coupon_type", "Unknown"))
    except ValueError:
        ct_enum = CouponType.UNKNOWN

    model_inst = Instrument(
        isin=inst["isin"],
        name=inst.get("name", ""),
        category=cat_enum,
        issue_date=None,
        maturity_date=mat_date,
        coupon_type=ct_enum,
        coupon_rate=inst.get("coupon_rate"),
        outstanding_amount=inst.get("amount"),
        currency=inst.get("currency", "EUR"),
        crr2_rank=crr2_rank,
    )
    result = assess_mrel_eligibility(model_inst, ref_date)

    conn.execute("""
        INSERT OR REPLACE INTO instruments (
            isin, name, category, issue_date, maturity_date,
            coupon_type, coupon_rate, outstanding_amount, currency,
            crr2_rank, listing_venue, mrel_eligible, mrel_layer,
            eligibility_reason, classification_confidence,
            bail_in_clause, capital_protected, capital_protection_pct,
            original_amount, underlying_linked, prospectus_url, source_pdf
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        inst["isin"],
        inst.get("name", ""),
        category,
        None,  # issue_date — will be enriched later
        inst.get("maturity"),
        inst.get("coupon_type", "Unknown"),
        inst.get("coupon_rate"),
        inst.get("amount"),  # outstanding = issued amount initially
        inst.get("currency", "EUR"),
        crr2_rank,
        None,  # listing_venue
        1 if result.eligible else 0,
        result.mrel_layer,
        result.reason,
        0.95,  # high confidence from official sources
        1 if category in ("AT1", "Tier 2") else None,  # bail_in_clause
        None,  # capital_protected
        None,  # capital_protection_pct
        inst.get("amount"),  # original_amount = issued
        0,     # not underlying_linked
        None,  # prospectus_url
        f"source:{inst.get('source', 'manual')}",
    ))


async def main():
    conn = sqlite3.connect(str(DB_PATH))
    db_isins = set(r[0] for r in conn.execute("SELECT isin FROM instruments").fetchall())
    print(f"Current instruments in DB: {len(db_isins)}")

    # Step 1: Import from Pillar 3 EU CCA
    print("\n[1/4] Parsing Pillar 3 EU CCA capital instruments...")
    p3_instruments = parse_pillar3()
    p3_new = 0
    for inst in p3_instruments:
        if inst["isin"] not in db_isins:
            insert_instrument(conn, inst)
            db_isins.add(inst["isin"])
            p3_new += 1
            print(f"  + {inst['isin']}: {inst['category']} ({inst.get('amount', 0)/1e6:.0f}M)")
        else:
            # Update category if wrong (e.g., XS2089968270 classified as T2 but is AT1)
            current = conn.execute("SELECT category FROM instruments WHERE isin = ?", (inst["isin"],)).fetchone()
            if current and current[0] != inst["category"]:
                print(f"  ~ {inst['isin']}: {current[0]} -> {inst['category']}")
                conn.execute("UPDATE instruments SET category = ?, crr2_rank = ? WHERE isin = ?",
                           (inst["category"],
                            {"AT1": 2, "Tier 2": 3}.get(inst["category"]),
                            inst["isin"]))
    conn.commit()
    print(f"  Added {p3_new} capital instruments from Pillar 3")

    # Step 2: Import from ESMA FIRDS
    print("\n[2/4] Finding EMTN bonds from ESMA FIRDS...")
    emtn_instruments = find_emtn_bonds(db_isins)
    emtn_new = 0
    for inst in sorted(emtn_instruments, key=lambda x: -(x.get("amount") or 0)):
        if inst["isin"] not in db_isins:
            insert_instrument(conn, inst)
            db_isins.add(inst["isin"])
            emtn_new += 1
            amt_str = f"{inst.get('amount', 0)/1e6:.0f}M" if inst.get("amount") else "N/A"
            print(f"  + {inst['isin']}: {inst['category']} ({amt_str}) {inst.get('name', '')[:40]}")
    conn.commit()
    print(f"  Added {emtn_new} EMTN bonds")

    # Step 3: Enrich with ESMA FIRDS amounts
    print("\n[3/4] Enriching all instruments with ESMA FIRDS amounts...")
    all_isins = [r[0] for r in conn.execute("SELECT isin FROM instruments").fetchall()]
    firds_records = await fetch_all_firds_records(all_isins)
    firds_updated = 0
    for rec in firds_records:
        if rec.issued_amount:
            conn.execute(
                "UPDATE instruments SET original_amount = ?, outstanding_amount = ? WHERE isin = ?",
                (rec.issued_amount, rec.issued_amount, rec.isin),
            )
            firds_updated += 1
    conn.commit()
    print(f"  ESMA: updated {firds_updated}/{len(all_isins)} with issued amounts")

    # Step 4: Enrich with TradingView outstanding amounts
    print("\n[4/4] Fetching TradingView outstanding amounts...")
    tv_records = await fetch_all_tv_bonds(all_isins)
    tv_updated = 0
    for rec in tv_records:
        if rec.outstanding_amount:
            conn.execute(
                "UPDATE instruments SET outstanding_amount = ? WHERE isin = ?",
                (rec.outstanding_amount, rec.isin),
            )
            tv_updated += 1
    conn.commit()
    print(f"  TradingView: updated {tv_updated}/{len(all_isins)} with outstanding amounts")

    # Summary
    print("\n" + "=" * 60)
    print("INSTRUMENT SUMMARY")
    print("=" * 60)
    for row in conn.execute("""
        SELECT category, COUNT(*),
               PRINTF('%.0f', SUM(COALESCE(outstanding_amount, 0))/1e6)
        FROM instruments GROUP BY category ORDER BY COUNT(*) DESC
    """).fetchall():
        print(f"  {row[0]:40s}  {row[1]:>4d} instruments  {row[2]:>8s}M EUR")

    total = conn.execute("SELECT COUNT(*) FROM instruments").fetchone()[0]
    print(f"\n  Total: {total} instruments")

    conn.close()


if __name__ == "__main__":
    asyncio.run(main())
