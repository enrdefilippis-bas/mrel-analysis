"""Standalone script to enrich instruments with ESMA FIRDS (issued) and TradingView (outstanding) amounts.

Usage: python3 enrich_amounts.py
"""
from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path

from scrapers.esma_firds import fetch_all_firds_records
from scrapers.tradingview import fetch_all_tv_bonds

DB_PATH = Path("data/db/mrel.db")


async def main() -> None:
    conn = sqlite3.connect(str(DB_PATH))
    isins = [r[0] for r in conn.execute("SELECT isin FROM instruments").fetchall()]
    print(f"Total instruments in DB: {len(isins)}")

    # Step 1: ESMA FIRDS — issued amounts
    print("\n[1/2] Fetching ESMA FIRDS issued amounts...")
    firds_records = await fetch_all_firds_records(isins)
    print(f"  ESMA FIRDS returned {len(firds_records)} records")

    firds_updated = 0
    for rec in firds_records:
        if rec.issued_amount:
            # ESMA FIRDS is authoritative for issued nominal — always overwrite original_amount
            # Also use as outstanding_amount baseline (TradingView will overwrite with current outstanding)
            conn.execute(
                "UPDATE instruments SET original_amount = ?, outstanding_amount = ? WHERE isin = ?",
                (rec.issued_amount, rec.issued_amount, rec.isin),
            )
            firds_updated += 1
    conn.commit()
    print(f"  Updated {firds_updated} instruments with ESMA issued amounts")

    # Step 2: TradingView — current outstanding amounts
    print("\n[2/2] Fetching TradingView outstanding amounts...")
    tv_records = await fetch_all_tv_bonds(isins)
    print(f"  TradingView returned {len(tv_records)} records")

    tv_updated = 0
    for rec in tv_records:
        if rec.outstanding_amount:
            conn.execute(
                "UPDATE instruments SET outstanding_amount = ? WHERE isin = ?",
                (rec.outstanding_amount, rec.isin),
            )
            tv_updated += 1
    conn.commit()
    print(f"  Updated {tv_updated} instruments with TradingView outstanding amounts")

    # Summary
    stats = conn.execute("""
        SELECT
            COUNT(*) as total,
            COUNT(original_amount) as has_original,
            COUNT(outstanding_amount) as has_outstanding,
            SUM(CASE WHEN original_amount IS NOT NULL AND outstanding_amount IS NOT NULL
                     AND original_amount != outstanding_amount THEN 1 ELSE 0 END) as buybacks
        FROM instruments
    """).fetchone()
    print(f"\n{'='*60}")
    print(f"AMOUNT COVERAGE SUMMARY")
    print(f"{'='*60}")
    print(f"  Total instruments:    {stats[0]}")
    print(f"  Original amount:      {stats[1]}/{stats[0]} ({stats[1]*100//stats[0]}%)")
    print(f"  Outstanding amount:   {stats[2]}/{stats[0]} ({stats[2]*100//stats[0]}%)")
    print(f"  Buybacks detected:    {stats[3]}")

    # Show buyback details
    buybacks = conn.execute(
        "SELECT isin, name, original_amount, outstanding_amount FROM instruments "
        "WHERE original_amount IS NOT NULL AND outstanding_amount IS NOT NULL "
        "AND original_amount != outstanding_amount "
        "ORDER BY (original_amount - outstanding_amount) DESC"
    ).fetchall()
    if buybacks:
        print(f"\nInstruments with buybacks (outstanding < issued):")
        for isin, name, orig, out in buybacks:
            diff_pct = (1 - out / orig) * 100 if orig else 0
            print(f"  {isin}: issued={orig:>14,.0f}  outstanding={out:>14,.0f}  ({diff_pct:.1f}% bought back)  {(name or '')[:50]}")

    conn.close()


if __name__ == "__main__":
    asyncio.run(main())
