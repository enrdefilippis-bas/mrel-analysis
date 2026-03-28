"""TradingView Bond Screener scraper.

Queries the TradingView scanner API to retrieve outstanding amounts
and other bond reference data. Unlike ESMA FIRDS (which gives issued amounts),
TradingView tracks current outstanding amounts (post-buybacks).
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass

import httpx

SCANNER_URL = "https://scanner.tradingview.com/bond/scan"

TV_COLUMNS = [
    "name",           # ISIN
    "description",    # Full description
    "outstanding_amount",
    "issue_amount",
    "nominal_value",
    "currency",
    "exchange",
    "issue_date",     # Unix timestamp
    "maturity_date",  # YYYYMMDD integer
    "subtype",
]


@dataclass
class TVBondRecord:
    isin: str
    description: str | None = None
    outstanding_amount: float | None = None
    issue_amount: float | None = None
    nominal_value: float | None = None
    currency: str | None = None
    exchange: str | None = None
    issue_date: int | None = None    # Unix timestamp
    maturity_date: int | None = None  # YYYYMMDD integer
    subtype: str | None = None
    ticker: str | None = None


async def fetch_banco_bpm_bonds(client: httpx.AsyncClient) -> list[TVBondRecord]:
    """Fetch all Banco BPM bonds from TradingView, deduplicated by ISIN."""
    payload = {
        "markets": ["bond"],
        "symbols": {"query": {"types": []}, "tickers": []},
        "options": {"lang": "en"},
        "columns": TV_COLUMNS,
        "filter": [{"left": "description", "operation": "match", "right": "Banco BPM"}],
        "sort": {"sortBy": "issue_amount", "sortOrder": "desc"},
        "range": [0, 500],
    }

    # Retry up to 3 times on transient errors
    data = None
    for attempt in range(3):
        try:
            resp = await client.post(SCANNER_URL, json=payload)
            resp.raise_for_status()
            data = resp.json()
            break
        except httpx.HTTPError:
            if attempt < 2:
                await asyncio.sleep(2)
            else:
                raise
    if not data:
        return []

    # Deduplicate by ISIN, preferring EUROTLX > LUXSE > other exchanges
    exchange_priority = {"EUROTLX": 0, "LUXSE": 1, "MOT": 2}
    seen: dict[str, tuple[int, TVBondRecord]] = {}

    for item in data.get("data", []):
        d = item["d"]
        isin = d[0]
        exchange = d[6]
        priority = exchange_priority.get(exchange, 99)

        record = TVBondRecord(
            isin=isin,
            description=d[1],
            outstanding_amount=d[2],
            issue_amount=d[3],
            nominal_value=d[4],
            currency=d[5],
            exchange=exchange,
            issue_date=d[7],
            maturity_date=d[8],
            subtype=d[9],
            ticker=item["s"],
        )

        if isin not in seen or priority < seen[isin][0]:
            seen[isin] = (priority, record)

    return [record for _, record in seen.values()]


async def fetch_bonds_by_isins(
    client: httpx.AsyncClient, isins: list[str], batch_size: int = 20
) -> list[TVBondRecord]:
    """Fetch specific ISINs from TradingView bond scanner.

    Queries in batches since the filter API doesn't support OR on name field.
    Falls back to individual ISIN lookups.
    """
    all_records: list[TVBondRecord] = []
    found_isins: set[str] = set()

    for i in range(0, len(isins), batch_size):
        batch = isins[i:i + batch_size]
        for isin in batch:
            payload = {
                "markets": ["bond"],
                "symbols": {"query": {"types": []}, "tickers": []},
                "options": {"lang": "en"},
                "columns": TV_COLUMNS,
                "filter": [{"left": "name", "operation": "match", "right": isin}],
                "range": [0, 10],
            }
            try:
                resp = await client.post(SCANNER_URL, json=payload)
                resp.raise_for_status()
                data = resp.json()

                exchange_priority = {"EUROTLX": 0, "LUXSE": 1, "MOT": 2}
                best: TVBondRecord | None = None
                best_priority = 999

                for item in data.get("data", []):
                    d = item["d"]
                    exchange = d[6]
                    priority = exchange_priority.get(exchange, 99)

                    record = TVBondRecord(
                        isin=d[0],
                        description=d[1],
                        outstanding_amount=d[2],
                        issue_amount=d[3],
                        nominal_value=d[4],
                        currency=d[5],
                        exchange=exchange,
                        issue_date=d[7],
                        maturity_date=d[8],
                        subtype=d[9],
                        ticker=item["s"],
                    )

                    if priority < best_priority:
                        best = record
                        best_priority = priority

                if best and best.isin not in found_isins:
                    all_records.append(best)
                    found_isins.add(best.isin)

            except httpx.HTTPError:
                continue

            await asyncio.sleep(0.2)  # Rate limit

    return all_records


async def fetch_all_tv_bonds(isins: list[str]) -> list[TVBondRecord]:
    """Main entry point: fetch outstanding amounts from TradingView.

    Strategy:
    1. Bulk-fetch all Banco BPM bonds (fast, gets ~70 unique ISINs)
    2. For remaining ISINs not found in bulk, query individually
    """
    async with httpx.AsyncClient(
        headers={
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
            "Content-Type": "application/json",
        },
        timeout=30,
    ) as client:
        # Step 1: Bulk fetch
        bulk_records = await fetch_banco_bpm_bonds(client)
        found_isins = {r.isin for r in bulk_records}

        # Step 2: Individual lookups for missing ISINs
        missing = [isin for isin in isins if isin not in found_isins]
        if missing:
            individual_records = await fetch_bonds_by_isins(client, missing)
            bulk_records.extend(individual_records)

    return bulk_records
