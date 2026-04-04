"""ESMA FIRDS (Financial Instruments Reference Data System) scraper.

Queries the public Solr API to retrieve issued nominal amounts and other
reference data for instruments identified by ISIN.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass

import httpx

FIRDS_URL = "https://registers.esma.europa.eu/solr/esma_registers_firds/select"

FIELDS = (
    "isin,gnr_full_name,gnr_short_name,gnr_cfi_code,"
    "bnd_nmnl_value_total,bnd_nmnl_value_curr_code,bnd_nmnl_value_unit,"
    "bnd_maturity_date,bnd_fixed_rate,bnd_seniority,"
    "lei,mic,mrkt_trdng_start_date"
)


@dataclass
class FIRDSRecord:
    isin: str
    name: str | None = None
    issued_amount: float | None = None
    currency: str | None = None
    maturity_date: str | None = None
    coupon_rate: float | None = None
    seniority: str | None = None
    cfi_code: str | None = None
    nominal_unit: float | None = None
    trading_start_date: str | None = None


async def fetch_firds_record(
    client: httpx.AsyncClient, isin: str
) -> FIRDSRecord | None:
    """Fetch a single ISIN from ESMA FIRDS. Returns None if not found."""
    try:
        resp = await client.get(
            FIRDS_URL,
            params={
                "q": f"isin:{isin} AND type_s:parent",
                "wt": "json",
                "rows": "1",
                "fl": FIELDS,
            },
        )
        resp.raise_for_status()
        data = resp.json()
        docs = data.get("response", {}).get("docs", [])
        if not docs:
            return None

        doc = docs[0]
        tsd = doc.get("mrkt_trdng_start_date")
        return FIRDSRecord(
            isin=isin,
            name=doc.get("gnr_full_name") or doc.get("gnr_short_name"),
            issued_amount=doc.get("bnd_nmnl_value_total"),
            currency=doc.get("bnd_nmnl_value_curr_code"),
            maturity_date=doc.get("bnd_maturity_date", "")[:10] if doc.get("bnd_maturity_date") else None,
            coupon_rate=doc.get("bnd_fixed_rate"),
            seniority=doc.get("bnd_seniority"),
            cfi_code=doc.get("gnr_cfi_code"),
            nominal_unit=doc.get("bnd_nmnl_value_unit"),
            trading_start_date=tsd[:10] if tsd else None,
        )
    except (httpx.HTTPError, KeyError, ValueError):
        return None


async def fetch_all_firds_records(
    isins: list[str], max_concurrent: int = 10, delay: float = 0.1
) -> list[FIRDSRecord]:
    """Fetch FIRDS records for multiple ISINs with light rate limiting."""
    results: list[FIRDSRecord] = []
    semaphore = asyncio.Semaphore(max_concurrent)

    async with httpx.AsyncClient(
        timeout=30,
        follow_redirects=True,
    ) as client:
        async def _fetch(isin: str) -> None:
            async with semaphore:
                await asyncio.sleep(delay)
                record = await fetch_firds_record(client, isin)
                if record:
                    results.append(record)

        await asyncio.gather(*[_fetch(isin) for isin in isins])

    return results
