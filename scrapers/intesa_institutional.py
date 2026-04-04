"""Intesa Sanpaolo institutional bond finder via ESMA FIRDS.

Queries ESMA FIRDS by Intesa's LEI to find AT1, Tier 2, Senior Non-Preferred,
and institutional Senior Preferred bonds that are not listed on the retail
product website (institutional-only issuances, typically >= 50M EUR).
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import date

import httpx

FIRDS_URL = "https://registers.esma.europa.eu/solr/esma_registers_firds/select"

# Intesa Sanpaolo LEI
INTESA_LEI = "2W8N8UU78PMDQKZENC08"

FIELDS = (
    "isin,gnr_full_name,gnr_short_name,gnr_cfi_code,"
    "bnd_nmnl_value_total,bnd_nmnl_value_curr_code,bnd_nmnl_value_unit,"
    "bnd_maturity_date,bnd_fixed_rate,bnd_seniority,"
    "lei,mic,mrkt_trdng_start_date"
)

# FIRDS seniority codes → CRR2 classification
SENIORITY_MAP = {
    "JUND": ("AT1", 2),
    "SBOD": ("Tier 2", 3),
    "SNDB": ("Senior Non-Preferred or Senior Preferred", 4),  # needs further disambiguation
}


@dataclass
class InstitutionalBond:
    isin: str
    name: str | None = None
    seniority: str | None = None          # FIRDS seniority code
    seniority_label: str | None = None    # Human-readable label
    crr2_rank: int | None = None
    issued_amount: float | None = None
    currency: str | None = None
    maturity_date: date | None = None
    issue_date: date | None = None
    coupon_rate: float | None = None
    cfi_code: str | None = None


async def _query_firds(
    client: httpx.AsyncClient,
    seniority: str,
    min_amount: float = 50_000_000,
) -> list[dict]:
    """Query FIRDS for Intesa bonds with given seniority and minimum nominal."""
    query = (
        f"lei:{INTESA_LEI}"
        f" AND bnd_seniority:{seniority}"
        f" AND bnd_nmnl_value_total:[{min_amount} TO *]"
        " AND type_s:parent"
    )
    results = []
    start = 0
    rows = 100

    while True:
        try:
            resp = await client.get(
                FIRDS_URL,
                params={
                    "q": query,
                    "wt": "json",
                    "rows": str(rows),
                    "start": str(start),
                    "fl": FIELDS,
                    "sort": "bnd_nmnl_value_total desc",
                },
            )
            resp.raise_for_status()
            data = resp.json()
            docs = data.get("response", {}).get("docs", [])
            results.extend(docs)
            total = data.get("response", {}).get("numFound", 0)
            start += rows
            if start >= total:
                break
            await asyncio.sleep(0.2)
        except (httpx.HTTPError, KeyError, ValueError) as e:
            print(f"  Warning: FIRDS query failed for seniority={seniority}: {e}")
            break

    return results


def _parse_date(s: str | None) -> date | None:
    if not s:
        return None
    try:
        return date.fromisoformat(s[:10])
    except ValueError:
        return None


def _doc_to_bond(doc: dict, seniority_code: str) -> InstitutionalBond:
    label, rank = SENIORITY_MAP.get(seniority_code, ("Unknown", None))
    tsd = doc.get("mrkt_trdng_start_date")
    return InstitutionalBond(
        isin=doc["isin"],
        name=doc.get("gnr_full_name") or doc.get("gnr_short_name"),
        seniority=seniority_code,
        seniority_label=label,
        crr2_rank=rank,
        issued_amount=doc.get("bnd_nmnl_value_total"),
        currency=doc.get("bnd_nmnl_value_curr_code"),
        maturity_date=_parse_date(doc.get("bnd_maturity_date")),
        issue_date=_parse_date(tsd) if tsd else None,
        coupon_rate=doc.get("bnd_fixed_rate"),
        cfi_code=doc.get("gnr_cfi_code"),
    )


async def fetch_intesa_institutional_bonds(
    ref_date: date | None = None,
    seniorities: list[str] | None = None,
    min_amount: float = 50_000_000,
) -> list[InstitutionalBond]:
    """Fetch Intesa institutional bonds from FIRDS.

    Args:
        ref_date: If provided, filter out bonds that matured before this date.
        seniorities: FIRDS seniority codes to query. Defaults to JUND, SBOD, SNDB.
        min_amount: Minimum nominal value (EUR). Default 50M.

    Returns:
        Deduplicated list of InstitutionalBond objects.
    """
    if seniorities is None:
        seniorities = list(SENIORITY_MAP.keys())

    all_bonds: list[InstitutionalBond] = []
    seen_isins: set[str] = set()

    async with httpx.AsyncClient(
        timeout=30,
        follow_redirects=True,
    ) as client:
        for sen in seniorities:
            docs = await _query_firds(client, sen, min_amount)
            label = SENIORITY_MAP.get(sen, (sen, None))[0]
            count = 0
            for doc in docs:
                bond = _doc_to_bond(doc, sen)
                # Skip matured bonds if ref_date is provided
                if ref_date and bond.maturity_date and bond.maturity_date < ref_date:
                    continue
                if bond.isin not in seen_isins:
                    all_bonds.append(bond)
                    seen_isins.add(bond.isin)
                    count += 1
            print(f"  FIRDS {label} ({sen}): {len(docs)} found, {count} after filters")
            await asyncio.sleep(0.3)

    return all_bonds
