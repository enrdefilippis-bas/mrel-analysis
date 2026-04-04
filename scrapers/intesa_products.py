"""Intesa Sanpaolo product website scraper.

Queries the REST API at prodottiequotazioni.intesasanpaolo.com to retrieve
all retail certificates and bonds with their classification data (protection %,
maturity, underlying, etc.).

The API was discovered by intercepting network requests from the Next.js frontend.
No browser automation is needed — direct httpx calls to the REST endpoints.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import date, datetime

import httpx

BASE_URL = "https://prodottiequotazioni.intesasanpaolo.com"
INSTANT_URL = f"{BASE_URL}/rest/public/title/instantbyindex"

# Earliest possible unix timestamp (1970) and far future for max range
DATE_FROM = 0
DATE_TO = 2556057600  # ~2050-12-31


@dataclass
class IntesaProduct:
    """A single product from Intesa's retail platform."""
    isin: str
    name: str | None = None
    category_code: str | None = None      # EP, DIP, BN, CC, XP, TW, DICP, BH, BO
    category_label: str | None = None     # Human-readable label
    issuance_date: date | None = None
    maturity_date: date | None = None
    protection_pct: float | None = None   # e.g. 100.0 for 100%
    participation_pct: float | None = None
    cap_pct: float | None = None
    underlying: str | None = None
    currency: str | None = None
    issuer_name: str | None = None
    early_redeemed: bool = False
    real_redemption_date: date | None = None
    financial_strategy: str | None = None  # LONG / SHORT


# Product categories and their API parameters.
# Each tuple: (typology, macroTypology, typeManaged, label, protection_type)
# protection_type: "capital_protected", "conditionally_protected", "non_protected", "bond"
PRODUCT_CATEGORIES = [
    ("Equity_Protection",                   "CE", "EP",   "Equity Protection",          "capital_protected"),
    ("Digital_Capital_Protect",             "CE", "DIP",  "Digital Protected",           "capital_protected"),
    ("Bonus",                               "CE", "BN",   "Bonus",                       "conditionally_protected"),
    ("Cash_Collect",                        "CE", "CC",   "Cash Collect",                "conditionally_protected"),
    ("Express",                             "CE", "XP",   "Express",                     "conditionally_protected"),
    ("Twin_Win_One_Win",                    "CE", "TW",   "Twin Win / One Win",          "conditionally_protected"),
    ("Digital_Capital_Condition_Protect",   "CE", "DICP", "Digital Conditional",          "conditionally_protected"),
    ("Benchmark",                           "CE", "BH",   "Benchmark",                   "non_protected"),
    ("Bonds",                               "B",  "BO",   "Obbligazioni",                "bond"),
]


def _unix_to_date(ts: int | float | None) -> date | None:
    """Convert a unix timestamp (seconds) to a date, or None."""
    if not ts or ts <= 0:
        return None
    try:
        return datetime.utcfromtimestamp(int(ts)).date()
    except (ValueError, OSError, OverflowError):
        return None


def _parse_product(item: dict, category_code: str, category_label: str) -> IntesaProduct:
    """Parse a single product dict from the API response."""
    real_rd = item.get("realRedemptionDate", 0)
    return IntesaProduct(
        isin=item["isin"],
        name=item.get("name"),
        category_code=category_code,
        category_label=category_label,
        issuance_date=_unix_to_date(item.get("issuanceDate")),
        maturity_date=_unix_to_date(item.get("expirationDate")),
        protection_pct=item.get("protectionPct"),
        participation_pct=item.get("participationPct"),
        cap_pct=item.get("capPct"),
        underlying=item.get("underlyingName"),
        currency=item.get("curSymbol") or item.get("currency"),
        issuer_name=item.get("issuerName"),
        early_redeemed=bool(item.get("rimborsoAnticipato")),
        real_redemption_date=_unix_to_date(real_rd) if real_rd else None,
        financial_strategy=item.get("financialStrategy"),
    )


async def fetch_category(
    client: httpx.AsyncClient,
    typology: str,
    macro_typology: str,
    type_managed: str,
    label: str,
) -> list[IntesaProduct]:
    """Fetch all products for a single category from the REST API."""
    params = {
        "typology": typology,
        "macroTypology": macro_typology,
        "typeManaged": type_managed,
        "expirationDateFrom": str(DATE_FROM),
        "expirationDateTo": str(DATE_TO),
    }
    try:
        resp = await client.get(INSTANT_URL, params=params)
        resp.raise_for_status()
        data = resp.json()
        titles = data.get("content", {}).get("titles", [])
        return [_parse_product(item, type_managed, label) for item in titles]
    except (httpx.HTTPError, KeyError, ValueError) as e:
        print(f"  Warning: failed to fetch {label}: {e}")
        return []


async def fetch_all_intesa_products(
    categories: list[tuple] | None = None,
    delay: float = 0.5,
) -> list[IntesaProduct]:
    """Fetch all Intesa retail products across all categories.

    Returns a deduplicated list (by ISIN). Some products may appear
    in multiple categories; we keep the first occurrence.
    """
    if categories is None:
        categories = PRODUCT_CATEGORIES

    all_products: list[IntesaProduct] = []
    seen_isins: set[str] = set()

    async with httpx.AsyncClient(
        timeout=30,
        follow_redirects=True,
        headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"},
    ) as client:
        for typology, macro, code, label, _prot_type in categories:
            products = await fetch_category(client, typology, macro, code, label)
            new_count = 0
            for p in products:
                if p.isin not in seen_isins:
                    all_products.append(p)
                    seen_isins.add(p.isin)
                    new_count += 1
            print(f"  {label}: {len(products)} found, {new_count} new (total: {len(all_products)})")
            await asyncio.sleep(delay)

    return all_products
