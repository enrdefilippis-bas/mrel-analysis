from __future__ import annotations
import re
from dataclasses import dataclass
import httpx
from bs4 import BeautifulSoup

BASE_URL = "https://www.borsaitaliana.it"
SEARCH_URL = f"{BASE_URL}/borsa/obbligazioni/advanced-search.html"


@dataclass
class BorsaItalianaInstrument:
    isin: str
    name: str
    market: str
    outstanding_amount: float | None
    currency: str | None
    maturity_date: str | None
    coupon_rate: str | None
    last_price: float | None
    detail_url: str | None


async def search_bonds_by_issuer(
    issuer: str = "BANCO BPM",
    market: str = "",
) -> list[BorsaItalianaInstrument]:
    instruments = []
    page = 1

    async with httpx.AsyncClient(
        headers={
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
            "Accept": "text/html",
            "Referer": f"{BASE_URL}/borsa/obbligazioni/ricerca-avanzata.html",
        },
        follow_redirects=True,
        timeout=30,
    ) as client:
        while True:
            params = {
                "issuerName": issuer,
                "page": str(page),
                "lang": "it",
            }
            if market:
                params["market"] = market

            try:
                resp = await client.get(SEARCH_URL, params=params)
                resp.raise_for_status()
            except httpx.HTTPError as e:
                print(f"Error fetching Borsa Italiana page {page}: {e}")
                break

            page_instruments = _parse_search_results(resp.text)
            if not page_instruments:
                break

            instruments.extend(page_instruments)
            page += 1

            if page > 50:
                print("Warning: hit 50-page limit on Borsa Italiana search")
                break

    return instruments


def _parse_search_results(html: str) -> list[BorsaItalianaInstrument]:
    soup = BeautifulSoup(html, "lxml")
    instruments = []

    table = soup.find("table")
    if not table:
        return []

    rows = table.find_all("tr")
    for row in rows[1:]:
        cells = row.find_all("td")
        if len(cells) < 3:
            continue

        link = row.find("a", href=True)
        name = cells[0].get_text(strip=True) if cells else ""
        isin = ""
        detail_url = None

        if link:
            href = link.get("href", "")
            detail_url = BASE_URL + href if href.startswith("/") else href
            isin_match = re.search(r"\b(IT|XS)\d{10}\b", f"{name} {href}")
            if isin_match:
                isin = isin_match.group(0)

        instruments.append(BorsaItalianaInstrument(
            isin=isin,
            name=name,
            market=cells[1].get_text(strip=True) if len(cells) > 1 else "",
            outstanding_amount=_parse_amount(cells),
            currency="EUR",
            maturity_date=None,
            coupon_rate=None,
            last_price=None,
            detail_url=detail_url,
        ))

    return instruments


def _parse_amount(cells: list) -> float | None:
    for cell in cells:
        text = cell.get_text(strip=True).replace(".", "").replace(",", ".")
        try:
            val = float(text)
            if val > 1000:
                return val
        except ValueError:
            continue
    return None


async def get_instrument_detail(detail_url: str) -> dict:
    async with httpx.AsyncClient(
        headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"},
        follow_redirects=True,
        timeout=30,
    ) as client:
        resp = await client.get(detail_url)
        resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "lxml")
    details = {}

    for row in soup.find_all("tr"):
        cells = row.find_all(["th", "td"])
        if len(cells) == 2:
            key = cells[0].get_text(strip=True).lower()
            value = cells[1].get_text(strip=True)
            details[key] = value

    return details
