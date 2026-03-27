from __future__ import annotations
import asyncio
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
    """Parse Borsa Italiana bond search results table.

    Table structure: row 0 is empty, row 1 is header (Isin, Descrizione, Ultimo, Cedola, Scadenza, ''),
    data rows start at row 2. ISIN is extracted from the href code= parameter in cell[0].
    """
    soup = BeautifulSoup(html, "lxml")
    instruments = []

    table = soup.find("table")
    if not table:
        return []

    rows = table.find_all("tr")
    # Skip header rows (row 0 is empty, row 1 is headers)
    for row in rows[2:]:
        cells = row.find_all("td")
        if len(cells) < 5:
            continue

        # Extract ISIN from the link href (code= parameter)
        isin = ""
        detail_url = None
        link = cells[0].find("a", href=True)
        if link:
            href = link.get("href", "")
            code_match = re.search(r"code=([A-Z0-9]+)", href)
            if code_match:
                isin = code_match.group(1)
            detail_url = BASE_URL + href if href.startswith("/") else href

        name = cells[1].get_text(strip=True)
        coupon_rate = cells[3].get_text(strip=True) or None
        maturity_date = cells[4].get_text(strip=True) or None

        # Parse last price from cell[2]
        last_price = None
        price_text = cells[2].get_text(strip=True).replace(".", "").replace(",", ".")
        try:
            last_price = float(price_text)
        except ValueError:
            pass

        instruments.append(BorsaItalianaInstrument(
            isin=isin,
            name=name,
            market="MOT",
            outstanding_amount=None,  # Not available in search results
            currency="EUR",
            maturity_date=maturity_date,
            coupon_rate=coupon_rate,
            last_price=last_price,
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


@dataclass
class InstrumentDetail:
    """Detailed data from Borsa Italiana instrument page."""
    isin: str
    name: str | None = None
    maturity_date: str | None = None
    coupon_rate: float | None = None
    currency: str | None = None
    market: str | None = None


DETAIL_URLS = [
    "{base}/borsa/obbligazioni/eurotlx/dati-completi.html?isin={isin}&mic=ETLX&lang=it",
    "{base}/borsa/obbligazioni/mot/dati-completi.html?isin={isin}&mic=MOTX&lang=it",
    "{base}/borsa/obbligazioni/euromot/dati-completi.html?isin={isin}&mic=MOTX&lang=it",
]


def _parse_detail_page(html: str) -> dict[str, str]:
    """Parse key-value pairs from all tables on a Borsa Italiana detail page."""
    soup = BeautifulSoup(html, "lxml")
    details = {}
    for table in soup.find_all("table"):
        for row in table.find_all("tr"):
            cells = row.find_all(["th", "td"])
            if len(cells) == 2:
                key = cells[0].get_text(strip=True).lower()
                value = cells[1].get_text(strip=True)
                if key and value:
                    details[key] = value
    return details


async def fetch_instrument_detail(
    client: httpx.AsyncClient, isin: str
) -> InstrumentDetail | None:
    """Fetch instrument detail from Borsa Italiana by ISIN, trying multiple markets."""
    for url_tpl in DETAIL_URLS:
        url = url_tpl.format(base=BASE_URL, isin=isin)
        try:
            resp = await client.get(url)
            if resp.status_code != 200:
                continue
            details = _parse_detail_page(resp.text)
            if not details or "codice isin" not in details:
                continue

            # Parse coupon rate
            coupon = None
            for key in ["tasso cedola su base annua", "tasso cedola periodale"]:
                if key in details and details[key]:
                    try:
                        coupon = float(details[key].replace(",", "."))
                    except ValueError:
                        pass
                    if coupon:
                        break

            market = "EuroTLX" if "etlx" in url.lower() else "MOT"

            return InstrumentDetail(
                isin=isin,
                name=details.get("nome") or details.get("descrizione"),
                maturity_date=details.get("data di scadenza"),
                coupon_rate=coupon,
                currency=details.get("valuta di negoziazione", "EUR"),
                market=market,
            )
        except httpx.HTTPError:
            continue
    return None


async def fetch_all_instrument_details(
    isins: list[str], max_concurrent: int = 2, delay: float = 1.0
) -> list[InstrumentDetail]:
    """Fetch details for multiple ISINs with rate limiting to avoid 403s."""
    results: list[InstrumentDetail] = []
    semaphore = asyncio.Semaphore(max_concurrent)
    failed = 0

    async with httpx.AsyncClient(
        headers={
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            "Accept": "text/html,application/xhtml+xml",
            "Accept-Language": "it-IT,it;q=0.9",
        },
        follow_redirects=True,
        timeout=15,
    ) as client:
        async def _fetch(isin: str):
            nonlocal failed
            async with semaphore:
                await asyncio.sleep(delay)  # Rate limit
                detail = await fetch_instrument_detail(client, isin)
                if detail:
                    results.append(detail)
                else:
                    failed += 1
                    if failed >= 10:
                        return  # Stop if too many failures (likely blocked)

        await asyncio.gather(*[_fetch(isin) for isin in isins])

    return results
