from __future__ import annotations
import asyncio
import re
from pathlib import Path
from dataclasses import dataclass
import httpx
from bs4 import BeautifulSoup

BASE_URL = "https://gruppo.bancobpm.it"

PAGES = {
    "domestic": f"{BASE_URL}/investor-relations/strumenti-di-debito/emissioni-domestiche/",
    "international": f"{BASE_URL}/investor-relations/strumenti-di-debito/emissioni-internazionali/",
    "at1": f"{BASE_URL}/investor-relations/strumenti-finanziari/additional-tier-1/",
}

PILLAR3_PDF_URL = "https://gruppo.bancobpm.it/media/dlm_uploads/Pillar-3-Dicembre-2024_Documento.pdf"
PILLAR3_XLSX_URL = "https://gruppo.bancobpm.it/media/dlm_uploads/EU_CCA_20241231.xlsx"

ISIN_PATTERN = re.compile(r"\b(IT|XS)\d{10}\b")


@dataclass
class ProspectusLink:
    isin: str | None
    title: str
    pdf_url: str
    section: str
    doc_type: str


async def fetch_page(client: httpx.AsyncClient, url: str) -> str:
    resp = await client.get(url, follow_redirects=True, timeout=30)
    resp.raise_for_status()
    return resp.text


def extract_pdf_links(html: str, section: str, base_url: str = BASE_URL) -> list[ProspectusLink]:
    soup = BeautifulSoup(html, "lxml")
    links = []

    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"]
        if not href.lower().endswith(".pdf"):
            continue

        if href.startswith("/"):
            href = base_url + href
        elif not href.startswith("http"):
            continue

        title = a_tag.get_text(strip=True)
        parent_text = ""
        parent = a_tag.find_parent(["div", "li", "td", "tr"])
        if parent:
            parent_text = parent.get_text(" ", strip=True)

        combined_text = f"{title} {href} {parent_text}"
        isin_match = ISIN_PATTERN.search(combined_text)
        isin = isin_match.group(0) if isin_match else None

        # Classify based on the link's own title and href first,
        # then fall back to parent context for broader matching.
        own_text_lower = f"{title} {href}".lower()
        text_lower = combined_text.lower()

        if any(kw in own_text_lower for kw in ["prospetto di base", "base prospectus"]):
            doc_type = "base_prospectus"
        elif any(kw in own_text_lower for kw in ["condizioni definitive", "final terms"]):
            doc_type = "final_terms"
        elif "supplement" in own_text_lower:
            doc_type = "supplement"
        elif any(kw in own_text_lower for kw in ["nota informativa", "information note"]):
            doc_type = "information_note"
        elif any(kw in text_lower for kw in ["condizioni definitive", "final terms"]):
            doc_type = "final_terms"
        elif any(kw in text_lower for kw in ["prospetto di base", "base prospectus"]):
            doc_type = "base_prospectus"
        elif "supplement" in text_lower:
            doc_type = "supplement"
        elif any(kw in text_lower for kw in ["nota informativa", "information note"]):
            doc_type = "information_note"
        else:
            doc_type = "other"

        links.append(ProspectusLink(
            isin=isin, title=title, pdf_url=href,
            section=section, doc_type=doc_type,
        ))

    return links


async def scrape_all_prospectus_links() -> list[ProspectusLink]:
    all_links: list[ProspectusLink] = []
    async with httpx.AsyncClient(
        headers={"User-Agent": "MREL-Analysis/1.0 (academic research)"},
        follow_redirects=True,
    ) as client:
        for section, url in PAGES.items():
            try:
                html = await fetch_page(client, url)
                links = extract_pdf_links(html, section)
                all_links.extend(links)
                print(f"[{section}] Found {len(links)} PDF links")
            except httpx.HTTPError as e:
                print(f"[{section}] Error fetching {url}: {e}")
    return all_links


async def download_pdf(client: httpx.AsyncClient, url: str, output_dir: Path, filename: str | None = None) -> Path:
    if filename is None:
        filename = url.split("/")[-1]
    output_path = output_dir / filename
    if output_path.exists():
        return output_path
    resp = await client.get(url, follow_redirects=True, timeout=60)
    resp.raise_for_status()
    output_path.write_bytes(resp.content)
    return output_path


async def download_all_final_terms(links: list[ProspectusLink], output_dir: Path, max_concurrent: int = 5) -> list[tuple[ProspectusLink, Path]]:
    output_dir.mkdir(parents=True, exist_ok=True)
    final_terms = [l for l in links if l.doc_type == "final_terms"]
    results = []
    semaphore = asyncio.Semaphore(max_concurrent)

    async with httpx.AsyncClient(
        headers={"User-Agent": "MREL-Analysis/1.0 (academic research)"},
        follow_redirects=True,
    ) as client:
        async def _download(link: ProspectusLink):
            async with semaphore:
                try:
                    path = await download_pdf(client, link.pdf_url, output_dir)
                    results.append((link, path))
                except httpx.HTTPError as e:
                    print(f"Error downloading {link.pdf_url}: {e}")

        await asyncio.gather(*[_download(l) for l in final_terms])

    return results
