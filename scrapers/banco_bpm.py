from __future__ import annotations
import asyncio
import json
import re
from pathlib import Path
from dataclasses import dataclass
import httpx

BASE_URL = "https://gruppo.bancobpm.it"

PAGES = {
    "domestic": f"{BASE_URL}/investor-relations/strumenti-di-debito/emissioni-domestiche/",
    "international": f"{BASE_URL}/investor-relations/strumenti-di-debito/emissioni-internazionali/",
    "at1": f"{BASE_URL}/investor-relations/strumenti-finanziari/additional-tier-1/",
}

PILLAR3_PDF_URL = "https://gruppo.bancobpm.it/media/dlm_uploads/Pillar-3-Dicembre-2024_Documento.pdf"
PILLAR3_XLSX_URL = "https://gruppo.bancobpm.it/media/dlm_uploads/EU_CCA_20241231.xlsx"

ISIN_PATTERN = re.compile(r"\b(IT|XS)\d{10}\b")

# Regex to extract JSON document objects embedded in page source
_JSON_DOC_RE = re.compile(r'\{[^{}]*"fileUrl"\s*:\s*"[^"]*\.pdf"[^{}]*\}')


@dataclass
class ProspectusLink:
    isin: str | None
    title: str
    pdf_url: str
    section: str
    doc_type: str  # final_terms, emission_docs, base_prospectus, supplement, other


async def fetch_page(client: httpx.AsyncClient, url: str) -> str:
    resp = await client.get(url, follow_redirects=True, timeout=30)
    resp.raise_for_status()
    return resp.text


def extract_pdf_links(html: str, section: str) -> list[ProspectusLink]:
    """Extract PDF links from JSON objects embedded in the Banco BPM IR pages.

    The site renders content via JS using JSON data embedded in the HTML source.
    Each document is a JSON object with keys: title, fileUrl, year, tax, etc.
    """
    links = []

    for match in _JSON_DOC_RE.finditer(html):
        try:
            obj = json.loads(match.group(0))
        except json.JSONDecodeError:
            continue

        pdf_url = obj.get("fileUrl", "")
        if not pdf_url or not pdf_url.lower().endswith(".pdf"):
            continue

        # Normalize relative URLs to absolute
        if not pdf_url.startswith("http"):
            if not pdf_url.startswith("/"):
                pdf_url = "/" + pdf_url
            pdf_url = BASE_URL + pdf_url

        title = obj.get("title", "")
        combined = f"{title} {pdf_url}".lower()

        # Extract ISIN
        isin_match = ISIN_PATTERN.search(f"{title} {pdf_url}")
        isin = isin_match.group(0) if isin_match else None

        # Classify document type
        if any(kw in combined for kw in ["condizioni definitive", "final terms", "cdns", "cd-ns"]):
            doc_type = "final_terms"
        elif any(kw in combined for kw in ["documenti emissione", "documenti-emissione"]):
            doc_type = "emission_docs"
        elif any(kw in combined for kw in ["prospetto di base", "base prospectus"]):
            doc_type = "base_prospectus"
        elif "supplement" in combined:
            doc_type = "supplement"
        elif any(kw in combined for kw in ["nota informativa", "information note"]):
            doc_type = "information_note"
        elif "prospectus" in combined:
            doc_type = "prospectus"
        else:
            doc_type = "other"

        links.append(ProspectusLink(
            isin=isin, title=title, pdf_url=pdf_url,
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
    """Download Final Terms and Emission Docs PDFs (both contain per-ISIN prospectus data)."""
    output_dir.mkdir(parents=True, exist_ok=True)
    # Include both final_terms and emission_docs — both have per-ISIN instrument details
    downloadable = [l for l in links if l.doc_type in ("final_terms", "emission_docs", "prospectus") and l.isin]
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

        await asyncio.gather(*[_download(l) for l in downloadable])

    return results
