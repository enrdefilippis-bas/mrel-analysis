from __future__ import annotations

import argparse
import csv
import json
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse

import httpx
import pdfplumber
from bs4 import BeautifulSoup

from dashboard.official_pillar3 import load_official_pillar3_long

DEFAULT_REFERENCE_DATE = "2025-06-30"
ITALY = "Italy"

EXTRA_BANKS_BY_REFERENCE_DATE: dict[str, tuple[str, ...]] = {
    "2025-06-30": (
        "UniCredit S.p.A.",
        "Intesa Sanpaolo S.p.A.",
        "BPER Banca S.p.A.",
    ),
}

DATE_TOKENS = (
    "30.06.2025",
    "30/06/2025",
    "30-06-2025",
    "30_06_2025",
    "30 giugno 2025",
    "30 june 2025",
    "giugno 2025",
    "june 2025",
    "20250630",
)

POSITIVE_HINTS = (
    "pillar",
    "public disclosure",
    "public disclosures",
    "informativa",
    "terzo pilastro",
    "iii pilastro",
)

NEGATIVE_HINTS = (
    "marzo",
    "march",
    "settembre",
    "september",
    "dicembre",
    "december",
    "bilancio",
    "financial report",
    "presentation",
)

KEYWORD_PATTERNS = (
    re.compile(r"\bcbr\b", re.IGNORECASE),
    re.compile(r"combined buffer requirement", re.IGNORECASE),
    re.compile(r"combined buffer", re.IGNORECASE),
    re.compile(r"riserva combinata(?: di capitale)?", re.IGNORECASE),
    re.compile(r"requisito combinato di riserva(?: del capitale)?", re.IGNORECASE),
)

MREL_CONTEXT_PATTERNS = (
    re.compile(r"\bmrel\b", re.IGNORECASE),
    re.compile(r"\btlac\b", re.IGNORECASE),
    re.compile(r"eligible liabilities", re.IGNORECASE),
    re.compile(r"passivit[aà] ammissibili", re.IGNORECASE),
    re.compile(r"fondi propri e passivit[aà] ammissibili", re.IGNORECASE),
)

EXPLICIT_ON_TOP_PATTERNS = (
    re.compile(
        r"(combined buffer requirement|riserva combinata(?: di capitale)?)"
        r".{0,120}(on top|in addition to|added to|oltre|in aggiunta|aggiunt\w+)",
        re.IGNORECASE | re.DOTALL,
    ),
    re.compile(
        r"(on top|in addition to|added to|oltre|in aggiunta|aggiunt\w+)"
        r".{0,120}(combined buffer requirement|riserva combinata(?: di capitale)?)",
        re.IGNORECASE | re.DOTALL,
    ),
    re.compile(
        r"(mrel[\w\s\-():%,]{0,80})?(maggiorat\w+ del|a cui sommare|da sommare a)"
        r".{0,120}(combined buffer requirement|riserva combinata(?: di capitale)?|requisito combinato di riserva(?: del capitale)?|\bcbr\b)",
        re.IGNORECASE | re.DOTALL,
    ),
    re.compile(
        r"(combined buffer requirement|riserva combinata(?: di capitale)?|requisito combinato di riserva(?: del capitale)?|\bcbr\b)"
        r".{0,120}(maggiorat\w+ del|a cui sommare|da sommare a)",
        re.IGNORECASE | re.DOTALL,
    ),
    re.compile(
        r"(va aggiunt\w+|in aggiunta ai?|aggiunt\w+ ai?)"
        r".{0,120}(requisiti?\s+mrel|mrel[\w\s\-():%,]{0,80})",
        re.IGNORECASE | re.DOTALL,
    ),
)

EXPLICIT_INCLUDED_PATTERNS = (
    re.compile(
        r"(includes|including|inclusive of|comprensiv\w+|inclus\w+)"
        r".{0,120}(combined buffer requirement|riserva combinata(?: di capitale)?)",
        re.IGNORECASE | re.DOTALL,
    ),
    re.compile(
        r"(combined buffer requirement|riserva combinata(?: di capitale)?)"
        r".{0,120}(included|including|inclusive|compresa|compreso|inclus\w+)",
        re.IGNORECASE | re.DOTALL,
    ),
    re.compile(
        r"(incl\.?|comprensiv\w+|inclusiv\w+)"
        r".{0,120}(\bcbr\b|combined buffer requirement|riserva combinata(?: di capitale)?|requisito combinato di riserva(?: del capitale)?)",
        re.IGNORECASE | re.DOTALL,
    ),
    re.compile(
        r"(\bcbr\b|combined buffer requirement|riserva combinata(?: di capitale)?|requisito combinato di riserva(?: del capitale)?)"
        r".{0,120}(incl\.?|comprensiv\w+|inclusiv\w+)",
        re.IGNORECASE | re.DOTALL,
    ),
)


@dataclass(frozen=True)
class SourceConfig:
    bank_name: str
    landing_url: str | None = None
    pdf_url: str | None = None
    note: str | None = None


@dataclass(frozen=True)
class MatchSnippet:
    page: int
    keyword: str
    snippet: str


@dataclass(frozen=True)
class CBRExtractionResult:
    bank_name: str
    reference_date: str
    source_url: str | None
    source_type: str
    note: str | None
    status: str
    pdf_path: str | None
    text_path: str | None
    match_count: int
    classification: str
    matches: tuple[MatchSnippet, ...]


SOURCE_REGISTRY: dict[str, SourceConfig] = {
    "BANCO BPM SOCIETA' PER AZIONI": SourceConfig(
        bank_name="BANCO BPM SOCIETA' PER AZIONI",
        pdf_url="https://gruppo.bancobpm.it/media/dlm_uploads/P3-Giugno-2025_Documento.pdf",
    ),
    "BANCA MEDIOLANUM S.P.A.": SourceConfig(
        bank_name="BANCA MEDIOLANUM S.P.A.",
        pdf_url="https://www.bancamediolanum.it/static-assets/documenti/file/it/2025/12/23/Pillar_III_30giugno2025.pdf",
    ),
    "BANCA POPOLARE DI SONDRIO SOCIETA' PER AZIONI": SourceConfig(
        bank_name="BANCA POPOLARE DI SONDRIO SOCIETA' PER AZIONI",
        landing_url="https://istituzionale.popso.it/it/archivio-generale/investor/pillar3",
    ),
    "Banca Monte dei Paschi di Siena S.p.A.": SourceConfig(
        bank_name="Banca Monte dei Paschi di Siena S.p.A.",
        pdf_url="https://www.gruppomps.it/static/upload/inf/informativa-al-pubblico---giugno-2025.pdf",
    ),
    "CASSA CENTRALE BANCA - CREDITO COOPERATIVO ITALIANOSOCIETA' PER AZIONI (IN SIGLA CASSA CENTRALE BANCA)": SourceConfig(
        bank_name="CASSA CENTRALE BANCA - CREDITO COOPERATIVO ITALIANOSOCIETA' PER AZIONI (IN SIGLA CASSA CENTRALE BANCA)",
        landing_url="https://www.cassacentrale.it/it/investitori/pillar-3",
    ),
    "CREDITO EMILIANO HOLDING SOCIETA' PER AZIONI": SourceConfig(
        bank_name="CREDITO EMILIANO HOLDING SOCIETA' PER AZIONI",
        pdf_url="https://cdn.financialreports.eu/financialreports/media/filings/4312/2025/RNS/4312_rns_2025-09-16_b2dc6af3-f551-41e0-abc0-9f6b783b415c.pdf",
        note="Using a FinancialReports mirror because the official Credem investor page does not expose a stable PDF link in static HTML.",
    ),
    "ICCREA BANCA S.P.A. - ISTITUTO CENTRALE DEL CREDITO COOPERATIVO (IN FORMA ABBREVIATA: ICCREA BANCA S.P.A.)": SourceConfig(
        bank_name="ICCREA BANCA S.P.A. - ISTITUTO CENTRALE DEL CREDITO COOPERATIVO (IN FORMA ABBREVIATA: ICCREA BANCA S.P.A.)",
        landing_url="https://www.gruppobcciccrea.it/Pagine/Governance/Informativa-Pillar-III.aspx",
    ),
    "Mediobanca - Banca di Credito Finanziario S.p.A.": SourceConfig(
        bank_name="Mediobanca - Banca di Credito Finanziario S.p.A.",
        pdf_url="https://www.mediobanca.com/static/upload_new/doc/documento-pillar-ita-30-06-2025.pdf",
    ),
    "MEDIOBANCA PREMIER S.P.A.": SourceConfig(
        bank_name="MEDIOBANCA PREMIER S.P.A.",
        pdf_url="https://www.mediobanca.com/static/upload_new/doc/documento-pillar-ita-30-06-2025.pdf",
        note="Using the Mediobanca Group Pillar 3 document as fallback because no standalone Mediobanca Premier Pillar 3 source was pre-mapped.",
    ),
    "UniCredit S.p.A.": SourceConfig(
        bank_name="UniCredit S.p.A.",
        pdf_url="https://www.unicreditgroup.eu/content/dam/unicreditgroup-eu/documents/en/investors/third-pillar-basel/2025/UniCredit-Group-Disclosure-Pillar-III-as-at-30-June-2025.pdf",
        note="Included manually for CBR research because the entity is not present in the workbook export used for the dashboard.",
    ),
    "Intesa Sanpaolo S.p.A.": SourceConfig(
        bank_name="Intesa Sanpaolo S.p.A.",
        pdf_url="https://group.intesasanpaolo.com/content/dam/portalgroup/repository-documenti/investor-relations/Contenuti/RISORSE/Documenti%20PDF/governance/Pillar3_30062025.pdf",
        note="Included manually for CBR research because the entity is not present in the workbook export used for the dashboard.",
    ),
    "BPER Banca S.p.A.": SourceConfig(
        bank_name="BPER Banca S.p.A.",
        landing_url="https://group.bper.it/investor-relations/risultati-gruppo/pillar-3",
        note="Included manually for CBR research because the entity is not present in the workbook export used for the dashboard.",
    ),
}


def bank_slug(name: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")
    return slug or "bank"


def filter_banks_from_long_df(
    records_df: Any,
    country: str = ITALY,
    reference_date: str = DEFAULT_REFERENCE_DATE,
) -> list[str]:
    subset = records_df[
        (records_df["country"] == country)
        & (records_df["reference_date"] == reference_date)
    ]
    return sorted(subset["entity_name"].dropna().unique().tolist())


def list_italian_banks_for_reference_date(
    reference_date: str = DEFAULT_REFERENCE_DATE,
    workbook_path: str | None = None,
) -> list[str]:
    records_df = load_official_pillar3_long(workbook_path)
    workbook_banks = filter_banks_from_long_df(records_df, country=ITALY, reference_date=reference_date)
    extra_banks = list(EXTRA_BANKS_BY_REFERENCE_DATE.get(reference_date, ()))
    return sorted({*workbook_banks, *extra_banks})


def extract_pdf_links(html: str, base_url: str) -> list[dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    found: list[dict[str, str]] = []
    for anchor in soup.find_all("a", href=True):
        href = anchor.get("href", "").strip()
        if not href:
            continue
        absolute_url = urljoin(base_url, href)
        label = " ".join(anchor.get_text(" ", strip=True).split())
        if ".pdf" not in absolute_url.lower() and "pdf" not in label.lower():
            continue
        found.append({"url": absolute_url, "label": label})
    return found


def score_pdf_candidate(url: str, label: str = "") -> int:
    haystack = f"{url} {label}".lower()
    score = 0
    for token in DATE_TOKENS:
        if token in haystack:
            score += 12
    for token in POSITIVE_HINTS:
        if token in haystack:
            score += 3
    for token in NEGATIVE_HINTS:
        if token in haystack and not any(date_token in haystack for date_token in DATE_TOKENS[:8]):
            score -= 6
    if "2024" in haystack or "2023" in haystack:
        score -= 10
    if "2025" in haystack:
        score += 2
    return score


def choose_best_pdf_link(links: list[dict[str, str]]) -> str | None:
    scored = sorted(
        ((score_pdf_candidate(item["url"], item.get("label", "")), item["url"]) for item in links),
        key=lambda item: item[0],
        reverse=True,
    )
    if not scored or scored[0][0] <= 0:
        return None
    return scored[0][1]


def discover_pdf_url(client: httpx.Client, source: SourceConfig) -> tuple[str | None, str]:
    if source.pdf_url:
        return source.pdf_url, "direct_pdf"
    if not source.landing_url:
        return None, "missing_source"

    response = client.get(source.landing_url)
    response.raise_for_status()
    links = extract_pdf_links(response.text, source.landing_url)
    best_url = choose_best_pdf_link(links)
    if not best_url:
        return None, "discovery_failed"
    return best_url, "discovered_from_landing_page"


def fetch_pdf(client: httpx.Client, pdf_url: str, output_path: Path) -> None:
    response = client.get(pdf_url)
    response.raise_for_status()
    output_path.write_bytes(response.content)


def extract_pdf_text_pages(pdf_path: Path) -> list[str]:
    pages: list[str] = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ""
            pages.append(" ".join(text.split()))
    return pages


def has_mrel_context(text: str) -> bool:
    return any(pattern.search(text) for pattern in MREL_CONTEXT_PATTERNS)


def build_match_snippets(page_texts: list[str], window: int = 220) -> tuple[MatchSnippet, ...]:
    snippets: list[MatchSnippet] = []
    seen: set[tuple[int, str]] = set()
    for page_index, page_text in enumerate(page_texts, start=1):
        if not page_text:
            continue
        for pattern in KEYWORD_PATTERNS:
            for match in pattern.finditer(page_text):
                start = max(match.start() - window, 0)
                end = min(match.end() + window, len(page_text))
                snippet = page_text[start:end].strip()
                if not has_mrel_context(snippet):
                    continue
                signature = (page_index, snippet)
                if signature in seen:
                    continue
                seen.add(signature)
                snippets.append(
                    MatchSnippet(
                        page=page_index,
                        keyword=pattern.pattern,
                        snippet=snippet,
                    )
                )
    return tuple(snippets)


def classify_cbr_text(page_texts: list[str]) -> str:
    relevant_snippets = build_match_snippets(page_texts)
    if not relevant_snippets:
        return "no_match"

    combined_text = "\n".join(snippet.snippet for snippet in relevant_snippets)
    for pattern in EXPLICIT_ON_TOP_PATTERNS:
        if pattern.search(combined_text):
            return "explicit_on_top"
    for pattern in EXPLICIT_INCLUDED_PATTERNS:
        if pattern.search(combined_text):
            return "explicit_included"
    return "mentioned_unclear"


def write_text_dump(page_texts: list[str], output_path: Path) -> None:
    chunks = [f"=== PAGE {index} ===\n{text}" for index, text in enumerate(page_texts, start=1)]
    output_path.write_text("\n\n".join(chunks))


def scrape_bank_cbr(
    client: httpx.Client,
    bank_name: str,
    reference_date: str,
    output_root: Path,
) -> CBRExtractionResult:
    raw_dir = output_root / "raw"
    text_dir = output_root / "text"
    json_dir = output_root / "json"
    raw_dir.mkdir(parents=True, exist_ok=True)
    text_dir.mkdir(parents=True, exist_ok=True)
    json_dir.mkdir(parents=True, exist_ok=True)

    source = SOURCE_REGISTRY.get(bank_name, SourceConfig(bank_name=bank_name))
    pdf_url, source_type = discover_pdf_url(client, source)
    slug = bank_slug(bank_name)

    if not pdf_url:
        result = CBRExtractionResult(
            bank_name=bank_name,
            reference_date=reference_date,
            source_url=source.landing_url or source.pdf_url,
            source_type=source_type,
            note=source.note,
            status="missing_pdf_url",
            pdf_path=None,
            text_path=None,
            match_count=0,
            classification="source_not_found",
            matches=(),
        )
        (json_dir / f"{slug}.json").write_text(json.dumps(asdict(result), indent=2))
        return result

    suffix = Path(urlparse(pdf_url).path).suffix or ".pdf"
    pdf_path = raw_dir / f"{slug}{suffix}"
    fetch_pdf(client, pdf_url, pdf_path)

    page_texts = extract_pdf_text_pages(pdf_path)
    text_path = text_dir / f"{slug}.txt"
    write_text_dump(page_texts, text_path)

    matches = build_match_snippets(page_texts)
    classification = classify_cbr_text(page_texts)
    result = CBRExtractionResult(
        bank_name=bank_name,
        reference_date=reference_date,
        source_url=pdf_url,
        source_type=source_type,
        note=source.note,
        status="ok",
        pdf_path=str(pdf_path),
        text_path=str(text_path),
        match_count=len(matches),
        classification=classification,
        matches=matches,
    )
    (json_dir / f"{slug}.json").write_text(json.dumps(asdict(result), indent=2))
    return result


def write_summary(results: list[CBRExtractionResult], output_root: Path) -> None:
    summary_path = output_root / "summary.csv"
    with summary_path.open("w", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "bank_name",
                "reference_date",
                "status",
                "classification",
                "match_count",
                "source_type",
                "source_url",
                "pdf_path",
                "text_path",
                "note",
            ],
        )
        writer.writeheader()
        for result in results:
            writer.writerow(
                {
                    "bank_name": result.bank_name,
                    "reference_date": result.reference_date,
                    "status": result.status,
                    "classification": result.classification,
                    "match_count": result.match_count,
                    "source_type": result.source_type,
                    "source_url": result.source_url,
                    "pdf_path": result.pdf_path,
                    "text_path": result.text_path,
                    "note": result.note,
                }
            )


def run_scrape(
    reference_date: str = DEFAULT_REFERENCE_DATE,
    output_dir: str = "cbr",
    workbook_path: str | None = None,
) -> list[CBRExtractionResult]:
    output_root = Path(output_dir)
    output_root.mkdir(parents=True, exist_ok=True)
    banks = list_italian_banks_for_reference_date(reference_date=reference_date, workbook_path=workbook_path)

    results: list[CBRExtractionResult] = []
    with httpx.Client(
        headers={"User-Agent": "MREL-Analysis/1.0 (CBR PDF research)"},
        follow_redirects=True,
        timeout=90,
    ) as client:
        for bank_name in banks:
            results.append(scrape_bank_cbr(client, bank_name, reference_date, output_root))

    write_summary(results, output_root)
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="Scrape Italian Pillar 3 PDFs and extract CBR wording.")
    parser.add_argument("--reference-date", default=DEFAULT_REFERENCE_DATE)
    parser.add_argument("--output-dir", default="cbr")
    parser.add_argument("--workbook-path", default=None)
    args = parser.parse_args()

    results = run_scrape(
        reference_date=args.reference_date,
        output_dir=args.output_dir,
        workbook_path=args.workbook_path,
    )
    print(f"Processed {len(results)} banks")
    for result in results:
        print(f"{result.bank_name}: {result.status} | {result.classification} | {result.source_url}")


if __name__ == "__main__":
    main()
