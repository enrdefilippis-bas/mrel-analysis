# MREL Analysis — Banco BPM — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a Streamlit dashboard that classifies Banco BPM debt instruments at the ISIN level for MREL eligibility by scraping prospectuses, cross-referencing Borsa Italiana, and reconciling against Pillar 3 data (ref date 31.12.2024).

**Architecture:** Prospectus-first pipeline — scrape Final Terms PDFs from Banco BPM IR, extract ISINs and contractual terms, classify per CRR2/BRRD2 rules, enrich with outstanding amounts from Borsa Italiana, and validate against Pillar 3 aggregates. Streamlit dashboard for exploration and analysis.

**Tech Stack:** Python 3.14, Streamlit, httpx, BeautifulSoup4, pdfplumber, pandas, plotly, SQLite

---

## Task 1: Project Scaffolding & Dependencies

**Files:**
- Create: `requirements.txt`
- Create: `scrapers/__init__.py`
- Create: `parsers/__init__.py`
- Create: `models/__init__.py`
- Create: `dashboard/__init__.py`
- Create: `dashboard/views/__init__.py`
- Create: `dashboard/components/__init__.py`
- Create: `.gitignore`

**Step 1: Create project directories**

```bash
cd ~/Desktop/mrel-analysis
mkdir -p scrapers parsers models dashboard/views dashboard/components data/{raw,processed,db} tests
```

**Step 2: Create requirements.txt**

```
httpx>=0.27
beautifulsoup4>=4.12
pdfplumber>=0.11
pandas>=2.2
plotly>=5.24
streamlit>=1.40
openpyxl>=3.1
python-dotenv>=1.0
lxml>=5.3
```

**Step 3: Create .gitignore**

```
venv/
__pycache__/
*.pyc
data/raw/
data/db/
.env
*.egg-info/
.streamlit/
```

**Step 4: Create all `__init__.py` files (empty)**

**Step 5: Set up venv and install dependencies**

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

**Step 6: Commit**

```bash
git add requirements.txt .gitignore scrapers/ parsers/ models/ dashboard/ tests/
git commit -m "feat: scaffold project structure and dependencies"
```

---

## Task 2: Data Models

**Files:**
- Create: `models/instrument.py`
- Create: `models/eligibility.py`
- Create: `models/mrel_stack.py`
- Create: `tests/test_models.py`

**Step 1: Write failing tests for Instrument model**

```python
# tests/test_models.py
from datetime import date
from models.instrument import Instrument, InstrumentCategory, CouponType

def test_instrument_creation():
    inst = Instrument(
        isin="IT0005692246",
        name="Banco BPM 4.5% 2027",
        category=InstrumentCategory.SENIOR_PREFERRED,
        issue_date=date(2024, 1, 15),
        maturity_date=date(2027, 1, 15),
        coupon_type=CouponType.FIXED,
        coupon_rate=4.5,
        outstanding_amount=500_000_000,
        currency="EUR",
        listing_venue="MOT",
        mrel_eligible=True,
        eligibility_reason="Plain vanilla senior, residual maturity > 1yr",
        crr2_rank=5,
    )
    assert inst.isin == "IT0005692246"
    assert inst.mrel_eligible is True

def test_residual_maturity():
    inst = Instrument(
        isin="IT0005692246",
        name="Test Bond",
        category=InstrumentCategory.SENIOR_PREFERRED,
        issue_date=date(2024, 1, 15),
        maturity_date=date(2027, 1, 15),
        coupon_type=CouponType.FIXED,
        outstanding_amount=500_000_000,
        currency="EUR",
        crr2_rank=5,
    )
    ref_date = date(2024, 12, 31)
    rm = inst.residual_maturity_years(ref_date)
    assert rm > 2.0
    assert rm < 2.1

def test_residual_maturity_expired():
    inst = Instrument(
        isin="IT0005692246",
        name="Expired Bond",
        category=InstrumentCategory.SENIOR_PREFERRED,
        issue_date=date(2020, 1, 1),
        maturity_date=date(2024, 6, 30),
        coupon_type=CouponType.FIXED,
        outstanding_amount=100_000_000,
        currency="EUR",
        crr2_rank=5,
    )
    ref_date = date(2024, 12, 31)
    rm = inst.residual_maturity_years(ref_date)
    assert rm < 0

def test_perpetual_residual_maturity():
    inst = Instrument(
        isin="XS1234567890",
        name="AT1 Perpetual",
        category=InstrumentCategory.AT1,
        issue_date=date(2020, 1, 1),
        maturity_date=None,  # perpetual
        coupon_type=CouponType.FIXED,
        outstanding_amount=500_000_000,
        currency="EUR",
        crr2_rank=2,
    )
    ref_date = date(2024, 12, 31)
    rm = inst.residual_maturity_years(ref_date)
    assert rm == float("inf")
```

**Step 2: Run tests to verify they fail**

```bash
cd ~/Desktop/mrel-analysis
source venv/bin/activate
python -m pytest tests/test_models.py -v
```
Expected: FAIL — `ModuleNotFoundError: No module named 'models.instrument'`

**Step 3: Implement Instrument model**

```python
# models/instrument.py
from __future__ import annotations
from dataclasses import dataclass, field
from datetime import date
from enum import Enum


class InstrumentCategory(Enum):
    CET1 = "CET1"
    AT1 = "AT1"
    TIER2 = "Tier 2"
    SENIOR_NON_PREFERRED = "Senior Non-Preferred"
    SENIOR_PREFERRED = "Senior Preferred"
    STRUCTURED_NOTE_PROTECTED = "Structured Note (Capital Protected)"
    CERTIFICATE = "Certificate (Non-Protected)"
    COVERED_BOND = "Covered Bond"
    UNKNOWN = "Unknown"


class CouponType(Enum):
    FIXED = "Fixed"
    FLOATING = "Floating"
    ZERO_COUPON = "Zero Coupon"
    STEP_UP = "Step Up"
    STEP_DOWN = "Step Down"
    STRUCTURED = "Structured"
    UNKNOWN = "Unknown"


@dataclass
class Instrument:
    isin: str
    name: str
    category: InstrumentCategory
    issue_date: date | None
    maturity_date: date | None  # None = perpetual
    coupon_type: CouponType
    outstanding_amount: float | None
    currency: str
    crr2_rank: int | None = None
    coupon_rate: float | None = None
    listing_venue: str | None = None
    mrel_eligible: bool | None = None
    eligibility_reason: str | None = None
    prospectus_url: str | None = None
    source_pdf: str | None = None
    classification_confidence: float = 1.0  # 0.0-1.0
    bail_in_clause: bool | None = None
    capital_protected: bool | None = None
    underlying_linked: bool | None = None
    raw_prospectus_text: str | None = None

    def residual_maturity_years(self, ref_date: date) -> float:
        if self.maturity_date is None:
            return float("inf")
        delta = self.maturity_date - ref_date
        return delta.days / 365.25

    def is_maturity_eligible(self, ref_date: date) -> bool:
        return self.residual_maturity_years(ref_date) > 1.0
```

**Step 4: Run tests to verify they pass**

```bash
python -m pytest tests/test_models.py -v
```
Expected: all PASS

**Step 5: Implement eligibility criteria**

```python
# models/eligibility.py
from __future__ import annotations
from dataclasses import dataclass
from datetime import date
from models.instrument import Instrument, InstrumentCategory


@dataclass
class EligibilityResult:
    eligible: bool
    reason: str
    mrel_layer: str  # "subordination", "total", "excluded"
    crr2_article: str | None = None


def assess_mrel_eligibility(inst: Instrument, ref_date: date) -> EligibilityResult:
    """Assess MREL eligibility per CRR2 Art. 72a and BRRD2 Art. 45b."""

    # Covered bonds are always excluded
    if inst.category == InstrumentCategory.COVERED_BOND:
        return EligibilityResult(
            eligible=False,
            reason="Covered bonds excluded from MREL",
            mrel_layer="excluded",
            crr2_article="Art. 72a(2)(d)",
        )

    # Certificates (non-protected) excluded
    if inst.category == InstrumentCategory.CERTIFICATE:
        return EligibilityResult(
            eligible=False,
            reason="Non-protected certificate: principal at risk, excluded from MREL",
            mrel_layer="excluded",
            crr2_article="Art. 72a(2)(l)",
        )

    # Check residual maturity >= 1 year
    if not inst.is_maturity_eligible(ref_date):
        return EligibilityResult(
            eligible=False,
            reason=f"Residual maturity < 1 year (maturity: {inst.maturity_date})",
            mrel_layer="excluded",
            crr2_article="Art. 72c(1)",
        )

    # Subordination bucket: CET1, AT1, T2, SNP
    subordination_categories = {
        InstrumentCategory.CET1,
        InstrumentCategory.AT1,
        InstrumentCategory.TIER2,
        InstrumentCategory.SENIOR_NON_PREFERRED,
    }

    if inst.category in subordination_categories:
        return EligibilityResult(
            eligible=True,
            reason=f"{inst.category.value}: counts towards subordination and total MREL",
            mrel_layer="subordination",
            crr2_article="Art. 72a(1)",
        )

    # Total MREL bucket: Senior Preferred and capital-protected structured notes
    if inst.category in (
        InstrumentCategory.SENIOR_PREFERRED,
        InstrumentCategory.STRUCTURED_NOTE_PROTECTED,
    ):
        return EligibilityResult(
            eligible=True,
            reason=f"{inst.category.value}: counts towards total MREL only",
            mrel_layer="total",
            crr2_article="Art. 72a(1)",
        )

    return EligibilityResult(
        eligible=False,
        reason=f"Unknown category: {inst.category.value}",
        mrel_layer="excluded",
    )
```

**Step 6: Implement MREL stack computation**

```python
# models/mrel_stack.py
from __future__ import annotations
from dataclasses import dataclass, field
from datetime import date
from models.instrument import Instrument, InstrumentCategory
from models.eligibility import assess_mrel_eligibility


@dataclass
class MRELStack:
    """MREL capacity waterfall by instrument category."""

    ref_date: date
    cet1: float = 0.0
    at1: float = 0.0
    tier2: float = 0.0
    senior_non_preferred: float = 0.0
    senior_preferred: float = 0.0
    structured_notes_protected: float = 0.0
    excluded_certificates: float = 0.0
    excluded_covered_bonds: float = 0.0
    excluded_maturity: float = 0.0
    excluded_other: float = 0.0

    @property
    def subordination_capacity(self) -> float:
        return self.cet1 + self.at1 + self.tier2 + self.senior_non_preferred

    @property
    def total_mrel_capacity(self) -> float:
        return (
            self.subordination_capacity
            + self.senior_preferred
            + self.structured_notes_protected
        )

    @property
    def total_excluded(self) -> float:
        return (
            self.excluded_certificates
            + self.excluded_covered_bonds
            + self.excluded_maturity
            + self.excluded_other
        )

    @classmethod
    def from_instruments(cls, instruments: list[Instrument], ref_date: date) -> MRELStack:
        stack = cls(ref_date=ref_date)
        category_map = {
            InstrumentCategory.CET1: "cet1",
            InstrumentCategory.AT1: "at1",
            InstrumentCategory.TIER2: "tier2",
            InstrumentCategory.SENIOR_NON_PREFERRED: "senior_non_preferred",
            InstrumentCategory.SENIOR_PREFERRED: "senior_preferred",
            InstrumentCategory.STRUCTURED_NOTE_PROTECTED: "structured_notes_protected",
        }

        for inst in instruments:
            amount = inst.outstanding_amount or 0.0
            result = assess_mrel_eligibility(inst, ref_date)

            if result.eligible:
                attr = category_map.get(inst.category)
                if attr:
                    setattr(stack, attr, getattr(stack, attr) + amount)
            else:
                if inst.category == InstrumentCategory.CERTIFICATE:
                    stack.excluded_certificates += amount
                elif inst.category == InstrumentCategory.COVERED_BOND:
                    stack.excluded_covered_bonds += amount
                elif not inst.is_maturity_eligible(ref_date):
                    stack.excluded_maturity += amount
                else:
                    stack.excluded_other += amount

        return stack

    def to_dict(self) -> dict:
        return {
            "CET1": self.cet1,
            "AT1": self.at1,
            "Tier 2": self.tier2,
            "Senior Non-Preferred": self.senior_non_preferred,
            "Senior Preferred": self.senior_preferred,
            "Structured Notes (Protected)": self.structured_notes_protected,
            "Total Subordination": self.subordination_capacity,
            "Total MREL": self.total_mrel_capacity,
            "Excluded - Certificates": self.excluded_certificates,
            "Excluded - Covered Bonds": self.excluded_covered_bonds,
            "Excluded - Maturity < 1yr": self.excluded_maturity,
            "Excluded - Other": self.excluded_other,
        }
```

**Step 7: Add tests for eligibility and stack**

```python
# tests/test_eligibility.py
from datetime import date
from models.instrument import Instrument, InstrumentCategory, CouponType
from models.eligibility import assess_mrel_eligibility
from models.mrel_stack import MRELStack

REF_DATE = date(2024, 12, 31)

def _make_instrument(category, maturity_date, amount=500_000_000):
    return Instrument(
        isin="IT0001234567",
        name="Test",
        category=category,
        issue_date=date(2024, 1, 1),
        maturity_date=maturity_date,
        coupon_type=CouponType.FIXED,
        outstanding_amount=amount,
        currency="EUR",
        crr2_rank=5,
    )

def test_senior_preferred_eligible():
    inst = _make_instrument(InstrumentCategory.SENIOR_PREFERRED, date(2027, 1, 1))
    result = assess_mrel_eligibility(inst, REF_DATE)
    assert result.eligible is True
    assert result.mrel_layer == "total"

def test_certificate_excluded():
    inst = _make_instrument(InstrumentCategory.CERTIFICATE, date(2027, 1, 1))
    result = assess_mrel_eligibility(inst, REF_DATE)
    assert result.eligible is False
    assert result.mrel_layer == "excluded"

def test_maturity_under_1yr_excluded():
    inst = _make_instrument(InstrumentCategory.SENIOR_PREFERRED, date(2025, 6, 1))
    result = assess_mrel_eligibility(inst, REF_DATE)
    assert result.eligible is False

def test_snp_subordination():
    inst = _make_instrument(InstrumentCategory.SENIOR_NON_PREFERRED, date(2028, 1, 1))
    result = assess_mrel_eligibility(inst, REF_DATE)
    assert result.eligible is True
    assert result.mrel_layer == "subordination"

def test_structured_note_protected_eligible():
    inst = _make_instrument(InstrumentCategory.STRUCTURED_NOTE_PROTECTED, date(2027, 1, 1))
    result = assess_mrel_eligibility(inst, REF_DATE)
    assert result.eligible is True
    assert result.mrel_layer == "total"

def test_mrel_stack_computation():
    instruments = [
        _make_instrument(InstrumentCategory.TIER2, date(2030, 1, 1), 200_000_000),
        _make_instrument(InstrumentCategory.SENIOR_NON_PREFERRED, date(2029, 1, 1), 300_000_000),
        _make_instrument(InstrumentCategory.SENIOR_PREFERRED, date(2028, 1, 1), 400_000_000),
        _make_instrument(InstrumentCategory.CERTIFICATE, date(2027, 1, 1), 100_000_000),
    ]
    stack = MRELStack.from_instruments(instruments, REF_DATE)
    assert stack.subordination_capacity == 500_000_000  # T2 + SNP
    assert stack.total_mrel_capacity == 900_000_000  # + Senior
    assert stack.excluded_certificates == 100_000_000
```

**Step 8: Run all tests**

```bash
python -m pytest tests/ -v
```
Expected: all PASS

**Step 9: Commit**

```bash
git add models/ tests/
git commit -m "feat: add data models — Instrument, eligibility, MREL stack"
```

---

## Task 3: Banco BPM IR Scraper (Prospectus Links)

**Files:**
- Create: `scrapers/banco_bpm.py`
- Create: `tests/test_scrapers.py`

**Step 1: Write the scraper to discover prospectus PDF links**

The Banco BPM IR site has these key pages:
- Domestic issuances: `https://gruppo.bancobpm.it/investor-relations/strumenti-di-debito/emissioni-domestiche/`
- International issuances (EMTN): `https://gruppo.bancobpm.it/investor-relations/strumenti-di-debito/emissioni-internazionali/`
- AT1: `https://gruppo.bancobpm.it/investor-relations/strumenti-finanziari/additional-tier-1/`

```python
# scrapers/banco_bpm.py
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

# Match ISIN patterns: IT or XS followed by 10 digits
ISIN_PATTERN = re.compile(r"\b(IT|XS)\d{10}\b")


@dataclass
class ProspectusLink:
    isin: str | None
    title: str
    pdf_url: str
    section: str  # domestic, international, at1
    doc_type: str  # final_terms, base_prospectus, supplement, other


async def fetch_page(client: httpx.AsyncClient, url: str) -> str:
    resp = await client.get(url, follow_redirects=True, timeout=30)
    resp.raise_for_status()
    return resp.text


def extract_pdf_links(html: str, section: str, base_url: str = BASE_URL) -> list[ProspectusLink]:
    """Extract all PDF links from an IR page, attempting to identify ISINs and doc types."""
    soup = BeautifulSoup(html, "lxml")
    links = []

    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"]
        if not href.lower().endswith(".pdf"):
            continue

        # Normalize URL
        if href.startswith("/"):
            href = base_url + href
        elif not href.startswith("http"):
            continue

        title = a_tag.get_text(strip=True)
        parent_text = ""
        parent = a_tag.find_parent(["div", "li", "td", "tr"])
        if parent:
            parent_text = parent.get_text(" ", strip=True)

        # Try to extract ISIN from title, href, or surrounding text
        combined_text = f"{title} {href} {parent_text}"
        isin_match = ISIN_PATTERN.search(combined_text)
        isin = isin_match.group(0) if isin_match else None

        # Classify document type
        text_lower = combined_text.lower()
        if any(kw in text_lower for kw in ["condizioni definitive", "final terms"]):
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
            isin=isin,
            title=title,
            pdf_url=href,
            section=section,
            doc_type=doc_type,
        ))

    return links


async def scrape_all_prospectus_links() -> list[ProspectusLink]:
    """Scrape all sections of the Banco BPM IR site for PDF links."""
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


async def download_pdf(
    client: httpx.AsyncClient,
    url: str,
    output_dir: Path,
    filename: str | None = None,
) -> Path:
    """Download a PDF to the output directory."""
    if filename is None:
        filename = url.split("/")[-1]
    output_path = output_dir / filename
    if output_path.exists():
        return output_path  # already cached

    resp = await client.get(url, follow_redirects=True, timeout=60)
    resp.raise_for_status()
    output_path.write_bytes(resp.content)
    return output_path


async def download_all_final_terms(
    links: list[ProspectusLink],
    output_dir: Path,
    max_concurrent: int = 5,
) -> list[tuple[ProspectusLink, Path]]:
    """Download all Final Terms PDFs, returning (link, local_path) pairs."""
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
```

**Step 2: Write a basic integration test**

```python
# tests/test_scrapers.py
import pytest
from scrapers.banco_bpm import extract_pdf_links, ISIN_PATTERN

SAMPLE_HTML = """
<html><body>
<div>
  <a href="/media/dlm_uploads/IT0005692246_Condizioni_Definitive.pdf">
    IT0005692246 - Condizioni Definitive e Nota di Sintesi
  </a>
  <a href="/media/dlm_uploads/Base_Prospectus_2025.pdf">
    Base Prospectus - 16 May 2025
  </a>
  <a href="/media/dlm_uploads/some_doc.docx">Not a PDF</a>
</div>
</body></html>
"""

def test_extract_pdf_links():
    links = extract_pdf_links(SAMPLE_HTML, "domestic")
    assert len(links) == 2  # only PDFs
    ft = [l for l in links if l.doc_type == "final_terms"]
    assert len(ft) == 1
    assert ft[0].isin == "IT0005692246"

def test_isin_pattern():
    assert ISIN_PATTERN.search("IT0005692246") is not None
    assert ISIN_PATTERN.search("XS1686880599") is not None
    assert ISIN_PATTERN.search("US1234567890") is None
    assert ISIN_PATTERN.search("NOTANISIN") is None
```

**Step 3: Run tests**

```bash
python -m pytest tests/test_scrapers.py -v
```
Expected: PASS

**Step 4: Commit**

```bash
git add scrapers/banco_bpm.py tests/test_scrapers.py
git commit -m "feat: add Banco BPM IR scraper for prospectus PDF links"
```

---

## Task 4: PDF Parser & Prospectus Clause Extractor

**Files:**
- Create: `parsers/pdf_parser.py`
- Create: `parsers/prospectus.py`
- Create: `tests/test_parsers.py`

**Step 1: Implement PDF text extraction**

```python
# parsers/pdf_parser.py
from __future__ import annotations
from pathlib import Path
import pdfplumber


def extract_text(pdf_path: Path | str) -> str:
    """Extract full text from a PDF file."""
    pdf_path = Path(pdf_path)
    text_parts = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            page_text = page.extract_text()
            if page_text:
                text_parts.append(page_text)
    return "\n".join(text_parts)


def extract_tables(pdf_path: Path | str) -> list[list[list[str | None]]]:
    """Extract all tables from a PDF, returning list of tables (each a list of rows)."""
    pdf_path = Path(pdf_path)
    tables = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            page_tables = page.extract_tables()
            if page_tables:
                tables.extend(page_tables)
    return tables
```

**Step 2: Implement prospectus clause extractor**

This is the core classification engine — it parses Final Terms text and extracts key fields to determine instrument type.

```python
# parsers/prospectus.py
from __future__ import annotations
import re
from dataclasses import dataclass


@dataclass
class ProspectusData:
    """Extracted data from a Final Terms / prospectus PDF."""
    isin: str | None = None
    instrument_name: str | None = None
    issue_date: str | None = None
    maturity_date: str | None = None
    nominal_amount: str | None = None
    coupon_type: str | None = None
    coupon_rate: str | None = None
    currency: str | None = None

    # Classification signals
    is_subordinated: bool = False
    is_senior_non_preferred: bool = False
    is_covered_bond: bool = False
    is_capital_protected: bool = False
    is_underlying_linked: bool = False
    has_barrier: bool = False
    has_autocallable: bool = False
    has_bail_in_clause: bool = False

    # Raw extracted clauses for audit
    subordination_clause: str | None = None
    bail_in_clause: str | None = None
    capital_protection_clause: str | None = None
    payoff_clause: str | None = None

    confidence: float = 1.0


ISIN_RE = re.compile(r"\b(IT|XS)\d{10}\b")

# Patterns for classification (Italian + English)
SUBORDINATION_PATTERNS = [
    r"subordinat[oaie]",
    r"tier\s*2",
    r"classe\s*subordinata",
    r"subordinated",
]

SNP_PATTERNS = [
    r"senior\s*non[- ]?prefer",
    r"art(?:icolo|\.)\s*12[- ]?c",
    r"crediti\s*di\s*secondo\s*livello",
    r"non[- ]?preferred\s*senior",
]

COVERED_BOND_PATTERNS = [
    r"obbligazioni?\s*bancari[ae]\s*garantit[ae]",
    r"covered\s*bond",
    r"obg",
]

CAPITAL_PROTECTION_PATTERNS = [
    r"protezione\s*del\s*capitale",
    r"capital\s*protect",
    r"rimborso\s*(?:minimo\s*)?(?:a\s*scadenza\s*)?(?:pari\s*al\s*)?100\s*%",
    r"valore\s*nominale\s*a\s*scadenza",
    r"rimborso\s*integrale\s*del\s*(?:valore\s*)?nominale",
    r"100%\s*del\s*valore\s*nominale\s*a\s*scadenza",
]

UNDERLYING_LINKED_PATTERNS = [
    r"sottostan(?:te|ti)",
    r"underlying",
    r"indice\s*di\s*riferimento",
    r"linked\s*to",
    r"basket",
    r"azioni?\s*sottostan",
]

BARRIER_PATTERNS = [
    r"barriera",
    r"barrier",
    r"knock[- ]?(?:in|out)",
    r"livello\s*(?:di\s*)?barriera",
]

AUTOCALLABLE_PATTERNS = [
    r"autocall",
    r"rimborso\s*anticipato\s*(?:automatico|condizionato)",
    r"early\s*redemption\s*(?:automatic|conditional)",
    r"callable\s*(?:su|on)\s*(?:base|basis)",
]

BAIL_IN_PATTERNS = [
    r"bail[- ]?in",
    r"risoluzione",
    r"resolution",
    r"brrd",
    r"art(?:icolo|\.)\s*44",
    r"svalutazione\s*e\s*(?:di\s*)?conversione",
    r"write[- ]?down",
]

DATE_RE = re.compile(
    r"(\d{1,2})[/.\- ](\d{1,2}|\w+)[/.\- ](\d{4})"
)

AMOUNT_RE = re.compile(
    r"(?:eur|€)\s*([\d.,]+(?:\.\d{3})*(?:,\d+)?)\b"
    r"|"
    r"([\d.,]+(?:\.\d{3})*(?:,\d+)?)\s*(?:eur|€|euro)",
    re.IGNORECASE,
)


def _search_patterns(text: str, patterns: list[str]) -> tuple[bool, str | None]:
    """Search text for any of the given patterns, return (found, matched_context)."""
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            start = max(0, match.start() - 100)
            end = min(len(text), match.end() + 100)
            context = text[start:end].strip()
            return True, context
    return False, None


def parse_prospectus(text: str) -> ProspectusData:
    """Parse a Final Terms / prospectus text and extract classification data."""
    data = ProspectusData()
    text_lower = text.lower()

    # Extract ISIN
    isin_match = ISIN_RE.search(text)
    if isin_match:
        data.isin = isin_match.group(0)

    # Extract currency
    if "eur" in text_lower or "€" in text_lower:
        data.currency = "EUR"

    # Classification signals
    data.is_subordinated, data.subordination_clause = _search_patterns(text, SUBORDINATION_PATTERNS)
    data.is_senior_non_preferred, _ = _search_patterns(text, SNP_PATTERNS)
    data.is_covered_bond, _ = _search_patterns(text, COVERED_BOND_PATTERNS)
    data.is_capital_protected, data.capital_protection_clause = _search_patterns(text, CAPITAL_PROTECTION_PATTERNS)
    data.is_underlying_linked, data.payoff_clause = _search_patterns(text, UNDERLYING_LINKED_PATTERNS)
    data.has_barrier, _ = _search_patterns(text, BARRIER_PATTERNS)
    data.has_autocallable, _ = _search_patterns(text, AUTOCALLABLE_PATTERNS)
    data.has_bail_in_clause, data.bail_in_clause = _search_patterns(text, BAIL_IN_PATTERNS)

    # Confidence scoring
    signals = [
        data.is_subordinated,
        data.is_senior_non_preferred,
        data.is_covered_bond,
        data.is_capital_protected,
        data.is_underlying_linked,
        data.has_barrier,
        data.has_autocallable,
    ]
    # If conflicting signals, lower confidence
    if data.is_capital_protected and data.has_barrier:
        data.confidence = 0.6  # unusual combo
    elif sum(signals) == 0:
        data.confidence = 0.5  # no clear signal
    else:
        data.confidence = 0.9

    return data
```

**Step 3: Write tests**

```python
# tests/test_parsers.py
from parsers.prospectus import parse_prospectus

def test_parse_senior_vanilla():
    text = """
    Codice ISIN: IT0005692246
    Obbligazioni a Tasso Fisso 4.50% 15/01/2027
    Valore Nominale: EUR 1.000
    Data di Emissione: 15/01/2024
    Data di Scadenza: 15/01/2027
    Tasso di Interesse: 4.50% annuo
    Rimborso a scadenza: 100% del Valore Nominale
    Le obbligazioni sono soggette allo strumento del bail-in ai sensi della BRRD.
    """
    data = parse_prospectus(text)
    assert data.isin == "IT0005692246"
    assert data.is_subordinated is False
    assert data.is_underlying_linked is False
    assert data.has_bail_in_clause is True
    assert data.is_capital_protected is True  # 100% rimborso

def test_parse_certificate():
    text = """
    Codice ISIN: IT0005695249
    Certificati Banco BPM Autocallable con Barriera
    Sottostante: Indice FTSE MIB
    Livello Barriera: 60% del valore iniziale
    Rimborso anticipato automatico condizionato
    """
    data = parse_prospectus(text)
    assert data.isin == "IT0005695249"
    assert data.has_autocallable is True
    assert data.has_barrier is True
    assert data.is_underlying_linked is True

def test_parse_snp():
    text = """
    ISIN: XS2034154190
    Senior Non-Preferred Notes
    Art. 12-c del Regolamento CRR
    Soggette a bail-in ai sensi della Direttiva BRRD
    Data di Scadenza: 15/06/2029
    """
    data = parse_prospectus(text)
    assert data.isin == "XS2034154190"
    assert data.is_senior_non_preferred is True
    assert data.has_bail_in_clause is True

def test_parse_tier2():
    text = """
    ISIN: IT0005572166
    Obbligazioni Subordinate Tier 2 a Tasso Fisso
    Classe subordinata
    Data di Scadenza: 20/03/2034
    Soggette a write-down e conversione ai sensi della BRRD
    """
    data = parse_prospectus(text)
    assert data.isin == "IT0005572166"
    assert data.is_subordinated is True
    assert data.has_bail_in_clause is True

def test_parse_structured_note_protected():
    text = """
    ISIN: IT0005697989
    Obbligazioni con Opzione Digitale legate all'indice Euribor 3M
    Sottostante: Euribor 3M
    Protezione del capitale a scadenza
    Rimborso minimo a scadenza pari al 100% del Valore Nominale
    """
    data = parse_prospectus(text)
    assert data.isin == "IT0005697989"
    assert data.is_underlying_linked is True
    assert data.is_capital_protected is True
    assert data.has_barrier is False
```

**Step 4: Run tests**

```bash
python -m pytest tests/test_parsers.py -v
```
Expected: PASS

**Step 5: Commit**

```bash
git add parsers/ tests/test_parsers.py
git commit -m "feat: add PDF parser and prospectus clause extractor with classification patterns"
```

---

## Task 5: Instrument Classifier

**Files:**
- Create: `parsers/classifier.py`
- Create: `tests/test_classifier.py`

**Step 1: Implement the classifier that maps ProspectusData → Instrument**

```python
# parsers/classifier.py
from __future__ import annotations
from datetime import date
from parsers.prospectus import ProspectusData
from models.instrument import Instrument, InstrumentCategory, CouponType


def classify_instrument(data: ProspectusData) -> InstrumentCategory:
    """Classify an instrument based on parsed prospectus data.

    Priority order (most specific first):
    1. Covered Bond
    2. Tier 2 (subordinated)
    3. Senior Non-Preferred (Art. 12c CRR2)
    4. Certificate (non-protected: underlying-linked + barrier/autocallable, no capital protection)
    5. Structured Note (capital-protected: underlying-linked + capital protection)
    6. Senior Preferred (plain vanilla — no underlying, no subordination)
    7. Unknown
    """
    if data.is_covered_bond:
        return InstrumentCategory.COVERED_BOND

    if data.is_subordinated and not data.is_senior_non_preferred:
        return InstrumentCategory.TIER2

    if data.is_senior_non_preferred:
        return InstrumentCategory.SENIOR_NON_PREFERRED

    # Rank 5 three-way split
    if data.is_underlying_linked:
        if data.has_barrier or data.has_autocallable:
            if data.is_capital_protected:
                # Conflicting: has barriers but also capital protection — flag as low confidence
                return InstrumentCategory.STRUCTURED_NOTE_PROTECTED
            return InstrumentCategory.CERTIFICATE
        if data.is_capital_protected:
            return InstrumentCategory.STRUCTURED_NOTE_PROTECTED
        # Underlying-linked but no barrier/autocall and no explicit capital protection
        # Conservative: classify as certificate (needs manual review)
        return InstrumentCategory.CERTIFICATE

    # No underlying link — plain vanilla senior
    if not data.is_subordinated:
        return InstrumentCategory.SENIOR_PREFERRED

    return InstrumentCategory.UNKNOWN


def _parse_date(date_str: str | None) -> date | None:
    """Try to parse a date string in common Italian/EU formats."""
    if not date_str:
        return None
    import re
    # dd/mm/yyyy or dd.mm.yyyy or dd-mm-yyyy
    m = re.match(r"(\d{1,2})[/.\-](\d{1,2})[/.\-](\d{4})", date_str.strip())
    if m:
        return date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
    return None


def _map_coupon_type(data: ProspectusData) -> CouponType:
    """Infer coupon type from prospectus text signals."""
    if data.coupon_type:
        ct = data.coupon_type.lower()
        if "fisso" in ct or "fixed" in ct:
            return CouponType.FIXED
        if "variabile" in ct or "float" in ct:
            return CouponType.FLOATING
        if "zero" in ct:
            return CouponType.ZERO_COUPON
        if "step up" in ct:
            return CouponType.STEP_UP
        if "step down" in ct:
            return CouponType.STEP_DOWN
    if data.is_underlying_linked:
        return CouponType.STRUCTURED
    return CouponType.UNKNOWN


def prospectus_to_instrument(data: ProspectusData) -> Instrument:
    """Convert parsed prospectus data to an Instrument model."""
    category = classify_instrument(data)

    # Map category to CRR2 rank
    rank_map = {
        InstrumentCategory.CET1: 1,
        InstrumentCategory.AT1: 2,
        InstrumentCategory.TIER2: 3,
        InstrumentCategory.SENIOR_NON_PREFERRED: 4,
        InstrumentCategory.SENIOR_PREFERRED: 5,
        InstrumentCategory.STRUCTURED_NOTE_PROTECTED: 5,
        InstrumentCategory.CERTIFICATE: 5,
        InstrumentCategory.COVERED_BOND: None,
    }

    return Instrument(
        isin=data.isin or "UNKNOWN",
        name=data.instrument_name or "Unknown Instrument",
        category=category,
        issue_date=_parse_date(data.issue_date),
        maturity_date=_parse_date(data.maturity_date),
        coupon_type=_map_coupon_type(data),
        outstanding_amount=None,  # filled later from Borsa Italiana
        currency=data.currency or "EUR",
        crr2_rank=rank_map.get(category),
        mrel_eligible=None,  # assessed later by eligibility engine
        bail_in_clause=data.has_bail_in_clause,
        capital_protected=data.is_capital_protected,
        underlying_linked=data.is_underlying_linked,
        classification_confidence=data.confidence,
        raw_prospectus_text=None,  # omit to save memory
    )
```

**Step 2: Write tests**

```python
# tests/test_classifier.py
from parsers.prospectus import ProspectusData
from parsers.classifier import classify_instrument, prospectus_to_instrument
from models.instrument import InstrumentCategory

def test_classify_senior_vanilla():
    data = ProspectusData(isin="IT0001", is_underlying_linked=False, is_subordinated=False)
    assert classify_instrument(data) == InstrumentCategory.SENIOR_PREFERRED

def test_classify_certificate():
    data = ProspectusData(
        isin="IT0002",
        is_underlying_linked=True,
        has_barrier=True,
        has_autocallable=True,
        is_capital_protected=False,
    )
    assert classify_instrument(data) == InstrumentCategory.CERTIFICATE

def test_classify_structured_note_protected():
    data = ProspectusData(
        isin="IT0003",
        is_underlying_linked=True,
        is_capital_protected=True,
        has_barrier=False,
    )
    assert classify_instrument(data) == InstrumentCategory.STRUCTURED_NOTE_PROTECTED

def test_classify_snp():
    data = ProspectusData(isin="XS0001", is_senior_non_preferred=True)
    assert classify_instrument(data) == InstrumentCategory.SENIOR_NON_PREFERRED

def test_classify_tier2():
    data = ProspectusData(isin="IT0004", is_subordinated=True)
    assert classify_instrument(data) == InstrumentCategory.TIER2

def test_classify_covered_bond():
    data = ProspectusData(isin="IT0005", is_covered_bond=True)
    assert classify_instrument(data) == InstrumentCategory.COVERED_BOND

def test_classify_barrier_with_protection():
    """Conflicting signals: barrier but also capital protected → structured note."""
    data = ProspectusData(
        isin="IT0006",
        is_underlying_linked=True,
        has_barrier=True,
        is_capital_protected=True,
    )
    assert classify_instrument(data) == InstrumentCategory.STRUCTURED_NOTE_PROTECTED

def test_prospectus_to_instrument():
    data = ProspectusData(
        isin="IT0005692246",
        instrument_name="Banco BPM 4.5% 2027",
        is_underlying_linked=False,
        is_subordinated=False,
        has_bail_in_clause=True,
        currency="EUR",
        confidence=0.9,
    )
    inst = prospectus_to_instrument(data)
    assert inst.isin == "IT0005692246"
    assert inst.category == InstrumentCategory.SENIOR_PREFERRED
    assert inst.crr2_rank == 5
    assert inst.bail_in_clause is True
```

**Step 3: Run tests**

```bash
python -m pytest tests/test_classifier.py -v
```
Expected: PASS

**Step 4: Commit**

```bash
git add parsers/classifier.py tests/test_classifier.py
git commit -m "feat: add instrument classifier — maps prospectus data to MREL categories"
```

---

## Task 6: Borsa Italiana Scraper

**Files:**
- Create: `scrapers/borsa_italiana.py`
- Create: `tests/test_borsa_italiana.py`

**Step 1: Implement the Borsa Italiana bond search scraper**

Borsa Italiana uses an AJAX endpoint at `/borsa/obbligazioni/advanced-search.html` with jQuery. We can replicate the query.

```python
# scrapers/borsa_italiana.py
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
    market: str  # MOT, ExtraMOT, EuroTLX
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
    """Search Borsa Italiana for bonds by issuer name."""
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

            # Safety limit
            if page > 50:
                print("Warning: hit 50-page limit on Borsa Italiana search")
                break

    return instruments


def _parse_search_results(html: str) -> list[BorsaItalianaInstrument]:
    """Parse the search results HTML table."""
    soup = BeautifulSoup(html, "lxml")
    instruments = []

    table = soup.find("table")
    if not table:
        return []

    rows = table.find_all("tr")
    for row in rows[1:]:  # skip header
        cells = row.find_all("td")
        if len(cells) < 3:
            continue

        # Extract ISIN from link
        link = row.find("a", href=True)
        name = cells[0].get_text(strip=True) if cells else ""
        isin = ""
        detail_url = None

        if link:
            href = link.get("href", "")
            detail_url = BASE_URL + href if href.startswith("/") else href
            # ISIN often in the link text or href
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
    """Try to extract an amount from table cells."""
    for cell in cells:
        text = cell.get_text(strip=True).replace(".", "").replace(",", ".")
        try:
            val = float(text)
            if val > 1000:  # likely an amount, not a percentage
                return val
        except ValueError:
            continue
    return None


async def get_instrument_detail(detail_url: str) -> dict:
    """Fetch detailed info for a single instrument from its Borsa Italiana page."""
    async with httpx.AsyncClient(
        headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"},
        follow_redirects=True,
        timeout=30,
    ) as client:
        resp = await client.get(detail_url)
        resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "lxml")
    details = {}

    # Parse key-value pairs from detail page
    for row in soup.find_all("tr"):
        cells = row.find_all(["th", "td"])
        if len(cells) == 2:
            key = cells[0].get_text(strip=True).lower()
            value = cells[1].get_text(strip=True)
            details[key] = value

    return details
```

**Step 2: Write tests**

```python
# tests/test_borsa_italiana.py
from scrapers.borsa_italiana import _parse_search_results

SAMPLE_HTML = """
<table>
<tr><th>Nome</th><th>Mercato</th><th>Importo</th></tr>
<tr>
  <td><a href="/borsa/obbligazioni/mot/scheda/IT0005692246.html">BANCO BPM 4.5% 2027</a></td>
  <td>MOT</td>
  <td>500.000.000</td>
</tr>
<tr>
  <td><a href="/borsa/obbligazioni/mot/scheda/XS2034154190.html">BANCO BPM SNP 2029</a></td>
  <td>MOT</td>
  <td>750.000.000</td>
</tr>
</table>
"""

def test_parse_search_results():
    results = _parse_search_results(SAMPLE_HTML)
    assert len(results) == 2
    assert results[0].isin == "IT0005692246"
    assert results[0].outstanding_amount == 500000000.0
    assert results[1].isin == "XS2034154190"
```

**Step 3: Run tests**

```bash
python -m pytest tests/test_borsa_italiana.py -v
```
Expected: PASS

**Step 4: Commit**

```bash
git add scrapers/borsa_italiana.py tests/test_borsa_italiana.py
git commit -m "feat: add Borsa Italiana bond search scraper"
```

---

## Task 7: Pillar 3 Parser

**Files:**
- Create: `scrapers/pillar3.py`
- Create: `tests/test_pillar3.py`

**Step 1: Implement Pillar 3 PDF and XLSX parser**

We have two known files:
- PDF: `https://gruppo.bancobpm.it/media/dlm_uploads/Pillar-3-Dicembre-2024_Documento.pdf` (5.44 MB)
- XLSX: `https://gruppo.bancobpm.it/media/dlm_uploads/EU_CCA_20241231.xlsx` (capital instruments)

```python
# scrapers/pillar3.py
from __future__ import annotations
import re
from dataclasses import dataclass
from pathlib import Path
import httpx
import pandas as pd
from parsers.pdf_parser import extract_text, extract_tables


PILLAR3_PDF_URL = "https://gruppo.bancobpm.it/media/dlm_uploads/Pillar-3-Dicembre-2024_Documento.pdf"
PILLAR3_XLSX_URL = "https://gruppo.bancobpm.it/media/dlm_uploads/EU_CCA_20241231.xlsx"


@dataclass
class Pillar3Aggregates:
    """Aggregate MREL data from Pillar 3 tables EU TLAC1 / TLAC1a."""
    cet1: float | None = None
    at1: float | None = None
    tier2: float | None = None
    senior_non_preferred: float | None = None
    senior_preferred: float | None = None
    total_own_funds: float | None = None
    total_eligible_liabilities: float | None = None
    total_mrel: float | None = None
    subordination_amount: float | None = None
    # Requirements
    mrel_trea: float | None = None  # % of TREA
    mrel_tem: float | None = None  # % of TEM (leverage)
    subordination_trea: float | None = None
    subordination_tem: float | None = None


async def download_pillar3_files(output_dir: Path) -> tuple[Path, Path]:
    """Download Pillar 3 PDF and XLSX to local cache."""
    output_dir.mkdir(parents=True, exist_ok=True)
    pdf_path = output_dir / "pillar3_dec2024.pdf"
    xlsx_path = output_dir / "EU_CCA_20241231.xlsx"

    async with httpx.AsyncClient(
        headers={"User-Agent": "MREL-Analysis/1.0 (academic research)"},
        follow_redirects=True,
        timeout=120,
    ) as client:
        if not pdf_path.exists():
            print("Downloading Pillar 3 PDF (5.4 MB)...")
            resp = await client.get(PILLAR3_PDF_URL)
            resp.raise_for_status()
            pdf_path.write_bytes(resp.content)

        if not xlsx_path.exists():
            print("Downloading EU CCA XLSX...")
            resp = await client.get(PILLAR3_XLSX_URL)
            resp.raise_for_status()
            xlsx_path.write_bytes(resp.content)

    return pdf_path, xlsx_path


def parse_capital_instruments_xlsx(xlsx_path: Path) -> pd.DataFrame:
    """Parse the EU CCA (Capital Instruments) XLSX file."""
    df = pd.read_excel(xlsx_path)
    return df


def parse_pillar3_mrel_tables(pdf_path: Path) -> Pillar3Aggregates:
    """Extract MREL aggregate data from Pillar 3 PDF.

    Looks for EU TLAC1, EU TLAC1a, and TLAC3 tables.
    This is a best-effort extraction — table formats may vary.
    """
    text = extract_text(pdf_path)
    tables = extract_tables(pdf_path)
    aggregates = Pillar3Aggregates()

    # Search for TLAC/MREL related numbers in the text
    # Pattern: look for sections containing "TLAC" or "MREL"
    tlac_sections = []
    lines = text.split("\n")
    in_tlac = False
    section_lines = []

    for line in lines:
        if re.search(r"(?:EU\s*)?TLAC|MREL", line, re.IGNORECASE):
            in_tlac = True
            section_lines = [line]
        elif in_tlac:
            section_lines.append(line)
            if len(section_lines) > 50:
                tlac_sections.append("\n".join(section_lines))
                in_tlac = False

    if section_lines and in_tlac:
        tlac_sections.append("\n".join(section_lines))

    # Also try to extract from tables
    for table in tables:
        if not table:
            continue
        table_text = str(table).lower()
        if "tlac" in table_text or "mrel" in table_text:
            _extract_values_from_table(table, aggregates)

    return aggregates


def _extract_values_from_table(table: list[list], aggregates: Pillar3Aggregates) -> None:
    """Try to extract MREL values from a parsed PDF table."""
    for row in table:
        if not row or len(row) < 2:
            continue
        label = str(row[0]).lower() if row[0] else ""
        value_str = str(row[-1]).strip() if row[-1] else ""

        # Try to parse numeric value
        value = _parse_number(value_str)
        if value is None:
            continue

        if "cet1" in label or "cet 1" in label:
            aggregates.cet1 = value
        elif "additional tier 1" in label or "at1" in label:
            aggregates.at1 = value
        elif "tier 2" in label:
            aggregates.tier2 = value
        elif "fondi propri" in label or "own funds" in label:
            aggregates.total_own_funds = value
        elif "passività ammissibili" in label or "eligible liabilities" in label:
            aggregates.total_eligible_liabilities = value


def _parse_number(s: str) -> float | None:
    """Parse a number string that may use Italian formatting (. for thousands, , for decimal)."""
    s = s.strip().replace(" ", "")
    if not s or s == "-" or s == "n.a." or s == "n.d.":
        return None
    # Remove thousands separators, convert decimal comma
    s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None
```

**Step 2: Write tests**

```python
# tests/test_pillar3.py
from scrapers.pillar3 import _parse_number, Pillar3Aggregates, _extract_values_from_table

def test_parse_number_italian():
    assert _parse_number("1.234.567,89") == 1234567.89
    assert _parse_number("500.000") == 500000.0
    assert _parse_number("-") is None
    assert _parse_number("n.a.") is None

def test_extract_values_from_table():
    table = [
        ["CET1 capital", "10.500"],
        ["Additional Tier 1", "1.200"],
        ["Tier 2", "2.300"],
        ["Fondi Propri", "14.000"],
    ]
    agg = Pillar3Aggregates()
    _extract_values_from_table(table, agg)
    assert agg.cet1 == 10500.0
    assert agg.at1 == 1200.0
    assert agg.tier2 == 2300.0
    assert agg.total_own_funds == 14000.0
```

**Step 3: Run tests**

```bash
python -m pytest tests/test_pillar3.py -v
```
Expected: PASS

**Step 4: Commit**

```bash
git add scrapers/pillar3.py tests/test_pillar3.py
git commit -m "feat: add Pillar 3 PDF/XLSX parser for MREL aggregates"
```

---

## Task 8: Data Pipeline Orchestrator

**Files:**
- Create: `pipeline.py`

**Step 1: Implement the main pipeline that ties scrapers + parsers + models together**

```python
# pipeline.py
"""MREL Analysis Pipeline — orchestrates scraping, parsing, classification, and storage."""
from __future__ import annotations
import asyncio
import json
import sqlite3
from datetime import date
from pathlib import Path

import pandas as pd

from scrapers.banco_bpm import (
    scrape_all_prospectus_links,
    download_all_final_terms,
    ProspectusLink,
)
from scrapers.borsa_italiana import search_bonds_by_issuer
from scrapers.pillar3 import download_pillar3_files, parse_pillar3_mrel_tables, parse_capital_instruments_xlsx
from parsers.pdf_parser import extract_text
from parsers.prospectus import parse_prospectus
from parsers.classifier import prospectus_to_instrument, classify_instrument
from models.instrument import Instrument, InstrumentCategory
from models.eligibility import assess_mrel_eligibility
from models.mrel_stack import MRELStack

DATA_DIR = Path("data")
RAW_DIR = DATA_DIR / "raw"
DB_DIR = DATA_DIR / "db"
PROCESSED_DIR = DATA_DIR / "processed"

REF_DATE = date(2024, 12, 31)


def init_db(db_path: Path) -> sqlite3.Connection:
    """Initialize SQLite database with instrument table."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.execute("""
        CREATE TABLE IF NOT EXISTS instruments (
            isin TEXT PRIMARY KEY,
            name TEXT,
            category TEXT,
            issue_date TEXT,
            maturity_date TEXT,
            coupon_type TEXT,
            coupon_rate REAL,
            outstanding_amount REAL,
            currency TEXT,
            crr2_rank INTEGER,
            listing_venue TEXT,
            mrel_eligible INTEGER,
            mrel_layer TEXT,
            eligibility_reason TEXT,
            classification_confidence REAL,
            bail_in_clause INTEGER,
            capital_protected INTEGER,
            underlying_linked INTEGER,
            prospectus_url TEXT,
            source_pdf TEXT
        )
    """)
    conn.commit()
    return conn


def save_instrument(conn: sqlite3.Connection, inst: Instrument, mrel_layer: str = "") -> None:
    """Upsert an instrument into the database."""
    conn.execute("""
        INSERT OR REPLACE INTO instruments VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
    """, (
        inst.isin,
        inst.name,
        inst.category.value,
        str(inst.issue_date) if inst.issue_date else None,
        str(inst.maturity_date) if inst.maturity_date else None,
        inst.coupon_type.value,
        inst.coupon_rate,
        inst.outstanding_amount,
        inst.currency,
        inst.crr2_rank,
        inst.listing_venue,
        1 if inst.mrel_eligible else 0 if inst.mrel_eligible is not None else None,
        mrel_layer,
        inst.eligibility_reason,
        inst.classification_confidence,
        1 if inst.bail_in_clause else 0 if inst.bail_in_clause is not None else None,
        1 if inst.capital_protected else 0 if inst.capital_protected is not None else None,
        1 if inst.underlying_linked else 0 if inst.underlying_linked is not None else None,
        inst.prospectus_url,
        inst.source_pdf,
    ))
    conn.commit()


def load_instruments(conn: sqlite3.Connection) -> list[Instrument]:
    """Load all instruments from the database."""
    cursor = conn.execute("SELECT * FROM instruments")
    columns = [desc[0] for desc in cursor.description]
    instruments = []
    for row in cursor.fetchall():
        data = dict(zip(columns, row))
        inst = Instrument(
            isin=data["isin"],
            name=data["name"] or "",
            category=InstrumentCategory(data["category"]) if data["category"] else InstrumentCategory.UNKNOWN,
            issue_date=date.fromisoformat(data["issue_date"]) if data["issue_date"] else None,
            maturity_date=date.fromisoformat(data["maturity_date"]) if data["maturity_date"] else None,
            coupon_type=__import__("models.instrument", fromlist=["CouponType"]).CouponType(data["coupon_type"]) if data["coupon_type"] else __import__("models.instrument", fromlist=["CouponType"]).CouponType.UNKNOWN,
            coupon_rate=data["coupon_rate"],
            outstanding_amount=data["outstanding_amount"],
            currency=data["currency"] or "EUR",
            crr2_rank=data["crr2_rank"],
            listing_venue=data["listing_venue"],
            mrel_eligible=bool(data["mrel_eligible"]) if data["mrel_eligible"] is not None else None,
            eligibility_reason=data["eligibility_reason"],
            classification_confidence=data["classification_confidence"] or 1.0,
            bail_in_clause=bool(data["bail_in_clause"]) if data["bail_in_clause"] is not None else None,
            capital_protected=bool(data["capital_protected"]) if data["capital_protected"] is not None else None,
            underlying_linked=bool(data["underlying_linked"]) if data["underlying_linked"] is not None else None,
            prospectus_url=data["prospectus_url"],
            source_pdf=data["source_pdf"],
        )
        instruments.append(inst)
    return instruments


async def run_pipeline() -> None:
    """Run the full MREL analysis pipeline."""
    print("=" * 60)
    print("MREL ANALYSIS PIPELINE — Banco BPM — Ref Date: 31.12.2024")
    print("=" * 60)

    # Step 1: Scrape prospectus links
    print("\n[1/6] Scraping Banco BPM IR for prospectus links...")
    links = await scrape_all_prospectus_links()
    print(f"  Found {len(links)} total PDF links")
    final_terms = [l for l in links if l.doc_type == "final_terms"]
    print(f"  Of which {len(final_terms)} are Final Terms")

    # Step 2: Download Final Terms PDFs
    print("\n[2/6] Downloading Final Terms PDFs...")
    downloaded = await download_all_final_terms(links, RAW_DIR / "final_terms")
    print(f"  Downloaded {len(downloaded)} Final Terms PDFs")

    # Step 3: Parse and classify
    print("\n[3/6] Parsing prospectuses and classifying instruments...")
    db_path = DB_DIR / "mrel.db"
    conn = init_db(db_path)

    classified_count = 0
    for link, pdf_path in downloaded:
        try:
            text = extract_text(pdf_path)
            if not text.strip():
                continue
            prospectus_data = parse_prospectus(text)
            if not prospectus_data.isin:
                # Try to use ISIN from the link
                if link.isin:
                    prospectus_data.isin = link.isin
                else:
                    continue

            inst = prospectus_to_instrument(prospectus_data)
            inst.prospectus_url = link.pdf_url
            inst.source_pdf = str(pdf_path)

            # Assess MREL eligibility
            result = assess_mrel_eligibility(inst, REF_DATE)
            inst.mrel_eligible = result.eligible
            inst.eligibility_reason = result.reason

            save_instrument(conn, inst, result.mrel_layer)
            classified_count += 1
        except Exception as e:
            print(f"  Error processing {pdf_path.name}: {e}")

    print(f"  Classified {classified_count} instruments")

    # Step 4: Enrich with Borsa Italiana data
    print("\n[4/6] Fetching Borsa Italiana data for outstanding amounts...")
    try:
        borsa_instruments = await search_bonds_by_issuer("BANCO BPM")
        print(f"  Found {len(borsa_instruments)} instruments on Borsa Italiana")

        # Match by ISIN and update outstanding amounts
        enriched = 0
        for bi in borsa_instruments:
            if bi.isin and bi.outstanding_amount:
                conn.execute(
                    "UPDATE instruments SET outstanding_amount = ?, listing_venue = ? WHERE isin = ?",
                    (bi.outstanding_amount, bi.market, bi.isin),
                )
                enriched += 1
        conn.commit()
        print(f"  Enriched {enriched} instruments with outstanding amounts")
    except Exception as e:
        print(f"  Warning: Borsa Italiana scraping failed: {e}")

    # Step 5: Download and parse Pillar 3
    print("\n[5/6] Downloading and parsing Pillar 3 data...")
    try:
        pdf_path, xlsx_path = await download_pillar3_files(RAW_DIR / "pillar3")
        aggregates = parse_pillar3_mrel_tables(pdf_path)
        capital_df = parse_capital_instruments_xlsx(xlsx_path)
        print(f"  Pillar 3 aggregates extracted")
        print(f"  Capital instruments XLSX: {len(capital_df)} rows")

        # Save aggregates as JSON
        agg_path = PROCESSED_DIR / "pillar3_aggregates.json"
        agg_path.parent.mkdir(parents=True, exist_ok=True)
        import dataclasses
        with open(agg_path, "w") as f:
            json.dump(dataclasses.asdict(aggregates), f, indent=2)
    except Exception as e:
        print(f"  Warning: Pillar 3 processing failed: {e}")

    # Step 6: Compute MREL stack and export
    print("\n[6/6] Computing MREL stack...")
    instruments = load_instruments(conn)
    stack = MRELStack.from_instruments(instruments, REF_DATE)

    print("\n" + "=" * 60)
    print("MREL STACK SUMMARY")
    print("=" * 60)
    for key, val in stack.to_dict().items():
        if val:
            print(f"  {key}: EUR {val:,.0f}")

    # Export to Excel
    export_path = PROCESSED_DIR / "mrel_instruments.xlsx"
    export_path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame([{
        "ISIN": i.isin,
        "Name": i.name,
        "Category": i.category.value,
        "Issue Date": i.issue_date,
        "Maturity Date": i.maturity_date,
        "Coupon Type": i.coupon_type.value,
        "Outstanding (EUR)": i.outstanding_amount,
        "CRR2 Rank": i.crr2_rank,
        "MREL Eligible": i.mrel_eligible,
        "Eligibility Reason": i.eligibility_reason,
        "Confidence": i.classification_confidence,
    } for i in instruments])
    df.to_excel(export_path, index=False)
    print(f"\nExported {len(instruments)} instruments to {export_path}")

    conn.close()
    print("\nPipeline complete.")


if __name__ == "__main__":
    asyncio.run(run_pipeline())
```

**Step 2: Run a quick sanity check (no live scraping yet)**

```bash
python -c "from pipeline import init_db; from pathlib import Path; init_db(Path('data/db/mrel.db')); print('DB init OK')"
```
Expected: `DB init OK`

**Step 3: Commit**

```bash
git add pipeline.py
git commit -m "feat: add data pipeline orchestrator — ties scrapers, parsers, and models together"
```

---

## Task 9: Streamlit Dashboard

**Files:**
- Create: `dashboard/app.py`
- Create: `dashboard/views/explorer.py`
- Create: `dashboard/views/waterfall.py`
- Create: `dashboard/views/reconciliation.py`
- Create: `dashboard/views/audit.py`
- Create: `dashboard/components/charts.py`

**Step 1: Create reusable chart components**

```python
# dashboard/components/charts.py
from __future__ import annotations
import plotly.graph_objects as go
import pandas as pd
from models.mrel_stack import MRELStack


def waterfall_chart(stack: MRELStack) -> go.Figure:
    """Create a waterfall chart showing MREL stack composition."""
    categories = [
        "CET1", "AT1", "Tier 2", "Senior Non-Preferred",
        "Senior Preferred", "Structured Notes (Protected)",
        "Total MREL",
    ]
    values = [
        stack.cet1, stack.at1, stack.tier2,
        stack.senior_non_preferred, stack.senior_preferred,
        stack.structured_notes_protected,
        stack.total_mrel_capacity,
    ]
    measures = ["relative"] * 6 + ["total"]

    fig = go.Figure(go.Waterfall(
        name="MREL Stack",
        orientation="v",
        measure=measures,
        x=categories,
        y=values,
        textposition="outside",
        text=[f"EUR {v:,.0f}M" if v else "" for v in [v / 1e6 if v else 0 for v in values]],
        connector={"line": {"color": "rgb(63, 63, 63)"}},
        increasing={"marker": {"color": "#2E86AB"}},
        totals={"marker": {"color": "#1B4965"}},
    ))

    fig.update_layout(
        title="MREL Capacity Waterfall — Banco BPM (31.12.2024)",
        yaxis_title="EUR",
        showlegend=False,
        height=500,
    )

    return fig


def category_pie_chart(df: pd.DataFrame) -> go.Figure:
    """Create a pie chart of instrument categories by outstanding amount."""
    cat_amounts = df.groupby("Category")["Outstanding (EUR)"].sum().reset_index()
    cat_amounts = cat_amounts[cat_amounts["Outstanding (EUR)"] > 0]

    fig = go.Figure(go.Pie(
        labels=cat_amounts["Category"],
        values=cat_amounts["Outstanding (EUR)"],
        hole=0.4,
        textinfo="label+percent",
    ))

    fig.update_layout(
        title="Instrument Mix by Outstanding Amount",
        height=400,
    )
    return fig
```

**Step 2: Create the Instrument Explorer view**

```python
# dashboard/views/explorer.py
from __future__ import annotations
import streamlit as st
import pandas as pd


def render(df: pd.DataFrame) -> None:
    """Render the Instrument Explorer view."""
    st.header("Instrument Explorer")

    # Filters
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        categories = ["All"] + sorted(df["Category"].unique().tolist())
        selected_cat = st.selectbox("Category", categories)

    with col2:
        eligible_options = ["All", "Eligible", "Excluded"]
        selected_elig = st.selectbox("MREL Eligible", eligible_options)

    with col3:
        venues = ["All"] + sorted(df["Listing Venue"].dropna().unique().tolist())
        selected_venue = st.selectbox("Listing Venue", venues)

    with col4:
        confidence_min = st.slider("Min Confidence", 0.0, 1.0, 0.0, 0.1)

    # Apply filters
    filtered = df.copy()
    if selected_cat != "All":
        filtered = filtered[filtered["Category"] == selected_cat]
    if selected_elig == "Eligible":
        filtered = filtered[filtered["MREL Eligible"] == True]
    elif selected_elig == "Excluded":
        filtered = filtered[filtered["MREL Eligible"] == False]
    if selected_venue != "All":
        filtered = filtered[filtered["Listing Venue"] == selected_venue]
    filtered = filtered[filtered["Confidence"] >= confidence_min]

    # Summary metrics
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Instruments", len(filtered))
    col2.metric("MREL Eligible", len(filtered[filtered["MREL Eligible"] == True]))
    total_outstanding = filtered["Outstanding (EUR)"].sum()
    col3.metric("Total Outstanding", f"EUR {total_outstanding:,.0f}")
    eligible_outstanding = filtered[filtered["MREL Eligible"] == True]["Outstanding (EUR)"].sum()
    col4.metric("Eligible Outstanding", f"EUR {eligible_outstanding:,.0f}")

    # Display table
    st.dataframe(
        filtered,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Outstanding (EUR)": st.column_config.NumberColumn(format="EUR %.0f"),
            "Confidence": st.column_config.ProgressColumn(min_value=0, max_value=1),
            "MREL Eligible": st.column_config.CheckboxColumn(),
        },
    )

    # Export
    if st.button("Export to Excel"):
        filtered.to_excel("data/processed/filtered_export.xlsx", index=False)
        st.success("Exported to data/processed/filtered_export.xlsx")
```

**Step 3: Create the Waterfall view**

```python
# dashboard/views/waterfall.py
from __future__ import annotations
import streamlit as st
import pandas as pd
from models.mrel_stack import MRELStack
from models.instrument import Instrument, InstrumentCategory, CouponType
from datetime import date
from dashboard.components.charts import waterfall_chart, category_pie_chart


def _df_to_instruments(df: pd.DataFrame) -> list[Instrument]:
    """Convert dashboard DataFrame back to Instrument objects."""
    instruments = []
    for _, row in df.iterrows():
        try:
            cat = InstrumentCategory(row["Category"])
        except ValueError:
            cat = InstrumentCategory.UNKNOWN
        try:
            ct = CouponType(row.get("Coupon Type", "Unknown"))
        except ValueError:
            ct = CouponType.UNKNOWN

        mat_date = None
        if pd.notna(row.get("Maturity Date")):
            try:
                mat_date = pd.to_datetime(row["Maturity Date"]).date()
            except Exception:
                pass

        issue_date = None
        if pd.notna(row.get("Issue Date")):
            try:
                issue_date = pd.to_datetime(row["Issue Date"]).date()
            except Exception:
                pass

        instruments.append(Instrument(
            isin=row["ISIN"],
            name=row.get("Name", ""),
            category=cat,
            issue_date=issue_date,
            maturity_date=mat_date,
            coupon_type=ct,
            outstanding_amount=row.get("Outstanding (EUR)"),
            currency="EUR",
            crr2_rank=row.get("CRR2 Rank"),
            mrel_eligible=row.get("MREL Eligible"),
        ))
    return instruments


def render(df: pd.DataFrame, ref_date: date) -> None:
    """Render the MREL Stack Waterfall view."""
    st.header("MREL Stack Waterfall")

    instruments = _df_to_instruments(df)
    stack = MRELStack.from_instruments(instruments, ref_date)

    # Key metrics
    col1, col2, col3 = st.columns(3)
    col1.metric("Subordination Capacity", f"EUR {stack.subordination_capacity:,.0f}")
    col2.metric("Total MREL Capacity", f"EUR {stack.total_mrel_capacity:,.0f}")
    col3.metric("Total Excluded", f"EUR {stack.total_excluded:,.0f}")

    # Waterfall chart
    fig = waterfall_chart(stack)
    st.plotly_chart(fig, use_container_width=True)

    # Pie chart
    fig2 = category_pie_chart(df)
    st.plotly_chart(fig2, use_container_width=True)

    # Detailed breakdown table
    st.subheader("Detailed Breakdown")
    breakdown = pd.DataFrame([
        {"Component": k, "Amount (EUR)": v}
        for k, v in stack.to_dict().items()
    ])
    st.dataframe(breakdown, use_container_width=True, hide_index=True)
```

**Step 4: Create the Reconciliation view**

```python
# dashboard/views/reconciliation.py
from __future__ import annotations
import json
import streamlit as st
import pandas as pd
from pathlib import Path


def render(df: pd.DataFrame) -> None:
    """Render the Reconciliation view — bottom-up vs Pillar 3."""
    st.header("Reconciliation: Bottom-Up vs Pillar 3")

    # Load Pillar 3 aggregates if available
    agg_path = Path("data/processed/pillar3_aggregates.json")
    if not agg_path.exists():
        st.warning("Pillar 3 aggregates not yet available. Run the pipeline first.")
        return

    with open(agg_path) as f:
        p3 = json.load(f)

    # Bottom-up totals
    eligible = df[df["MREL Eligible"] == True]
    bu_by_cat = eligible.groupby("Category")["Outstanding (EUR)"].sum()

    # Build comparison table
    categories = ["CET1", "AT1", "Tier 2", "Senior Non-Preferred", "Senior Preferred",
                  "Structured Note (Capital Protected)"]
    p3_keys = ["cet1", "at1", "tier2", "senior_non_preferred", "senior_preferred", None]

    rows = []
    for cat, p3_key in zip(categories, p3_keys):
        bu_val = bu_by_cat.get(cat, 0)
        p3_val = p3.get(p3_key) if p3_key else None
        delta = (bu_val - p3_val) if p3_val is not None else None

        rows.append({
            "Category": cat,
            "Bottom-Up (EUR)": bu_val,
            "Pillar 3 (EUR)": p3_val,
            "Delta (EUR)": delta,
            "Delta %": f"{delta / p3_val * 100:.1f}%" if p3_val and delta is not None else "N/A",
        })

    recon_df = pd.DataFrame(rows)
    st.dataframe(recon_df, use_container_width=True, hide_index=True)

    # Highlight discrepancies
    if any(r["Delta (EUR)"] is not None and abs(r["Delta (EUR)"]) > 1_000_000 for r in rows):
        st.warning("Significant discrepancies detected (> EUR 1M). Check the Audit view for details.")
```

**Step 5: Create the Audit view**

```python
# dashboard/views/audit.py
from __future__ import annotations
import streamlit as st
import pandas as pd


def render(df: pd.DataFrame) -> None:
    """Render the Data Quality & Audit view."""
    st.header("Data Quality & Audit")

    # Low confidence instruments
    st.subheader("Low Confidence Classifications")
    low_conf = df[df["Confidence"] < 0.8].sort_values("Confidence")
    if len(low_conf) > 0:
        st.warning(f"{len(low_conf)} instruments with confidence < 80%")
        st.dataframe(low_conf, use_container_width=True, hide_index=True)
    else:
        st.success("All instruments classified with high confidence")

    # Missing outstanding amounts
    st.subheader("Missing Outstanding Amounts")
    missing_amt = df[df["Outstanding (EUR)"].isna() | (df["Outstanding (EUR)"] == 0)]
    if len(missing_amt) > 0:
        st.warning(f"{len(missing_amt)} instruments without outstanding amount data")
        st.dataframe(missing_amt[["ISIN", "Name", "Category"]], use_container_width=True, hide_index=True)
    else:
        st.success("All instruments have outstanding amount data")

    # Category distribution
    st.subheader("Category Distribution")
    cat_counts = df["Category"].value_counts().reset_index()
    cat_counts.columns = ["Category", "Count"]
    st.dataframe(cat_counts, use_container_width=True, hide_index=True)

    # Unknown/unclassified
    st.subheader("Unclassified Instruments")
    unknown = df[df["Category"] == "Unknown"]
    if len(unknown) > 0:
        st.error(f"{len(unknown)} instruments could not be classified")
        st.dataframe(unknown, use_container_width=True, hide_index=True)
    else:
        st.success("All instruments classified")

    # Export full dataset
    st.subheader("Export")
    if st.button("Export Full Dataset to Excel"):
        df.to_excel("data/processed/full_audit_export.xlsx", index=False)
        st.success("Exported to data/processed/full_audit_export.xlsx")
```

**Step 6: Create the main Streamlit app**

```python
# dashboard/app.py
"""MREL Analysis Dashboard — Banco BPM"""
from __future__ import annotations
import sqlite3
from datetime import date
from pathlib import Path

import streamlit as st
import pandas as pd

from dashboard.views import explorer, waterfall, reconciliation, audit

st.set_page_config(
    page_title="MREL Analysis — Banco BPM",
    page_icon="🏦",
    layout="wide",
)

DB_PATH = Path("data/db/mrel.db")


@st.cache_data(ttl=300)
def load_data() -> pd.DataFrame:
    if not DB_PATH.exists():
        return pd.DataFrame()
    conn = sqlite3.connect(str(DB_PATH))
    df = pd.read_sql("SELECT * FROM instruments", conn)
    conn.close()

    # Rename columns for display
    column_map = {
        "isin": "ISIN",
        "name": "Name",
        "category": "Category",
        "issue_date": "Issue Date",
        "maturity_date": "Maturity Date",
        "coupon_type": "Coupon Type",
        "coupon_rate": "Coupon Rate",
        "outstanding_amount": "Outstanding (EUR)",
        "currency": "Currency",
        "crr2_rank": "CRR2 Rank",
        "listing_venue": "Listing Venue",
        "mrel_eligible": "MREL Eligible",
        "mrel_layer": "MREL Layer",
        "eligibility_reason": "Eligibility Reason",
        "classification_confidence": "Confidence",
        "bail_in_clause": "Bail-in Clause",
        "capital_protected": "Capital Protected",
        "underlying_linked": "Underlying Linked",
    }
    df = df.rename(columns=column_map)

    # Convert boolean columns
    for col in ["MREL Eligible", "Bail-in Clause", "Capital Protected", "Underlying Linked"]:
        if col in df.columns:
            df[col] = df[col].map({1: True, 0: False, None: None})

    return df


def main():
    st.title("MREL Analysis — Banco BPM")
    st.caption("Reference Date: 31.12.2024 | Prospectus-First Classification per CRR2/BRRD2/SRB")

    # Sidebar
    st.sidebar.title("Navigation")
    ref_date = st.sidebar.date_input("Reference Date", date(2024, 12, 31))
    view = st.sidebar.radio(
        "View",
        ["Instrument Explorer", "MREL Stack Waterfall", "Reconciliation", "Data Quality & Audit"],
    )

    if st.sidebar.button("Refresh Data"):
        st.cache_data.clear()

    # Load data
    df = load_data()

    if df.empty:
        st.warning(
            "No data available. Run the pipeline first:\n\n"
            "```bash\npython pipeline.py\n```"
        )
        return

    # Render selected view
    if view == "Instrument Explorer":
        explorer.render(df)
    elif view == "MREL Stack Waterfall":
        waterfall.render(df, ref_date)
    elif view == "Reconciliation":
        reconciliation.render(df)
    elif view == "Data Quality & Audit":
        audit.render(df)


if __name__ == "__main__":
    main()
```

**Step 7: Run Streamlit locally to verify it loads**

```bash
cd ~/Desktop/mrel-analysis
source venv/bin/activate
streamlit run dashboard/app.py --server.headless true
```
Expected: app starts on port 8501, shows "No data available" message (since pipeline hasn't run yet)

**Step 8: Commit**

```bash
git add dashboard/
git commit -m "feat: add Streamlit dashboard with 4 views — explorer, waterfall, reconciliation, audit"
```

---

## Task 10: End-to-End Integration Test

**Files:**
- Create: `tests/test_integration.py`

**Step 1: Write an integration test with sample data**

```python
# tests/test_integration.py
"""End-to-end test with synthetic data — no network calls."""
import sqlite3
from datetime import date
from pathlib import Path
from models.instrument import Instrument, InstrumentCategory, CouponType
from models.eligibility import assess_mrel_eligibility
from models.mrel_stack import MRELStack
from pipeline import init_db, save_instrument, load_instruments

REF_DATE = date(2024, 12, 31)

SAMPLE_INSTRUMENTS = [
    Instrument(
        isin="IT0005000001", name="BBPM Senior 4.5% 2027",
        category=InstrumentCategory.SENIOR_PREFERRED,
        issue_date=date(2024, 1, 15), maturity_date=date(2027, 1, 15),
        coupon_type=CouponType.FIXED, outstanding_amount=500_000_000,
        currency="EUR", crr2_rank=5,
    ),
    Instrument(
        isin="XS0050000002", name="BBPM SNP 3.75% 2029",
        category=InstrumentCategory.SENIOR_NON_PREFERRED,
        issue_date=date(2023, 6, 1), maturity_date=date(2029, 6, 1),
        coupon_type=CouponType.FIXED, outstanding_amount=750_000_000,
        currency="EUR", crr2_rank=4,
    ),
    Instrument(
        isin="IT0005000003", name="BBPM Tier 2 5% 2034",
        category=InstrumentCategory.TIER2,
        issue_date=date(2024, 3, 1), maturity_date=date(2034, 3, 1),
        coupon_type=CouponType.FIXED, outstanding_amount=300_000_000,
        currency="EUR", crr2_rank=3,
    ),
    Instrument(
        isin="IT0005000004", name="BBPM Cert Autocall FTSE MIB",
        category=InstrumentCategory.CERTIFICATE,
        issue_date=date(2024, 2, 1), maturity_date=date(2027, 2, 1),
        coupon_type=CouponType.STRUCTURED, outstanding_amount=50_000_000,
        currency="EUR", crr2_rank=5,
    ),
    Instrument(
        isin="IT0005000005", name="BBPM Structured Capital Protected",
        category=InstrumentCategory.STRUCTURED_NOTE_PROTECTED,
        issue_date=date(2024, 4, 1), maturity_date=date(2028, 4, 1),
        coupon_type=CouponType.STRUCTURED, outstanding_amount=200_000_000,
        currency="EUR", crr2_rank=5,
    ),
    Instrument(
        isin="IT0005000006", name="BBPM Senior Expiring Soon",
        category=InstrumentCategory.SENIOR_PREFERRED,
        issue_date=date(2023, 1, 1), maturity_date=date(2025, 6, 1),
        coupon_type=CouponType.FIXED, outstanding_amount=100_000_000,
        currency="EUR", crr2_rank=5,
    ),
]


def test_full_pipeline_with_synthetic_data(tmp_path):
    db_path = tmp_path / "test.db"
    conn = init_db(db_path)

    # Save instruments and assess eligibility
    for inst in SAMPLE_INSTRUMENTS:
        result = assess_mrel_eligibility(inst, REF_DATE)
        inst.mrel_eligible = result.eligible
        inst.eligibility_reason = result.reason
        save_instrument(conn, inst, result.mrel_layer)

    # Load back
    loaded = load_instruments(conn)
    assert len(loaded) == 6

    # Compute stack
    stack = MRELStack.from_instruments(loaded, REF_DATE)

    # Subordination: T2 (300M) + SNP (750M) = 1.05B
    assert stack.subordination_capacity == 1_050_000_000

    # Total MREL: sub (1.05B) + Senior 2027 (500M) + Structured Protected (200M) = 1.75B
    # Senior expiring 2025-06 has residual < 1yr → excluded
    assert stack.total_mrel_capacity == 1_750_000_000

    # Excluded: certificate (50M) + maturity < 1yr senior (100M) = 150M
    assert stack.excluded_certificates == 50_000_000
    assert stack.excluded_maturity == 100_000_000

    conn.close()
```

**Step 2: Run integration test**

```bash
python -m pytest tests/test_integration.py -v
```
Expected: PASS

**Step 3: Run all tests**

```bash
python -m pytest tests/ -v
```
Expected: all PASS

**Step 4: Commit**

```bash
git add tests/test_integration.py
git commit -m "feat: add end-to-end integration test with synthetic MREL data"
```

---

## Task 11: Run Live Pipeline & Verify

**Step 1: Run the live pipeline**

```bash
cd ~/Desktop/mrel-analysis
source venv/bin/activate
python pipeline.py
```

Expected: pipeline scrapes Banco BPM IR, downloads PDFs, classifies instruments, fetches Borsa Italiana data, downloads Pillar 3, and outputs MREL stack summary.

**Step 2: Verify output files exist**

```bash
ls -la data/db/mrel.db
ls -la data/processed/mrel_instruments.xlsx
ls -la data/processed/pillar3_aggregates.json
ls -la data/raw/final_terms/ | head -20
ls -la data/raw/pillar3/
```

**Step 3: Launch dashboard and verify all views**

```bash
streamlit run dashboard/app.py
```

Check each view:
- Instrument Explorer: instruments load with filters working
- MREL Stack Waterfall: chart renders with correct hierarchy
- Reconciliation: bottom-up vs Pillar 3 comparison visible
- Data Quality: any low-confidence or missing-data items flagged

**Step 4: Commit any fixes needed after live testing**

```bash
git add -A
git commit -m "fix: adjustments after live pipeline run"
```

---

## Summary

| Task | Description | Dependencies |
|------|-------------|-------------|
| 1 | Project scaffolding & dependencies | None |
| 2 | Data models (Instrument, Eligibility, MRELStack) | Task 1 |
| 3 | Banco BPM IR scraper | Task 1 |
| 4 | PDF parser & prospectus clause extractor | Task 1 |
| 5 | Instrument classifier | Tasks 2, 4 |
| 6 | Borsa Italiana scraper | Task 1 |
| 7 | Pillar 3 parser | Task 1 |
| 8 | Data pipeline orchestrator | Tasks 2-7 |
| 9 | Streamlit dashboard (4 views) | Tasks 2, 8 |
| 10 | End-to-end integration test | Tasks 2, 5, 8 |
| 11 | Live pipeline run & verification | All |

Tasks 3, 4, 6, 7 can be built in parallel. Tasks 2 and 5 are sequential. Task 8 depends on all data tasks. Task 9 can start once Task 2 is done (uses mock data initially).
