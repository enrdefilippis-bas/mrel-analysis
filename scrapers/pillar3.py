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
    cet1: float | None = None
    at1: float | None = None
    tier2: float | None = None
    senior_non_preferred: float | None = None
    senior_preferred: float | None = None
    total_own_funds: float | None = None
    total_eligible_liabilities: float | None = None
    total_mrel: float | None = None
    subordination_amount: float | None = None
    mrel_trea: float | None = None
    mrel_tem: float | None = None
    subordination_trea: float | None = None
    subordination_tem: float | None = None


async def download_pillar3_files(output_dir: Path) -> tuple[Path, Path]:
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
    df = pd.read_excel(xlsx_path)
    return df


def parse_pillar3_mrel_tables(pdf_path: Path) -> Pillar3Aggregates:
    text = extract_text(pdf_path)
    tables = extract_tables(pdf_path)
    aggregates = Pillar3Aggregates()

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

    for table in tables:
        if not table:
            continue
        table_text = str(table).lower()
        if "tlac" in table_text or "mrel" in table_text:
            _extract_values_from_table(table, aggregates)

    return aggregates


def _extract_values_from_table(table: list[list], aggregates: Pillar3Aggregates) -> None:
    for row in table:
        if not row or len(row) < 2:
            continue
        label = str(row[0]).lower() if row[0] else ""
        value_str = str(row[-1]).strip() if row[-1] else ""

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
    s = s.strip().replace(" ", "")
    if not s or s == "-" or s == "n.a." or s == "n.d.":
        return None
    s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None
