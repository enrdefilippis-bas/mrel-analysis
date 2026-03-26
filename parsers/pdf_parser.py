from __future__ import annotations
from pathlib import Path
import pdfplumber


def extract_text(pdf_path: Path | str) -> str:
    pdf_path = Path(pdf_path)
    text_parts = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            page_text = page.extract_text()
            if page_text:
                text_parts.append(page_text)
    return "\n".join(text_parts)


def extract_tables(pdf_path: Path | str) -> list[list[list[str | None]]]:
    pdf_path = Path(pdf_path)
    tables = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            page_tables = page.extract_tables()
            if page_tables:
                tables.extend(page_tables)
    return tables
