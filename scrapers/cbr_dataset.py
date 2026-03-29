from __future__ import annotations

import argparse
import csv
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any


CLASSIFICATION_TO_TREATMENT = {
    "explicit_on_top": "on_top",
    "explicit_included": "included",
    "mentioned_unclear": "unclear",
    "no_match": "not_found",
    "source_not_found": "source_not_found",
}

TREATMENT_PRIORITY = {
    "on_top": 0,
    "included": 1,
    "unclear": 2,
    "not_found": 3,
    "source_not_found": 4,
}

SNIPPET_PRIORITY_PATTERNS = (
    re.compile(r"(va aggiunt\w+|in aggiunta|a cui sommare|da sommare|maggiorat\w+)", re.IGNORECASE),
    re.compile(r"(non inclusiv\w+|al netto del requisito combinato)", re.IGNORECASE),
    re.compile(r"(incl\.?\s*cbr|comprensiv\w+|inclusiv\w+)", re.IGNORECASE),
    re.compile(r"(riserva combinata|combined buffer requirement|\bcbr\b)", re.IGNORECASE),
)


@dataclass(frozen=True)
class CBRDatasetRow:
    bank_name: str
    reference_date: str
    scrape_status: str
    cbr_treatment: str
    source_url: str | None
    source_type: str
    evidence_page: int | None
    evidence_keyword: str | None
    evidence_quote: str | None
    match_count: int
    pdf_path: str | None
    text_path: str | None
    note: str | None


def treatment_from_classification(classification: str) -> str:
    return CLASSIFICATION_TO_TREATMENT.get(classification, "unknown")


def score_snippet(snippet: dict[str, Any], treatment: str) -> tuple[int, int]:
    text = str(snippet.get("snippet", ""))
    base_score = 0
    for index, pattern in enumerate(SNIPPET_PRIORITY_PATTERNS):
        if pattern.search(text):
            base_score = max(base_score, len(SNIPPET_PRIORITY_PATTERNS) - index)

    if treatment == "on_top" and re.search(r"(non inclusiv\w+|al netto)", text, re.IGNORECASE):
        base_score += 2
    if treatment == "included" and re.search(r"(incl\.?\s*cbr|comprensiv\w+|inclusiv\w+)", text, re.IGNORECASE):
        base_score += 2

    page = int(snippet.get("page", 0) or 0)
    return (base_score, -page)


def select_best_match(matches: list[dict[str, Any]], treatment: str) -> dict[str, Any] | None:
    if not matches:
        return None
    return max(matches, key=lambda item: score_snippet(item, treatment))


def load_result_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def normalize_result(result: dict[str, Any]) -> CBRDatasetRow:
    classification = str(result.get("classification", ""))
    treatment = treatment_from_classification(classification)
    matches = list(result.get("matches", []))
    best_match = select_best_match(matches, treatment)

    return CBRDatasetRow(
        bank_name=str(result.get("bank_name", "")),
        reference_date=str(result.get("reference_date", "")),
        scrape_status=str(result.get("status", "")),
        cbr_treatment=treatment,
        source_url=result.get("source_url"),
        source_type=str(result.get("source_type", "")),
        evidence_page=(int(best_match["page"]) if best_match and best_match.get("page") is not None else None),
        evidence_keyword=(str(best_match.get("keyword", "")) if best_match else None),
        evidence_quote=(str(best_match.get("snippet", "")).strip() if best_match else None),
        match_count=int(result.get("match_count", 0) or 0),
        pdf_path=result.get("pdf_path"),
        text_path=result.get("text_path"),
        note=result.get("note"),
    )


def build_dataset_rows(json_dir: Path) -> list[CBRDatasetRow]:
    rows = [
        normalize_result(load_result_json(path))
        for path in sorted(json_dir.glob("*.json"))
        if path.name != ".gitkeep"
    ]
    return sorted(
        rows,
        key=lambda row: (
            row.reference_date,
            TREATMENT_PRIORITY.get(row.cbr_treatment, 99),
            row.bank_name.casefold(),
        ),
    )


def rows_to_dicts(rows: list[CBRDatasetRow]) -> list[dict[str, Any]]:
    return [
        {
            "bank_name": row.bank_name,
            "reference_date": row.reference_date,
            "scrape_status": row.scrape_status,
            "cbr_treatment": row.cbr_treatment,
            "source_url": row.source_url,
            "source_type": row.source_type,
            "evidence_page": row.evidence_page,
            "evidence_keyword": row.evidence_keyword,
            "evidence_quote": row.evidence_quote,
            "match_count": row.match_count,
            "pdf_path": row.pdf_path,
            "text_path": row.text_path,
            "note": row.note,
        }
        for row in rows
    ]


def write_dataset_files(rows: list[CBRDatasetRow], output_root: Path) -> tuple[Path, Path]:
    csv_path = output_root / "dataset.csv"
    json_path = output_root / "dataset.json"
    data = rows_to_dicts(rows)

    with csv_path.open("w", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "bank_name",
                "reference_date",
                "scrape_status",
                "cbr_treatment",
                "source_url",
                "source_type",
                "evidence_page",
                "evidence_keyword",
                "evidence_quote",
                "match_count",
                "pdf_path",
                "text_path",
                "note",
            ],
        )
        writer.writeheader()
        writer.writerows(data)

    json_path.write_text(json.dumps(data, indent=2, ensure_ascii=False))
    return csv_path, json_path


def build_cbr_dataset(output_root: str = "cbr") -> list[CBRDatasetRow]:
    root = Path(output_root)
    json_dir = root / "json"
    rows = build_dataset_rows(json_dir)
    write_dataset_files(rows, root)
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Build a normalized CBR treatment dataset from scraped JSON outputs.")
    parser.add_argument("--output-dir", default="cbr")
    args = parser.parse_args()

    rows = build_cbr_dataset(output_root=args.output_dir)
    print(f"Built dataset with {len(rows)} rows in {args.output_dir}")


if __name__ == "__main__":
    main()
