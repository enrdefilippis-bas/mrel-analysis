from __future__ import annotations

import csv
import json
from pathlib import Path

from scrapers.cbr_dataset import (
    build_dataset_rows,
    select_best_match,
    treatment_from_classification,
    write_dataset_files,
)


def test_treatment_from_classification_maps_expected_values() -> None:
    assert treatment_from_classification("explicit_on_top") == "on_top"
    assert treatment_from_classification("explicit_included") == "included"
    assert treatment_from_classification("mentioned_unclear") == "unclear"
    assert treatment_from_classification("no_match") == "not_found"


def test_select_best_match_prefers_non_inclusive_on_top_wording() -> None:
    matches = [
        {
            "page": 19,
            "keyword": "requisito combinato",
            "snippet": "Il requisito MREL-TREA va aggiunto il requisito combinato di riserva di capitale applicabile.",
        },
        {
            "page": 21,
            "keyword": "requisito combinato",
            "snippet": "I requisiti MREL-TREA sono riportati al netto del requisito combinato di riserva di capitale.",
        },
    ]

    best = select_best_match(matches, "on_top")

    assert best is not None
    assert "al netto del requisito combinato" in best["snippet"]


def test_build_dataset_rows_and_write_outputs(tmp_path: Path) -> None:
    json_dir = tmp_path / "json"
    json_dir.mkdir()
    sample = {
        "bank_name": "BANCO BPM SOCIETA' PER AZIONI",
        "reference_date": "2025-06-30",
        "source_url": "https://example.com/bpm.pdf",
        "source_type": "direct_pdf",
        "note": None,
        "status": "ok",
        "pdf_path": "cbr/raw/bpm.pdf",
        "text_path": "cbr/text/bpm.txt",
        "match_count": 2,
        "classification": "explicit_on_top",
        "matches": [
            {
                "page": 19,
                "keyword": "requisito combinato",
                "snippet": "Il requisito MREL-TREA va aggiunto il requisito combinato di riserva di capitale applicabile.",
            },
            {
                "page": 21,
                "keyword": "requisito combinato",
                "snippet": "I requisiti MREL-TREA sono riportati al netto del requisito combinato di riserva di capitale.",
            },
        ],
    }
    (json_dir / "bpm.json").write_text(json.dumps(sample))

    rows = build_dataset_rows(json_dir)
    csv_path, json_path = write_dataset_files(rows, tmp_path)

    assert len(rows) == 1
    assert rows[0].cbr_treatment == "on_top"
    assert rows[0].evidence_page == 21
    assert "al netto del requisito combinato" in (rows[0].evidence_quote or "")

    with csv_path.open() as handle:
        csv_rows = list(csv.DictReader(handle))
    assert csv_rows[0]["cbr_treatment"] == "on_top"

    data = json.loads(json_path.read_text())
    assert data[0]["bank_name"] == "BANCO BPM SOCIETA' PER AZIONI"
