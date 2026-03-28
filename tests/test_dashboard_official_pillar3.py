from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from dashboard.official_pillar3 import (
    WORKBOOK_PATH,
    build_official_waterfall,
    build_tlac3_rank_table,
    get_bank_logo_url,
    get_bank_monogram,
    get_normalized_requirement_profile,
    get_template_coverage,
    list_bank_dates,
    list_official_banks,
    load_official_pillar3_long,
)


pytestmark = pytest.mark.skipif(not Path(WORKBOOK_PATH).exists(), reason="Official workbook not available")


def test_load_official_pillar3_long_normalizes_export():
    df = load_official_pillar3_long(str(WORKBOOK_PATH))

    assert {"entity_name", "template", "row", "column", "reference_date", "fact_value"}.issubset(df.columns)
    assert df["entity_name"].nunique() == 82
    assert sorted(df["reference_date"].unique().tolist()) == ["2025-06-30", "2025-09-30", "2025-12-31"]
    assert len(list_official_banks(str(WORKBOOK_PATH))) == 82


def test_banco_bpm_dates_are_filtered_per_bank():
    dates = list_bank_dates("BANCO BPM SOCIETA' PER AZIONI", str(WORKBOOK_PATH))
    assert dates == ["2025-06-30", "2025-12-31"]


def test_template_coverage_matches_known_examples():
    assert get_template_coverage("BANCO BPM SOCIETA' PER AZIONI", "2025-12-31", str(WORKBOOK_PATH)) == {
        "KM2": True,
        "TLAC1": True,
        "TLAC3": True,
    }
    assert get_template_coverage("BANCO BPM SOCIETA' PER AZIONI", "2025-06-30", str(WORKBOOK_PATH)) == {
        "KM2": True,
        "TLAC1": False,
        "TLAC3": False,
    }
    assert get_template_coverage("DNB BANK ASA", "2025-12-31", str(WORKBOOK_PATH)) == {
        "KM2": True,
        "TLAC1": True,
        "TLAC3": False,
    }


def test_official_waterfall_requires_tlac1_and_matches_banco_bpm_total():
    waterfall = build_official_waterfall("BANCO BPM SOCIETA' PER AZIONI", "2025-12-31", str(WORKBOOK_PATH))
    assert waterfall is not None
    assert waterfall.total_mrel == pytest.approx(22_205_622_311.0)
    assert waterfall.requirement_lines

    no_tlac1 = build_official_waterfall("BANCO BPM SOCIETA' PER AZIONI", "2025-06-30", str(WORKBOOK_PATH))
    assert no_tlac1 is None


def test_tlac3_rank_table_is_available_only_when_template_exists():
    tlac3 = build_tlac3_rank_table("BANCO BPM SOCIETA' PER AZIONI", "2025-12-31", str(WORKBOOK_PATH))
    assert not tlac3.empty
    assert tlac3.iloc[-1]["Rank"] == "Total"
    assert tlac3.iloc[2]["Potentially Eligible for MREL/TLAC"] != pytest.approx(3_250_000_000.0)
    assert pd.isna(tlac3.iloc[2]["Potentially Eligible for MREL/TLAC"])
    assert tlac3.iloc[3]["Potentially Eligible for MREL/TLAC"] == pytest.approx(3_250_000_000.0)

    missing_tlac3 = build_tlac3_rank_table("DNB BANK ASA", "2025-12-31", str(WORKBOOK_PATH))
    assert missing_tlac3.empty


def test_bank_logo_helpers_cover_known_and_unknown_banks():
    assert get_bank_logo_url("BANCO BPM SOCIETA' PER AZIONI") is not None
    assert get_bank_logo_url("Unknown Bank Example") is None
    assert get_bank_monogram("BANCO BPM SOCIETA' PER AZIONI") == "BB"


def test_requirement_normalization_handles_scale_and_cbr():
    sabadell = get_normalized_requirement_profile("Banco de Sabadell, S.A.", "2025-06-30", str(WORKBOOK_PATH))
    assert sabadell.actual_mrel_trea == pytest.approx(0.28415071)
    assert sabadell.requirement_mrel_trea == pytest.approx(0.2531)
    assert sabadell.cbr_trea is None
    assert sabadell.binding_mrel_trea == pytest.approx(0.2531)

    santander = get_normalized_requirement_profile("Banco Santander, S.A.", "2025-12-31", str(WORKBOOK_PATH))
    assert santander.cbr_disclosed is True
    assert santander.cbr_trea == pytest.approx(0.0442)
    assert santander.requirement_mrel_trea == pytest.approx(0.3192)
    assert santander.binding_mrel_trea == pytest.approx(0.3634)

    banco_bpm = get_normalized_requirement_profile("BANCO BPM SOCIETA' PER AZIONI", "2025-12-31", str(WORKBOOK_PATH))
    assert banco_bpm.cbr_disclosed is False
    assert banco_bpm.requirement_mrel_trea == pytest.approx(0.2260)
