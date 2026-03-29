from __future__ import annotations

from datetime import date

import pandas as pd

from dashboard.instrument_intelligence import (
    CURRENT_OUTSTANDING_COLUMN,
    RECONCILIATION_REFERENCE_DATE,
    build_active_today_view,
    build_reference_snapshot,
    snapshot_column_name,
)


def _sample_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "ISIN": "XS2530053789",
                "Name": "BAMIIM 6 09/13/26 BOND",
                "Category": "Senior Non-Preferred",
                "Issue Date": "2022-09-13",
                "Maturity Date": "2026-09-13",
                "Outstanding (EUR)": 283_576_000.0,
                "Original Amount (EUR)": 500_000_000.0,
                "MREL Eligible": True,
                "MREL Layer": "Senior Non-Preferred",
                "Eligibility Reason": "SNP",
                "Confidence": 1.0,
            },
            {
                "ISIN": "IT0005640203",
                "Name": "Unknown Instrument",
                "Category": "Senior Non-Preferred",
                "Issue Date": "2024-06-04",
                "Maturity Date": None,
                "Outstanding (EUR)": 75_000_000.0,
                "Original Amount (EUR)": 75_000_000.0,
                "MREL Eligible": True,
                "MREL Layer": "subordination",
                "Eligibility Reason": "wrong category",
                "Confidence": 0.8,
            },
            {
                "ISIN": "IT0005632267",
                "Name": "BANCO BPM 25/30 MTN",
                "Category": "Senior Preferred",
                "Issue Date": None,
                "Maturity Date": "2030-01-21",
                "Outstanding (EUR)": 500_000_000.0,
                "Original Amount (EUR)": 500_000_000.0,
                "MREL Eligible": True,
                "MREL Layer": "total",
                "Eligibility Reason": "missing issue date",
                "Confidence": 1.0,
            },
            {
                "ISIN": "IT0005675126",
                "Name": "BAMI 3.125 23/10/31",
                "Category": "Senior Preferred",
                "Issue Date": None,
                "Maturity Date": "2031-10-23",
                "Outstanding (EUR)": 500_000_000.0,
                "Original Amount (EUR)": 500_000_000.0,
                "MREL Eligible": True,
                "MREL Layer": "total",
                "Eligibility Reason": "missing issue date",
                "Confidence": 1.0,
            },
            {
                "ISIN": "IT0005695918",
                "Name": "Future Instrument",
                "Category": "Certificate (Non-Protected)",
                "Issue Date": "2026-04-01",
                "Maturity Date": "2030-04-01",
                "Outstanding (EUR)": 0.0,
                "Original Amount (EUR)": 10_000_000.0,
                "MREL Eligible": False,
                "MREL Layer": "",
                "Eligibility Reason": "",
                "Confidence": 1.0,
            },
            {
                "ISIN": "IT0005218380",
                "Name": "Banco BPM S.p.A. — Common Equity",
                "Category": "CET1",
                "Issue Date": None,
                "Maturity Date": None,
                "Outstanding (EUR)": 7_033_288_000.0,
                "Original Amount (EUR)": 7_033_288_000.0,
                "MREL Eligible": True,
                "MREL Layer": "",
                "Eligibility Reason": "",
                "Confidence": 1.0,
            },
        ]
    )


def test_build_active_today_view_filters_to_instruments_in_being_today() -> None:
    active = build_active_today_view(_sample_df(), today=date(2026, 3, 29))

    assert "IT0005695918" not in active["ISIN"].tolist()
    assert set(active["ISIN"]) == {
        "XS2530053789",
        "IT0005640203",
        "IT0005632267",
        "IT0005675126",
        "IT0005218380",
    }
    assert CURRENT_OUTSTANDING_COLUMN in active.columns
    assert snapshot_column_name("2025-03-31") in active.columns
    assert snapshot_column_name("2025-06-30") in active.columns
    assert snapshot_column_name("2025-09-30") in active.columns


def test_build_active_today_view_applies_known_banco_bpm_category_corrections() -> None:
    active = build_active_today_view(_sample_df(), today=date(2026, 3, 29))
    corrected = active[active["ISIN"] == "IT0005640203"].iloc[0]

    assert corrected["Category"] == "Senior Preferred"
    assert corrected["Issue Date"] == "2025-03-13"
    assert corrected["MREL Layer"] == "non_subordination"
    assert corrected[snapshot_column_name("2025-03-31")] == 75_000_000.0

    green_snp = active[active["ISIN"] == "IT0005675126"].iloc[0]
    assert green_snp["Category"] == "Senior Non-Preferred"
    assert green_snp["Issue Date"] == "2025-10-23"
    assert green_snp["MREL Layer"] == "Senior Non-Preferred"


def test_build_reference_snapshot_uses_historical_amounts_and_excludes_future_issues() -> None:
    snapshot = build_reference_snapshot(
        _sample_df(),
        RECONCILIATION_REFERENCE_DATE,
        today=date(2026, 3, 29),
    )

    assert set(snapshot["ISIN"]) == {"XS2530053789", "IT0005218380"}
    xs253 = snapshot[snapshot["ISIN"] == "XS2530053789"].iloc[0]
    assert xs253["Outstanding (EUR)"] == 500_000_000.0
