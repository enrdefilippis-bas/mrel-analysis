from __future__ import annotations

import pandas as pd

from dashboard.views.reconciliation import _apply_historical_outstanding_overrides


def test_apply_historical_outstanding_overrides_adjusts_banco_bpm_snp_for_2024_year_end() -> None:
    df = pd.DataFrame(
        [
            {
                "ISIN": "XS2530053789",
                "Category": "Senior Non-Preferred",
                "Outstanding (EUR)": 283_576_000.0,
                "MREL Eligible": True,
            },
            {
                "ISIN": "XS2558591967",
                "Category": "Senior Non-Preferred",
                "Outstanding (EUR)": 500_000_000.0,
                "MREL Eligible": True,
            },
        ]
    )

    adjusted = _apply_historical_outstanding_overrides(df)

    xs253 = adjusted[adjusted["ISIN"] == "XS2530053789"].iloc[0]
    xs255 = adjusted[adjusted["ISIN"] == "XS2558591967"].iloc[0]

    assert xs253["Outstanding (EUR)"] == 375_000_000.0
    assert xs255["Outstanding (EUR)"] == 500_000_000.0
