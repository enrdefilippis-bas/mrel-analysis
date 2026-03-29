from __future__ import annotations

from datetime import date

import pandas as pd

RECONCILIATION_REFERENCE_DATE = "2024-12-31"
SNAPSHOT_DATES = ("2025-03-31", "2025-06-30", "2025-09-30")
CURRENT_OUTSTANDING_COLUMN = "Outstanding Current (EUR)"
ORIGINAL_AMOUNT_COLUMN = "Original Amount (EUR)"

KNOWN_INSTRUMENT_OVERRIDES = {
    "IT0005640203": {
        "Name": "BANCO BPM 3.92% Senior Preferred Notes due 13 March 2031",
        "Category": "Senior Preferred",
        "Issue Date": "2025-03-13",
        "Maturity Date": "2031-03-13",
        "MREL Layer": "non_subordination",
        "Eligibility Reason": "Senior Preferred: counts towards total MREL, not subordination.",
    },
    "IT0005641540": {
        "Name": "BANCO BPM 3.74% Senior Preferred Notes due 27 March 2031",
        "Category": "Senior Preferred",
        "Issue Date": "2025-03-27",
        "Maturity Date": "2031-03-27",
        "MREL Layer": "non_subordination",
        "Eligibility Reason": "Senior Preferred: counts towards total MREL, not subordination.",
    },
    "IT0005632267": {
        "Name": "BANCO BPM 3.375% Senior Preferred Notes due 21 January 2030",
        "Category": "Senior Preferred",
        "Issue Date": "2025-01-21",
        "MREL Layer": "total",
        "Eligibility Reason": "Senior Preferred: counts towards total MREL only.",
    },
    "IT0005675126": {
        "Name": "BANCO BPM 3.125% Green Senior Non-Preferred Notes due 23 October 2031",
        "Category": "Senior Non-Preferred",
        "Issue Date": "2025-10-23",
        "MREL Layer": "Senior Non-Preferred",
        "Eligibility Reason": "Senior Non-Preferred: counts towards subordination and total MREL.",
    },
}


def snapshot_column_name(reference_date: str) -> str:
    return f"Outstanding {pd.Timestamp(reference_date).strftime('%d-%m-%Y')} (EUR)"


def _parse_date(value: object) -> date | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return None
    return pd.Timestamp(text).date()


def _active_on_date(row: pd.Series, as_of: date) -> bool:
    issue_date = _parse_date(row.get("Issue Date"))
    maturity_date = _parse_date(row.get("Maturity Date"))

    if issue_date and issue_date > as_of:
        return False
    if maturity_date and maturity_date < as_of:
        return False
    return True


def _snapshot_amount(row: pd.Series, as_of: date, today: date) -> float | None:
    if not _active_on_date(row, as_of):
        return None

    current_amount = row.get(CURRENT_OUTSTANDING_COLUMN)
    original_amount = row.get(ORIGINAL_AMOUNT_COLUMN)

    if as_of >= today:
        return float(current_amount) if pd.notna(current_amount) else None

    if pd.notna(original_amount) and float(original_amount) > 0:
        return float(original_amount)
    if pd.notna(current_amount) and float(current_amount) > 0:
        return float(current_amount)
    return None


def normalize_instrument_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    normalized = df.copy()

    if CURRENT_OUTSTANDING_COLUMN not in normalized.columns and "Outstanding (EUR)" in normalized.columns:
        normalized[CURRENT_OUTSTANDING_COLUMN] = normalized["Outstanding (EUR)"]

    for isin, overrides in KNOWN_INSTRUMENT_OVERRIDES.items():
        mask = normalized["ISIN"] == isin
        for column, value in overrides.items():
            normalized.loc[mask, column] = value

    return normalized


def build_active_today_view(df: pd.DataFrame, today: date | None = None) -> pd.DataFrame:
    today = today or date.today()
    active = normalize_instrument_dataframe(df)

    for reference_date in SNAPSHOT_DATES:
        active[snapshot_column_name(reference_date)] = active.apply(
            lambda row: _snapshot_amount(row, pd.Timestamp(reference_date).date(), today),
            axis=1,
        )

    issue_dates = pd.to_datetime(active["Issue Date"], errors="coerce")
    maturity_dates = pd.to_datetime(active["Maturity Date"], errors="coerce")
    current_amounts = pd.to_numeric(active[CURRENT_OUTSTANDING_COLUMN], errors="coerce")

    active = active[
        ((issue_dates.isna()) | (issue_dates.dt.date <= today))
        & ((maturity_dates.isna()) | (maturity_dates.dt.date > today))
        & (current_amounts.fillna(0) > 0)
    ].copy()

    active["Outstanding (EUR)"] = active[CURRENT_OUTSTANDING_COLUMN]
    return active.sort_values(["Category", "Issue Date", "ISIN"], na_position="last").reset_index(drop=True)


def build_reference_snapshot(
    df: pd.DataFrame,
    reference_date: str = RECONCILIATION_REFERENCE_DATE,
    today: date | None = None,
) -> pd.DataFrame:
    today = today or date.today()
    as_of = pd.Timestamp(reference_date).date()
    snapshot = normalize_instrument_dataframe(df)
    snapshot["Outstanding (EUR)"] = snapshot.apply(lambda row: _snapshot_amount(row, as_of, today), axis=1)

    snapshot = snapshot[
        snapshot.apply(lambda row: _active_on_date(row, as_of), axis=1)
        & pd.to_numeric(snapshot["Outstanding (EUR)"], errors="coerce").fillna(0).gt(0)
    ].copy()

    return snapshot.sort_values(["Category", "Issue Date", "ISIN"], na_position="last").reset_index(drop=True)
