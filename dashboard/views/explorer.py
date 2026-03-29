from __future__ import annotations
import json
import streamlit as st
import pandas as pd
from pathlib import Path

from dashboard.instrument_intelligence import CURRENT_OUTSTANDING_COLUMN, SNAPSHOT_DATES, snapshot_column_name


def _load_pillar3() -> dict | None:
    agg_path = Path(__file__).resolve().parent.parent.parent / "data" / "processed" / "pillar3_aggregates.json"
    if not agg_path.exists():
        return None
    with open(agg_path) as f:
        return json.load(f)


def render(df: pd.DataFrame) -> None:
    st.header("Instrument Explorer")
    current_amount_col = CURRENT_OUTSTANDING_COLUMN if CURRENT_OUTSTANDING_COLUMN in df.columns else "Outstanding (EUR)"

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

    # Pillar 3 reference for category comparison
    p3 = _load_pillar3()
    p3_ref = None
    p3_label = "Pillar 3"
    p3_note = None
    if p3 and selected_cat in ("CET1", "AT1", "Tier 2", "Senior Non-Preferred"):
        cc1 = p3.get("own_funds_cc1", {})
        tlac1 = p3.get("mrel_tlac1_composition", {})
        unit = 1000 if p3.get("amounts_unit") == "thousands_eur" else 1
        if selected_cat == "CET1":
            p3_ref = cc1.get("cet1", 0) * unit
            p3_label = "P3 Own Funds (CC1)"
        elif selected_cat == "AT1":
            p3_ref = cc1.get("at1", 0) * unit
            p3_label = "P3 Own Funds (CC1)"
        elif selected_cat == "Tier 2":
            # Per-ISIN CCA amounts = t2_before_adjustments; t2 after = own funds after Art.66 deductions
            p3_ref = cc1.get("t2_before_adjustments", 0) * unit
            p3_label = "P3 CCA Sum (pre-deductions)"
            t2_after = cc1.get("t2", 0) * unit
            deductions = p3_ref - t2_after
            p3_note = f"T2 own funds after Art. 66 deductions: EUR {t2_after/1e6:,.0f}M (deductions: EUR {deductions/1e6:,.0f}M)".replace(",", ".")
        elif selected_cat == "Senior Non-Preferred":
            p3_ref = tlac1.get("subordinated_eligible_liabilities", 0) * unit
            p3_label = "P3 Sub. Elig. Liab. (TLAC1)"

    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Total Instruments", len(filtered))
    col2.metric("MREL Eligible", len(filtered[filtered["MREL Eligible"] == True]))
    total_outstanding = filtered[current_amount_col].sum()
    col3.metric("Total Outstanding", f"EUR {total_outstanding:,.0f}".replace(",", "."))
    eligible_outstanding = filtered[filtered["MREL Eligible"] == True][current_amount_col].sum()
    col4.metric("Eligible Outstanding", f"EUR {eligible_outstanding:,.0f}".replace(",", "."))
    if p3_ref is not None:
        col5.metric(p3_label, f"EUR {p3_ref:,.0f}".replace(",", "."),
                     delta=f"{(eligible_outstanding - p3_ref)/1e6:+,.0f}M vs P3".replace(",", "."),
                     delta_color="off")
    else:
        col5.metric("Pillar 3", "N/A")

    if p3_note:
        st.caption(p3_note)

    display_columns = [
        "ISIN",
        "Name",
        "Category",
        "Issue Date",
        "Maturity Date",
        current_amount_col,
        *[snapshot_column_name(reference_date) for reference_date in SNAPSHOT_DATES],
        "Original Amount (EUR)",
        "Currency",
        "Listing Venue",
        "MREL Eligible",
        "MREL Layer",
        "Eligibility Reason",
        "Confidence",
    ]
    display_columns = [column for column in display_columns if column in filtered.columns]

    st.dataframe(
        filtered[display_columns],
        use_container_width=True,
        hide_index=True,
        column_config={
            current_amount_col: st.column_config.NumberColumn("Outstanding Current (EUR)", format="EUR %,.0f"),
            "Original Amount (EUR)": st.column_config.NumberColumn(format="EUR %,.0f"),
            **{
                snapshot_column_name(reference_date): st.column_config.NumberColumn(format="EUR %,.0f")
                for reference_date in SNAPSHOT_DATES
            },
            "Confidence": st.column_config.ProgressColumn(min_value=0, max_value=1),
            "MREL Eligible": st.column_config.CheckboxColumn(),
        },
    )

    if st.button("Export to Excel"):
        filtered.to_excel("data/processed/filtered_export.xlsx", index=False)
        st.success("Exported to data/processed/filtered_export.xlsx")
