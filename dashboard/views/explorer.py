from __future__ import annotations
import streamlit as st
import pandas as pd


def render(df: pd.DataFrame) -> None:
    st.header("Instrument Explorer")

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

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Instruments", len(filtered))
    col2.metric("MREL Eligible", len(filtered[filtered["MREL Eligible"] == True]))
    total_outstanding = filtered["Outstanding (EUR)"].sum()
    col3.metric("Total Outstanding", f"EUR {total_outstanding:,.0f}")
    eligible_outstanding = filtered[filtered["MREL Eligible"] == True]["Outstanding (EUR)"].sum()
    col4.metric("Eligible Outstanding", f"EUR {eligible_outstanding:,.0f}")

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

    if st.button("Export to Excel"):
        filtered.to_excel("data/processed/filtered_export.xlsx", index=False)
        st.success("Exported to data/processed/filtered_export.xlsx")
