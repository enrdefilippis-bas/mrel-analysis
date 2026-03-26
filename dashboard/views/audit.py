from __future__ import annotations
import streamlit as st
import pandas as pd


def render(df: pd.DataFrame) -> None:
    st.header("Data Quality & Audit")

    st.subheader("Low Confidence Classifications")
    low_conf = df[df["Confidence"] < 0.8].sort_values("Confidence")
    if len(low_conf) > 0:
        st.warning(f"{len(low_conf)} instruments with confidence < 80%")
        st.dataframe(low_conf, use_container_width=True, hide_index=True)
    else:
        st.success("All instruments classified with high confidence")

    st.subheader("Missing Outstanding Amounts")
    missing_amt = df[df["Outstanding (EUR)"].isna() | (df["Outstanding (EUR)"] == 0)]
    if len(missing_amt) > 0:
        st.warning(f"{len(missing_amt)} instruments without outstanding amount data")
        st.dataframe(missing_amt[["ISIN", "Name", "Category"]], use_container_width=True, hide_index=True)
    else:
        st.success("All instruments have outstanding amount data")

    st.subheader("Category Distribution")
    cat_counts = df["Category"].value_counts().reset_index()
    cat_counts.columns = ["Category", "Count"]
    st.dataframe(cat_counts, use_container_width=True, hide_index=True)

    st.subheader("Unclassified Instruments")
    unknown = df[df["Category"] == "Unknown"]
    if len(unknown) > 0:
        st.error(f"{len(unknown)} instruments could not be classified")
        st.dataframe(unknown, use_container_width=True, hide_index=True)
    else:
        st.success("All instruments classified")

    st.subheader("Export")
    if st.button("Export Full Dataset to Excel"):
        df.to_excel("data/processed/full_audit_export.xlsx", index=False)
        st.success("Exported to data/processed/full_audit_export.xlsx")
