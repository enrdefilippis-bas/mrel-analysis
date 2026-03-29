from __future__ import annotations
import streamlit as st
import pandas as pd


def _classify_note_type(name: str) -> str:
    if pd.isna(name) or name == "Unknown Instrument":
        return "Unknown (no name available)"
    n = name.lower()
    if "mc ind link" in n or "mc  ind" in n:
        return "Multi Callable Index Linked"
    if " cc " in f" {n} " or n.startswith("bpm cc ") or n.startswith("cc "):
        return "Credit Certificate (single stock linked)"
    if "ep cp" in n or "ep  cp" in n:
        return "Equity Premium Capital Protected"
    if "ep " in n and ("eurostoxx" in n or "utilitie" in n):
        return "Equity Premium (index linked)"
    return name[:50]


def render(df: pd.DataFrame) -> None:
    st.header("Data Quality & Audit")

    st.subheader("Low Confidence Classifications")
    low_conf = df[df["Confidence"] < 0.8].sort_values("Confidence")
    if len(low_conf) > 0:
        st.warning(f"{len(low_conf)} instruments with confidence < 80%")

        st.markdown(
            "**Why low confidence?** These are Banca Akros certificates classified as "
            "capital-protected based on instrument name patterns or source-data flags, "
            "not from verified prospectus text. The capital protection terms should be "
            "confirmed from the Final Terms / KID to raise confidence."
        )

        # Group by note type
        low_conf = low_conf.copy()
        low_conf["Note Type"] = low_conf["Name"].apply(_classify_note_type)
        type_summary = (
            low_conf.groupby("Note Type")
            .agg(Count=("ISIN", "count"), Total_EUR=("Outstanding (EUR)", "sum"))
            .sort_values("Total_EUR", ascending=False)
            .reset_index()
        )
        type_summary["Total (EUR M)"] = type_summary["Total_EUR"].apply(
            lambda v: f"{v / 1e6:,.1f}M".replace(",", ".") if pd.notna(v) else "N/A"
        )
        st.dataframe(
            type_summary[["Note Type", "Count", "Total (EUR M)"]],
            use_container_width=True, hide_index=True,
        )

        # Expandable detail per type
        for note_type in type_summary["Note Type"]:
            subset = low_conf[low_conf["Note Type"] == note_type]
            count = len(subset)
            reason = subset["Eligibility Reason"].iloc[0] if "Eligibility Reason" in subset.columns else ""
            with st.expander(f"{note_type} ({count} instruments)"):
                if reason:
                    st.caption(reason)
                st.dataframe(
                    subset[["ISIN", "Name", "Issue Date", "Maturity Date", "Outstanding (EUR)",
                            "MREL Eligible", "Confidence"]],
                    use_container_width=True, hide_index=True,
                )
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
