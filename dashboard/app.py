"""MREL Analysis Dashboard — Banco BPM"""
from __future__ import annotations
import sqlite3
from datetime import date
from pathlib import Path

import streamlit as st
import pandas as pd

from dashboard.views import explorer, waterfall, reconciliation, audit

st.set_page_config(
    page_title="MREL Analysis — Banco BPM",
    page_icon="🏦",
    layout="wide",
)

DB_PATH = Path("data/db/mrel.db")


@st.cache_data(ttl=300)
def load_data() -> pd.DataFrame:
    if not DB_PATH.exists():
        return pd.DataFrame()
    conn = sqlite3.connect(str(DB_PATH))
    df = pd.read_sql("SELECT * FROM instruments", conn)
    conn.close()

    column_map = {
        "isin": "ISIN",
        "name": "Name",
        "category": "Category",
        "issue_date": "Issue Date",
        "maturity_date": "Maturity Date",
        "coupon_type": "Coupon Type",
        "coupon_rate": "Coupon Rate",
        "outstanding_amount": "Outstanding (EUR)",
        "currency": "Currency",
        "crr2_rank": "CRR2 Rank",
        "listing_venue": "Listing Venue",
        "mrel_eligible": "MREL Eligible",
        "mrel_layer": "MREL Layer",
        "eligibility_reason": "Eligibility Reason",
        "classification_confidence": "Confidence",
        "bail_in_clause": "Bail-in Clause",
        "capital_protected": "Capital Protected",
        "underlying_linked": "Underlying Linked",
    }
    df = df.rename(columns=column_map)

    for col in ["MREL Eligible", "Bail-in Clause", "Capital Protected", "Underlying Linked"]:
        if col in df.columns:
            df[col] = df[col].map({1: True, 0: False, None: None})

    return df


def main():
    st.title("MREL Analysis — Banco BPM")
    st.caption("Reference Date: 31.12.2024 | Prospectus-First Classification per CRR2/BRRD2/SRB")

    st.sidebar.title("Navigation")
    ref_date = st.sidebar.date_input("Reference Date", date(2024, 12, 31))
    view = st.sidebar.radio(
        "View",
        ["Instrument Explorer", "MREL Stack Waterfall", "Reconciliation", "Data Quality & Audit"],
    )

    if st.sidebar.button("Refresh Data"):
        st.cache_data.clear()

    df = load_data()

    if df.empty:
        st.warning(
            "No data available. Run the pipeline first:\n\n"
            "```bash\npython pipeline.py\n```"
        )
        return

    if view == "Instrument Explorer":
        explorer.render(df)
    elif view == "MREL Stack Waterfall":
        waterfall.render(df, ref_date)
    elif view == "Reconciliation":
        reconciliation.render(df)
    elif view == "Data Quality & Audit":
        audit.render(df)


if __name__ == "__main__":
    main()
