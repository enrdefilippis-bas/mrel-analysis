from __future__ import annotations
import json
import streamlit as st
import pandas as pd
from pathlib import Path


def render(df: pd.DataFrame) -> None:
    st.header("Reconciliation: Bottom-Up vs Pillar 3")

    agg_path = Path("data/processed/pillar3_aggregates.json")
    if not agg_path.exists():
        st.warning("Pillar 3 aggregates not yet available. Run the pipeline first.")
        return

    with open(agg_path) as f:
        p3 = json.load(f)

    eligible = df[df["MREL Eligible"] == True]
    bu_by_cat = eligible.groupby("Category")["Outstanding (EUR)"].sum()

    categories = ["CET1", "AT1", "Tier 2", "Senior Non-Preferred", "Senior Preferred",
                  "Structured Note (Capital Protected)"]
    p3_keys = ["cet1", "at1", "tier2", "senior_non_preferred", "senior_preferred", None]

    rows = []
    for cat, p3_key in zip(categories, p3_keys):
        bu_val = bu_by_cat.get(cat, 0)
        p3_val = p3.get(p3_key) if p3_key else None
        delta = (bu_val - p3_val) if p3_val is not None else None

        rows.append({
            "Category": cat,
            "Bottom-Up (EUR)": bu_val,
            "Pillar 3 (EUR)": p3_val,
            "Delta (EUR)": delta,
            "Delta %": f"{delta / p3_val * 100:.1f}%" if p3_val and delta is not None else "N/A",
        })

    recon_df = pd.DataFrame(rows)
    st.dataframe(recon_df, use_container_width=True, hide_index=True)

    if any(r["Delta (EUR)"] is not None and abs(r["Delta (EUR)"]) > 1_000_000 for r in rows):
        st.warning("Significant discrepancies detected (> EUR 1M). Check the Audit view for details.")
