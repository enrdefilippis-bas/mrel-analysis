from __future__ import annotations
import json
import streamlit as st
import pandas as pd
from pathlib import Path


def _load_pillar3() -> dict | None:
    agg_path = Path(__file__).resolve().parent.parent.parent / "data" / "processed" / "pillar3_aggregates.json"
    if not agg_path.exists():
        return None
    with open(agg_path) as f:
        return json.load(f)


def _fmt_eur(val) -> str:
    """Format number in European style with dot thousand separators."""
    if val is None:
        return "N/A"
    return f"{val:,.0f}".replace(",", ".")


def render(df: pd.DataFrame) -> None:
    st.header("Reconciliation: Bottom-Up vs Pillar 3")

    p3 = _load_pillar3()
    if not p3:
        st.warning("Pillar 3 aggregates not yet available. Run the pipeline first.")
        return

    # Extract Pillar 3 official amounts (thousands EUR -> EUR)
    tlac1 = p3.get("mrel_tlac1_composition", {})
    km2 = p3.get("mrel_km2", {})
    cc1 = p3.get("own_funds_cc1", {})
    unit = 1000 if p3.get("amounts_unit") == "thousands_eur" else 1

    p3_values = {
        "CET1": cc1.get("cet1", 0) * unit,
        "AT1": cc1.get("at1", 0) * unit,
        "Tier 2": cc1.get("t2", 0) * unit,
        "Senior Non-Preferred": tlac1.get("subordinated_eligible_liabilities", 0) * unit,
    }
    p3_non_sub = tlac1.get("non_subordinated_eligible_liabilities", 0) * unit

    eligible = df[df["MREL Eligible"] == True]
    bu_by_cat = eligible.groupby("Category")["Outstanding (EUR)"].sum()

    # Own funds (CET1, AT1, T2): use Pillar 3 directly — regulatory adjustments
    # (retained earnings, amortization, deductions) can't be derived from instruments
    own_funds_categories = ["CET1", "AT1", "Tier 2"]
    # Liabilities: bottom-up vs Pillar 3 comparison
    liability_categories = ["Senior Non-Preferred"]
    # Non-subordinated categories (Pillar 3 reports them combined)
    non_sub_categories = ["Senior Preferred", "Structured Note (Capital Protected)"]

    rows = []
    raw_deltas = []

    for cat in own_funds_categories:
        p3_val = p3_values[cat]
        rows.append({
            "Category": f"{cat} (Pillar 3)",
            "Bottom-Up (EUR)": _fmt_eur(p3_val),
            "Pillar 3 (EUR)": _fmt_eur(p3_val),
            "Delta (EUR)": _fmt_eur(0),
            "Delta %": "0.0%",
        })
        raw_deltas.append(0)

    for cat in liability_categories:
        bu_val = bu_by_cat.get(cat, 0)
        p3_val = p3_values.get(cat)
        delta = (bu_val - p3_val) if p3_val is not None else None
        raw_deltas.append(delta)
        rows.append({
            "Category": cat,
            "Bottom-Up (EUR)": _fmt_eur(bu_val),
            "Pillar 3 (EUR)": _fmt_eur(p3_val),
            "Delta (EUR)": _fmt_eur(delta),
            "Delta %": f"{delta / p3_val * 100:.1f}%" if p3_val and delta is not None else "N/A",
        })

    # Non-subordinated: show each component then subtotal vs Pillar 3
    bu_non_sub_total = 0
    for cat in non_sub_categories:
        bu_val = bu_by_cat.get(cat, 0)
        bu_non_sub_total += bu_val
        rows.append({
            "Category": f"  {cat}",
            "Bottom-Up (EUR)": _fmt_eur(bu_val),
            "Pillar 3 (EUR)": "",
            "Delta (EUR)": "",
            "Delta %": "",
        })

    # Non-subordinated subtotal vs Pillar 3
    delta_non_sub = bu_non_sub_total - p3_non_sub if p3_non_sub else None
    raw_deltas.append(delta_non_sub)
    rows.append({
        "Category": "Non-Subordinated Total (SP + Structured)",
        "Bottom-Up (EUR)": _fmt_eur(bu_non_sub_total),
        "Pillar 3 (EUR)": _fmt_eur(p3_non_sub),
        "Delta (EUR)": _fmt_eur(delta_non_sub),
        "Delta %": f"{delta_non_sub / p3_non_sub * 100:.1f}%" if p3_non_sub and delta_non_sub is not None else "N/A",
    })

    # Pre-grandfathering non-subordinated (eligible deposits & other senior)
    pre_grandf = tlac1.get("pre_grandfathering_non_subordinated", 0) * unit
    rows.append({
        "Category": "Pre-Grandfathering Non-Sub (Eligible Deposits)",
        "Bottom-Up (EUR)": "—",
        "Pillar 3 (EUR)": _fmt_eur(pre_grandf),
        "Delta (EUR)": "",
        "Delta %": "",
    })

    # Deductions on eligible liabilities
    deductions = tlac1.get("deduction_investments_eligible_liabilities", 0) * unit
    rows.append({
        "Category": "Deductions (investments in elig. liabilities)",
        "Bottom-Up (EUR)": "—",
        "Pillar 3 (EUR)": _fmt_eur(-deductions),
        "Delta (EUR)": "",
        "Delta %": "",
    })

    # Grand total
    own_funds_total = sum(p3_values[cat] for cat in own_funds_categories)
    liabilities_bu = sum(bu_by_cat.get(cat, 0) for cat in liability_categories + non_sub_categories)
    bu_total = own_funds_total + liabilities_bu + pre_grandf - deductions
    p3_total = km2.get("eligible_own_funds_and_liabilities", 0) * unit
    delta_total = bu_total - p3_total if p3_total else None
    raw_deltas.append(delta_total)
    rows.append({
        "Category": "TOTAL MREL",
        "Bottom-Up (EUR)": _fmt_eur(bu_total),
        "Pillar 3 (EUR)": _fmt_eur(p3_total),
        "Delta (EUR)": _fmt_eur(delta_total),
        "Delta %": f"{(bu_total - p3_total) / p3_total * 100:.1f}%" if p3_total else "N/A",
    })

    recon_df = pd.DataFrame(rows)
    st.dataframe(recon_df, use_container_width=True, hide_index=True)

    if any(d is not None and abs(d) > 1_000_000 for d in raw_deltas):
        st.warning("Significant discrepancies detected (> EUR 1M). Check the Audit view for details.")

    # MREL ratios comparison
    st.subheader("MREL Ratios (Pillar 3 Official)")
    reqs = p3.get("mrel_requirements", {})
    ratio_rows = [
        {"Metric": "MREL / TREA", "Actual": f"{km2.get('mrel_pct_trea', 0):.2f}%",
         "Requirement": f"{reqs.get('mrel_trea_pct', 0):.2f}%",
         "Buffer": f"{km2.get('mrel_pct_trea', 0) - reqs.get('mrel_trea_pct', 0):.2f}%"},
        {"Metric": "MREL / TEM", "Actual": f"{km2.get('mrel_pct_tem', 0):.2f}%",
         "Requirement": f"{reqs.get('mrel_tem_pct', 0):.2f}%",
         "Buffer": f"{km2.get('mrel_pct_tem', 0) - reqs.get('mrel_tem_pct', 0):.2f}%"},
        {"Metric": "Subordination / TREA", "Actual": f"{km2.get('subordinated_pct_trea', 0):.2f}%",
         "Requirement": f"{reqs.get('subordination_trea_pct', 0):.2f}%",
         "Buffer": f"{km2.get('subordinated_pct_trea', 0) - reqs.get('subordination_trea_pct', 0):.2f}%"},
        {"Metric": "Subordination / TEM", "Actual": f"{km2.get('subordinated_pct_tem', 0):.2f}%",
         "Requirement": f"{reqs.get('subordination_tem_pct', 0):.2f}%",
         "Buffer": f"{km2.get('subordinated_pct_tem', 0) - reqs.get('subordination_tem_pct', 0):.2f}%"},
    ]
    st.dataframe(pd.DataFrame(ratio_rows), use_container_width=True, hide_index=True)

    # Creditor hierarchy
    st.subheader("Creditor Hierarchy (EU TLAC3a)")
    hierarchy = p3.get("creditor_hierarchy_tlac3a", {})
    if hierarchy:
        hier_rows = [
            {"Rank": "1 - Equity", "Amount (EUR M)": hierarchy.get("rank_1_equity", 0) * unit / 1e6},
            {"Rank": "2 - Subordinated Claims", "Amount (EUR M)": hierarchy.get("rank_2_subordinated_claims", 0) * unit / 1e6},
            {"Rank": "3 - Sub. Liabilities (not own funds)", "Amount (EUR M)": hierarchy.get("rank_3_subordinated_liabilities_not_own_funds", 0) * unit / 1e6},
            {"Rank": "4 - Senior Non-Preferred", "Amount (EUR M)": hierarchy.get("rank_4_senior_non_preferred", 0) * unit / 1e6},
            {"Rank": "5 - Unsecured Claims", "Amount (EUR M)": hierarchy.get("rank_5_unsecured_claims", 0) * unit / 1e6},
            {"Rank": "6 - Deposits & Other Senior", "Amount (EUR M)": hierarchy.get("rank_6_deposits_and_other_senior", 0) * unit / 1e6},
            {"Rank": "TOTAL", "Amount (EUR M)": hierarchy.get("total", 0) * unit / 1e6},
        ]
        st.dataframe(pd.DataFrame(hier_rows), use_container_width=True, hide_index=True)
