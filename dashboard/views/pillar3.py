from __future__ import annotations
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from dashboard.official_pillar3 import (
    build_tlac3_rank_table,
    describe_cbr_treatment,
    get_cbr_research_record,
    get_normalized_requirement_profile,
    get_template_coverage,
    get_template_snapshot,
)


def _first_numeric(snapshot: pd.DataFrame, row_code: str) -> float | None:
    numeric = snapshot[snapshot["row"] == row_code]["fact_value_numeric"].dropna()
    if numeric.empty:
        return None
    return float(numeric.iloc[0])


def _sum_numeric(snapshot: pd.DataFrame, row_codes: list[str]) -> float:
    numeric = snapshot[snapshot["row"].isin(row_codes)]["fact_value_numeric"].dropna()
    if numeric.empty:
        return 0.0
    return float(numeric.sum())


def _fmt_eur(value: float | None) -> str:
    if value is None:
        return "N/A"
    return f"EUR {value:,.0f}".replace(",", ".")


def _fmt_pct(decimal_value: float | None) -> str:
    if decimal_value is None:
        return "N/A"
    return f"{decimal_value * 100:.2f}%"


def render(entity_name: str, reference_date: str) -> None:
    st.subheader("Pillar 3 Official")

    coverage = get_template_coverage(entity_name, reference_date)
    km2 = get_template_snapshot(entity_name, reference_date, "KM2")
    tlac1 = get_template_snapshot(entity_name, reference_date, "TLAC1")
    profile = get_normalized_requirement_profile(entity_name, reference_date)
    cbr_research = get_cbr_research_record(entity_name, reference_date)

    if km2.empty and tlac1.empty and not coverage.get("TLAC3"):
        st.warning("No official Pillar 3 data is available for this bank/date.")
        return

    if not km2.empty:
        total_mrel = _first_numeric(km2, "0010")
        subordinated = _first_numeric(km2, "0020")
        trea = _first_numeric(km2, "0030")
        tem = _first_numeric(km2, "0060")

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total MREL", _fmt_eur(total_mrel))
        col2.metric("Of Which Subordinated", _fmt_eur(subordinated))
        col3.metric("TREA", _fmt_eur(trea))
        col4.metric("TEM", _fmt_eur(tem))

        status_col1, status_col2, status_col3 = st.columns(3)
        status_col1.metric("CBR Disclosed", "Yes" if profile.cbr_disclosed else "No")
        status_col2.metric("CBR / TREA", _fmt_pct(profile.cbr_trea))
        status_col3.metric("Binding MREL / TREA", _fmt_pct(profile.binding_mrel_trea))

        if profile.ratio_scale_notes:
            st.caption("Normalization notes: " + " | ".join(profile.ratio_scale_notes))
        elif not profile.cbr_disclosed:
            st.caption("No CBR row disclosed for this bank/date. KM2 requirements remain ex-CBR.")
        if cbr_research is not None:
            research_caption = f"Reviewed Pillar 3 PDF: {describe_cbr_treatment(cbr_research.cbr_treatment)}."
            if cbr_research.evidence_page is not None:
                research_caption += f" Evidence page: {cbr_research.evidence_page}."
            st.caption(research_caption)

        ratio_data = [
            {
                "Metric": "MREL / TREA",
                "Actual": profile.actual_mrel_trea,
                "Requirement": profile.binding_mrel_trea,
                "Requirement Raw": profile.requirement_mrel_trea,
                "Base": trea,
            },
            {
                "Metric": "MREL / TEM",
                "Actual": profile.actual_mrel_tem,
                "Requirement": profile.requirement_mrel_tem,
                "Requirement Raw": profile.requirement_mrel_tem,
                "Base": tem,
            },
            {
                "Metric": "Subordination / TREA",
                "Actual": profile.actual_subordination_trea,
                "Requirement": profile.requirement_subordination_trea,
                "Requirement Raw": profile.requirement_subordination_trea,
                "Base": trea,
            },
            {
                "Metric": "Subordination / TEM",
                "Actual": profile.actual_subordination_tem,
                "Requirement": profile.requirement_subordination_tem,
                "Requirement Raw": profile.requirement_subordination_tem,
                "Base": tem,
            },
        ]
        ratio_rows = []
        for entry in ratio_data:
            actual = entry["Actual"]
            requirement = entry["Requirement"]
            buffer = (actual - requirement) if actual is not None and requirement is not None else None
            buffer_eur = (buffer * entry["Base"]) if buffer is not None and entry["Base"] is not None else None
            ratio_rows.append(
                {
                    "Metric": entry["Metric"],
                    "Actual": _fmt_pct(actual),
                    "Requirement (Normalized)": _fmt_pct(requirement),
                    "Requirement (Official Raw)": _fmt_pct(entry["Requirement Raw"]),
                    "Buffer": _fmt_pct(buffer),
                    "Buffer (EUR)": _fmt_eur(buffer_eur),
                }
            )

        st.subheader("MREL Ratios vs Requirements (Normalized)")
        st.dataframe(pd.DataFrame(ratio_rows), use_container_width=True, hide_index=True)

        fig = go.Figure()
        fig.add_trace(
            go.Bar(
                name="Actual",
                x=[row["Metric"] for row in ratio_data],
                y=[(row["Actual"] or 0) * 100 for row in ratio_data],
                marker_color="#2E86AB",
            )
        )
        fig.add_trace(
            go.Bar(
                name="Requirement",
                x=[row["Metric"] for row in ratio_data],
                y=[(row["Requirement"] or 0) * 100 for row in ratio_data],
                marker_color="#E74C3C",
            )
        )
        fig.update_layout(
            title="Official KM2 Ratios (Normalized)",
            yaxis_title="% of TREA / TEM",
            barmode="group",
            height=360,
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("KM2 metrics are not available for this bank/date.")

    st.subheader("MREL Composition (EU TLAC1)")
    if tlac1.empty:
        st.info("TLAC1 composition is not available for this bank/date.")
    else:
        tlac_rows = [
            {"Component": "CET1", "Amount (EUR)": _first_numeric(tlac1, "0010")},
            {"Component": "AT1", "Amount (EUR)": _first_numeric(tlac1, "0020")},
            {"Component": "Tier 2", "Amount (EUR)": _first_numeric(tlac1, "0060")},
            {"Component": "Own Funds", "Amount (EUR)": _first_numeric(tlac1, "0090")},
            {"Component": "Subordinated Eligible Liabilities", "Amount (EUR)": _sum_numeric(tlac1, ["0100", "0110", "0120", "0130"])},
            {"Component": "Non-Subordinated Eligible Liabilities", "Amount (EUR)": _first_numeric(tlac1, "0160")},
            {"Component": "Eligible Liabilities Before Adjustments", "Amount (EUR)": _first_numeric(tlac1, "0190")},
            {"Component": "Own Funds and Eligible Liabilities Before Adjustments", "Amount (EUR)": _first_numeric(tlac1, "0210")},
            {"Component": "MPE Deductions", "Amount (EUR)": -(_first_numeric(tlac1, "0220") or 0)},
            {"Component": "Investments in Eligible Liabilities", "Amount (EUR)": -(_first_numeric(tlac1, "0230") or 0)},
            {"Component": "Total MREL After Adjustments", "Amount (EUR)": _first_numeric(tlac1, "0250")},
            {"Component": "Of Which Own Funds and Subordinated", "Amount (EUR)": _first_numeric(tlac1, "0260")},
        ]
        tlac_df = pd.DataFrame(tlac_rows)
        st.dataframe(
            tlac_df,
            use_container_width=True,
            hide_index=True,
            column_config={"Amount (EUR)": st.column_config.NumberColumn(format="EUR %,.0f")},
        )

    st.subheader("Creditor Ranking (EU TLAC3)")
    tlac3_table = build_tlac3_rank_table(entity_name, reference_date)
    if tlac3_table.empty:
        st.info("TLAC3 creditor ranking is not available for this bank/date.")
    else:
        st.dataframe(
            tlac3_table,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Liabilities and Own Funds": st.column_config.NumberColumn(format="EUR %,.0f"),
                "Excluded Liabilities": st.column_config.NumberColumn(format="EUR %,.0f"),
                "Less Excluded Liabilities": st.column_config.NumberColumn(format="EUR %,.0f"),
                "Potentially Eligible for MREL/TLAC": st.column_config.NumberColumn(format="EUR %,.0f"),
            },
        )
