from __future__ import annotations
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from dashboard.official_pillar3 import (
    CBRResearchRecord,
    NormalizedRequirementProfile,
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


def _eur_compact(value: float | None) -> str:
    """Format a EUR amount (in thousands) as a compact readable string."""
    if value is None:
        return "N/A"
    abs_val = abs(value)
    if abs_val >= 1_000_000:
        return f"EUR {value / 1_000_000:.1f}bn"
    if abs_val >= 1_000:
        return f"EUR {value / 1_000:.0f}m"
    return f"EUR {value:.0f}k"


def _build_qualitative_bullets(
    profile: NormalizedRequirementProfile,
    km2: pd.DataFrame,
    tlac1: pd.DataFrame,
    cbr_research: CBRResearchRecord | None,
) -> list[str]:
    bullets: list[str] = []

    trea = _first_numeric(km2, "0030")

    # --- MREL / TREA headroom ---
    if profile.actual_mrel_trea is not None and profile.binding_mrel_trea is not None:
        buffer = profile.actual_mrel_trea - profile.binding_mrel_trea
        bps = buffer * 10_000
        req_label = "binding (incl. CBR)" if profile.cbr_disclosed else "stated"
        buffer_eur = trea * buffer if trea else None
        headroom_str = f" ({_eur_compact(buffer_eur)} of headroom)" if buffer_eur is not None else ""
        if buffer >= 0:
            quality = "substantial" if bps > 500 else ("adequate" if bps > 200 else "limited")
            bullets.append(
                f"**MREL / TREA** is {_fmt_pct(profile.actual_mrel_trea)}, {bps:+.0f} bps above the "
                f"{req_label} requirement of {_fmt_pct(profile.binding_mrel_trea)}{headroom_str} — {quality} cushion."
            )
        else:
            bullets.append(
                f"**MREL / TREA** of {_fmt_pct(profile.actual_mrel_trea)} is {abs(bps):.0f} bps **below** "
                f"the {req_label} requirement of {_fmt_pct(profile.binding_mrel_trea)}."
            )

    # --- Subordination / TREA headroom ---
    if profile.actual_subordination_trea is not None and profile.binding_subordination_trea is not None:
        sub_buf = profile.actual_subordination_trea - profile.binding_subordination_trea
        sub_bps = sub_buf * 10_000
        req_label = "binding (incl. CBR)" if profile.cbr_disclosed else "stated"
        sub_buf_eur = trea * sub_buf if trea else None
        headroom_str = f" ({_eur_compact(sub_buf_eur)} of headroom)" if sub_buf_eur is not None else ""
        if sub_buf >= 0:
            quality = "substantial" if sub_bps > 500 else ("adequate" if sub_bps > 200 else "limited")
            bullets.append(
                f"**Subordination / TREA** is {_fmt_pct(profile.actual_subordination_trea)}, {sub_bps:+.0f} bps above "
                f"the {req_label} requirement of {_fmt_pct(profile.binding_subordination_trea)}{headroom_str} — {quality} cushion."
            )
        else:
            bullets.append(
                f"**Subordination / TREA** of {_fmt_pct(profile.actual_subordination_trea)} is {abs(sub_bps):.0f} bps "
                f"**below** the {req_label} requirement of {_fmt_pct(profile.binding_subordination_trea)}."
            )

    # --- CBR treatment ---
    if profile.cbr_disclosed and profile.cbr_trea is not None:
        ex_cbr_note = ""
        if profile.requirement_mrel_trea is not None:
            ex_cbr_note = f"; ex-CBR SRB floor is {_fmt_pct(profile.requirement_mrel_trea)} TREA"
        bullets.append(
            f"**Combined Buffer Requirement** of {_fmt_pct(profile.cbr_trea)} TREA is additive to the MREL "
            f"requirement{ex_cbr_note}."
        )
    elif cbr_research is not None and cbr_research.cbr_treatment == "included":
        bullets.append("**CBR** is already embedded in the disclosed MREL requirement (not additive).")

    # --- TLAC1 composition ---
    if not tlac1.empty:
        cet1 = _first_numeric(tlac1, "0010") or 0.0
        at1 = _first_numeric(tlac1, "0020") or 0.0
        t2 = _first_numeric(tlac1, "0060") or 0.0
        own_funds_raw = _first_numeric(tlac1, "0090")
        own_funds = own_funds_raw if own_funds_raw is not None else cet1 + at1 + t2
        sub_el = _sum_numeric(tlac1, ["0100", "0110", "0120", "0130"])
        non_sub = _first_numeric(tlac1, "0160") or 0.0
        total = _first_numeric(tlac1, "0250")

        if total and total > 0:
            of_pct = own_funds / total
            sub_el_pct = sub_el / total
            non_sub_pct = non_sub / total
            bullets.append(
                f"**MREL composition**: own funds {_fmt_pct(of_pct)} ({_eur_compact(own_funds)}), "
                f"subordinated eligible liabilities {_fmt_pct(sub_el_pct)} ({_eur_compact(sub_el)}), "
                f"non-subordinated eligible liabilities {_fmt_pct(non_sub_pct)} ({_eur_compact(non_sub)})."
            )

            # Does own funds alone cover the subordination requirement?
            if profile.binding_subordination_trea is not None and trea:
                sub_req_eur = profile.binding_subordination_trea * trea
                if own_funds >= sub_req_eur:
                    bullets.append(
                        f"Own funds ({_eur_compact(own_funds)}) alone cover the subordination requirement "
                        f"({_eur_compact(sub_req_eur)}); subordinated eligible liabilities provide additional buffer."
                    )
                else:
                    gap = sub_req_eur - own_funds
                    bullets.append(
                        f"Own funds ({_eur_compact(own_funds)}) fall {_eur_compact(gap)} short of the subordination "
                        f"requirement ({_eur_compact(sub_req_eur)}); {_eur_compact(sub_el)} of subordinated eligible "
                        f"liabilities bridges the gap."
                    )

            # Non-sub materiality
            if non_sub > 0 and non_sub_pct > 0.15:
                bullets.append(
                    f"Non-subordinated eligible liabilities represent {_fmt_pct(non_sub_pct)} of total MREL "
                    f"— a meaningful share that is sensitive to BRRD2 grandfathering eligibility criteria."
                )

    # --- Binding metric: TREA vs TEM ---
    if (
        profile.actual_mrel_trea is not None
        and profile.actual_mrel_tem is not None
        and profile.binding_mrel_trea is not None
        and profile.requirement_mrel_tem is not None
        and profile.binding_mrel_trea > 0
        and profile.requirement_mrel_tem > 0
    ):
        trea_cov = profile.actual_mrel_trea / profile.binding_mrel_trea
        tem_cov = profile.actual_mrel_tem / profile.requirement_mrel_tem
        binding = "TREA" if trea_cov <= tem_cov else "TEM"
        slack = "TEM" if binding == "TREA" else "TREA"
        bullets.append(
            f"**Binding metric**: {binding} is the tighter constraint (MREL covers {trea_cov:.2f}x TREA "
            f"requirement vs {tem_cov:.2f}x TEM requirement); {slack} provides more headroom."
        )

    return bullets


def _render_qualitative_commentary(
    profile: NormalizedRequirementProfile,
    km2: pd.DataFrame,
    tlac1: pd.DataFrame,
    cbr_research: CBRResearchRecord | None,
) -> None:
    bullets = _build_qualitative_bullets(profile, km2, tlac1, cbr_research)
    if not bullets:
        return
    with st.expander("Qualitative Analysis", expanded=True):
        for b in bullets:
            st.markdown(f"- {b}")


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

        status_col1, status_col2, status_col3, status_col4 = st.columns(4)
        status_col1.metric("CBR Disclosed", "Yes" if profile.cbr_disclosed else "No")
        status_col2.metric("CBR / TREA", _fmt_pct(profile.cbr_trea))
        status_col3.metric("Binding MREL / TREA", _fmt_pct(profile.binding_mrel_trea))
        status_col4.metric("Binding Subordination / TREA", _fmt_pct(profile.binding_subordination_trea))

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
                "Requirement": profile.binding_subordination_trea,
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
        _render_qualitative_commentary(profile, km2, tlac1, cbr_research)
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
