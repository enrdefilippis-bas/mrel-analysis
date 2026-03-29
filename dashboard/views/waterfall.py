from __future__ import annotations
import streamlit as st
import pandas as pd
from dashboard.components.charts import waterfall_chart
from dashboard.official_pillar3 import (
    build_official_waterfall,
    describe_cbr_treatment,
    get_cbr_research_record,
    get_normalized_requirement_profile,
    get_template_coverage,
)


def _fmt_eur(value: float) -> str:
    return f"EUR {value:,.0f}".replace(",", ".")


def render(entity_name: str, reference_date: str) -> None:
    st.subheader("MREL Stack Waterfall")

    coverage = get_template_coverage(entity_name, reference_date)
    if not coverage.get("TLAC1"):
        st.info(
            "Official TLAC1 composition is not available for this bank/date. "
            "The Pillar 3 Official tab still shows the KM2 metrics and any other official tables present."
        )
        return

    waterfall = build_official_waterfall(entity_name, reference_date)
    if waterfall is None:
        st.info("The official waterfall cannot be built for this selection.")
        return
    profile = get_normalized_requirement_profile(entity_name, reference_date)
    cbr_research = get_cbr_research_record(entity_name, reference_date)

    first_req = waterfall.requirement_lines[0]["value"] if waterfall.requirement_lines else None
    second_req = waterfall.requirement_lines[1]["value"] if len(waterfall.requirement_lines) > 1 else None

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total MREL", _fmt_eur(waterfall.total_mrel))
    col2.metric("Of Which Subordinated", _fmt_eur(waterfall.subordination_total))
    col3.metric("TREA", _fmt_eur(waterfall.trea) if waterfall.trea else "N/A")
    col4.metric("MREL Requirement", _fmt_eur(float(first_req)) if first_req else "N/A")

    if not coverage.get("KM2"):
        st.info("KM2 is missing for this bank/date, so requirement lines are not shown on the waterfall.")
    elif second_req:
        st.caption(
            f"Official requirement lines: MREL {_fmt_eur(float(first_req))} | "
            f"Subordination {_fmt_eur(float(second_req))}"
        )
        if cbr_research is not None and cbr_research.cbr_treatment == "on_top" and profile.cbr_trea is not None:
            st.caption(f"CBR normalized on top of MREL/TREA: {profile.cbr_trea * 100:.2f}%")
        elif cbr_research is not None and cbr_research.cbr_treatment == "included" and profile.cbr_trea is not None:
            st.caption(f"CBR already included in the reviewed Pillar 3 MREL/TREA: {profile.cbr_trea * 100:.2f}%")
        elif profile.cbr_disclosed and profile.cbr_trea is not None:
            st.caption(f"CBR normalized on top of MREL/TREA: {profile.cbr_trea * 100:.2f}%")
        else:
            st.caption("CBR not disclosed in the workbook for this bank/date; MREL requirement remains ex-CBR.")
        if cbr_research is not None:
            st.caption(f"Reviewed Pillar 3 PDF: {describe_cbr_treatment(cbr_research.cbr_treatment)}.")

    fig = waterfall_chart(
        waterfall.components,
        title=f"Official MREL Stack Waterfall — {entity_name} ({reference_date})",
        requirement_lines=waterfall.requirement_lines,
    )
    st.plotly_chart(fig, use_container_width=True)

    breakdown = pd.DataFrame(waterfall.components).rename(columns={"label": "Component", "value": "Amount (EUR)"})
    st.dataframe(
        breakdown[["Component", "Amount (EUR)"]],
        use_container_width=True,
        hide_index=True,
        column_config={"Amount (EUR)": st.column_config.NumberColumn(format="EUR %,.0f")},
    )
