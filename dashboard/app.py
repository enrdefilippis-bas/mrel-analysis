"""MREL Analysis Dashboard."""
from __future__ import annotations
import html
import sqlite3
from pathlib import Path

import streamlit as st
import pandas as pd

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from dashboard import official_pillar3
from dashboard.views import explorer, waterfall, reconciliation, audit, pillar3

st.set_page_config(
    page_title="MREL Analysis Dashboard",
    page_icon="🏦",
    layout="wide",
)

DB_DIR = Path(__file__).resolve().parent.parent / "data" / "db"
BANK_DB_MAP = {
    "Banco BPM": "mrel.db",
    "Intesa Sanpaolo": "intesa.db",
}
HOME_PAGE = "home"
INSTRUMENT_PAGE = "instrument_intelligence"
OFFICIAL_PAGE = "pillar3_official"


@st.cache_data(ttl=300)
def load_instrument_data(db_name: str = "mrel.db") -> pd.DataFrame:
    db_path = DB_DIR / db_name
    if not db_path.exists():
        return pd.DataFrame()
    conn = sqlite3.connect(str(db_path))
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


def _default_bank(banks: list[str]) -> str | None:
    if not banks:
        return None
    preferred = "BANCO BPM SOCIETA' PER AZIONI"
    return preferred if preferred in banks else banks[0]


def _set_page(page: str) -> None:
    st.session_state["current_page"] = page
    st.rerun()


def _render_bank_logo_badge(bank_name: str) -> None:
    logo_url = official_pillar3.get_bank_logo_url(bank_name)
    monogram = official_pillar3.get_bank_monogram(bank_name)
    safe_name = html.escape(bank_name)

    if logo_url:
        visual = (
            f'<img src="{html.escape(logo_url)}" alt="{safe_name} logo" '
            'style="width:42px;height:42px;border-radius:12px;background:#fff;padding:6px;'
            'border:1px solid rgba(255,255,255,0.12);" />'
        )
    else:
        visual = (
            '<div style="width:42px;height:42px;border-radius:12px;display:flex;align-items:center;'
            'justify-content:center;font-weight:700;font-size:0.95rem;background:rgba(59,130,246,0.18);'
            'border:1px solid rgba(96,165,250,0.35);">'
            f"{html.escape(monogram)}"
            "</div>"
        )

    st.markdown(
        """
        <div style="margin-top: 1.75rem; padding: 0.55rem 0.75rem; border: 1px solid rgba(255,255,255,0.10);
             border-radius: 14px; display: flex; align-items: center; gap: 0.75rem; min-height: 74px;">
            VISUAL
            <div style="min-width:0;">
                <div style="font-size: 0.72rem; opacity: 0.72; margin-bottom: 0.1rem;">Selected bank</div>
                <div style="font-weight: 600; line-height: 1.25;">BANK_NAME</div>
            </div>
        </div>
        """
        .replace("VISUAL", visual)
        .replace("BANK_NAME", safe_name),
        unsafe_allow_html=True,
    )


def _render_cbr_research_notice(bank_name: str, reference_date: str) -> None:
    record = official_pillar3.get_cbr_research_record(bank_name, reference_date)
    if record is None:
        st.caption("CBR PDF research: no reviewed Pillar 3 evidence is stored for this bank/date.")
        return

    treatment_text = official_pillar3.describe_cbr_treatment(record.cbr_treatment)
    if record.cbr_treatment == "on_top":
        st.success(f"CBR PDF research: {treatment_text}.")
    elif record.cbr_treatment == "included":
        st.info(f"CBR PDF research: {treatment_text}.")
    else:
        st.warning(f"CBR PDF research: {treatment_text}.")

    detail_bits: list[str] = []
    if record.evidence_page is not None:
        detail_bits.append(f"Page {record.evidence_page}")
    if record.match_count is not None:
        detail_bits.append(f"{record.match_count} keyword match{'es' if record.match_count != 1 else ''}")
    if record.source_type:
        detail_bits.append(record.source_type.replace("_", " "))
    if record.source_url:
        detail_bits.append(f"[Source PDF]({record.source_url})")
    if detail_bits:
        st.caption(" | ".join(detail_bits))

    if record.evidence_quote:
        st.markdown(f"> {record.evidence_quote}")
    if record.note:
        st.caption(record.note)


def _render_top_bar() -> None:
    nav_col, title_col, action_col = st.columns([1.2, 5, 1])
    with nav_col:
        with st.popover("Pages", use_container_width=True):
            st.caption("Navigate")
            if st.button("Home", use_container_width=True, key="nav_home"):
                _set_page(HOME_PAGE)
            if st.button("Instrument Intelligence", use_container_width=True, key="nav_page_1"):
                _set_page(INSTRUMENT_PAGE)
            if st.button("Pillar 3 Official", use_container_width=True, key="nav_page_2"):
                _set_page(OFFICIAL_PAGE)

    with title_col:
        st.title("MREL Analysis Dashboard")
        st.caption(
            "Homepage + 2 pagine distinte: Instrument Intelligence e Pillar 3 Official."
        )

    with action_col:
        if st.button("Refresh", use_container_width=True, key="refresh_cache"):
            st.cache_data.clear()
            st.rerun()


def _render_home() -> None:
    st.markdown("### Choose a page")
    st.caption("Entra nella prima o nella seconda pagina dalla homepage.")

    col1, col2 = st.columns(2)

    with col1:
        with st.container(border=True):
            st.subheader("Instrument Intelligence")
            st.caption(
                "Current scope: Banco BPM instrument dataset with explorer, reconciliation, "
                "and data quality controls."
            )
            if st.button("Open Instrument Intelligence", use_container_width=True, key="home_open_page_1"):
                _set_page(INSTRUMENT_PAGE)

    with col2:
        with st.container(border=True):
            st.subheader("Pillar 3 Official")
            st.caption(
                "Official KM2, TLAC1, and TLAC3 disclosures from the workbook, across all "
                "banks and all available dates."
            )
            if st.button("Open Pillar 3 Official", use_container_width=True, key="home_open_page_2"):
                _set_page(OFFICIAL_PAGE)


def _render_instrument_page() -> None:
    st.header("Instrument Intelligence")

    available_banks = [b for b, db in BANK_DB_MAP.items() if (DB_DIR / db).exists()]
    if not available_banks:
        st.warning("No instrument data available. Run a pipeline first.")
        return

    selected_bank = st.selectbox(
        "Bank",
        available_banks,
        index=0,
        key="instrument_bank",
    )
    db_name = BANK_DB_MAP[selected_bank]

    if selected_bank == "Banco BPM":
        st.caption(
            "For the Pillar 3 reconciliation, outstanding amounts are interpreted as of 31-12-2024. "
            "Instrument Explorer and Audit continue to show the operational dataset amounts."
        )
    else:
        st.caption(
            f"Instrument dataset for {selected_bank}. "
            "Pillar 3 reconciliation is not yet available for this bank."
        )

    instrument_df = load_instrument_data(db_name)
    if instrument_df.empty:
        st.warning(f"Instrument data for {selected_bank} is empty.")
        return

    tab_explorer, tab_recon, tab_audit = st.tabs(
        ["Instrument Explorer", "Reconciliation", "Data Quality & Audit"]
    )
    with tab_explorer:
        explorer.render(instrument_df)
    with tab_recon:
        if selected_bank == "Banco BPM":
            reconciliation.render(instrument_df)
        else:
            st.info(f"Pillar 3 reconciliation is not yet available for {selected_bank}.")
    with tab_audit:
        audit.render(instrument_df)


def _render_official_page() -> None:
    st.header("Pillar 3 Official")

    all_countries = official_pillar3.list_official_countries()
    if not all_countries:
        st.warning("The official Pillar 3 workbook is not available.")
        return

    country_col, bank_col, logo_col, date_col = st.columns([1.4, 2.2, 1.2, 1.2])
    with country_col:
        selected_country = st.selectbox(
            "Country",
            ["All"] + all_countries,
            index=(["All"] + all_countries).index("Italy") if "Italy" in all_countries else 0,
            key="official_country",
        )

    country_filter = selected_country if selected_country != "All" else None
    banks = official_pillar3.list_official_banks(country=country_filter)
    if not banks:
        st.warning("No banks available for the selected country.")
        return

    default_bank = _default_bank(banks)
    with bank_col:
        selected_bank = st.selectbox(
            "Bank",
            banks,
            index=banks.index(default_bank) if default_bank else 0,
            key="official_bank",
        )
    with logo_col:
        _render_bank_logo_badge(selected_bank)

    bank_dates = official_pillar3.list_bank_dates(selected_bank)
    if not bank_dates:
        st.warning("No reference dates are available for the selected bank.")
        return

    with date_col:
        selected_date = st.selectbox(
            "Reference Date",
            bank_dates,
            index=len(bank_dates) - 1,
            key="official_reference_date",
        )

    coverage = official_pillar3.get_template_coverage(selected_bank, selected_date)
    coverage_labels = [name for name, is_available in coverage.items() if is_available]
    missing_labels = [name for name, is_available in coverage.items() if not is_available]
    st.caption(
        f"Coverage for {selected_bank} on {selected_date}: "
        f"{', '.join(coverage_labels) if coverage_labels else 'no official templates'}"
        + (f" | Missing: {', '.join(missing_labels)}" if missing_labels else "")
    )
    _render_cbr_research_notice(selected_bank, selected_date)

    official_tab1, official_tab2 = st.tabs(["MREL Stack Waterfall", "Pillar 3 Official"])
    with official_tab1:
        waterfall.render(selected_bank, selected_date)
    with official_tab2:
        pillar3.render(selected_bank, selected_date)


def main():
    if "current_page" not in st.session_state:
        st.session_state["current_page"] = HOME_PAGE

    _render_top_bar()
    st.divider()

    current_page = st.session_state["current_page"]
    if current_page == INSTRUMENT_PAGE:
        _render_instrument_page()
    elif current_page == OFFICIAL_PAGE:
        _render_official_page()
    else:
        _render_home()


if __name__ == "__main__":
    main()
