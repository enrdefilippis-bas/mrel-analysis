from __future__ import annotations
import streamlit as st
import pandas as pd
from models.mrel_stack import MRELStack
from models.instrument import Instrument, InstrumentCategory, CouponType
from datetime import date
from dashboard.components.charts import waterfall_chart, category_pie_chart


def _df_to_instruments(df: pd.DataFrame) -> list[Instrument]:
    instruments = []
    for _, row in df.iterrows():
        try:
            cat = InstrumentCategory(row["Category"])
        except ValueError:
            cat = InstrumentCategory.UNKNOWN
        try:
            ct = CouponType(row.get("Coupon Type", "Unknown"))
        except ValueError:
            ct = CouponType.UNKNOWN

        mat_date = None
        if pd.notna(row.get("Maturity Date")):
            try:
                mat_date = pd.to_datetime(row["Maturity Date"]).date()
            except Exception:
                pass

        issue_date = None
        if pd.notna(row.get("Issue Date")):
            try:
                issue_date = pd.to_datetime(row["Issue Date"]).date()
            except Exception:
                pass

        instruments.append(Instrument(
            isin=row["ISIN"],
            name=row.get("Name", ""),
            category=cat,
            issue_date=issue_date,
            maturity_date=mat_date,
            coupon_type=ct,
            outstanding_amount=row.get("Outstanding (EUR)"),
            currency="EUR",
            crr2_rank=row.get("CRR2 Rank"),
            mrel_eligible=row.get("MREL Eligible"),
        ))
    return instruments


def render(df: pd.DataFrame, ref_date: date) -> None:
    st.header("MREL Stack Waterfall")

    instruments = _df_to_instruments(df)
    stack = MRELStack.from_instruments(instruments, ref_date)

    col1, col2, col3 = st.columns(3)
    col1.metric("Subordination Capacity", f"EUR {stack.subordination_capacity:,.0f}")
    col2.metric("Total MREL Capacity", f"EUR {stack.total_mrel_capacity:,.0f}")
    col3.metric("Total Excluded", f"EUR {stack.total_excluded:,.0f}")

    fig = waterfall_chart(stack)
    st.plotly_chart(fig, use_container_width=True)

    fig2 = category_pie_chart(df)
    st.plotly_chart(fig2, use_container_width=True)

    st.subheader("Detailed Breakdown")
    breakdown = pd.DataFrame([
        {"Component": k, "Amount (EUR)": v}
        for k, v in stack.to_dict().items()
    ])
    st.dataframe(breakdown, use_container_width=True, hide_index=True)
