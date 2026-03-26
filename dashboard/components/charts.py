from __future__ import annotations
import plotly.graph_objects as go
import pandas as pd
from models.mrel_stack import MRELStack


def waterfall_chart(stack: MRELStack) -> go.Figure:
    categories = [
        "CET1", "AT1", "Tier 2", "Senior Non-Preferred",
        "Senior Preferred", "Structured Notes (Protected)",
        "Total MREL",
    ]
    values = [
        stack.cet1, stack.at1, stack.tier2,
        stack.senior_non_preferred, stack.senior_preferred,
        stack.structured_notes_protected,
        stack.total_mrel_capacity,
    ]
    measures = ["relative"] * 6 + ["total"]

    fig = go.Figure(go.Waterfall(
        name="MREL Stack",
        orientation="v",
        measure=measures,
        x=categories,
        y=values,
        textposition="outside",
        text=[f"EUR {v/1e6:,.0f}M" if v else "" for v in values],
        connector={"line": {"color": "rgb(63, 63, 63)"}},
        increasing={"marker": {"color": "#2E86AB"}},
        totals={"marker": {"color": "#1B4965"}},
    ))

    fig.update_layout(
        title="MREL Capacity Waterfall — Banco BPM (31.12.2024)",
        yaxis_title="EUR",
        showlegend=False,
        height=500,
    )
    return fig


def category_pie_chart(df: pd.DataFrame) -> go.Figure:
    cat_amounts = df.groupby("Category")["Outstanding (EUR)"].sum().reset_index()
    cat_amounts = cat_amounts[cat_amounts["Outstanding (EUR)"] > 0]

    fig = go.Figure(go.Pie(
        labels=cat_amounts["Category"],
        values=cat_amounts["Outstanding (EUR)"],
        hole=0.4,
        textinfo="label+percent",
    ))

    fig.update_layout(title="Instrument Mix by Outstanding Amount", height=400)
    return fig
