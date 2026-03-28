from __future__ import annotations
import plotly.graph_objects as go
import pandas as pd
from models.mrel_stack import MRELStack


def _format_eur_short(value: float) -> str:
    return f"EUR {value / 1e6:,.0f}M".replace(",", ".")


def _stack_to_waterfall_items(stack: MRELStack) -> list[dict[str, object]]:
    return [
        {"label": "CET1", "value": stack.cet1, "measure": "relative"},
        {"label": "AT1", "value": stack.at1, "measure": "relative"},
        {"label": "Tier 2", "value": stack.tier2, "measure": "relative"},
        {"label": "Senior Non-Preferred", "value": stack.senior_non_preferred, "measure": "relative"},
        {"label": "Senior Preferred", "value": stack.senior_preferred, "measure": "relative"},
        {"label": "Structured Notes (Protected)", "value": stack.structured_notes_protected, "measure": "relative"},
        {"label": "Total MREL", "value": stack.total_mrel_capacity, "measure": "total"},
    ]


def waterfall_chart(
    data: MRELStack | list[dict[str, object]] | tuple[dict[str, object], ...],
    title: str = "MREL Capacity Waterfall",
    requirement_lines: list[dict[str, object]] | tuple[dict[str, object], ...] | None = None,
) -> go.Figure:
    items = _stack_to_waterfall_items(data) if isinstance(data, MRELStack) else list(data)
    categories = [str(item["label"]) for item in items]
    values = [float(item["value"]) for item in items]
    measures = [str(item.get("measure", "relative")) for item in items]

    fig = go.Figure(go.Waterfall(
        name="MREL Stack",
        orientation="v",
        measure=measures,
        x=categories,
        y=values,
        textposition="outside",
        text=[f"EUR {v/1e6:,.0f}M".replace(",", ".") if v else "" for v in values],
        connector={"line": {"color": "rgb(63, 63, 63)"}},
        increasing={"marker": {"color": "#2E86AB"}},
        decreasing={"marker": {"color": "#D95D39"}},
        totals={"marker": {"color": "#1B4965"}},
    ))

    for line in requirement_lines or []:
        line_value = line.get("value")
        if line_value is None:
            continue
        annotation = line.get("annotation") or str(line.get("label", "Requirement"))
        fig.add_hline(
            y=float(line_value),
            line_dash=str(line.get("dash", "dash")),
            line_color=str(line.get("color", "red")),
            line_width=2,
            annotation_text=f"{annotation} = {_format_eur_short(float(line_value))}",
            annotation_position="top left",
        )

    fig.update_layout(
        title=title,
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
