# -*- coding: utf-8 -*-
"""Componentes reutilizables de Streamlit (KPIs y gráficos simples)."""

from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st

from app.charts.theme import (
    AxisFormat,
    PLOTLY_CONFIG,
    apply_exec_style,
    apply_soft_markers,
    apply_thin_lines,
    format_axis_units,
)


def kpi(col, label: str, value: str):
    col.metric(label, value)


def line_chart(
    container,
    df: pd.DataFrame,
    x: str,
    y: str,
    title: str,
    color: str | None = None,
    *,
    x_label: str = "Periodo",
    x_tickformat: str | None = None,
    y_label: str | None = None,
    y_format: str = ",.0f",
    subtitle: str | None = None,
    source: str | None = "EGASA · Data Mart",
):
    if df.empty:
        container.info(f"Sin datos para {title}")
        return
    fig = px.line(df, x=x, y=y, color=color, title=title)
    apply_thin_lines(fig)
    apply_soft_markers(fig)
    format_axis_units(
        fig,
        x=AxisFormat(title=x_label, tickformat=x_tickformat),
        y=AxisFormat(title=y_label or title, tickformat=y_format),
    )
    apply_exec_style(fig, title=title, subtitle=subtitle or "Tendencia mensual", source=source)
    container.plotly_chart(fig, use_container_width=True, config=PLOTLY_CONFIG)


def bar_chart(
    container,
    df: pd.DataFrame,
    x: str,
    y: str,
    color: str | None = None,
    title: str | None = None,
    *,
    x_label: str = "Periodo",
    x_tickformat: str | None = None,
    y_label: str | None = None,
    y_format: str = ",.0f",
    subtitle: str | None = None,
    source: str | None = "EGASA · Data Mart",
):
    if df.empty:
        container.info(f"Sin datos para {title or 'gráfico'}")
        return
    fig = px.bar(df, x=x, y=y, color=color, title=title)
    format_axis_units(
        fig,
        x=AxisFormat(title=x_label, tickformat=x_tickformat),
        y=AxisFormat(title=y_label or (title or y), tickformat=y_format),
    )
    apply_exec_style(fig, title=title or "", subtitle=subtitle or "Distribución por periodo", source=source)
    container.plotly_chart(fig, use_container_width=True, config=PLOTLY_CONFIG)
