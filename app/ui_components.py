# -*- coding: utf-8 -*-
"""Componentes reutilizables de Streamlit (KPIs y gráficos simples)."""

from __future__ import annotations

import streamlit as st
import plotly.express as px
import pandas as pd


def kpi(col, label: str, value: str):
    col.metric(label, value)


def line_chart(container, df: pd.DataFrame, x: str, y: str, title: str, color: str | None = None):
    if df.empty:
        container.info(f"Sin datos para {title}")
        return
    fig = px.line(df, x=x, y=y, color=color, title=title)
    container.plotly_chart(fig, use_container_width=True)


def bar_chart(container, df: pd.DataFrame, x: str, y: str, color: str | None = None, title: str | None = None):
    if df.empty:
        container.info(f"Sin datos para {title or 'gráfico'}")
        return
    fig = px.bar(df, x=x, y=y, color=color, title=title)
    container.plotly_chart(fig, use_container_width=True)
