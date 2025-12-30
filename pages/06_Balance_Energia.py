import pandas as pd
import plotly.express as px
import streamlit as st

from app.charts.theme import AxisFormat, apply_exec_style, apply_thin_lines, apply_unified_hover, format_axis_units
from utils.data import load_csv
from utils.filters import ensure_periodo_str, filter_by_periodo, sidebar_periodo_selector

st.set_page_config(layout="wide")
st.title("⚖️ Balance de Energía (Perfil + R)")

perfil = load_csv("balance_perfil_mensual.csv", parse_dates=["fecha_mes"])
r = load_csv("balance_r_mensual.csv", parse_dates=["fecha_mes"])

if perfil.empty and r.empty:
    st.warning("No hay balance_perfil_mensual.csv / balance_r_mensual.csv.")
    st.stop()

if not perfil.empty:
    perfil = ensure_periodo_str(perfil, "periodo")
if not r.empty:
    r = ensure_periodo_str(r, "periodo")

periodos = []
if not perfil.empty:
    periodos = sorted(perfil["periodo"].unique())
elif not r.empty:
    periodos = sorted(r["periodo"].unique())

p_ini, p_fin = sidebar_periodo_selector(periodos, "Periodo Balance")

perfil_f = filter_by_periodo(perfil, "periodo", p_ini, p_fin) if not perfil.empty else perfil
r_f = filter_by_periodo(r, "periodo", p_ini, p_fin) if not r.empty else r

st.markdown("## 1) Perfil (componentes del balance)")

if not perfil_f.empty:
    # conceptos que suelen ser “restas”
    negativos = {"CONSUMOS AUX.", "PERDIDAS"}
    df = perfil_f.copy()
    df["signo"] = df["concepto"].str.upper().apply(lambda x: -1 if x in negativos else 1)
    df["energia_mwh_signed"] = df["energia_mwh"] * df["signo"]

    fig = px.bar(
        df,
        x="periodo",
        y="energia_mwh_signed",
        color="concepto",
        title="Balance mensual (MWh) — positivos vs negativos",
    )
    apply_unified_hover(fig, fmt=":,.0f", units="MWh")
    format_axis_units(
        fig,
        x=AxisFormat(title="Periodo"),
        y=AxisFormat(title="Energía (MWh)", tickformat=",.0f"),
    )
    apply_exec_style(
        fig,
        title="Balance mensual (positivos vs negativos)",
        subtitle="Componentes del perfil (MWh)",
        source="EGASA · Data Mart",
    )
    st.plotly_chart(fig, use_container_width=True)

    # líneas clave si existen
    claves = df[df["concepto"].str.upper().isin({"ENERGIA DISPONIBLE", "VENTA DE ENERGIA"})].copy()
    if not claves.empty:
        fig2 = px.line(claves, x="periodo", y="energia_mwh", color="concepto", title="Líneas clave (MWh)")
        apply_thin_lines(fig2)
        apply_unified_hover(fig2, fmt=":,.0f", units="MWh")
        format_axis_units(
            fig2,
            x=AxisFormat(title="Periodo"),
            y=AxisFormat(title="Energía (MWh)", tickformat=",.0f"),
        )
        apply_exec_style(
            fig2,
            title="Líneas clave del balance",
            subtitle="Energía disponible vs venta de energía",
            source="EGASA · Data Mart",
        )
        st.plotly_chart(fig2, use_container_width=True)
else:
    st.info("Sin datos de Perfil en el rango seleccionado.")

st.markdown("## 2) R (ventas por segmento)")

if not r_f.empty:
    df = r_f.copy()
    fig = px.bar(df, x="periodo", y="energia_mwh", color="segmento", title="Ventas por segmento (MWh)")
    apply_unified_hover(fig, fmt=":,.0f", units="MWh")
    format_axis_units(
        fig,
        x=AxisFormat(title="Periodo"),
        y=AxisFormat(title="Energía (MWh)", tickformat=",.0f"),
    )
    apply_exec_style(
        fig,
        title="Ventas por segmento",
        subtitle="Mercado R mensual",
        source="EGASA · Data Mart",
    )
    st.plotly_chart(fig, use_container_width=True)

    total = df[df["segmento"].str.upper().eq("TOTAL")].groupby("periodo")["energia_mwh"].sum().reset_index()
    if not total.empty:
        fig_total = px.line(total, x="periodo", y="energia_mwh", title="Ventas Total (MWh)")
        apply_thin_lines(fig_total)
        apply_unified_hover(fig_total, fmt=":,.0f", units="MWh")
        format_axis_units(
            fig_total,
            x=AxisFormat(title="Periodo"),
            y=AxisFormat(title="Energía (MWh)", tickformat=",.0f"),
        )
        apply_exec_style(
            fig_total,
            title="Ventas totales mensuales",
            subtitle="Energía vendida (MWh)",
            source="EGASA · Data Mart",
        )
        st.plotly_chart(fig_total, use_container_width=True)
else:
    st.info("Sin datos de R en el rango seleccionado.")
