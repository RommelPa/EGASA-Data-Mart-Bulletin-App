import plotly.express as px
import streamlit as st

from app.charts.theme import (
    AxisFormat,
    PLOTLY_CONFIG,
    apply_exec_style,
    apply_soft_markers,
    apply_thin_lines,
    apply_unified_hover,
    format_axis_units,
)
from utils.data import load_csv, load_centrales
from utils.filters import sidebar_periodo_selector, filter_by_periodo

st.set_page_config(layout="wide")
st.title("⚡ Generación mensual (2010–2025)")

gen = load_csv("generacion_mensual.csv")
centrales = load_centrales()

periodos = sorted(gen["periodo"].astype(str).unique()) if not gen.empty else []
p_ini, p_fin = sidebar_periodo_selector(periodos)

gen = filter_by_periodo(gen, "periodo", p_ini, p_fin)

if gen.empty:
    st.warning("No hay datos de generación mensual.")
    st.stop()

st.markdown("### 1) Generación total")
total = gen.groupby("periodo")["energia_mwh"].sum().reset_index()
fig_total = px.line(total, x="periodo", y="energia_mwh", title="Total mensual (MWh)")
apply_thin_lines(fig_total)
apply_soft_markers(fig_total)
apply_unified_hover(fig_total, fmt=":,.0f", units="MWh")
format_axis_units(
    fig_total,
    x=AxisFormat(title="Periodo"),
    y=AxisFormat(title="Energía (MWh)", tickformat=",.0f"),
)
apply_exec_style(
    fig_total,
    title="Generación total mensual",
    subtitle="Energia generada por periodo (MWh)",
    source="EGASA · Data Mart",
)
st.plotly_chart(fig_total, use_container_width=True, config=PLOTLY_CONFIG)

st.markdown("### 2) Por central (Top N)")
top_n = st.sidebar.slider("Top N centrales", 3, 12, 9)
byc = gen.groupby(["periodo", "central"])["energia_mwh"].sum().reset_index()
rank = byc.groupby("central")["energia_mwh"].sum().sort_values(ascending=False).head(top_n).index
byc = byc[byc["central"].isin(rank)]
fig_top = px.bar(byc, x="periodo", y="energia_mwh", color="central", title="Top centrales (MWh)")
apply_unified_hover(fig_top, fmt=":,.0f", units="MWh")
format_axis_units(
    fig_top,
    x=AxisFormat(title="Periodo"),
    y=AxisFormat(title="Energía (MWh)", tickformat=",.0f"),
)
apply_exec_style(
    fig_top,
    title=f"Top {top_n} centrales (MWh)",
    subtitle="Ordenadas por aporte acumulado",
    source="EGASA · Data Mart",
)
st.plotly_chart(fig_top, use_container_width=True, config=PLOTLY_CONFIG)

st.markdown("### 3) Mix Hidro vs Térmica")
if not centrales.empty:
    tmp = gen.merge(centrales, on="central_id", how="left")
    mix = tmp.groupby(["periodo", "tipo"])["energia_mwh"].sum().reset_index()
    fig_mix = px.bar(mix, x="periodo", y="energia_mwh", color="tipo", title="Mix mensual")
    apply_unified_hover(fig_mix, fmt=":,.0f", units="MWh")
    format_axis_units(
        fig_mix,
        x=AxisFormat(title="Periodo"),
        y=AxisFormat(title="Energía (MWh)", tickformat=",.0f"),
    )
    apply_exec_style(
        fig_mix,
        title="Mix mensual por tecnología",
        subtitle="Hidro vs Térmica",
        source="EGASA · Data Mart",
    )
    st.plotly_chart(fig_mix, use_container_width=True, config=PLOTLY_CONFIG)

st.markdown("### 4) Estacionalidad (heatmap)")
gen["anio"] = gen["periodo"].astype(str).str[:4].astype(int)
gen["mes"] = gen["periodo"].astype(str).str[4:6].astype(int)
hm = gen.groupby(["anio", "mes"])["energia_mwh"].sum().reset_index()
fig = px.density_heatmap(hm, x="mes", y="anio", z="energia_mwh", title="Heatmap estacionalidad (MWh)")
format_axis_units(
    fig,
    x=AxisFormat(title="Mes"),
    y=AxisFormat(title="Año", tickformat="d"),
)
apply_exec_style(
    fig,
    title="Estacionalidad de generación",
    subtitle="Mapa de calor mensual (MWh)",
    source="EGASA · Data Mart",
    hovermode="closest",
)
st.plotly_chart(fig, use_container_width=True, config=PLOTLY_CONFIG)
