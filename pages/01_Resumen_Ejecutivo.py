import streamlit as st
import plotly.express as px

from app.charts.theme import AxisFormat, apply_exec_style, apply_soft_markers, apply_thin_lines, format_axis_units
from app.ui_components import kpi, line_chart, bar_chart
from utils.data import load_csv, load_centrales, metadata_token
from utils.filters import sidebar_periodo_selector, filter_by_periodo

st.set_page_config(layout="wide")
st.title("📌 Resumen Ejecutivo")

meta_token = metadata_token()
gen = load_csv("generacion_mensual.csv", meta_token=meta_token)
perfil = load_csv("balance_perfil_mensual.csv", parse_dates=["fecha_mes"], meta_token=meta_token)
seg = load_csv("balance_r_mensual.csv", parse_dates=["fecha_mes"], meta_token=meta_token)
precio = load_csv("precio_medio_mensual.csv", meta_token=meta_token)
rep = load_csv("represas_diario.csv", meta_token=meta_token)

# periodos base
from utils.filters import ensure_periodo_str
gen = ensure_periodo_str(gen, "periodo")
periodos = sorted(gen["periodo"].unique())
p_ini, p_fin = sidebar_periodo_selector(periodos, "Generación")

gen_f = filter_by_periodo(gen, "periodo", p_ini, p_fin)
perfil_f = filter_by_periodo(perfil, "periodo", p_ini, p_fin)
seg_f = filter_by_periodo(seg, "periodo", p_ini, p_fin)
precio_f = filter_by_periodo(precio, "periodo", p_ini, p_fin)

st.markdown("### 1) Indicadores clave")
colA, colB, colC, colD = st.columns(4)

mwh_mes = gen_f.groupby("periodo")["energia_mwh"].sum().iloc[-1] if not gen_f.empty else 0
kpi(colA, "Generación último mes (MWh)", f"{mwh_mes:,.0f}")

mix = None
centrales = load_centrales()
if not centrales.empty and not gen_f.empty:
    tmp = gen_f.merge(centrales, on="central_id", how="left")
    last_p = gen_f["periodo"].max()
    mix = tmp[tmp["periodo"] == last_p].groupby("tipo")["energia_mwh"].sum()
    if not mix.empty:
        pct_h = (mix.get("HIDRO", 0) / mix.sum()) * 100 if mix.sum() else 0
        kpi(colB, "Mix Hidro (%)", f"{pct_h:,.1f}%")

venta_total = seg_f[seg_f["segmento"].str.upper().eq("TOTAL")].groupby("periodo")["energia_mwh"].sum()
kpi(colC, "Ventas último mes (MWh)", f"{(venta_total.iloc[-1] if len(venta_total) else 0):,.0f}")

if not precio_f.empty and "precio_medio_soles_mwh" in precio_f.columns:
    kpi(colD, "Precio medio último mes (S/MWh)", f"{precio_f.sort_values('periodo')['precio_medio_soles_mwh'].iloc[-1]:,.2f}")

st.divider()
st.markdown("### 2) Tendencias (últimos meses del rango)")

c1, c2 = st.columns(2)

if not gen_f.empty:
    s = gen_f.groupby("periodo")["energia_mwh"].sum().reset_index()
    line_chart(
        c1,
        s,
        x="periodo",
        y="energia_mwh",
        title="Generación total",
        y_label="Energía (MWh)",
        y_format=",.0f",
        subtitle="Producción mensual consolidada",
    )

if mix is not None and not mix.empty:
    fig = px.pie(values=mix.values, names=mix.index, title="Mix Hidro vs Térmica (último mes)")
    fig.update_traces(textposition="inside", textinfo="percent+label", pull=[0.03] * len(mix))
    apply_exec_style(
        fig,
        title="Mix Hidro vs Térmica (último mes)",
        subtitle="Composición de generación por tecnología",
        source="EGASA · Data Mart",
        hovermode="closest",
    )
    c2.plotly_chart(fig, use_container_width=True)

c3, c4 = st.columns(2)

if not seg_f.empty:
    s2 = seg_f.groupby(["periodo", "segmento"])["energia_mwh"].sum().reset_index()
    bar_chart(
        c3,
        s2,
        x="periodo",
        y="energia_mwh",
        color="segmento",
        title="Ventas por segmento",
        y_label="Energía (MWh)",
        y_format=",.0f",
        subtitle="Participación mensual por segmento",
    )

if not rep.empty and "pct_llenado" in rep.columns:
    rep_sorted = rep.sort_values("pct_llenado", ascending=False)
    fig = px.bar(rep_sorted, x="reservorio", y="pct_llenado", title="Estado diario represas")
    format_axis_units(
        fig,
        x=AxisFormat(title="Reservorio"),
        y=AxisFormat(title="Nivel de llenado (%)", tickformat=".1%"),
    )
    apply_exec_style(
        fig,
        title="Estado diario represas",
        subtitle="Porcentaje de llenado reportado",
        source="EGASA · Data Mart",
        hovermode="x unified",
    )
    c4.plotly_chart(fig, use_container_width=True)
