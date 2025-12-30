import pandas as pd
import plotly.express as px
import streamlit as st

from app.charts.theme import (
    AxisFormat,
    apply_exec_style,
    apply_soft_markers,
    apply_thin_lines,
    apply_unified_hover,
    format_axis_units,
)
from utils.data import load_csv
from utils.filters import sidebar_periodo_selector, filter_by_periodo, ensure_periodo_str

st.set_page_config(layout="wide")
st.title("💧 Hidrología mensual")

vol = load_csv("hidro_volumen_mensual.csv")
cau = load_csv("hidro_caudal_mensual.csv")

if vol.empty and cau.empty:
    st.warning("No hay datos de hidrología en data_mart.")
    st.stop()

# volumen: reservorio, anio, mes, volumen_000m3, periodo
if not vol.empty and "periodo" in vol.columns:
    vol = ensure_periodo_str(vol, "periodo")
# caudal: estacion, anio, mes, caudal_m3s, periodo
if not cau.empty and "periodo" in cau.columns:
    cau = ensure_periodo_str(cau, "periodo")

periodos = []
if not vol.empty and "periodo" in vol.columns:
    periodos = sorted(vol["periodo"].unique())
elif not cau.empty and "periodo" in cau.columns:
    periodos = sorted(cau["periodo"].unique())

p_ini, p_fin = sidebar_periodo_selector(periodos, "Periodo Hidro")

vol_f = filter_by_periodo(vol, "periodo", p_ini, p_fin) if not vol.empty else vol
cau_f = filter_by_periodo(cau, "periodo", p_ini, p_fin) if not cau.empty else cau

st.markdown("## 1) Volúmenes mensuales (000 m³)")

if not vol_f.empty:
    reservorios = sorted(vol_f["reservorio"].dropna().unique())
    sel = st.sidebar.multiselect("Reservorios", reservorios, default=reservorios[:3] if len(reservorios) >= 3 else reservorios)

    df = vol_f[vol_f["reservorio"].isin(sel)].copy()
    df["fecha_mes"] = pd.to_datetime(df["periodo"] + "01", format="%Y%m%d", errors="coerce")
    df["volumen_mm3"] = df["volumen_000m3"] / 1_000

    fig = px.line(df, x="fecha_mes", y="volumen_mm3", color="reservorio", title="Volumen mensual por reservorio")
    apply_thin_lines(fig)
    apply_soft_markers(fig)
    apply_unified_hover(fig, fmt=":,.2f", units="millones de m³")
    format_axis_units(
        fig,
        x=AxisFormat(title="Fecha", tickformat="%d %b %Y"),
        y=AxisFormat(title="Volumen útil (millones de m³)", tickformat=",.2f"),
    )
    apply_exec_style(
        fig,
        title="Volumen útil por reservorio",
        subtitle="Serie mensual — unidades en millones de m³",
        source="EGASA · Data Mart",
    )
    st.plotly_chart(fig, use_container_width=True)

    st.markdown("### Comparativo YoY (mismo mes, por año)")
    df["anio"] = df["periodo"].str[:4].astype(int)
    df["mes"] = df["periodo"].str[4:6].astype(int)
    mes_sel = st.selectbox("Mes", sorted(df["mes"].unique()))
    yoy = (
        df[df["mes"] == mes_sel]
        .groupby(["anio", "reservorio"])["volumen_mm3"]
        .mean()
        .reset_index()
    )
    fig_yoy = px.line(yoy, x="anio", y="volumen_mm3", color="reservorio", title=f"YoY Volumen (mes={mes_sel})")
    apply_thin_lines(fig_yoy)
    apply_soft_markers(fig_yoy)
    apply_unified_hover(fig_yoy, fmt=":,.2f", units="millones de m³")
    format_axis_units(
        fig_yoy,
        x=AxisFormat(title="Año", tickformat="d"),
        y=AxisFormat(title="Volumen útil (millones de m³)", tickformat=",.2f"),
    )
    apply_exec_style(
        fig_yoy,
        title="Comparativo YoY de volumen útil",
        subtitle=f"Mes seleccionado: {mes_sel}",
        source="EGASA · Data Mart",
    )
    st.plotly_chart(fig_yoy, use_container_width=True)
else:
    st.info("No hay datos de volumen en el rango seleccionado.")

st.markdown("## 2) Caudal mensual (m³/s)")

if not cau_f.empty:
    df = cau_f.copy()
    df["fecha_mes"] = pd.to_datetime(df["periodo"] + "01", format="%Y%m%d", errors="coerce")
    fig = px.line(df, x="fecha_mes", y="caudal_m3s", color="estacion", title="Caudal mensual")
    apply_thin_lines(fig)
    apply_soft_markers(fig)
    apply_unified_hover(fig, fmt=":,.2f", units="m³/s")
    format_axis_units(
        fig,
        x=AxisFormat(title="Fecha", tickformat="%d %b %Y"),
        y=AxisFormat(title="Caudal (m³/s)", tickformat=",.2f"),
    )
    apply_exec_style(
        fig,
        title="Caudal mensual por estación",
        subtitle="Serie mensual — m³/s",
        source="EGASA · Data Mart",
    )
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("No hay datos de caudal en el rango seleccionado.")
