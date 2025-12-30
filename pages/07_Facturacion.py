import pandas as pd
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
from utils.data import load_csv
from utils.filters import ensure_periodo_str, filter_by_periodo, sidebar_periodo_selector

st.set_page_config(layout="wide")
st.title("💰 Facturación / Comercial")

ventas_mwh = load_csv("ventas_mensual_mwh.csv")
ventas_s = load_csv("ventas_mensual_soles.csv")  # si existe en tu mart
ingresos = load_csv("ingresos_mensual.csv")      # si existe en tu mart
precio = load_csv("precio_medio_mensual.csv")

if ventas_mwh.empty and precio.empty and ventas_s.empty and ingresos.empty:
    st.warning("No hay datasets comerciales en data_mart.")
    st.stop()

if not ventas_mwh.empty:
    ventas_mwh = ensure_periodo_str(ventas_mwh, "periodo")
if not ventas_s.empty and "periodo" in ventas_s.columns:
    ventas_s = ensure_periodo_str(ventas_s, "periodo")
if not ingresos.empty and "periodo" in ingresos.columns:
    ingresos = ensure_periodo_str(ingresos, "periodo")
if not precio.empty:
    precio = ensure_periodo_str(precio, "periodo")

# periodos base: preferimos ventas_mwh
periodos = []
for df in [ventas_mwh, precio, ventas_s, ingresos]:
    if not df.empty and "periodo" in df.columns:
        periodos = sorted(df["periodo"].unique())
        break

p_ini, p_fin = sidebar_periodo_selector(periodos, "Periodo Comercial")

ventas_mwh_f = filter_by_periodo(ventas_mwh, "periodo", p_ini, p_fin) if not ventas_mwh.empty else ventas_mwh
precio_f = filter_by_periodo(precio, "periodo", p_ini, p_fin) if not precio.empty else precio
ventas_s_f = filter_by_periodo(ventas_s, "periodo", p_ini, p_fin) if not ventas_s.empty else ventas_s
ingresos_f = filter_by_periodo(ingresos, "periodo", p_ini, p_fin) if not ingresos.empty else ingresos

st.markdown("## 1) Ventas de energía (MWh)")

if not ventas_mwh_f.empty:
    # KPIs
    total_mes = ventas_mwh_f.groupby("periodo")["mwh"].sum()
    col1, col2 = st.columns(2)
    col1.metric("Ventas último mes (MWh)", f"{(total_mes.iloc[-1] if len(total_mes) else 0):,.0f}")
    col2.metric("Clientes únicos", f"{ventas_mwh_f['cliente'].nunique():,}")

    # tendencia
    s = ventas_mwh_f.groupby("periodo")["mwh"].sum().reset_index()
    fig = px.line(s, x="periodo", y="mwh", title="Ventas total mensual (MWh)")
    apply_thin_lines(fig)
    apply_soft_markers(fig)
    apply_unified_hover(fig, fmt=":,.0f", units="MWh")
    format_axis_units(
        fig,
        x=AxisFormat(title="Periodo"),
        y=AxisFormat(title="Ventas (MWh)", tickformat=",.0f"),
    )
    apply_exec_style(
        fig,
        title="Ventas totales mensuales",
        subtitle="Energía vendida (MWh)",
        source="EGASA · Data Mart",
    )
    st.plotly_chart(fig, use_container_width=True, config=PLOTLY_CONFIG)

    # top clientes
    top_n = st.sidebar.slider("Top N clientes", 5, 30, 10)
    top = ventas_mwh_f.groupby("cliente")["mwh"].sum().sort_values(ascending=False).head(top_n).reset_index()
    fig_top = px.bar(top, x="cliente", y="mwh", title=f"Top {top_n} clientes (MWh)")
    apply_unified_hover(fig_top, fmt=":,.0f", units="MWh")
    format_axis_units(
        fig_top,
        x=AxisFormat(title="Cliente"),
        y=AxisFormat(title="Ventas (MWh)", tickformat=",.0f"),
    )
    apply_exec_style(
        fig_top,
        title=f"Top {top_n} clientes por energía vendida",
        subtitle="Ordenado por MWh acumulados",
        source="EGASA · Data Mart",
    )
    st.plotly_chart(fig_top, use_container_width=True, config=PLOTLY_CONFIG)
else:
    st.info("No hay ventas_mensual_mwh en el rango.")

st.markdown("## 2) Precio medio (S/MWh)")

if not precio_f.empty:
    # promedio mensual (promedio simple; si quieres ponderado por MWh lo hacemos luego)
    s = precio_f.groupby("periodo")["precio_medio_soles_mwh"].mean().reset_index()
    fig_precio = px.line(s, x="periodo", y="precio_medio_soles_mwh", title="Precio medio mensual (S/MWh)")
    apply_thin_lines(fig_precio)
    apply_soft_markers(fig_precio)
    apply_unified_hover(fig_precio, fmt=":,.2f", units="S/MWh")
    format_axis_units(
        fig_precio,
        x=AxisFormat(title="Periodo"),
        y=AxisFormat(title="Precio medio (S/MWh)", tickformat=",.2f"),
    )
    apply_exec_style(
        fig_precio,
        title="Precio medio mensual",
        subtitle="Soles por MWh",
        source="EGASA · Data Mart",
    )
    st.plotly_chart(fig_precio, use_container_width=True, config=PLOTLY_CONFIG)

    # dispersión por cliente (último periodo)
    last_p = precio_f["periodo"].max()
    d = precio_f[precio_f["periodo"] == last_p].copy()
    if not d.empty:
        fig_disp = px.box(d, x="cliente", y="precio_medio_soles_mwh", title=f"Distribución por cliente ({last_p})")
        apply_unified_hover(fig_disp, fmt=":,.2f", units="S/MWh")
        format_axis_units(
            fig_disp,
            x=AxisFormat(title="Cliente"),
            y=AxisFormat(title="Precio medio (S/MWh)", tickformat=",.2f"),
        )
        apply_exec_style(
            fig_disp,
            title=f"Distribución de precio medio — {last_p}",
            subtitle="Precio facturado por cliente",
            source="EGASA · Data Mart",
            hovermode="closest",
        )
        st.plotly_chart(fig_disp, use_container_width=True, config=PLOTLY_CONFIG)
else:
    st.info("No hay precio_medio_mensual en el rango.")

st.markdown("## 3) Ventas valorizadas / Ingresos (si existen)")
if not ventas_s_f.empty:
    st.dataframe(ventas_s_f.head(50), use_container_width=True)
if not ingresos_f.empty:
    st.dataframe(ingresos_f.head(50), use_container_width=True)
