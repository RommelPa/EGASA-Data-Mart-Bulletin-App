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
    short_spanish_date,
)
from utils.data import load_generacion_15min, list_yyyymm_15min, metadata_token

st.set_page_config(layout="wide")
st.title("⏱️ Generación 15-min (2025)")

meta_token = metadata_token()
yyyymm_list = list_yyyymm_15min(meta_token=meta_token)
if not yyyymm_list:
    st.warning("No hay archivos generacion_15min_YYYYMM.csv en data_mart.")
    st.stop()

yyyymm = st.sidebar.selectbox("Selecciona YYYYMM", yyyymm_list)
df = load_generacion_15min(yyyymm, meta_token=meta_token)

if df.empty:
    st.warning("No hay datos 15-min para el periodo seleccionado.")
    st.stop()

# seguridad extra
df["fecha_hora"] = pd.to_datetime(df["fecha_hora"], errors="coerce")
df = df.dropna(subset=["fecha_hora"]).copy()

df["fecha"] = df["fecha_hora"].dt.date

centrales = sorted([c for c in df["central"].dropna().unique() if str(c).strip() != ""])
central = st.sidebar.selectbox("Central", ["(Todas)"] + centrales)

if central != "(Todas)":
    df = df[df["central"] == central].copy()

dias = sorted(df["fecha"].unique())
dia = st.sidebar.selectbox("Día", dias, index=max(0, len(dias) - 1))

df_dia = df[df["fecha"] == dia].copy()

st.markdown(f"### Perfil 15-min — **{dia}**  |  **{central}**")

fig = px.line(
    df_dia,
    x="fecha_hora",
    y="energia_mwh",
    color="unidad",
    title="Perfil 15-min por unidad (MWh por intervalo)",
)
apply_thin_lines(fig)
apply_soft_markers(fig)
apply_unified_hover(fig, fmt=":,.2f", units="MWh")
format_axis_units(
    fig,
    x=AxisFormat(title="Hora", tickformat="%H:%M"),
    y=AxisFormat(title="Energía (MWh)", tickformat=",.2f"),
)
apply_exec_style(
    fig,
    title="Perfil 15-min por unidad",
    subtitle=f"Energía por intervalo — {short_spanish_date(df_dia['fecha_hora'].iloc[0])}",
    source="EGASA · Data Mart",
)
st.plotly_chart(fig, use_container_width=True, config=PLOTLY_CONFIG)

st.markdown("### Agregado horario")
df_dia["hora"] = df_dia["fecha_hora"].dt.floor("H")
h = df_dia.groupby("hora")["energia_mwh"].sum().reset_index()
fig_h = px.line(h, x="hora", y="energia_mwh", title="Energía por hora (MWh)")
apply_thin_lines(fig_h)
apply_soft_markers(fig_h)
apply_unified_hover(fig_h, fmt=":,.2f", units="MWh")
format_axis_units(
    fig_h,
    x=AxisFormat(title="Hora", tickformat="%H:%M"),
    y=AxisFormat(title="Energía (MWh)", tickformat=",.2f"),
)
apply_exec_style(fig_h, title="Energía por hora", subtitle="Promedio por intervalo horario", source="EGASA · Data Mart")
st.plotly_chart(fig_h, use_container_width=True, config=PLOTLY_CONFIG)

st.markdown("### Comparación Día vs Día")
dia2 = st.sidebar.selectbox("Comparar con", dias, index=max(0, len(dias) - 2))

d1 = df[df["fecha"] == dia].copy()
d2 = df[df["fecha"] == dia2].copy()

for d in (d1, d2):
    d["hora"] = d["fecha_hora"].dt.floor("H")

h1 = d1.groupby("hora")["energia_mwh"].sum().reset_index()
h2 = d2.groupby("hora")["energia_mwh"].sum().reset_index()

h1["dia"] = str(dia)
h2["dia"] = str(dia2)

comp = pd.concat([h1, h2], ignore_index=True).rename(columns={"hora": "t"})
fig_comp = px.line(comp, x="t", y="energia_mwh", color="dia", title="Comparación por hora")
apply_thin_lines(fig_comp)
apply_soft_markers(fig_comp)
apply_unified_hover(fig_comp, fmt=":,.2f", units="MWh")
format_axis_units(
    fig_comp,
    x=AxisFormat(title="Hora", tickformat="%H:%M"),
    y=AxisFormat(title="Energía (MWh)", tickformat=",.2f"),
)
apply_exec_style(
    fig_comp,
    title="Comparación por hora",
    subtitle="Energía consolidada por día",
    source="EGASA · Data Mart",
)
st.plotly_chart(fig_comp, use_container_width=True, config=PLOTLY_CONFIG)
