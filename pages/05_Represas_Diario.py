import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from app.charts.theme import (
    AxisFormat,
    EXEC_THEME,
    PLOTLY_CONFIG,
    apply_exec_style,
    apply_soft_markers,
    apply_thin_lines,
    apply_unified_hover,
    format_axis_units,
    short_spanish_date,
)
from utils.data import load_csv

st.set_page_config(layout="wide")
st.title("🏞️ Estado diario de represas")

rep = load_csv("represas_diario.csv")

if rep.empty:
    st.warning("No hay represas_diario.csv en data_mart.")
    st.stop()

# asegurar fecha
rep["fecha"] = pd.to_datetime(rep["fecha"], errors="coerce")
rep = rep.dropna(subset=["fecha"]).copy()

fechas = sorted(rep["fecha"].dt.date.unique())
fecha_sel = st.sidebar.selectbox("Fecha", fechas, index=max(0, len(fechas) - 1))

df = rep[rep["fecha"].dt.date == fecha_sel].copy()

st.markdown(f"### Reporte: **{fecha_sel}**")

# KPI simples
col1, col2, col3 = st.columns(3)
if "pct_llenado" in df.columns:
    col1.metric("% llenado promedio", f"{df['pct_llenado'].mean():,.2f}")
col2.metric("Reservorios reportados", f"{df['reservorio'].nunique():,}")
if "volumen_actual" in df.columns and df["volumen_actual"].notna().any():
    col3.metric("Volumen actual (suma)", f"{df['volumen_actual'].sum():,.2f}")

st.divider()

# Bar % llenado
if "pct_llenado" in df.columns:
    st.markdown("### % de llenado por reservorio")
    fig = px.bar(df.sort_values("pct_llenado", ascending=False), x="reservorio", y="pct_llenado", title="% Llenado")
    apply_unified_hover(fig, fmt=":.1%", units="")
    format_axis_units(
        fig,
        x=AxisFormat(title="Reservorio"),
        y=AxisFormat(title="Nivel de llenado (%)", tickformat=".1%"),
    )
    apply_exec_style(
        fig,
        title="% de llenado por reservorio",
        subtitle=f"Corte: {fecha_sel}",
        source="EGASA · Data Mart",
    )
    st.plotly_chart(fig, use_container_width=True, config=PLOTLY_CONFIG)

# Tabla operativa
st.markdown("### Tabla operativa")
st.dataframe(df, use_container_width=True)

st.markdown("### Evolución volumen útil y nivel (doble eje)")
reservorios_hist = sorted(rep["reservorio"].dropna().unique())
reservorio_sel = st.selectbox("Reservorio para evolución histórica", reservorios_hist)
hist = rep[rep["reservorio"] == reservorio_sel].copy()

if not hist.empty:
    hist["fecha"] = pd.to_datetime(hist["fecha"], errors="coerce")
    hist = hist.dropna(subset=["fecha"])
    hist = hist.sort_values("fecha")
    hist["volumen_mm3"] = hist["volumen_actual"]

    nivel_label = "Nivel (m s. n. m.)"
    nivel_col = None
    for cand in ("nivel_msnm", "cota_msnm", "cota"):
        if cand in hist.columns:
            nivel_col = cand
            break
    if nivel_col is None and "pct_llenado" in hist.columns:
        nivel_col = "pct_llenado"
        hist[nivel_col] = hist[nivel_col] * (100 if hist[nivel_col].max() <= 1 else 1)
        nivel_label = "Nivel (% de llenado)"

    fig_dual = go.Figure()
    fig_dual.add_bar(
        x=hist["fecha"],
        y=hist["volumen_mm3"],
        name="Volumen útil",
        marker_color=EXEC_THEME["primary"],
        opacity=0.9,
        hovertemplate="%{x|%d %b %Y}<br>Volumen útil: %{y:,.2f} millones de m³<extra></extra>",
    )

    if nivel_col:
        fig_dual.add_trace(
            go.Scatter(
                x=hist["fecha"],
                y=hist[nivel_col],
                name="Nivel",
                mode="lines+markers",
                marker={"color": EXEC_THEME["secondary"], "size": 8},
                line={"width": 2.2},
                hovertemplate="%{x|%d %b %Y}<br>Nivel: %{y:,.2f}<extra></extra>",
                yaxis="y2",
            )
        )

    y2_axis = AxisFormat(title=nivel_label, tickformat=",.2f") if nivel_col else None
    format_axis_units(
        fig_dual,
        x=AxisFormat(title="Fecha", tickformat="%d %b %Y"),
        y=AxisFormat(title="Volumen útil (millones de m³)", tickformat=",.2f"),
        y2=y2_axis,
    )
    apply_exec_style(
        fig_dual,
        title="Evolución Volumen útil y Nivel",
        subtitle=f"{reservorio_sel} — {short_spanish_date(hist['fecha'].min())} a {short_spanish_date(hist['fecha'].max())}",
        source="EGASA · Data Mart",
    )
    st.plotly_chart(fig_dual, use_container_width=True, config=PLOTLY_CONFIG)
else:
    st.info("Sin historial para el reservorio seleccionado.")

# Alertas simples
st.markdown("### Alertas")
alerts = []
if "pct_llenado" in df.columns:
    umbral = st.sidebar.slider("Umbral alerta % llenado", 0.0, 1.0, 0.20, 0.01)
    low = df[df["pct_llenado"] < umbral][["reservorio", "pct_llenado"]].sort_values("pct_llenado")
    if not low.empty:
        alerts.append(("Bajo % llenado", low))

if alerts:
    for title, a in alerts:
        st.warning(title)
        st.dataframe(a, use_container_width=True)
else:
    st.success("Sin alertas con los umbrales actuales.")
