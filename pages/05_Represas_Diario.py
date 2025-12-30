import streamlit as st
import pandas as pd
import plotly.express as px

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
    st.plotly_chart(fig, use_container_width=True)

# Tabla operativa
st.markdown("### Tabla operativa")
st.dataframe(df, use_container_width=True)

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