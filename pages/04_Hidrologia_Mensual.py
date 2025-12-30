import streamlit as st
import plotly.express as px
import pandas as pd

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

    fig = px.line(df, x="fecha_mes", y="volumen_000m3", color="reservorio", title="Volumen mensual por reservorio")
    st.plotly_chart(fig, use_container_width=True)

    st.markdown("### Comparativo YoY (mismo mes, por año)")
    df["anio"] = df["periodo"].str[:4].astype(int)
    df["mes"] = df["periodo"].str[4:6].astype(int)
    mes_sel = st.selectbox("Mes", sorted(df["mes"].unique()))
    yoy = df[df["mes"] == mes_sel].groupby(["anio", "reservorio"])["volumen_000m3"].mean().reset_index()
    st.plotly_chart(px.line(yoy, x="anio", y="volumen_000m3", color="reservorio", title=f"YoY Volumen (mes={mes_sel})"),
                    use_container_width=True)
else:
    st.info("No hay datos de volumen en el rango seleccionado.")

st.markdown("## 2) Caudal mensual (m³/s)")

if not cau_f.empty:
    df = cau_f.copy()
    df["fecha_mes"] = pd.to_datetime(df["periodo"] + "01", format="%Y%m%d", errors="coerce")
    fig = px.line(df, x="fecha_mes", y="caudal_m3s", color="estacion", title="Caudal mensual")
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("No hay datos de caudal en el rango seleccionado.")