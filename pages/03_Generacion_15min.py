import streamlit as st
import pandas as pd
import plotly.express as px

from utils.data import load_generacion_15min, list_yyyymm_15min

st.set_page_config(layout="wide")
st.title("⏱️ Generación 15-min (2025)")

yyyymm_list = list_yyyymm_15min()
if not yyyymm_list:
    st.warning("No hay archivos generacion_15min_YYYYMM.csv en data_mart.")
    st.stop()

yyyymm = st.sidebar.selectbox("Selecciona YYYYMM", yyyymm_list)
df = load_generacion_15min(yyyymm)

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
st.plotly_chart(fig, use_container_width=True)

st.markdown("### Agregado horario")
df_dia["hora"] = df_dia["fecha_hora"].dt.floor("H")
h = df_dia.groupby("hora")["energia_mwh"].sum().reset_index()
st.plotly_chart(px.line(h, x="hora", y="energia_mwh", title="Energía por hora (MWh)"), use_container_width=True)

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
st.plotly_chart(px.line(comp, x="t", y="energia_mwh", color="dia", title="Comparación por hora"), use_container_width=True)
