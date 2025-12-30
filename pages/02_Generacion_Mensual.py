import streamlit as st
import plotly.express as px

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
st.plotly_chart(px.line(total, x="periodo", y="energia_mwh", title="Total mensual (MWh)"), use_container_width=True)

st.markdown("### 2) Por central (Top N)")
top_n = st.sidebar.slider("Top N centrales", 3, 12, 9)
byc = gen.groupby(["periodo", "central"])["energia_mwh"].sum().reset_index()
rank = byc.groupby("central")["energia_mwh"].sum().sort_values(ascending=False).head(top_n).index
byc = byc[byc["central"].isin(rank)]
st.plotly_chart(px.bar(byc, x="periodo", y="energia_mwh", color="central", title="Top centrales (MWh)"),
                use_container_width=True)

st.markdown("### 3) Mix Hidro vs Térmica")
if not centrales.empty:
    tmp = gen.merge(centrales, on="central_id", how="left")
    mix = tmp.groupby(["periodo", "tipo"])["energia_mwh"].sum().reset_index()
    st.plotly_chart(px.bar(mix, x="periodo", y="energia_mwh", color="tipo", title="Mix mensual"), use_container_width=True)

st.markdown("### 4) Estacionalidad (heatmap)")
gen["anio"] = gen["periodo"].astype(str).str[:4].astype(int)
gen["mes"] = gen["periodo"].astype(str).str[4:6].astype(int)
hm = gen.groupby(["anio", "mes"])["energia_mwh"].sum().reset_index()
fig = px.density_heatmap(hm, x="mes", y="anio", z="energia_mwh", title="Heatmap estacionalidad (MWh)")
st.plotly_chart(fig, use_container_width=True)