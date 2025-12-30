import pandas as pd
import plotly.express as px
import streamlit as st

from app.charts.theme import AxisFormat, apply_exec_style, format_axis_units
from utils.data import load_csv

st.set_page_config(layout="wide")
st.title("📄 Contratos (2025–2036)")

con = load_csv("contratos_base.csv")

if con.empty:
    st.warning("No existe contratos_base.csv en data_mart.")
    st.stop()

# intenta parse fechas comunes si están
for c in con.columns:
    if "fecha" in c.lower() or "inicio" in c.lower() or "fin" in c.lower():
        con[c] = pd.to_datetime(con[c], errors="coerce")

# identificar cliente
cliente_col = next((c for c in con.columns if "cliente" in c.lower()), None)
if cliente_col is None:
    cliente_col = con.columns[0]

clientes = sorted([x for x in con[cliente_col].dropna().astype(str).unique() if x.strip() != ""])
sel_clientes = st.sidebar.multiselect("Clientes", clientes, default=clientes[:10] if len(clientes) > 10 else clientes)

df = con[con[cliente_col].astype(str).isin(sel_clientes)].copy()

st.markdown("## 1) Tabla")
st.dataframe(df, use_container_width=True)

st.markdown("## 2) Resumen")
col1, col2, col3 = st.columns(3)
col1.metric("Contratos", f"{len(df):,}")
col2.metric("Clientes", f"{df[cliente_col].nunique():,}")

# Timeline si encontramos inicio/fin
inicio_col = next((c for c in df.columns if "inicio" in c.lower()), None)
fin_col = next((c for c in df.columns if "fin" in c.lower()), None)

if inicio_col and fin_col:
    d2 = df.dropna(subset=[inicio_col, fin_col]).copy()
    if not d2.empty:
        d2[inicio_col] = pd.to_datetime(d2[inicio_col], errors="coerce")
        d2[fin_col] = pd.to_datetime(d2[fin_col], errors="coerce")
        d2 = d2.dropna(subset=[inicio_col, fin_col])

        fig = px.timeline(
            d2,
            x_start=inicio_col,
            x_end=fin_col,
            y=cliente_col,
            title="Vigencias (timeline)",
        )
        fig.update_yaxes(autorange="reversed")
        format_axis_units(
            fig,
            x=AxisFormat(title="Vigencia", tickformat="%d %b %Y"),
            y=AxisFormat(title="Cliente"),
        )
        apply_exec_style(
            fig,
            title="Vigencias de contratos",
            subtitle="Inicio y fin declarados",
            source="EGASA · Data Mart",
            hovermode="closest",
        )
        st.plotly_chart(fig, use_container_width=True)
else:
    st.info("No se detectaron columnas de inicio/fin para graficar timeline.")
