import streamlit as st

st.set_page_config(page_title="EGASA | Boletín Operativo", layout="wide")

st.title("EGASA — Boletín Operativo (Streamlit + Plotly)")
st.markdown(
    """
Este aplicativo consolida **Generación**, **Hidrología**, **Balance Energético**, **Comercial** y **Contratos**.
Usa el menú lateral (páginas) para navegar.

✅ Fuente: `data_mart/*.csv` (ETL).
"""
)

st.info("Siguiente paso: abre **📌 Resumen Ejecutivo** y valida consistencia mes a mes.")
