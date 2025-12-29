# -*- coding: utf-8 -*-

"""Página comercial."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st

DATA_MART = Path(__file__).resolve().parents[2] / "data_mart"


def main() -> None:
    st.title("Comercial")

    archivos = [
        ("Ventas mensual (MWh)", "ventas_mensual_mwh.csv"),
        ("Ventas mensual (S/.)", "ventas_mensual_soles.csv"),
        ("Ingresos mensual", "ingresos_mensual.csv"),
        ("Precio medio", "precio_medio_mensual.csv"),
    ]
    for titulo, fname in archivos:
        st.subheader(titulo)
        path = DATA_MART / fname
        if path.exists():
            df = pd.read_csv(path)
            st.dataframe(df.head(200))
        else:
            st.info(f"No se encontró {fname}")


if __name__ == "__main__":
    main()
