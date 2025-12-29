# -*- coding: utf-8 -*-

"""Página de resumen general."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st

DATA_MART = Path(__file__).resolve().parents[2] / "data_mart"


def show_table(title: str, filename: str, nrows: int = 20) -> None:
    st.subheader(title)
    path = DATA_MART / filename
    if path.exists():
        df = pd.read_csv(path)
        st.dataframe(df.head(nrows))
    else:
        st.info(f"No se encontró {filename}")


def main() -> None:
    st.title("Resumen")
    show_table("Generación mensual", "generacion_mensual.csv")
    show_table("Ventas mensual (MWh)", "ventas_mensual_mwh.csv")
    show_table("Ventas mensual (S/.)", "ventas_mensual_soles.csv")
    show_table("Precio medio", "precio_medio_mensual.csv")


if __name__ == "__main__":
    main()
