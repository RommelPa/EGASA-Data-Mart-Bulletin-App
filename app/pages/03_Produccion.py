# -*- coding: utf-8 -*-

"""Página de producción."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st

DATA_MART = Path(__file__).resolve().parents[2] / "data_mart"


def main() -> None:
    st.title("Producción")

    mensual_path = DATA_MART / "generacion_mensual.csv"
    if mensual_path.exists():
        df = pd.read_csv(mensual_path)
        st.subheader("Generación mensual (MWh)")
        st.dataframe(df)
    else:
        st.info("No se encontró generacion_mensual.csv")

    st.subheader("Generación 15-min")
    for csv_file in sorted(DATA_MART.glob("generacion_15min_*.csv")):
        st.markdown(f"**{csv_file.name}**")
        df15 = pd.read_csv(csv_file, parse_dates=["fecha_hora"])
        st.dataframe(df15.head(200))


if __name__ == "__main__":
    main()
