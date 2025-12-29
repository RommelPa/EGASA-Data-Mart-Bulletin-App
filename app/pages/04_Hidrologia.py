# -*- coding: utf-8 -*-

"""Página de hidrología."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st

DATA_MART = Path(__file__).resolve().parents[2] / "data_mart"


def main() -> None:
    st.title("Hidrología")

    files = [
        ("Volumen mensual", "hidro_volumen_mensual.csv"),
        ("Caudal mensual", "hidro_caudal_mensual.csv"),
        ("Represas diario", "represas_diario.csv"),
    ]
    for title, fname in files:
        path = DATA_MART / fname
        st.subheader(title)
        if path.exists():
            df = pd.read_csv(path)
            st.dataframe(df.head(200))
        else:
            st.info(f"No se encontró {fname}")


if __name__ == "__main__":
    main()
