# -*- coding: utf-8 -*-

"""Página de contratos."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st

DATA_MART = Path(__file__).resolve().parents[2] / "data_mart"


def main() -> None:
    st.title("Contratos")

    base_path = DATA_MART / "contratos_base.csv"
    riesgo_path = DATA_MART / "contratos_riesgo.csv"

    st.subheader("Contratos base")
    if base_path.exists():
        st.dataframe(pd.read_csv(base_path))
    else:
        st.info("No se encontró contratos_base.csv")

    st.subheader("Contratos riesgo")
    if riesgo_path.exists():
        st.dataframe(pd.read_csv(riesgo_path))
    else:
        st.info("No se encontró contratos_riesgo.csv")


if __name__ == "__main__":
    main()
