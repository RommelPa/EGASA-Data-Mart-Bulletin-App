# -*- coding: utf-8 -*-

"""Página de calidad de datos."""

from __future__ import annotations

import json
from pathlib import Path

import streamlit as st

DATA_MART = Path(__file__).resolve().parents[2] / "data_mart"


def main() -> None:
    st.title("Calidad de Datos")
    metadata_path = DATA_MART / "metadata.json"
    if metadata_path.exists():
        data = json.loads(metadata_path.read_text(encoding="utf-8"))
        st.json(data)
    else:
        st.info("Ejecute el ETL para generar metadata.json")


if __name__ == "__main__":
    main()
