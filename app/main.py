# -*- coding: utf-8 -*-

"""Aplicación Streamlit para explorar el Data Mart."""

from __future__ import annotations

import streamlit as st


def main() -> None:
    st.set_page_config(page_title="EGASA Data Mart", layout="wide")
    st.title("EGASA - Data Mart")
    st.markdown(
        """
        Esta aplicación permite explorar los resultados del ETL local.
        Use el menú lateral para navegar entre las secciones.
        """
    )


if __name__ == "__main__":
    main()
