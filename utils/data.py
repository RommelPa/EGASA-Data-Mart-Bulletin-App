# utils/data.py
from __future__ import annotations

from pathlib import Path
import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parents[1]
DATA_MART = ROOT / "data_mart"
DATA_REF = ROOT / "data_reference"


@st.cache_data(show_spinner=False)
def load_csv(name: str, parse_dates: list[str] | None = None) -> pd.DataFrame:
    path = DATA_MART / name
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path, parse_dates=parse_dates, low_memory=False)
    return df


@st.cache_data(show_spinner=False)
def load_generacion_15min(yyyymm: str) -> pd.DataFrame:
    path = DATA_MART / f"generacion_15min_{yyyymm}.csv"
    if not path.exists():
        return pd.DataFrame()

    df = pd.read_csv(path, low_memory=False)

    # fuerza conversión (evita el error .dt)
    if "fecha_hora" in df.columns:
        df["fecha_hora"] = pd.to_datetime(df["fecha_hora"], errors="coerce")

    # limpieza opcional
    df = df.dropna(subset=["fecha_hora"])
    return df


@st.cache_data(show_spinner=False)
def load_centrales() -> pd.DataFrame:
    candidates = [
        DATA_REF / "centrales_egasa.csv",
        DATA_MART / "centrales_egasa.csv",
    ]
    for p in candidates:
        if p.exists():
            return pd.read_csv(p)
    return pd.DataFrame()


def list_yyyymm_15min() -> list[str]:
    if not DATA_MART.exists():
        return []
    out = []
    for p in DATA_MART.glob("generacion_15min_*.csv"):
        yyyymm = p.stem.replace("generacion_15min_", "")
        if yyyymm.isdigit() and len(yyyymm) == 6:
            out.append(yyyymm)
    return sorted(out)