# utils/data.py
from __future__ import annotations

from pathlib import Path
import pandas as pd
import streamlit as st

from app.data_access import (
    load_table as load_csv,
    load_generacion_15min,
    list_yyyymm_15min,
    get_metadata,
    metadata_token,
)


ROOT = Path(__file__).resolve().parents[1]
DATA_REF = ROOT / "data_reference"
DATA_MART = ROOT / "data_mart"


def load_centrales() -> pd.DataFrame:
    """Compatibilidad: carga maestro de centrales (no cacheado por simplicidad)."""

    candidates = [
        DATA_REF / "centrales_egasa.csv",
        DATA_MART / "centrales_egasa.csv",
    ]
    for p in candidates:
        if p.exists():
            return pd.read_csv(p)
    return pd.DataFrame()


__all__ = [
    "load_csv",
    "load_generacion_15min",
    "list_yyyymm_15min",
    "load_centrales",
    "get_metadata",
    "metadata_token",
]
