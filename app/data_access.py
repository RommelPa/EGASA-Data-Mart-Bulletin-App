# -*- coding: utf-8 -*-
"""Capa de acceso a datos para Streamlit (caching centralizado)."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parents[1]
DATA_MART = ROOT / "data_mart"


def _metadata_path() -> Path:
    return DATA_MART / "metadata.json"


def _metadata_token() -> float:
    path = _metadata_path()
    return path.stat().st_mtime if path.exists() else 0.0


@st.cache_data(show_spinner=False)
def get_metadata(meta_token: float | None = None) -> Dict[str, Any]:
    """Carga metadata.json. El parámetro meta_token fuerza invalidación."""

    path = _metadata_path()
    if not path.exists():
        return {"datasets": {}, "_meta_token": meta_token}
    with path.open("r", encoding="utf-8") as fh:
        data = json.load(fh)
    data["_meta_token"] = meta_token
    return data


def dataset_contract(name: str) -> Dict[str, Any]:
    meta = get_metadata(_metadata_token())
    return meta.get("datasets", {}).get(name, {}) if meta else {}


@st.cache_data(show_spinner=False)
def load_table(name: str, parse_dates: Optional[List[str]] = None, meta_token: float | None = None) -> pd.DataFrame:
    """Carga un CSV del data mart; se invalida al cambiar metadata.json."""

    path = DATA_MART / name
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path, parse_dates=parse_dates, low_memory=False)
    return df


@st.cache_data(show_spinner=False)
def load_generacion_15min(yyyymm: str, meta_token: float | None = None) -> pd.DataFrame:
    path = DATA_MART / f"generacion_15min_{yyyymm}.csv"
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path, low_memory=False)
    if "fecha_hora" in df.columns:
        df["fecha_hora"] = pd.to_datetime(df["fecha_hora"], errors="coerce")
    return df.dropna(subset=["fecha_hora"])


def list_yyyymm_15min(meta_token: float | None = None) -> List[str]:
    if not DATA_MART.exists():
        return []
    out = []
    for p in DATA_MART.glob("generacion_15min_*.csv"):
        yyyymm = p.stem.replace("generacion_15min_", "")
        if yyyymm.isdigit() and len(yyyymm) == 6:
            out.append(yyyymm)
    return sorted(out)


def metadata_token() -> float:
    """Exponer token para reuso externo (e.g. st.cache_data inputs)."""

    return _metadata_token()
