# -*- coding: utf-8 -*-

"""Funciones de limpieza y normalización."""

from __future__ import annotations

import logging
import unicodedata
from difflib import get_close_matches
from pathlib import Path
from typing import Dict

import pandas as pd

from .config import DATA_REFERENCE

logger = logging.getLogger(__name__)


def normalize_text(value: str) -> str:
    """Normalizar texto para comparaciones."""

    value = unicodedata.normalize("NFKD", value or "").encode("ascii", "ignore").decode()
    value = "".join(ch if ch.isalnum() else " " for ch in value)
    return " ".join(value.upper().split())


def load_centrales_reference(path: Path | None = None) -> pd.DataFrame:
    """Cargar maestros de centrales EGASA."""

    path = path or DATA_REFERENCE / "centrales_egasa.csv"
    if not path.exists():
        logger.warning("Referencia de centrales no encontrada: %s", path)
        return pd.DataFrame()
    df = pd.read_csv(path)
    df["central_nombre_norm"] = df["central_nombre"].map(normalize_text)
    return df


def map_central_id(df: pd.DataFrame, centrales_df: pd.DataFrame, source_col: str = "central") -> pd.DataFrame:
    """Agregar central_id usando el maestro."""

    if df.empty or centrales_df.empty:
        df["central_id"] = None
        return df

    centrales_map: Dict[str, str] = dict(
        zip(centrales_df["central_nombre_norm"], centrales_df["central_id"])
    )
    known_names = list(centrales_map.keys())

    cache: Dict[str, str | None] = {}

    def _match(name: str) -> str | None:
        if name in cache:
            return cache[name]
        direct = centrales_map.get(name)
        if direct:
            cache[name] = direct
            return direct
        closest = get_close_matches(name, known_names, n=1, cutoff=0.6)
        cache[name] = centrales_map.get(closest[0]) if closest else None
        return cache[name]

    df["central_nombre_norm"] = df[source_col].astype(str).map(normalize_text)
    df["central_id"] = df["central_nombre_norm"].map(_match)
    df = df.drop(columns=["central_nombre_norm"])
    return df


__all__ = ["normalize_text", "load_centrales_reference", "map_central_id"]
