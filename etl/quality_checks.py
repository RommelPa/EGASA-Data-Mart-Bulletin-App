# -*- coding: utf-8 -*-

"""Validaciones de calidad y generación de metadata."""

from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import pandas as pd

from .utils_io import record_file_info

logger = logging.getLogger(__name__)


def check_basic_issues(df: pd.DataFrame, key_columns: Iterable[str]) -> List[str]:
    """Regresar lista de alertas detectadas."""

    alerts: List[str] = []

    missing_columns = [col for col in key_columns if col not in df.columns]
    if missing_columns:
        alerts.append("columnas_no_detectadas")
        return alerts

    if df.empty:
        alerts.append("dataset_vacio")
        return alerts

    duplicates = df.duplicated(subset=list(key_columns)).sum()
    if duplicates:
        alerts.append(f"duplicados:{duplicates}")

    nulls = df[list(key_columns)].isna().sum().sum()
    if nulls:
        alerts.append(f"faltantes:{nulls}")

    negatives = df.select_dtypes(include=["number"]) < 0
    if negatives.any().any():
        alerts.append("valores_negativos")

    return alerts


def _date_bounds(df: pd.DataFrame) -> Tuple[str | None, str | None]:
    date_cols = [c for c in df.columns if "fecha" in c or "periodo" in c]
    if not date_cols:
        return None, None
    min_ts = None
    max_ts = None
    for col in date_cols:
        series = df[col]
        if pd.api.types.is_datetime64_any_dtype(series):
            current_min = series.min()
            current_max = series.max()
        else:
            parsed = pd.to_datetime(series, errors="coerce")
            current_min = parsed.min()
            current_max = parsed.max()
        if pd.notna(current_min):
            min_ts = min_ts if min_ts is not None and min_ts < current_min else current_min
        if pd.notna(current_max):
            max_ts = max_ts if max_ts is not None and max_ts > current_max else current_max
    min_str = min_ts.isoformat() if pd.notna(min_ts) else None
    max_str = max_ts.isoformat() if pd.notna(max_ts) else None
    return min_str, max_str


def _quality_counters(df: pd.DataFrame) -> Dict[str, int]:
    counters: Dict[str, int] = {}
    if "central_id" in df.columns:
        counters["centrales_no_mapeadas"] = int(df["central_id"].isna().sum())
    if "cliente" in df.columns:
        counters["clientes_vacios"] = int(df["cliente"].isna().sum() + (df["cliente"].astype(str).str.strip() == "").sum())
    return counters


def write_metadata(
    path: Path | None,
    datasets_info: Dict[str, Tuple[pd.DataFrame, Iterable[str]]],
    files_read: Iterable[Path],
) -> None:
    """Escribir metadata.json con métricas básicas."""

    from .config import DATA_MART

    metadata_path = DATA_MART / "metadata.json"
    metadata_path.parent.mkdir(parents=True, exist_ok=True)

    payload = {
        "fecha_ejecucion": datetime.utcnow().isoformat(),
        "archivos_leidos": [
            {"nombre": name, "modified_time": mtime, "size": size}
            for name, mtime, size in record_file_info(files_read)
        ],
        "datasets": {},
    }

    for name, (df, keys) in datasets_info.items():
        min_fecha, max_fecha = _date_bounds(df)
        meta = {
            "filas": int(len(df)),
            "alertas": check_basic_issues(df, keys),
        }
        if min_fecha or max_fecha:
            meta["fecha_min"] = min_fecha
            meta["fecha_max"] = max_fecha
        meta.update(_quality_counters(df))
        payload["datasets"][name] = meta

    with metadata_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)

    logger.info("Metadata escrita en %s", metadata_path)


__all__ = ["check_basic_issues", "write_metadata"]
