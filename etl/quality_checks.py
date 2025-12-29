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
        payload["datasets"][name] = {
            "filas": int(len(df)),
            "alertas": check_basic_issues(df, keys),
        }

    with metadata_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)

    logger.info("Metadata escrita en %s", metadata_path)


__all__ = ["check_basic_issues", "write_metadata"]
