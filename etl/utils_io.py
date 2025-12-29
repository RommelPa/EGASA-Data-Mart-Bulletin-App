# -*- coding: utf-8 -*-

"""Utilidades de entrada/salida para el ETL."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Iterable, List, Optional, Tuple

import pandas as pd

logger = logging.getLogger(__name__)


def detect_header_row(df_preview: pd.DataFrame, expected_columns: Iterable[str]) -> int:
    """Detectar la fila que contiene los nombres de columnas.

    Busca la primera fila que contenga la mayoría de columnas esperadas (case-insensitive).
    Devuelve el índice (0-based) que debe usarse como header en pandas.read_excel.
    """

    expected = {col.strip().lower() for col in expected_columns}
    for idx, row in df_preview.iterrows():
        row_values = {str(v).strip().lower() for v in row if pd.notna(v)}
        match_ratio = len(expected & row_values) / max(len(expected), 1)
        if match_ratio >= 0.6:
            return idx
    return 0


def read_excel_safe(
    path: Path,
    sheet_name: str | int | None = 0,
    expected_columns: Optional[Iterable[str]] = None,
    **kwargs,
) -> pd.DataFrame:
    """Leer Excel robustamente detectando header.

    Si expected_columns se proporciona, inspecciona las primeras filas para ubicar el header.
    """

    if not path.exists():
        logger.warning("Archivo no encontrado: %s", path)
        return pd.DataFrame()

    # Leer una vista previa para detectar header
    preview = pd.read_excel(path, sheet_name=sheet_name, nrows=20, header=None)
    header_row = 0
    if expected_columns:
        header_row = detect_header_row(preview, expected_columns)

    df = pd.read_excel(path, sheet_name=sheet_name, header=header_row, **kwargs)
    return df


def list_matching_files(base_dir: Path, pattern: str) -> List[Path]:
    """Listar archivos que contengan el patrón en su nombre."""

    if not base_dir.exists():
        return []
    return sorted([p for p in base_dir.iterdir() if pattern.lower() in p.name.lower()])


def safe_write_csv(df: pd.DataFrame, path: Path) -> int:
    """Escribir DataFrame a CSV asegurando carpetas. Devuelve número de filas."""

    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8")
    return len(df)


def record_file_info(files: Iterable[Path]) -> List[Tuple[str, float, int]]:
    """Registrar metadata básica de archivos leídos."""

    info: List[Tuple[str, float, int]] = []
    for f in files:
        if f.exists():
            stat = f.stat()
            info.append((f.name, stat.st_mtime, stat.st_size))
    return info


__all__ = [
    "detect_header_row",
    "read_excel_safe",
    "list_matching_files",
    "safe_write_csv",
    "record_file_info",
]
