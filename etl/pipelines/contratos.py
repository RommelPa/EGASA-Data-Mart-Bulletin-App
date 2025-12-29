# -*- coding: utf-8 -*-

"""Pipeline de contratos."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import pandas as pd

from ..config import DATA_LANDING, DATA_MART, LANDING_FILES, OUTPUT_FILES
from ..utils_io import list_matching_files, read_excel_safe, safe_write_csv

logger = logging.getLogger(__name__)


def _load_sheet(path: Path, target: str) -> pd.DataFrame:
    try:
        xls = pd.ExcelFile(path)
    except Exception as exc:
        logger.warning("No se pudo abrir %s: %s", path, exc)
        return pd.DataFrame()

    for sheet in xls.sheet_names:
        if target.lower() in sheet.lower():
            return read_excel_safe(path, sheet_name=sheet)
    return pd.DataFrame()


def _clean_contracts(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["cliente", "tipo", "fecha_inicio", "fecha_fin", "energia_mwh", "precio"])
    df = df.rename(columns=str.lower)
    for col in ["fecha_inicio", "fecha_fin"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    for col in df.columns:
        if "mwh" in col or "energia" in col:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def run_contratos() -> Tuple[List[Path], Dict[str, Tuple[pd.DataFrame, Iterable[str]]]]:
    """Ejecutar pipeline de contratos."""

    files_read: List[Path] = []
    datasets: Dict[str, Tuple[pd.DataFrame, Iterable[str]]] = {}

    contrato_files = list_matching_files(DATA_LANDING, LANDING_FILES["contratos"])
    base_df = pd.DataFrame(columns=["cliente", "tipo", "fecha_inicio", "fecha_fin", "energia_mwh", "precio"])
    riesgo_df = pd.DataFrame(columns=["cliente", "tipo", "fecha_inicio", "fecha_fin", "energia_mwh", "precio"])

    if contrato_files:
        path = contrato_files[0]
        files_read.append(path)
        base_df = _clean_contracts(_load_sheet(path, "CONTRATOS BASE DATOS"))
        riesgo_df = _clean_contracts(_load_sheet(path, "RIESGO"))

    safe_write_csv(base_df, DATA_MART / OUTPUT_FILES["contratos_base"])
    datasets["contratos_base"] = (base_df, ["cliente", "fecha_inicio"])

    if not riesgo_df.empty:
        safe_write_csv(riesgo_df, DATA_MART / OUTPUT_FILES["contratos_riesgo"])
    else:
        safe_write_csv(riesgo_df, DATA_MART / OUTPUT_FILES["contratos_riesgo"])
    datasets["contratos_riesgo"] = (riesgo_df, ["cliente", "fecha_inicio"])

    return files_read, datasets


__all__ = ["run_contratos"]
