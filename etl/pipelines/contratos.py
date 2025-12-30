# -*- coding: utf-8 -*-

"""Pipeline de contratos."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import pandas as pd

from ..config import DATA_LANDING, DATA_MART, LANDING_FILES, OUTPUT_FILES, get_source
from ..utils_io import list_matching_files, read_excel_safe, apply_table_rules, validate_and_write

logger = logging.getLogger(__name__)


def _load_sheet(path: Path, target: str) -> pd.DataFrame:
    try:
        xls = pd.ExcelFile(path)
    except Exception:
        logger.exception("No se pudo abrir %s", path)
        raise

    for sheet in xls.sheet_names:
        if target.lower() in sheet.lower():
            return read_excel_safe(path, sheet_name=sheet)
    return pd.DataFrame()


def _clean_contracts(df: pd.DataFrame) -> pd.DataFrame:
    df = df.dropna(how="all")
    if df.empty:
        return pd.DataFrame(columns=["cliente", "tipo_contrato", "fecha_inicio", "fecha_fin", "potencia_mw", "precio_hp_usd_mwh", "precio_fp_usd_mwh"])
    df = df.rename(columns=str.lower)
    mapping = {
        "nombre del cliente": "cliente",
        "tipo contrato": "tipo_contrato",
        "vigencia inicio": "fecha_inicio",
        "vigencia final": "fecha_fin",
        "potencia total (mw)": "potencia_mw",
        "precio energia hp (usd/mwh)": "precio_hp_usd_mwh",
        "precio energia fp (usd/mwh)": "precio_fp_usd_mwh",
    }
    df = df.rename(columns=mapping)
    for col in ["fecha_inicio", "fecha_fin", "vigencia inicio", "vigencia final"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    def _series(name: str) -> pd.Series:
        s = df.get(name)
        if s is None:
            s = pd.Series([None] * len(df))
        return s

    fecha_inicio_series = _series("fecha_inicio")
    if fecha_inicio_series.isna().all():
        fecha_inicio_series = _series("vigencia inicio")
    fecha_fin_series = _series("fecha_fin")
    if fecha_fin_series.isna().all():
        fecha_fin_series = _series("vigencia final")

    df_out = pd.DataFrame(
        {
            "cliente": _series("cliente"),
            "tipo_contrato": _series("tipo_contrato"),
            "fecha_inicio": fecha_inicio_series,
            "fecha_fin": fecha_fin_series,
            "potencia_mw": pd.to_numeric(_series("potencia_mw"), errors="coerce"),
            "precio_hp_usd_mwh": pd.to_numeric(_series("precio_hp_usd_mwh"), errors="coerce"),
            "precio_fp_usd_mwh": pd.to_numeric(_series("precio_fp_usd_mwh"), errors="coerce"),
        }
    )
    return df_out.dropna(how="all")


def run_contratos() -> Tuple[List[Path], Dict[str, Tuple[pd.DataFrame, Iterable[str]]]]:
    """Ejecutar pipeline de contratos."""

    files_read: List[Path] = []
    datasets: Dict[str, Tuple[pd.DataFrame, Iterable[str]]] = {}

    contratos_cfg = get_source("contratos")
    contrato_files = list_matching_files(DATA_LANDING, LANDING_FILES["contratos"])
    base_df = pd.DataFrame(columns=["cliente", "tipo", "fecha_inicio", "fecha_fin", "energia_mwh", "precio"])
    riesgo_df = pd.DataFrame(columns=["cliente", "tipo", "fecha_inicio", "fecha_fin", "energia_mwh", "precio"])

    if contrato_files:
        path = contrato_files[0]
        files_read.append(path)
        source_cfg = get_source("contratos")
        sheets = (source_cfg or {}).get("sheets", {})

        try:
            base_df = _clean_contracts(_load_sheet(path, sheets.get("base", "CONTRATOS BASE DATOS")))
            riesgo_df = _clean_contracts(_load_sheet(path, sheets.get("riesgo", "RIESGO")))
        except Exception:
            logger.exception("Error procesando contratos en %s", path)
            raise ValueError(f"No se pudieron procesar hojas de contratos definidas en {path.name}")

    elif (contratos_cfg or {}).get("required", True):
        raise FileNotFoundError(f"No se encontró archivo de contratos en {DATA_LANDING}")

    base_df = apply_table_rules("contratos_base", base_df)
    riesgo_df = apply_table_rules("contratos_riesgo", riesgo_df)

    validate_and_write("contratos_base", base_df, DATA_MART / OUTPUT_FILES["contratos_base"])
    datasets["contratos_base"] = (base_df, ["cliente", "fecha_inicio"])

    validate_and_write("contratos_riesgo", riesgo_df, DATA_MART / OUTPUT_FILES["contratos_riesgo"])
    datasets["contratos_riesgo"] = (riesgo_df, ["cliente", "fecha_inicio"])

    return files_read, datasets


__all__ = ["run_contratos"]
