# -*- coding: utf-8 -*-

"""Pipeline de hidrología."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import pandas as pd

from ..config import DATA_LANDING, DATA_MART, LANDING_FILES, OUTPUT_FILES
from ..utils_io import list_matching_files, read_excel_safe, safe_write_csv

logger = logging.getLogger(__name__)


def _procesar_control(path: Path) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Procesar archivo Control Hidrológico."""

    volumen_df = pd.DataFrame(columns=["reservorio", "periodo", "volumen_000m3"])
    caudal_df = pd.DataFrame(columns=["estacion", "periodo", "caudal_m3s"])

    try:
        xls = pd.ExcelFile(path)
    except Exception as exc:
        logger.warning("No se pudo leer %s: %s", path, exc)
        return volumen_df, caudal_df

    # Volúmenes: primera hoja
    if xls.sheet_names:
        vol_sheet = xls.sheet_names[0]
        df_vol = read_excel_safe(path, sheet_name=vol_sheet, expected_columns=["reservorio"])
        if not df_vol.empty:
            first_col = df_vol.columns[0]
            df_vol = df_vol.rename(columns={first_col: "reservorio"})
            month_cols = [c for c in df_vol.columns if c != "reservorio"]
            df_vol = df_vol.melt(id_vars=["reservorio"], value_vars=month_cols, var_name="periodo", value_name="volumen_000m3")
            df_vol["volumen_000m3"] = pd.to_numeric(df_vol["volumen_000m3"], errors="coerce")
            volumen_df = df_vol.dropna(subset=["reservorio"])

    # Caudal: hoja CAUDAL si existe
    if "CAUDAL" in [s.upper() for s in xls.sheet_names]:
        sheet = [s for s in xls.sheet_names if s.upper() == "CAUDAL"][0]
        df_cau = read_excel_safe(path, sheet_name=sheet, expected_columns=["periodo"])
        if not df_cau.empty:
            first_col = df_cau.columns[0]
            df_cau = df_cau.rename(columns={first_col: "estacion"})
            month_cols = [c for c in df_cau.columns if c != "estacion"]
            df_cau = df_cau.melt(id_vars=["estacion"], value_vars=month_cols, var_name="periodo", value_name="caudal_m3s")
            df_cau["caudal_m3s"] = pd.to_numeric(df_cau["caudal_m3s"], errors="coerce")
            caudal_df = df_cau.dropna(subset=["estacion"])

    return volumen_df, caudal_df


def _procesar_represas(path: Path) -> pd.DataFrame:
    """Procesar archivo BDREPRESAS."""

    df = read_excel_safe(path, sheet_name="INFORMEDIARIO", expected_columns=["FECHA"])
    if df.empty:
        df = read_excel_safe(path, expected_columns=["FECHA"])
    if df.empty:
        return pd.DataFrame(columns=["fecha", "reservorio", "nivel", "caudal", "volumen"])

    if "FECHA" not in df.columns:
        df.rename(columns={df.columns[0]: "FECHA"}, inplace=True)

    df_out = pd.DataFrame(
        {
            "fecha": pd.to_datetime(df["FECHA"], errors="coerce"),
            "reservorio": df.get("RESERVORIO", df.get("NOMBRE", "DESCONOCIDO")),
            "nivel": pd.to_numeric(df.get("NIVEL"), errors="coerce"),
            "caudal": pd.to_numeric(df.get("CAUDAL"), errors="coerce"),
            "volumen": pd.to_numeric(df.get("VOLUMEN"), errors="coerce"),
        }
    )
    df_out = df_out.dropna(subset=["fecha"])
    return df_out


def run_hidrologia() -> Tuple[List[Path], Dict[str, Tuple[pd.DataFrame, Iterable[str]]]]:
    """Ejecutar pipeline de hidrología."""

    files_read: List[Path] = []
    datasets: Dict[str, Tuple[pd.DataFrame, Iterable[str]]] = {}

    control_files = list_matching_files(DATA_LANDING, LANDING_FILES["hidrologia_control"])
    volumen_df = pd.DataFrame(columns=["reservorio", "periodo", "volumen_000m3"])
    caudal_df = pd.DataFrame(columns=["estacion", "periodo", "caudal_m3s"])
    if control_files:
        volumen_df, caudal_df = _procesar_control(control_files[0])
        files_read.append(control_files[0])

    safe_write_csv(volumen_df, DATA_MART / OUTPUT_FILES["hidro_volumen_mensual"])
    safe_write_csv(caudal_df, DATA_MART / OUTPUT_FILES["hidro_caudal_mensual"])
    datasets["hidro_volumen_mensual"] = (volumen_df, ["reservorio", "periodo"])
    datasets["hidro_caudal_mensual"] = (caudal_df, ["estacion", "periodo"])

    represas_files = list_matching_files(DATA_LANDING, LANDING_FILES["hidrologia_represas"])
    represas_df = pd.DataFrame(columns=["fecha", "reservorio", "nivel", "caudal", "volumen"])
    if represas_files:
        represas_df = _procesar_represas(represas_files[0])
        files_read.append(represas_files[0])

    safe_write_csv(represas_df, DATA_MART / OUTPUT_FILES["represas_diario"])
    datasets["represas_diario"] = (represas_df, ["fecha", "reservorio"])

    return files_read, datasets


__all__ = ["run_hidrologia"]
