# -*- coding: utf-8 -*-

"""Pipeline de facturación y precios."""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import pandas as pd

from ..config import DATA_LANDING, DATA_MART, LANDING_FILES, OUTPUT_FILES
from ..utils_io import detect_header_row, list_matching_files, safe_write_csv

logger = logging.getLogger(__name__)

MONTH_MAP = {
    "ENERO": "01",
    "FEBRERO": "02",
    "MARZO": "03",
    "ABRIL": "04",
    "MAYO": "05",
    "JUNIO": "06",
    "JULIO": "07",
    "AGOSTO": "08",
    "SETIEMBRE": "09",
    "SEPTIEMBRE": "09",
    "OCTUBRE": "10",
    "NOVIEMBRE": "11",
    "DICIEMBRE": "12",
}


def _read_with_header(path: Path, sheet_name: str, keywords: List[str]) -> pd.DataFrame:
    preview = pd.read_excel(path, sheet_name=sheet_name, header=None, nrows=60)
    header_row = detect_header_row(preview, keywords=keywords)
    if header_row == 0 and preview.iloc[0].isna().all():
        non_empty = preview.dropna(how="all")
        if not non_empty.empty:
            header_row = non_empty.index.min()
    if header_row == 0:
        for idx, row in preview.iterrows():
            vals = [str(v).strip().lower() for v in row if pd.notna(v)]
            if {"codigo", "cliente"} & set(vals):
                header_row = idx
                break
    df = pd.read_excel(path, sheet_name=sheet_name, header=header_row)
    return df


def _periodo_from_value(value: object) -> str | None:
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.strftime("%Y%m")
    if isinstance(value, (int, float)) and not pd.isna(value):
        # excel serial? ignore
        return None
    text = str(value).strip().upper()
    if text in MONTH_MAP:
        return f"2025{MONTH_MAP[text]}"
    return None


def _parse_sales(df: pd.DataFrame, value_name: str) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["cliente", "periodo", value_name])

    df = df.rename(columns=lambda c: c if isinstance(c, (pd.Timestamp, datetime)) else str(c).strip().upper())
    entity_col = "CLIENTE" if "CLIENTE" in df.columns else df.columns[0]
    month_cols = [
        c
        for c in df.columns
        if c != entity_col and (isinstance(c, (pd.Timestamp, datetime)) or str(c).upper() in MONTH_MAP)
    ]
    if not month_cols:
        logger.warning("No se detectaron columnas de meses en ventas (%s)", value_name)
        return pd.DataFrame(columns=["cliente", "periodo", value_name])

    df_long = df.melt(id_vars=[entity_col], value_vars=month_cols, var_name="periodo_raw", value_name=value_name)
    df_long = df_long.rename(columns={entity_col: "cliente"})
    df_long["periodo"] = df_long["periodo_raw"].map(_periodo_from_value)
    df_long[value_name] = pd.to_numeric(df_long[value_name], errors="coerce")
    df_long = df_long.dropna(subset=["cliente", "periodo"])
    return df_long[["cliente", "periodo", value_name]]


def _parse_ingresos(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["concepto", "periodo", "soles"])
    df = df.rename(columns=lambda c: c if isinstance(c, (pd.Timestamp, datetime)) else str(c).strip().upper())
    id_col = df.columns[0]
    month_cols = [
        c
        for c in df.columns
        if c != id_col and (isinstance(c, (pd.Timestamp, datetime)) or str(c).upper() in MONTH_MAP)
    ]
    if not month_cols:
        logger.warning("No se detectaron columnas de meses en Ingresos")
        return pd.DataFrame(columns=["concepto", "periodo", "soles"])
    df_long = df.melt(id_vars=[id_col], value_vars=month_cols, var_name="periodo_raw", value_name="soles")
    df_long = df_long.rename(columns={id_col: "concepto"})
    df_long["periodo"] = df_long["periodo_raw"].map(_periodo_from_value)
    df_long["soles"] = pd.to_numeric(df_long["soles"], errors="coerce")
    df_long = df_long.dropna(subset=["concepto", "periodo"])
    return df_long[["concepto", "periodo", "soles"]]


def run_facturacion() -> Tuple[List[Path], Dict[str, Tuple[pd.DataFrame, Iterable[str]]]]:
    """Ejecutar pipeline de facturación."""

    files_read: List[Path] = []
    datasets: Dict[str, Tuple[pd.DataFrame, Iterable[str]]] = {}

    fact_files = list_matching_files(DATA_LANDING, LANDING_FILES["facturacion"])
    ventas_mwh = pd.DataFrame(columns=["cliente", "periodo", "mwh"])
    ventas_soles = pd.DataFrame(columns=["cliente", "periodo", "soles"])
    ingresos = pd.DataFrame(columns=["concepto", "periodo", "soles"])
    precio_medio = pd.DataFrame(columns=["periodo", "precio_medio_soles_mwh"])

    if fact_files:
        path = fact_files[0]
        files_read.append(path)
        try:
            ventas_mwh_sheet = _read_with_header(path, "VENTAS (MWh)", ["cliente", "enero"])
            ventas_mwh = _parse_sales(ventas_mwh_sheet, "mwh")
        except Exception:
            logger.exception("Error procesando hoja VENTAS (MWh)")
            raise

        try:
            ventas_soles_sheet = _read_with_header(path, "VENTAS (S)", ["cliente", "enero"])
            ventas_soles = _parse_sales(ventas_soles_sheet, "soles")
        except Exception:
            logger.exception("Error procesando hoja VENTAS (S)")
            raise

        try:
            ingresos_sheet = _read_with_header(path, "Ingresos", ["enero"])
            ingresos = _parse_ingresos(ingresos_sheet)
        except Exception:
            logger.exception("Error procesando hoja Ingresos")
            raise

    # Calcular precio medio
    if not ventas_mwh.empty and not ventas_soles.empty:
        merged = ventas_mwh.merge(ventas_soles, on=["cliente", "periodo"], how="outer")
        merged["precio_medio_soles_mwh"] = merged.apply(
            lambda row: row["soles"] / row["mwh"] if pd.notna(row["mwh"]) and row["mwh"] else None,
            axis=1,
        )
        precio_medio = merged[["periodo", "precio_medio_soles_mwh"]].dropna(subset=["periodo"])

    safe_write_csv(ventas_mwh, DATA_MART / OUTPUT_FILES["ventas_mensual_mwh"])
    safe_write_csv(ventas_soles, DATA_MART / OUTPUT_FILES["ventas_mensual_soles"])
    safe_write_csv(ingresos, DATA_MART / OUTPUT_FILES["ingresos_mensual"])
    safe_write_csv(precio_medio, DATA_MART / OUTPUT_FILES["precio_medio_mensual"])

    datasets["ventas_mensual_mwh"] = (ventas_mwh, ["cliente", "periodo"])
    datasets["ventas_mensual_soles"] = (ventas_soles, ["cliente", "periodo"])
    datasets["ingresos_mensual"] = (ingresos, ["concepto", "periodo"])
    datasets["precio_medio_mensual"] = (precio_medio, ["periodo"])

    return files_read, datasets


__all__ = ["run_facturacion"]
