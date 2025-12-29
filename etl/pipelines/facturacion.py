# -*- coding: utf-8 -*-

"""Pipeline de facturación y precios."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import pandas as pd

from ..config import DATA_LANDING, DATA_MART, LANDING_FILES, OUTPUT_FILES
from ..utils_io import list_matching_files, read_excel_safe, safe_write_csv

logger = logging.getLogger(__name__)


def _tidy_sheet(df: pd.DataFrame, value_name: str) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["periodo", value_name])
    first_col = df.columns[0]
    df = df.rename(columns={first_col: "concepto"})
    month_cols = [c for c in df.columns if c != "concepto"]
    out = df.melt(id_vars=["concepto"], value_vars=month_cols, var_name="periodo", value_name=value_name)
    out["periodo"] = out["periodo"].astype(str)
    out[value_name] = pd.to_numeric(out[value_name], errors="coerce")
    return out


def _get_sheet(path: Path, candidates: List[str]) -> pd.DataFrame:
    try:
        xls = pd.ExcelFile(path)
    except Exception as exc:
        logger.warning("No se pudo abrir %s: %s", path, exc)
        return pd.DataFrame()

    for name in candidates:
        for sheet in xls.sheet_names:
            if sheet.lower() == name.lower():
                return read_excel_safe(path, sheet_name=sheet)
    return pd.DataFrame()


def run_facturacion() -> Tuple[List[Path], Dict[str, Tuple[pd.DataFrame, Iterable[str]]]]:
    """Ejecutar pipeline de facturación."""

    files_read: List[Path] = []
    datasets: Dict[str, Tuple[pd.DataFrame, Iterable[str]]] = {}

    fact_files = list_matching_files(DATA_LANDING, LANDING_FILES["facturacion"])
    ventas_mwh = pd.DataFrame(columns=["concepto", "periodo", "mwh"])
    ventas_soles = pd.DataFrame(columns=["concepto", "periodo", "soles"])
    ingresos = pd.DataFrame(columns=["concepto", "periodo", "soles"])
    precio_medio = pd.DataFrame(columns=["periodo", "precio_medio_soles_mwh"])

    if fact_files:
        path = fact_files[0]
        files_read.append(path)
        ventas_mwh = _tidy_sheet(_get_sheet(path, ["VENTAS (MWh)", "VENTAS MWH"]), "mwh")
        ventas_soles = _tidy_sheet(_get_sheet(path, ["VENTAS (S)", "VENTAS S"]), "soles")
        ingresos = _tidy_sheet(_get_sheet(path, ["Ingresos", "Balance"]), "soles")

    # Calcular precio medio
    if not ventas_mwh.empty and not ventas_soles.empty:
        merged = ventas_mwh.merge(ventas_soles, on=["concepto", "periodo"], how="outer")
        merged["precio_medio_soles_mwh"] = merged["soles"] / merged["mwh"]
        precio_medio = merged[["periodo", "precio_medio_soles_mwh"]]

    safe_write_csv(ventas_mwh, DATA_MART / OUTPUT_FILES["ventas_mensual_mwh"])
    safe_write_csv(ventas_soles, DATA_MART / OUTPUT_FILES["ventas_mensual_soles"])
    safe_write_csv(ingresos, DATA_MART / OUTPUT_FILES["ingresos_mensual"])
    safe_write_csv(precio_medio, DATA_MART / OUTPUT_FILES["precio_medio_mensual"])

    datasets["ventas_mensual_mwh"] = (ventas_mwh, ["concepto", "periodo"])
    datasets["ventas_mensual_soles"] = (ventas_soles, ["concepto", "periodo"])
    datasets["ingresos_mensual"] = (ingresos, ["concepto", "periodo"])
    datasets["precio_medio_mensual"] = (precio_medio, ["periodo"])

    return files_read, datasets


__all__ = ["run_facturacion"]
