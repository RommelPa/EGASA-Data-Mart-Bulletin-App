# -*- coding: utf-8 -*-

"""Pipeline de facturación y precios."""

from __future__ import annotations

import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import pandas as pd

from ..config import DATA_LANDING, DATA_MART, LANDING_FILES, OUTPUT_FILES, get_source
from ..utils_io import detect_header_row, list_matching_files, apply_table_rules, validate_and_write

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


def _normalize_sheet_name(name: str) -> str:
    """Normalizar nombre de hoja para comparación insensible a espacios/casos."""

    return re.sub(r"[^A-Z0-9]", "", str(name).upper())


def _find_sheet(path: Path, target: str) -> str | None:
    """Buscar hoja por nombre normalizado."""

    xls = pd.ExcelFile(path)
    target_norm = _normalize_sheet_name(target)
    for sheet in xls.sheet_names:
        normalized = _normalize_sheet_name(sheet)
        if normalized == target_norm or normalized.startswith(target_norm):
            return sheet
    return None


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


def _extract_year_from_filename(path: Path) -> int | None:
    """Intentar extraer año (4 dígitos) desde el nombre del archivo."""

    match = re.search(r"(20\d{2})", path.name)
    return int(match.group(1)) if match else None


def _aggregate_sales(df: pd.DataFrame, value_name: str) -> pd.DataFrame:
    """Agrupar ventas por periodo (anio/mes) y cliente."""

    if df.empty:
        return pd.DataFrame(columns=["cliente", "periodo", "anio", "mes", value_name])

    df_clean = df.copy()
    df_clean[value_name] = pd.to_numeric(df_clean[value_name], errors="coerce")
    df_clean = df_clean.dropna(subset=["periodo", "cliente", value_name])
    df_clean["periodo"] = df_clean["periodo"].astype(str).str.strip()
    df_clean = df_clean[df_clean["periodo"].str.len() == 6]
    df_clean["anio"] = pd.to_numeric(df_clean["periodo"].str[:4], errors="coerce").astype("Int64")
    df_clean["mes"] = pd.to_numeric(df_clean["periodo"].str[4:6], errors="coerce").astype("Int64")
    df_clean = df_clean.dropna(subset=["anio", "mes"])
    df_clean["anio"] = df_clean["anio"].astype(int)
    df_clean["mes"] = df_clean["mes"].astype(int)
    df_clean["cliente"] = df_clean["cliente"].astype(str).str.strip()
    df_clean = df_clean[df_clean["cliente"] != ""]

    grouped = (
        df_clean.groupby(["anio", "mes", "cliente"], as_index=False)[value_name]
        .sum()
        .assign(periodo=lambda d: d.apply(lambda row: f"{int(row['anio'])}{int(row['mes']):02d}", axis=1))
    )
    return grouped[["cliente", "periodo", "anio", "mes", value_name]]


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


def _parse_ingresos(df: pd.DataFrame, year: int | None) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["anio", "mes", "cliente_o_concepto", "soles"])

    df = df.dropna(axis=1, how="all")
    df = df.rename(columns=lambda c: c if isinstance(c, (pd.Timestamp, datetime)) else str(c).strip().upper())

    month_cols = [
        c
        for c in df.columns
        if (isinstance(c, (pd.Timestamp, datetime)) or str(c).upper() in MONTH_MAP) and pd.notna(c)
    ]
    if not month_cols:
        logger.warning("No se detectaron columnas de meses en Ingresos")
        return pd.DataFrame(columns=["anio", "mes", "cliente_o_concepto", "soles"])

    id_candidates = [c for c in df.columns if c not in month_cols]
    entity_col = None
    for candidate in id_candidates:
        if df[candidate].notna().any():
            entity_col = candidate
            break
    if entity_col is None:
        logger.warning("No se pudo identificar columna de concepto en Ingresos")
        return pd.DataFrame(columns=["anio", "mes", "cliente_o_concepto", "soles"])

    df_long = df.melt(id_vars=[entity_col], value_vars=month_cols, var_name="mes_raw", value_name="soles")
    df_long = df_long.rename(columns={entity_col: "cliente_o_concepto"})

    def _month_from_header(value: object) -> int | None:
        if isinstance(value, (pd.Timestamp, datetime)):
            return int(value.month)
        text = str(value).strip().upper()
        return int(MONTH_MAP[text]) if text in MONTH_MAP else None

    df_long["mes"] = df_long["mes_raw"].map(_month_from_header)
    anio_val = year or datetime.utcnow().year
    df_long["soles"] = pd.to_numeric(df_long["soles"], errors="coerce")
    df_long["cliente_o_concepto"] = df_long["cliente_o_concepto"].astype(str).str.strip()
    df_long["anio"] = anio_val
    df_long = df_long.dropna(subset=["cliente_o_concepto", "mes", "anio"])
    df_long["mes"] = df_long["mes"].astype(int)

    df_long["cliente_o_concepto_lower"] = df_long["cliente_o_concepto"].str.lower()
    df_long = df_long[df_long["cliente_o_concepto"] != ""]
    df_long = df_long[~df_long["cliente_o_concepto_lower"].isin({"nan", "none"})]
    df_long = df_long[~df_long["cliente_o_concepto"].str.contains("TOTAL", case=False, na=False)]
    df_long = df_long[~df_long["cliente_o_concepto"].str.contains("INGRESOS", case=False, na=False)]
    df_long = df_long.dropna(subset=["soles"])
    df_grouped = (
        df_long.groupby(["anio", "mes", "cliente_o_concepto"], as_index=False)["soles"].sum()
    )

    return df_grouped[["anio", "mes", "cliente_o_concepto", "soles"]]


def run_facturacion() -> Tuple[List[Path], Dict[str, Tuple[pd.DataFrame, Iterable[str]]]]:
    """Ejecutar pipeline de facturación."""

    files_read: List[Path] = []
    datasets: Dict[str, Tuple[pd.DataFrame, Iterable[str]]] = {}

    fact_cfg = get_source("facturacion")
    fact_files = list_matching_files(DATA_LANDING, LANDING_FILES["facturacion"])
    ventas_mwh = pd.DataFrame(columns=["cliente", "periodo", "mwh"])
    ventas_soles = pd.DataFrame(columns=["cliente", "periodo", "soles"])
    ingresos = pd.DataFrame(columns=["anio", "mes", "cliente_o_concepto", "soles"])
    precio_medio = pd.DataFrame(columns=["periodo", "anio", "mes", "cliente", "precio_medio_soles_mwh"])

    if fact_files:
        path = fact_files[0]
        files_read.append(path)
        sheets_cfg = (fact_cfg or {}).get("sheets", {})

        try:
            sheet_name = sheets_cfg.get("ventas_mwh", "VENTAS (MWh)")
            ventas_mwh_sheet = _read_with_header(path, sheet_name, ["cliente", "enero"])
            ventas_mwh = _parse_sales(ventas_mwh_sheet, "mwh")
        except Exception:
            logger.exception("Error procesando hoja de ventas MWh (%s)", sheet_name)
            raise ValueError(f"No se pudo procesar hoja de ventas MWh '{sheet_name}' en {path.name}")

        try:
            sheet_name = sheets_cfg.get("ventas_soles", "VENTAS (S)")
            ventas_soles_sheet = _read_with_header(path, sheet_name, ["cliente", "enero"])
            ventas_soles = _parse_sales(ventas_soles_sheet, "soles")
        except Exception:
            logger.exception("Error procesando hoja de ventas S (%s)", sheet_name)
            raise ValueError(f"No se pudo procesar hoja de ventas S '{sheet_name}' en {path.name}")

        try:
            ingresos_sheet_name = sheets_cfg.get("ingresos") or _find_sheet(path, "Ingresos")
            if ingresos_sheet_name:
                ingresos_sheet = _read_with_header(path, ingresos_sheet_name, ["enero"])
                ingresos = _parse_ingresos(ingresos_sheet, year=_extract_year_from_filename(path))
            else:
                logger.warning("Hoja Ingresos no encontrada en %s", path)
        except Exception:
            logger.exception("Error procesando hoja Ingresos")
            raise ValueError(f"No se pudo procesar hoja de Ingresos en {path.name}")
    elif (fact_cfg or {}).get("required", True):
        raise FileNotFoundError(f"No se encontró archivo de facturación en {DATA_LANDING}")

    # Calcular precio medio
    ventas_mwh_agg = _aggregate_sales(ventas_mwh, "mwh")
    ventas_soles_agg = _aggregate_sales(ventas_soles, "soles")
    if not ventas_mwh_agg.empty or not ventas_soles_agg.empty:
        merged = ventas_mwh_agg.merge(
            ventas_soles_agg, on=["anio", "mes", "cliente", "periodo"], how="outer"
        )
        merged["precio_medio_soles_mwh"] = merged.apply(
            lambda row: row["soles"] / row["mwh"] if pd.notna(row["mwh"]) and row["mwh"] else None,
            axis=1,
        )
        precio_medio = merged.dropna(subset=["periodo"]).drop_duplicates(
            subset=["anio", "mes", "cliente"]
        )[["periodo", "anio", "mes", "cliente", "precio_medio_soles_mwh"]]

    ventas_mwh = apply_table_rules("ventas_mensual_mwh", ventas_mwh)
    ventas_soles = apply_table_rules("ventas_mensual_soles", ventas_soles)
    ingresos = apply_table_rules("ingresos_mensual", ingresos)

    validate_and_write("ventas_mensual_mwh", ventas_mwh, DATA_MART / OUTPUT_FILES["ventas_mensual_mwh"])
    validate_and_write("ventas_mensual_soles", ventas_soles, DATA_MART / OUTPUT_FILES["ventas_mensual_soles"])
    validate_and_write("ingresos_mensual", ingresos, DATA_MART / OUTPUT_FILES["ingresos_mensual"])
    validate_and_write("precio_medio_mensual", precio_medio, DATA_MART / OUTPUT_FILES["precio_medio_mensual"])

    datasets["ventas_mensual_mwh"] = (ventas_mwh, ["cliente", "periodo"])
    datasets["ventas_mensual_soles"] = (ventas_soles, ["cliente", "periodo"])
    datasets["ingresos_mensual"] = (ingresos, ["anio", "mes", "cliente_o_concepto"])
    datasets["precio_medio_mensual"] = (precio_medio, ["periodo", "cliente"])

    return files_read, datasets


__all__ = ["run_facturacion"]
