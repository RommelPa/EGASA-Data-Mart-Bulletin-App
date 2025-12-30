# -*- coding: utf-8 -*-
"""Pipeline Balance Energía: hojas Perfil y R."""

from __future__ import annotations

import datetime as dt
import logging
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import pandas as pd

from ..config import DATA_LANDING, DATA_MART, LANDING_FILES, OUTPUT_FILES
from ..utils_io import list_matching_files, safe_write_csv

logger = logging.getLogger(__name__)


PERFIL_CONCEPTOS = {
    "PRODUCCION HIDRAULICA",
    "PRODUCCION TERMICA",
    "COMPRA DE ENERGIA",
    "CONSUMOS AUX.",
    "PERDIDAS",
    "ENERGIA DISPONIBLE",
    "VENTA DE ENERGIA",
    "VENTA EN COES",
    "CONTRATOS",
}

SEGMENTOS_R = {"COES", "REGULADOS", "LIBRES", "TOTAL"}


def _find_header_row(preview: pd.DataFrame, keyword: str) -> int:
    """Encuentra fila header buscando una palabra clave exacta (case-insensitive)."""
    key = keyword.strip().upper()
    for i in range(min(len(preview), 120)):
        row_vals = [str(v).strip().upper() for v in preview.iloc[i].tolist() if pd.notna(v)]
        if any(key == v for v in row_vals):
            return int(i)
    return 0


def _normalize_columns_keep_dates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpia nombres de columnas sin convertir Timestamps a string
    (si los conviertes, ya no podrás detectar columnas mensuales).
    """
    new_cols = []
    for c in df.columns:
        if isinstance(c, (pd.Timestamp, dt.datetime)):
            new_cols.append(c)
        else:
            new_cols.append(str(c).strip())
    df = df.copy()
    df.columns = new_cols
    return df


def _date_cols(cols: Iterable[object]) -> List[object]:
    """Devuelve columnas que son fechas tipo datetime/pandas Timestamp."""
    out: List[object] = []
    for c in cols:
        if isinstance(c, (pd.Timestamp, dt.datetime)):
            out.append(c)
    return out


def _process_perfil(path: Path) -> pd.DataFrame:
    """
    Lee hoja Perfil (valores en GWh mensual) y normaliza a formato largo:
    periodo (YYYYMM), fecha_mes, concepto, energia_mwh, energia_gwh
    """
    preview = pd.read_excel(path, sheet_name="Perfil", header=None, nrows=60)
    header_row = _find_header_row(preview, "Concepto")

    df = pd.read_excel(path, sheet_name="Perfil", header=header_row)
    if df.empty:
        return pd.DataFrame(columns=["periodo", "fecha_mes", "concepto", "energia_mwh", "energia_gwh"])

    df = _normalize_columns_keep_dates(df)

    # Columna concepto
    concepto_col = next((c for c in df.columns if isinstance(c, str) and c.strip().upper() == "CONCEPTO"), None)
    if concepto_col is None:
        logger.warning("Perfil: no se encontró columna Concepto.")
        return pd.DataFrame(columns=["periodo", "fecha_mes", "concepto", "energia_mwh", "energia_gwh"])

    df = df.rename(columns={concepto_col: "concepto"})
    df = df.dropna(subset=["concepto"]).copy()

    df["concepto"] = df["concepto"].astype(str).str.strip()
    # typo típico
    df["concepto"] = df["concepto"].str.replace("EERGIA DISPONIBLE", "ENERGIA DISPONIBLE", case=False, regex=False)

    df["concepto_norm"] = df["concepto"].str.upper()
    df = df[df["concepto_norm"].isin(PERFIL_CONCEPTOS)].copy()

    if df.empty:
        return pd.DataFrame(columns=["periodo", "fecha_mes", "concepto", "energia_mwh", "energia_gwh"])

    # Opción A: si hay repetidos por concepto, nos quedamos con el primero
    df = df.drop_duplicates(subset=["concepto_norm"], keep="first")

    # Columnas mensuales (datetime)
    cols_fechas = _date_cols(df.columns)
    if not cols_fechas:
        logger.warning("Perfil: no se detectaron columnas datetime (meses).")
        return pd.DataFrame(columns=["periodo", "fecha_mes", "concepto", "energia_mwh", "energia_gwh"])

    df_long = df.melt(
        id_vars=["concepto_norm"],
        value_vars=cols_fechas,
        var_name="fecha_mes",
        value_name="energia_gwh",
    )

    df_long["energia_gwh"] = pd.to_numeric(df_long["energia_gwh"], errors="coerce")
    df_long = df_long.dropna(subset=["fecha_mes", "energia_gwh"]).copy()

    df_long["energia_mwh"] = df_long["energia_gwh"] * 1000.0
    df_long["periodo"] = pd.to_datetime(df_long["fecha_mes"]).dt.strftime("%Y%m")
    df_long = df_long.rename(columns={"concepto_norm": "concepto"})

    df_long = df_long[["periodo", "fecha_mes", "concepto", "energia_mwh", "energia_gwh"]].sort_values(
        ["periodo", "concepto"]
    )

    # unicidad
    df_long = df_long.drop_duplicates(subset=["periodo", "concepto"], keep="last")

    return df_long.reset_index(drop=True)


def _process_r(path: Path) -> pd.DataFrame:
    """
    Lee hoja R (segmentos COES/Regulados/Libres/Total, MWh mensual) y normaliza.
    """
    preview = pd.read_excel(path, sheet_name="R", header=None, nrows=120)
    header_row = _find_header_row(preview, "Año")

    df = pd.read_excel(path, sheet_name="R", header=header_row)
    if df.empty:
        return pd.DataFrame(columns=["periodo", "fecha_mes", "segmento", "energia_mwh"])

    df = _normalize_columns_keep_dates(df)

    # Columna segmento (en el archivo es "Año")
    seg_col = next((c for c in df.columns if isinstance(c, str) and c.strip().upper() in {"AÑO", "ANO"}), None)
    if seg_col is None:
        # fallback: primera columna tipo texto que no sea Unnamed
        text_cols = [
            c for c in df.columns
            if isinstance(c, str) and not c.strip().upper().startswith("UNNAMED")
        ]
        seg_col = text_cols[0] if text_cols else df.columns[0]

    df = df.rename(columns={seg_col: "segmento"})
    df = df.dropna(subset=["segmento"]).copy()
    df["segmento"] = df["segmento"].astype(str).str.strip()
    df["segmento_norm"] = df["segmento"].str.upper()

    df = df[df["segmento_norm"].isin(SEGMENTOS_R)].copy()
    if df.empty:
        return pd.DataFrame(columns=["periodo", "fecha_mes", "segmento", "energia_mwh"])

    # Columnas mensuales (datetime)
    cols_fechas = _date_cols(df.columns)
    if not cols_fechas:
        logger.warning("R: no se detectaron columnas datetime (meses).")
        return pd.DataFrame(columns=["periodo", "fecha_mes", "segmento", "energia_mwh"])

    df_long = df.melt(
        id_vars=["segmento_norm"],
        value_vars=cols_fechas,
        var_name="fecha_mes",
        value_name="energia_mwh",
    )

    df_long["energia_mwh"] = pd.to_numeric(df_long["energia_mwh"], errors="coerce")
    df_long = df_long.dropna(subset=["fecha_mes", "energia_mwh"]).copy()

    df_long["periodo"] = pd.to_datetime(df_long["fecha_mes"]).dt.strftime("%Y%m")
    df_long = df_long.rename(columns={"segmento_norm": "segmento"})

    df_long = df_long[["periodo", "fecha_mes", "segmento", "energia_mwh"]].sort_values(["periodo", "segmento"])
    df_long = df_long.drop_duplicates(subset=["periodo", "segmento"], keep="last")

    return df_long.reset_index(drop=True)


def run_balance_energia() -> Tuple[List[Path], Dict[str, Tuple[pd.DataFrame, Iterable[str]]]]:
    """Ejecutar pipeline Balance Energía (Perfil y R)."""
    files_read: List[Path] = []
    datasets: Dict[str, Tuple[pd.DataFrame, Iterable[str]]] = {}

    balance_files = list_matching_files(DATA_LANDING, LANDING_FILES["balance_energia"])
    if not balance_files:
        logger.info("No se encontró archivo balance en data_landing.")
        empty1 = pd.DataFrame(columns=["periodo", "fecha_mes", "concepto", "energia_mwh", "energia_gwh"])
        empty2 = pd.DataFrame(columns=["periodo", "fecha_mes", "segmento", "energia_mwh"])
        safe_write_csv(empty1, DATA_MART / OUTPUT_FILES["balance_perfil_mensual"])
        safe_write_csv(empty2, DATA_MART / OUTPUT_FILES["balance_r_mensual"])
        datasets["balance_perfil_mensual"] = (empty1, ["periodo", "concepto"])
        datasets["balance_r_mensual"] = (empty2, ["periodo", "segmento"])
        return files_read, datasets

    path = balance_files[0]
    files_read.append(path)

    perfil_df = _process_perfil(path)
    r_df = _process_r(path)

    safe_write_csv(perfil_df, DATA_MART / OUTPUT_FILES["balance_perfil_mensual"])
    safe_write_csv(r_df, DATA_MART / OUTPUT_FILES["balance_r_mensual"])

    datasets["balance_perfil_mensual"] = (perfil_df, ["periodo", "concepto"])
    datasets["balance_r_mensual"] = (r_df, ["periodo", "segmento"])

    return files_read, datasets


__all__ = ["run_balance_energia"]