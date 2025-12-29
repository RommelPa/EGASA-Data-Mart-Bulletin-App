# -*- coding: utf-8 -*-

"""Pipeline de producción (histórico y 15-min)."""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import pandas as pd

from ..config import DATA_LANDING, DATA_MART, DATA_REFERENCE, LANDING_FILES, OUTPUT_FILES
from ..utils_cleaning import load_centrales_reference, map_central_id
from ..utils_io import list_matching_files, read_excel_safe, safe_write_csv

logger = logging.getLogger(__name__)


CENTRALES_DEFAULT = [
    ("CH1", "CHARCANI I", "HIDRO", 1905, 1.76, "SUR"),
    ("CH2", "CHARCANI II", "HIDRO", 1912, 0.79, "SUR"),
    ("CH3", "CHARCANI III", "HIDRO", 1938, 4.56, "SUR"),
    ("CH4", "CHARCANI IV", "HIDRO", 1959, 14.40, "SUR"),
    ("CH5", "CHARCANI V", "HIDRO", 1989, 145.35, "SUR"),
    ("CH6", "CHARCANI VI", "HIDRO", 1976, 8.96, "SUR"),
    ("CT1", "C.T. CHILINA", "TERMICA", 1981, 22.00, "SUR"),
    ("CT2", "C.T. PISCO", "TERMICA", 2010, 74.80, "SUR"),
    ("CT3", "C.T. MOLLENDO", "TERMICA", 1997, 31.71, "SUR"),
]


def ensure_centrales_reference() -> Path:
    """Crear archivo de referencia de centrales si no existe."""

    path = DATA_REFERENCE / "centrales_egasa.csv"
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        df = pd.DataFrame(
            CENTRALES_DEFAULT,
            columns=[
                "central_id",
                "central_nombre",
                "tipo",
                "anio_puesta",
                "potencia_mw",
                "zona",
            ],
        )
        df.to_csv(path, index=False, encoding="utf-8")
        logger.info("Referencia de centrales creada en %s", path)
    return path


def _process_historico(path: Path, centrales_df: pd.DataFrame) -> pd.DataFrame:
    """Procesar energía mensual desde Excel histórico."""

    xls = pd.ExcelFile(path)
    frames: List[pd.DataFrame] = []
    for sheet in xls.sheet_names:
        try:
            year = int(str(sheet)[:4])
        except ValueError:
            continue
        if year < 2010 or year > 2025:
            continue

        df_sheet = read_excel_safe(path, sheet_name=sheet, expected_columns=["Central"])
        if df_sheet.empty:
            continue

        first_col = df_sheet.columns[0]
        df_sheet = df_sheet.rename(columns={first_col: "central"})
        month_cols = [c for c in df_sheet.columns if c != "central"]
        df_melt = df_sheet.melt(id_vars=["central"], value_vars=month_cols, var_name="mes", value_name="energia_kwh")
        df_melt = df_melt.dropna(subset=["central"])
        df_melt["energia_mwh"] = pd.to_numeric(df_melt["energia_kwh"], errors="coerce") / 1000
        df_melt["periodo"] = df_melt["mes"].apply(lambda m: f"{year}-{int(m):02d}" if str(m).isdigit() else f"{year}-{m}")
        df_melt = map_central_id(df_melt, centrales_df, source_col="central")
        frames.append(df_melt[["central_id", "central", "periodo", "energia_mwh"]])

    if frames:
        return pd.concat(frames, ignore_index=True)

    return pd.DataFrame(columns=["central_id", "central", "periodo", "energia_mwh"])


def _parse_timestamp(row: pd.Series) -> pd.Timestamp | None:
    """Combinar FECHA y HORA en un timestamp."""

    fecha = row.get("FECHA")
    hora = row.get("HORA")
    if pd.isna(fecha):
        return None
    try:
        ts = pd.to_datetime(f"{fecha} {hora}")
        return ts
    except Exception:
        return None


def _process_15min(path: Path, centrales_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """Procesar archivos 15-min y retornarlos particionados por mes (yyyymm)."""

    df = read_excel_safe(path, expected_columns=["FECHA", "HORA"])
    if df.empty:
        return {}

    if "FECHA" not in df.columns:
        df.rename(columns={df.columns[0]: "FECHA"}, inplace=True)
    if "HORA" not in df.columns and len(df.columns) > 1:
        df.rename(columns={df.columns[1]: "HORA"}, inplace=True)

    energy_cols = [c for c in df.columns if c not in {"FECHA", "HORA"}]
    records: List[pd.DataFrame] = []
    for col in energy_cols:
        tmp = df[["FECHA", "HORA", col]].copy()
        tmp["timestamp"] = tmp.apply(_parse_timestamp, axis=1)
        tmp = tmp.dropna(subset=["timestamp"])
        tmp["energia_mwh"] = pd.to_numeric(tmp[col], errors="coerce") / 1000
        central_parts = str(col).replace("kWh", "").replace("(kWh)", "").strip().split()
        central_name = " ".join(central_parts[:-1]) if len(central_parts) > 1 else central_parts[0] if central_parts else "DESCONOCIDO"
        unidad = central_parts[-1] if len(central_parts) > 1 else "U1"
        tmp["central"] = central_name
        tmp["unidad"] = unidad
        tmp = map_central_id(tmp.rename(columns={"timestamp": "fecha_hora"}), centrales_df, source_col="central")
        records.append(tmp[["fecha_hora", "central_id", "central", "unidad", "energia_mwh"]])

    if not records:
        return {}

    df_all = pd.concat(records, ignore_index=True)
    df_all["fecha_hora"] = pd.to_datetime(df_all["fecha_hora"], errors="coerce")
    df_all.drop_duplicates(subset=["fecha_hora", "central_id", "unidad"], inplace=True)
    df_all["yyyymm"] = df_all["fecha_hora"].dt.strftime("%Y%m")

    result: Dict[str, pd.DataFrame] = {}
    for yyyymm, group in df_all.groupby("yyyymm"):
        result[yyyymm] = group.drop(columns=["yyyymm"]).sort_values("fecha_hora")
    return result


def run_produccion() -> Tuple[pd.DataFrame, List[Path], Dict[str, Tuple[pd.DataFrame, Iterable[str]]]]:
    """Ejecutar pipelines de producción."""

    files_read: List[Path] = []
    datasets: Dict[str, Tuple[pd.DataFrame, Iterable[str]]] = {}

    ref_path = ensure_centrales_reference()
    centrales_df = load_centrales_reference(ref_path)
    files_read.append(ref_path)

    # Producción histórica
    historicos = list_matching_files(DATA_LANDING, LANDING_FILES["produccion_historica"])
    historico_df = pd.DataFrame(columns=["central_id", "central", "periodo", "energia_mwh"])
    if historicos:
        historico_df = _process_historico(historicos[0], centrales_df)
        files_read.append(historicos[0])
    safe_write_csv(historico_df, DATA_MART / OUTPUT_FILES["generacion_mensual"])
    datasets["generacion_mensual"] = (historico_df, ["central_id", "periodo"])

    # Producción 15min
    archivos_15 = list_matching_files(DATA_LANDING, LANDING_FILES["produccion_15min"])
    if archivos_15:
        files_read.append(archivos_15[0])
        particiones = _process_15min(archivos_15[0], centrales_df)
    else:
        particiones = {}

    if not particiones:
        yyyymm = datetime.utcnow().strftime("%Y%m")
        particiones[yyyymm] = pd.DataFrame(
            columns=["fecha_hora", "central_id", "central", "unidad", "energia_mwh"]
        )

    for yyyymm, df_part in particiones.items():
        path = DATA_MART / OUTPUT_FILES["generacion_15min_template"].format(yyyymm=yyyymm)
        safe_write_csv(df_part, path)
        datasets[f"generacion_15min_{yyyymm}"] = (df_part, ["fecha_hora", "central_id", "unidad"])

    return historico_df, files_read, datasets


__all__ = ["run_produccion"]
