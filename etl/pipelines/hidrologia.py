# -*- coding: utf-8 -*-

"""Pipeline de hidrología."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import pandas as pd

from ..config import DATA_LANDING, DATA_MART, LANDING_FILES, OUTPUT_FILES
from ..utils_io import detect_header_row, list_matching_files, read_excel_safe, safe_write_csv

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


def _melt_monthly(df: pd.DataFrame, id_var: str, value_name: str) -> pd.DataFrame:
    month_cols = [c for c in df.columns if any(m in str(c).upper() for m in MONTH_MAP)]
    df_out = df.melt(id_vars=[id_var], value_vars=month_cols, var_name="mes_raw", value_name=value_name)
    df_out[id_var] = pd.to_numeric(df_out[id_var], errors="coerce")
    df_out["mes_raw"] = df_out["mes_raw"].astype(str).str.upper().str.strip()
    df_out["mes"] = df_out["mes_raw"].map(MONTH_MAP)
    df_out = df_out.dropna(subset=["mes", id_var])
    df_out[value_name] = pd.to_numeric(df_out[value_name], errors="coerce")
    return df_out


def _procesar_control(path: Path) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Procesar archivo Control Hidrológico."""

    volumen_df = pd.DataFrame(columns=["reservorio", "anio", "mes", "volumen_000m3"])
    caudal_df = pd.DataFrame(columns=["estacion", "anio", "mes", "caudal_m3s"])

    try:
        xls = pd.ExcelFile(path)
    except Exception:
        logger.exception("No se pudo leer %s", path)
        raise

    volumen_sheets = ["AB", "EF", "EP", "PI", "CH", "BA", "TOTAL"]
    vol_frames: List[pd.DataFrame] = []
    for sheet in xls.sheet_names:
        if sheet.upper() not in {s.upper() for s in volumen_sheets}:
            continue
        preview = pd.read_excel(path, sheet_name=sheet, header=None, nrows=40)
        header_row = detect_header_row(preview, keywords=["año", "enero"])
        df_vol = pd.read_excel(path, sheet_name=sheet, header=header_row)
        if df_vol.empty:
            logger.warning("Hoja %s sin datos en control hidrológico", sheet)
            continue
        anio_col = next((c for c in df_vol.columns if str(c).upper().startswith("AÑO")), df_vol.columns[0])
        df_vol = df_vol.rename(columns={anio_col: "anio"})
        df_vol = _melt_monthly(df_vol, id_var="anio", value_name="volumen_000m3")
        df_vol["reservorio"] = sheet.upper()
        vol_frames.append(df_vol[["reservorio", "anio", "mes", "volumen_000m3"]])

    if vol_frames:
        volumen_df = pd.concat(vol_frames, ignore_index=True)

    if "CAUDAL" in [s.upper() for s in xls.sheet_names]:
        sheet = [s for s in xls.sheet_names if s.upper() == "CAUDAL"][0]
        preview = pd.read_excel(path, sheet_name=sheet, header=None, nrows=40)
        header_row = detect_header_row(preview, keywords=["año", "enero"])
        df_cau = pd.read_excel(path, sheet_name=sheet, header=header_row)
        if "AÑO" not in df_cau.columns:
            logger.warning("Hoja CAUDAL sin columna AÑO")
        else:
            df_cau = df_cau.rename(columns={"AÑO": "anio"})
            df_cau = _melt_monthly(df_cau, id_var="anio", value_name="caudal_m3s")
            df_cau["estacion"] = "Aguada Blanca"
            caudal_df = df_cau[["estacion", "anio", "mes", "caudal_m3s"]]

    return volumen_df, caudal_df


def _procesar_represas(path: Path) -> pd.DataFrame:
    """Procesar archivo BDREPRESAS."""

    try:
        preview = pd.read_excel(path, sheet_name="INFORMEDIARIO", header=None, nrows=80)
    except Exception:
        logger.exception("No se pudo abrir hoja INFORMEDIARIO en %s", path)
        raise

    header_row = None
    for idx, row in preview.iterrows():
        vals = [str(v).upper() for v in row if pd.notna(v)]
        if "REPRESA" in vals and any("CAPACIDAD" in v for v in vals):
            header_row = idx
            break
    if header_row is None:
        header_row = detect_header_row(preview, keywords=["represa"])
    df = pd.read_excel(path, sheet_name="INFORMEDIARIO", header=header_row)
    df = df.rename(columns=lambda c: str(c).strip())

    # Buscar fecha en columnas o en texto
    fecha_cols = [c for c in df.columns if isinstance(c, str) and any(ch.isdigit() for ch in c)]
    fecha_val = None
    for col in fecha_cols:
        if "20" in str(col):
            parts = str(col).replace(".", "-").replace("/", "-").split("-")
            if len(parts) >= 3:
                try:
                    year, month, day = int(parts[0]), int(parts[1]), int(parts[2])
                    fecha_val = pd.Timestamp(year=year, month=month, day=day)
                    break
                except Exception:
                    continue

    reservorio_col = next((c for c in df.columns if "REPRESA" in str(c).upper()), None)
    if not reservorio_col:
        logger.warning("No se pudo identificar columna de represa en INFORMEDIARIO")
        return pd.DataFrame(columns=["fecha", "reservorio"])

    df = df.rename(columns={reservorio_col: "reservorio"})
    keep_cols = ["reservorio"] + [c for c in df.columns if c not in {"reservorio", "Unnamed: 0"}]
    df_out = df[keep_cols].copy()
    df_out = df_out.dropna(subset=["reservorio"])
    df_out["fecha"] = fecha_val
    numeric_cols = [c for c in df_out.columns if c not in {"reservorio", "fecha"}]
    for col in numeric_cols:
        df_out[col] = pd.to_numeric(df_out[col], errors="coerce")
    cols_order = ["fecha", "reservorio"] + numeric_cols
    return df_out[cols_order]


def run_hidrologia() -> Tuple[List[Path], Dict[str, Tuple[pd.DataFrame, Iterable[str]]]]:
    """Ejecutar pipeline de hidrología."""

    files_read: List[Path] = []
    datasets: Dict[str, Tuple[pd.DataFrame, Iterable[str]]] = {}

    control_files = list_matching_files(DATA_LANDING, LANDING_FILES["hidrologia_control"])
    volumen_df = pd.DataFrame(columns=["reservorio", "anio", "mes", "volumen_000m3"])
    caudal_df = pd.DataFrame(columns=["estacion", "anio", "mes", "caudal_m3s"])
    if control_files:
        try:
            volumen_df, caudal_df = _procesar_control(control_files[0])
        except Exception:
            logger.exception("Error procesando control hidrológico %s", control_files[0])
            raise
        files_read.append(control_files[0])

    if not volumen_df.empty:
        volumen_df["periodo"] = volumen_df["anio"].astype(int).astype(str) + volumen_df["mes"]
    if not caudal_df.empty:
        caudal_df["periodo"] = caudal_df["anio"].astype(int).astype(str) + caudal_df["mes"]

    safe_write_csv(volumen_df, DATA_MART / OUTPUT_FILES["hidro_volumen_mensual"])
    safe_write_csv(caudal_df, DATA_MART / OUTPUT_FILES["hidro_caudal_mensual"])
    datasets["hidro_volumen_mensual"] = (volumen_df, ["reservorio", "periodo"])
    datasets["hidro_caudal_mensual"] = (caudal_df, ["estacion", "periodo"])

    represas_files = list_matching_files(DATA_LANDING, LANDING_FILES["hidrologia_represas"])
    represas_df = pd.DataFrame(columns=["fecha", "reservorio", "nivel", "caudal", "volumen"])
    if represas_files:
        try:
            represas_df = _procesar_represas(represas_files[0])
        except Exception:
            logger.exception("Error procesando represas %s", represas_files[0])
            raise
        files_read.append(represas_files[0])

    safe_write_csv(represas_df, DATA_MART / OUTPUT_FILES["represas_diario"])
    datasets["represas_diario"] = (represas_df, ["fecha", "reservorio"])

    return files_read, datasets


__all__ = ["run_hidrologia"]
