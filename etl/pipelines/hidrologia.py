# -*- coding: utf-8 -*-
"""Pipeline de hidrología."""

from __future__ import annotations

import logging
import re
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

MONTHS_ES = {
    "ENERO": 1,
    "FEBRERO": 2,
    "MARZO": 3,
    "ABRIL": 4,
    "MAYO": 5,
    "JUNIO": 6,
    "JULIO": 7,
    "AGOSTO": 8,
    "SETIEMBRE": 9,
    "SEPTIEMBRE": 9,
    "OCTUBRE": 10,
    "NOVIEMBRE": 11,
    "DICIEMBRE": 12,
}


def _melt_monthly(df: pd.DataFrame, id_var: str, value_name: str) -> pd.DataFrame:
    """Convierte formato ancho (ENE..DIC) a largo."""
    month_cols = []
    for c in df.columns:
        name = str(c).upper().strip()
        if name in MONTH_MAP:
            month_cols.append(c)

    if not month_cols:
        return pd.DataFrame(columns=[id_var, "mes_raw", "mes", value_name])

    df_out = df.melt(
        id_vars=[id_var],
        value_vars=month_cols,
        var_name="mes_raw",
        value_name=value_name,
    )
    df_out[id_var] = pd.to_numeric(df_out[id_var], errors="coerce")
    df_out["mes_raw"] = df_out["mes_raw"].astype(str).str.upper().str.strip()
    df_out["mes"] = df_out["mes_raw"].map(MONTH_MAP)
    df_out = df_out.dropna(subset=["mes", id_var])
    df_out[value_name] = pd.to_numeric(df_out[value_name], errors="coerce")
    return df_out


def _try_parse_date_from_string(s: str) -> pd.Timestamp | None:
    """Intenta parsear fechas comunes (incluye formatos con meses en español)."""
    if not s:
        return None

    text = str(s).strip()
    up = text.upper()

    # 1) yyyy.mm.dd o yyyy-mm-dd o yyyy/mm/dd
    m = re.search(r"\b(20\d{2})[./-](\d{1,2})[./-](\d{1,2})\b", up)
    if m:
        y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
        try:
            return pd.Timestamp(year=y, month=mo, day=d)
        except Exception:
            pass

    # 2) dd.mm.yyyy o dd-mm-yyyy o dd/mm/yyyy
    m = re.search(r"\b(\d{1,2})[./-](\d{1,2})[./-](20\d{2})\b", up)
    if m:
        d, mo, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        try:
            return pd.Timestamp(year=y, month=mo, day=d)
        except Exception:
            pass

    # 3) "AL 11 DE DICIEMBRE DE 2025"
    m = re.search(
        r"\b(?:AL\s*)?(\d{1,2})\s+DE\s+([A-ZÁÉÍÓÚÑ]+)\s+DE\s+(20\d{2})\b",
        up,
    )
    if m:
        d = int(m.group(1))
        mes_txt = (
            m.group(2)
            .replace("Á", "A")
            .replace("É", "E")
            .replace("Í", "I")
            .replace("Ó", "O")
            .replace("Ú", "U")
        )
        y = int(m.group(3))
        mo = MONTHS_ES.get(mes_txt)
        if mo:
            try:
                return pd.Timestamp(year=y, month=mo, day=d)
            except Exception:
                pass

    # 4) "11 DE DICIEMBRE 2025"
    m = re.search(
        r"\b(\d{1,2})\s+DE\s+([A-ZÁÉÍÓÚÑ]+)\s+(20\d{2})\b",
        up,
    )
    if m:
        d = int(m.group(1))
        mes_txt = (
            m.group(2)
            .replace("Á", "A")
            .replace("É", "E")
            .replace("Í", "I")
            .replace("Ó", "O")
            .replace("Ú", "U")
        )
        y = int(m.group(3))
        mo = MONTHS_ES.get(mes_txt)
        if mo:
            try:
                return pd.Timestamp(year=y, month=mo, day=d)
            except Exception:
                pass

    return None


def _extract_report_date_from_text(path: Path) -> pd.Timestamp | None:
    """
    Extrae la fecha del reporte desde el contenido "INFORMEDIARIO":
    - Busca patrones: 'AL 11 DE DICIEMBRE DE 2025'
    - O fechas tipo '2025.12.11' presentes en celdas
    """
    try:
        preview = pd.read_excel(path, sheet_name="INFORMEDIARIO", header=None, nrows=120)
    except Exception:
        logger.exception("No se pudo abrir hoja INFORMEDIARIO para extraer fecha en %s", path)
        return None

    best: pd.Timestamp | None = None
    for v in preview.values.ravel().tolist():
        if pd.isna(v):
            continue
        ts = _try_parse_date_from_string(str(v))
        if ts is not None:
            if best is None or ts > best:
                best = ts

    return best


def _procesar_control(path: Path) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Procesar archivo Control Hidrológico."""
    volumen_df = pd.DataFrame(columns=["reservorio", "anio", "mes", "volumen_000m3"])
    caudal_df = pd.DataFrame(columns=["estacion", "anio", "mes", "caudal_m3s"])

    try:
        xls = pd.ExcelFile(path)
    except Exception:
        logger.exception("No se pudo leer %s", path)
        raise

    volumen_sheets = {"AB", "EF", "EP", "PI", "CH", "BA", "TOTAL"}
    vol_frames: List[pd.DataFrame] = []

    for sheet in xls.sheet_names:
        if sheet.upper() not in volumen_sheets:
            continue

        preview = pd.read_excel(path, sheet_name=sheet, header=None, nrows=60)
        header_row = detect_header_row(preview, keywords=["año", "enero", "febrero"])
        df_vol = pd.read_excel(path, sheet_name=sheet, header=header_row)

        if df_vol.empty:
            logger.warning("Hoja %s sin datos en Control Hidrológico", sheet)
            continue

        anio_col = next((c for c in df_vol.columns if str(c).upper().startswith("AÑO")), df_vol.columns[0])
        df_vol = df_vol.rename(columns={anio_col: "anio"})

        df_long = _melt_monthly(df_vol, id_var="anio", value_name="volumen_000m3")
        if df_long.empty:
            continue

        df_long["reservorio"] = sheet.upper()
        vol_frames.append(df_long[["reservorio", "anio", "mes", "volumen_000m3"]])

    if vol_frames:
        volumen_df = pd.concat(vol_frames, ignore_index=True)

    if any(s.upper() == "CAUDAL" for s in xls.sheet_names):
        sheet = next(s for s in xls.sheet_names if s.upper() == "CAUDAL")
        preview = pd.read_excel(path, sheet_name=sheet, header=None, nrows=80)
        header_row = detect_header_row(preview, keywords=["año", "enero", "febrero"])
        df_cau = pd.read_excel(path, sheet_name=sheet, header=header_row)

        anio_col = next((c for c in df_cau.columns if str(c).upper().startswith("AÑO")), None)
        if anio_col is None:
            logger.warning("Hoja CAUDAL sin columna AÑO")
        else:
            df_cau = df_cau.rename(columns={anio_col: "anio"})
            df_long = _melt_monthly(df_cau, id_var="anio", value_name="caudal_m3s")
            if not df_long.empty:
                df_long["estacion"] = "Aguada Blanca"
                caudal_df = df_long[["estacion", "anio", "mes", "caudal_m3s"]]

    return volumen_df, caudal_df


def _normalize_colname(c: object) -> str:
    text = str(c).replace("\n", " ").replace("\r", " ").strip()
    text = re.sub(r"\s+", " ", text)
    return text


def _procesar_represas(path: Path) -> pd.DataFrame:
    """Procesar BDREPRESAS (INFORMEDIARIO) a tabla limpia para dashboard."""
    try:
        preview = pd.read_excel(path, sheet_name="INFORMEDIARIO", header=None, nrows=180)
    except Exception:
        logger.exception("No se pudo abrir hoja INFORMEDIARIO en %s", path)
        raise

    fecha_val = _extract_report_date_from_text(path)

    # Detectar fila de encabezado real
    header_row = None
    for idx, row in preview.iterrows():
        vals = [str(v).upper().strip() for v in row.tolist() if pd.notna(v)]
        has_rep = any("REPRESA" in v or "RESERVOR" in v for v in vals)
        has_cap_vol = any("CAPAC" in v for v in vals) or any("VOLUMEN" in v for v in vals) or any("%" in v for v in vals)
        if has_rep and has_cap_vol:
            header_row = int(idx)
            break

    if header_row is None:
        header_row = detect_header_row(preview, keywords=["represa", "reservorio", "capacidad", "volumen"])

    df = pd.read_excel(path, sheet_name="INFORMEDIARIO", header=header_row)
    if df.empty:
        return pd.DataFrame(columns=["fecha", "reservorio"])

    # Normalizar nombres
    df = df.rename(columns=lambda c: _normalize_colname(c))

    # Drop columnas 100% NaN
    df = df.dropna(axis=1, how="all")

    # Identificar columna reservorio/represa
    reservorio_col = next((c for c in df.columns if "REPRESA" in c.upper() or "RESERVOR" in c.upper()), None)
    if reservorio_col is None:
        non_numeric_cols = [c for c in df.columns if df[c].dtype == "object"]
        reservorio_col = non_numeric_cols[0] if non_numeric_cols else df.columns[0]

    df = df.rename(columns={reservorio_col: "reservorio"})

    # ✅ IMPORTANTE: dropna ANTES de convertir a string
    df = df.dropna(subset=["reservorio"])
    df["reservorio"] = df["reservorio"].astype(str).str.strip()

    # Filtrar filas basura (títulos/notas)
    bad_patterns = [
        r"^INFORME\s+DIARIO",
        r"VOLUMEN\s+BRUTO",
        r"^CON\s",  # ejemplos como "CON BAMPUTAÑE"
        r"^$",
        r"^NAN$",
    ]
    bad_re = re.compile("|".join(bad_patterns), flags=re.IGNORECASE)

    df_out = df.copy()
    df_out = df_out[~df_out["reservorio"].str.match(bad_re)]
    df_out = df_out.dropna(subset=["reservorio"])

    # Si no hay fecha, intentar derivar desde nombres de columnas
    if fecha_val is None:
        for c in df_out.columns:
            ts = _try_parse_date_from_string(str(c))
            if ts is not None:
                fecha_val = ts
                break

    df_out["fecha"] = fecha_val

    # Convertir numéricos (todas menos fecha/reservorio)
    numeric_cols = [c for c in df_out.columns if c not in {"fecha", "reservorio"}]
    for col in numeric_cols:
        df_out[col] = pd.to_numeric(df_out[col], errors="coerce")

    # --- LIMPIEZA FINAL (tu caso real) ---
    # 1) Eliminar filas de "cabecera dentro del body"
    df_out["reservorio"] = df_out["reservorio"].astype(str).str.strip()
    df_out = df_out[~df_out["reservorio"].str.fullmatch(r"(?i)represa")]
    df_out = df_out[df_out["reservorio"].str.lower() != "nan"]

    # 2) Eliminar columnas Unnamed casi vacías
    drop_cols = []
    for c in df_out.columns:
        if str(c).lower().startswith("unnamed"):
            if df_out[c].isna().mean() > 0.95:
                drop_cols.append(c)
    if drop_cols:
        df_out = df_out.drop(columns=drop_cols)

    # 3) Renombrar columnas por el patrón observado en tu CSV actual
    # Unnamed: 2  -> capacidad_util_max
    # Unnamed: 3  -> volumen_ref (2024.12.11)
    # Unnamed: 4  -> volumen_actual (2025.12.11)
    # Unnamed: 5  -> pct_llenado
    # Unnamed: 6  -> indicador_extra   ✅ (Opción A)
    # Unnamed: 8  -> volumen_dia_anterior_m3
    rename_map: Dict[str, str] = {}
    for c in df_out.columns:
        cl = str(c).strip().lower()
        if cl == "unnamed: 2":
            rename_map[c] = "capacidad_util_max"
        elif cl == "unnamed: 3":
            rename_map[c] = "volumen_ref"
        elif cl == "unnamed: 4":
            rename_map[c] = "volumen_actual"
        elif cl == "unnamed: 5":
            rename_map[c] = "pct_llenado"
        elif cl == "unnamed: 6":
            rename_map[c] = "indicador_extra"
        elif cl == "unnamed: 8":
            rename_map[c] = "volumen_dia_anterior_m3"
    if rename_map:
        df_out = df_out.rename(columns=rename_map)

    # 4) Convertir numéricos conocidos
    for col in [
        "capacidad_util_max",
        "volumen_ref",
        "volumen_actual",
        "pct_llenado",
        "volumen_dia_anterior_m3",
        "indicador_extra",
    ]:
        if col in df_out.columns:
            df_out[col] = pd.to_numeric(df_out[col], errors="coerce")

    # 5) Orden final (solo columnas útiles si existen)
    order = [
        "fecha",
        "reservorio",
        "capacidad_util_max",
        "volumen_ref",
        "volumen_actual",
        "pct_llenado",
        "volumen_dia_anterior_m3",
        "indicador_extra",
    ]
    kept = [c for c in order if c in df_out.columns]
    remaining = [c for c in df_out.columns if c not in kept]
    df_out = df_out[kept + remaining]

    # 6) Dedup final
    if "fecha" in df_out.columns and "reservorio" in df_out.columns:
        df_out = df_out.drop_duplicates(subset=["fecha", "reservorio"], keep="last")

    return df_out.reset_index(drop=True)


def run_hidrologia() -> Tuple[List[Path], Dict[str, Tuple[pd.DataFrame, Iterable[str]]]]:
    """Ejecutar pipeline de hidrología."""
    files_read: List[Path] = []
    datasets: Dict[str, Tuple[pd.DataFrame, Iterable[str]]] = {}

    # Control Hidrológico (mensual)
    control_files = list_matching_files(DATA_LANDING, LANDING_FILES["hidrologia_control"])
    volumen_df = pd.DataFrame(columns=["reservorio", "anio", "mes", "volumen_000m3"])
    caudal_df = pd.DataFrame(columns=["estacion", "anio", "mes", "caudal_m3s"])

    if control_files:
        volumen_df, caudal_df = _procesar_control(control_files[0])
        files_read.append(control_files[0])

    if not volumen_df.empty:
        volumen_df["anio"] = pd.to_numeric(volumen_df["anio"], errors="coerce")
        volumen_df["mes"] = volumen_df["mes"].astype(str).str.zfill(2)
        volumen_df = volumen_df.dropna(subset=["anio", "mes"])
        volumen_df["periodo"] = volumen_df["anio"].astype(int).astype(str) + volumen_df["mes"]

    if not caudal_df.empty:
        caudal_df["anio"] = pd.to_numeric(caudal_df["anio"], errors="coerce")
        caudal_df["mes"] = caudal_df["mes"].astype(str).str.zfill(2)
        caudal_df = caudal_df.dropna(subset=["anio", "mes"])
        caudal_df["periodo"] = caudal_df["anio"].astype(int).astype(str) + caudal_df["mes"]

    safe_write_csv(volumen_df, DATA_MART / OUTPUT_FILES["hidro_volumen_mensual"])
    safe_write_csv(caudal_df, DATA_MART / OUTPUT_FILES["hidro_caudal_mensual"])
    datasets["hidro_volumen_mensual"] = (volumen_df, ["reservorio", "periodo"])
    datasets["hidro_caudal_mensual"] = (caudal_df, ["estacion", "periodo"])

    # BDREPRESAS (diario)
    represas_files = list_matching_files(DATA_LANDING, LANDING_FILES["hidrologia_represas"])
    represas_df = pd.DataFrame(columns=["fecha", "reservorio"])

    if represas_files:
        represas_df = _procesar_represas(represas_files[0])
        files_read.append(represas_files[0])

    safe_write_csv(represas_df, DATA_MART / OUTPUT_FILES["represas_diario"])
    datasets["represas_diario"] = (represas_df, ["fecha", "reservorio"])

    return files_read, datasets


__all__ = ["run_hidrologia"]