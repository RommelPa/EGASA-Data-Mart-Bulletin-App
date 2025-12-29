# -*- coding: utf-8 -*-

"""Pipeline de producción (histórico y 15-min)."""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import pandas as pd

from ..config import DATA_LANDING, DATA_MART, DATA_REFERENCE, LANDING_FILES, OUTPUT_FILES
from ..utils_cleaning import load_centrales_reference, map_central_id
from ..utils_io import detect_header_row, list_matching_files, safe_write_csv

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


# -------------------------
# HISTÓRICO MENSUAL (2010-2025)
# -------------------------
def _process_historico(path: Path, centrales_df: pd.DataFrame) -> pd.DataFrame:
    """Procesar energía mensual desde Excel histórico."""
    try:
        xls = pd.ExcelFile(path)
    except Exception:
        logger.exception("No se pudo abrir histórico %s", path)
        raise

    frames: List[pd.DataFrame] = []
    month_map = {
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

    for sheet in xls.sheet_names:
        try:
            year = int(str(sheet).strip()[:4])
        except ValueError:
            continue
        if year < 2010 or year > 2025:
            continue

        preview = pd.read_excel(path, sheet_name=sheet, header=None, nrows=80)
        header_row = detect_header_row(preview, keywords=["central", "enero", "diciembre"])
        df_sheet = pd.read_excel(path, sheet_name=sheet, header=header_row)
        df_sheet = df_sheet.rename(columns=lambda c: str(c).strip().upper())

        if df_sheet.empty:
            logger.warning("Hoja %s sin datos luego del header detectado", sheet)
            continue

        if "CENTRAL" not in df_sheet.columns:
            logger.warning("No se detectó columna CENTRAL en hoja %s", sheet)
            continue

        df_sheet = df_sheet.rename(columns={"CENTRAL": "central"})
        month_cols = [c for c in df_sheet.columns if any(m in str(c).upper() for m in month_map)]
        if not month_cols:
            logger.warning("No se encontraron columnas de meses en hoja %s", sheet)
            continue

        df_melt = df_sheet.melt(
            id_vars=["central"],
            value_vars=month_cols,
            var_name="mes_raw",
            value_name="energia_kwh",
        )
        df_melt = df_melt.dropna(subset=["central"])

        df_melt["mes_raw"] = df_melt["mes_raw"].astype(str).str.upper().str.strip()
        df_melt["mes"] = df_melt["mes_raw"].map(lambda m: month_map.get(m, None))
        df_melt["mes"] = pd.to_numeric(df_melt["mes"], errors="coerce")
        df_melt = df_melt.dropna(subset=["mes"])
        df_melt["mes"] = df_melt["mes"].astype(int)

        df_melt["energia_mwh"] = pd.to_numeric(df_melt["energia_kwh"], errors="coerce") / 1000
        df_melt["anio"] = int(year)
        df_melt["periodo"] = df_melt.apply(lambda r: f"{int(r['anio'])}{int(r['mes']):02d}", axis=1)

        df_melt = map_central_id(df_melt, centrales_df, source_col="central")
        df_melt = df_melt.dropna(subset=["central_id"])

        frames.append(df_melt[["central_id", "central", "anio", "mes", "periodo", "energia_mwh"]])

    if frames:
        full = pd.concat(frames, ignore_index=True)
        full = full.dropna(subset=["periodo"])
        full = full.sort_values(["periodo", "central"])
        full = full.drop_duplicates(subset=["periodo", "central", "central_id"])
        return full

    return pd.DataFrame(columns=["central_id", "central", "anio", "mes", "periodo", "energia_mwh"])


# -------------------------
# 15-MIN (mensuales 2025)
# -------------------------
def _split_central_and_unidad(central_value: object, unidad_value: object) -> Tuple[str, str]:
    """Separar etiquetas de central y medidor permitiendo formato 'Central | Medidor'."""
    central_raw = str(central_value).strip() if pd.notna(central_value) else ""
    unidad_raw = str(unidad_value).strip() if pd.notna(unidad_value) else ""

    if "|" in central_raw:
        parts = [p.strip() for p in central_raw.split("|", 1)]
        central_raw = parts[0]
        if not unidad_raw and len(parts) > 1:
            unidad_raw = parts[1]
    elif "|" in unidad_raw:
        parts = [p.strip() for p in unidad_raw.split("|", 1)]
        if not central_raw and parts:
            central_raw = parts[0]
        unidad_raw = parts[1] if len(parts) > 1 else parts[0]

    return central_raw, unidad_raw


def _normalize_central_label(label: str) -> str:
    """Homologar variaciones de nombres de central."""
    if label is None or (isinstance(label, float) and pd.isna(label)):
        label = ""
    text = str(label).upper()
    text = text.replace("C.H.", "CHARCANI ").replace("C H", "CHARCANI ")
    text = text.replace("C.T.", "C.T. ").replace("CT.", "C.T. ").replace("CT ", "C.T. ")
    text = text.replace("C.T", "C.T.")
    text = re.sub(r"\s+", " ", text).strip(" .")

    roman_map = {"1": "I", "2": "II", "3": "III", "4": "IV", "5": "V", "6": "VI"}
    match_charcani = re.search(r"(CHARCANI|CH)\s*(I{1,3}|IV|V|VI|1|2|3|4|5|6)", text)
    if match_charcani:
        numeral = match_charcani.group(2)
        numeral = roman_map.get(numeral, numeral)
        return f"CHARCANI {numeral}"

    if "CHILINA" in text:
        return "C.T. CHILINA"
    if "PISCO" in text:
        return "C.T. PISCO"
    if "MOLLENDO" in text:
        return "C.T. MOLLENDO"

    return text or "CENTRAL"


def _clean_unidad_label(label: str) -> str:
    """Limpiar etiqueta de unidad/medidor."""
    if label is None or (isinstance(label, float) and pd.isna(label)):
        label = ""
    text = str(label).replace("-kWh", "").replace("kWh", "")
    text = text.replace("-", " ").replace("_", " ").strip()
    text = text.split()[0] if text else ""
    return text or "U1"


def _is_empty_header(x: object) -> bool:
    """True si el header está vacío/NaN/Unnamed/etc."""
    if x is None:
        return True
    if isinstance(x, float) and pd.isna(x):
        return True
    s = str(x).strip()
    if not s:
        return True
    sl = s.lower()
    return sl in {"nan", "none"} or sl.startswith("unnamed")


def _clean_header_str(x: object) -> str:
    """Normaliza texto de header y retorna '' si es vacío."""
    if _is_empty_header(x):
        return ""
    return re.sub(r"\s+", " ", str(x).strip())


def _process_15min(path: Path, centrales_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """Procesar archivos 15-min y retornarlos particionados por periodo real (YYYYMM)."""
    df_raw = pd.read_excel(path, header=None)
    if df_raw.empty or df_raw.shape[0] < 3:
        logger.warning("Archivo 15min %s sin filas útiles", path)
        return {}

    # Intento de detectar si el archivo no empieza exactamente en fila 0
    header_row = 0
    a1 = df_raw.iloc[0, 0]
    if isinstance(a1, str) and "FECHA" not in a1.upper():
        header_row = detect_header_row(df_raw.head(12), keywords=["fecha", "hora"])
    if header_row and header_row > 0:
        df_raw = df_raw.iloc[header_row:].reset_index(drop=True)

    if df_raw.shape[0] < 3:
        logger.warning("Archivo 15min %s no tiene datos tras header", path)
        return {}

    # Fila 0 = grupos (centrales), fila 1 = medidores
    raw_central_row = df_raw.iloc[0].tolist()
    unidad_row = df_raw.iloc[1].tolist() if df_raw.shape[0] > 1 else []

    # Forward-fill horizontal robusto (para evitar central_raw nan)
    cleaned_row: List[str] = []
    last_value: str = ""
    for idx, val in enumerate(raw_central_row):
        if idx == 0:
            cleaned_row.append("")  # columna de tiempo
            continue
        g = _clean_header_str(val)
        if g:
            last_value = g
        cleaned_row.append(last_value)

    central_row = pd.Series(cleaned_row)

    col_info: List[Dict[str, object]] = []
    for idx in range(len(central_row)):
        if idx == 0:
            col_info.append({"col": idx, "tipo": "fecha_hora"})
            continue

        central_label = _clean_header_str(central_row.iloc[idx])
        unidad_value = unidad_row[idx] if idx < len(unidad_row) else None
        unidad_label = _clean_header_str(unidad_value)

        # Si no hay central (ni siquiera tras ffill), ignorar columna.
        if not central_label:
            col_info.append(
                {
                    "col": idx,
                    "tipo": "ignorar",
                    "motivo": "sin_central",
                    "unidad_header": unidad_label or f"col_{idx}",
                }
            )
            continue

        # unidad fallback
        if not unidad_label:
            unidad_label = f"col_{idx}"

        # Normalización final
        central_clean = _normalize_central_label(central_label)
        unidad_clean = _clean_unidad_label(unidad_label)

        col_info.append(
            {
                "col": idx,
                "tipo": "dato",
                "central_raw": f"{central_label} | {unidad_label}",
                "central": central_clean,
                "unidad": unidad_clean,
            }
        )

    ignored = [x for x in col_info if x.get("tipo") == "ignorar"]
    if ignored:
        logger.warning(
            "Columnas 15min ignoradas por no tener central (ejemplos): %s",
            ignored[:10],
        )

    # Datos desde fila 2
    data = df_raw.iloc[2:].reset_index(drop=True)
    data = data.rename(columns={0: "FECHA_HORA"})
    data["FECHA_HORA"] = pd.to_datetime(data["FECHA_HORA"], errors="coerce")
    if data["FECHA_HORA"].isna().all():
        logger.warning("No se pudieron parsear timestamps en %s", path)
        return {}

    records: List[pd.DataFrame] = []
    for info in col_info:
        if info.get("tipo") != "dato":
            continue

        col_idx = int(info["col"])
        series = pd.to_numeric(data.get(col_idx), errors="coerce")

        # Primero filtrar valores útiles
        mask = series.notna() & (series >= 0) & data["FECHA_HORA"].notna()
        if not mask.any():
            continue

        tmp = pd.DataFrame(
            {
                "fecha_hora": data.loc[mask, "FECHA_HORA"],
                "central": info["central"],
                "central_raw": info["central_raw"],
                "unidad": info["unidad"],
                "energia_mwh": series.loc[mask] / 1000,
            }
        )

        # Mapear central_id (ya no debería haber central vacía aquí)
        mapped = map_central_id(tmp.copy(), centrales_df, source_col="central")
        records.append(mapped)

    records = [df for df in records if df is not None and not df.empty]
    if not records:
        return {}

    df_all = pd.concat(records, ignore_index=True)
    df_all = df_all.dropna(subset=["fecha_hora"])
    df_all["periodo"] = df_all["fecha_hora"].dt.strftime("%Y%m")
    df_all = df_all.dropna(subset=["periodo"])
    df_all = df_all.sort_values(["fecha_hora", "central", "unidad"])

    particiones: Dict[str, pd.DataFrame] = {}
    for periodo, df_part in df_all.groupby("periodo"):
        df_part = df_part.drop_duplicates(subset=["fecha_hora", "central_id", "unidad"])
        particiones[str(periodo)] = df_part

    return particiones


# -------------------------
# ORQUESTACIÓN
# -------------------------
def run_produccion() -> Tuple[pd.DataFrame, List[Path], Dict[str, Tuple[pd.DataFrame, Iterable[str]]]]:
    """Ejecutar pipelines de producción."""
    files_read: List[Path] = []
    datasets: Dict[str, Tuple[pd.DataFrame, Iterable[str]]] = {}

    ref_path = ensure_centrales_reference()
    centrales_df = load_centrales_reference(ref_path)
    files_read.append(ref_path)

    # Producción histórica
    historicos = list_matching_files(DATA_LANDING, LANDING_FILES["produccion_historica"])
    historico_df = pd.DataFrame(columns=["central_id", "central", "anio", "mes", "periodo", "energia_mwh"])
    if historicos:
        historico_df = _process_historico(historicos[0], centrales_df)
        files_read.append(historicos[0])

    safe_write_csv(historico_df, DATA_MART / OUTPUT_FILES["generacion_mensual"])
    datasets["generacion_mensual"] = (historico_df, ["central_id", "anio", "mes", "periodo"])

    # Producción 15min
    archivos_15 = list_matching_files(DATA_LANDING, LANDING_FILES["produccion_15min"])
    particiones: Dict[str, pd.DataFrame] = {}

    for archivo in archivos_15:
        files_read.append(archivo)
        particiones_archivo = _process_15min(archivo, centrales_df)

        for periodo, df_part in particiones_archivo.items():
            existing_path = DATA_MART / OUTPUT_FILES["generacion_15min_template"].format(yyyymm=periodo)

            if existing_path.exists():
                prev = pd.read_csv(existing_path, parse_dates=["fecha_hora"], low_memory=False)
            else:
                prev = pd.DataFrame(columns=df_part.columns)

            # Asegurar mismas columnas
            for col in df_part.columns:
                if col not in prev.columns:
                    prev[col] = None

            frames_to_concat = [df for df in (prev[df_part.columns], df_part) if df is not None and not df.empty]
            if frames_to_concat:
                merged = (
                    pd.concat(frames_to_concat, ignore_index=True)
                    .drop_duplicates(subset=["fecha_hora", "central_id", "unidad"])
                    .sort_values(["fecha_hora", "central_id", "unidad"])
                )
            else:
                merged = pd.DataFrame(columns=df_part.columns)

            safe_write_csv(merged, existing_path)
            particiones[periodo] = merged

    for periodo, df_part in particiones.items():
        datasets[f"generacion_15min_{periodo}"] = (df_part, ["fecha_hora", "central_id", "unidad"])

    return historico_df, files_read, datasets


__all__ = ["run_produccion"]