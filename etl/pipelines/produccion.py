# -*- coding: utf-8 -*-

"""Pipeline de producción (histórico y 15-min)."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import pandas as pd

from ..config import DATA_LANDING, DATA_MART, DATA_REFERENCE, LANDING_FILES, OUTPUT_FILES
from ..utils_cleaning import load_centrales_reference, map_central_id
from ..utils_io import detect_header_row, list_matching_files, read_excel_safe, safe_write_csv

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

        preview = pd.read_excel(path, sheet_name=sheet, header=None, nrows=60)
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
        df_melt = df_melt.dropna(subset=["mes"])
        df_melt["energia_mwh"] = pd.to_numeric(df_melt["energia_kwh"], errors="coerce") / 1000
        df_melt["anio"] = year
        df_melt["periodo"] = df_melt["anio"].astype(int).astype(str) + df_melt["mes"]
        df_melt = map_central_id(df_melt, centrales_df, source_col="central")
        frames.append(df_melt[["central_id", "central", "periodo", "energia_mwh"]])

    if frames:
        full = pd.concat(frames, ignore_index=True)
        full = full.dropna(subset=["periodo"])
        full = full.sort_values(["periodo", "central"])
        full = full.drop_duplicates(subset=["periodo", "central", "central_id"])
        return full

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
    """Procesar archivos 15-min y retornarlos particionados por periodo real (YYYYMM)."""

    df_raw = pd.read_excel(path, header=None)
    if df_raw.empty or df_raw.shape[0] < 3:
        logger.warning("Archivo 15min %s sin filas útiles", path)
        return {}

    header_row = 0
    fecha_hora_col = df_raw.iloc[header_row, 0]
    if isinstance(fecha_hora_col, str) and "FECHA" not in fecha_hora_col.upper():
        header_row = detect_header_row(df_raw.head(10), keywords=["fecha", "hora"])
    if header_row != 0:
        df_raw = df_raw.iloc[header_row:].reset_index(drop=True)

    if df_raw.shape[0] < 3:
        logger.warning("Archivo 15min %s no tiene datos tras header", path)
        return {}

    # Construir metadatos de columnas: fila 0 centrales, fila 1 unidades/medidores
    col_info = []
    for idx, col_name in enumerate(df_raw.iloc[0]):
        if idx == 0:
            col_info.append({"col": idx, "tipo": "fecha_hora"})
            continue
        central_label = str(col_name) if pd.notna(col_name) else f"CENTRAL_{idx}"
        unidad_label = str(df_raw.iloc[1, idx]) if idx < len(df_raw.columns) else "U1"
        unidad_clean = unidad_label.split()[0].replace("-", "").replace("_", "") or "U1"
        central_clean = central_label.replace("C.H.", "").replace("C.T.", "").replace("CH", "CHARCANI").strip()
        col_info.append(
            {
                "col": idx,
                "central": central_clean,
                "unidad": unidad_clean,
            }
        )

    data = df_raw.iloc[2:].reset_index(drop=True)
    data = data.rename(columns={0: "FECHA_HORA"})
    data["FECHA_HORA"] = pd.to_datetime(data["FECHA_HORA"], errors="coerce")

    records: List[pd.DataFrame] = []
    for info in col_info:
        if info.get("tipo") == "fecha_hora":
            continue
        col_idx = info["col"]
        series = pd.to_numeric(data.get(col_idx), errors="coerce")
        tmp = pd.DataFrame(
            {
                "fecha_hora": data["FECHA_HORA"],
                "central": info["central"],
                "unidad": info["unidad"],
                "energia_mwh": series / 1000,
            }
        ).dropna(subset=["fecha_hora"])
        tmp = map_central_id(tmp, centrales_df, source_col="central")
        records.append(tmp)

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
        particiones[periodo] = df_part

    return particiones


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
    particiones: Dict[str, pd.DataFrame] = {}
    for archivo in archivos_15:
        files_read.append(archivo)
        try:
            particiones_archivo = _process_15min(archivo, centrales_df)
        except Exception:
            logger.exception("Error procesando archivo 15min %s", archivo)
            raise
        for periodo, df_part in particiones_archivo.items():
            existing_path = DATA_MART / OUTPUT_FILES["generacion_15min_template"].format(yyyymm=periodo)
            if existing_path.exists():
                prev = pd.read_csv(existing_path, parse_dates=["fecha_hora"])
            else:
                prev = pd.DataFrame(columns=df_part.columns)
            merged = (
                pd.concat([prev, df_part], ignore_index=True)
                .drop_duplicates(subset=["fecha_hora", "central_id", "unidad"])
                .sort_values(["fecha_hora", "central_id", "unidad"])
            )
            safe_write_csv(merged, existing_path)
            particiones[periodo] = merged

    for periodo, df_part in particiones.items():
        datasets[f"generacion_15min_{periodo}"] = (df_part, ["fecha_hora", "central_id", "unidad"])

    return historico_df, files_read, datasets


__all__ = ["run_produccion"]
