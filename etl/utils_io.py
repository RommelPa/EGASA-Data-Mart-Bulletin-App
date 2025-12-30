# -*- coding: utf-8 -*-

"""Utilidades de entrada/salida para el ETL."""

from __future__ import annotations

import logging
import os
import re
import tempfile
from pathlib import Path
from typing import Iterable, List, Optional, Tuple

import pandas as pd

from .config import table_rules, REPORTS_DIR, LOGS_DIR
from etl.schemas import get_schema
import json
from datetime import datetime
logger = logging.getLogger(__name__)

_RUN_CONTEXT: dict = {
    "run_id": None,
    "strict": True,
    "run_log_path": None,
}


def detect_header_row(
    df_preview: pd.DataFrame,
    expected_columns: Optional[Iterable[str]] = None,
    keywords: Optional[Iterable[str]] = None,
) -> int:
    """Detectar la fila que contiene los nombres de columnas.

    Prioriza coincidencias exactas de *keywords* (case-insensitive) y, si no
    existen, usa expected_columns para buscar una coincidencia parcial.
    Devuelve el índice (0-based) que debe usarse como header en pandas.read_excel.
    """

    keyword_set = {kw.strip().lower() for kw in keywords or [] if kw}
    expected_set = {col.strip().lower() for col in expected_columns or []}

    for idx, row in df_preview.iterrows():
        row_values = [str(v).strip().lower() for v in row if pd.notna(v) and str(v).strip()]
        row_text = " ".join(row_values)

        if keyword_set and all(any(kw in value for value in row_values) or kw in row_text for kw in keyword_set):
            return idx

        if expected_set:
            match_ratio = len(expected_set & set(row_values)) / max(len(expected_set), 1)
            if match_ratio >= 0.6:
                return idx

    return 0


def read_excel_safe(
    path: Path,
    sheet_name: str | int | None = 0,
    expected_columns: Optional[Iterable[str]] = None,
    header_keywords: Optional[Iterable[str]] = None,
    **kwargs,
) -> pd.DataFrame:
    """Leer Excel robustamente detectando header.

    Si expected_columns se proporciona, inspecciona las primeras filas para ubicar el header.
    """

    if not path.exists():
        logger.warning("Archivo no encontrado: %s", path)
        return pd.DataFrame()

    try:
        preview = pd.read_excel(path, sheet_name=sheet_name, nrows=30, header=None)
        header_row = 0
        if expected_columns or header_keywords:
            header_row = detect_header_row(preview, expected_columns, header_keywords)
        df = pd.read_excel(path, sheet_name=sheet_name, header=header_row, **kwargs)
        return df
    except Exception:
        logger.exception("Error leyendo Excel %s sheet=%s", path, sheet_name)
        raise


def list_matching_files(base_dir: Path, pattern: str) -> List[Path]:
    """Listar archivos que cumplan el patrón (substring o regex)."""

    if not base_dir.exists():
        return []

    compiled: re.Pattern[str] | None = None
    try:
        compiled = re.compile(pattern, flags=re.IGNORECASE)
    except re.error:
        compiled = None

    def _match(path: Path) -> bool:
        if compiled:
            return bool(compiled.search(path.name))
        return pattern.lower() in path.name.lower()

    return sorted([p for p in base_dir.iterdir() if _match(p)])


WINDOWS_INVALID_CHARS = re.compile(r'[<>:"/\\|?*]')


def sanitize_filename(name: str) -> str:
    """Quita caracteres inválidos y recorta espacios/puntos finales.

    Pensado para compatibilidad con Windows y Linux.
    """

    cleaned = WINDOWS_INVALID_CHARS.sub("_", name)
    cleaned = cleaned.rstrip(" .")
    return cleaned or "_"


def _atomic_write_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w",
        delete=False,
        dir=str(path.parent),
        prefix=f"{path.stem}_",
        suffix=".tmp",
        encoding="utf-8",
    ) as tmp:
        df.to_csv(tmp.name, index=False, encoding="utf-8")
        temp_path = Path(tmp.name)

    os.replace(temp_path, path)


def safe_write_csv(df: pd.DataFrame, path: Path) -> int:
    """Escribir DataFrame a CSV de forma segura.

    Usa un archivo temporal en el mismo directorio para evitar problemas de
    rename/reemplazo en Windows.
    """

    sanitized = path.with_name(sanitize_filename(path.name))
    _atomic_write_csv(df, sanitized)
    return len(df)


def record_file_info(files: Iterable[Path]) -> List[Tuple[str, float, int]]:
    """Registrar metadata básica de archivos leídos."""

    info: List[Tuple[str, float, int]] = []
    for f in files:
        if f.exists():
            stat = f.stat()
            info.append((f.name, stat.st_mtime, stat.st_size))
    return info


def apply_table_rules(dataset: str, df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica reglas declaradas en config.yml/config.toml:
    - renombrado de columnas
    - validación de columnas obligatorias
    """

    rules = table_rules(dataset)
    if not rules:
        return df

    rename_map = rules.get("rename") or {}
    required = rules.get("required_columns") or []

    out = df.rename(columns=rename_map) if rename_map else df

    missing = [col for col in required if col not in out.columns]
    if missing:
        raise ValueError(f"Tabla '{dataset}': faltan columnas requeridas {missing}")

    return out


def set_run_context(run_id: str, strict: bool) -> None:
    """Registrar contexto global para reportes de validación."""

    _RUN_CONTEXT["run_id"] = run_id
    _RUN_CONTEXT["strict"] = strict


def _write_validation_report(dataset: str, error: Exception) -> Path:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    run_id = _RUN_CONTEXT.get("run_id") or datetime.utcnow().strftime("%Y%m%d%H%M%S")
    safe_dataset = sanitize_filename(dataset)
    report_path = REPORTS_DIR / f"validation_{run_id}_{safe_dataset}.json"
    payload = {"dataset": dataset, "run_id": run_id, "error": str(error)}
    try:
        import pandera.errors as pe

        if isinstance(error, pe.SchemaErrors):
            payload["failure_cases"] = error.failure_cases.to_dict(orient="records")
    except Exception:
        pass

    with report_path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)
    return report_path


def validate_and_write(dataset: str, df: pd.DataFrame, path: Path) -> int:
    """Valida (pandera) y escribe CSV. En modo estricto, falla si hay errores."""

    schema = get_schema(dataset)
    strict = bool(_RUN_CONTEXT.get("strict", True))

    if schema is None:
        return safe_write_csv(df, path)

    if df.empty:
        # No validamos datasets vacíos; solo escribimos para mantener contratos de salida.
        return safe_write_csv(df, path)

    try:
        validated = schema.validate(df, lazy=True)
    except Exception as exc:  # pandera SchemaErrors o similares
        report = _write_validation_report(dataset, exc)
        msg = f"Validación falló para {dataset}. Reporte: {report}"
        if strict:
            logger.error(msg)
            raise
        logger.warning(msg)
        # modo non-strict: escribir de todos modos
        return safe_write_csv(df, path)

    return safe_write_csv(validated, path)


def default_log_extra(**kwargs) -> dict:
    """Construye extras de logging con run_id y placeholders."""

    extra_defaults = {
        "run_id": _RUN_CONTEXT.get("run_id"),
        "stage": "-",
        "file": "-",
        "rows_in": "-",
        "rows_out": "-",
        "duration_ms": "-",
    }
    extra_defaults.update(kwargs)
    return extra_defaults


def ensure_runs_log() -> Path:
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    path = LOGS_DIR / "etl_runs.jsonl"
    _RUN_CONTEXT["run_log_path"] = path
    return path


def record_etl_run(run_id: str, started_at: str, finished_at: str, status: str, tables: dict, warnings: list[str] | None = None, error: str | None = None) -> Path:
    """Append run metadata to etl_runs.jsonl."""

    path = ensure_runs_log()
    payload = {
        "run_id": run_id,
        "started_at": started_at,
        "finished_at": finished_at,
        "status": status,
        "tables": tables,
        "warnings": warnings or [],
    }
    if error:
        payload["error"] = error
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(payload, ensure_ascii=False) + "\n")
    return path


__all__ = [
    "detect_header_row",
    "read_excel_safe",
    "list_matching_files",
    "safe_write_csv",
    "record_file_info",
    "apply_table_rules",
    "validate_and_write",
    "set_run_context",
]
