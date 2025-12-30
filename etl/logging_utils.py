# -*- coding: utf-8 -*-

"""Utilidades de logging compartidas para el ETL."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Iterable


class ContextFilter(logging.Filter):
    """Agrega campos de contexto esperados por los formatters.

    Se asegura de que *todas* las entradas de log tengan los atributos usados
    por nuestro formato estándar, evitando ``KeyError`` al formatear.
    """

    __slots__ = ("_defaults",)

    def __init__(self, run_id: str | None = None) -> None:
        super().__init__()
        self._defaults = {
            "stage": "-",
            "file": "-",
            "rows_in": "-",
            "rows_out": "-",
            "duration_ms": "-",
            "run_id": run_id or "-",
        }

    def _fields(self) -> Iterable[str]:
        return self._defaults.keys()

    def filter(self, record: logging.LogRecord) -> bool:  # type: ignore[override]
        for field, value in self._defaults.items():
            current = getattr(record, field, None)
            if current in (None, ""):
                setattr(record, field, value)
        return True

    def update_run_id(self, run_id: str | None) -> None:
        if run_id:
            self._defaults["run_id"] = run_id


def _ensure_filter(run_id: str | None) -> ContextFilter:
    root = logging.getLogger()
    existing = next((f for f in root.filters if isinstance(f, ContextFilter)), None)
    flt = existing or ContextFilter(run_id=run_id)
    if not existing:
        root.addFilter(flt)
    flt.update_run_id(run_id)

    for handler in root.handlers:
        if flt not in getattr(handler, "filters", []):
            handler.addFilter(flt)
    return flt


def setup_logging(log_file: Path, run_id: str | None = None) -> None:
    """Configura logging global con formato consistente.

    Debe llamarse una sola vez al inicio de la ejecución.
    """

    log_file.parent.mkdir(parents=True, exist_ok=True)
    _ensure_filter(run_id)

    logging.basicConfig(
        level=logging.INFO,
        format=(
            "%(asctime)s [%(levelname)s] stage=%(stage)s file=%(file)s "
            "rows_in=%(rows_in)s rows_out=%(rows_out)s duration_ms=%(duration_ms)s "
            "run_id=%(run_id)s %(name)s - %(message)s"
        ),
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )

    # Asegurar que los handlers recién creados tengan el filtro también.
    _ensure_filter(run_id)


__all__ = ["ContextFilter", "setup_logging"]
