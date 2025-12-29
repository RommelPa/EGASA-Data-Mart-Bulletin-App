# -*- coding: utf-8 -*-

"""Configuración y rutas base del ETL."""

from __future__ import annotations

from pathlib import Path
from typing import Dict

# Directorio base del proyecto
BASE_DIR: Path = Path(__file__).resolve().parent.parent

# Carpetas principales
DATA_LANDING: Path = BASE_DIR / "data_landing"
DATA_REFERENCE: Path = BASE_DIR / "data_reference"
DATA_MART: Path = BASE_DIR / "data_mart"
LOGS_DIR: Path = BASE_DIR / "logs"

# Archivos de entrada esperados (patrones)
LANDING_FILES: Dict[str, str] = {
    "produccion_historica": "PRODUCCION EGASA DESDE 2010",
    "produccion_15min": "PRODUCCIÓN DE ENERGÍA",
    "hidrologia_control": "Control Hidrológico.xlsx",
    "hidrologia_represas": "BDREPRESAS.xlsx",
    "facturacion": "Facturacion",
    "contratos": "CONTRATOS BASE DATOS (egasa)",
}

# Archivos de salida
OUTPUT_FILES: Dict[str, str] = {
    "generacion_mensual": "generacion_mensual.csv",
    "generacion_15min_template": "generacion_15min_{yyyymm}.csv",
    "hidro_volumen_mensual": "hidro_volumen_mensual.csv",
    "hidro_caudal_mensual": "hidro_caudal_mensual.csv",
    "represas_diario": "represas_diario.csv",
    "ventas_mensual_mwh": "ventas_mensual_mwh.csv",
    "ventas_mensual_soles": "ventas_mensual_soles.csv",
    "ingresos_mensual": "ingresos_mensual.csv",
    "precio_medio_mensual": "precio_medio_mensual.csv",
    "contratos_base": "contratos_base.csv",
    "contratos_riesgo": "contratos_riesgo.csv",
}

# Logging
LOG_FILE: Path = LOGS_DIR / "etl.log"


def ensure_directories() -> None:
    """Crear directorios necesarios si no existen."""

    for path in (DATA_LANDING, DATA_REFERENCE, DATA_MART, LOGS_DIR):
        path.mkdir(parents=True, exist_ok=True)


__all__ = [
    "BASE_DIR",
    "DATA_LANDING",
    "DATA_REFERENCE",
    "DATA_MART",
    "LOGS_DIR",
    "LANDING_FILES",
    "OUTPUT_FILES",
    "LOG_FILE",
    "ensure_directories",
]
