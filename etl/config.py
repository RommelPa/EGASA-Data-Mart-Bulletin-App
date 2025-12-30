# -*- coding: utf-8 -*-

"""Configuración y rutas base del ETL.

La configuración puede ser declarada en ``config.yml`` (preferido) o
``config.toml``. Si no existe, se usan los valores por defecto definidos
en ``DEFAULT_CONFIG``.
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict, Any

import logging

try:  # Python 3.11+ trae tomllib en stdlib
    import tomllib  # type: ignore
except Exception:  # pragma: no cover - fallback si no existe
    tomllib = None

try:
    import yaml
except ImportError:  # pragma: no cover - PyYAML se declara en requirements
    yaml = None

logger = logging.getLogger(__name__)

# Directorio base del proyecto
BASE_DIR: Path = Path(__file__).resolve().parent.parent


# --- Configuración declarativa (con defaults) ---
DEFAULT_CONFIG: Dict[str, Any] = {
    "paths": {
        "input": "data_landing",
        "output": "data_mart",
        "reference": "data_reference",
        "logs": "logs",
        "reports": "reports",
    },
    "sources": {
        "produccion_historica": {"pattern": "PRODUCCION EGASA DESDE 2010"},
        "produccion_15min": {"pattern": "PRODUCCIÓN DE ENERGÍA"},
        "hidrologia_control": {
            "pattern": "Control Hidrológico.xlsx",
            "sheets": {
                "volumen": ["AB", "EF", "EP", "PI", "CH", "BA", "TOTAL"],
                "caudal": ["CAUDAL"],
            },
            "required": True,
        },
        "hidrologia_represas": {
            "pattern": "BDREPRESAS.xlsx",
            "sheet": "INFORMEDIARIO",
            "required": True,
        },
        "facturacion": {
            "pattern": "Facturacion",
            "sheets": {
                "ventas_mwh": "VENTAS (MWh)",
                "ventas_soles": "VENTAS (S)",
                "ingresos": "Ingresos",
            },
            "required": True,
        },
        "contratos": {
            "pattern": "Revision de Volumen Optimo",
            "sheets": {
                "base": "CONTRATOS BASE DATOS",
                "riesgo": "RIESGO",
            },
            "required": False,
        },
        "balance_energia": {
            "pattern": "balance 2025",
            "sheets": {"perfil": "Perfil", "r": "R"},
            "required": False,
        },
    },
    "tables": {
        "ventas_mensual_mwh": {
            "required_columns": ["cliente"],
            "rename": {},
        },
        "ventas_mensual_soles": {
            "required_columns": ["cliente"],
            "rename": {},
        },
        "ingresos_mensual": {
            "required_columns": ["anio", "mes", "cliente_o_concepto", "soles"],
            "rename": {},
        },
        "represas_diario": {
            "required_columns": ["reservorio"],
            "rename": {},
        },
        "contratos_base": {
            "required_columns": ["cliente", "fecha_inicio", "fecha_fin"],
            "rename": {},
        },
        "contratos_riesgo": {
            "required_columns": ["cliente", "fecha_inicio", "fecha_fin"],
            "rename": {},
        },
    },
}


_CONFIG_CACHE: Dict[str, Any] | None = None


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    """Merge recursivo simple para defaults + overrides de usuario."""

    for key, value in override.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            base[key] = _deep_merge(dict(base[key]), value)
        else:
            base[key] = value
    return base


def _load_from_yaml(path: Path) -> Dict[str, Any]:
    if yaml is None:
        raise ImportError("PyYAML no está instalado, pero se solicitó leer config.yml")
    with path.open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}


def _load_from_toml(path: Path) -> Dict[str, Any]:
    if tomllib is None:
        raise ImportError("tomllib no está disponible para leer config.toml")
    with path.open("rb") as fh:
        return tomllib.load(fh) or {}


def load_config(config_path: Path | None = None) -> Dict[str, Any]:
    """Cargar configuración declarativa.

    Orden de prioridad:
    - ``config_path`` si se pasa explícitamente.
    - ``config.yml`` (YAML)
    - ``config.toml`` (TOML)
    - ``DEFAULT_CONFIG``
    """

    global _CONFIG_CACHE
    if _CONFIG_CACHE is not None:
        return _CONFIG_CACHE

    base_cfg = dict(DEFAULT_CONFIG)
    cfg_path = config_path or BASE_DIR / "config.yml"

    try:
        if cfg_path.exists():
            if cfg_path.suffix.lower() in {".toml", ".tml"}:
                user_cfg = _load_from_toml(cfg_path)
            else:
                try:
                    user_cfg = _load_from_yaml(cfg_path)
                except ImportError:
                    logger.warning("PyYAML no disponible, omitiendo %s", cfg_path.name)
                    user_cfg = {}
            _CONFIG_CACHE = _deep_merge(base_cfg, user_cfg)
            logger.info("Configuración cargada desde %s", cfg_path.name)
            return _CONFIG_CACHE
        alt_toml = BASE_DIR / "config.toml"
        if alt_toml.exists():
            user_cfg = _load_from_toml(alt_toml)
            _CONFIG_CACHE = _deep_merge(base_cfg, user_cfg)
            logger.info("Configuración cargada desde %s", alt_toml.name)
            return _CONFIG_CACHE
    except Exception:
        logger.exception("Error leyendo archivo de configuración; usando defaults")

    _CONFIG_CACHE = base_cfg
    return _CONFIG_CACHE


def apply_runtime_overrides(
    config_path: Path | None = None,
    paths_override: Dict[str, str] | None = None,
) -> None:
    """Permite recargar configuración en tiempo de ejecución (CLI).

    - `config_path`: ruta alternativa a config.yml|toml.
    - `paths_override`: dict opcional con keys input/output/reference/logs/reports.
    """

    global CONFIG, PATHS, DATA_LANDING, DATA_REFERENCE, DATA_MART, LOGS_DIR, REPORTS_DIR, LANDING_FILES

    # reset cache y recargar
    global _CONFIG_CACHE
    _CONFIG_CACHE = None
    CONFIG = load_config(config_path)

    if paths_override:
        CONFIG.setdefault("paths", {}).update({k: v for k, v in paths_override.items() if v})

    PATHS = {
        "input": BASE_DIR / CONFIG["paths"].get("input", "data_landing"),
        "output": BASE_DIR / CONFIG["paths"].get("output", "data_mart"),
        "reference": BASE_DIR / CONFIG["paths"].get("reference", "data_reference"),
        "logs": BASE_DIR / CONFIG["paths"].get("logs", "logs"),
        "reports": BASE_DIR / CONFIG["paths"].get("reports", "reports"),
    }

    DATA_LANDING = PATHS["input"]
    DATA_REFERENCE = PATHS["reference"]
    DATA_MART = PATHS["output"]
    LOGS_DIR = PATHS["logs"]
    REPORTS_DIR = PATHS["reports"]

    LANDING_FILES = _landing_files_from_config(CONFIG)

    # Propagar overrides a módulos ya importados
    try:
        from etl.pipelines import produccion, hidrologia, facturacion, contratos, balance_energia

        produccion.DATA_LANDING = DATA_LANDING
        produccion.DATA_REFERENCE = DATA_REFERENCE
        produccion.DATA_MART = DATA_MART
        produccion.LANDING_FILES = LANDING_FILES

        hidrologia.DATA_LANDING = DATA_LANDING
        hidrologia.DATA_MART = DATA_MART
        hidrologia.LANDING_FILES = LANDING_FILES

        facturacion.DATA_LANDING = DATA_LANDING
        facturacion.DATA_MART = DATA_MART
        facturacion.LANDING_FILES = LANDING_FILES

        contratos.DATA_LANDING = DATA_LANDING
        contratos.DATA_MART = DATA_MART
        contratos.LANDING_FILES = LANDING_FILES

        balance_energia.DATA_LANDING = DATA_LANDING
        balance_energia.DATA_MART = DATA_MART
        balance_energia.LANDING_FILES = LANDING_FILES
    except Exception:
        pass


# Config cargada (paths y tablas se exponen como constantes legadas)
CONFIG: Dict[str, Any] = load_config()

PATHS = {
    "input": BASE_DIR / CONFIG["paths"].get("input", "data_landing"),
    "output": BASE_DIR / CONFIG["paths"].get("output", "data_mart"),
    "reference": BASE_DIR / CONFIG["paths"].get("reference", "data_reference"),
    "logs": BASE_DIR / CONFIG["paths"].get("logs", "logs"),
    "reports": BASE_DIR / CONFIG["paths"].get("reports", "reports"),
}

DATA_LANDING: Path = PATHS["input"]
DATA_REFERENCE: Path = PATHS["reference"]
DATA_MART: Path = PATHS["output"]
LOGS_DIR: Path = PATHS["logs"]
REPORTS_DIR: Path = PATHS["reports"]


def _landing_files_from_config(cfg: Dict[str, Any]) -> Dict[str, str]:
    sources = cfg.get("sources", {})
    return {name: info.get("pattern", "") for name, info in sources.items()}


LANDING_FILES: Dict[str, str] = _landing_files_from_config(CONFIG)

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
    "balance_perfil_mensual": "balance_perfil_mensual.csv",
    "balance_r_mensual": "balance_r_mensual.csv",
}

# Logging
LOG_FILE: Path = LOGS_DIR / "etl.log"


def ensure_directories() -> None:
    """Crear directorios necesarios si no existen."""

    for path in PATHS.values():
        path.mkdir(parents=True, exist_ok=True)


def get_source(name: str) -> Dict[str, Any]:
    return CONFIG.get("sources", {}).get(name, {})


def table_rules(name: str) -> Dict[str, Any]:
    return CONFIG.get("tables", {}).get(name, {})


__all__ = [
    "BASE_DIR",
    "CONFIG",
    "PATHS",
    "DATA_LANDING",
    "DATA_REFERENCE",
    "DATA_MART",
    "LOGS_DIR",
    "REPORTS_DIR",
    "LANDING_FILES",
    "OUTPUT_FILES",
    "LOG_FILE",
    "ensure_directories",
    "get_source",
    "table_rules",
    "load_config",
    "apply_runtime_overrides",
]
