# -*- coding: utf-8 -*-

"""Pruebas básicas del ETL."""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from etl.run_etl import main
from etl.config import ensure_directories


def test_run_etl_creates_outputs(tmp_path, monkeypatch):
    """El ETL debe ejecutarse sin errores y generar archivos esperados."""

    # Redirigir directorios a tmp para no ensuciar
    data_landing = tmp_path / "data_landing"
    data_reference = tmp_path / "data_reference"
    data_mart = tmp_path / "data_mart"
    logs_dir = tmp_path / "logs"
    monkeypatch.setattr("etl.config.DATA_LANDING", data_landing)
    monkeypatch.setattr("etl.config.DATA_REFERENCE", data_reference)
    monkeypatch.setattr("etl.config.DATA_MART", data_mart)
    monkeypatch.setattr("etl.config.LOGS_DIR", logs_dir)
    monkeypatch.setattr("etl.config.LOG_FILE", logs_dir / "etl.log")
    monkeypatch.setattr("etl.pipelines.produccion.DATA_LANDING", data_landing)
    monkeypatch.setattr("etl.pipelines.produccion.DATA_REFERENCE", data_reference)
    monkeypatch.setattr("etl.pipelines.produccion.DATA_MART", data_mart)
    monkeypatch.setattr("etl.pipelines.hidrologia.DATA_LANDING", data_landing)
    monkeypatch.setattr("etl.pipelines.hidrologia.DATA_MART", data_mart)
    monkeypatch.setattr("etl.pipelines.facturacion.DATA_LANDING", data_landing)
    monkeypatch.setattr("etl.pipelines.facturacion.DATA_MART", data_mart)
    monkeypatch.setattr("etl.pipelines.contratos.DATA_LANDING", data_landing)
    monkeypatch.setattr("etl.pipelines.contratos.DATA_MART", data_mart)
    ensure_directories()

    main()

    expected = [
        "generacion_mensual.csv",
        "hidro_volumen_mensual.csv",
        "hidro_caudal_mensual.csv",
        "represas_diario.csv",
        "ventas_mensual_mwh.csv",
        "ventas_mensual_soles.csv",
        "ingresos_mensual.csv",
        "precio_medio_mensual.csv",
        "contratos_base.csv",
        "contratos_riesgo.csv",
    ]
    for fname in expected:
        path = data_mart / fname
        assert path.exists(), f"{fname} no fue generado"
