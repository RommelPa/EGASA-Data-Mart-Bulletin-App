# -*- coding: utf-8 -*-

"""Pruebas básicas del ETL."""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from etl.run_etl import main
from etl.config import ensure_directories
import pandas as pd


def test_run_etl_creates_outputs(tmp_path, monkeypatch):
    """El ETL debe ejecutarse sin errores y generar archivos esperados."""

    # Redirigir directorios a tmp para no ensuciar
    data_landing = tmp_path / "data_landing"
    data_reference = tmp_path / "data_reference"
    data_mart = tmp_path / "data_mart"
    logs_dir = tmp_path / "logs"
    # reutilizamos config de prueba general, pero ajustando rutas
    config_template = Path("tests/config_test.toml").read_text()
    config_path = tmp_path / "config.toml"
    config_path.write_text(
        config_template.replace("tests/data_landing", str(data_landing))
        .replace("tests/data_mart", str(data_mart))
        .replace("tests/data_reference", str(data_reference))
        .replace("tests/logs", str(logs_dir))
        .replace("tests/reports", str(tmp_path / "reports"))
    )

    # Crear Excels sintéticos
    prod_file = data_landing / "PRODUCCION_TEST.xlsx"
    prod_file.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(prod_file, engine="openpyxl") as writer:
        pd.DataFrame({"CENTRAL": ["CH1"], "ENERO": [1000]}).to_excel(writer, sheet_name="2010", index=False)

    fact_file = data_landing / "FACT_TEST.xlsx"
    with pd.ExcelWriter(fact_file, engine="openpyxl") as writer:
        pd.DataFrame({"CLIENTE": ["ABC"], "ENERO": [10]}).to_excel(writer, sheet_name="VENTAS (MWh)", index=False)
        pd.DataFrame({"CLIENTE": ["ABC"], "ENERO": [1000]}).to_excel(writer, sheet_name="VENTAS (S)", index=False)
        pd.DataFrame({"CONCEPTO": ["Linea"], "ENERO": [500]}).to_excel(writer, sheet_name="Ingresos", index=False)

    # Ejecutar con argv limpio
    monkeypatch.setattr(sys, "argv", ["run_etl", "--config", str(config_path), "--input", str(data_landing), "--output", str(data_mart), "--non-strict"])

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
