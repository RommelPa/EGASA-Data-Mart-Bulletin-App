import json
import pandas as pd
import sys
from pathlib import Path

from etl import run_etl


def _write_excel(path: Path, sheets: dict[str, pd.DataFrame]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        for name, df in sheets.items():
            df.to_excel(writer, sheet_name=name, index=False)


def test_etl_creates_outputs_and_metadata(tmp_path: Path, monkeypatch):
    landing = tmp_path / "landing"
    mart = tmp_path / "mart"
    logs = tmp_path / "logs"
    reports = tmp_path / "reports"

    # Config mínima para pruebas
    cfg_template = Path("tests/config_test.toml").read_text()
    config_path = tmp_path / "config.toml"
    config_path.write_text(
        cfg_template.replace("tests/data_landing", str(landing))
        .replace("tests/data_mart", str(mart))
        .replace("tests/data_reference", str(tmp_path / "ref"))
        .replace("tests/logs", str(logs))
        .replace("tests/reports", str(reports))
    )

    # Producción histórica
    prod_file = landing / "PRODUCCION_TEST.xlsx"
    df_hist = pd.DataFrame({"CENTRAL": ["CH1"], "ENERO": [1000]})
    _write_excel(prod_file, {"2010": df_hist})

    # Facturación
    fact_file = landing / "FACT_TEST.xlsx"
    ventas_df = pd.DataFrame({"CLIENTE": ["ABC"], "ENERO": [10]})
    _write_excel(
        fact_file,
        {
            "VENTAS (MWh)": ventas_df,
            "VENTAS (S)": pd.DataFrame({"CLIENTE": ["ABC"], "ENERO": [1000]}),
            "Ingresos": pd.DataFrame({"CONCEPTO": ["Linea"], "ENERO": [500]}),
        },
    )

    # Ejecutar ETL con overrides
    monkeypatch.setenv("PYTHONPATH", str(Path(__file__).resolve().parents[1]))
    argv = [
        "run_etl",
        "--config",
        str(config_path),
        "--input",
        str(landing),
        "--output",
        str(mart),
        "--non-strict",
    ]
    monkeypatch.setattr(sys, "argv", argv)
    run_etl.main()

    # Validar archivos generados
    expected_files = [
        mart / "generacion_mensual.csv",
        mart / "ventas_mensual_mwh.csv",
        mart / "ventas_mensual_soles.csv",
        mart / "metadata.json",
    ]
    for f in expected_files:
        assert f.exists(), f"{f} no fue generado"

    # Metadata contiene datasets escritos
    metadata = json.loads((mart / "metadata.json").read_text(encoding="utf-8"))
    assert "generacion_mensual" in metadata.get("datasets", {})
    assert "ventas_mensual_mwh" in metadata.get("datasets", {})
