import pandas as pd
import pytest
from pathlib import Path

from etl.utils_io import validate_and_write, set_run_context
from etl import utils_io


def test_validate_and_write_success(tmp_path: Path):
    out_path = tmp_path / "ventas.csv"
    set_run_context(run_id="testrun_ok", strict=True)
    utils_io.REPORTS_DIR = tmp_path / "reports"  # type: ignore

    df = pd.DataFrame(
        {
            "cliente": ["A"],
            "periodo": ["202501"],
            "anio": [2025],
            "mes": [1],
            "mwh": [100.0],
        }
    )

    rows = validate_and_write("ventas_mensual_mwh", df, out_path)
    assert out_path.exists()
    assert rows == 1


def test_validate_and_write_fail_strict(tmp_path: Path):
    out_path = tmp_path / "ventas.csv"
    reports_dir = tmp_path / "reports"
    utils_io.REPORTS_DIR = reports_dir  # type: ignore
    set_run_context(run_id="testrun_fail", strict=True)

    df = pd.DataFrame({"cliente": ["A"], "mwh": [100.0]})  # falta periodo/anio/mes

    with pytest.raises(Exception):
        validate_and_write("ventas_mensual_mwh", df, out_path)

    report_files = list(reports_dir.glob("validation_testrun_fail_ventas_mensual_mwh.json"))
    assert report_files, "Debe generarse reporte de validación fallida"
