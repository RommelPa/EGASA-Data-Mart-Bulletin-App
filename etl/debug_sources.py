# -*- coding: utf-8 -*-

"""Script de diagnóstico rápido de los archivos en data_landing."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Iterable, List
import sys

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from etl import config
from etl.utils_io import detect_header_row

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("debug_sources")


def _exists(paths: Iterable[Path]) -> List[Path]:
    return [p for p in paths if p.exists()]


def _print_header_row(df_preview: pd.DataFrame, keywords: List[str]) -> None:
    idx = detect_header_row(df_preview, keywords=keywords)
    logger.info("Fila candidata para header (%s): %s", keywords, idx)
    with pd.option_context("display.max_columns", 40, "display.width", 200):
        logger.info("Vista previa fila %s: %s", idx, df_preview.iloc[idx].tolist())


def check_historico(path: Path) -> None:
    logger.info("== Histórico 2010 ==")
    preview = pd.read_excel(path, sheet_name="2010", header=None)
    logger.info("Shape hoja 2010: %s", preview.shape)
    _print_header_row(preview.head(40), ["central", "enero", "diciembre"])
    match_rows = preview.apply(
        lambda row: any(str(v).upper().strip() == "CENTRAL" for v in row), axis=1
    )
    if match_rows.any():
        idx = match_rows.idxmax()
        logger.info("Fila con CENTRAL: %s -> %s", idx, preview.iloc[idx].tolist())


def check_15min(path: Path) -> None:
    logger.info("== Producción 15-min ==")
    df = pd.read_excel(path, header=None)
    logger.info("Shape: %s", df.shape)
    a1 = df.iat[0, 0] if not df.empty else None
    logger.info("Celda A1: %s", a1)
    header_like = df[df.apply(lambda r: any(str(v).upper().startswith("FECHA") for v in r), axis=1)]
    logger.info("Filas con FECHA/HORA (primeras 5):\n%s", header_like.head())


def check_control_hidrologico(path: Path) -> None:
    logger.info("== Control Hidrológico ==")
    xls = pd.ExcelFile(path)
    logger.info("Hojas: %s", xls.sheet_names)

    for hoja in ["CH", "CAUDAL"]:
        if hoja not in xls.sheet_names:
            logger.warning("Hoja %s no encontrada", hoja)
            continue
        df = pd.read_excel(path, sheet_name=hoja, header=None)
        logger.info("Hoja %s shape: %s", hoja, df.shape)
        header_rows = df[df.apply(lambda r: "AÑO" in [str(v).upper() for v in r], axis=1)]
        if not header_rows.empty:
            first_idx = header_rows.index[0]
            logger.info("Fila header %s: idx=%s valores=%s", hoja, first_idx, df.loc[first_idx].tolist())
        else:
            logger.warning("No se detectó header AÑO/ENERO en %s", hoja)


def check_bd_represas(path: Path) -> None:
    logger.info("== BDREPRESAS ==")
    xls = pd.ExcelFile(path)
    logger.info("Hojas: %s", xls.sheet_names)
    if "INFORMEDIARIO" in xls.sheet_names:
        df = pd.read_excel(path, sheet_name="INFORMEDIARIO", header=None)
        logger.info("Hoja INFORMEDIARIO shape: %s", df.shape)
    else:
        logger.warning("Hoja INFORMEDIARIO no encontrada")


def check_facturacion(path: Path) -> None:
    logger.info("== Facturación ==")
    xls = pd.ExcelFile(path)
    logger.info("Hojas: %s", xls.sheet_names)
    if "VENTAS (MWh)" in xls.sheet_names:
        df = pd.read_excel(path, sheet_name="VENTAS (MWh)", header=None)
        logger.info("VENTAS (MWh) shape: %s", df.shape)
        _print_header_row(df.head(20), ["cliente", "enero"])
    else:
        logger.warning("Hoja VENTAS (MWh) no encontrada")


def main() -> None:
    landing = config.DATA_LANDING
    expected = [
        landing / "PRODUCCION EGASA DESDE 2010 (NOV2025).xlsx",
        landing / "PRODUCCIÓN DE ENERGÍA_ENERO 2025.xlsx",
        landing / "Control Hidrológico.xlsx",
        landing / "BDREPRESAS.xlsx",
        landing / "Facturacion 2025.xlsx",
    ]

    found = _exists(expected)
    missing = [p for p in expected if p not in found]
    for p in found:
        logger.info("OK: %s", p.name)
    for p in missing:
        logger.error("FALTA: %s", p.name)

    if found:
        check_historico(expected[0])
        check_15min(expected[1])
        check_control_hidrologico(expected[2])
        check_bd_represas(expected[3])
        check_facturacion(expected[4])


if __name__ == "__main__":
    main()
