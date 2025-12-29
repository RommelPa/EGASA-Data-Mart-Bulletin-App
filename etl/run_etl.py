# -*- coding: utf-8 -*-

"""Script de orquestación para el ETL de EGASA."""

from __future__ import annotations

import logging
from datetime import datetime

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from etl import pipelines, config


def setup_logging() -> None:
    log_file = config.LOG_FILE
    log_file.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )


def main() -> None:
    config.ensure_directories()
    setup_logging()
    logger = logging.getLogger(__name__)
    logger.info("Iniciando ETL %s", datetime.utcnow().isoformat())

    files_read = []
    datasets = {}

    prod_df, prod15_files, prod_datasets = pipelines.run_produccion()
    files_read.extend(prod15_files)
    datasets.update(prod_datasets)

    hidro_files, hidro_datasets = pipelines.run_hidrologia()
    files_read.extend(hidro_files)
    datasets.update(hidro_datasets)

    fact_files, fact_datasets = pipelines.run_facturacion()
    files_read.extend(fact_files)
    datasets.update(fact_datasets)

    contratos_files, contratos_datasets = pipelines.run_contratos()
    files_read.extend(contratos_files)
    datasets.update(contratos_datasets)

    from etl.quality_checks import write_metadata

    write_metadata(
        path=None,  # no se usa, mantenido para compatibilidad
        datasets_info=datasets,
        files_read=files_read,
    )

    logger.info("ETL finalizado.")


if __name__ == "__main__":
    main()
