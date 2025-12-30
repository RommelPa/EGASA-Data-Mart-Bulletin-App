# -*- coding: utf-8 -*-

"""Script de orquestación para el ETL de EGASA."""

from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

from etl import pipelines, config
from etl.logging_utils import setup_logging
from etl.utils_io import set_run_context, default_log_extra, record_etl_run, ensure_runs_log

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Orquestador ETL EGASA")
    parser.add_argument("--input", help="Directorio data_landing override")
    parser.add_argument("--output", help="Directorio data_mart override")
    parser.add_argument("--config", help="Ruta alternativa a config.yml|toml")
    parser.add_argument("--month", help="Mes objetivo (YYYYMM) opcional", default=None)
    parser.add_argument("--force", action="store_true", help="Forzar re-procesamiento (placeholder)")
    strict_group = parser.add_mutually_exclusive_group()
    strict_group.add_argument("--strict", action="store_true", help="Fallar si hay errores de validación (default)")
    strict_group.add_argument("--non-strict", action="store_true", help="Solo advertir validaciones fallidas")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    strict = False if args.non_strict else True
    run_id = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    # Aplicar overrides tempranos
    config.apply_runtime_overrides(
        config_path=Path(args.config) if args.config else None,
        paths_override={"input": args.input, "output": args.output},
    )
    set_run_context(run_id=run_id, strict=strict)
    config.ensure_directories()
    ensure_runs_log()
    setup_logging(config.LOG_FILE, run_id=run_id)
    logger = logging.getLogger(__name__)
    logger.info("Iniciando ETL %s", datetime.utcnow().isoformat(), extra=default_log_extra(stage="orchestrator", run_id=run_id))
    cfg_path = config.BASE_DIR / "config.yml"
    logger.info("Config cargada: %s", cfg_path if cfg_path.exists() else "defaults", extra=default_log_extra(stage="orchestrator", run_id=run_id))
    logger.info("run_id=%s strict=%s", run_id, strict, extra=default_log_extra(stage="orchestrator", run_id=run_id))

    files_read = []
    datasets = {}
    tables_rows: dict = {}
    started_at = datetime.utcnow().isoformat()

    try:
        t0 = time.perf_counter()
        prod_df, prod15_files, prod_datasets = pipelines.run_produccion()
        duration = int((time.perf_counter() - t0) * 1000)
        files_read.extend(prod15_files)
        datasets.update(prod_datasets)
        tables_rows.update({k: len(v[0]) for k, v in prod_datasets.items()})
        logger.info("Producción completada", extra=default_log_extra(stage="produccion", file="*", rows_in=len(prod15_files), rows_out=len(prod_df), duration_ms=duration))

        t0 = time.perf_counter()
        hidro_files, hidro_datasets = pipelines.run_hidrologia()
        duration = int((time.perf_counter() - t0) * 1000)
        files_read.extend(hidro_files)
        datasets.update(hidro_datasets)
        tables_rows.update({k: len(v[0]) for k, v in hidro_datasets.items()})
        logger.info("Hidrología completada", extra=default_log_extra(stage="hidrologia", file="*", rows_in=len(hidro_files), rows_out=sum(len(v[0]) for v in hidro_datasets.values()), duration_ms=duration))

        t0 = time.perf_counter()
        fact_files, fact_datasets = pipelines.run_facturacion()
        duration = int((time.perf_counter() - t0) * 1000)
        files_read.extend(fact_files)
        datasets.update(fact_datasets)
        tables_rows.update({k: len(v[0]) for k, v in fact_datasets.items()})
        logger.info("Facturación completada", extra=default_log_extra(stage="facturacion", file="*", rows_in=len(fact_files), rows_out=sum(len(v[0]) for v in fact_datasets.values()), duration_ms=duration))

        t0 = time.perf_counter()
        contratos_files, contratos_datasets = pipelines.run_contratos()
        duration = int((time.perf_counter() - t0) * 1000)
        files_read.extend(contratos_files)
        datasets.update(contratos_datasets)
        tables_rows.update({k: len(v[0]) for k, v in contratos_datasets.items()})
        logger.info("Contratos completados", extra=default_log_extra(stage="contratos", file="*", rows_in=len(contratos_files), rows_out=sum(len(v[0]) for v in contratos_datasets.values()), duration_ms=duration))

        t0 = time.perf_counter()
        balance_files, balance_datasets = pipelines.run_balance_energia()
        duration = int((time.perf_counter() - t0) * 1000)
        files_read.extend(balance_files)
        datasets.update(balance_datasets)
        tables_rows.update({k: len(v[0]) for k, v in balance_datasets.items()})
        logger.info("Balance energía completado", extra=default_log_extra(stage="balance_energia", file="*", rows_in=len(balance_files), rows_out=sum(len(v[0]) for v in balance_datasets.values()), duration_ms=duration))

        from etl.quality_checks import write_metadata

        write_metadata(
            path=None,  # no se usa, mantenido para compatibilidad
            datasets_info=datasets,
            files_read=files_read,
        )

        logger.info("ETL finalizado.", extra=default_log_extra(stage="orchestrator", run_id=run_id))
        finished_at = datetime.utcnow().isoformat()
        record_etl_run(run_id=run_id, started_at=started_at, finished_at=finished_at, status="success", tables=tables_rows)
    except Exception as exc:
        finished_at = datetime.utcnow().isoformat()
        suggestion = "Verifica config.yml (patrones y sheets) y la existencia de archivos en data_landing."
        logger.error("ETL falló: %s | Sugerencia: %s", exc, suggestion, extra=default_log_extra(stage="orchestrator", run_id=run_id))
        record_etl_run(run_id=run_id, started_at=started_at, finished_at=finished_at, status="failed", tables=tables_rows, error=str(exc))
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
