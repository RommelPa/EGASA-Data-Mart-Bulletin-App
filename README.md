
# EGASA - ETL & Data Mart

Este proyecto implementa un sistema de ingeniería de datos para EGASA, permitiendo la consolidación de información operativa, hidrológica y comercial sin necesidad de bases de datos externas o infraestructura cloud.

## Instalación
1. Instale Python 3.10+ (Windows).
2. Abra una terminal en la carpeta del proyecto.
3. Instale las dependencias (vía `pyproject.toml`):
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # en Windows: .venv\Scripts\activate
   pip install --upgrade pip
   pip install .
   ```

## Preparación de Datos (data_landing)
Coloque los archivos Excel originales en la carpeta `./data_landing/`. El ETL espera los siguientes nombres (o patrones):
- `PRODUCCION EGASA DESDE 2010 (NOV2025).xlsx`
- `PRODUCCIÓN DE ENERGÍA_OCTUBRE 2025.xlsx` (y otros meses)
- `Control Hidrológico.xlsx`
- `BDREPRESAS.xlsx`
- `Facturacion 2025.xlsx`
- `Revision de Volumen Optimo de Contratos 2025-2036 - Listo.xlsx`

## Ejecución
1. **Ejecutar ETL** (CLI `python -m etl` o binario `egasa-etl`):
   ```bash
   python -m etl --strict --config config.yml --input data_landing --output data_mart
   # variantes:
   python -m etl --non-strict         # solo advierte validaciones
   python -m etl --month 202501       # marca en logs el mes objetivo (placeholder)
   python -m etl --force              # placeholder para reprocesar todo
   ```
   Esto generará los CSVs en `./data_mart/` y actualizará `metadata.json`.
   - Si una validación pandera falla, se escribirá un reporte en `./reports/validation_<run_id>_<tabla>.json`.
   - Cada corrida queda registrada en `logs/etl_runs.jsonl` con run_id, estado, tablas y filas por tabla.
   - Logs incluyen `run_id`, stage, file, rows_in/out, duration_ms para facilitar trazabilidad.

2. **Ejecutar Dashboard**:
   ```bash
   streamlit run app/main.py
   ```

## Troubleshooting
- `FileNotFoundError` al correr el ETL: revisa `config.yml` y que los archivos esperados existan en `data_landing` (puedes marcar `required=false` por fuente si solo algunas son opcionales).
- `Schema validation failed`: revisa los reportes en `./reports/validation_<run_id>_*.json` para ver filas/columnas faltantes.
- Cache de Streamlit desactualizada: si cambiaste CSVs o `metadata.json`, usa “Clear cache” en el menú de Streamlit o reinicia la app.
- Faltan dependencias frontend (Vite/React): el frontend en `frontend/` es un placeholder; el dashboard real usa Streamlit.

## Actualización Mensual
Simplemente agregue los nuevos archivos Excel a `data_landing` y vuelva a ejecutar el ETL. El sistema de datos 15-min es incremental y evitará duplicados.

## Configuración declarativa
El ETL lee `config.yml` (o `config.toml`) en la raíz del proyecto:
- Rutas: `paths.input` (landing), `paths.output` (data_mart), `paths.reference`, `paths.logs`.
- Patrones de archivos/sheets por fuente en `sources.*`.
- Reglas por tabla (`tables.*`) con columnas obligatorias y renombrados.

Ejemplo mínimo (`config.yml`):
```yaml
paths:
  input: data_landing
  output: data_mart

sources:
  facturacion:
    pattern: "Facturacion"
    sheets:
      ventas_mwh: "VENTAS (MWh)"
      ventas_soles: "VENTAS (S)"
      ingresos: "Ingresos"

tables:
  ventas_mensual_mwh:
    required_columns: ["cliente", "periodo"]
```

Si falta una hoja o una columna requerida, el ETL abortará con un mensaje legible indicando la tabla/hoja faltante.
