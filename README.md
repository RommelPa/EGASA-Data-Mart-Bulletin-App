
# EGASA - ETL & Data Mart

Este proyecto implementa un sistema de ingeniería de datos para EGASA, permitiendo la consolidación de información operativa, hidrológica y comercial sin necesidad de bases de datos externas o infraestructura cloud.

## Instalación
1. Instale Python 3.10+ (Windows).
2. Abra una terminal en la carpeta del proyecto.
3. Instale las dependencias:
   ```bash
   pip install -r requirements.txt
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
1. **Ejecutar ETL**:
   ```bash
   python etl/run_etl.py
   ```
   Esto generará los CSVs en `./data_mart/` y actualizará `metadata.json`.

2. **Ejecutar Dashboard**:
   ```bash
   streamlit run app/main.py
   ```

## Actualización Mensual
Simplemente agregue los nuevos archivos Excel a `data_landing` y vuelva a ejecutar el ETL. El sistema de datos 15-min es incremental y evitará duplicados.
