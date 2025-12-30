# -*- coding: utf-8 -*-

"""Esquemas de validación de datos con pandera."""

from __future__ import annotations

import pandera as pa
from pandera import Column, DataFrameSchema, Check


SCHEMAS = {
    "ventas_mensual_mwh": DataFrameSchema(
        {
            "cliente": Column(pa.String, nullable=False),
            "periodo": Column(pa.String, coerce=True, nullable=False, checks=Check.str_length(6, 6)),
            "anio": Column(pa.Int64, nullable=True, coerce=True),
            "mes": Column(pa.Int64, nullable=True, coerce=True),
            "mwh": Column(pa.Float64, nullable=True, coerce=True, checks=Check.ge(0)),
        },
        coerce=True,
    ),
    "ventas_mensual_soles": DataFrameSchema(
        {
            "cliente": Column(pa.String, nullable=False),
            "periodo": Column(pa.String, coerce=True, nullable=False, checks=Check.str_length(6, 6)),
            "anio": Column(pa.Int64, nullable=True, coerce=True),
            "mes": Column(pa.Int64, nullable=True, coerce=True),
            "soles": Column(pa.Float64, nullable=True, coerce=True, checks=Check.ge(0)),
        },
        coerce=True,
    ),
    "ingresos_mensual": DataFrameSchema(
        {
            "anio": Column(pa.Int64, nullable=False, coerce=True),
            "mes": Column(pa.Int64, nullable=False, coerce=True, checks=Check.in_range(1, 12)),
            "cliente_o_concepto": Column(pa.String, nullable=False),
            "soles": Column(pa.Float64, nullable=False, coerce=True),
        },
        coerce=True,
    ),
    "represas_diario": DataFrameSchema(
        {
            "fecha": Column(pa.DateTime, nullable=True, coerce=True),
            "reservorio": Column(pa.String, nullable=False),
            "volumen_actual": Column(pa.Float64, nullable=True, coerce=True),
            "pct_llenado": Column(pa.Float64, nullable=True, coerce=True, checks=Check.in_range(0, 150, inclusive=True)),
        },
        coerce=True,
    ),
    "hidro_volumen_mensual": DataFrameSchema(
        {
            "reservorio": Column(pa.String, nullable=False),
            "anio": Column(pa.Int64, nullable=False, coerce=True),
            "mes": Column(pa.String, nullable=False, checks=Check.str_length(2, 2)),
            "periodo": Column(pa.String, nullable=False, checks=Check.str_length(6, 6)),
            "volumen_000m3": Column(pa.Float64, nullable=True, coerce=True, checks=Check.ge(0)),
        },
        coerce=True,
    ),
    "hidro_caudal_mensual": DataFrameSchema(
        {
            "estacion": Column(pa.String, nullable=False),
            "anio": Column(pa.Int64, nullable=False, coerce=True),
            "mes": Column(pa.String, nullable=False, checks=Check.str_length(2, 2)),
            "periodo": Column(pa.String, nullable=False, checks=Check.str_length(6, 6)),
            "caudal_m3s": Column(pa.Float64, nullable=True, coerce=True, checks=Check.ge(0)),
        },
        coerce=True,
    ),
    "balance_perfil_mensual": DataFrameSchema(
        {
            "periodo": Column(pa.String, nullable=False, checks=Check.str_length(6, 6)),
            "fecha_mes": Column(pa.DateTime, nullable=True, coerce=True),
            "concepto": Column(pa.String, nullable=False),
            "energia_mwh": Column(pa.Float64, nullable=True, coerce=True),
            "energia_gwh": Column(pa.Float64, nullable=True, coerce=True),
        },
        coerce=True,
    ),
    "balance_r_mensual": DataFrameSchema(
        {
            "periodo": Column(pa.String, nullable=False, checks=Check.str_length(6, 6)),
            "fecha_mes": Column(pa.DateTime, nullable=True, coerce=True),
            "segmento": Column(pa.String, nullable=False),
            "energia_mwh": Column(pa.Float64, nullable=True, coerce=True),
        },
        coerce=True,
    ),
    "contratos_base": DataFrameSchema(
        {
            "cliente": Column(pa.String, nullable=False),
            "tipo_contrato": Column(pa.String, nullable=True),
            "fecha_inicio": Column(pa.DateTime, nullable=True, coerce=True),
            "fecha_fin": Column(pa.DateTime, nullable=True, coerce=True),
            "potencia_mw": Column(pa.Float64, nullable=True, coerce=True, checks=Check.ge(0)),
            "precio_hp_usd_mwh": Column(pa.Float64, nullable=True, coerce=True, checks=Check.ge(0)),
            "precio_fp_usd_mwh": Column(pa.Float64, nullable=True, coerce=True, checks=Check.ge(0)),
        },
        coerce=True,
    ),
    "contratos_riesgo": DataFrameSchema(
        {
            "cliente": Column(pa.String, nullable=False),
            "tipo_contrato": Column(pa.String, nullable=True),
            "fecha_inicio": Column(pa.DateTime, nullable=True, coerce=True),
            "fecha_fin": Column(pa.DateTime, nullable=True, coerce=True),
            "potencia_mw": Column(pa.Float64, nullable=True, coerce=True, checks=Check.ge(0)),
            "precio_hp_usd_mwh": Column(pa.Float64, nullable=True, coerce=True, checks=Check.ge(0)),
            "precio_fp_usd_mwh": Column(pa.Float64, nullable=True, coerce=True, checks=Check.ge(0)),
        },
        coerce=True,
    ),
}


def get_schema(dataset: str) -> DataFrameSchema | None:
    """Obtener el esquema pandera para un dataset conocido."""
    if dataset in SCHEMAS:
        return SCHEMAS[dataset]
    # patrones como generacion_15min_YYYYMM no se validan (por ahora)
    return None


__all__ = ["get_schema", "SCHEMAS"]
