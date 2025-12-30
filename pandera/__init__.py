# Minimal stub implementation of pandera used for local/offline validation.
# This is not a full replacement for pandera; it supports only the features
# required by this project (DataFrameSchema, Column, Check, SchemaErrors).

from __future__ import annotations

import pandas as pd

String = "string"
Int64 = "int64"
Float64 = "float64"
DateTime = "datetime64[ns]"


class Check:
    def __init__(self, fn, name: str | None = None):
        self.fn = fn
        self.name = name or fn.__name__

    def __call__(self, series: pd.Series):
        return self.fn(series)

    @staticmethod
    def ge(min_value):
        return Check(lambda s: s >= min_value, name=f"ge_{min_value}")

    @staticmethod
    def in_range(min_value, max_value, inclusive=True):
        if inclusive:
            return Check(lambda s: s.between(min_value, max_value), name=f"in_range_{min_value}_{max_value}")
        return Check(lambda s: (s > min_value) & (s < max_value), name=f"in_range_{min_value}_{max_value}")

    @staticmethod
    def str_length(min_value, max_value):
        return Check(
            lambda s: s.astype(str).str.len().between(min_value, max_value),
            name=f"str_length_{min_value}_{max_value}",
        )


class Column:
    def __init__(self, dtype, nullable=False, coerce=False, checks=None):
        self.dtype = dtype
        self.nullable = nullable
        self.coerce = coerce
        self.checks = checks if isinstance(checks, list) else ([checks] if checks else [])


class SchemaErrors(Exception):
    def __init__(self, failure_cases):
        super().__init__("Schema validation failed")
        self.failure_cases = pd.DataFrame(failure_cases)


class DataFrameSchema:
    def __init__(self, columns: dict, coerce: bool = False):
        self.columns = columns
        self.coerce = coerce

    def _coerce_series(self, series: pd.Series, dtype):
        if dtype in {Int64, Float64}:
            return pd.to_numeric(series, errors="coerce")
        if dtype == DateTime:
            return pd.to_datetime(series, errors="coerce")
        return series.astype(str)

    def validate(self, df: pd.DataFrame, lazy: bool = False):
        failures = []
        df_validated = df.copy()

        for col_name, col in self.columns.items():
            if col_name not in df_validated.columns:
                failures.append({"column": col_name, "check": "required", "failure_case": "missing", "index": None})
                if not lazy:
                    raise SchemaErrors(failures)
                continue

            series = df_validated[col_name]
            if col.coerce or self.coerce:
                series = self._coerce_series(series, col.dtype)
                df_validated[col_name] = series

            if not col.nullable and series.isna().any():
                na_indices = series[series.isna()].index.tolist()
                failures.append({"column": col_name, "check": "non_null", "failure_case": "NaN", "index": na_indices})
                if not lazy:
                    raise SchemaErrors(failures)

            for check in col.checks:
                result = check(series)
                if isinstance(result, pd.Series):
                    failed_idx = result.index[~result].tolist()
                    if failed_idx:
                        failures.append(
                            {"column": col_name, "check": check.name, "failure_case": series.loc[failed_idx].tolist(), "index": failed_idx}
                        )
                        if not lazy:
                            raise SchemaErrors(failures)
                elif not bool(result):
                    failures.append({"column": col_name, "check": check.name, "failure_case": "failed", "index": None})
                    if not lazy:
                        raise SchemaErrors(failures)

        if failures:
            raise SchemaErrors(failures)

        return df_validated


__all__ = ["DataFrameSchema", "Column", "Check", "SchemaErrors", "String", "Int64", "Float64", "DateTime"]

# errors namespace compatibility
class errors:
    SchemaErrors = SchemaErrors
