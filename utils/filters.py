from __future__ import annotations

import streamlit as st
import pandas as pd


def sidebar_periodo_selector(periodos: list[str], label: str = "Periodo") -> tuple[str, str]:
    """
    periodos debe venir como lista de strings YYYYMM (ordenada).
    """
    if not periodos:
        return ("", "")
    i1 = 0
    i2 = len(periodos) - 1
    col1, col2 = st.sidebar.columns(2)
    p_ini = col1.selectbox(f"{label} inicio", periodos, index=i1)
    p_fin = col2.selectbox(f"{label} fin", periodos, index=i2)
    if p_ini > p_fin:
        p_ini, p_fin = p_fin, p_ini
    return p_ini, p_fin


def ensure_periodo_str(df: pd.DataFrame, col_periodo: str = "periodo") -> pd.DataFrame:
    """
    Convierte col_periodo a string YYYYMM:
    - int/float -> 'YYYYMM'
    - string -> limpia espacios
    """
    if df.empty or col_periodo not in df.columns:
        return df

    out = df.copy()

    # Si es numérico, pasarlo a int y luego a string
    if pd.api.types.is_numeric_dtype(out[col_periodo]):
        out[col_periodo] = pd.to_numeric(out[col_periodo], errors="coerce").astype("Int64")
        out[col_periodo] = out[col_periodo].astype(str)
    else:
        out[col_periodo] = out[col_periodo].astype(str)

    out[col_periodo] = out[col_periodo].str.strip()
    # por si quedaron cosas como '202501.0'
    out[col_periodo] = out[col_periodo].str.replace(r"\.0$", "", regex=True)

    # Normaliza longitud (por seguridad)
    out.loc[out[col_periodo].str.len() == 5, col_periodo] = "0" + out[col_periodo]

    return out


def filter_by_periodo(df: pd.DataFrame, col_periodo: str, p_ini: str, p_fin: str) -> pd.DataFrame:
    if df.empty or not p_ini or not p_fin or col_periodo not in df.columns:
        return df

    df2 = ensure_periodo_str(df, col_periodo)
    p_ini = str(p_ini).strip()
    p_fin = str(p_fin).strip()

    return df2[(df2[col_periodo] >= p_ini) & (df2[col_periodo] <= p_fin)].copy()