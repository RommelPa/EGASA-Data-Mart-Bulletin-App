import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from app.charts.theme import (
    AxisFormat,
    PLOTLY_CONFIG,
    apply_exec_style,
    apply_soft_markers,
    apply_thin_lines,
    apply_unified_hover,
    format_axis_units,
)
from utils.data import load_csv
from utils.filters import ensure_periodo_str, filter_by_periodo, sidebar_periodo_selector

st.set_page_config(layout="wide")
st.title("🔎 Insights / Cruces")

# -----------------------------
# Helpers
# -----------------------------
def scatter_with_fit(df: pd.DataFrame, x: str, y: str, title: str) -> go.Figure:
    d = df[[x, y]].dropna().copy()
    fig = go.Figure()

    friendly_labels = {
        "caudal_m3s": "Caudal (m³/s)",
        "volumen_millones_m3": "Volumen útil (millones de m³)",
        "Volumen útil (Mm³)": "Volumen útil (millones de m³)",
        "gen_mwh": "Generación (MWh)",
    }
    friendly_formats = {
        "caudal_m3s": ",.2f",
        "volumen_millones_m3": ",.2f",
        "Volumen útil (Mm³)": ",.2f",
        "gen_mwh": ",.0f",
    }

    if d.empty:
        return apply_exec_style(
            fig,
            title=f"{title}",
            subtitle="Sin datos para el rango seleccionado",
            hovermode="closest",
        )

    fig.add_trace(go.Scatter(x=d[x], y=d[y], mode="markers", name="Datos"))

    # Fit lineal si se puede
    if d[x].nunique() >= 2:
        try:
            a, b = np.polyfit(d[x].astype(float).values, d[y].astype(float).values, 1)
            x_line = np.linspace(d[x].min(), d[x].max(), 50)
            y_line = a * x_line + b
            fig.add_trace(go.Scatter(x=x_line, y=y_line, mode="lines", name="Tendencia"))
        except Exception:
            # si algo raro pasa, no bloquea el dashboard
            pass

    format_axis_units(
        fig,
        x=AxisFormat(title=friendly_labels.get(x, x), tickformat=friendly_formats.get(x, ",.2f")),
        y=AxisFormat(title=friendly_labels.get(y, y), tickformat=friendly_formats.get(y, ",.2f")),
    )
    apply_exec_style(fig, title=title, subtitle="Relación simple", hovermode="closest")
    apply_soft_markers(fig)
    apply_thin_lines(fig)
    return fig


# -----------------------------
# Load datasets
# -----------------------------
gen = load_csv("generacion_mensual.csv")
vol = load_csv("hidro_volumen_mensual.csv")
cau = load_csv("hidro_caudal_mensual.csv")
ventas = load_csv("ventas_mensual_mwh.csv")
precio = load_csv("precio_medio_mensual.csv")
r = load_csv("balance_r_mensual.csv")

# -----------------------------
# Normalizar periodos (CORRECTO)
# -----------------------------
if not gen.empty and "periodo" in gen.columns:
    gen = ensure_periodo_str(gen, "periodo")
if not vol.empty and "periodo" in vol.columns:
    vol = ensure_periodo_str(vol, "periodo")
if not cau.empty and "periodo" in cau.columns:
    cau = ensure_periodo_str(cau, "periodo")
if not ventas.empty and "periodo" in ventas.columns:
    ventas = ensure_periodo_str(ventas, "periodo")
if not precio.empty and "periodo" in precio.columns:
    precio = ensure_periodo_str(precio, "periodo")
if not r.empty and "periodo" in r.columns:
    r = ensure_periodo_str(r, "periodo")

# -----------------------------
# construir tabla mensual “macro”
# -----------------------------
if gen.empty or "energia_mwh" not in gen.columns:
    st.warning("Falta generacion_mensual.csv o columna energia_mwh para insights.")
    st.stop()

gen_total = (
    gen.groupby("periodo")["energia_mwh"]
    .sum()
    .reset_index()
    .rename(columns={"energia_mwh": "gen_mwh"})
)

ventas_total = pd.DataFrame(columns=["periodo", "ventas_mwh"])
if not ventas.empty and "mwh" in ventas.columns:
    ventas_total = (
        ventas.groupby("periodo")["mwh"]
        .sum()
        .reset_index()
        .rename(columns={"mwh": "ventas_mwh"})
    )

precio_total = pd.DataFrame(columns=["periodo", "precio_medio"])
if not precio.empty and "precio_medio_soles_mwh" in precio.columns:
    precio_total = (
        precio.groupby("periodo")["precio_medio_soles_mwh"]
        .mean()
        .reset_index()
        .rename(columns={"precio_medio_soles_mwh": "precio_medio"})
    )

caudal_total = pd.DataFrame(columns=["periodo", "caudal_m3s"])
if not cau.empty and "caudal_m3s" in cau.columns:
    caudal_total = cau.groupby("periodo")["caudal_m3s"].mean().reset_index()

vol_total = pd.DataFrame(columns=["periodo", "volumen_000m3"])
if not vol.empty and "volumen_000m3" in vol.columns:
    vol_total = vol.groupby("periodo")["volumen_000m3"].sum().reset_index()
    vol_total["volumen_millones_m3"] = vol_total["volumen_000m3"] / 1_000

base = (
    gen_total.merge(ventas_total, on="periodo", how="left")
    .merge(precio_total, on="periodo", how="left")
    .merge(caudal_total, on="periodo", how="left")
    .merge(vol_total, on="periodo", how="left")
)

periodos = sorted(base["periodo"].dropna().unique())
p_ini, p_fin = sidebar_periodo_selector(periodos, "Periodo Insights")
base = filter_by_periodo(base, "periodo", p_ini, p_fin)

# -----------------------------
# 1) Generación vs Ventas
# -----------------------------
st.markdown("## 1) Generación vs Ventas")
if "ventas_mwh" in base.columns and base["ventas_mwh"].notna().any():
    fig = px.line(base, x="periodo", y=["gen_mwh", "ventas_mwh"], title="Generación vs Ventas (MWh)")
    apply_thin_lines(fig)
    apply_soft_markers(fig)
    apply_unified_hover(fig, fmt=":,.0f", units="MWh")
    format_axis_units(
        fig,
        x=AxisFormat(title="Periodo"),
        y=AxisFormat(title="Energía (MWh)", tickformat=",.0f"),
    )
    apply_exec_style(
        fig,
        title="Generación vs Ventas",
        subtitle="Energía (MWh) consolidada",
        source="EGASA · Data Mart",
    )
    st.plotly_chart(fig, use_container_width=True, config=PLOTLY_CONFIG)
else:
    st.info("No hay ventas para cruzar (ventas_mensual_mwh).")

# -----------------------------
# 2) Hidrología vs Generación
# -----------------------------
st.markdown("## 2) Hidrología vs Generación")
c1, c2 = st.columns(2)

if "caudal_m3s" in base.columns and base["caudal_m3s"].notna().any():
    c1.plotly_chart(
        scatter_with_fit(base, "caudal_m3s", "gen_mwh", "Caudal vs Generación"),
        use_container_width=True,
        config=PLOTLY_CONFIG,
    )
else:
    c1.info("Sin caudal para el rango.")

if "volumen_millones_m3" in base.columns and base["volumen_millones_m3"].notna().any():
    c2.plotly_chart(
        scatter_with_fit(base.rename(columns={"volumen_millones_m3": "Volumen útil (Mm³)"}), "Volumen útil (Mm³)", "gen_mwh", "Volumen útil vs Generación"),
        use_container_width=True,
        config=PLOTLY_CONFIG,
    )
else:
    c2.info("Sin volumen para el rango.")

# -----------------------------
# 3) Precio medio
# -----------------------------
st.markdown("## 3) Precio medio mensual (S/MWh)")
if "precio_medio" in base.columns and base["precio_medio"].notna().any():
    fig = px.line(base, x="periodo", y="precio_medio", title="Precio medio mensual (S/MWh)")
    apply_thin_lines(fig)
    apply_soft_markers(fig)
    apply_unified_hover(fig, fmt=":,.2f", units="S/MWh")
    format_axis_units(
        fig,
        x=AxisFormat(title="Periodo"),
        y=AxisFormat(title="Precio medio (S/MWh)", tickformat=",.2f"),
    )
    apply_exec_style(
        fig,
        title="Precio medio mensual",
        subtitle="Soles por MWh",
        source="EGASA · Data Mart",
    )
    st.plotly_chart(fig, use_container_width=True, config=PLOTLY_CONFIG)
else:
    st.info("No hay precio medio para el rango.")
