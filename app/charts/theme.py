"""Tema ejecutivo y utilitarios de formato para los gráficos Plotly del dashboard."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Mapping, MutableMapping, Sequence

import plotly.graph_objects as go


EXEC_THEME = {
    "font_family": "Inter, 'Open Sans', Arial",
    "font_color": "#1F2A44",
    "bg_color": "#FFFFFF",
    "grid_color": "#E6E9F0",
    "primary": "#2E7BD9",
    "secondary": "#4FAF62",
    "accent": "#8393B9",
    "hover_bg": "#F3F6FB",
}


SPANISH_MONTH_ABBR = ["Ene", "Feb", "Mar", "Abr", "May", "Jun", "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"]


def short_spanish_date(dt) -> str:
    """Devuelve un texto estilo '20 Dic 2025' compatible con hover."""
    try:
        month = SPANISH_MONTH_ABBR[dt.month - 1]
        return f"{dt.day:02d} {month} {dt.year}"
    except Exception:
        return str(dt)


@dataclass
class AxisFormat:
    title: str
    tickformat: str | None = None
    ticksuffix: str | None = None
    showgrid: bool = True
    hover_format: str | None = None
    axis: str = "y"


def _apply_axis(fig: go.Figure, axis_format: AxisFormat, secondary: bool = False) -> None:
    axis_key = f"{axis_format.axis}axis"
    if secondary and axis_format.axis == "y":
        axis_key = "yaxis2"
    axis_args: MutableMapping[str, object] = {
        "title": {"text": axis_format.title, "font": {"size": 13}},
        "showgrid": axis_format.showgrid,
        "gridcolor": EXEC_THEME["grid_color"],
        "zeroline": False,
    }
    if axis_format.tickformat:
        axis_args["tickformat"] = axis_format.tickformat
    if axis_format.ticksuffix:
        axis_args["ticksuffix"] = axis_format.ticksuffix
    fig.update_layout(**{axis_key: axis_args})


def format_axis_units(
    fig: go.Figure,
    *,
    x: AxisFormat | None = None,
    y: AxisFormat | None = None,
    y2: AxisFormat | None = None,
    hover_formats: Mapping[str, str] | None = None,
) -> None:
    """Aplica formatos de ejes y hover para una figura.

    Args:
        fig: Figura Plotly.
        x: Configuración del eje X.
        y: Configuración del eje Y principal.
        y2: Configuración del eje Y secundario (para dual axis).
        hover_formats: Mapeo opcional trace.name -> formato de valor usado en hovertemplate.
    """

    if x:
        _apply_axis(fig, x)
    if y:
        _apply_axis(fig, y)
    if y2:
        _apply_axis(fig, y2, secondary=True)
        fig.update_layout(yaxis2={"overlaying": "y", "side": "right"})

    if hover_formats:
        for trace in fig.data:
            fmt = hover_formats.get(trace.name)
            if not fmt:
                continue
            trace.hovertemplate = "%{x}<br>%{fullData.name}: " + fmt + "<extra></extra>"


def apply_exec_style(
    fig: go.Figure,
    *,
    title: str,
    subtitle: str | None = None,
    source: str | None = None,
    hovermode: str | None = "x unified",
    legend_names: Mapping[str, str] | None = None,
) -> go.Figure:
    """Aplica estilo ejecutivo uniforme a la figura y devuelve la misma figura."""
    if legend_names:
        for trace in fig.data:
            if trace.name in legend_names:
                trace.name = legend_names[trace.name]

    subtitle_text = f"<br><span style='font-size:14px; color:{EXEC_THEME['accent']}; font-weight:400'>{subtitle}</span>" if subtitle else ""
    source_text = (
        f"<br><span style='font-size:12px; color:{EXEC_THEME['accent']}; font-weight:400'>Fuente: {source}</span>"
        if source
        else ""
    )

    fig.update_layout(
        template="plotly_white",
        title={
            "text": f"<b>{title}</b>{subtitle_text}{source_text}",
            "font": {"size": 20, "family": EXEC_THEME["font_family"], "color": EXEC_THEME["font_color"]},
            "x": 0.0,
            "xanchor": "left",
        },
        font={"family": EXEC_THEME["font_family"], "color": EXEC_THEME["font_color"]},
        paper_bgcolor=EXEC_THEME["bg_color"],
        plot_bgcolor=EXEC_THEME["bg_color"],
        hovermode=hovermode,
        hoverlabel={
            "bgcolor": EXEC_THEME["hover_bg"],
            "font_size": 13,
            "font_family": EXEC_THEME["font_family"],
        },
        locale="es",
        legend={
            "orientation": "h",
            "yanchor": "bottom",
            "y": -0.2,
            "xanchor": "center",
            "x": 0.5,
            "title": None,
            "font": {"size": 12},
        },
        margin={"l": 60, "r": 40, "t": 90, "b": 60},
    )

    fig.update_xaxes(showline=True, linecolor=EXEC_THEME["grid_color"], tickfont={"size": 12})
    fig.update_yaxes(showline=True, linecolor=EXEC_THEME["grid_color"], tickfont={"size": 12})

    return fig


def apply_thin_lines(fig: go.Figure, width: float = 2.2, dash: str | None = None, opacity: float | None = None) -> None:
    """Uniforma el grosor de líneas para reducir ruido visual."""
    for trace in fig.data:
        if hasattr(trace, "line"):
            line_updates: MutableMapping[str, object] = {"width": width}
            if dash:
                line_updates["dash"] = dash
            if opacity is not None:
                trace.opacity = opacity
            trace.update(line=line_updates)


def apply_soft_markers(fig: go.Figure, size: int = 8) -> None:
    """Aplica estilo de markers suave."""
    for trace in fig.data:
        if hasattr(trace, "marker"):
            trace.update(marker={"size": size, "line": {"width": 0}, "opacity": 0.8})


def apply_unified_hover(fig: go.Figure, fmt: str, units: str | None = None) -> None:
    """Sobrescribe hovertemplate con formato uniforme."""
    unit_txt = f" {units}" if units else ""
    for trace in fig.data:
        trace.hovertemplate = "%{x}<br>%{fullData.name}: %{y" + fmt + "}" + unit_txt + "<extra></extra>"
