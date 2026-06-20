"""
src/utils/matplotlib_theme.py
──────────────────────────────
Shared matplotlib dark-theme helpers.

Previously duplicated as ``_style_axes`` + ``_df_to_png`` in:
  • src/tools/report_publisher.py
  • src/tools/visualization/correlation_chart.py
  • src/tools/chart_tools.py  (partial)

Usage
-----
    from src.utils.matplotlib_theme import apply_dark_theme, fig_to_png_b64

    fig, ax = plt.subplots()
    apply_dark_theme(ax)
    b64 = fig_to_png_b64(fig)
"""

from __future__ import annotations

import base64
import io

# Default dark-background colour used across all Mosaic charts.
DARK_BG        = "#1a1a2e"
DARK_BG_DEEP   = "#0d0d1a"   # slightly darker variant used in correlation charts
TICK_COLOR     = "#cccccc"
SPINE_COLOR    = "#333355"
LABEL_COLOR    = "#cccccc"
TITLE_COLOR    = "#e0e0ff"
GRID_COLOR     = "#333355"
DEFAULT_DPI    = 150


def apply_dark_theme(ax, bg: str = DARK_BG) -> None:
    """
    Apply consistent dark-theme styling to a matplotlib ``Axes`` object.

    Args:
        ax: ``matplotlib.axes.Axes`` to style.
        bg: Background hex colour.  Defaults to :data:`DARK_BG`.
    """
    ax.set_facecolor(bg)
    ax.tick_params(colors=TICK_COLOR, labelsize=8)
    for spine in ax.spines.values():
        spine.set_edgecolor(SPINE_COLOR)
    ax.yaxis.label.set_color(LABEL_COLOR)
    ax.xaxis.label.set_color(LABEL_COLOR)
    ax.title.set_color(TITLE_COLOR)
    ax.grid(color=GRID_COLOR, linewidth=0.4, alpha=0.6)


def fig_to_png_b64(fig, dpi: int = DEFAULT_DPI) -> str:
    """
    Serialize a matplotlib ``Figure`` to a base64-encoded PNG string.

    Args:
        fig: ``matplotlib.figure.Figure`` to serialize.
        dpi: Output resolution.  Defaults to :data:`DEFAULT_DPI`.

    Returns:
        Base64-encoded PNG string (UTF-8).
    """
    buf = io.BytesIO()
    fig.savefig(
        buf,
        format="png",
        dpi=dpi,
        bbox_inches="tight",
        facecolor=fig.get_facecolor(),
    )
    buf.seek(0)
    return base64.b64encode(buf.read()).decode()
