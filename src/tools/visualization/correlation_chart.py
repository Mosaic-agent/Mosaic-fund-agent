"""
src/tools/visualization/correlation_chart.py
─────────────────────────────────────────────
Matplotlib visualization generators for anomaly correlation mapping.
"""

from __future__ import annotations

import base64
import io
import logging
from datetime import date
from typing import Any, List, Optional

import matplotlib
matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from src.ml.correlation import CorrelationFinding, EventType

log = logging.getLogger(__name__)


def _df_to_png(fig) -> str:
    """Save a matplotlib Figure to a base64-encoded PNG string."""
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=150, bbox_inches="tight",
                facecolor=fig.get_facecolor())
    buf.seek(0)
    return base64.b64encode(buf.read()).decode()


def _style_axes(ax, bg: str = "#0d0d1a") -> None:
    """Apply consistent dark-theme styling to a matplotlib Axes."""
    ax.set_facecolor(bg)
    ax.tick_params(colors="#cccccc", labelsize=8)
    for spine in ax.spines.values():
        spine.set_edgecolor("#333355")
    ax.yaxis.label.set_color("#cccccc")
    ax.xaxis.label.set_color("#cccccc")
    ax.title.set_color("#e0e0ff")
    ax.grid(color="#333355", linewidth=0.4, alpha=0.6)


def render_correlation_timeline_png(
    symbol: str,
    findings: List[CorrelationFinding],
    df_ohlcv: pd.DataFrame,
) -> Optional[str]:
    """
    Renders the daily price timeline with vertical event lines and shaded connection bands.
    """
    if df_ohlcv.empty:
        return None

    try:
        df_ohlcv = df_ohlcv.copy()
        df_ohlcv["trade_date"] = pd.to_datetime(df_ohlcv["trade_date"])
        df_ohlcv = df_ohlcv.sort_values("trade_date").reset_index(drop=True)

        BG = "#0d0d1a"
        fig, ax = plt.subplots(figsize=(10, 4.5), facecolor=BG)
        _style_axes(ax, BG)

        # Plot price history
        dates = df_ohlcv["trade_date"].values
        closes = df_ohlcv["close"].values
        ax.plot(dates, closes, color="#4da6ff", linewidth=1.2, label=f"{symbol} Price")

        # Keep track of plotted labels to avoid duplicate legend entries
        plotted_categories = set()

        # Group findings by event type to color/style them
        for f in findings:
            anom_dt = pd.to_datetime(f.anomaly_date)
            ev_dt = pd.to_datetime(f.event.trade_date)

            # Get matching close prices to position markers vertically
            anom_close = df_ohlcv.loc[df_ohlcv["trade_date"].dt.date == f.anomaly_date, "close"]
            anom_y = float(anom_close.iloc[0]) if not anom_close.empty else closes.mean()

            # Determine colors/markers based on event type
            if f.event.event_type == EventType.COMPANY_FILING:
                color = "#ffd700"  # Gold
                label = "Company Event (Filing)"
            elif f.event.event_type in (EventType.MACRO_RATE_DECISION, EventType.MACRO_GEOPOLITICAL):
                color = "#cc88ff"  # Purple/Violet
                label = "Macro Policy / Event"
            else:
                color = "#ff7744"  # Orange/Coral
                label = "Macro Shock"

            # Draw shaded correlation band
            left_dt, right_dt = min(anom_dt, ev_dt), max(anom_dt, ev_dt)
            ax.axvspan(left_dt, right_dt, color=color, alpha=0.12, zorder=1)

            # Draw vertical dashed line for the event ex-date
            lbl_v = label if label not in plotted_categories else None
            if lbl_v:
                plotted_categories.add(label)
            ax.axvline(ev_dt, color=color, linestyle="--", linewidth=0.8, alpha=0.6, label=lbl_v)

            # Draw anomaly marker
            anom_label = "Correlated Anomaly (🔴)" if "anomaly" not in plotted_categories else None
            if anom_label:
                plotted_categories.add("anomaly")
            ax.scatter(anom_dt, anom_y, color="#ff4444", s=35, zorder=3, edgecolors="#ffffff", linewidths=0.5, label=anom_label)

        # Labels & Styling
        ax.set_title(f"{symbol} — Event Correlation Timeline (Mapped Anomalies & Triggers)", fontsize=10, pad=8)
        ax.set_ylabel("Price (INR)", fontsize=8)
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%b '%y"))
        ax.xaxis.set_major_locator(mdates.MonthLocator(interval=1))
        plt.setp(ax.get_xticklabels(), rotation=30, ha="right", fontsize=7)

        # Place legend nicely
        ax.legend(fontsize=7, facecolor="#1a1a2e", edgecolor="#333355", labelcolor="#cccccc", loc="upper left")

        plt.tight_layout(pad=0.5)
        png = _df_to_png(fig)
        plt.close(fig)
        return png

    except Exception as exc:
        log.warning("Failed to render correlation timeline chart for %s: %s", symbol, exc, exc_info=True)
        return None


def render_lead_lag_grid_png(
    symbol: str,
    findings: List[CorrelationFinding],
) -> Optional[str]:
    """
    Renders a 2D grid plotting Correlation Score vs. Lead-Lag Days relative to the event.
    Negative lead-lag days = anomaly happened before event (potential leak).
    Positive lead-lag days = anomaly happened after event (market reaction).
    """
    if not findings:
        return None

    try:
        BG = "#0d0d1a"
        fig, ax = plt.subplots(figsize=(8, 4), facecolor=BG)
        _style_axes(ax, BG)

        # Draw vertical line at X = 0 (the event date)
        ax.axvline(0, color="#ffffff", linestyle=":", linewidth=1.0, alpha=0.5)
        ax.text(-0.3, 95, "Pre-Event (Potential Leak)", color="#ffaaaa", fontsize=7, ha="right", style="italic")
        ax.text(0.3, 95, "Post-Event (Market Reaction)", color="#aaffaa", fontsize=7, ha="left", style="italic")

        plotted_types = set()

        for f in findings:
            x = f.lead_lag_days
            y = f.correlation_score

            if f.event.event_type == EventType.COMPANY_FILING:
                color = "#ffd700"  # Gold
                marker = "o"
                label = "Company Filing"
            elif f.event.event_type in (EventType.MACRO_RATE_DECISION, EventType.MACRO_GEOPOLITICAL):
                color = "#cc88ff"  # Purple
                marker = "s"
                label = "Macro Policy / Event"
            else:
                color = "#ff7744"  # Orange
                marker = "^"
                label = "Macro Shock"

            lbl = label if label not in plotted_types else None
            if lbl:
                plotted_types.add(label)

            ax.scatter(x, y, color=color, marker=marker, s=50, edgecolors="#ffffff", linewidths=0.5, zorder=3, label=lbl)

            # Label individual points with event label
            event_name = f.event.label
            if len(event_name) > 18:
                event_name = event_name[:15] + "..."
            ax.text(x + 0.15, y - 1, f"{event_name}\n({f.anomaly_date})", color="#cccccc", fontsize=6, zorder=4)

        # Labels & Styling
        ax.set_title(f"{symbol} — Anomaly Lead-Lag Grid (Feature Space)", fontsize=10, pad=8)
        ax.set_xlabel("Lead / Lag (Trading Days from Event)", fontsize=8)
        ax.set_ylabel("Correlation / Leak Score (0-100)", fontsize=8)
        
        # Grid range config
        ax.set_xlim(-7, 7)
        ax.set_ylim(0, 105)

        ax.legend(fontsize=7, facecolor="#1a1a2e", edgecolor="#333355", labelcolor="#cccccc", loc="lower right")

        plt.tight_layout(pad=0.5)
        png = _df_to_png(fig)
        plt.close(fig)
        return png

    except Exception as exc:
        log.warning("Failed to render correlation lead-lag grid for %s: %s", symbol, exc, exc_info=True)
        return None
