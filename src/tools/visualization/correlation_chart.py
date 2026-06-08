"""
src/tools/visualization/correlation_chart.py
─────────────────────────────────────────────
Matplotlib visualization generators for anomaly correlation mapping.
"""

from __future__ import annotations

import base64
import io
import logging
from collections import defaultdict
from datetime import date
from typing import Any, List, Optional

import numpy as np
import pandas as pd

from src.ml.correlation import CorrelationFinding, EventType

log = logging.getLogger(__name__)

# Set backend once at module level — calling matplotlib.use("Agg") inside
# functions rebuilds the font manager on every call (~200ms + memory spike).
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates


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



def _event_style(event_type: EventType):
    """Return (color, marker, legend_label) for an event type."""
    if event_type == EventType.COMPANY_FILING:
        return "#ffd700", "o", "Company Filing"
    elif event_type == EventType.NEWS_ANNOUNCEMENT:
        return "#44ddaa", "D", "News Announcement"
    elif event_type in (EventType.MACRO_RATE_DECISION, EventType.MACRO_GEOPOLITICAL):
        return "#cc88ff", "s", "Macro Policy / Event"
    else:
        return "#ff7744", "^", "Macro Shock"


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

        dates = df_ohlcv["trade_date"].values
        closes = df_ohlcv["close"].values
        ax.plot(dates, closes, color="#4da6ff", linewidth=1.2, label=f"{symbol} Price")

        plotted_categories = set()
        anomaly_label_added = False

        for f in findings:
            anom_dt = pd.to_datetime(f.anomaly_date)
            ev_dt = pd.to_datetime(f.event.trade_date)

            anom_close = df_ohlcv.loc[df_ohlcv["trade_date"].dt.date == f.anomaly_date, "close"]
            anom_y = float(anom_close.iloc[0]) if not anom_close.empty else closes.mean()

            color, _, label = _event_style(f.event.event_type)

            # Shaded correlation band — ensure minimum 1-day width so it's always visible
            left_dt = min(anom_dt, ev_dt)
            right_dt = max(anom_dt, ev_dt)
            if left_dt == right_dt:
                right_dt = left_dt + pd.Timedelta(days=1)
            ax.axvspan(left_dt, right_dt, color=color, alpha=0.15, zorder=1)

            # Vertical dashed line for the event date
            lbl_v = label if label not in plotted_categories else None
            if lbl_v:
                plotted_categories.add(label)
            ax.axvline(ev_dt, color=color, linestyle="--", linewidth=0.8, alpha=0.6, label=lbl_v)

            # Anomaly marker — plain text label avoids missing-glyph warnings
            anom_label = "Correlated Anomaly" if not anomaly_label_added else None
            if anom_label:
                anomaly_label_added = True
            ax.scatter(anom_dt, anom_y, color="#ff4444", s=40, zorder=3,
                       edgecolors="#ffffff", linewidths=0.5, label=anom_label)

        ax.set_title(f"{symbol} — Event Correlation Timeline (Mapped Anomalies & Triggers)", fontsize=10, pad=8)
        ax.set_ylabel("Price (INR)", fontsize=8)
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%b '%y"))
        ax.xaxis.set_major_locator(mdates.MonthLocator(interval=1))
        plt.setp(ax.get_xticklabels(), rotation=30, ha="right", fontsize=7)

        ax.legend(fontsize=7, facecolor="#1a1a2e", edgecolor="#333355",
                  labelcolor="#cccccc", loc="upper left")

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
    Renders a 2D grid plotting Correlation Score vs. Lead-Lag Days.
    Negative = anomaly before event (potential leak). Positive = after (market reaction).
    Labels are collision-aware: stacked vertically within each x-column with leader lines.
    """
    if not findings:
        return None

    try:

        BG = "#0d0d1a"
        fig, ax = plt.subplots(figsize=(10, 5), facecolor=BG)
        _style_axes(ax, BG)

        ax.axvline(0, color="#ffffff", linestyle=":", linewidth=1.0, alpha=0.5)
        ax.text(-0.3, 97, "Pre-Event (Potential Leak)",
                color="#ffaaaa", fontsize=7, ha="right", style="italic")
        ax.text(0.3, 97, "Post-Event (Market Reaction)",
                color="#aaffaa", fontsize=7, ha="left", style="italic")

        # --- Plot scatter markers ---
        plotted_types = set()
        for f in findings:
            color, marker, label = _event_style(f.event.event_type)
            lbl = label if label not in plotted_types else None
            if lbl:
                plotted_types.add(label)
            ax.scatter(f.lead_lag_days, f.correlation_score,
                       color=color, marker=marker, s=55,
                       edgecolors="#ffffff", linewidths=0.5, zorder=4, label=lbl)

        # --- Collision-aware label placement ---
        # Group findings by x-column (lead_lag_days). Within each column sort by
        # score descending, then assign y-positions with a fixed vertical step so
        # labels don't overlap. Draw a thin leader line from point to label.
        x_groups: dict[int, list[CorrelationFinding]] = defaultdict(list)
        for f in findings:
            x_groups[f.lead_lag_days].append(f)

        LABEL_STEP = 9        # minimum vertical spacing between labels (score units)
        LABEL_X_OFFSET = 0.18 # horizontal nudge

        for x_val, group in x_groups.items():
            group_sorted = sorted(group, key=lambda f: -f.correlation_score)

            # Assign label y-positions starting from each point's score, stepping up
            assigned_y: list[float] = []
            for rank, f in enumerate(group_sorted):
                base_y = f.correlation_score
                # push up until clear of all previously assigned labels
                candidate = base_y + 2
                for prev_y in assigned_y:
                    if abs(candidate - prev_y) < LABEL_STEP:
                        candidate = prev_y + LABEL_STEP
                assigned_y.append(candidate)

            for (f, label_y) in zip(group_sorted, assigned_y):
                event_name = f.event.label
                if len(event_name) > 20:
                    event_name = event_name[:17] + "..."

                label_text = f"{event_name}\n({f.anomaly_date})"
                color, _, _ = _event_style(f.event.event_type)

                # Leader line from scatter point to label anchor
                if abs(label_y - f.correlation_score) > 3:
                    ax.annotate(
                        label_text,
                        xy=(f.lead_lag_days, f.correlation_score),
                        xytext=(f.lead_lag_days + LABEL_X_OFFSET, label_y),
                        color="#cccccc",
                        fontsize=6,
                        ha="left",
                        va="center",
                        arrowprops=dict(
                            arrowstyle="-",
                            color="#555577",
                            lw=0.6,
                        ),
                        zorder=5,
                    )
                else:
                    ax.text(f.lead_lag_days + LABEL_X_OFFSET, label_y,
                            label_text, color="#cccccc", fontsize=6,
                            ha="left", va="center", zorder=5)

        # --- Axis range: tight around data, not wasted whitespace ---
        x_vals = [f.lead_lag_days for f in findings]
        x_min, x_max = min(x_vals), max(x_vals)
        ax.set_xlim(x_min - 1.5, x_max + 4.5)   # extra right margin for labels
        ax.set_ylim(0, 110)

        ax.set_title(f"{symbol} — Anomaly Lead-Lag Grid (Feature Space)", fontsize=10, pad=8)
        ax.set_xlabel("Lead / Lag (Trading Days from Event)", fontsize=8)
        ax.set_ylabel("Correlation / Leak Score (0-100)", fontsize=8)

        ax.legend(fontsize=7, facecolor="#1a1a2e", edgecolor="#333355",
                  labelcolor="#cccccc", loc="lower right")

        plt.tight_layout(pad=0.5)
        png = _df_to_png(fig)
        plt.close(fig)
        return png

    except Exception as exc:
        log.warning("Failed to render correlation lead-lag grid for %s: %s", symbol, exc, exc_info=True)
        return None
