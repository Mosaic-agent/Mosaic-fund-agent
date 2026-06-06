"""
src/tools/report_publisher.py
─────────────────────────────
PDF research report publisher.

Pipeline
────────
  1. Markdown research note (from deep-dive agent)
  2. python-markdown → body HTML
  3. Jinja2 → styled HTML page  (professional research-report template)
  4. matplotlib → chart images   (price + MACD + GARCH vol as base64 PNGs)
  5. weasyprint → PDF file       (written to output/reports/<symbol>_<date>.pdf)

Public API
──────────
    publish_research_pdf(symbol, report_markdown, filename="")
        @tool — LangChain tool for use by the equity research agent

    generate_pdf_bytes(symbol, report_markdown)
        → bytes — for programmatic use (tests, API endpoints)
"""

from __future__ import annotations

import base64
import io
import logging
import os
from datetime import datetime
from pathlib import Path

log = logging.getLogger(__name__)

# ── Output directory ─────────────────────────────────────────────────────────

_OUTPUT_DIR = Path("output") / "reports"


# ── Chart image generators (matplotlib) ──────────────────────────────────────

def _df_to_png(fig) -> str:
    """Save a matplotlib Figure to a base64-encoded PNG string."""
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=150, bbox_inches="tight",
                facecolor=fig.get_facecolor())
    buf.seek(0)
    return base64.b64encode(buf.read()).decode()


def _style_axes(ax, bg: str = "#1a1a2e") -> None:
    """Apply consistent dark-theme styling to a matplotlib Axes."""
    ax.set_facecolor(bg)
    ax.tick_params(colors="#cccccc", labelsize=8)
    for spine in ax.spines.values():
        spine.set_edgecolor("#333355")
    ax.yaxis.label.set_color("#cccccc")
    ax.xaxis.label.set_color("#cccccc")
    ax.title.set_color("#e0e0ff")
    ax.grid(color="#333355", linewidth=0.4, alpha=0.6)


def render_price_chart_png(
    symbol: str,
    days: int = 365,
    category: str = "",
    df: pd.DataFrame | None = None,
) -> str | None:
    """
    Render a price chart as a base64 PNG.

    Shows: close price line, 50-day EMA, GARCH ±1σ band (if available),
    🔴 anomaly markers, 🏦 corporate action markers.

    Returns base64 PNG string, or None if data unavailable.
    """
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import matplotlib.dates as mdates
        import pandas as pd
        import numpy as np

        if df is None or df.empty:
            from src.db.pool import query_df
            params: dict = {"sym": symbol.upper()}
            cat_clause = "AND category = {cat:String}" if category else ""
            if category:
                params["cat"] = category

            df = query_df(
                f"""
                SELECT trade_date,
                       toFloat64(argMax(open,   imported_at)) AS open,
                       toFloat64(argMax(high,   imported_at)) AS high,
                       toFloat64(argMax(low,    imported_at)) AS low,
                       toFloat64(argMax(close,  imported_at)) AS close,
                       toFloat64(argMax(volume, imported_at)) AS volume
                FROM market_data.daily_prices FINAL
                WHERE symbol = {{sym:String}} {cat_clause}
                GROUP BY trade_date ORDER BY trade_date ASC
                """,
                parameters=params,
            )
        else:
            df = df.copy()

        if df.empty:
            return None

        df["trade_date"] = pd.to_datetime(df["trade_date"])
        df = df.sort_values("trade_date").reset_index(drop=True)
        cutoff = df["trade_date"].max() - pd.Timedelta(days=days)
        df = df[df["trade_date"] >= cutoff].copy()
        if len(df) < 5:
            return None

        df["ema50"] = df["close"].ewm(span=50, adjust=False).mean()

        # Get anomaly and corp-action dates
        anomaly_dates: set = set()
        corp_dates: set = set()
        try:
            from src.tools.chart_tools import _composite_anomaly_dates
            result = _composite_anomaly_dates(symbol, category)
            if result:
                anomaly_dates, corp_dates = result
        except Exception:
            pass

        BG = "#0d0d1a"
        fig, ax = plt.subplots(figsize=(10, 4), facecolor=BG)
        _style_axes(ax, BG)

        dates = df["trade_date"].values
        closes = df["close"].values

        ax.plot(dates, closes, color="#4da6ff", linewidth=1.0, label=symbol)
        ax.plot(dates, df["ema50"].values, color="#ffaa44", linewidth=0.8,
                linestyle="--", alpha=0.7, label="EMA50")

        # Anomaly markers
        anom_mask = df["trade_date"].dt.normalize().isin(anomaly_dates)
        if anom_mask.any():
            ax.scatter(
                df.loc[anom_mask, "trade_date"],
                df.loc[anom_mask, "close"],
                color="#ff4444", s=40, zorder=5, label="Anomaly", marker="o",
            )
        corp_mask = df["trade_date"].dt.normalize().isin(corp_dates)
        if corp_mask.any():
            ax.scatter(
                df.loc[corp_mask, "trade_date"],
                df.loc[corp_mask, "close"],
                color="#ffd700", s=45, zorder=5, label="Corp Action", marker="D",
            )

        ax.xaxis.set_major_formatter(mdates.DateFormatter("%b '%y"))
        ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
        plt.setp(ax.get_xticklabels(), rotation=30, ha="right", fontsize=7)

        latest = float(closes[-1])
        chg = (closes[-1] / closes[0] - 1) * 100 if len(closes) >= 2 else 0.0
        ax.set_title(f"{symbol} — {days}d price  |  ₹{latest:.2f}  |  {chg:+.1f}%",
                     fontsize=10, pad=6)
        ax.set_ylabel("Price (₹)", fontsize=8)
        ax.legend(fontsize=7, facecolor="#1a1a2e", edgecolor="#333355",
                  labelcolor="#cccccc", loc="upper left")
        plt.tight_layout(pad=0.5)
        png = _df_to_png(fig)
        plt.close(fig)
        return png

    except Exception as exc:
        log.warning("Price chart PNG failed for %s: %s", symbol, exc)
        return None


def render_macd_chart_png(
    symbol: str,
    days: int = 180,
    category: str = "",
    df: pd.DataFrame | None = None,
) -> str | None:
    """
    Render a MACD(12,26,9) chart as a base64 PNG.

    Shows: MACD line, signal line, histogram coloured by direction.
    Returns base64 PNG string, or None if data unavailable.
    """
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import matplotlib.dates as mdates
        import pandas as pd
        import numpy as np

        if df is None or df.empty:
            from src.db.pool import query_df
            params: dict = {"sym": symbol.upper()}
            cat_clause = "AND category = {cat:String}" if category else ""
            if category:
                params["cat"] = category

            # Fetch extra history for EMA warm-up (MACD needs ~35 extra bars)
            df = query_df(
                f"""
                SELECT trade_date,
                       toFloat64(argMax(close, imported_at)) AS close
                FROM market_data.daily_prices FINAL
                WHERE symbol = {{sym:String}} {cat_clause}
                GROUP BY trade_date ORDER BY trade_date ASC
                """,
                parameters=params,
            )
        else:
            df = df.copy()

        if df.empty or len(df) < 30:
            return None

        df["trade_date"] = pd.to_datetime(df["trade_date"])
        df = df.sort_values("trade_date").reset_index(drop=True)

        df["ema12"] = df["close"].ewm(span=12, adjust=False).mean()
        df["ema26"] = df["close"].ewm(span=26, adjust=False).mean()
        df["macd"]  = df["ema12"] - df["ema26"]
        df["signal"] = df["macd"].ewm(span=9, adjust=False).mean()
        df["hist"]   = df["macd"] - df["signal"]

        # Trim to display window
        cutoff = df["trade_date"].max() - pd.Timedelta(days=days)
        df = df[df["trade_date"] >= cutoff].copy()
        if len(df) < 5:
            return None

        BG = "#0d0d1a"
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 5), facecolor=BG,
                                        gridspec_kw={"height_ratios": [2, 1]})
        for ax in (ax1, ax2):
            _style_axes(ax, BG)

        # Top: close price
        ax1.plot(df["trade_date"], df["close"], color="#4da6ff", linewidth=1.0)
        ax1.set_title(f"{symbol} — MACD(12,26,9)  |  {days}d", fontsize=10, pad=6)
        ax1.set_ylabel("Price (₹)", fontsize=8)
        ax1.tick_params(labelbottom=False)

        # Bottom: MACD
        ax2.plot(df["trade_date"], df["macd"],   color="#00ccff", linewidth=1.0, label="MACD")
        ax2.plot(df["trade_date"], df["signal"], color="#ff8800", linewidth=0.9,
                 linestyle="--", label="Signal")
        colours = ["#22cc55" if v >= 0 else "#ff4444" for v in df["hist"]]
        ax2.bar(df["trade_date"], df["hist"], color=colours, alpha=0.6, width=1.2)
        ax2.axhline(0, color="#555566", linewidth=0.6)
        ax2.set_ylabel("MACD", fontsize=8)
        ax2.legend(fontsize=7, facecolor="#1a1a2e", edgecolor="#333355",
                   labelcolor="#cccccc", loc="upper left")

        ax2.xaxis.set_major_formatter(mdates.DateFormatter("%b '%y"))
        ax2.xaxis.set_major_locator(mdates.MonthLocator(interval=1))
        plt.setp(ax2.get_xticklabels(), rotation=30, ha="right", fontsize=7)

        plt.tight_layout(pad=0.4)
        png = _df_to_png(fig)
        plt.close(fig)
        return png

    except Exception as exc:
        log.warning("MACD chart PNG failed for %s: %s", symbol, exc)
        return None


def render_garch_vol_png(
    symbol: str,
    days: int = 180,
    df: pd.DataFrame | None = None,
) -> str | None:
    """
    Render a GARCH annualised volatility chart as a base64 PNG.
    Reads from weight_checkpoints (populated by the GOLDBEES pipeline).
    Falls back to computing from price returns if pipeline data absent.
    """
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import matplotlib.dates as mdates
        import pandas as pd
        import numpy as np

        df_vol = pd.DataFrame()
        if df is not None and not df.empty and "garch_vol" in df.columns:
            df_vol = df[["trade_date", "garch_vol"]].dropna().copy()
        else:
            from src.db.pool import query_df
            df_vol = query_df(
                "SELECT as_of AS trade_date, garch_vol_pct AS garch_vol "
                "FROM market_data.weight_checkpoints FINAL "
                "WHERE symbol = {sym:String} AND garch_vol_pct IS NOT NULL "
                "ORDER BY as_of ASC",
                parameters={"sym": symbol.upper()},
            )
            # If still empty, try computing it on the fly if df is provided
            if df_vol.empty and df is not None and not df.empty:
                from src.ml.anomaly import run_composite_anomaly
                df_res, _, _ = run_composite_anomaly(df)
                df_vol = df_res[["trade_date", "garch_vol"]].dropna().copy()

        if df_vol.empty:
            return None

        df_vol["trade_date"] = pd.to_datetime(df_vol["trade_date"])
        cutoff = df_vol["trade_date"].max() - pd.Timedelta(days=days)
        df_vol = df_vol[df_vol["trade_date"] >= cutoff].copy()
        if len(df_vol) < 3:
            return None

        BG = "#0d0d1a"
        fig, ax = plt.subplots(figsize=(10, 3), facecolor=BG)
        _style_axes(ax, BG)

        ax.fill_between(df_vol["trade_date"], df_vol["garch_vol"], alpha=0.25,
                        color="#aa44ff")
        ax.plot(df_vol["trade_date"], df_vol["garch_vol"], color="#cc88ff", linewidth=1.0)

        latest_vol = float(df_vol["garch_vol"].iloc[-1])
        ax.set_title(
            f"{symbol} — GARCH Annualised Volatility  |  Latest: {latest_vol:.1f}%",
            fontsize=10, pad=6,
        )
        ax.set_ylabel("Vol % (ann.)", fontsize=8)
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%b '%y"))
        ax.xaxis.set_major_locator(mdates.MonthLocator(interval=1))
        plt.setp(ax.get_xticklabels(), rotation=30, ha="right", fontsize=7)
        plt.tight_layout(pad=0.4)
        png = _df_to_png(fig)
        plt.close(fig)
        return png

    except Exception as exc:
        log.warning("GARCH vol PNG failed for %s: %s", symbol, exc)
        return None


def render_anomaly_clusters_png(
    symbol: str,
    days: int = 365,
    category: str = "",
    df: pd.DataFrame | None = None,
) -> str | None:
    """
    Render a GARCH composite anomaly regime cluster plot as a base64 PNG.
    Plots Daily Return vs. Intraday Range and colors by regime classification.
    """
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import pandas as pd
        import numpy as np
        from src.ml.anomaly import build_features, run_composite_anomaly

        if df is None or df.empty:
            from src.db.pool import query_df
            params: dict = {"sym": symbol.upper()}
            cat_clause = "AND category = {cat:String}" if category else ""
            if category:
                params["cat"] = category

            df = query_df(
                f"""
                SELECT trade_date,
                       toFloat64(argMax(open,   imported_at)) AS open,
                       toFloat64(argMax(high,   imported_at)) AS high,
                       toFloat64(argMax(low,    imported_at)) AS low,
                       toFloat64(argMax(close,  imported_at)) AS close,
                       toFloat64(argMax(volume, imported_at)) AS volume
                FROM market_data.daily_prices FINAL
                WHERE symbol = {{sym:String}} {cat_clause}
                GROUP BY trade_date ORDER BY trade_date ASC
                """,
                parameters=params,
            )
        else:
            df = df.copy()

        if df.empty or len(df) < 60:
            return None

        df["trade_date"] = pd.to_datetime(df["trade_date"])
        df = build_features(df)

        df_corp = None
        try:
            _ca = query_df(
                "SELECT ex_date, action_type, ratio, purpose "
                "FROM market_data.corporate_actions FINAL "
                "WHERE symbol = {sym:String}",
                parameters={"sym": symbol.upper()},
            )
            if not _ca.empty:
                _ca["ex_date"] = pd.to_datetime(_ca["ex_date"])
                df_corp = _ca
        except Exception:
            pass

        df_result, _, _ = run_composite_anomaly(df, df_corp_actions=df_corp)

        # Trim to display window
        cutoff = df_result["trade_date"].max() - pd.Timedelta(days=days)
        df_result = df_result[df_result["trade_date"] >= cutoff].copy()
        if df_result.empty:
            return None

        BG = "#0d0d1a"
        fig, ax = plt.subplots(figsize=(10, 4.5), facecolor=BG)
        _style_axes(ax, BG)

        # Style specifications matching the dark theme
        regime_colors = {
            '✅ Normal': '#444466',
            '🔥 Volatile Breakout': '#ff4444',
            '⚡ Flash Crash / Black Swan (EXIT)': '#cc66ff',
            '📈 Strong Trend (HODL)': '#44ff44',
            '🧨 Blow-off Top (Weak)': '#ffaa44',
            '🏦 Corporate Action': '#ffd700',
            '🔀 Regime Shift (Change Point)': '#00ffff'
        }

        # Plot each regime
        for regime, color in regime_colors.items():
            sub = df_result[df_result['regime'] == regime]
            if not sub.empty:
                label_clean = (
                    regime.split('(')[0]
                    .replace('✅', '')
                    .replace('🔥', '')
                    .replace('⚡', '')
                    .replace('📈', '')
                    .replace('🧨', '')
                    .replace('🏦', '')
                    .replace('🔀', '')
                    .strip()
                )
                ax.scatter(
                    sub['daily_return'],
                    sub['range_pct'],
                    color=color,
                    s=25 if regime == '✅ Normal' else 50,
                    alpha=0.4 if regime == '✅ Normal' else 0.85,
                    edgecolors='none' if regime == '✅ Normal' else '#ffffff',
                    linewidths=0.5,
                    label=label_clean,
                    zorder=2 if regime == '✅ Normal' else 3
                )

        ax.axvline(0, color="#333355", linestyle="--", linewidth=0.6)
        ax.set_xlabel("Daily Return (%)", fontsize=8)
        ax.set_ylabel("Intraday Range (%)", fontsize=8)
        ax.set_title(f"{symbol} — Anomaly Regime Clusters (Return vs. Range)", fontsize=10, pad=6)
        ax.legend(fontsize=7, facecolor="#1a1a2e", edgecolor="#333355",
                  labelcolor="#cccccc", loc="upper right")
        plt.tight_layout(pad=0.5)
        png = _df_to_png(fig)
        plt.close(fig)
        return png
    except Exception as exc:
        log.warning("Anomaly clusters PNG failed for %s: %s", symbol, exc)
        return None


# ── HTML template ──────────────────────────────────────────────────────────

_HTML_TEMPLATE = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>{{ title }}</title>
<style>
  @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600;700&family=JetBrains+Mono:wght@400;600&display=swap');

  :root {
    --bg:       #ffffff;
    --surface:  #f8f9fc;
    --border:   #dee2ee;
    --text:     #1a1d2e;
    --muted:    #6b7280;
    --accent:   #2563eb;
    --accent2:  #7c3aed;
    --positive: #16a34a;
    --negative: #dc2626;
    --warn:     #d97706;
  }

  * { box-sizing: border-box; margin: 0; padding: 0; }

  body {
    font-family: 'Inter', sans-serif;
    background: var(--bg);
    color: var(--text);
    font-size: 10pt;
    line-height: 1.55;
  }

  /* ── Cover header ── */
  .cover {
    background: linear-gradient(135deg, #1e3a8a 0%, #312e81 60%, #1e1b4b 100%);
    color: #ffffff;
    padding: 32px 40px 24px;
    border-bottom: 4px solid #6366f1;
  }
  .cover .platform { font-size: 9pt; opacity: 0.65; letter-spacing: 1px; text-transform: uppercase; }
  .cover h1 { font-size: 20pt; font-weight: 700; margin: 6px 0 4px; }
  .cover .subtitle { font-size: 10pt; opacity: 0.8; }
  .cover .meta { margin-top: 14px; font-size: 8pt; opacity: 0.6; }

  /* ── Page body ── */
  .body { padding: 28px 40px 40px; }

  /* ── Section headings from Markdown ── */
  h1 { font-size: 15pt; font-weight: 700; color: var(--accent2);
       border-bottom: 2px solid var(--accent2); padding-bottom: 4px;
       margin: 24px 0 12px; }
  h2 { font-size: 12pt; font-weight: 600; color: var(--accent);
       margin: 20px 0 8px; }
  h3 { font-size: 10.5pt; font-weight: 600; color: var(--text);
       margin: 16px 0 6px; }
  h4 { font-size: 10pt; font-weight: 600; color: var(--muted);
       margin: 14px 0 4px; }

  p { margin: 6px 0 10px; }

  /* ── Tables ── */
  table {
    width: 100%;
    border-collapse: collapse;
    font-size: 9pt;
    margin: 10px 0 16px;
    break-inside: avoid;
  }
  th {
    background: #1e3a8a;
    color: #ffffff;
    font-weight: 600;
    padding: 6px 10px;
    text-align: left;
    font-size: 8.5pt;
  }
  td {
    padding: 5px 10px;
    border-bottom: 1px solid var(--border);
    vertical-align: top;
  }
  tr:nth-child(even) td { background: var(--surface); }
  tr:hover td { background: #eff6ff; }

  /* ── Code / inline code ── */
  code {
    font-family: 'JetBrains Mono', monospace;
    font-size: 8.5pt;
    background: #f1f5f9;
    border: 1px solid var(--border);
    border-radius: 3px;
    padding: 1px 4px;
    color: #1e40af;
  }
  pre {
    background: #0f172a;
    color: #e2e8f0;
    font-family: 'JetBrains Mono', monospace;
    font-size: 8pt;
    padding: 12px 16px;
    border-radius: 6px;
    overflow-x: auto;
    margin: 10px 0 14px;
    break-inside: avoid;
  }
  pre code {
    background: transparent;
    border: none;
    padding: 0;
    color: inherit;
  }

  /* ── Blockquotes ── */
  blockquote {
    border-left: 3px solid var(--accent);
    padding: 6px 14px;
    background: #eff6ff;
    color: var(--muted);
    margin: 10px 0;
    font-size: 9pt;
    break-inside: avoid;
  }

  /* ── Lists ── */
  ul, ol { padding-left: 20px; margin: 6px 0 10px; }
  li { margin: 3px 0; }

  /* ── Horizontal rule ── */
  hr { border: none; border-top: 1px solid var(--border); margin: 18px 0; }

  /* ── Chart section ── */
  .charts-section {
    margin: 20px 0 28px;
  }
  .chart-block {
    margin-bottom: 20px;
    break-inside: avoid;
  }
  .chart-label {
    font-size: 8.5pt;
    font-weight: 600;
    color: var(--muted);
    text-transform: uppercase;
    letter-spacing: 0.5px;
    margin-bottom: 6px;
  }
  .chart-block img {
    width: 100%;
    border: 1px solid var(--border);
    border-radius: 6px;
  }

  /* ── Disclaimer footer ── */
  .disclaimer {
    margin-top: 32px;
    padding: 12px 16px;
    background: #fefce8;
    border: 1px solid #fde68a;
    border-radius: 4px;
    font-size: 7.5pt;
    color: var(--muted);
    break-inside: avoid;
  }

  /* ── Page breaks ── */
  .page-break { page-break-after: always; break-after: page; }

  @page {
    size: A4;
    margin: 14mm 14mm 14mm 14mm;
    @bottom-center {
      content: "{{ footer_symbol }} | Mosaic Fund Agent | Page " counter(page) " of " counter(pages);
      font-family: 'Inter', sans-serif;
      font-size: 7pt;
      color: #9ca3af;
    }
    @top-right {
      content: "{{ date_str }}";
      font-family: 'Inter', sans-serif;
      font-size: 7pt;
      color: #9ca3af;
    }
  }
</style>
</head>
<body>

<div class="cover">
  <div class="platform">Mosaic Fund Agent — {{ report_type }}</div>
  <h1>{{ headline }}</h1>
  <div class="subtitle">{{ subtitle }}</div>
  <div class="meta">Report date: {{ date_str }} &nbsp;|&nbsp; Prepared by Mosaic autonomous research agent</div>
</div>

<div class="body">

{% if charts %}
<div class="charts-section">
  {% for chart in charts %}
  <div class="chart-block">
    <div class="chart-label">{{ chart.label }}</div>
    <img src="data:image/png;base64,{{ chart.data }}" alt="{{ chart.label }}"/>
  </div>
  {% endfor %}
</div>
<div class="page-break"></div>
{% endif %}

{{ report_html }}

<div class="disclaimer">
  <strong>Disclaimer:</strong> This report is generated by an automated AI research agent for informational and educational purposes only.
  It does not constitute investment advice, a solicitation, or a recommendation to buy or sell any security.
  All data is sourced from public market feeds and may be delayed. Past performance is not indicative of future results.
  Mosaic Fund Agent and its operators accept no liability for investment decisions made on the basis of this report.
  Always conduct independent due diligence and consult a SEBI-registered investment advisor before investing.
</div>

</div><!-- /body -->
</body>
</html>
"""


# ── PDF assembler ─────────────────────────────────────────────────────────────

def generate_pdf_bytes(
    symbol: str,
    report_markdown: str,
    company_name: str = "",
    days_price: int = 365,
    days_macd: int = 180,
    days_garch: int = 180,
) -> bytes:
    """
    Convert a Markdown research report to PDF bytes.

    Parameters
    ----------
    symbol          : NSE symbol (MSUMI, RELIANCE, …)
    report_markdown : Full Markdown text of the research note
    company_name    : Optional human-readable company name for the cover
    days_price/macd/garch : Look-back windows for each chart

    Returns PDF as raw bytes.
    """
    import markdown as md
    from jinja2 import Template
    from weasyprint import HTML as WP_HTML

    # ── 1. Markdown → HTML body ──────────────────────────────────────────────
    extensions = ["tables", "fenced_code", "nl2br", "sane_lists"]
    report_html = md.markdown(report_markdown, extensions=extensions)

    # ── 2. Render charts in parallel ─────────────────────────────────────────
    from concurrent.futures import ThreadPoolExecutor
    from src.db.pool import query_df

    df = None
    try:
        df = query_df(
            """
            SELECT trade_date,
                   toFloat64(argMax(open,   imported_at)) AS open,
                   toFloat64(argMax(high,   imported_at)) AS high,
                   toFloat64(argMax(low,    imported_at)) AS low,
                   toFloat64(argMax(close,  imported_at)) AS close,
                   toFloat64(argMax(volume, imported_at)) AS volume
            FROM market_data.daily_prices FINAL
            WHERE symbol = {sym:String}
            GROUP BY trade_date ORDER BY trade_date ASC
            """,
            parameters={"sym": symbol.upper()},
        )
        if df.empty:
            df = None
    except Exception as e:
        log.warning("Pre-fetching daily prices failed for %s: %s", symbol, e)
        df = None

    charts = []
    with ThreadPoolExecutor(max_workers=4) as pool:
        f_price = pool.submit(render_price_chart_png, symbol, days_price, df=df)
        f_macd  = pool.submit(render_macd_chart_png,  symbol, days_macd, df=df)
        f_garch = pool.submit(render_garch_vol_png,   symbol, days_garch, df=df)
        f_clust = pool.submit(render_anomaly_clusters_png, symbol, days_price, df=df)

        price_png = f_price.result(timeout=60)
        macd_png  = f_macd.result(timeout=60)
        garch_png = f_garch.result(timeout=60)
        clust_png = f_clust.result(timeout=60)

    if price_png:
        charts.append({"label": f"{symbol} — 1-Year Price  |  🔴 Anomaly  |  🟡 Corp Action", "data": price_png})
    if macd_png:
        charts.append({"label": f"{symbol} — MACD(12,26,9)", "data": macd_png})
    if garch_png:
        charts.append({"label": f"{symbol} — GARCH Annualised Volatility", "data": garch_png})
    if clust_png:
        charts.append({"label": f"{symbol} — Anomaly Regime Clusters (Feature Space)", "data": clust_png})

    # ── 3. Render HTML template ───────────────────────────────────────────────
    display = company_name or symbol.upper()
    tmpl = Template(_HTML_TEMPLATE)
    html = tmpl.render(
        title=f"{display} — Equity Research Note",
        report_type="Equity Research",
        headline=display,
        subtitle=f"{symbol.upper()} · NSE · Equity Research Note",
        footer_symbol=symbol.upper(),
        date_str=datetime.now().strftime("%d %B %Y"),
        report_html=report_html,
        charts=charts,
    )

    # ── 4. weasyprint → PDF ───────────────────────────────────────────────────
    return WP_HTML(string=html).write_pdf()


# ── LangChain tool ────────────────────────────────────────────────────────────

from langchain_core.tools import tool


def _detect_symbols_in_markdown(text: str) -> list[str]:
    """
    Best-effort extraction of NSE-style symbols (2-10 uppercase letters/digits)
    from a Markdown research report.

    Strategy:
      1. Explicit `**SYMBOL**` bold patterns (most reliable — agent often bolds tickers)
      2. Known-good patterns: lines starting with ###/#### that contain an all-caps word
      3. `symbol:` / `Symbol:` key-value pairs in Markdown tables
    Deduplicates and preserves order of first occurrence.
    """
    import re
    seen: list[str] = []
    visited: set[str] = set()

    # Skip common English words that look like tickers
    _SKIP = {
        "NSE", "BSE", "ETF", "NAV", "FII", "DII", "RBI", "SEBI", "CAGR",
        "EBIT", "EBITDA", "ROE", "ROCE", "EPS", "P/E", "P/B", "FCF",
        "YOY", "QOQ", "MOM", "BUY", "SELL", "HOLD", "WATCH", "HIGH",
        "LOW", "YES", "NO", "N/A", "NULL", "NONE", "INR", "USD",
        "MACD", "GARCH", "PELT", "EXIT", "HODL", "NORMAL",
    }

    # Pattern 1: **SYMBOL** bold in markdown
    for m in re.finditer(r"\*\*([A-Z][A-Z0-9]{1,9})\*\*", text):
        s = m.group(1)
        if s not in _SKIP and s not in visited:
            seen.append(s); visited.add(s)

    # Pattern 2: heading lines with standalone ALL-CAPS word
    for line in text.splitlines():
        if line.startswith(("###", "####", "##")):
            for m in re.finditer(r"\b([A-Z][A-Z0-9]{2,9})\b", line):
                s = m.group(1)
                if s not in _SKIP and s not in visited:
                    seen.append(s); visited.add(s)

    # Pattern 3: table | Symbol | TICKER | or `TICKER` inline code
    for m in re.finditer(r"`([A-Z][A-Z0-9]{2,9})`", text):
        s = m.group(1)
        if s not in _SKIP and s not in visited:
            seen.append(s); visited.add(s)

    return seen[:6]   # cap at 6 to avoid chart-generation explosion


def generate_consolidated_pdf_bytes(
    report_markdown: str,
    symbols: list[str],
    title: str = "",
    report_type: str = "Research Report",
) -> bytes:
    """
    Convert a Markdown report to a consolidated PDF.

    Chart strategy (to keep file size reasonable):
      1 symbol  → price (365d) + MACD (180d) + GARCH vol (180d)   — full 3-chart set
      2–4 syms  → price chart only per symbol (parallel render)   — comparative view
      5+ syms   → no auto-charts (report body is the full content)

    Returns raw PDF bytes.
    """
    import markdown as md
    from jinja2 import Template
    from weasyprint import HTML as WP_HTML
    from concurrent.futures import ThreadPoolExecutor

    extensions = ["tables", "fenced_code", "nl2br", "sane_lists"]
    report_html = md.markdown(report_markdown, extensions=extensions)

    charts: list[dict] = []
    n = len(symbols)

    if n == 1:
        sym = symbols[0]
        from src.db.pool import query_df
        df = None
        try:
            df = query_df(
                """
                SELECT trade_date,
                       toFloat64(argMax(open,   imported_at)) AS open,
                       toFloat64(argMax(high,   imported_at)) AS high,
                       toFloat64(argMax(low,    imported_at)) AS low,
                       toFloat64(argMax(close,  imported_at)) AS close,
                       toFloat64(argMax(volume, imported_at)) AS volume
                FROM market_data.daily_prices FINAL
                WHERE symbol = {sym:String}
                GROUP BY trade_date ORDER BY trade_date ASC
                """,
                parameters={"sym": sym.upper()},
            )
            if df.empty:
                df = None
        except Exception as e:
            log.warning("Pre-fetching daily prices failed for %s: %s", sym, e)
            df = None

        with ThreadPoolExecutor(max_workers=4) as pool:
            f_p = pool.submit(render_price_chart_png, sym, 365, df=df)
            f_m = pool.submit(render_macd_chart_png,  sym, 180, df=df)
            f_g = pool.submit(render_garch_vol_png,   sym, 180, df=df)
            f_c = pool.submit(render_anomaly_clusters_png, sym, 365, df=df)
            for label, fut in [
                (f"{sym} — 1-Year Price  |  🔴 Anomaly  |  🟡 Corp Action", f_p),
                (f"{sym} — MACD(12,26,9)", f_m),
                (f"{sym} — GARCH Annualised Volatility", f_g),
                (f"{sym} — Anomaly Regime Clusters (Feature Space)", f_c),
            ]:
                png = fut.result(timeout=60)
                if png:
                    charts.append({"label": label, "data": png})

    elif 2 <= n <= 4:
        # One price chart per symbol, rendered in parallel
        with ThreadPoolExecutor(max_workers=n) as pool:
            futures = {
                pool.submit(render_price_chart_png, sym, 365): sym
                for sym in symbols
            }
            for fut, sym in futures.items():
                png = fut.result(timeout=60)
                if png:
                    charts.append({
                        "label": f"{sym} — 1-Year Price  |  🔴 Anomaly  |  🟡 Corp Action",
                        "data": png,
                    })

    # Build cover metadata
    if not title:
        if n == 1:
            title = f"{symbols[0]} — Equity Research Note"
        elif n <= 4:
            title = "Consolidated Research — " + " · ".join(symbols)
        else:
            title = "Portfolio Research Report"

    headline = title
    if n == 1:
        subtitle = f"{symbols[0]} · NSE · Equity Research Note"
    elif n <= 4:
        subtitle = " · ".join(symbols) + " · NSE"
    else:
        subtitle = f"{n} symbols · Portfolio Report"

    footer_sym = symbols[0] if symbols else "Mosaic"
    tmpl = Template(_HTML_TEMPLATE)
    html = tmpl.render(
        title=title,
        report_type=report_type,
        headline=headline,
        subtitle=subtitle,
        footer_symbol=footer_sym,
        date_str=datetime.now().strftime("%d %B %Y"),
        report_html=report_html,
        charts=charts,
    )
    return WP_HTML(string=html).write_pdf()


@tool
def publish_research_pdf(
    symbol: str,
    report_markdown: str,
    filename: str = "",
) -> str:
    """
    Publish a completed equity research note as a professionally styled PDF.

    Assembles: cover page → matplotlib price chart (🔴 anomalies, 🟡 corp actions)
    → MACD(12,26,9) chart → GARCH volatility chart → full research note body
    → legal disclaimer. Saves to output/reports/<symbol>_<date>.pdf.

    Call this as the FINAL step of every deep-dive research workflow, after all
    analysis sections have been written. Pass the complete Markdown report text.

    Args:
        symbol:          NSE trading symbol (e.g. MSUMI, RELIANCE)
        report_markdown: Full Markdown research note produced by the agent
        filename:        Optional output filename (default: <SYMBOL>_YYYYMMDD.pdf)

    Returns the absolute path of the saved PDF.
    """
    symbol_upper = symbol.strip().upper()
    # Delegate to the consolidated engine (single-symbol path)
    try:
        pdf_bytes = generate_consolidated_pdf_bytes(
            report_markdown,
            symbols=[symbol_upper],
            report_type="Equity Research",
        )
    except Exception as exc:
        return f"PDF generation failed for {symbol_upper}: {exc}"

    _OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    if not filename:
        filename = f"{symbol_upper}_{datetime.now().strftime('%Y%m%d')}.pdf"
    if not filename.endswith(".pdf"):
        filename += ".pdf"
    out_path = _OUTPUT_DIR / filename
    out_path.write_bytes(pdf_bytes)

    size_kb = len(pdf_bytes) // 1024
    return (
        f"✅ Research report saved: **{out_path.resolve()}**\n"
        f"Size: {size_kb} KB  |  Charts: price, MACD, GARCH vol, anomaly clusters  |  "
        f"Symbol: {symbol_upper}  |  Date: {datetime.now().strftime('%d %b %Y')}"
    )


@tool
def publish_consolidated_pdf(
    report_markdown: str,
    symbols: str = "",
    title: str = "",
    filename: str = "",
) -> str:
    """
    Publish the COMPLETE final output of any agent run as a single consolidated PDF.

    Use this as the universal last step — works for single-symbol deep dives,
    multi-symbol comparative reports, anomaly reports, news reports, and portfolio
    summaries.  It auto-detects which symbols are covered by the report when
    `symbols` is not provided.

    Chart strategy (auto-selected by symbol count):
      1 symbol  → price chart + MACD(12,26,9) + GARCH vol      (full research set)
      2–4 syms  → one price chart per symbol, rendered in parallel  (comparative)
      5+ syms   → report body only — no per-symbol charts

    Args:
        report_markdown : The COMPLETE Markdown text of the agent's final output.
                          Pass everything — all sections, tables, analysis.
        symbols         : Comma-separated NSE symbols covered (e.g. "MSUMI,HDFCBANK").
                          Leave blank to auto-detect from the report text.
        title           : Custom PDF title / cover headline. Auto-generated if blank.
        filename        : Output filename. Default: <SYMBOLS>_<YYYYMMDD>.pdf

    Returns the absolute path of the saved PDF.
    """
    # Parse or detect symbols
    sym_list: list[str] = []
    if symbols.strip():
        sym_list = [s.strip().upper() for s in symbols.split(",") if s.strip()]
    if not sym_list:
        sym_list = _detect_symbols_in_markdown(report_markdown)

    # Choose report_type label for cover
    n = len(sym_list)
    if n == 1:
        report_type = "Equity Research"
    elif n >= 2:
        report_type = "Consolidated Research"
    else:
        report_type = "Research Report"

    try:
        pdf_bytes = generate_consolidated_pdf_bytes(
            report_markdown,
            symbols=sym_list,
            title=title,
            report_type=report_type,
        )
    except Exception as exc:
        return f"Consolidated PDF generation failed: {exc}"

    _OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    if not filename:
        date_tag = datetime.now().strftime("%Y%m%d")
        sym_tag  = "_".join(sym_list[:3]) if sym_list else "report"
        filename = f"{sym_tag}_{date_tag}.pdf"
    if not filename.endswith(".pdf"):
        filename += ".pdf"
    out_path = _OUTPUT_DIR / filename
    out_path.write_bytes(pdf_bytes)

    size_kb   = len(pdf_bytes) // 1024
    chart_desc = (
        "price + MACD + GARCH + clusters" if n == 1
        else f"price × {n} symbols" if n <= 4
        else "report only (5+ symbols)"
    )
    sym_desc = ", ".join(sym_list) if sym_list else "auto-detected"
    return (
        f"✅ Consolidated report saved: **{out_path.resolve()}**\n"
        f"Symbols: {sym_desc}  |  Charts: {chart_desc}  |  "
        f"Size: {size_kb} KB  |  Date: {datetime.now().strftime('%d %b %Y')}"
    )
