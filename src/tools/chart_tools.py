"""
src/tools/chart_tools.py
────────────────────────
Console chart tools using plotext — ASCII/Unicode charts rendered in the terminal.

All tools return a string (the chart) suitable for Rich panel display.
Falls back to a plain markdown table if plotext is not installed.
"""

from __future__ import annotations

import logging
import numpy as np
from typing import Any

from langchain_core.tools import tool

logger = logging.getLogger(__name__)

from typing import Any, Sequence  # noqa: F401

_SPARKS = " ▂▃▄▅▆▇█"
_CHART_HEIGHT = 20


def render_sparkline(values: Sequence[float]) -> str:
    """Render a sequence of numerical values as a compact Unicode sparkline string."""
    clean = [v for v in values if v is not None and not (isinstance(v, float) and (v != v))]
    if not clean:
        return ""
    min_v, max_v = min(clean), max(clean)
    if min_v == max_v:
        return _SPARKS[0] * len(clean)
    return "".join(_SPARKS[min(7, max(0, int(7 * (val - min_v) / (max_v - min_v))))] for val in clean)


def _chart_width() -> int:
    """Terminal width minus panel borders — capped at 100, minimum 60."""
    try:
        import shutil
        return max(60, min(100, shutil.get_terminal_size().columns - 8))
    except Exception:
        return 76


# Session-level cache for composite anomaly dates.
# Keyed by (symbol_upper, category, n_rows): n_rows acts as a version token —
# a cache miss on new data without any explicit invalidation machinery.
_ANOMALY_DATES_CACHE: dict[tuple, tuple[set, set]] = {}  # → (anomaly_dates, corp_action_dates)


import functools
import re

_ACTIVE_CHARTS: dict[str, str] = {}

def get_active_charts() -> dict[str, str]:
    global _ACTIVE_CHARTS
    return _ACTIVE_CHARTS

def save_active_chart(key: str, chart_str: str) -> None:
    get_active_charts()[key] = chart_str

def clear_active_charts() -> None:
    get_active_charts().clear()


def _extract_symbol_for_chart(text: str) -> str | None:
    """Extract stock or ETF symbol from markdown headings/context."""
    import re
    # Match "# SYMBOL —", "# SYMBOL (NSE)", "**SYMBOL (EXCHANGE)**"
    m = re.search(r"^[#\s]*([A-Z0-9_\-\.]{2,15})\s+[—–\-\(]", text, re.MULTILINE)
    if m:
        return m.group(1).strip().upper()
    m = re.search(r"\b([A-Z0-9_\-\.]{2,15})\s+—\s+Equity Research", text)
    if m:
        return m.group(1).strip().upper()
    m = re.search(r"\b([A-Z0-9_\-\.]{2,15})\s+VISTAS", text, re.IGNORECASE)
    if m:
        return m.group(1).strip().upper()
    m = re.search(r"\b(NUVOCO|RELIANCE|TCS|INFY|HDFCBANK|TATAMOTORS|ITC|SBIN|BAJFINANCE|GOLDBEES|NIFTYBEES|SILVERBEES)\b", text)
    if m:
        return m.group(1).strip().upper()
    return None


def inject_chart_placeholders(text: str) -> str:
    """Replace [CHART:xxx] tokens in text with the real chart strings a plot_*
    tool call saved this run (via save_active_chart). If a placeholder remains
    and wasn't rendered yet, automatically extracts the symbol from text and
    generates the chart on demand."""
    try:
        chart_by_type = get_active_charts().copy()
        for tname, chart_str in chart_by_type.items():
            placeholders = [f"[CHART:{tname}]"]
            if tname.startswith("plot_") and tname.endswith("_chart"):
                placeholders.append(f"[CHART:{tname[5:-6]}]")
            if "macd" in tname.lower() or "mcad" in tname.lower():
                placeholders.extend(["[CHART:macd]", "[CHART:mcad]", "[CHART:plot_macd_chart]"])
            for ph in placeholders:
                if ph in text:
                    text = text.replace(ph, chart_str)
                    break

        # Fallback on-demand generation if placeholders are still present in text
        if "[CHART:price]" in text or "[CHART:plot_price_chart]" in text:
            sym = _extract_symbol_for_chart(text)
            if sym:
                try:
                    plot_price_chart.invoke({"symbol": sym, "days": 365})
                    latest_charts = get_active_charts()
                    if "price" in latest_charts:
                        text = text.replace("[CHART:price]", latest_charts["price"])
                        text = text.replace("[CHART:plot_price_chart]", latest_charts["price"])
                except Exception as exc:
                    logger.debug("On-demand plot_price_chart failed for %s: %s", sym, exc)

        if "[CHART:shareholding]" in text or "[CHART:plot_shareholding_bar]" in text:
            sym = _extract_symbol_for_chart(text)
            if sym:
                try:
                    plot_shareholding_bar.invoke({"symbol": sym})
                    latest_charts = get_active_charts()
                    if "shareholding" in latest_charts:
                        text = text.replace("[CHART:shareholding]", latest_charts["shareholding"])
                        text = text.replace("[CHART:plot_shareholding_bar]", latest_charts["shareholding"])
                except Exception as exc:
                    logger.debug("On-demand plot_shareholding_bar failed for %s: %s", sym, exc)

        if "[CHART:macd]" in text or "[CHART:plot_macd_chart]" in text or "[CHART:mcad]" in text:
            sym = _extract_symbol_for_chart(text)
            if sym:
                try:
                    plot_macd_chart.invoke({"symbol": sym})
                    latest_charts = get_active_charts()
                    if "macd" in latest_charts:
                        for ph in ["[CHART:macd]", "[CHART:mcad]", "[CHART:plot_macd_chart]"]:
                            text = text.replace(ph, latest_charts["macd"])
                except Exception as exc:
                    logger.debug("On-demand plot_macd_chart failed for %s: %s", sym, exc)

    except Exception:
        logger.warning("Chart placeholder substitution failed", exc_info=True)
    return text

def clean_chart_tool_output(func):
    """
    No-op decorator since _build() now intercepts the plotting output directly
    and saves it to thread-local storage.
    """
    return func


def _load_corp_actions(symbol: str) -> "pd.DataFrame | None":
    """Load corporate actions from ClickHouse for a symbol. Returns None on miss."""
    try:
        import pandas as pd
        from src.db.pool import query_df
        df = query_df(
            "SELECT ex_date, action_type FROM market_data.corporate_actions FINAL "
            "WHERE symbol = {sym:String}",
            parameters={"sym": symbol.upper()},
        )
        if df.empty:
            return None
        df["ex_date"] = pd.to_datetime(df["ex_date"])
        return df
    except Exception:
        return None


def _composite_anomaly_dates(symbol: str, category: str = "") -> tuple[set, set] | None:
    """
    Run the composite anomaly pipeline (GARCH vol-normalization + Isolation
    Forest + PELT change-point detection) on the symbol's FULL OHLCV history,
    incorporating NSE corporate actions so mechanical price jumps on split /
    bonus / demerger ex-dates are NOT flagged as anomalies.

    Returns
    -------
    (anomaly_dates, corp_action_dates) : two sets of normalised pd.Timestamps
        anomaly_dates     → red 🔴 dots on the chart
        corp_action_dates → gold 🏦 markers (splits, bonuses, demergers, etc.)
    None : pipeline unavailable / too few rows — caller falls back to naive threshold.
    """
    try:
        import pandas as pd
        from src.db.pool import query_df

        # Parameterised query — prevents SQL injection via symbol/category values.
        params: dict = {"sym": symbol.upper()}
        cat_clause = "AND category = {cat:String}" if category else ""
        if category:
            params["cat"] = category
        # Full history (not just the display window) — GARCH/PELT need it.
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
        if df.empty or len(df) < 60:
            return None

        # Return cached result if row count (data version) is unchanged.
        cache_key = (symbol.upper(), category, len(df))
        if cache_key in _ANOMALY_DATES_CACHE:
            logger.info("Using cached anomaly detection results for %s.", symbol.upper())
            return _ANOMALY_DATES_CACHE[cache_key]

        logger.info("Refitting anomaly detection models (cache miss for %s)...", symbol.upper())
        df_corp = _load_corp_actions(symbol)

        df["trade_date"] = pd.to_datetime(df["trade_date"])
        df = df.sort_values("trade_date").reset_index(drop=True)
        df["volume"] = df["volume"].fillna(0.0)

        # COT (gold only) + USDINR cross-asset features — same pattern as
        # explain_price_anomalies in src/tools/market/gold.py.
        df_cot = df_fx = None
        if "GOLD" in symbol.upper():
            try:
                _cot = query_df(
                    "SELECT report_date, mm_net, open_interest FROM market_data.cot_gold"
                )
                if not _cot.empty:
                    _cot["report_date"] = pd.to_datetime(_cot["report_date"])
                    df_cot = _cot
            except Exception as exc:
                logger.warning("COT fetch failed (non-fatal): %s", exc)
        try:
            _fx = query_df(
                "SELECT symbol, trade_date, toFloat64(close) AS close "
                "FROM market_data.fx_rates FINAL WHERE symbol = 'USDINR'"
            )
            if not _fx.empty:
                _fx["trade_date"] = pd.to_datetime(_fx["trade_date"])
                df_fx = _fx
        except Exception as exc:
            logger.warning("FX fetch failed (non-fatal): %s", exc)

        from src.ml.anomaly import run_composite_anomaly
        df_result, df_flagged, _ = run_composite_anomaly(
            df[["trade_date", "open", "high", "low", "close", "volume"]].copy(),
            df_cot=df_cot, df_fx=df_fx,
            df_corp_actions=df_corp,
            symbol=symbol, category=category,
            store=False,
        )
        anomaly_dates = {
            pd.Timestamp(d).normalize() for d in df_flagged["trade_date"]
        }
        # Corporate action dates for 🏦 markers (all action types, from df_result)
        corp_action_dates: set = set()
        if "is_corporate_action" in df_result.columns:
            corp_action_dates = {
                pd.Timestamp(d).normalize()
                for d in df_result.loc[df_result["is_corporate_action"], "trade_date"]
            }
        result = (anomaly_dates, corp_action_dates)
        _ANOMALY_DATES_CACHE[cache_key] = result
        return result
    except Exception as exc:
        logger.warning("Composite anomaly detection failed, naive fallback: %s", exc)
        return None


def sparkline(values: list[float]) -> str:
    """Return a single-line Unicode sparkline string for an array of floats."""
    if not values:
        return ""
    mn, mx = min(values), max(values)
    rng = mx - mn or 1
    return "".join(_SPARKS[min(7, int((v - mn) / rng * 8))] for v in values)


def _plt():
    """Lazy import of plotext — raises ImportError with a helpful message if missing."""
    try:
        import plotext as _p
        if not hasattr(_p, "clear_figure"):
            _p.clear_figure = getattr(_p, "clf", getattr(_p, "clc", lambda: None))
        if not hasattr(_p, "clf"):
            _p.clf = getattr(_p, "clear_figure", lambda: None)
        return _p
    except ImportError:
        raise ImportError(
            "plotext is not installed.  Run: pip install plotext>=5.2.0"
        )


def _build(plt: Any) -> str:
    """Build the chart string and reset plotext state. ANSI color codes are kept —
    Text.from_ansi() in the callback handler renders them as Rich colors."""
    out = plt.build()
    plt.clear_figure()
    
    # Infer key from the caller tool name to save the chart into the thread-local store
    import inspect
    frame = inspect.currentframe()
    key = "chart"
    try:
        caller_frame = frame
        while caller_frame:
            func_name = caller_frame.f_code.co_name
            if func_name.startswith("plot_"):
                if "price" in func_name.lower():
                    key = "price"
                elif "shareholding" in func_name.lower():
                    key = "shareholding"
                elif "macd" in func_name.lower() or "mcad" in func_name.lower():
                    key = "macd"
                else:
                    key = func_name
                break
            caller_frame = caller_frame.f_back
    finally:
        del frame
        
    save_active_chart(key, out)
    return out


def _build_raw(plt: Any) -> str:
    """Build and return chart string without saving to active_charts store.
    Used for independent subplot rendering to avoid cursor positioning escape code loss."""
    out = plt.build()
    plt.clear_figure()
    return out


def _data_table(headers: list[str], rows: list[list], title: str = "") -> str:
    """
    Format chart data as a compact Markdown table appended below the chart.
    Only the last 15 rows are shown to keep output tight.
    """
    if not rows:
        return ""
    rows = rows[-15:]  # most recent rows only
    header_row = " | ".join(headers)
    sep_row    = " | ".join("---" for _ in headers)
    data_rows  = "\n".join("| " + " | ".join(str(v) for v in row) + " |" for row in rows)
    heading = f"\n**{title}**\n" if title else "\n"
    return f"{heading}| {header_row} |\n| {sep_row} |\n{data_rows}"


@tool
@clean_chart_tool_output
def plot_price_chart(
    symbol: str,
    days: int = 60,
    category: str = "",
    start_date: str = "",
    end_date: str = "",
) -> str:
    """
    Plot a price (close) trend chart for any NSE symbol from ClickHouse.
    Renders as an ASCII line chart directly in the terminal.

    Args:
        symbol:     NSE symbol — e.g. GOLDBEES, NIFTYBEES, RELIANCE, LICI
        days:       Number of trading days to show (default 60). Ignored if start_date is set.
        category:   'etfs', 'stocks', 'indices', 'commodities' — leave blank to auto-detect
        start_date: Optional start date in YYYY-MM-DD format (e.g. '2019-01-01')
        end_date:   Optional end date in YYYY-MM-DD format (e.g. '2019-12-31')

    Example: plot_price_chart("GOLDBEES", start_date="2019-01-01", end_date="2019-12-31")
    """
    try:
        logger.info("Plotting price chart for %s (days=%s)...", symbol, days)
        from src.db.pool import query_df
        cat_filter = f"AND category = '{category}'" if category else ""
        if start_date:
            date_filter = f"AND trade_date >= '{start_date}'"
            if end_date:
                date_filter += f" AND trade_date <= '{end_date}'"
            days_desc = f"{start_date} to {end_date or 'today'}"
        else:
            date_filter = f"AND trade_date >= today() - {days}"
            days_desc = f"{days}d"

        df = query_df(f"""
            SELECT trade_date,
                   toFloat64(argMax(open,   imported_at)) AS open,
                   toFloat64(argMax(high,   imported_at)) AS high,
                   toFloat64(argMax(low,    imported_at)) AS low,
                   toFloat64(argMax(close,  imported_at)) AS close,
                   toFloat64(argMax(volume, imported_at)) AS volume
            FROM market_data.daily_prices FINAL
            WHERE symbol = '{symbol.upper()}' {cat_filter}
              {date_filter}
            GROUP BY trade_date
            ORDER BY trade_date ASC
        """)
        if df.empty:
            # Fallback to yfinance price history
            from src.tools.yahoo_finance import fetch_price_history
            clean_symbol = symbol.upper()
            exchange = "NSE"
            if clean_symbol.endswith(".NS"):
                clean_symbol = clean_symbol[:-3]
                exchange = "NSE"
            elif clean_symbol.endswith(".BO"):
                clean_symbol = clean_symbol[:-3]
                exchange = "BSE"

            if start_date:
                hist = fetch_price_history(
                    clean_symbol,
                    exchange,
                    start_date=start_date,
                    end_date=end_date,
                )
            else:
                if days <= 30:
                    yf_period = "1mo"
                elif days <= 90:
                    yf_period = "3mo"
                elif days <= 180:
                    yf_period = "6mo"
                elif days <= 365:
                    yf_period = "1y"
                else:
                    yf_period = "2y"
                hist = fetch_price_history(clean_symbol, exchange, period=yf_period)

            if not hist:
                return f"No price data found for {symbol} (tried ClickHouse and Yahoo Finance fallback)."

            dates  = [r["date"] for r in hist]
            prices = [r["close"] for r in hist]
        else:
            dates  = df["trade_date"].astype(str).tolist()
            prices = df["close"].tolist()

        import pandas as pd
        ret_s = pd.Series(prices).pct_change()
        vol_s = (ret_s.rolling(20).std() * np.sqrt(252) * 100.0).fillna(ret_s.std() * np.sqrt(252) * 100.0 if not pd.isna(ret_s.std()) else 0.0).tolist()
        volumes = df["volume"].tolist() if not df.empty and "volume" in df.columns else ([r.get("volume", 0) for r in hist] if hist else [0] * len(prices))

        spark  = sparkline(prices)
        chg    = ((prices[-1] - prices[0]) / prices[0] * 100) if len(prices) >= 2 else 0

        plt = _plt()
        plt.clear_figure()
        xs = list(range(len(prices)))

        # Each panel is built and rendered independently (its own clear_figure()
        # + build() pair) rather than via plt.subplots()+one build() call.
        # plotext's multi-subplot grid emits absolute cursor-positioning ANSI
        # codes that only resolve correctly on a live, attached TTY — captured
        # non-interactively (piped script, LangChain tool return value), those
        # codes overwrite each other and only the last panel survives. Three
        # independent full-width builds concatenated as plain text render
        # correctly in both contexts.
        plt.plot_size(_chart_width(), 14)

        # Panel 1: Price
        plt.plot(xs, prices, label=symbol)

        # Overlay anomalies
        try:
            anomaly_xs:    list[int] = []
            anomaly_ys:    list[float] = []
            corp_act_xs:   list[int] = []
            corp_act_ys:   list[float] = []

            pipeline_result = _composite_anomaly_dates(symbol, category)
            if pipeline_result is not None:
                flagged_dates, corp_dates = pipeline_result
                for idx, d in enumerate(dates):
                    try:
                        ts = pd.Timestamp(d).normalize()
                        if ts in flagged_dates:
                            anomaly_xs.append(idx)
                            anomaly_ys.append(prices[idx])
                        elif ts in corp_dates:
                            corp_act_xs.append(idx)
                            corp_act_ys.append(prices[idx])
                    except Exception:
                        continue
            else:
                # Naive fallback — composite unavailable
                returns = pd.Series(prices).pct_change() * 100
                std_ret = returns.std()
                threshold = max(2.0, 2.5 * std_ret) if not pd.isna(std_ret) else 2.0
                for idx, ret_val in enumerate(returns):
                    if not pd.isna(ret_val) and abs(ret_val) >= threshold:
                        anomaly_xs.append(idx)
                        anomaly_ys.append(prices[idx])

            if anomaly_xs:
                plt.scatter(anomaly_xs, anomaly_ys, color="red", marker="🔴", label="Anomaly")
            if corp_act_xs:
                plt.scatter(corp_act_xs, corp_act_ys, color="yellow", marker="🏦", label="Corp Action")
        except Exception as exc:
            logger.warning("Failed to plot price anomalies on chart: %s", exc)

        plt.title(f"{symbol} — {days_desc} price  |  {chg:+.1f}%  |  {spark}")
        if symbol.upper().endswith("=F") or "=F" in symbol.upper():
            plt.ylabel("Price ($)")
        else:
            plt.ylabel("Price (₹)")

        # Set date ticks
        n = len(dates)
        step = max(1, n // 5)
        tick_idx = list(range(0, n, step))
        if tick_idx and tick_idx[-1] != n - 1:
            tick_idx.append(n - 1)
        tick_lbl = [str(dates[i])[:10] for i in tick_idx]
        plt.xticks(tick_idx, tick_lbl)

        price_panel = plt.build()
        plt.clear_figure()

        # Panel 2: 20D Annualized Volatility %
        plt.plot_size(_chart_width(), 10)
        plt.plot(xs, vol_s, label="20D Vol (%)", color="magenta")
        plt.ylabel("Vol (%)")
        plt.xticks(tick_idx, tick_lbl)
        vol_panel = plt.build()
        plt.clear_figure()

        # Panel 3: Volume
        plt.plot_size(_chart_width(), 10)
        plt.bar(xs, volumes, label="Volume", color="cyan")
        plt.ylabel("Volume")
        plt.xticks(tick_idx, tick_lbl)
        volume_panel = plt.build()
        plt.clear_figure()

        chart_text = price_panel + "\n" + vol_panel + "\n" + volume_panel
        save_active_chart("price", chart_text)
        table = _data_table(
            ["Date", "Open", "High", "Low", "Close", "Volume"],
            [
                [str(r["trade_date"])[:10],
                 f"{r['open']:.2f}", f"{r['high']:.2f}",
                 f"{r['low']:.2f}",  f"{r['close']:.2f}",
                 f"{int(r['volume']):,}"]
                for _, r in df.tail(15).iterrows()
            ],
            title=f"{symbol} — Price Data",
        )
        return chart_text + "\n" + table
    except ImportError as exc:
        return str(exc)
    except Exception as exc:
        return f"Error plotting {symbol}: {exc}"


@tool
@clean_chart_tool_output
def plot_fii_dii_chart(days: int = 30) -> str:
    """
    Plot FII and DII net flows as a dual bar chart.
    Green bars = DII net inflow, Red bars = FII net outflow.

    Args:
        days: Number of trading days to show (default 30)

    Example: plot_fii_dii_chart(days=20)
    """
    try:
        from src.db.pool import query_df
        df = query_df(f"""
            SELECT trade_date,
                   toFloat64(argMax(fii_net_cr, imported_at)) AS fii_net,
                   toFloat64(argMax(dii_net_cr, imported_at)) AS dii_net
            FROM market_data.fii_dii_flows FINAL
            WHERE trade_date >= today() - {days}
            GROUP BY trade_date
            ORDER BY trade_date ASC
        """)
        if df.empty:
            return "No FII/DII flow data found."

        labels  = df["trade_date"].astype(str).tolist()
        fii     = df["fii_net"].tolist()
        dii     = df["dii_net"].tolist()
        fii_tot = sum(fii)
        dii_tot = sum(dii)

        plt = _plt()
        plt.clear_figure()
        plt.bar(list(range(len(fii))), fii, label=f"FII net  (Σ {fii_tot:+,.0f} Cr)")
        plt.bar(list(range(len(dii))), dii, label=f"DII net  (Σ {dii_tot:+,.0f} Cr)")
        plt.title(f"FII / DII Net Flows — last {days} days")
        
        # Format ticks to show short dates (DD/MM) rather than indices (0..18)
        from datetime import datetime
        step = max(1, len(fii) // 5)  # target showing ~5-6 ticks to prevent overlap
        tick_indices = []
        tick_labels = []
        for idx in range(0, len(fii), step):
            tick_indices.append(idx)
            try:
                dt = datetime.strptime(labels[idx], "%Y-%m-%d")
                tick_labels.append(dt.strftime("%d/%m"))
            except Exception:
                tick_labels.append(labels[idx])
        
        # Ensure the last date is included
        if (len(fii) - 1) not in tick_indices:
            tick_indices.append(len(fii) - 1)
            try:
                dt = datetime.strptime(labels[-1], "%Y-%m-%d")
                tick_labels.append(dt.strftime("%d/%m"))
            except Exception:
                tick_labels.append(labels[-1])
                
        plt.xticks(tick_indices, tick_labels)
        
        plt.xlabel(f"{labels[0]} → {labels[-1]}")
        plt.ylabel("₹ Crore")
        plt.plot_size(_chart_width(), _CHART_HEIGHT)
        chart = _build(plt)
        table = _data_table(
            ["Date", "FII Net (₹ Cr)", "DII Net (₹ Cr)", "Combined"],
            [[str(r["trade_date"])[:10],
              f"{r['fii_net']:+,.0f}", f"{r['dii_net']:+,.0f}",
              f"{r['fii_net']+r['dii_net']:+,.0f}"]
             for _, r in df.iterrows()],
            title="FII / DII Flow Data",
        )
        return chart + table
    except ImportError as exc:
        return str(exc)
    except Exception as exc:
        return f"Error plotting FII/DII flows: {exc}"


@tool
@clean_chart_tool_output
def plot_dxy_chart(days: int = 365) -> str:
    """
    Plot US Dollar Index (DXY) price trend as a line chart.

    Args:
        days: Number of calendar days of history to show (default 365 = 1 year)

    Example: plot_dxy_chart(days=365)
    """
    try:
        from src.db.pool import query_df
        import pandas as pd
        from datetime import datetime

        df = query_df(f"""
            SELECT trade_date, toFloat64(argMax(close, imported_at)) AS close
            FROM market_data.daily_prices FINAL
            WHERE symbol = 'DXY'
              AND trade_date >= today() - {days}
            GROUP BY trade_date
            ORDER BY trade_date ASC
        """)
        if df.empty:
            return (
                "No DXY data found. "
                "Run: `mosaic import --categories indices` to import it."
            )

        labels = df["trade_date"].astype(str).tolist()
        prices = df["close"].tolist()

        plt = _plt()
        plt.clear_figure()
        plt.plot(list(range(len(prices))), prices, label="DXY")
        plt.title(f"US Dollar Index (DXY) — last {days} days")

        step = max(1, len(prices) // 6)
        tick_indices, tick_labels = [], []
        for idx in range(0, len(prices), step):
            tick_indices.append(idx)
            try:
                dt = datetime.strptime(labels[idx], "%Y-%m-%d")
                tick_labels.append(dt.strftime("%b'%y"))
            except Exception:
                tick_labels.append(labels[idx])
        if (len(prices) - 1) not in tick_indices:
            tick_indices.append(len(prices) - 1)
            try:
                dt = datetime.strptime(labels[-1], "%Y-%m-%d")
                tick_labels.append(dt.strftime("%b'%y"))
            except Exception:
                tick_labels.append(labels[-1])

        plt.xticks(tick_indices, tick_labels)
        plt.xlabel(f"{labels[0]} → {labels[-1]}")
        plt.ylabel("DXY Level")
        plt.plot_size(_chart_width(), _CHART_HEIGHT)
        chart = _build(plt)

        hi = max(prices)
        lo = min(prices)
        hi_date = labels[prices.index(hi)]
        lo_date = labels[prices.index(lo)]
        latest = prices[-1]
        chg_pct = (latest - prices[0]) / prices[0] * 100

        summary = (
            f"\n**DXY Summary ({labels[0]} → {labels[-1]})**\n"
            f"| Metric | Value |\n| --- | --- |\n"
            f"| Latest | {latest:.2f} |\n"
            f"| Period High | {hi:.2f} ({hi_date}) |\n"
            f"| Period Low | {lo:.2f} ({lo_date}) |\n"
            f"| Period Change | {chg_pct:+.2f}% |"
        )
        return chart + summary
    except ImportError as exc:
        return str(exc)
    except Exception as exc:
        return f"Error plotting DXY chart: {exc}"


@tool
@clean_chart_tool_output
def plot_signal_scores(top_n: int = 18) -> str:
    """
    Plot the latest composite ETF signal scores as a horizontal bar chart.
    Bars are sorted by score — useful for spotting the strongest buy/sell signals.

    Args:
        top_n: Max ETFs to show (default 18 = all tracked ETFs)

    Example: plot_signal_scores()
    """
    try:
        from src.db.pool import query_df
        df = query_df(f"""
            SELECT etf_symbol,
                   argMax(composite_score, as_of) AS score,
                   argMax(action, as_of)          AS action
            FROM market_data.signal_composite FINAL
            GROUP BY etf_symbol
            ORDER BY score DESC
            LIMIT {top_n}
        """)
        if df.empty:
            return "No signal data found. Run `signals --save` first."

        symbols = df["etf_symbol"].tolist()
        scores  = df["score"].tolist()

        plt = _plt()
        plt.clear_figure()
        plt.bar(symbols, scores, orientation="horizontal")
        plt.title("ETF Composite Signal Scores (latest)")
        plt.xlabel("Score (0–100)")
        plt.plot_size(_chart_width(), max(len(symbols) + 4, 16))
        chart = _build(plt)
        table = _data_table(
            ["ETF", "Score", "Action"],
            [[r["etf_symbol"], f"{r['score']:.1f}", r["action"]]
             for _, r in df.iterrows()],
            title="Signal Scores",
        )
        return chart + table
    except ImportError as exc:
        return str(exc)
    except Exception as exc:
        return f"Error plotting signal scores: {exc}"


@tool
@clean_chart_tool_output
def plot_nav_chart(symbol_or_scheme: str, days: int = 90) -> str:
    """
    Plot the NAV trend for a mutual fund or ETF from ClickHouse.

    Accepts either an NSE symbol (e.g. 'GOLDBEES', 'NIFTYBEES') or a numeric
    scheme code (e.g. '152056' for DSP Multi Asset).

    Args:
        symbol_or_scheme: NSE symbol or numeric scheme code
        days:             Number of days of history to show (default 90)

    Examples:
        plot_nav_chart("GOLDBEES", days=30)
        plot_nav_chart("152056", days=180)
    """
    try:
        from src.db.pool import query_df

        val = symbol_or_scheme.strip()
        # Numeric → query by scheme_code; alphabetic → query by symbol
        if val.isdigit():
            where = f"scheme_code = '{val}'"
            label = f"scheme {val}"
        else:
            where = f"symbol = '{val.upper()}'"
            label = val.upper()

        df = query_df(f"""
            SELECT nav_date,
                   toFloat64(argMax(nav, imported_at)) AS nav
            FROM market_data.mf_nav FINAL
            WHERE {where}
              AND nav_date >= today() - {days}
            GROUP BY nav_date
            ORDER BY nav_date ASC
        """)
        if df.empty:
            return f"No NAV data found for '{val}' (last {days} days)."

        dates = df["nav_date"].astype(str).tolist()
        navs  = df["nav"].tolist()
        spark = sparkline(navs)
        chg   = ((navs[-1] - navs[0]) / navs[0] * 100) if len(navs) >= 2 else 0

        plt = _plt()
        plt.clear_figure()
        xs = list(range(len(navs)))
        plt.plot(xs, navs, label=label)
        plt.title(f"NAV — {label}  |  {chg:+.1f}%  |  {spark}")
        plt.ylabel("NAV (₹)")
        plt.plot_size(_chart_width(), _CHART_HEIGHT)
        n = len(dates)
        step = max(1, n // 5)
        tick_idx = list(range(0, n, step))
        if tick_idx and tick_idx[-1] != n - 1:
            tick_idx.append(n - 1)
        plt.xticks(tick_idx, [str(dates[i])[:10] for i in tick_idx])
        return _build(plt)
    except ImportError as exc:
        return str(exc)
    except Exception as exc:
        return f"Error plotting NAV for '{symbol_or_scheme}': {exc}"


@tool
@clean_chart_tool_output
def plot_multi_price_chart(symbols: str, days: int = 60, category: str = "") -> str:
    """
    Plot price trends for multiple NSE symbols on the same chart for comparison.

    Args:
        symbols:  Comma-separated NSE symbols — e.g. 'GOLDBEES,SILVERBEES,NIFTYBEES'
        days:     Trading days to show (default 60)
        category: Ignored (retained for backward compatibility)

    Example: plot_multi_price_chart("GOLDBEES,SILVERBEES", days=90)
    """
    try:
        from src.db.pool import query_df
        sym_list = [s.strip().upper() for s in symbols.split(",") if s.strip()]
        if not sym_list:
            return "No symbols provided."

        series_data = {}  # symbol -> dict of date_str -> price
        symbol_data_counts = {}  # symbol -> count

        for s in sym_list:
            # 1. Try ClickHouse (any category)
            df_sym = query_df(f"""
                SELECT trade_date, toFloat64(argMax(close, imported_at)) AS close
                FROM market_data.daily_prices FINAL
                WHERE symbol = '{s}'
                  AND trade_date >= today() - {days}
                GROUP BY trade_date
                ORDER BY trade_date ASC
            """)

            dates_prices = {}
            if not df_sym.empty and len(df_sym) >= 5:
                for _, row in df_sym.iterrows():
                    dates_prices[str(row["trade_date"])] = float(row["close"])
            else:
                # 2. Try Yahoo Finance fallback
                from src.tools.yahoo_finance import fetch_price_history
                clean_symbol = s
                exchange = "NSE"
                if clean_symbol.endswith(".NS"):
                    clean_symbol = clean_symbol[:-3]
                    exchange = "NSE"
                elif clean_symbol.endswith(".BO"):
                    clean_symbol = clean_symbol[:-3]
                    exchange = "BSE"

                if days <= 30:
                    yf_period = "1mo"
                elif days <= 90:
                    yf_period = "3mo"
                elif days <= 180:
                    yf_period = "6mo"
                elif days <= 365:
                    yf_period = "1y"
                else:
                    yf_period = "2y"

                hist = fetch_price_history(clean_symbol, exchange, period=yf_period)
                if hist:
                    for r in hist:
                        dates_prices[r["date"]] = r["close"]

            if dates_prices:
                series_data[s] = dates_prices
                symbol_data_counts[s] = len(dates_prices)

        if not series_data:
            return f"No price data found for symbols: {symbols} (tried ClickHouse and Yahoo Finance fallback)."

        # Build common date axis from the symbol with most data points
        ref_sym = max(symbol_data_counts, key=symbol_data_counts.get)
        ref_dates = sorted(list(series_data[ref_sym].keys()))

        plt = _plt()
        plt.clear_figure()

        for sym, dates_prices in series_data.items():
            prices = []
            last_valid_price = None
            
            # Find the first valid price to fill backwards if needed
            first_valid_price = None
            sorted_dates = sorted(list(dates_prices.keys()))
            if sorted_dates:
                first_valid_price = dates_prices[sorted_dates[0]]

            for d in ref_dates:
                if d in dates_prices:
                    price = dates_prices[d]
                    last_valid_price = price
                else:
                    price = last_valid_price if last_valid_price is not None else first_valid_price
                prices.append(price)

            if not prices or all(p is None for p in prices):
                continue

            base = prices[0] or 1.0
            norm = [p / base * 100 for p in prices]
            plt.plot(list(range(len(norm))), norm, label=sym)

        plt.title(f"Normalised price comparison (base=100)  —  last {days} days")
        plt.ylabel("Indexed price (base 100)")
        plt.plot_size(_chart_width(), _CHART_HEIGHT)

        n = len(ref_dates)
        step = max(1, n // 5)
        tick_idx = list(range(0, n, step))
        if tick_idx and tick_idx[-1] != n - 1:
            tick_idx.append(n - 1)
        plt.xticks(tick_idx, [str(ref_dates[i])[:10] for i in tick_idx])
        return _build(plt)
    except ImportError as exc:
        return str(exc)
    except Exception as exc:
        return f"Error plotting {symbols}: {exc}"


@tool
@clean_chart_tool_output
def plot_fund_holdings_chart(fund_name: str, top_n: int = 15, as_of_month: str = "") -> str:
    """
    Horizontal bar chart of a DSP fund's top holdings weighted by pct_of_nav.

    Args:
        fund_name:    Partial or full fund name — e.g. 'DSP Multi Asset', 'DSP Flexi'
        top_n:        Max holdings to show (default 15)
        as_of_month:  'YYYY-MM-DD' for a specific month; blank = latest available

    Example: plot_fund_holdings_chart("DSP Multi Asset", top_n=10)
    """
    try:
        from src.db.pool import query_df

        month_filter = (
            f"AND as_of_month = toDate('{as_of_month}')"
            if as_of_month
            else "AND as_of_month = (SELECT max(as_of_month) FROM market_data.mf_holdings FINAL WHERE fund_name ILIKE '%{fund_name}%')"
        )
        df = query_df(f"""
            SELECT security_name,
                   toFloat64(argMax(pct_of_nav,  imported_at)) AS weight,
                   toFloat64(argMax(market_value_cr, imported_at)) AS mv_cr
            FROM market_data.mf_holdings FINAL
            WHERE fund_name ILIKE '%{fund_name}%'
              {month_filter}
            GROUP BY security_name
            ORDER BY weight DESC
            LIMIT {top_n}
        """)
        if df.empty:
            return f"No holdings found for fund matching '{fund_name}'."

        names   = [n[:28] for n in df["security_name"].tolist()]
        weights = df["weight"].tolist()
        total   = sum(weights)

        plt = _plt()
        plt.clear_figure()
        plt.bar(names, weights, orientation="horizontal")
        plt.title(f"Top {top_n} Holdings — {fund_name}  (total shown: {total:.1f}% of NAV)")
        plt.xlabel("Weight (% of NAV)")
        plt.plot_size(_chart_width(), max(top_n + 5, 16))
        return _build(plt)
    except ImportError as exc:
        return str(exc)
    except Exception as exc:
        return f"Error plotting holdings for '{fund_name}': {exc}"


@tool
@clean_chart_tool_output
def plot_signal_breakdown(etf_symbols: str = "") -> str:
    """
    Grouped bar chart showing the signal pillar weights (macro, sentiment,
    valuation, flow, ML) for each ETF — reveals which pillar drives the score.

    Args:
        etf_symbols: Comma-separated ETF symbols to include; blank = all tracked ETFs

    Example: plot_signal_breakdown("GOLDBEES,SILVERBEES,NIFTYBEES")
    """
    try:
        from src.db.pool import query_df

        sym_filter = ""
        if etf_symbols.strip():
            syms = ", ".join(f"'{s.strip().upper()}'" for s in etf_symbols.split(","))
            sym_filter = f"AND etf_symbol IN ({syms})"

        df = query_df(f"""
            SELECT etf_symbol,
                   argMax(macro_score,      as_of) AS macro,
                   argMax(sentiment_score,  as_of) AS sentiment,
                   argMax(valuation_score,  as_of) AS valuation,
                   argMax(flow_score,       as_of) AS flow,
                   argMax(ml_score,         as_of) AS ml,
                   argMax(composite_score,  as_of) AS composite
            FROM market_data.signal_composite FINAL
            WHERE 1=1 {sym_filter}
            GROUP BY etf_symbol
            ORDER BY composite DESC
        """)
        if df.empty:
            return "No signal data found. Run `signals --save` first."

        symbols = df["etf_symbol"].tolist()
        pillars = ["macro", "sentiment", "valuation", "flow", "ml"]

        plt = _plt()
        plt.clear_figure()
        for pillar in pillars:
            scores = df[pillar].tolist()
            plt.bar(symbols, scores, label=pillar)
        plt.title("Signal Pillar Breakdown by ETF (latest)")
        plt.ylabel("Score")
        plt.plot_size(_chart_width(), _CHART_HEIGHT + 4)
        return _build(plt)
    except ImportError as exc:
        return str(exc)
    except Exception as exc:
        return f"Error plotting signal breakdown: {exc}"


@tool
@clean_chart_tool_output
def plot_garch_volatility_chart(symbol: str = "GOLDBEES", days: int = 90) -> str:
    """
    Plot the historical GARCH(1,1) annualised volatility trend for a symbol.
    Also overlays the vol-target line so you can see when GARCH exceeds the target.

    Args:
        symbol: NSE symbol tracked by the Risk Governor (default 'GOLDBEES')
        days:   Number of days of history to show (default 90)

    Example: plot_garch_volatility_chart("GOLDBEES", days=180)
    """
    try:
        import pandas as pd
        from src.db.pool import query_df

        _SPARSE_THRESHOLD = 30  # fewer unique pipeline-run days → use fresh fit

        df = query_df(f"""
            SELECT as_of,
                   toFloat64(argMax(garch_vol_pct,    created_at)) AS garch_vol,
                   argMax(regime,                     created_at)  AS regime
            FROM market_data.weight_checkpoints FINAL
            WHERE symbol = '{symbol.upper()}' AND method = 'rg'
              AND as_of >= today() - {days}
            GROUP BY as_of
            ORDER BY as_of ASC
        """)

        use_fresh = df.empty or len(df) < _SPARSE_THRESHOLD

        if use_fresh:
            # weight_checkpoints too sparse — run a fresh GARCH fit directly on
            # daily_prices so every trading day has an accurate conditional vol.
            try:
                price_df = query_df(f"""
                    SELECT trade_date,
                           toFloat64(argMax(open,   imported_at)) AS open,
                           toFloat64(argMax(high,   imported_at)) AS high,
                           toFloat64(argMax(low,    imported_at)) AS low,
                           toFloat64(argMax(close,  imported_at)) AS close,
                           toFloat64(argMax(volume, imported_at)) AS volume
                    FROM market_data.daily_prices FINAL
                    WHERE symbol = '{symbol.upper()}'
                    GROUP BY trade_date ORDER BY trade_date ASC
                """)
                if price_df.empty or len(price_df) < 60:
                    return (
                        f"No GARCH data found for {symbol} — need ≥60 rows in "
                        "daily_prices. Run: `import --category etfs`"
                    )
                from src.ml.anomaly import build_features, fit_garch_residuals
                price_df = build_features(price_df)
                price_df, _ = fit_garch_residuals(price_df)
                cutoff = pd.to_datetime(price_df["trade_date"].max()) - pd.Timedelta(days=days)
                price_df = price_df[pd.to_datetime(price_df["trade_date"]) >= cutoff]
                df = price_df[["trade_date", "garch_vol"]].dropna().copy()
                df.columns = ["as_of", "garch_vol"]
                df["regime"] = ""
                source_label = "fresh fit"
            except Exception as exc:
                if df.empty:
                    return f"No GARCH data for {symbol}: {exc}"
                use_fresh = False  # fall back to whatever checkpoints we have
                source_label = "checkpoints (sparse)"
        else:
            source_label = "checkpoints"

        # weight_checkpoints is sparse (only populated on pipeline-run days).
        # Forward-fill to every calendar day so the x-axis reflects real time spacing.
        df["as_of"] = pd.to_datetime(df["as_of"])
        df = df.set_index("as_of").sort_index()
        full_idx = pd.date_range(df.index.min(), df.index.max(), freq="D")
        df = df.reindex(full_idx).ffill().reset_index()
        df.columns = ["as_of", "garch_vol", "regime"]
        df = df.dropna(subset=["garch_vol"])

        dates   = df["as_of"].dt.strftime("%Y-%m-%d").tolist()
        vols    = df["garch_vol"].tolist()
        xs      = list(range(len(vols)))

        spark   = sparkline(vols)
        avg_vol = sum(vols) / len(vols)

        # Vol target is configurable per symbol — default 15%
        try:
            from src.tools.risk_governor import vol_target_for
            target = vol_target_for(symbol.upper())
        except Exception:
            target = 15.0

        # X-tick labels: show ~5 evenly-spaced date labels instead of integers
        n = len(dates)
        tick_positions = [int(i * (n - 1) / 4) for i in range(5)] if n >= 5 else list(range(n))
        tick_labels = [dates[i] for i in tick_positions]

        plt = _plt()
        plt.clear_figure()
        plt.plot(xs, vols,                  label=f"GARCH vol  (avg {avg_vol:.1f}%)")
        plt.plot(xs, [target] * len(vols),  label=f"Vol target ({target:.0f}%)")
        plt.title(f"GARCH Annualised Volatility — {symbol}  |  {spark}  [{source_label}]")
        plt.xlabel("Date")
        plt.ylabel("Volatility (%)")
        plt.xticks(tick_positions, tick_labels)
        plt.plot_size(_chart_width(), _CHART_HEIGHT)
        chart = _build(plt)
        table = _data_table(
            ["Date", "GARCH Vol (%)", "vs Target"],
            [[d, f"{v:.2f}", f"{v - target:+.2f}"]
             for d, v in zip(dates, vols)],
            title=f"{symbol} — GARCH Volatility Data",
        )
        return chart + table
    except ImportError as exc:
        return str(exc)
    except Exception as exc:
        return f"Error plotting GARCH volatility for {symbol}: {exc}"


@tool
@clean_chart_tool_output
def plot_weight_recommendations(method: str = "blended_50") -> str:
    """
    Horizontal bar chart of recommended position weights from weight_checkpoints.

    Args:
        method: 'blended_50' (default) | 'blended_30' | 'kelly' | 'rg'

    Example: plot_weight_recommendations("blended_50")
    """
    try:
        from src.db.pool import query_df
        df = query_df(f"""
            SELECT symbol,
                   toFloat64(argMax(recommended_weight, as_of)) AS weight,
                   argMax(regime, as_of)                         AS regime,
                   toFloat64(argMax(garch_vol_pct, as_of))       AS vol_pct
            FROM market_data.weight_checkpoints FINAL
            WHERE method = '{method}'
            GROUP BY symbol
            ORDER BY weight DESC
        """)
        if df.empty:
            return f"No weight data found for method='{method}'. Run the GOLDBEES pipeline first."

        symbols = df["symbol"].tolist()
        weights = df["weight"].tolist()
        regimes = df["regime"].tolist()
        labels  = [f"{s} [{r[:4]}]" for s, r in zip(symbols, regimes)]

        plt = _plt()
        plt.clear_figure()
        plt.bar(labels, weights, orientation="horizontal")
        plt.title(f"Recommended Position Weights — {method}")
        plt.xlabel("Weight (fraction of portfolio)")
        plt.plot_size(_chart_width(), max(len(symbols) + 5, 12))
        return _build(plt)
    except ImportError as exc:
        return str(exc)
    except Exception as exc:
        return f"Error plotting weight recommendations: {exc}"


@tool
@clean_chart_tool_output
def plot_intl_etf_performance() -> str:
    """
    Bar chart comparing 3-year Total Return % for all 6 international ETFs:
    MAFANG, HNGSNGBEES, MON100, MASPTOP50, MAHKTECH, MONQ50.

    Example: plot_intl_etf_performance()
    """
    try:
        from src.ui.intl_etf_analysis import compute_performance, load_data
        from src.db.pool import get_pool
        price_wide, _ = load_data(get_pool())
        perf = compute_performance(price_wide)

        ret_col = "total_return_pct" if "total_return_pct" in perf.columns else "3Y Ret %"
        sym_col = "_sym" if "_sym" in perf.columns else "ETF"
        symbols = perf[sym_col].tolist()
        returns = perf[ret_col].tolist()
        labels = [f"{s}\n({v:+.0f}%)" for s, v in zip(symbols, returns)]

        plt = _plt()
        plt.clear_figure()
        plt.bar(symbols, returns, label="3Y Total Return %")
        plt.title("International ETF — 3-Year Total Return (%)")
        plt.ylabel("Return %")
        plt.plot_size(_chart_width(), _CHART_HEIGHT)
        return _build(plt)
    except ImportError as exc:
        return str(exc)
    except Exception as exc:
        return f"Error plotting intl ETF performance: {exc}"


@tool
@clean_chart_tool_output
def plot_intl_etf_premium(symbol: str = "MAFANG", days: int = 180) -> str:
    """
    Line chart of the scarcity premium / discount trend for one international ETF.
    Shows when the ETF trades above/below NAV — negative premium = buy opportunity.

    Args:
        symbol: ETF symbol — MAFANG, HNGSNGBEES, MON100, MAHKTECH, MONQ50
        days:   Days of history (default 180)

    Example: plot_intl_etf_premium("MAFANG", days=365)
    """
    try:
        from src.ui.intl_etf_analysis import _premium_series, load_data
        from src.db.pool import get_pool
        price_wide, nav_wide = load_data(get_pool())
        prem = _premium_series(price_wide, nav_wide)

        sym = symbol.upper()
        if sym not in prem.columns:
            return f"{sym} not found. Available: {', '.join(prem.columns.tolist())}"

        series = prem[sym].dropna().tail(days)
        if series.empty:
            return f"No premium data for {sym}."

        vals   = series.tolist()
        spark  = sparkline([max(0, v + 5) for v in vals])
        avg    = sum(vals) / len(vals)
        latest = vals[-1]

        plt = _plt()
        plt.clear_figure()
        plt.plot(list(range(len(vals))), vals, label=f"{sym} premium")
        plt.plot(list(range(len(vals))), [0.0] * len(vals), label="Par (0%)")
        plt.title(f"{sym} Scarcity Premium  |  latest {latest:.2f}%  avg {avg:.2f}%  {spark}")
        plt.ylabel("Premium %")
        plt.plot_size(_chart_width(), _CHART_HEIGHT)
        return _build(plt)
    except ImportError as exc:
        return str(exc)
    except Exception as exc:
        return f"Error plotting premium for {symbol}: {exc}"


@tool
def plot_ou_premium_chart(symbol: str = "MAFANG", lookback: int = 365) -> str:
    """
    Full 3-panel OU mean-reversion chart for an international ETF premium series.

    Panel 1: Premium time-series + OU equilibrium μ, ±1σ/2σ bands, rolling μ,
             and buy (▲) / sell (▼) signal markers at ±1.5σ thresholds.
    Panel 2: Rolling 90-day OU half-life trend.
    Panel 3: 60-day forward expected premium path + P(revert to μ) annotations.

    Returns a text summary with all key OU stats and saves the chart to output/.

    Args:
        symbol:   ETF symbol — MAFANG, HNGSNGBEES, MON100, MASPTOP50, MAHKTECH, MONQ50
        lookback: Days of history to display (default 365)

    Examples:
        plot_ou_premium_chart("MAFANG", 365)
        plot_ou_premium_chart("HNGSNGBEES", 180)
    """
    try:
        import matplotlib
        matplotlib.use("Agg")
        import pandas as pd
        from src.db.pool import get_pool
        from src.scripts.chart_ou_premium import build_ou_chart

        sym = symbol.upper()
        pool = get_pool()
        df = pool.query_df(f"""
            SELECT
                toDate(snapshot_at) AS trade_date,
                argMax(premium_discount_pct, snapshot_at) AS premium,
                argMax(market_price, snapshot_at) AS price,
                argMax(inav, snapshot_at) AS inav
            FROM market_data.inav_snapshots
            WHERE symbol = '{sym}'
            GROUP BY trade_date
            ORDER BY trade_date ASC
        """)
        if df.empty:
            return f"No iNAV snapshot data for {sym}."

        df["trade_date"] = pd.to_datetime(df["trade_date"])
        df = df.set_index("trade_date").sort_index()
        if lookback and lookback < len(df):
            df = df.iloc[-lookback:]

        fig, summary = build_ou_chart(df, sym)
        if fig is None:
            return summary.get("error", f"OU fit failed for {sym}.")

        import os
        save_dir = os.path.join("output", "reports")
        os.makedirs(save_dir, exist_ok=True)
        save_path = os.path.join(save_dir, f"{sym}_ou_premium_strategy.png")
        fig.savefig(save_path, dpi=150, bbox_inches="tight", facecolor="white")
        import matplotlib.pyplot as plt
        plt.close(fig)

        current, mu, ou_std = summary["current"], summary["mu"], summary["ou_std"]
        sigma_from_mu = (current - mu) / ou_std
        signal = ("🟢 BUY" if current < summary["buy_threshold"]
                  else ("🔴 SELL/AVOID" if current > summary["sell_threshold"] else "⚪ HOLD"))

        lines = [
            f"📊 OU Premium Chart saved → {save_path}",
            f"",
            f"{'═'*52}",
            f"  {sym} OU Mean-Reversion Summary",
            f"{'═'*52}",
            f"  Current premium    : {current:.2f}%  ({summary['percentile']:.0f}th percentile of history)",
            f"  OU equilibrium μ   : {mu:.2f}%",
            f"  Gap (current − μ)  : {current - mu:+.2f}%  ({sigma_from_mu:+.1f}σ from equilibrium)",
            f"  OU speed θ         : {summary['theta']:.4f}",
            f"  Half-life          : {summary['half_life']:.1f} days",
            f"  Stationary σ∞      : {ou_std:.2f}%",
            f"  R² of AR(1) fit    : {summary['r2']:.3f}",
            f"  Buy threshold      : < {summary['buy_threshold']:.2f}%  (μ − 1.5σ∞)",
            f"  Sell threshold     : > {summary['sell_threshold']:.2f}%  (μ + 1.5σ∞)",
            f"",
            f"  E[premium in 5d]   : {summary['e_5d']:.2f}%",
            f"  E[premium in 10d]  : {summary['e_10d']:.2f}%",
            f"  E[Δprem 10d]       : {summary['rev_10d']:+.2f}%",
            f"  P(→μ in 5d)        : {summary['prob_5d']:.1%}",
            f"  P(→μ in 10d)       : {summary['prob_10d']:.1%}",
            f"  P(→μ in 20d)       : {summary['prob_20d']:.1%}",
            f"",
            f"  Signal: {signal}",
            f"{'═'*52}",
        ]
        return "\n".join(lines)

    except Exception as exc:
        return f"Error building OU premium chart for {symbol}: {exc}"


@tool
@clean_chart_tool_output
def plot_shareholding_bar(symbol: str) -> str:
    """
    Plot a horizontal stacked bar chart of the shareholding pattern for an Indian NSE stock.
    Shows Promoter, FII, DII, Government, and Public holding percentages as a 100% stacked bar.

    Args:
        symbol: NSE ticker — e.g. GRWRHITECH, RELIANCE, LICI

    Example: plot_shareholding_bar("GRWRHITECH")
    """
    try:
        from src.tools.earnings_scraper import get_shareholding_pattern

        data = get_shareholding_pattern.invoke({"symbol": symbol})
        if "error" in data:
            return f"Shareholding data unavailable for {symbol}: {data['error']}"

        categories = ["Promoter", "FII", "DII", "Government", "Public"]
        values = [
            float(data.get("promoter_pct", 0)),
            float(data.get("fii_pct", 0)),
            float(data.get("dii_pct", 0)),
            float(data.get("government_pct", 0)),
            float(data.get("public_pct", 0)),
        ]

        # Calculate width dynamically based on terminal width, ensuring it fits
        width = max(40, _chart_width() - 8)

        # Distribute blocks using the largest remainder method to sum exactly to width
        total_val = sum(values) or 100.0
        raw_blocks = [val / total_val * width for val in values]
        blocks = [int(x) for x in raw_blocks]
        remainder = width - sum(blocks)

        # Distribute the remainder based on fractional parts
        fractional = [(raw_blocks[i] - blocks[i], i) for i in range(len(values))]
        fractional.sort(reverse=True, key=lambda x: x[0])
        for i in range(remainder):
            idx = fractional[i][1]
            blocks[idx] += 1

        # Patterns for high compatibility without color
        patterns = {
            "Promoter": "█",
            "FII": "▓",
            "DII": "▒",
            "Government": "▞",
            "Public": "░",
        }

        # Sleek ANSI terminal colors (High-intensity / 256 support)
        colors = {
            "Promoter": "\033[94m",    # Light Blue
            "FII": "\033[96m",         # Light Cyan
            "DII": "\033[93m",         # Light Yellow
            "Government": "\033[95m",  # Light Magenta
            "Public": "\033[90m",      # Dark Gray
        }
        reset = "\033[0m"

        # Build the horizontal stacked bar
        bar_parts = []
        for cat, blk in zip(categories, blocks):
            if blk > 0:
                char = patterns[cat]
                color = colors[cat]
                bar_parts.append(f"{color}{char * blk}{reset}")
        bar_str = "".join(bar_parts)

        # Build legend and details
        deltas = {
            "Promoter": float(data.get("promoter_pct_qoq_delta", 0)),
            "FII": float(data.get("fii_pct_qoq_delta", 0)),
            "DII": float(data.get("dii_pct_qoq_delta", 0)),
            "Government": float(data.get("government_pct_qoq_delta", 0)),
            "Public": float(data.get("public_pct_qoq_delta", 0)),
        }

        quarter = data.get("latest_quarter", "latest quarter")
        title_line = f"\033[1m{symbol.upper()} — Shareholding Composition ({quarter})\033[0m"

        # Format the lines wrapped in clean borders
        border_top = f"┌{'─' * (width + 2)}┐"
        bar_line = f"│ {bar_str} │"
        border_bottom = f"└{'─' * (width + 2)}┘"

        legend_lines = []
        for cat, val in zip(categories, values):
            if val > 0 or deltas[cat] != 0:
                color = colors[cat]
                char = patterns[cat]
                delta = deltas[cat]
                delta_str = f"({delta:+.2f}% QoQ)" if delta != 0 else ""
                legend_lines.append(f"  {color}{char}{reset} {cat:<10}: {val:>6.2f}%   {delta_str}")

        lines = [
            title_line,
            border_top,
            bar_line,
            border_bottom,
            ""
        ] + legend_lines

        res = "\n".join(lines)
        save_active_chart("shareholding", res)
        return res
    except Exception as exc:
        return f"Error plotting shareholding for {symbol}: {exc}"


@tool
@clean_chart_tool_output
def plot_macd_chart(symbol: str, days: int = 180, category: str = "") -> str:
    """
    Plot a MACD (12, 26, 9) indicator chart for an NSE symbol from ClickHouse.

    Renders a stacked ASCII chart:
      • Top:    Price + EMA12 + EMA26
      • Bottom: MACD line + Signal line + Histogram bars

    Args:
        symbol:   NSE ticker — e.g. ADVENZYMES, RELIANCE, GOLDBEES
        days:     Trading-day lookback window (default 180; min 60)
        category: 'stocks' or 'etfs' — leave blank to auto-detect

    All EMA / MACD math is computed in pandas (never in the LLM).
    """
    try:
        import pandas as pd
        from src.db.pool import query_df

        sym = symbol.upper().strip()
        if sym.endswith(".NS") or sym.endswith(".BO"):
            sym = sym[:-3]
        if ":" in sym:
            parts = sym.split(":")
            if parts[0] in ("NSE", "BSE"):
                sym = parts[1]
            else:
                sym = parts[0]
        sym = sym.strip()

        lookback = max(60, days)
        cat_filter = f"AND category = '{category}'" if category else ""
        df = query_df(f"""
            SELECT trade_date,
                   toFloat64(argMax(close, imported_at)) AS close
            FROM market_data.daily_prices FINAL
            WHERE symbol = '{sym}' {cat_filter}
              AND trade_date >= today() - {lookback + 60}
            GROUP BY trade_date
            ORDER BY trade_date ASC
        """)

        if df.empty:
            # Auto-import then retry
            try:
                from src.tools.agent_tools import check_and_refresh_symbol_data
                status = check_and_refresh_symbol_data.invoke({"symbol": sym})
                logger.info("plot_macd_chart auto-import for %s: %s", sym, status)
                df = query_df(f"""
                    SELECT trade_date,
                           toFloat64(argMax(close, imported_at)) AS close
                    FROM market_data.daily_prices FINAL
                    WHERE symbol = '{sym}' {cat_filter}
                      AND trade_date >= today() - {lookback + 60}
                    GROUP BY trade_date
                    ORDER BY trade_date ASC
                """)
            except Exception as imp_exc:
                logger.debug("plot_macd_chart auto-import attempt failed for %s: %s", sym, imp_exc)

        # Fallback to Yahoo Finance price history if ClickHouse has insufficient data
        if df.empty or len(df) < 35:
            try:
                from src.tools.yahoo_finance import fetch_price_history
                exchange = "BSE" if symbol.upper().endswith(".BO") or ":BSE" in symbol.upper() else "NSE"
                if lookback <= 90:
                    yf_period = "6mo"
                elif lookback <= 180:
                    yf_period = "1y"
                elif lookback <= 365:
                    yf_period = "2y"
                else:
                    yf_period = "5y"
                hist = fetch_price_history(sym, exchange, period=yf_period)
                if hist:
                    yf_df = pd.DataFrame(hist)
                    if "close" in yf_df.columns and "date" in yf_df.columns:
                        yf_df = yf_df.rename(columns={"date": "trade_date"})
                        yf_df["trade_date"] = pd.to_datetime(yf_df["trade_date"])
                        yf_df["close"] = yf_df["close"].astype(float)
                        df = yf_df.sort_values("trade_date").reset_index(drop=True)
            except Exception as yf_exc:
                logger.debug("plot_macd_chart yfinance fallback failed for %s: %s", sym, yf_exc)

        if df.empty or len(df) < 35:
            return f"Insufficient price history for {sym} (need ≥35 bars for MACD, got {len(df)})."

        # Compute EMA-12, EMA-26, MACD, Signal, Histogram in pandas
        df["ema12"]  = df["close"].ewm(span=12, adjust=False).mean()
        df["ema26"]  = df["close"].ewm(span=26, adjust=False).mean()
        df["macd"]   = df["ema12"] - df["ema26"]
        df["signal"] = df["macd"].ewm(span=9, adjust=False).mean()
        df["hist"]   = df["macd"] - df["signal"]

        # Restrict display window
        view = df.tail(days).reset_index(drop=True)
        dates  = view["trade_date"].astype(str).tolist()
        xs     = list(range(len(view)))

        # Shared date ticks on both subplots
        n = len(dates)
        step = max(1, n // 5)
        tick_idx = list(range(0, n, step))
        if tick_idx and tick_idx[-1] != n - 1:
            tick_idx.append(n - 1)
        tick_lbl = [str(dates[i])[:10] for i in tick_idx]

        last_close = float(view["close"].iloc[-1])
        last_macd  = float(view["macd"].iloc[-1])
        last_sig   = float(view["signal"].iloc[-1])
        last_hist  = float(view["hist"].iloc[-1])
        cross = "BULL" if last_macd > last_sig else "BEAR"

        plt = _plt()
        plt.clear_figure()

        # Panel 1: Price + EMA12 + EMA26 (rendered independently)
        plt.plot_size(_chart_width(), 13)
        plt.plot(xs, view["close"].tolist(),  label="Close",  color="white")
        plt.plot(xs, view["ema12"].tolist(),  label="EMA-12", color="cyan")
        plt.plot(xs, view["ema26"].tolist(),  label="EMA-26", color="magenta")
        plt.title(f"{sym} — MACD(12,26,9) | Close ₹{last_close:.2f} | MACD {last_macd:+.2f} | Signal {last_sig:+.2f} | Hist {last_hist:+.2f} ({cross})")
        if sym.endswith("=F") or "=F" in sym:
            plt.ylabel("Price ($)")
        else:
            plt.ylabel("Price (₹)")
        plt.xticks(tick_idx, tick_lbl)
        price_panel = plt.build()
        plt.clear_figure()

        # Panel 2: MACD + Signal + Histogram (rendered independently)
        plt.plot_size(_chart_width(), 10)
        plt.plot(xs, view["macd"].tolist(),   label="MACD",   color="cyan")
        plt.plot(xs, view["signal"].tolist(), label="Signal", color="orange")
        plt.bar(xs, view["hist"].tolist(),    label="Hist",   color="green")
        plt.ylabel("MACD")
        plt.xticks(tick_idx, tick_lbl)
        macd_panel = plt.build()
        plt.clear_figure()

        chart_str = price_panel + "\n" + macd_panel
        save_active_chart("macd", chart_str)
        save_active_chart("plot_macd_chart", chart_str)

        # Compute technical analysis metrics
        ema12_val = float(view["ema12"].iloc[-1])
        ema26_val = float(view["ema26"].iloc[-1])

        # 1. Crossover state & history
        is_bullish = bool(last_macd > last_sig)
        crossover_label = "BULLISH (MACD Line > Signal Line)" if is_bullish else "BEARISH (MACD Line < Signal Line)"

        crossover_days_ago = None
        crossover_date = None
        for i in range(len(view) - 2, -1, -1):
            prev_bullish = bool(view["macd"].iloc[i] > view["signal"].iloc[i])
            if prev_bullish != is_bullish:
                crossover_days_ago = len(view) - 1 - i
                crossover_date = str(view["trade_date"].iloc[i+1])[:10]
                break

        # 2. Histogram Momentum Trend
        hist_trend = "strengthening"
        if len(view) >= 3:
            h1 = float(view["hist"].iloc[-2])
            h2 = float(view["hist"].iloc[-1])
            if abs(h2) > abs(h1):
                hist_trend = "strengthening / expanding"
            else:
                hist_trend = "weakening / contracting"

        # 3. Price vs EMAs Alignment
        if last_close > ema12_val and last_close > ema26_val:
            ema_alignment = "Bullish (Price > EMA-12 > EMA-26)" if ema12_val > ema26_val else "Price above both EMAs"
        elif last_close < ema12_val and last_close < ema26_val:
            ema_alignment = "Bearish (Price < EMA-12 < EMA-26)" if ema12_val < ema26_val else "Price below both EMAs"
        else:
            ema_alignment = "Mixed (Price between EMA-12 and EMA-26)"

        analysis_lines = [
            "",
            "════════════════════════════════════════════════════════════════════════════════",
            f"📊 MACD Technical Analysis: {sym}",
            "════════════════════════════════════════════════════════════════════════════════",
            f"• Close Price:      ₹{last_close:.2f}",
            f"• EMA-12 / EMA-26:  ₹{ema12_val:.2f} / ₹{ema26_val:.2f}  ({ema_alignment})",
            f"• MACD Line:        {last_macd:+.4f}",
            f"• Signal Line:      {last_sig:+.4f}",
            f"• Histogram (Diff): {last_hist:+.4f}  (Momentum is {hist_trend})",
        ]

        if crossover_days_ago is not None:
            crossover_idx = len(view) - crossover_days_ago
            price_at_crossover = float(view["close"].iloc[crossover_idx])
            if price_at_crossover > 0:
                pct_chg = ((last_close - price_at_crossover) / price_at_crossover) * 100
                analysis_lines.append(
                    f"• Crossover State:  {crossover_label} since {crossover_date} "
                    f"({crossover_days_ago} trading days ago; price change: {pct_chg:+.2f}%)"
                )
            else:
                analysis_lines.append(
                    f"• Crossover State:  {crossover_label} since {crossover_date} "
                    f"({crossover_days_ago} trading days ago)"
                )
        else:
            analysis_lines.append(f"• Crossover State:  {crossover_label} (No crossover in the last {len(view)} trading days)")

        analysis_lines.append("════════════════════════════════════════════════════════════════════════════════")

        return chart_str + "\n" + "\n".join(analysis_lines)
    except ImportError as exc:
        return str(exc)
    except Exception as exc:
        logger.error("plot_macd_chart failed for %s: %s", symbol, exc)
        return f"Error plotting MACD for {symbol}: {exc}"


CHART_TOOLS = [
    plot_price_chart,
    plot_fii_dii_chart,
    plot_dxy_chart,
    plot_signal_scores,
    plot_nav_chart,
    plot_multi_price_chart,
    plot_fund_holdings_chart,
    plot_signal_breakdown,
    plot_weight_recommendations,
    plot_garch_volatility_chart,
    plot_intl_etf_performance,
    plot_intl_etf_premium,
    plot_ou_premium_chart,
    plot_shareholding_bar,
    plot_macd_chart,
]
