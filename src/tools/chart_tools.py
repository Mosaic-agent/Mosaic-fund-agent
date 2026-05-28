"""
src/tools/chart_tools.py
────────────────────────
Console chart tools using plotext — ASCII/Unicode charts rendered in the terminal.

All tools return a string (the chart) suitable for Rich panel display.
Falls back to a plain markdown table if plotext is not installed.
"""

from __future__ import annotations

import logging
from typing import Any

from langchain_core.tools import tool

logger = logging.getLogger(__name__)

_SPARKS = "▁▂▃▄▅▆▇█"
_CHART_HEIGHT = 20


def _chart_width() -> int:
    """Terminal width minus panel borders — capped at 100, minimum 60."""
    try:
        import shutil
        return max(60, min(100, shutil.get_terminal_size().columns - 8))
    except Exception:
        return 76


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
    return out


@tool
def plot_price_chart(symbol: str, days: int = 60, category: str = "") -> str:
    """
    Plot a price (close) trend chart for any NSE symbol from ClickHouse.
    Renders as an ASCII line chart directly in the terminal.

    Args:
        symbol:   NSE symbol — e.g. GOLDBEES, NIFTYBEES, RELIANCE, LICI
        days:     Number of trading days to show (default 60)
        category: 'etfs', 'stocks', 'indices', 'commodities' — leave blank to
                  auto-detect from the database (recommended)

    Example: plot_price_chart("GOLDBEES", days=90)
    """
    try:
        from src.db.pool import query_df
        cat_filter = f"AND category = '{category}'" if category else ""
        df = query_df(f"""
            SELECT trade_date,
                   toFloat64(argMax(close, imported_at)) AS close
            FROM market_data.daily_prices FINAL
            WHERE symbol = '{symbol.upper()}' {cat_filter}
              AND trade_date >= today() - {days}
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

        spark  = sparkline(prices)
        chg    = ((prices[-1] - prices[0]) / prices[0] * 100) if len(prices) >= 2 else 0

        plt = _plt()
        plt.clear_figure()
        plt.plot(list(range(len(prices))), prices, label=symbol)
        plt.title(f"{symbol} — {days}d price  |  {chg:+.1f}%  |  {spark}")
        plt.xlabel(f"{dates[0]} → {dates[-1]}")
        plt.ylabel("Price (₹)")
        plt.plot_size(_chart_width(), _CHART_HEIGHT)
        return _build(plt)
    except ImportError as exc:
        return str(exc)
    except Exception as exc:
        return f"Error plotting {symbol}: {exc}"


@tool
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
        plt.xlabel(f"{labels[0]} → {labels[-1]}")
        plt.ylabel("₹ Crore")
        plt.plot_size(_chart_width(), _CHART_HEIGHT)
        return _build(plt)
    except ImportError as exc:
        return str(exc)
    except Exception as exc:
        return f"Error plotting FII/DII flows: {exc}"


@tool
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
        return _build(plt)
    except ImportError as exc:
        return str(exc)
    except Exception as exc:
        return f"Error plotting signal scores: {exc}"


@tool
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
        plt.plot(list(range(len(navs))), navs, label=label)
        plt.title(f"NAV — {label}  |  {chg:+.1f}%  |  {spark}")
        plt.xlabel(f"{dates[0]} → {dates[-1]}")
        plt.ylabel("NAV (₹)")
        plt.plot_size(_chart_width(), _CHART_HEIGHT)
        return _build(plt)
    except ImportError as exc:
        return str(exc)
    except Exception as exc:
        return f"Error plotting NAV for '{symbol_or_scheme}': {exc}"


@tool
def plot_multi_price_chart(symbols: str, days: int = 60, category: str = "etfs") -> str:
    """
    Plot price trends for multiple NSE symbols on the same chart for comparison.

    Args:
        symbols:  Comma-separated NSE symbols — e.g. 'GOLDBEES,SILVERBEES,NIFTYBEES'
        days:     Trading days to show (default 60)
        category: 'etfs' or 'stocks'

    Example: plot_multi_price_chart("GOLDBEES,SILVERBEES", days=90)
    """
    try:
        from src.db.pool import query_df
        sym_list = [s.strip().upper() for s in symbols.split(",") if s.strip()]
        if not sym_list:
            return "No symbols provided."

        sym_sql = ", ".join(f"'{s}'" for s in sym_list)
        df = query_df(f"""
            SELECT symbol, trade_date,
                   toFloat64(argMax(close, imported_at)) AS close
            FROM market_data.daily_prices FINAL
            WHERE symbol IN ({sym_sql}) AND category = '{category}'
              AND trade_date >= today() - {days}
            GROUP BY symbol, trade_date
            ORDER BY symbol, trade_date ASC
        """)
        if df.empty:
            return f"No price data for symbols: {symbols}"

        plt = _plt()
        plt.clear_figure()
        for sym, grp in df.groupby("symbol"):
            prices = grp["close"].tolist()
            # Normalise to 100 for fair comparison
            base = prices[0] or 1
            norm = [p / base * 100 for p in prices]
            plt.plot(list(range(len(norm))), norm, label=sym)

        plt.title(f"Normalised price comparison (base=100)  —  last {days} days")
        plt.ylabel("Indexed price (base 100)")
        plt.plot_size(_chart_width(), _CHART_HEIGHT)
        return _build(plt)
    except ImportError as exc:
        return str(exc)
    except Exception as exc:
        return f"Error plotting {symbols}: {exc}"


@tool
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
        from src.db.pool import query_df
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
        if df.empty:
            return (
                f"No GARCH data found for {symbol} in weight_checkpoints. "
                "Run the GOLDBEES pipeline first: `run goldbees`"
            )

        dates   = df["as_of"].astype(str).tolist()
        vols    = df["garch_vol"].tolist()
        spark   = sparkline(vols)
        avg_vol = sum(vols) / len(vols)

        # Vol target is configurable per symbol — default 15%
        try:
            from src.tools.risk_governor import vol_target_for
            target = vol_target_for(symbol.upper())
        except Exception:
            target = 15.0

        plt = _plt()
        plt.clear_figure()
        plt.plot(list(range(len(vols))), vols,           label=f"GARCH vol  (avg {avg_vol:.1f}%)")
        plt.plot(list(range(len(vols))), [target] * len(vols), label=f"Vol target ({target:.0f}%)")
        plt.title(f"GARCH Annualised Volatility — {symbol}  |  {spark}")
        plt.xlabel(f"{dates[0]} → {dates[-1]}")
        plt.ylabel("Volatility (%)")
        plt.plot_size(_chart_width(), _CHART_HEIGHT)
        return _build(plt)
    except ImportError as exc:
        return str(exc)
    except Exception as exc:
        return f"Error plotting GARCH volatility for {symbol}: {exc}"


@tool
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


CHART_TOOLS = [
    plot_price_chart,
    plot_fii_dii_chart,
    plot_signal_scores,
    plot_nav_chart,
    plot_multi_price_chart,
    plot_fund_holdings_chart,
    plot_signal_breakdown,
    plot_weight_recommendations,
    plot_garch_volatility_chart,
    plot_intl_etf_performance,
    plot_intl_etf_premium,
]
