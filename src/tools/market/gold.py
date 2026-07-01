"""
src/tools/market/gold.py
────────────────────────
Gold/GARCH domain tools — anomaly explanation and risk governor analysis.
Both tools contain real business logic (not just subprocess wrappers) and
are gold/commodity-specific in their cross-asset feature loading.
"""

from __future__ import annotations

import logging

from langchain_core.tools import tool

from src.tools._subprocess import _run_cmd

logger = logging.getLogger(__name__)


@tool
def run_risk_governor_analysis() -> str:
    """
    Compute GARCH-based position sizing and volatility targeting decision for GOLDBEES.
    Use this when asked about GOLDBEES position sizing, GARCH volatility, risk targeting, or risk model output.
    """
    python_code = """
import sys; sys.path.insert(0,'.')
from src.tools.risk_governor import compute_position_weight, explain_decision, vol_target_for
from src.db.pool import get_pool
import pandas as pd, warnings
warnings.filterwarnings('ignore')
try:
    price_df = get_pool().query_df('''
        SELECT trade_date,
               toFloat64(argMax(open,   imported_at)) AS open,
               toFloat64(argMax(high,   imported_at)) AS high,
               toFloat64(argMax(low,    imported_at)) AS low,
               toFloat64(argMax(close,  imported_at)) AS close,
               toFloat64(argMax(volume, imported_at)) AS volume
        FROM market_data.daily_prices
        WHERE symbol='GOLDBEES' AND category='etfs'
        GROUP BY trade_date ORDER BY trade_date DESC LIMIT 300
    ''')
    price_df['trade_date'] = pd.to_datetime(price_df['trade_date'])
    price_df = price_df.sort_values('trade_date').reset_index(drop=True)
    from src.ml.anomaly import run_composite_anomaly
    df_r, _, _ = run_composite_anomaly(price_df, symbol='GOLDBEES', category='etfs', store=False)
    garch_vol = float(df_r['garch_vol'].dropna().iloc[-1])
    regime    = str(df_r['regime'].iloc[-1])
    latest    = float(df_r['close'].iloc[-1])
    ema50     = float(price_df['close'].ewm(span=50, adjust=False).mean().iloc[-1])
    below_ema = latest < ema50
except Exception as e:
    garch_vol = 16.5; regime = '✅ Normal'; below_ema = False
    print(f'Warning: using defaults ({e})')

vol_target = vol_target_for('GOLDBEES')
d = compute_position_weight(
    garch_annual_vol_pct=garch_vol,
    regime=regime,
    vol_target_pct=vol_target,
    price_below_ema50=below_ema,
)
print(explain_decision(d))
"""
    return _run_cmd(["-c", python_code])


@tool
def explain_price_anomalies(
    symbol: str,
    exchange: str | None = "NSE",
    days: int = 90,
    z_threshold: float = 3.0,
    contamination: float = 0.03,
) -> str:
    """
    Scan price history for GOLDBEES or any asset to identify return anomalies (daily return outlier shocks)
    in the last N days, automatically query historical news for those dates, and explain the causes.
    Always call `plot_price_chart` in parallel with this tool to visually display the price trend.
    Use this when the user asks to explain GOLDBEES price anomalies, chart spikes, or sudden drops.
    Default window is 90 days (~6-8 anomalies at 8% fire rate) — enough for one quarter of context.
    Use days=30 for recent-only, days=180 for seasonal review.
    """
    import pandas as pd
    from datetime import datetime
    from src.db.pool import query_df
    from src.tools.news_search import search_financial_news
    from src.utils.symbol_mapper import get_company_name

    symbol_upper = symbol.strip().upper()
    exchange_val = (exchange or "NSE").strip().upper()

    # 1. Fetch full OHLCV from ClickHouse (GARCH needs open/high/low/close/volume + ≥60 rows)
    df = pd.DataFrame()
    try:
        q = f"""
            SELECT trade_date,
                   toFloat64(argMax(open,   imported_at)) AS open,
                   toFloat64(argMax(high,   imported_at)) AS high,
                   toFloat64(argMax(low,    imported_at)) AS low,
                   toFloat64(argMax(close,  imported_at)) AS close,
                   toFloat64(argMax(volume, imported_at)) AS volume
            FROM market_data.daily_prices FINAL
            WHERE symbol = '{symbol_upper}'
            GROUP BY trade_date ORDER BY trade_date ASC
        """
        df = query_df(q)
    except Exception as e:
        logger.warning("ClickHouse query failed, falling back to yfinance: %s", e)

    if df.empty:
        try:
            import yfinance as yf
            if symbol_upper.endswith("=F") or "=F" in symbol_upper:
                ticker_sym = symbol_upper
            else:
                suffix = ".BO" if exchange_val == "BSE" else ".NS"
                ticker_sym = f"{symbol_upper}{suffix}"
            hist = yf.Ticker(ticker_sym).history(period="2y")
            if not hist.empty:
                df = hist.reset_index()[["Date", "Open", "High", "Low", "Close", "Volume"]]
                df.columns = ["trade_date", "open", "high", "low", "close", "volume"]
                df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.tz_localize(None)
        except Exception as e:
            return f"Error: Could not retrieve price history for {symbol_upper}: {e}"

    if df.empty:
        return f"Error: Price history for {symbol_upper} is empty."

    df = df.sort_values("trade_date").reset_index(drop=True)
    df["volume"] = df["volume"].fillna(0.0)
    df["daily_return"]   = df["close"].pct_change() * 100
    df["volatility_20d"] = df["daily_return"].rolling(20, min_periods=1).std()
    df["volume_ma20"]    = df["volume"].rolling(20, min_periods=1).mean()

    cutoff_date = pd.Timestamp(datetime.now().date()) - pd.Timedelta(days=days)

    if df[df["trade_date"] >= cutoff_date].empty:
        return f"No price data found in the last {days} days for {symbol_upper}."

    # 2. GARCH composite pipeline — full history, then filter to window
    composite_ok = False
    regime_map: dict = {}
    finalz_map: dict = {}
    threshold = z_threshold

    is_gold = "GOLD" in symbol_upper

    try:
        if len(df) < 60:
            raise ValueError(f"Only {len(df)} rows — need ≥60 for GARCH")

        df_cot: "pd.DataFrame | None" = None
        df_fx:  "pd.DataFrame | None" = None

        if is_gold:
            try:
                _cot = query_df(
                    "SELECT report_date, mm_net, open_interest FROM market_data.cot_gold"
                )
                if not _cot.empty:
                    _cot["report_date"] = pd.to_datetime(_cot["report_date"])
                    df_cot = _cot
            except Exception as _e:
                logger.warning("COT fetch failed (non-fatal): %s", _e)

        try:
            _fx = query_df(
                "SELECT symbol, trade_date, toFloat64(close) AS close "
                "FROM market_data.fx_rates FINAL WHERE symbol = 'USDINR'"
            )
            if not _fx.empty:
                _fx["trade_date"] = pd.to_datetime(_fx["trade_date"])
                df_fx = _fx
        except Exception as _e:
            logger.warning("FX fetch failed (non-fatal): %s", _e)

        from src.ml.anomaly import run_composite_anomaly
        df_result, df_flagged, _ = run_composite_anomaly(
            df[["trade_date", "open", "high", "low", "close", "volume"]].copy(),
            contamination=contamination,
            z_threshold=z_threshold,
            df_cot=df_cot,
            df_fx=df_fx,
            symbol=symbol_upper,
            store=False,
        )

        for _, r in df_result.iterrows():
            ts = pd.Timestamp(r["trade_date"]).normalize()
            regime_map[ts] = r.get("regime", "—")
            finalz_map[ts] = r.get("final_z")

        recent_flagged = df_flagged[
            pd.to_datetime(df_flagged["trade_date"]) >= cutoff_date
        ]["trade_date"].apply(lambda d: pd.Timestamp(d).normalize())
        anomalies = df[df["trade_date"].apply(
            lambda d: pd.Timestamp(d).normalize()
        ).isin(set(recent_flagged))].copy()
        composite_ok = True

    except Exception as _e:
        logger.warning("Composite anomaly failed, using naive threshold fallback: %s", _e)
        df_recent = df[df["trade_date"] >= cutoff_date].copy()
        std_ret = df_recent["daily_return"].std()
        threshold = max(2.0, 2.5 * std_ret) if not pd.isna(std_ret) else 2.0
        anomalies = df_recent[df_recent["daily_return"].abs() >= threshold].copy()

    if anomalies.empty:
        detection_note = (
            f"GARCH composite (Final Z > {z_threshold})" if composite_ok
            else f"naive threshold ({threshold:.2f}%)"
        )
        return (
            f"No price anomalies detected for {symbol_upper} in the last {days} days "
            f"({detection_note})."
        )

    anomalies = anomalies.sort_values("trade_date", ascending=False)

    # 3. Pre-load repo readers for forward ML/signal context
    repo = None
    try:
        from src.db.pool import get_pool
        from src.db.repository import MarketDataRepository
        repo = MarketDataRepository(get_pool())
    except Exception as _e:
        logger.warning("Could not initialise repository for forward context: %s", _e)

    # 4. Quarterly results (stocks only)
    is_stock = False
    quarterly_results = None
    try:
        from src.tools.inav_fetcher import is_etf
        is_stock = not is_etf(symbol_upper)
    except Exception as e:
        logger.warning("Could not determine if %s is an ETF: %s", symbol_upper, e)

    if is_stock:
        try:
            from src.tools.earnings_scraper import get_quarterly_results
            res = get_quarterly_results.invoke({"input_str": f"{symbol_upper}:{exchange_val}"})
            if res and "error" not in res:
                quarterly_results = res
        except Exception as e:
            logger.warning("Failed to fetch quarterly results for %s: %s", symbol_upper, e)

    def format_volume(vol: float) -> str:
        if vol >= 10_000_000:
            return f"{vol / 10_000_000:.2f} Cr"
        elif vol >= 100_000:
            return f"{vol / 100_000:.2f} L"
        elif vol >= 1_000:
            return f"{vol / 1_000:.1f}k"
        return f"{vol:,.0f}"

    detection_method = (
        f"GARCH composite (Final Z > {z_threshold})" if composite_ok
        else f"naive threshold (≥{threshold:.2f}%)"
    )
    output = []
    output.append(f"### 🔍 Price Anomaly & News Correlation Report: {symbol_upper}")
    output.append(
        f"Detected **{len(anomalies)}** anomaly dates in the last {days} days ({detection_method}):\n"
    )

    output.append(
        "| Date | Close Price (₹) | Daily Return (%) | 20d Volatility | Volume (Spike) | Regime | Final Z | News Search Query |"
    )
    output.append("| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |")

    anomaly_queries = []
    for _, row in anomalies.iterrows():
        t_date     = row["trade_date"]
        t_date_key = pd.Timestamp(t_date).normalize()
        date_str   = t_date_key.strftime("%Y-%m-%d")
        daily_ret  = row["daily_return"]
        close_px   = row["close"]
        vol_20d    = row["volatility_20d"]
        volume     = row["volume"]
        vol_ma     = row["volume_ma20"]
        vol_ratio  = volume / vol_ma if vol_ma > 0 else 1.0

        vol_spike_str = f"{format_volume(volume)} ({vol_ratio:.1f}x)"
        vol_20d_str   = f"{vol_20d:.2f}%" if not pd.isna(vol_20d) else "N/A"
        regime        = regime_map.get(t_date_key, "—")
        fz_val        = finalz_map.get(t_date_key)
        fz_str        = f"{fz_val:+.2f}" if fz_val is not None else "N/A"

        company_name = get_company_name(symbol_upper)
        if "GOLD" in symbol_upper:
            query = "gold price India custom duty import tax"
        elif "SILVER" in symbol_upper:
            query = "silver price India customs tax"
        elif symbol_upper in ["NIFTYBEES", "BANKBEES", "JUNIORBEES", "MID150BEES"]:
            query = "Nifty stock market India news"
        else:
            query = f"{company_name or symbol_upper} stock news"

        output.append(
            f"| {date_str} | {close_px:.2f} | {daily_ret:+.2f}% | {vol_20d_str} | {vol_spike_str} | {regime} | {fz_str} | `{query}` |"
        )
        anomaly_queries.append(
            (date_str, close_px, daily_ret, vol_20d_str, vol_spike_str, regime, fz_str, query)
        )

    output.append("\n" + "─" * 40 + "\n")
    output.append("### 📰 Detailed Date-by-Date News Correlation:\n")

    # ── Parallelise all network I/O: news searches + repo reads fire concurrently ──
    from concurrent.futures import ThreadPoolExecutor, as_completed

    def _fetch_date_context(item):
        date_str, close_px, daily_ret, vol_20d_str, vol_spike_str, regime, fz_str, query = item
        result = {"item": item, "news": None, "ml_pred": None, "sig": None}
        try:
            news_output = search_financial_news.invoke(
                {"query": query, "max_results": 3, "target_date": date_str}
            )
            if "No news found" in news_output:
                news_output = search_financial_news.invoke(
                    {"query": f"{symbol_upper} share price news", "max_results": 3, "target_date": date_str}
                )
            result["news"] = news_output
        except Exception as exc:
            result["news"] = f"❌ News search failed: {exc}"
        if repo is not None:
            try:
                result["ml_pred"] = repo.ml_prediction_asof(date_str)
                result["sig"]     = repo.signal_composite_asof(symbol_upper, date_str)
            except Exception:
                pass
        return result

    # Fire all date lookups in parallel (capped at 6 workers to avoid rate-limiting)
    parallel_results: dict[str, dict] = {}
    with ThreadPoolExecutor(max_workers=min(6, len(anomaly_queries))) as pool:
        futures = {pool.submit(_fetch_date_context, item): item[0] for item in anomaly_queries}
        for fut in as_completed(futures):
            res = fut.result()
            parallel_results[res["item"][0]] = res

    for date_str, close_px, daily_ret, vol_20d_str, vol_spike_str, regime, fz_str, query in anomaly_queries:
        ctx        = parallel_results.get(date_str, {})
        news_output = ctx.get("news", "")
        ml_pred     = ctx.get("ml_pred")
        sig         = ctx.get("sig")

        output.append(
            f"#### 📅 Date: **{date_str}** | Close: ₹{close_px:.2f} | Daily Return: **{daily_ret:+.2f}%**"
        )
        if regime != "—":
            output.append(f"- **GARCH Regime:** {regime}")
        if fz_str != "N/A":
            output.append(f"- **Composite Final Z:** {fz_str}")
        output.append(f"- **20-Day Rolling Daily Volatility:** {vol_20d_str}")
        output.append(f"- **Trading Volume:** {vol_spike_str}")
        output.append(news_output)

        if abs(daily_ret) >= 3.0 and news_output and "neutral" in news_output.lower():
            output.append(
                "\n> ⚠️ **Divergence signal:** Neutral news sentiment on a high-magnitude move — "
                "possible policy surprise or pre-positioning before public announcement."
            )

        if quarterly_results:
            news_lower = (news_output or "").lower()
            earnings_keywords = ["result", "earning", "profit", "revenue", "q1", "q2", "q3", "q4", "sales"]
            if any(kw in news_lower for kw in earnings_keywords):
                output.append("\n**📊 Correlated Quarterly Financial Results:**")
                output.append(f"- **Reporting Period:** {quarterly_results.get('period', 'N/A')}")

                rev_yoy = quarterly_results.get("revenue_yoy_pct")
                rev_cr  = quarterly_results.get("revenue_cr")
                rev_str = f"₹{rev_cr:.2f} Cr" if isinstance(rev_cr, (int, float)) else str(rev_cr)
                output.append(
                    f"- **Revenue:** {rev_str} ({rev_yoy:+.2f}% YoY)"
                    if isinstance(rev_yoy, (int, float))
                    else f"- **Revenue:** {rev_str} ({rev_yoy} YoY)"
                )

                prof_yoy = quarterly_results.get("profit_yoy_pct")
                prof_cr  = quarterly_results.get("net_profit_cr")
                prof_str = f"₹{prof_cr:.2f} Cr" if isinstance(prof_cr, (int, float)) else str(prof_cr)
                output.append(
                    f"- **Net Profit:** {prof_str} ({prof_yoy:+.2f}% YoY)"
                    if isinstance(prof_yoy, (int, float))
                    else f"- **Net Profit:** {prof_str} ({prof_yoy} YoY)"
                )

                eps     = quarterly_results.get("eps")
                eps_yoy = quarterly_results.get("eps_yoy_pct")
                eps_str = f"₹{eps:.2f}" if isinstance(eps, (int, float)) else str(eps)
                output.append(
                    f"- **EPS:** {eps_str} ({eps_yoy:+.2f}% YoY)"
                    if isinstance(eps_yoy, (int, float))
                    else f"- **EPS:** {eps_str} ({eps_yoy} YoY)"
                )

                if quarterly_results.get("guidance"):
                    output.append(f"- **Guidance:** {quarterly_results.get('guidance')}")
                output.append(
                    f"- **Source:** [Screener/Yahoo]({quarterly_results.get('source_url', '#')})"
                )

        # Forward ML/signal context (already fetched in parallel above)
        if ml_pred or sig:
            output.append("\n**📡 What the models said on this date:**")
        if ml_pred:
            signal_label = ml_pred.get("regime_signal", "N/A")
            prob         = ml_pred.get("prob_up", 0.0)
            exp_ret      = ml_pred.get("expected_return_pct", 0.0)
            direction    = (
                "mean-reversion (bearish 5d expectation)"
                if exp_ret < 0
                else "continuation (bullish 5d expectation)"
            )
            output.append(
                f"- **ML (5d forecast):** `{signal_label}` | prob_up={prob:.0%} | "
                f"expected_return={exp_ret:+.2f}% → {direction}"
            )
        if sig:
            action    = sig.get("action", "N/A")
            score     = sig.get("composite_score", 0.0)
            anom_flag = sig.get("anomaly_flag", "")
            verdict   = (
                "✅ Signal confirmed shock"
                if action in ("BUY", "WATCH_LONG")
                else "⚠️ Signal contradicted by shock"
            )
            output.append(
                f"- **Composite signal:** `{action}` (score={score:.1f}, anomaly_flag={anom_flag}) — {verdict}"
            )
        output.append("\n" + "─" * 40 + "\n")

    # COMEX chart (gold/silver only)
    comex_symbol = None
    if "GOLD" in symbol_upper:
        comex_symbol = "GC=F"
    elif "SILVER" in symbol_upper:
        comex_symbol = "SI=F"

    if comex_symbol:
        try:
            from src.tools.chart_tools import plot_price_chart
            comex_chart = plot_price_chart.invoke({"symbol": comex_symbol, "days": days})
            output.append("### 📈 Correlated COMEX Futures Price Chart:")
            output.append(
                f"To assist in visual correlation, here is the price chart for the underlying "
                f"COMEX futures contract (**{comex_symbol}**):\n"
            )
            output.append("```text")
            output.append(comex_chart)
            output.append("```")
            output.append("\n" + "─" * 40 + "\n")
        except Exception as e:
            logger.warning("Failed to append COMEX chart for %s: %s", symbol_upper, e)

    # GARCH vol chart (from weight_checkpoints — requires pipeline to have run)
    try:
        from src.tools.chart_tools import plot_garch_volatility_chart
        garch_chart = plot_garch_volatility_chart.invoke({"symbol": symbol_upper, "days": days})
        if "No GARCH data found" not in garch_chart:
            output.append("### 📊 GARCH Annualised Volatility Chart:")
            output.append(
                f"Here is the GARCH(1,1) annualised volatility trend for **{symbol_upper}** "
                f"over the last {days} days:\n"
            )
            output.append("```text")
            output.append(garch_chart)
            output.append("```")
            output.append("\n" + "─" * 40 + "\n")
    except Exception as e:
        logger.warning("Failed to append GARCH volatility chart for %s: %s", symbol_upper, e)

    return "\n".join(output)


GOLD_TOOLS = [
    run_risk_governor_analysis,
    explain_price_anomalies,
]
