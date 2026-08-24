"""
src/tools/market/equity.py
──────────────────────────
Internet search agent for anomaly-date news correlation on any NSE/BSE stock.

`search_anomaly_events` is the public tool:
  1. Runs the composite anomaly pipeline (GARCH + Isolation Forest + PELT) on
     the symbol's full OHLCV history to detect the same red-dot dates the
     price chart highlights.
  2. Filters flagged dates to the requested window.
  3. Dispatches parallel Google News searches (GNews) for each date —
     primary query is company-name + date-specific terms, with a fallback
     broadened query when the primary returns nothing.
  4. Returns a structured Markdown report: anomaly summary table followed by
     per-date news correlation sections.

This gives the LLM the same anomaly dates that the chart shows, then grounds
each date in actual published news rather than training-data guesses.
"""

from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import NamedTuple

import pandas as pd
from langchain_core.tools import tool

log = logging.getLogger(__name__)

# ── News cache ────────────────────────────────────────────────────────────────
# Keyed by "SYMBOL:date_str"; entries expire after 24 hours.
_NEWS_CACHE_TTL = 86_400  # seconds

class _CacheEntry(NamedTuple):
    result: str
    ts: float  # time.monotonic() when stored — reset on process restart

_news_cache: dict[str, _CacheEntry] = {}

# ── helpers ───────────────────────────────────────────────────────────────────

def _build_query(symbol: str, company_name: str, date_str: str,
                 regime: str, daily_ret: float) -> str:
    """
    Construct a targeted Google News query for an anomaly date.
    Regime and return direction inform the query so the search captures
    the right class of event (crash news vs. rally news vs. regulatory).
    """
    base = company_name if company_name else symbol
    # Direction hint helps GNews rank relevant headlines higher
    if daily_ret >= 3.0:
        direction = "rally surge gain"
    elif daily_ret <= -3.0:
        direction = "fall drop crash"
    else:
        direction = "news"

    # Regime-specific enrichment
    if "Flash Crash" in regime:
        suffix = "crash drop news India"
    elif "Regime Shift" in regime or "Volatile" in regime:
        suffix = "stock market India news"
    elif "Blow-off" in regime:
        suffix = "rally high volume India"
    elif "Crowded Long" in regime:
        suffix = "short squeeze India"
    else:
        suffix = f"{direction} India"

    return f"{base} {suffix}"


def _fallback_query(symbol: str, company_name: str) -> str:
    base = company_name if company_name else symbol
    return f"{base} NSE share price news India"


def _search_one_date(
    symbol: str,
    company_name: str,
    date_str: str,
    regime: str,
    daily_ret: float,
    max_results: int,
) -> tuple[str, str]:
    """
    Run multi-source news search for one anomaly date.

    Search cascade (stops at first hit):
      1. GNews — primary query (company + regime-context terms), exact date
      2. GNews — broadened fallback query, exact date
      3. GNews — ±1 day window (handles publication lag)
      4. NewsAPI — when available and date is within 30 days
      5. Corporate action heuristic — extreme returns (>20%) flag likely split/bonus/demerger

    Results are cached per (symbol, date_str) for 24 hours to avoid triggering
    Google News abuse detection on repeated calls for the same stock/day.

    Returns (date_str, markdown_block).
    """
    # ── Cache lookup ──────────────────────────────────────────────────────────
    cache_key = f"{symbol}:{date_str}"
    cached = _news_cache.get(cache_key)
    if cached and (time.monotonic() - cached.ts) < _NEWS_CACHE_TTL:
        log.debug("News cache hit for %s on %s", symbol, date_str)
        return date_str, cached.result

    def _store_and_return(md: str) -> tuple[str, str]:
        _news_cache[cache_key] = _CacheEntry(result=md, ts=time.monotonic())
        return date_str, md

    from datetime import date as _date
    from src.tools.news_search import search_financial_news

    today = _date.today()
    try:
        from dateutil import parser as _dp
        target_dt = _dp.parse(date_str).date()
    except Exception:
        target_dt = today

    days_ago = (today - target_dt).days

    # ── Heuristic: corporate action for extreme returns ───────────────────────
    if abs(daily_ret) >= 20.0:
        action_type = (
            "stock split, demerger, or bonus issue"
            if daily_ret < 0
            else "bonus, rights issue, or price adjustment"
        )
        return _store_and_return(
            f"> ⚙️ **Likely corporate action** ({daily_ret:+.1f}%): "
            f"a return of this magnitude almost always indicates a {action_type} "
            f"rather than a market-driven move. Verify via NSE corporate actions page."
        )

    # ── 0. Local ClickHouse news check (< 3ms) ───────────────────────────────
    try:
        from src.db.pool import query_df as _qdf
        news_df = _qdf(
            f"""
            SELECT title, sentiment, impact_tier, url
            FROM market_data.news_articles FINAL
            WHERE (symbols LIKE '%{symbol}%' OR title ILIKE '%{symbol}%' OR title ILIKE '%{company_name}%')
              AND toDate(fetched_at) BETWEEN toDate('{target_dt}') - 1 AND toDate('{target_dt}') + 1
            ORDER BY impact_tier ASC, fetched_at DESC LIMIT {max_results}
            """
        )
        if not news_df.empty:
            rows = []
            for _, nr in news_df.iterrows():
                rows.append(f"- **{nr['title']}** (Sentiment: `{nr['sentiment']}` | Tier: `{nr['impact_tier']}`)")
            return _store_and_return("\n".join(rows))
    except Exception:
        pass

    primary = _build_query(symbol, company_name, date_str, regime, daily_ret)

    # ── 1. GNews exact date, primary query ────────────────────────────────────
    try:
        result = search_financial_news.invoke(
            {"query": primary, "max_results": max_results, "target_date": date_str}
        )
        if "No news found" not in result:
            return _store_and_return(result)
    except Exception:
        result = "No news found"

    # ── 2. GNews exact date, broadened query ─────────────────────────────────
    try:
        fallback = _fallback_query(symbol, company_name)
        result = search_financial_news.invoke(
            {"query": fallback, "max_results": max_results, "target_date": date_str}
        )
        if "No news found" not in result:
            return _store_and_return(result)
    except Exception:
        pass

    # ── 3. GNews ±1 day window (publication lag) — only if recent ─────────────
    if days_ago <= 60:
        from datetime import timedelta
        for delta in (-1, 1):
            try:
                adj_date = (target_dt + timedelta(days=delta)).strftime("%Y-%m-%d")
                adj_result = search_financial_news.invoke(
                    {"query": primary, "max_results": max_results, "target_date": adj_date}
                )
                if "No news found" not in adj_result:
                    return _store_and_return(f"*(news from {adj_date})*\n{adj_result}")
            except Exception:
                pass

    # ── 4. NewsAPI — only viable within last 30 days ──────────────────────────
    if days_ago <= 30:
        try:
            from src.tools.newsapi_search import get_newsapi_stock_news
            na_result = get_newsapi_stock_news.invoke(
                {"symbol": symbol, "target_date": date_str}
            )
            if na_result and "No articles" not in str(na_result) and "error" not in str(na_result).lower():
                return _store_and_return(str(na_result))
        except Exception:
            pass

    return _store_and_return(result)  # final fallback (the "No news found" message)


def clear_news_cache() -> None:
    """Evict all entries; useful for testing or forced refresh."""
    _news_cache.clear()


# ── public tool ───────────────────────────────────────────────────────────────

@tool
def search_anomaly_events(
    symbol: str,
    days: int = 90,
    category: str = "",
    max_news_per_date: int = 3,
    z_threshold: float = 3.0,
    contamination: float = 0.03,
    volume_z_threshold: float | None = 4.0,
) -> str:
    """
    Internet search agent: detects price anomaly dates for any NSE/BSE stock
    using the composite pipeline (GARCH volatility normalisation + Isolation
    Forest + PELT change-point detection) — the same algorithm that places red
    dots on the price chart — then runs parallel Google News searches for each
    flagged date to explain the cause of each shock.

    Use when the user asks:
      - "What caused the anomalies on MSUMI's chart?"
      - "Search for news on MSUMI spike dates"
      - "Explain the red dots on RELIANCE chart"
      - "What happened on the flagged dates for HDFCBANK?"

    Args:
        symbol:               NSE/BSE trading symbol (e.g. MSUMI, RELIANCE, HDFCBANK)
        days:                 Look-back window in calendar days for anomaly filtering
                              (default 90). GARCH/PELT always fit on full history.
        category:             ClickHouse category filter (etfs / stocks / indices).
                              Leave blank to auto-detect.
        max_news_per_date:    Max news articles to surface per anomaly date (default 3).
        z_threshold:          |Final Z| cutoff for flagging anomalies (default 3.0).
        contamination:        Isolation Forest contamination fraction (default 0.03).
        volume_z_threshold:   |z_volume| cutoff for volume-only block-deal anomalies
                              (default 4.0). Set None to disable volume voting.
    """
    symbol_upper = symbol.strip().upper()

    # ── 1. Fetch full OHLCV ───────────────────────────────────────────────────
    df = pd.DataFrame()
    try:
        from src.db.pool import query_df
        params: dict = {"sym": symbol_upper}
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
    except Exception as exc:
        log.warning("ClickHouse OHLCV fetch failed for %s: %s", symbol_upper, exc)

    if df.empty:
        try:
            import yfinance as yf
            suffix = ".BO" if category == "bse" else ".NS"
            hist = yf.Ticker(f"{symbol_upper}{suffix}").history(period="2y")
            if not hist.empty:
                df = hist.reset_index()[["Date", "Open", "High", "Low", "Close", "Volume"]]
                df.columns = ["trade_date", "open", "high", "low", "close", "volume"]
                df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.tz_localize(None)
        except Exception as exc:
            return f"Error: Could not fetch price history for {symbol_upper}: {exc}"

    if df.empty:
        return f"No price history found for {symbol_upper}."

    df = df.sort_values("trade_date").reset_index(drop=True)
    df["volume"] = df["volume"].fillna(0.0)

    if len(df) < 60:
        return (
            f"Insufficient history for {symbol_upper} ({len(df)} rows). "
            f"Need ≥60 rows for the GARCH/PELT anomaly pipeline. "
            f"Run `import --category stocks` to backfill."
        )

    # ── 2. Load corporate actions from ClickHouse (for anomaly suppression) ──
    df_corp: pd.DataFrame | None = None
    try:
        from src.db.pool import query_df as _qdf
        _ca = _qdf(
            "SELECT ex_date, action_type, ratio, purpose "
            "FROM market_data.corporate_actions FINAL "
            "WHERE symbol = {sym:String}",
            parameters={"sym": symbol_upper},
        )
        if not _ca.empty:
            _ca["ex_date"] = pd.to_datetime(_ca["ex_date"])
            df_corp = _ca
    except Exception as exc:
        log.debug("Corporate actions not available for %s: %s", symbol_upper, exc)

    # ── 3. Run composite anomaly pipeline ────────────────────────────────────
    try:
        from src.ml.anomaly import run_composite_anomaly
        df_result, df_flagged, _ = run_composite_anomaly(
            df[["trade_date", "open", "high", "low", "close", "volume"]].copy(),
            contamination=contamination,
            z_threshold=z_threshold,
            df_corp_actions=df_corp,
            symbol=symbol_upper,
            category=category,
            volume_z_threshold=volume_z_threshold,
        )
    except Exception as exc:
        return f"Anomaly pipeline failed for {symbol_upper}: {exc}"

    # Filter flagged dates to the requested window — anchored to the DATA's last
    # trade_date, not datetime.now(). A now-anchor drops recent anomalies when
    # data is even slightly stale and makes results depend on the run date.
    df_flagged = df_flagged.copy()
    df_flagged["trade_date"] = pd.to_datetime(df_flagged["trade_date"])
    anchor = pd.to_datetime(df_result["trade_date"]).max()
    cutoff = anchor - pd.Timedelta(days=days)
    recent = df_flagged[df_flagged["trade_date"] >= cutoff].copy()

    # Also attach daily_return for query construction
    df_result["trade_date"] = pd.to_datetime(df_result["trade_date"])
    ret_map = df_result.set_index(df_result["trade_date"].dt.normalize())["daily_return"].to_dict()

    if recent.empty:
        return (
            f"No anomaly dates detected for {symbol_upper} in the last {days} days "
            f"(GARCH composite Final Z > {z_threshold}). The price action has been within normal bounds."
        )

    recent = recent.sort_values("trade_date", ascending=False)

    # ── 4. Resolve company name for better search queries ─────────────────────
    company_name = ""
    try:
        from src.tools.symbol_mapper import get_company_name
        company_name = get_company_name(symbol_upper) or ""
    except Exception:
        pass

    # ── 4. Build report header ────────────────────────────────────────────────
    lines: list[str] = []
    lines.append(f"## 🔍 Anomaly News Correlation: {symbol_upper}")
    if company_name:
        lines.append(f"**{company_name}** | last {days} days | {len(recent)} anomaly date(s) detected\n")
    else:
        lines.append(f"last {days} days | {len(recent)} anomaly date(s) detected\n")

    lines.append(
        "| Date | Close (₹) | Return | Regime | Final Z |"
    )
    lines.append("| :--- | :--- | :--- | :--- | :--- |")

    anomaly_rows: list[dict] = []
    for _, row in recent.iterrows():
        t = pd.Timestamp(row["trade_date"]).normalize()
        date_str = t.strftime("%Y-%m-%d")
        close = float(row["close"])
        ret_val = ret_map.get(t, row.get("daily_return", float("nan")))
        regime = str(row.get("regime", "—"))
        fz = row.get("final_z")
        fz_str = f"{fz:+.2f}" if fz is not None and not pd.isna(fz) else "N/A"
        ret_str = f"{ret_val:+.2f}%" if not pd.isna(ret_val) else "N/A"
        lines.append(f"| {date_str} | {close:.2f} | {ret_str} | {regime} | {fz_str} |")
        anomaly_rows.append({
            "date_str": date_str,
            "close": close,
            "ret": ret_val,
            "regime": regime,
            "fz_str": fz_str,
        })

    lines.append("")
    lines.append("---\n")
    lines.append("### 📰 Google News Search — Per Anomaly Date\n")

    # ── 5. Parallel Google News searches (top 4 dates by magnitude) ──────────
    # Pick top 4 dates with highest absolute Z-scores to prevent search throttling & timeouts
    search_candidates = sorted(
        anomaly_rows,
        key=lambda x: abs(float(x["fz_str"])) if x["fz_str"] != "N/A" else 0.0,
        reverse=True,
    )[:4]

    search_results: dict[str, str] = {}
    if search_candidates:
        with ThreadPoolExecutor(max_workers=min(len(search_candidates), 4)) as pool:
            futures = {
                pool.submit(
                    _search_one_date,
                    symbol_upper,
                    company_name,
                    r["date_str"],
                    r["regime"],
                    float(r["ret"]) if not pd.isna(r["ret"]) else 0.0,
                    max_news_per_date,
                ): r["date_str"]
                for r in search_candidates
            }
            for fut in as_completed(futures):
                try:
                    date_str, md_block = fut.result(timeout=15)
                    search_results[date_str] = md_block
                except Exception as exc:
                    date_str = futures[fut]
                    search_results[date_str] = f"Search timed out/failed: {exc}"

    # ── 6. Render per-date sections in chronological order (newest first) ─────
    for r in anomaly_rows:
        date_str = r["date_str"]
        ret_str = f"{r['ret']:+.2f}%" if not pd.isna(r["ret"]) else "N/A"
        lines.append(
            f"#### 📅 {date_str} | ₹{r['close']:.2f} | {ret_str} | {r['regime']} | Z={r['fz_str']}"
        )
        if date_str in search_results:
            news_block = search_results[date_str]
            if "No news found" in news_block:
                lines.append(
                    "> ⚠️ No news found on this exact date — event may be pre-positioned "
                    "(institutional block trade, policy leak, or off-market deal)."
                )
            else:
                lines.append(news_block)
        else:
            lines.append("> ℹ️ *Price shock recorded in summary table above.*")
        lines.append("")

    # ── 7. Historical precedents from Qdrant ─────────────────────────────────
    if recent.shape[0] > 0:
        try:
            from src.ml.anomaly import retrieve_similar_anomalies
            top_row = recent.iloc[0]
            top_regime = str(top_row.get("regime", ""))
            top_date = top_row["trade_date"]
            similar = retrieve_similar_anomalies(
                symbol=symbol_upper,
                regime=top_regime,
                trade_date=top_date,
                k=5,
                category=category,
            )
            if similar:
                lines.append("---\n")
                lines.append("### 🕰️ Historical Precedents (Qdrant Similarity Search)\n")
                lines.append(
                    f"Past anomalies most similar to **{top_regime}** on {symbol_upper}:\n"
                )
                lines.append("| Date | Symbol | Regime | Final Z | Return | Similarity |")
                lines.append("| :--- | :--- | :--- | :--- | :--- | :--- |")
                for s in similar:
                    lines.append(
                        f"| {s['trade_date']} | {s['symbol']} | {s['regime']} "
                        f"| {s['final_z']:+.2f} | {s['daily_return']:+.2f}% "
                        f"| {s['similarity']:.3f} |"
                    )
                lines.append("")
        except Exception as _qdrant_err:
            log.debug("Qdrant historical precedents unavailable: %s", _qdrant_err)

    return "\n".join(lines)


@tool
def get_corporate_actions(symbol: str) -> str:
    """
    Fetch NSE corporate actions (splits, bonuses, demergers, rights, dividends)
    for an NSE-listed stock, upsert them into ClickHouse, and return a Markdown
    summary table.

    Use when the user asks:
      - "What corporate actions has MSUMI had?"
      - "Did RELIANCE do a stock split?"
      - "Show me HDFCBANK bonus history"
      - Whenever a chart anomaly is suspected to be a corporate event

    The fetched data is stored in `market_data.corporate_actions` and is
    automatically used by `search_anomaly_events` and the price chart to
    suppress / label corporate action dates.
    """
    symbol_upper = symbol.strip().upper()

    # ── 1. Fetch from NSE ────────────────────────────────────────────────────
    from src.importer.fetchers.nse_corporate_actions_fetcher import (
        fetch_corporate_actions, PRICE_IMPACTING_TYPES,
    )
    rows = fetch_corporate_actions(symbol_upper)

    if not rows:
        return (
            f"No corporate actions found for **{symbol_upper}** on NSE.\n\n"
            f"This may mean: (1) the symbol is not listed on NSE equities, "
            f"(2) NSE returned no data, or (3) the network request failed."
        )

    # ── 2. Upsert into ClickHouse ────────────────────────────────────────────
    stored = 0
    try:
        import pandas as _pd
        from src.db.pool import acquire as _acquire
        from src.importer.clickhouse import _DDL_CORPORATE_ACTIONS
        df_ins = _pd.DataFrame(rows)
        with _acquire() as client:
            client.command(_DDL_CORPORATE_ACTIONS)  # ensure table exists (idempotent)
            client.insert_df("market_data.corporate_actions", df_ins)
        stored = len(df_ins)
    except Exception as exc:
        log.warning("Could not upsert corporate actions for %s: %s", symbol_upper, exc)

    # ── 3. Build Markdown table ───────────────────────────────────────────────
    lines = [
        f"## 🏦 Corporate Actions: {symbol_upper}",
        f"{len(rows)} event(s) fetched from NSE{f' · {stored} stored in ClickHouse' if stored else ''}.\n",
        "| Ex-Date | Action Type | Ratio / Amount | Purpose |",
        "| :--- | :--- | :--- | :--- |",
    ]
    for r in sorted(rows, key=lambda x: x["ex_date"], reverse=True):
        tag = "⚠️ suppresses anomaly" if r["action_type"] in PRICE_IMPACTING_TYPES else ""
        lines.append(
            f"| {r['ex_date']} | `{r['action_type']}` {tag} | {r['ratio'] or '—'} "
            f"| {r['purpose'][:80]} |"
        )

    return "\n".join(lines)


@tool
def find_similar_anomaly_events(
    symbol: str,
    regime: str = "",
    trade_date: str = "",
    k: int = 5,
    category: str = "",
    same_asset_only: bool = False,
) -> str:
    """
    Search Qdrant for historical anomaly events semantically similar to the
    given symbol and regime. Uses the market_anomalies vector collection built
    from all previous anomaly pipeline runs.

    Use when the user asks:
      - "What historical events looked like this flash crash on GOLDBEES?"
      - "Has RELIANCE had similar anomalies in the past?"
      - "Find historical precedents for this volatile breakout"
      - "What happened last time HDFCBANK had a high GARCH residual?"

    Args:
        symbol:          NSE symbol to search for (e.g. GOLDBEES, RELIANCE).
        regime:          Anomaly regime label to match (e.g. "⚡ Flash Crash / Black Swan (EXIT)").
                         Leave blank to match any regime for this symbol.
        trade_date:      ISO date string (YYYY-MM-DD) of the reference anomaly.
                         Leave blank to use today's date.
        k:               Number of similar events to return (default 5).
        category:        Asset category (etfs / stocks / indices). Leave blank for any.
        same_asset_only: If True, restrict results to the same symbol.
    """
    from datetime import date as _date
    from src.ml.anomaly import retrieve_similar_anomalies

    symbol_upper = symbol.strip().upper()

    ref_date: _date
    if trade_date:
        try:
            from datetime import datetime as _dt
            ref_date = _dt.strptime(trade_date.strip(), "%Y-%m-%d").date()
        except ValueError:
            return f"Invalid trade_date format '{trade_date}'. Use YYYY-MM-DD."
    else:
        ref_date = _date.today()

    regime_query = regime.strip() or f"{symbol_upper} anomaly"

    similar = retrieve_similar_anomalies(
        symbol=symbol_upper,
        regime=regime_query,
        trade_date=ref_date,
        k=k,
        category=category,
        same_asset_only=same_asset_only,
    )

    if not similar:
        return (
            f"No historical anomaly precedents found for **{symbol_upper}** "
            f"(regime: {regime_query or 'any'}). "
            "The Qdrant market_anomalies collection may be empty — "
            "run `search_anomaly_events` first to populate it."
        )

    lines = [
        f"## 🕰️ Historical Anomaly Precedents: {symbol_upper}",
        f"Regime query: **{regime_query}** | Top {len(similar)} matches\n",
        "| Date | Symbol | Category | Regime | Final Z | Return | Similarity |",
        "| :--- | :--- | :--- | :--- | :--- | :--- | :--- |",
    ]
    for s in similar:
        lines.append(
            f"| {s['trade_date']} | {s['symbol']} | {s['category']} | {s['regime']} "
            f"| {s['final_z']:+.2f} | {s['daily_return']:+.2f}% | {s['similarity']:.3f} |"
        )
    lines.append("")
    lines.append("**Context descriptions used for similarity:**")
    for s in similar:
        lines.append(f"- {s['text']}")

    return "\n".join(lines)


EQUITY_ANOMALY_TOOLS = [search_anomaly_events, get_corporate_actions, find_similar_anomaly_events]
