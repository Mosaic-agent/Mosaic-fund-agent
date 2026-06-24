"""
src/tools/skills_tools.py
─────────────────────────
Core skill tools for the Mosaic-agent ReAct agents.

What lives here: general-purpose tools that don't fit a tighter domain —
ClickHouse query, symbol import, iNAV lookup, premium alerts, deep-dive,
and the shared subprocess helpers (_run_cmd, _run_cmd_streaming).

Shell-command runners (run_goldbees_pipeline, run_macro_scanner, …) →
    src/tools/runners.py

Gold/GARCH domain tools (explain_price_anomalies, run_risk_governor_analysis) →
    src/tools/market/gold.py

Both modules are re-exported here for backward compatibility.
"""

from __future__ import annotations

import os
import sys
import logging
import re
from langchain_core.tools import tool

from src.tools._subprocess import (  # shared helpers — no circular dep
    PROJECT_ROOT,
    _run_cmd,
    _run_cmd_streaming,
    _clean_terminal_output,
)

logger = logging.getLogger(__name__)

# ── Runner tools (shell-command wrappers) — defined in runners.py ─────────────
# Re-exported here so existing `from src.tools.skills_tools import X` calls
# keep working without change.
from src.tools.runners import (  # noqa: E402
    run_goldbees_pipeline,
    run_daily_signal_composite,
    run_macro_scanner,
    run_etf_news_sentiment,
    run_dsp_multi_asset_importer,
    run_nippon_importer,
    run_icici_importer,
    run_all_multi_asset_importers,
    run_multi_asset_holdings_mom_yoy,
    run_multi_asset_consensus,
    run_data_engineering_importer,
    run_comex_analysis,
    run_whale_tracker,
    run_dsp_multi_asset_comparison,
    run_fund_mom_returns,
    run_market_indicators,
)


def import_symbol_data_impl(
    symbol: str,
    days: int = 365,
    start_date: str = "",
    end_date: str = "",
    data_source: str = "shoonya",
) -> str:
    """
    Core implementation to import price history for a specific symbol.
    """
    from datetime import date, timedelta, datetime
    from src.importer.source_preference import normalize_data_source

    sym = symbol.strip().upper()
    selected_source = normalize_data_source(data_source)
    if not selected_source:
        return "Invalid data_source. Use shoonya, nse, or yfinance."
    if start_date:
        try:
            from_date = datetime.strptime(start_date, "%Y-%m-%d").date()
        except ValueError:
            return f"Invalid start_date format: {start_date}. Expected YYYY-MM-DD."
        if end_date:
            try:
                to_date = datetime.strptime(end_date, "%Y-%m-%d").date()
            except ValueError:
                return f"Invalid end_date format: {end_date}. Expected YYYY-MM-DD."
        else:
            to_date = date.today()
        days_desc = f"{start_date} to {end_date or 'today'}"
    else:
        to_date = date.today()
        from_date = to_date - timedelta(days=max(days, 1))
        days_desc = f"{days} days"

    # Build lookup: nse_symbol → (yahoo_ticker, category)
    try:
        from src.importer.registry import ETFS, STOCKS, COMMODITIES, INDICES
        _lookup: dict[str, tuple[str, str]] = {}
        for nse, yahoo in ETFS:
            _lookup[nse] = (yahoo, "etfs")
        for nse, yahoo in STOCKS:
            _lookup[nse] = (yahoo, "stocks")
        for nse, yahoo in COMMODITIES:
            _lookup[nse] = (yahoo, "commodities")
        for nse, yahoo in INDICES:
            _lookup[nse] = (yahoo, "indices")
    except Exception as exc:
        return f"Registry load error: {exc}"

    if sym not in _lookup:
        # Fall back to Yahoo Finance search via resolve_company_info
        sys.stdout.write(f"  {sym} not in static registry — searching Yahoo Finance...\n")
        sys.stdout.flush()
        try:
            from src.tools.company_resolver import resolve_company_info
            info = resolve_company_info(sym, auto_import=False)
            yf_sym = info.get("yf_symbol", "")
            market = info.get("market", "")
            source = info.get("source", "")
            if yf_sym and market == "India" and source != "fallback_unverified":
                yahoo_ticker = yf_sym
                category = "stocks"
                nse_sym = info.get("nse_symbol") or re.sub(r"\.(NS|BO)$", "", yf_sym, flags=re.I)
                sym = nse_sym  # use the resolved NSE symbol going forward
                sys.stdout.write(
                    f"  Resolved → {sym} ({yahoo_ticker}) on {info.get('exchange', 'NSE')}\n"
                )
                sys.stdout.flush()
            elif source == "fallback_unverified":
                return (
                    f"SYMBOL_NOT_FOUND: '{sym}' could not be verified on Yahoo Finance. "
                    f"Possible causes: typo, delisted stock, or symbol not yet on NSE. "
                    f"Try the exact NSE symbol (e.g. 'GODIGIT' for Go Digit, 'LICI' for LIC)."
                )
            else:
                cname = info.get("company_name", "unknown")
                return (
                    f"UNKNOWN_SYMBOL: Yahoo search found '{cname}' ({market}) for '{sym}', "
                    f"not an Indian listing. Try the exact NSE symbol (e.g. LICICORP, HNGSNGBEES)."
                )
        except Exception as exc:
            return (
                f"UNKNOWN_SYMBOL: '{sym}' not in registry and Yahoo search failed: {exc}.\n"
                f"Try the exact NSE symbol directly."
            )
    else:
        yahoo_ticker, category = _lookup[sym]

    # ── Watermark-aware delta sync ─────────────────────────────────────────
    # When the caller didn't provide an explicit start_date, look up the last
    # successful import watermark and start from the day after — so only the
    # missing days are fetched instead of blindly re-fetching `days` history.
    watermark_source = selected_source if category in ("stocks", "etfs") else "yfinance"
    if not start_date:
        try:
            from src.db.pool import query_df as _qdf
            wm = _qdf(
                f"SELECT last_date FROM market_data.import_watermarks FINAL "
                f"WHERE source = '{watermark_source}' AND symbol = '{sym}' "
                f"ORDER BY last_date DESC LIMIT 1"
            )
            if not wm.empty:
                last_date = wm.iloc[0]["last_date"]
                # Advance one trading day past the watermark
                delta_from = last_date + timedelta(days=1)
                if delta_from >= to_date:
                    return (
                        f"{sym} is already up to date. "
                        f"Last import: {last_date} — nothing to fetch."
                    )
                days_desc = f"delta {delta_from} → {to_date} (last import: {last_date})"
                from_date = delta_from
                sys.stdout.write(
                    f"  Watermark found: {last_date} → importing delta from {from_date}\n"
                )
                sys.stdout.flush()
        except Exception as _wm_exc:
            sys.stdout.write(f"  No watermark found ({_wm_exc}) — using {days_desc}\n")
            sys.stdout.flush()

    sys.stdout.write(
        f"  Importing {sym} ({yahoo_ticker}) | {from_date} → {to_date} ({days_desc})\n"
    )
    sys.stdout.flush()

    try:
        if category in ("stocks", "etfs"):
            if selected_source == "nse":
                from src.importer.fetchers.adapters import NSElibFetcher
                rows = NSElibFetcher(category, [(sym, yahoo_ticker)]).fetch(from_date, to_date)
            elif selected_source == "yfinance":
                from src.importer.fetchers.yfinance_fetcher import fetch_ohlcv
                rows = fetch_ohlcv([(sym, yahoo_ticker)], category, from_date, to_date)
            else:
                from src.importer.fetchers.adapters import ShoonyaFetcher
                rows = ShoonyaFetcher(category, [(sym, yahoo_ticker)]).fetch(from_date, to_date)
        else:
            from src.importer.fetchers.yfinance_fetcher import fetch_ohlcv
            rows = fetch_ohlcv([(sym, yahoo_ticker)], category, from_date, to_date)
    except Exception as exc:
        return f"Fetch error for {sym}: {exc}"

    if not rows:
        return (
            f"No data returned for {sym} ({yahoo_ticker}) over {from_date} → {to_date}.\n"
            f"Yahoo Finance may not have data for this range, or the symbol is delisted."
        )

    sys.stdout.write(f"  Fetched {len(rows)} rows — inserting into ClickHouse...\n")
    sys.stdout.flush()

    try:
        from src.importer.clickhouse import ClickHouseImporter

        ch = ClickHouseImporter()   # uses pool singleton — no explicit params needed
        try:
            ch.ensure_schema()
            n = ch.insert_prices(rows)
            max_date = max(r["trade_date"] for r in rows)
            ch.set_watermark(watermark_source, sym, max_date)
            sys.stdout.write(f"  ✓ {n} rows inserted. Last trade_date: {max_date}\n")
            sys.stdout.flush()
            return f"Imported {sym}: {n} rows inserted, {from_date} → {max_date}."
        finally:
            ch.close()
    except Exception as exc:
        return f"ClickHouse insert error for {sym}: {exc}"


@tool
def import_symbol_data(
    symbol: str,
    days: int = 365,
    start_date: str = "",
    end_date: str = "",
    data_source: str = "",
) -> str:
    """
    Import price history for a SPECIFIC NSE symbol. This is the PREFERRED tool when the user
    names a particular stock/ETF to import — e.g. "import ADVENZYMES", "refresh GOLDBEES data",
    "update RELIANCE prices". Much faster than bulk import since it fetches only one symbol.

    For bulk category imports (all ETFs, all stocks), use run_data_engineering_importer instead.

    Args:
        symbol:     NSE symbol to import (e.g. "ADVENZYMES", "GOLDBEES", "RELIANCE", "NIFTY50").
                    Uppercased automatically. Covers ETFs, stocks, commodities, and indices.
        days:       Calendar days of history back from today. Ignored if start_date is set.
        start_date: Optional start date in YYYY-MM-DD format (e.g. '2019-01-01')
        end_date:   Optional end date in YYYY-MM-DD format (e.g. '2019-12-31')
        data_source: Required for stocks/ETFs. Ask the user to choose:
                     1=Shoonya, 2=NSE, or 3=yfinance.
    """
    from src.importer.source_preference import resolve_data_source

    try:
        data_source, _ = resolve_data_source(data_source)
    except ValueError as exc:
        return f"Invalid data source: {exc}"
    if not data_source:
        return (
            "DATA_SOURCE_REQUIRED: Ask the user which data source to use before importing:\n"
            "1. Shoonya\n2. NSE\n3. yfinance"
        )
    return import_symbol_data_impl(symbol, days, start_date, end_date, data_source)


# ── Gold/GARCH domain tools — defined in market/gold.py ──────────────────────
from src.tools.market.gold import (  # noqa: E402
    run_risk_governor_analysis,
    explain_price_anomalies,
)

# ── Macro context tools — defined in market_context.py ───────────────────────
from src.tools.market_context import get_dxy_context  # noqa: E402


@tool
def query_clickhouse_db(sql_query: str) -> str:
    """
    Execute a read-only SQL query on the ClickHouse 'market_data' database.
    Use this to run custom SELECT, SHOW, DESCRIBE, or EXPLAIN queries to retrieve historical prices,
    mutual fund holdings, flows, predictions, watermarks, fx rates, or US stock deep-dive data.
    
    Rules:
      - Always query tables using the 'FINAL' modifier to deduplicate rows (e.g. `market_data.mf_holdings FINAL`).
      - Only SELECT, SHOW, DESCRIBE, EXPLAIN, and WITH queries are permitted.
      - The database is 'market_data'. Available tables include:
        * Indian Markets: daily_prices, mf_nav, mf_holdings, fii_dii_flows, fii_dii_fno_daily, signal_composite, ml_predictions, weight_checkpoints, inav_snapshots, cot_gold, fx_rates, macro_indicators, news_articles, import_watermarks, corporate_actions, stock_earnings, stock_insider_trades, stock_valuation
        * US Stocks Deep-Dive: deepdive_exec_comp, deepdive_filings, deepdive_financials, deepdive_headcount, deepdive_jobs, deepdive_prices, deepdive_reports, deepdive_segments, deepdive_valuation, deepdive_watermarks
    """
    clean_query = sql_query.strip()
    first_word = clean_query.split()[0].upper() if clean_query else ""
    if first_word not in ("SELECT", "SHOW", "DESCRIBE", "EXPLAIN", "WITH"):
        return "Error: Only read-only queries (SELECT, SHOW, DESCRIBE, EXPLAIN, WITH) are permitted."

    # Auto-fix common SQLite/PostgreSQL patterns before hitting ClickHouse
    try:
        from src.tools.db_tools import _auto_fix_sql
        clean_query, _changes = _auto_fix_sql(clean_query)
    except Exception:
        pass

    # Auto-import missing symbols if found in the SQL query
    try:
        import re
        symbols_found = set()
        # Pattern 1: symbol = 'XYZ'
        for m in re.finditer(r"\bsymbol\s*=\s*['\"]([a-zA-Z0-9&_\.-]+)['\"]", clean_query, re.I):
            symbols_found.add(m.group(1).upper())
        # Pattern 2: symbol IN ('XYZ', 'ABC')
        for m in re.finditer(r"\bsymbol\s+in\s*\(([^)]+)\)", clean_query, re.I):
            list_content = m.group(1)
            for item in re.finditer(r"['\"]([a-zA-Z0-9&_\.-]+)['\"]", list_content):
                symbols_found.add(item.group(1).upper())

        if symbols_found:
            from src.db.pool import query_df as _internal_qdf
            for sym in sorted(symbols_found):
                check_df = _internal_qdf(f"SELECT count() as cnt FROM market_data.daily_prices FINAL WHERE symbol = '{sym}'")
                if not check_df.empty and check_df.iloc[0]['cnt'] == 0:
                    import sys
                    sys.stdout.write(f"Symbol {sym} not found in DB. Executing auto-import...\n")
                    sys.stdout.flush()
                    import_res = import_symbol_data_impl(sym)
                    sys.stdout.write(f"Auto-import result: {import_res}\n")
                    sys.stdout.flush()
    except Exception as exc:
        pass

    try:
        from src.db.pool import query_df
        df = query_df(clean_query)
        if df.empty:
            return "Query executed successfully, but returned 0 rows."

        max_rows = 100
        truncated = len(df) > max_rows
        df_subset = df.head(max_rows)

        # Stale-data detection — set flag for chat loop
        try:
            from src.tools.db_tools import _stale_flag, _check_table_freshness, STALE_THRESHOLD_DAYS
            import pandas as pd
            date_cols = [c for c in df.columns
                         if any(k in c.lower() for k in ("date", "as_of", "snapshot", "fetched"))]
            if date_cols:
                latest = pd.to_datetime(df[date_cols[0]]).max()
                days_old = (pd.Timestamp.now() - latest).days
                if days_old > STALE_THRESHOLD_DAYS:
                    hint = _check_table_freshness(clean_query)
                    if hint:
                        _stale_flag.hint = {**hint, "days_ago": days_old,
                                             "last_date": str(latest)[:10]}
        except Exception:
            pass

        result_str = df_subset.to_markdown(index=False)
        if truncated:
            result_str += f"\n\n[Warning: Output truncated to {max_rows} rows from total of {len(df)} rows]"
        return result_str
    except Exception as e:
        hint = (
            "\n\nClickHouse column names: trade_date (daily_prices), nav_date (mf_nav), "
            "as_of_month (mf_holdings), as_of (signal_composite/ml_predictions).\n"
            "Date functions: today(), today()-30, toStartOfMonth(today()).\n"
            f"SQL attempted:\n```sql\n{clean_query}\n```"
        )
        return f"Error executing ClickHouse query: {e}{hint}"


@tool
def get_live_inav(symbol: str) -> str:
    """
    Get the previous-day declared NAV, current last traded price, and the
    price-vs-NAV spread for any NSE-listed ETF.

    IMPORTANT — what "NAV" means here:
      NSE's public ETF API only exposes the PREVIOUS DAY's declared NAV (the value
      AMC computes at end-of-day). The true real-time iNAV (updated every 15 seconds)
      is only accessible through NSE's Akamai-protected /api/quote-equity endpoint
      which requires a real browser session and is not available programmatically.

      For INTERNATIONAL ETFs (HNGSNGBEES, MAFANG, MON100…): prev-day NAV IS the
      correct reference since the overseas market is closed during Indian trading hours.

      For DOMESTIC COMMODITY ETFs (GOLDBEES, SILVERBEES…): the true live iNAV tracks
      MCX gold/silver prices (which include ~8–9% import duty + GST premium over COMEX).
      The prev-day NAV is a reasonable reference but may differ from the true intraday
      iNAV by 1–2% when commodity prices move significantly during the session.

      For DOMESTIC EQUITY ETFs (NIFTYBEES, BANKBEES…): the true live iNAV tracks the
      current index level; prev-day NAV is a close approximation.

    Use this tool (NOT query_clickhouse_db) whenever a user asks:
      "what is the NAV of GOLDBEES / SILVERBEES / HNGSNGBEES"
      "current NAV", "ETF premium vs NAV", "is it trading at discount"

    Args:
        symbol: NSE ETF symbol — e.g. "GOLDBEES", "SILVERBEES", "HNGSNGBEES", "NIFTYBEES"

    Returns:
        Formatted string with: prev-day declared NAV, current LTP, spread %, snapshot time,
        and data source.
    """
    from src.importer.fetchers.nse_inav_fetcher import get_latest_inav
    from src.utils.ist import fmt_ist
    data = get_latest_inav(symbol.strip().upper(), store_to_db=True)
    if data is None:
        return (
            f"No iNAV data available for {symbol.upper()}. "
            f"NSE API may be unreachable (check market hours: IST 09:15–15:30)."
        )
    from datetime import datetime, timezone, timedelta
    prem = data["premium_discount_pct"]
    direction = "PREMIUM" if prem > 0 else "DISCOUNT"

    # Compute age of snapshot in seconds
    snap_dt = data.get("snapshot_at")
    age_str = ""
    if snap_dt is not None:
        try:
            snap_utc = snap_dt if snap_dt.tzinfo else snap_dt.replace(tzinfo=timezone.utc)
            age_sec = int((datetime.now(timezone.utc) - snap_utc).total_seconds())
            if age_sec < 60:
                age_str = f", {age_sec}s ago"
            else:
                age_str = f", {age_sec // 60}m {age_sec % 60}s ago"
        except Exception:
            pass

    src_map = {
        "kite_live":    "live iNAV via Kite",
        "nse_api_live": "live from NSE (prev-day NAV)",
        "db":           f"cached (DB{age_str})",
    }
    src_label = src_map.get(data["source"], data["source"])
    snap_ist = fmt_ist(snap_dt)
    nav_note = (
        "⚠ NSE API only publishes the previous day's declared NAV — "
        "not a real-time intraday iNAV. For domestic commodity ETFs (GOLDBEES, SILVERBEES) "
        "the true live iNAV tracks current MCX/COMEX prices. "
        "For international ETFs (HNGSNGBEES, MAFANG etc.) prev-day NAV is correct "
        "since the overseas market is closed during Indian hours."
    )
    is_live_inav = data["source"] == "kite_live"
    nav_label = "iNAV (live)" if is_live_inav else "Prev-Day Declared NAV"
    note = "" if is_live_inav else f"\n\n_{nav_note}_"
    return (
        f"**{data['symbol']} iNAV Snapshot** ({src_label}, {snap_ist})\n"
        f"- {nav_label}: ₹{data['inav']}\n"
        f"- Market Price (LTP): ₹{data['market_price']}\n"
        f"- {direction}: {prem:+.4f}%\n"
        f"  {'→ ETF trades above iNAV (expensive)' if prem > 0 else '→ ETF trades below iNAV (discount)'}"
        f"{note}"
    )


@tool
def run_premium_alerts(
    lookback: int = 30,
    lookback_unit: str = "days",
    z_threshold: float = -1.5,
    symbols: str = "",
    min_snapshots: int = 5,
) -> str:
    """
    Scarcity Premium/Discount Alerts for international Indian ETFs (MAFANG, HNGSNGBEES, etc.).
    Trades the premium created by RBI's overseas investment cap.

    iNAV data freshness:
      - During market hours (IST 09:15–15:30): DB snapshot must be ≤ 10 min old.
        If stale, the NSE API is called live and the fresh snapshot is stored to DB.
      - Outside market hours: last available DB snapshot (up to 4 days old) is used.
    The 'inav_source' field in results indicates "db" (cached) or "nse_api_live".

    Args:
        lookback: Numeric period for the history window (default 30).
        lookback_unit: Unit for the lookback period. One of:
                         "days"   — calendar days  (e.g. lookback=30)
                         "months" — calendar months (e.g. lookback=3  → 90 days)
                         "years"  — calendar years  (e.g. lookback=1  → 365 days)
                       Examples:
                         "6 months" → lookback=6,  lookback_unit="months"
                         "1 year"   → lookback=1,  lookback_unit="years"
                         "90 days"  → lookback=90, lookback_unit="days"
        z_threshold: Z-score at or below which SCREAMING BUY fires (default -1.5).
        symbols: Comma-separated NSE symbols to scan (e.g., 'MAFANG,HNGSNGBEES'). Default is all international ETFs.
        min_snapshots: Minimum hourly snapshots required to compute a meaningful Z-score (default 5).
    """
    unit = lookback_unit.lower().rstrip("s")  # normalise: "months" → "month"
    if unit == "month":
        lookback_days = lookback * 30
    elif unit == "year":
        lookback_days = lookback * 365
    else:
        lookback_days = lookback  # default: treat as days

    args = ["src/main.py", "premium-alerts", "--lookback", str(lookback_days), "--z-threshold", str(z_threshold), "--min-snapshots", str(min_snapshots)]
    if symbols:
        args.extend(["--symbols", symbols])
    return _run_cmd(args)


def _build_llm_for_deepdive():
    from config.settings import settings
    provider = settings.llm_provider.lower()
    
    # 1. OpenRouter
    if provider == "openrouter":
        from langchain_openai import ChatOpenAI
        return ChatOpenAI(
            model=settings.llm_model,
            api_key=settings.openrouter_api_key,
            base_url="https://openrouter.ai/api/v1",
            temperature=0,
            max_tokens=settings.llm_token_budget,
            timeout=settings.cloud_llm_request_timeout,
        )
        
    # 2. Local/Custom OpenAI-compatible Endpoint
    if settings.llm_base_url:
        from langchain_openai import ChatOpenAI
        is_nvidia = "nvidia" in settings.llm_base_url.lower()
        if is_nvidia:
            from src.utils.nim_pool import NIMPool
            return NIMPool.get().acquire(
                model=settings.llm_model,
                extra_body={},
                timeout=settings.llm_request_timeout,
                max_tokens=settings.llm_token_budget,
            )
        extra_body = {"options": {"num_ctx": settings.llm_context_window}}
        if settings.llm_think:
            extra_body["think"] = True
        return ChatOpenAI(
            model=settings.llm_model,
            base_url=settings.llm_base_url,
            api_key=settings.openai_api_key or "local",
            temperature=0,
            max_tokens=settings.llm_token_budget,
            extra_body=extra_body,
            timeout=settings.llm_request_timeout,
            streaming=False,
        )
        
    # 3. Google/Gemini
    if provider == "google":
        from langchain_google_genai import ChatGoogleGenerativeAI
        return ChatGoogleGenerativeAI(
            model=settings.llm_model,
            google_api_key=settings.google_api_key,
            temperature=0,
            max_output_tokens=settings.llm_token_budget,
        )
        
    # 4. Anthropic
    if provider == "anthropic":
        from langchain_anthropic import ChatAnthropic
        return ChatAnthropic(
            model=settings.llm_model,
            api_key=settings.anthropic_api_key,
            temperature=0,
            max_tokens=settings.llm_token_budget,
            extra_headers={"anthropic-beta": "prompt-caching-2024-07-31"},
            timeout=settings.cloud_llm_request_timeout,
        )
        
    # 5. OpenAI Cloud (Default)
    from langchain_openai import ChatOpenAI
    return ChatOpenAI(
        model=settings.llm_model,
        api_key=settings.openai_api_key,
        temperature=0,
        max_tokens=settings.llm_token_budget,
        timeout=settings.cloud_llm_request_timeout,
    )


@tool
def run_deepdive_analysis(ticker: str, section: str | None = None, skip_fetch: bool = False) -> str:
    """
    Run a comprehensive company deep-dive report for a US listed stock (e.g., ADSK, AAPL, MSFT)
    using the deep-dive engine.
    This fetches SEC filings (via sec-api.io), XBRL financials, job postings, and peer market data,
    and compiles a multi-section markdown report saved under output/deepdive/<TICKER>/<DATE>/report.md.
    Args:
        ticker: The US ticker symbol (e.g., 'ADSK', 'AAPL').
        section: Optional. Limit execution to one specific section:
                 core_business, financials, competitors, investments, execution, valuation, talent.
        skip_fetch: If True, uses cached data only and skips live network calls.
    """
    from datetime import date
    from pathlib import Path
    from config.settings import settings
    
    ticker_clean = ticker.strip().upper()
    args = ["src/main.py", "deepdive", ticker_clean]
    if section:
        args.extend(["--section", section])
    if skip_fetch:
        args.append("--skip-fetch")
        
    cmd_output = _run_cmd(args)
    
    # Locate output directories
    today_str = date.today().isoformat()
    out_dir = Path(PROJECT_ROOT) / "output" / "deepdive" / ticker_clean / today_str
    prompts_dir = out_dir / "prompts"
    sections_dir = out_dir / "sections"
    
    # We must support generating missing narrative sections
    from src.deepdive.analyze.gemini_cli import SECTION_KEYS
    targets = [section] if section and section in SECTION_KEYS else SECTION_KEYS
    
    sections_dir.mkdir(parents=True, exist_ok=True)
    
    llm = None
    generated_any = False
    
    for key in targets:
        prompt_file = prompts_dir / f"{key}_assembled.txt"
        section_file = sections_dir / f"{key}.md"
        
        # If the prompt exists and the section hasn't been generated yet (or is empty placeholder)
        if prompt_file.exists():
            # Check if we need to generate it
            needs_generation = True
            if section_file.exists():
                content = section_file.read_text(encoding="utf-8").strip()
                # If it's a template error placeholder or empty, regenerate
                if len(content) > 100 and not content.startswith("<!--"):
                    needs_generation = False
            
            if needs_generation:
                logger.info(f"Generating narrative for deep-dive section '{key}' using configured LLM...")
                prompt_content = prompt_file.read_text(encoding="utf-8")
                
                try:
                    if llm is None:
                        llm = _build_llm_for_deepdive()
                    
                    response = llm.invoke(prompt_content)
                    narrative = response.content if hasattr(response, 'content') else str(response)
                    
                    section_file.write_text(narrative.strip(), encoding="utf-8")
                    generated_any = True
                except Exception as exc:
                    logger.error(f"Failed to generate deep-dive section '{key}' narrative: {exc}")
                    
    # If we generated any sections, re-run Phase 7 assembly (using --skip-fetch to use cache)
    if generated_any:
        logger.info(f"Re-running deep-dive assembly to compile final report.md for {ticker_clean}...")
        assembly_args = ["src/main.py", "deepdive", ticker_clean, "--skip-fetch"]
        if section:
            assembly_args.extend(["--section", section])
        assembly_output = _run_cmd(assembly_args)
        cmd_output += f"\n\nAssembly Log:\n{assembly_output}"
        
    # Preview output
    report_path = os.path.join(PROJECT_ROOT, "output", "deepdive", ticker_clean, today_str, "report.md")
    if os.path.exists(report_path):
        try:
            with open(report_path, "r", encoding="utf-8") as f:
                content = f.read(1500)
            preview = f"\n\n--- PREVIEW OF THE REPORT ---\n{content}...\n[End of Preview. Full report is available at: {report_path}]"
        except Exception:
            preview = f"\n\nFull report was successfully saved to: {report_path}"
    else:
        # Fallback to search if not today's date
        base_dir = os.path.join(PROJECT_ROOT, "output", "deepdive", ticker_clean)
        if os.path.exists(base_dir):
            dates = sorted([d for d in os.listdir(base_dir) if os.path.isdir(os.path.join(base_dir, d))], reverse=True)
            if dates:
                report_path = os.path.join(base_dir, dates[0], "report.md")
                if os.path.exists(report_path):
                    with open(report_path, "r", encoding="utf-8") as f:
                        content = f.read(1500)
                    preview = f"\n\n--- PREVIEW OF THE REPORT ({dates[0]}) ---\n{content}...\n[End of Preview. Full report is available at: {report_path}]"
                else:
                    preview = ""
            else:
                preview = ""
        else:
            preview = ""
            
    return f"Deep-dive analysis executed.\n\nCommand Log:\n{cmd_output}{preview}"


@tool
def read_deepdive_report(ticker: str) -> str:
    """
    Read the latest compiled US equity deep-dive report for a given ticker.
    Use this to read, summarize, or answer follow-up questions about a company's deep-dive report.
    Args:
        ticker: The US ticker symbol (e.g. 'ADSK', 'AAPL').
    """
    from datetime import date
    from pathlib import Path
    
    ticker_clean = ticker.strip().upper()
    base_dir = Path(PROJECT_ROOT) / "output" / "deepdive" / ticker_clean
    if not base_dir.exists():
        return f"No deep-dive reports found for ticker: {ticker_clean}"
    
    today_str = date.today().isoformat()
    report_path = base_dir / today_str / "report.md"
    
    if not report_path.exists():
        # Find latest date directory
        if not os.path.exists(base_dir):
            return f"No deep-dive reports found for ticker: {ticker_clean}"
        dates = sorted([d for d in os.listdir(base_dir) if os.path.isdir(base_dir / d)], reverse=True)
        if not dates:
            return f"No deep-dive reports found for ticker: {ticker_clean}"
        report_path = base_dir / dates[0] / "report.md"
        
    if not report_path.exists():
        return f"Report file not found for ticker {ticker_clean} (expected at {report_path})"
        
    try:
        content = report_path.read_text(encoding="utf-8")
        return content
    except Exception as exc:
        return f"Error reading report for {ticker_clean}: {exc}"


# ── Workflow tools (LangGraph StateGraph — token-efficient) ───────────────────

@tool
def run_autonomous_research(question: str) -> str:
    """
    Deep multi-domain equity research via LangGraph StateGraph workflow.

    Token-efficient alternative to AutonomousResearchAgent (80% fewer tokens).
    Runs all data fetch in parallel Python threads; uses LLM only for
    adversarial verification and final synthesis.
    """
    from src.workflows.autonomous_research import run
    return run(question)


@tool
def run_india_equity_research_workflow(question: str) -> str:
    """
    Guaranteed 8-section NSE/BSE equity research note via StateGraph.

    All 12 data-fetch tools run in parallel and are guaranteed to complete
    before synthesis starts — no silent section skips.
    """
    from src.workflows.india_equity import run
    return run(question)


@tool
def run_multi_fund_consensus_workflow(period: str = "mom") -> str:
    """
    Cross-fund MF consensus workflow: per-fund MoM/YoY analysis for 7 funds
    in parallel, then aggregate consensus synthesis.

    period: 'mom' (default) or 'yoy'
    """
    from src.workflows.multi_fund_consensus import run
    return run(period)


@tool
def run_portfolio_workflow() -> str:
    """
    Portfolio analysis with adversarial verification.

    Reads holdings from market_data.user_holdings FINAL, enriches each in
    parallel, scores with LLM, adversarially verifies HIGH-conviction calls,
    then synthesises with macro context.
    """
    from src.workflows.portfolio_analysis import run
    return run()


# ── Canonical tool list ────────────────────────────────────────────────────────
# Single source of truth — all other lists are subsets of this.
SKILLS_TOOLS = [
    # Runners (shell wrappers) — defined in runners.py, re-exported above
    run_goldbees_pipeline,
    run_daily_signal_composite,
    run_macro_scanner,
    run_etf_news_sentiment,
    run_dsp_multi_asset_importer,
    run_nippon_importer,
    run_icici_importer,
    run_all_multi_asset_importers,
    run_multi_asset_holdings_mom_yoy,
    run_multi_asset_consensus,
    run_data_engineering_importer,
    run_comex_analysis,
    run_whale_tracker,
    run_dsp_multi_asset_comparison,
    run_fund_mom_returns,
    run_market_indicators,
    # Gold/GARCH domain — defined in market/gold.py, re-exported above
    run_risk_governor_analysis,
    explain_price_anomalies,
    # Macro context tools — defined in market_context.py, re-exported above
    get_dxy_context,
    # General-purpose tools defined in this file
    import_symbol_data,
    get_live_inav,
    query_clickhouse_db,
    run_premium_alerts,
    run_deepdive_analysis,
    read_deepdive_report,
    # Workflow tools — LangGraph StateGraph (token-efficient, guaranteed coverage)
    run_autonomous_research,
    run_india_equity_research_workflow,
    run_multi_fund_consensus_workflow,
    run_portfolio_workflow,
]
