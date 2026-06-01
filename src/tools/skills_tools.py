"""
src/tools/skills_tools.py
─────────────────────────
LangChain tool wrappers around the core skills and scripts of the Mosaic-agent.
Allows the ReAct agent to run goldbees reports, macro scanning, signal aggregator,
ETF news sentiment, DSP Multi-Asset imports, and risk governor calculations.
"""

from __future__ import annotations

import os
import re
import sys
import subprocess
from typing import Any
from langchain_core.tools import tool

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))


def _clean_terminal_output(text: str) -> str:
    """Removes terminal box-drawing characters and excessive blank lines to make tool output easier for LLMs to read."""
    # Characters to remove or replace
    replacements = {
        "█": "#", "░": ".", "▒": ".", "▓": "#",
        "■": "*", "▲": "^", "▼": "v"
    }
    
    lines = []
    for line in text.splitlines():
        # Specific replacements
        for char, replacement in replacements.items():
            line = line.replace(char, replacement)
        
        # Clean all Unicode box-drawing characters (U+2500 to U+257F block)
        cleaned_chars = []
        for char in line:
            val = ord(char)
            if 0x2500 <= val <= 0x257F:
                if char in ("─", "━", "═", "┄", "┅", "┈", "┉", "╌", "╍"):
                    cleaned_chars.append("-")
                else:
                    cleaned_chars.append("")
            else:
                cleaned_chars.append(char)
        line = "".join(cleaned_chars)
        
        # Strip trailing/leading spaces
        line = line.strip()
        
        # Skip empty lines if they were just box drawings or blank
        if line and not all(c in "-_ " for c in line):
            lines.append(line)
            
    return "\n".join(lines)


def _run_cmd(args: list[str]) -> str:
    """Helper to run a command via subprocess from the project root with the correct Python interpreter."""
    env = os.environ.copy()
    env["ALLOW_LOCAL_RUN"] = "1"

    # Ensure project root is in PYTHONPATH
    if "PYTHONPATH" in env:
        env["PYTHONPATH"] = PROJECT_ROOT + os.pathsep + env["PYTHONPATH"]
    else:
        env["PYTHONPATH"] = PROJECT_ROOT

    cmd = [sys.executable] + args
    try:
        res = subprocess.run(
            cmd,
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            env=env,
            timeout=300,
        )
        output = res.stdout
        if res.stderr:
            output += "\n--- STDERR ---\n" + res.stderr
        return _clean_terminal_output(output)
    except Exception as e:
        return f"Error executing command {' '.join(cmd)}: {e}"


# Pre-compiled ANSI escape code stripper used by streaming output
import re as _re
_ANSI_STRIP_RE = _re.compile(r"\x1b\[[0-9;]*[a-zA-Z]|\x1b[()][A-Z0-9=><]|\x1b[ABCDEF78]")


def _run_cmd_streaming(args: list[str]) -> str:
    """
    Like _run_cmd but prints each output line to the terminal as the subprocess
    runs, giving live progress. Used for long-running operations like data import.
    Returns the full collected output as a string when done.
    """
    env = os.environ.copy()
    env["ALLOW_LOCAL_RUN"] = "1"
    env["NO_COLOR"] = "1"       # tell Rich / Typer inside subprocess to skip ANSI codes
    env["TERM"] = "dumb"        # further signal: no colour support
    if "PYTHONPATH" in env:
        env["PYTHONPATH"] = PROJECT_ROOT + os.pathsep + env["PYTHONPATH"]
    else:
        env["PYTHONPATH"] = PROJECT_ROOT

    cmd = [sys.executable] + args
    collected: list[str] = []

    try:
        proc = subprocess.Popen(
            cmd,
            cwd=PROJECT_ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            env=env,
            bufsize=1,          # line-buffered
        )
        for raw_line in iter(proc.stdout.readline, ""):
            # Strip ANSI codes left over despite NO_COLOR
            stripped = _ANSI_STRIP_RE.sub("", raw_line).rstrip()
            # Apply box-drawing cleanup (shared with _run_cmd)
            cleaned = _clean_terminal_output(stripped)
            if cleaned:
                sys.stdout.write(f"  {cleaned}\n")
                sys.stdout.flush()
                collected.append(cleaned)
        proc.wait()
        rc = proc.returncode
    except Exception as exc:
        return f"Import error: {exc}\n" + "\n".join(collected)

    result = "\n".join(collected) if collected else "Import completed (no output)."
    if rc != 0:
        result += f"\n[Process exited with code {rc}]"
    return result


@tool
def run_goldbees_pipeline() -> str:
    """
    Run the GOLDBEES pipeline report script.
    This prints the pre-baked gold recommendation block, including ML prediction probability,
    Risk Governor and Kelly weights, and iNAV premium.
    Use this when asked for GOLDBEES recommendation, today's gold signal, or what to do with GOLDBEES.
    """
    return _run_cmd(["src/scripts/goldbees_report.py"])


@tool
def run_daily_signal_composite(save: bool = True) -> str:
    """
    Run the composite signal aggregator to compute scores for all 18 ETFs.
    Combines macro events, news sentiment, NAV Z-scores, FII/DII flows, ML predictions, and anomaly regimes.
    Use this when asked for ETF buy/sell recommendations, composite scores, or signal dashboard.
    """
    args = ["src/main.py", "signals"]
    if save:
        args.append("--save")
    return _run_cmd(args)


@tool
def run_macro_scanner(max_themes: int = 3) -> str:
    """
    Scan live macro and geopolitical events, mapping their directional impact to Indian ETFs.
    Use this when asked about active macro themes, geopolitical risks, crude oil/INR shocks, or Fed/RBI policy events.
    """
    return _run_cmd(["src/main.py", "macro", "--max", str(max_themes)])


@tool
def run_etf_news_sentiment(max_articles: int = 3, save: bool = True) -> str:
    """
    Fetch and tag the latest news articles by ETF category with sentiment scores.
    Use this when asked for ETF news, sentiment trends, or saving ETF news.
    """
    args = ["src/main.py", "etf-news", "--max", str(max_articles)]
    if save:
        args.append("--save")
    return _run_cmd(args)


@tool
def run_dsp_multi_asset_importer() -> str:
    """
    Import and backfill DSP Multi Asset Allocation Fund holdings history into the ClickHouse database.
    Use this when asked to import or update DSP holdings data.
    """
    return _run_cmd(["src/scripts/dsp/import_all_dsp_equity.py"])


@tool
def run_data_engineering_importer(category: str = "etfs,stocks,mf,fii_dii,cot,fx_rates,inav", full: bool = False, symbol: str = "") -> str:
    """
    Trigger the historical ClickHouse data engineering pipeline to import and sync BULK data.
    Use ONLY for bulk category imports — e.g. "import all stocks", "refresh ETFs", "sync everything".

    IMPORTANT: If the user names a SPECIFIC symbol (e.g. "import ADVENZYMES", "refresh GOLDBEES"),
    do NOT use this tool — use `import_symbol_data(symbol)` instead.
    This tool imports ALL symbols in a category, which is slow and wasteful when only one is needed.

    If called with a symbol anyway, it will auto-redirect to import_symbol_data for that symbol.

    Args:
        category: Comma-separated list of categories to import.
                  Valid values: etfs, stocks, mf, fii_dii, cot, fx_rates, inav.
        full: If True, performs a full backfill ignoring watermarks.
        symbol: (optional) If a specific symbol is provided, redirects to import_symbol_data.
    """
    # Safety net: if caller passed a specific symbol, redirect to per-symbol import
    if symbol and symbol.strip():
        return import_symbol_data_impl(symbol.strip().upper())

    args = ["src/main.py", "import"]
    if full:
        args.append("--full")
    else:
        args.extend(["--category", category])
    return _run_cmd_streaming(args)


def import_symbol_data_impl(symbol: str, days: int = 365) -> str:
    """
    Core implementation to import price history for a specific symbol.
    """
    from datetime import date, timedelta

    sym = symbol.strip().upper()
    to_date = date.today()
    from_date = to_date - timedelta(days=max(days, 1))

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

    sys.stdout.write(
        f"  Importing {sym} ({yahoo_ticker}) | {from_date} → {to_date} ({days} days)\n"
    )
    sys.stdout.flush()

    try:
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
        from config.settings import settings
        from src.importer.clickhouse import ClickHouseImporter

        ch = ClickHouseImporter(
            host=settings.clickhouse_host,
            port=settings.clickhouse_port,
            database=settings.clickhouse_database,
            username=settings.clickhouse_user,
            password=settings.clickhouse_password,
        )
        try:
            n = ch.insert_prices(rows)
            max_date = max(r["trade_date"] for r in rows)
            ch.set_watermark("yfinance", sym, max_date)
            sys.stdout.write(f"  ✓ {n} rows inserted. Last trade_date: {max_date}\n")
            sys.stdout.flush()
            return f"Imported {sym}: {n} rows inserted, {from_date} → {max_date}."
        finally:
            ch.close()
    except Exception as exc:
        return f"ClickHouse insert error for {sym}: {exc}"


@tool
def import_symbol_data(symbol: str, days: int = 365) -> str:
    """
    Import price history for a SPECIFIC NSE symbol. This is the PREFERRED tool when the user
    names a particular stock/ETF to import — e.g. "import ADVENZYMES", "refresh GOLDBEES data",
    "update RELIANCE prices". Much faster than bulk import since it fetches only one symbol.

    For bulk category imports (all ETFs, all stocks), use run_data_engineering_importer instead.

    Args:
        symbol: NSE symbol to import (e.g. "ADVENZYMES", "GOLDBEES", "RELIANCE", "NIFTY50").
                Uppercased automatically. Covers ETFs, stocks, commodities, and indices.
        days:   Calendar days of history back from today.
                365=1 year · 730=2 years · 180=6 months · 90=3 months · 30=1 month
    """
    return import_symbol_data_impl(symbol, days)


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
    df_r, _, _ = run_composite_anomaly(price_df)
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
def query_clickhouse_db(sql_query: str) -> str:
    """
    Execute a read-only SQL query on the ClickHouse 'market_data' database.
    Use this to run custom SELECT, SHOW, DESCRIBE, or EXPLAIN queries to retrieve historical prices,
    mutual fund holdings, flows, predictions, watermarks, fx rates, or US stock deep-dive data.
    
    Rules:
      - Always query tables using the 'FINAL' modifier to deduplicate rows (e.g. `market_data.mf_holdings FINAL`).
      - Only SELECT, SHOW, DESCRIBE, EXPLAIN, and WITH queries are permitted.
      - The database is 'market_data'. Available tables include:
        * Indian Markets: daily_prices, mf_nav, mf_holdings, fii_dii_flows, ml_predictions, signal_composite, inav_snapshots, fx_rates
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


def _summarize_whale_tracker_output(output: str) -> str:
    """Parses raw whale tracker output and appends a concise summary of main accumulation/trim actions as Markdown tables."""
    accumulations = []
    trims = []
    
    current_fund = ""
    for line in output.splitlines():
        line_str = line.strip()
        if "Multi Asset" in line_str:
            current_fund = line_str.split("(")[0].strip()
            
        # Parse lines containing changes
        if "%" in line_str and ("+" in line_str or "-" in line_str):
            parts = line_str.split()
            if len(parts) >= 5:
                change_str = parts[-1].rstrip("%")
                try:
                    change_val = float(change_str)
                    pct_indices = [i for i, p in enumerate(parts) if "%" in p]
                    if len(pct_indices) >= 2:
                        prev_pct_idx = pct_indices[-2]
                        theme_idx = 1 if parts[0].startswith(("🥇", "🥈", "⚛️", "🛢️", "🏗️")) else 0
                        theme = " ".join(parts[:theme_idx+1])
                        security = " ".join(parts[theme_idx+1:prev_pct_idx])
                        
                        if change_val > 0.05:
                            accumulations.append((change_val, security, theme, current_fund))
                        elif change_val < -0.05:
                            trims.append((change_val, security, theme, current_fund))
                except ValueError:
                    continue
                    
    # Sort by absolute change magnitude
    accumulations.sort(key=lambda x: x[0], reverse=True)
    trims.sort(key=lambda x: x[0])
    
    summary = "\n\n### 🐋 Whale Tracker Concise Summary\n\n"
    summary += "#### Top Accumulations (Increasing Weight)\n\n"
    summary += "| Fund | Theme | Security | Change |\n"
    summary += "| :--- | :--- | :--- | ---: |\n"
    if accumulations:
        for change_val, security, theme, fund in accumulations[:5]:
            summary += f"| {fund} | {theme} | {security} | {change_val:+.2f}% |\n"
    else:
        summary += "| None detected | | | |\n"
        
    summary += "\n#### Top Trims (Reducing Weight)\n\n"
    summary += "| Fund | Theme | Security | Change |\n"
    summary += "| :--- | :--- | :--- | ---: |\n"
    if trims:
        for change_val, security, theme, fund in trims[:5]:
            summary += f"| {fund} | {theme} | {security} | {change_val:+.2f}% |\n"
    else:
        summary += "| None detected | | | |\n"

    # Extract sector/theme trend allocations if present in the output
    if "Unified Macro Theme Allocations" in output:
        sub = output.split("Unified Macro Theme Allocations")[1]
        if "High-Conviction Equity Cross-Ownership" in sub:
            sub = sub.split("High-Conviction Equity Cross-Ownership")[0]
            
        themes = []
        latest_weights = []
        flow_changes = []
        
        for line in sub.splitlines():
            if "%" in line:
                line_clean = line
                for emoji in ["🥈", "🥇", "⚛️", "🛢️", "🏗️"]:
                    line_clean = line_clean.replace(emoji, "")
                parts = line_clean.split()
                if len(parts) >= 4:
                    theme_name = parts[0]
                    if theme_name in ["Silver", "Gold", "Nuclear/Grid", "Energy", "Infra"]:
                        try:
                            latest_w = float(parts[-2].replace("%", "").strip())
                            flow_c = float(parts[-1].replace("%", "").strip())
                            themes.append(theme_name)
                            latest_weights.append(latest_w)
                            flow_changes.append(flow_c)
                        except ValueError:
                            pass
        
        if themes:
            try:
                import plotext as plt
                import re
                ansi_escape = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")
                
                # Plot Combined Latest Weights
                plt.clear_figure()
                plt.bar(themes, latest_weights, orientation="horizontal")
                plt.title("Combined Latest Weights by Sector/Theme (%)")
                plt.plot_size(70, 15)
                latest_chart = plt.build()
                latest_chart_clean = ansi_escape.sub("", latest_chart)
                
                # Plot Net Flow Change
                plt.clear_figure()
                plt.bar(themes, flow_changes, orientation="horizontal")
                plt.title("Net Flow Change by Sector/Theme (%)")
                plt.plot_size(70, 15)
                flow_chart = plt.build()
                flow_chart_clean = ansi_escape.sub("", flow_chart)
                
                summary += "\n\n#### Combined Latest Weights by Sector/Theme (%)\n"
                summary += f"```text\n{latest_chart_clean}\n```\n"
                summary += "\n\n#### Net Flow Change by Sector/Theme (%)\n"
                summary += f"```text\n{flow_chart_clean}\n```\n"
            except Exception as e:
                summary += f"\n\n*(Note: Could not generate ASCII charts: {e})*\n"
                
    return summary


@tool
def run_whale_tracker() -> str:
    """
    Track weight shifts and institutional moves in core macro themes (Gold, Silver, Nuclear/Grid, Energy, Infra)
    across all 7 major multi-asset funds: Nippon India, DSP Multi Asset, DSP Omni FoF, Bajaj, Quant, and ICICI.
    Use this to identify what large institutional multi-asset funds are accumulating or trimming.
    """
    raw_output = _run_cmd(["src/scripts/market/whale_tracker.py"])
    return _summarize_whale_tracker_output(raw_output)


@tool
def run_dsp_multi_asset_comparison() -> str:
    """
    Run a comparative analysis between DSP Multi Asset Allocation Fund (Standard) and DSP Multi Asset Omni FoF.
    Compares asset class weights, structures, taxation, and Netra Quant framework strategies.
    """
    return _run_cmd(["src/scripts/dsp/compare_dsp_multi_asset.py"])


@tool
def run_fund_mom_returns(scheme_code: str | None = None, search_query: str | None = None, months: int = 12) -> str:
    """
    Analyze Month-over-Month (MoM) NAV returns for any Indian mutual fund.
    You must provide either a scheme_code or a search_query.
    Args:
        scheme_code: The scheme code of the fund (e.g., '152056' for DSP Multi Asset).
        search_query: Search string to look up the fund scheme code if not known (e.g., 'DSP Multi Asset').
        months: Number of months of history to fetch (default 12).
    """
    if not scheme_code and not search_query:
        return "Error: You must provide either scheme_code or search_query."
    
    args = ["src/scripts/portfolio/fund_mom_returns.py", "--months", str(months)]
    if scheme_code:
        args.extend(["--scheme", str(scheme_code)])
    elif search_query:
        args.extend(["--search", search_query])
        
    return _run_cmd(args)


@tool
def run_comex_analysis() -> str:
    """
    Run COMEX commodity pre-market signal analysis.
    This fetches live spot prices from gold-api.com for Gold (XAU), Silver (XAG), Copper (HG),
    compares against previous-day Yahoo Finance futures closes, and classifies signals.
    Use this when asked for COMEX gold/silver/copper prices, COMEX signals, or commodity pre-market trends.
    """
    return _run_cmd(["src/main.py", "comex"])


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
def run_premium_alerts(lookback: int = 30, z_threshold: float = -1.5, symbols: str = "", min_snapshots: int = 5) -> str:
    """
    Scarcity Premium/Discount Alerts for international Indian ETFs (MAFANG, HNGSNGBEES, etc.).
    Trades the premium created by RBI's overseas investment cap.

    iNAV data freshness:
      - During market hours (IST 09:15–15:30): DB snapshot must be ≤ 10 min old.
        If stale, the NSE API is called live and the fresh snapshot is stored to DB.
      - Outside market hours: last available DB snapshot (up to 4 days old) is used.
    The 'inav_source' field in results indicates "db" (cached) or "nse_api_live".

    Args:
        lookback: Days of iNAV history used to compute mean/std (default 30).
        z_threshold: Z-score at or below which SCREAMING BUY fires (default -1.5).
        symbols: Comma-separated NSE symbols to scan (e.g., 'MAFANG,HNGSNGBEES'). Default is all international ETFs.
        min_snapshots: Minimum hourly snapshots required to compute a meaningful Z-score (default 5).
    """
    args = ["src/main.py", "premium-alerts", "--lookback", str(lookback), "--z-threshold", str(z_threshold), "--min-snapshots", str(min_snapshots)]
    if symbols:
        args.extend(["--symbols", symbols])
    return _run_cmd(args)


# Unified list of core skill tools
SKILLS_TOOLS = [
    run_goldbees_pipeline,
    run_daily_signal_composite,
    run_macro_scanner,
    run_etf_news_sentiment,
    run_dsp_multi_asset_importer,
    run_data_engineering_importer,
    import_symbol_data,
    run_risk_governor_analysis,
    query_clickhouse_db,
    run_whale_tracker,
    run_dsp_multi_asset_comparison,
    run_fund_mom_returns,
    run_comex_analysis,
    run_premium_alerts,
]


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
    ticker_clean = ticker.strip().upper()
    args = ["src/main.py", "deepdive", ticker_clean]
    if section:
        args.extend(["--section", section])
    if skip_fetch:
        args.append("--skip-fetch")
        
    cmd_output = _run_cmd(args)
    
    # Try to locate the generated report.md file
    from datetime import date
    today_str = date.today().isoformat()
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


# Unified list of core skill tools
SKILLS_TOOLS = [
    run_goldbees_pipeline,
    run_daily_signal_composite,
    run_macro_scanner,
    run_etf_news_sentiment,
    run_dsp_multi_asset_importer,
    run_data_engineering_importer,
    import_symbol_data,
    get_live_inav,
    run_risk_governor_analysis,
    query_clickhouse_db,
    run_whale_tracker,
    run_dsp_multi_asset_comparison,
    run_fund_mom_returns,
    run_comex_analysis,
    run_premium_alerts,
    run_deepdive_analysis,
]
