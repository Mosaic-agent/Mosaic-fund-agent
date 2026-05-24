"""
src/tools/skills_tools.py
─────────────────────────
LangChain tool wrappers around the core skills and scripts of the Mosaic-agent.
Allows the ReAct agent to run goldbees reports, macro scanning, signal aggregator,
ETF news sentiment, DSP Multi-Asset imports, and risk governor calculations.
"""

from __future__ import annotations

import os
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
            timeout=60,
        )
        output = res.stdout
        if res.stderr:
            output += "\n--- STDERR ---\n" + res.stderr
        return _clean_terminal_output(output)
    except Exception as e:
        return f"Error executing command {' '.join(cmd)}: {e}"


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
def run_data_engineering_importer(category: str = "etfs,stocks,mf,fii_dii,cot,fx_rates", full: bool = False) -> str:
    """
    Trigger the historical ClickHouse data engineering pipeline to import and sync data from external APIs.
    Use this when asked to sync/import/refresh general market data or specific categories.
    Args:
        category: Comma-separated list of categories to import (etfs, stocks, mf, fii_dii, cot, fx_rates).
        full: If True, performs a full backfill ignoring watermarks.
    """
    args = ["src/main.py", "import"]
    if full:
        args.append("--full")
    else:
        args.extend(["--category", category])
    return _run_cmd(args)


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
    mutual fund holdings, flows, predictions, watermarks, or fx rates.
    
    Rules:
      - Always query tables using the 'FINAL' modifier to deduplicate rows (e.g. `market_data.mf_holdings FINAL`).
      - Only SELECT, SHOW, DESCRIBE, EXPLAIN, and WITH queries are permitted.
      - The database is 'market_data'. Tables include: daily_prices, mf_nav, mf_holdings,
        fii_dii_flows, ml_predictions, signal_composite, inav_snapshots, fx_rates.
    """
    clean_query = sql_query.strip()
    first_word = clean_query.split()[0].upper() if clean_query else ""
    if first_word not in ("SELECT", "SHOW", "DESCRIBE", "EXPLAIN", "WITH"):
        return "Error: Only read-only queries (SELECT, SHOW, DESCRIBE, EXPLAIN, WITH) are permitted."
         
    try:
        from src.db.pool import query_df
        df = query_df(clean_query)
        if df.empty:
            return "Query executed successfully, but returned 0 rows."
        
        max_rows = 100
        truncated = len(df) > max_rows
        df_subset = df.head(max_rows)
        
        # Convert to markdown string output for easy reading
        result_str = df_subset.to_markdown(index=False)
        if truncated:
            result_str += f"\n\n[Warning: Output truncated to {max_rows} rows from total of {len(df)} rows]"
        return result_str
    except Exception as e:
        return f"Error executing ClickHouse query: {e}"


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
def run_premium_alerts(lookback: int = 30, z_threshold: float = -1.5, symbols: str = "", min_snapshots: int = 5) -> str:
    """
    Scarcity Premium/Discount Alerts for international Indian ETFs (MAFANG, HNGSNGBEES, etc.).
    Trades the premium created by RBI's overseas investment cap.
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
    run_risk_governor_analysis,
    query_clickhouse_db,
    run_whale_tracker,
    run_dsp_multi_asset_comparison,
    run_fund_mom_returns,
    run_comex_analysis,
    run_premium_alerts,
]
