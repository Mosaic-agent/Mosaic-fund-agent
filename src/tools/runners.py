"""
src/tools/runners.py
────────────────────
Thin @tool wrappers that delegate to CLI scripts via subprocess.
Each function here is a single _run_cmd / _run_cmd_streaming call — zero
business logic.  Business logic lives in src/ml/, src/db/, or dedicated
tool modules.
"""

from __future__ import annotations

from langchain_core.tools import tool

from src.tools._subprocess import (
    _run_cmd,
    _run_cmd_streaming,
    _summarize_whale_tracker_output,
)


def _import_symbol_data_impl(symbol: str, data_source: str = "") -> str:
    """Lazy proxy — avoids circular import between runners and skills_tools."""
    from src.tools.skills_tools import import_symbol_data_impl
    return import_symbol_data_impl(symbol, data_source=data_source)


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
def run_dsp_multi_asset_importer(full: bool = False, month: str = "", fresh: int = 0) -> str:
    """
    Import DSP Mutual Fund portfolio holdings into ClickHouse.

    Default (full=False): delta sync — discovers and imports only the latest
    month from dspim.com (fast, ~5–10s). Use this for "import DSP holdings",
    "refresh DSP data", "import DSP May holdings", etc.

    Set full=True to re-import the entire history. Use only when
    asked to "backfill", "re-import all DSP history", or "full DSP import".

    Set month to a specific month (YYYY-MM format) to import just that month.
    Set fresh to N to re-import the N most recent months.
    """
    args = ["src/main.py", "import", "--category", "dsp"]
    if full:
        args.append("--full")
    if month:
        args.extend(["--month", month])
    if fresh > 0:
        args.extend(["--fresh", str(fresh)])
    return _run_cmd_streaming(args)


@tool
def run_nippon_importer(full: bool = False, month: str = "", fresh: int = 0) -> str:
    """
    Import Nippon India Mutual Fund portfolio holdings into ClickHouse.

    Default (full=False): delta sync — imports only the latest month not yet
    in the database (fast). Use this for "import Nippon holdings",
    "refresh Nippon data", "import Nippon May holdings", etc.

    Set full=True to re-import the entire history. Use only when asked to
    "backfill", "re-import all Nippon history", or "full Nippon import".

    Set month to a specific month (YYYY-MM format) to import just that month.
    Set fresh to N to re-import the N most recent months.
    """
    args = ["src/main.py", "import", "--category", "nippon"]
    if full:
        args.append("--full")
    if month:
        args.extend(["--month", month])
    if fresh > 0:
        args.extend(["--fresh", str(fresh)])
    return _run_cmd_streaming(args)


@tool
def run_quant_importer(full: bool = False, month: str = "", fresh: int = 0) -> str:
    """
    Import Quant Mutual Fund portfolio holdings into ClickHouse.

    Default (full=False): delta sync — imports only the latest month not yet
    in the database (fast). Use this for "import Quant holdings",
    "refresh Quant data", "import Quant May holdings", etc.

    Set full=True to re-import the entire history. Use only when asked to
    "backfill", "re-import all Quant history", or "full Quant import".

    Set month to a specific month (YYYY-MM format) to import just that month.
    Set fresh to N to re-import the N most recent months.
    """
    args = ["src/main.py", "import", "--category", "quant"]
    if full:
        args.append("--full")
    if month:
        args.extend(["--month", month])
    if fresh > 0:
        args.extend(["--fresh", str(fresh)])
    return _run_cmd_streaming(args)


@tool
def run_multi_asset_holdings_mom_yoy(
    fund: str = "",
    scheme_code: str = "",
    search: str = "",
    top: int = 15,
    no_yoy: bool = False,
    list_funds: bool = False,
) -> str:
    """
    Analyse position-level Month-over-Month (MoM) and Year-over-Year (YoY)
    holdings changes for a multi-asset (or any) Indian mutual fund stored in
    `market_data.mf_holdings`.

    Different from `run_fund_mom_returns` (which is NAV-only): this tool shows
    what the fund manager actually bought, sold, increased, or trimmed at the
    security level, plus a roll-up of asset-class weight shifts.

    Use this when the user asks:
      • "How did DSP Multi Asset change MoM?"
      • "Show YoY position changes for Bajaj Multi Asset"
      • "What did Nippon Multi Asset add or trim this month?"
      • "Asset-class weight shift in DSP Multi Asset over 12 months"

    Args:
        fund:        Exact fund_name in mf_holdings (e.g. 'DSP_MULTI_ASSET').
        scheme_code: Numeric/code identifier (e.g. '152056').
        search:      Fuzzy fund_name search (e.g. 'DSP Multi'); takes first match.
        top:         Top N MoM/YoY movers to display (default 15).
        no_yoy:      Skip the YoY block when history is < 13 months.
        list_funds:  List every fund with ≥2 months of holdings and return.

    Provide at least one of fund / scheme_code / search (unless list_funds=True).
    """
    args = ["src/scripts/portfolio/multi_asset_holdings_mom_yoy.py"]
    if list_funds:
        args.append("--list")
    else:
        if not (fund or scheme_code or search):
            return (
                "FUND_REQUIRED: Provide fund, scheme_code, or search; "
                "or set list_funds=True to discover available funds."
            )
        if fund:
            args.extend(["--fund", fund])
        if scheme_code:
            args.extend(["--scheme", str(scheme_code)])
        if search:
            args.extend(["--search", search])
        args.extend(["--top", str(top)])
        if no_yoy:
            args.append("--no-yoy")
    return _run_cmd(args)


@tool
def run_multi_asset_consensus(
    period: str = "mom",
    min_funds: int = 2,
    min_delta: float = 0.10,
    asset: str = "",
    top: int = 15,
    no_rotation: bool = False,
) -> str:
    """
    Cross-fund pattern detector across all 7 multi-asset Indian mutual funds in
    `market_data.mf_holdings` (Nippon, Nippon FoF, DSP, DSP Omni, Bajaj, Quant,
    ICICI).

    Different from `run_multi_asset_holdings_mom_yoy` (single fund) and
    `run_whale_tracker` (theme-level, predefined ETF baskets): this tool finds
    **consensus signals** — securities and asset classes that *multiple
    multi-asset funds* are simultaneously adding to or trimming from. The
    institutional "smart money overlap" signal.

    Use this when the user asks:
      • "What are multi-asset funds collectively buying / selling?"
      • "Any pattern across multi-asset funds?"
      • "Smart-money consensus this month?"
      • "Are funds rotating from cash/bond to gold?"
      • "Which gold/equity name has multi-fund consensus?"

    Args:
        period:      'mom' (latest vs prev month, default) or 'yoy' (vs 12 months back).
        min_funds:   Min # of funds moving the same way to count as consensus (default 2).
        min_delta:   Min |Δ pct-pts| per fund to count as add/trim (default 0.10).
        asset:       Optional filter — restrict to a single asset_type (e.g. 'gold', 'equity', 'bond').
        top:         Top N rows to show per side (default 15).
        no_rotation: Skip the asset-class rotation roll-up.
    """
    if period not in ("mom", "yoy"):
        return f"INVALID_PERIOD: '{period}' — must be 'mom' or 'yoy'."
    args = [
        "src/scripts/portfolio/multi_asset_consensus.py",
        "--period", period,
        "--min-funds", str(min_funds),
        "--min-delta", str(min_delta),
        "--top", str(top),
    ]
    if asset:
        args.extend(["--asset", asset])
    if no_rotation:
        args.append("--no-rotation")
    return _run_cmd(args)


@tool
def run_data_engineering_importer(
    category: str = "etfs,stocks,mf,fii_dii,cot,fx_rates,inav",
    full: bool = False,
    symbol: str = "",
    data_source: str = "",
) -> str:
    """
    Trigger the historical ClickHouse data engineering pipeline to import and sync BULK data.
    Use ONLY for bulk category imports — e.g. "import all stocks", "refresh ETFs", "sync everything".

    IMPORTANT: If the user names a SPECIFIC symbol (e.g. "import ADVENZYMES", "refresh GOLDBEES"),
    do NOT use this tool — use `import_symbol_data(symbol)` instead.
    This tool imports ALL symbols in a category, which is slow and wasteful when only one is needed.

    If called with a symbol anyway, it will auto-redirect to import_symbol_data for that symbol.

    Args:
        category: Comma-separated list of categories to import.
                  Valid values: etfs, stocks, mf, fii_dii, cot, fx_rates, inav, world_bank, imf_weo, indian_macro, indian_macro_indicators.
        full: If True, performs a full backfill ignoring watermarks.
        symbol: (optional) If a specific symbol is provided, redirects to import_symbol_data.
        data_source: Required when importing stocks or ETFs. Ask the user to choose:
                     1=Shoonya, 2=NSE, or 3=yfinance.
    """
    market_categories = {part.strip().lower() for part in category.split(",")}
    needs_market_source = bool(symbol.strip() or {"stocks", "etfs"} & market_categories)
    if needs_market_source:
        from src.importer.source_preference import resolve_data_source

        try:
            data_source, _ = resolve_data_source(data_source)
        except ValueError as exc:
            return f"Invalid data source: {exc}"
    if needs_market_source and not data_source:
        return (
            "DATA_SOURCE_REQUIRED: Ask the user which data source to use before importing:\n"
            "1. Shoonya\n2. NSE\n3. yfinance"
        )
    if symbol and symbol.strip():
        return _import_symbol_data_impl(symbol.strip().upper(), data_source)
    args = ["src/main.py", "import"]
    if data_source:
        args.extend(["--source", data_source])
    if full:
        args.append("--full")
    else:
        args.extend(["--category", category])
    return _run_cmd_streaming(args)


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
def run_fund_mom_returns(
    scheme_code: str | None = None,
    search_query: str | None = None,
    months: int = 12,
) -> str:
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
def run_market_indicators() -> str:
    """
    Run the index valuation, market breadth, sector rotation, and macro indicators scorecard.
    Use this when the user asks for Nifty 50 or Nifty 500 P/E, P/B ratios, market breadth (Advances/Declines,
    percentage of stocks above 50/200 DMA), sector rotation rankings, rupee stress (USDINR deviation),
    or gold ETF (SPDR GLD) whale flows.
    """
    return _run_cmd(["src/scripts/portfolio/market_indicators.py"])


@tool
def run_icici_importer(full: bool = False, month: str = "", fresh: int = 0) -> str:
    """
    Import ICICI Prudential Mutual Fund portfolio holdings into ClickHouse.

    Default (full=False): delta sync — imports the current live portfolio
    snapshot from Morningstar (fast, run once a month). Use for "import ICICI
    holdings", "refresh ICICI data", "import ICICI Pru holdings", etc.

    Set full=True only when asked to "re-import all ICICI history".

    Set month to a specific month (YYYY-MM format) to import just that month.
    Set fresh to N to re-import the N most recent months.
    """
    args = ["src/main.py", "import", "--category", "icici"]
    if full:
        args.append("--full")
    if month:
        args.extend(["--month", month])
    if fresh > 0:
        args.extend(["--fresh", str(fresh)])
    return _run_cmd_streaming(args)


@tool
def run_all_multi_asset_importers(month: str = "", fresh: int = 0) -> str:
    """
    Import holdings for ALL tracked multi-asset funds in one shot.

    Runs DSP, Nippon, Quant, and ICICI Prudential importers sequentially.
    Use when the user says "import all multi asset funds", "refresh all fund holdings",
    "update all AMC holdings", "sync all multi asset", etc.

    Set month to a specific month (YYYY-MM format) to import just that month.
    Set fresh to N to re-import the N most recent months.

    Returns a combined status report for all four importers.
    """
    parts = []
    for label, category in [
        ("DSP",    "dsp"),
        ("Nippon", "nippon"),
        ("Quant",  "quant"),
        ("ICICI",  "icici"),
    ]:
        parts.append(f"=== {label} ===")
        args = ["src/main.py", "import", "--category", category]
        if month:
            args.extend(["--month", month])
        if fresh > 0:
            args.extend(["--fresh", str(fresh)])
        parts.append(_run_cmd_streaming(args))
    return "\n".join(parts)


RUNNER_TOOLS = [
    run_goldbees_pipeline,
    run_daily_signal_composite,
    run_macro_scanner,
    run_etf_news_sentiment,
    run_dsp_multi_asset_importer,
    run_nippon_importer,
    run_quant_importer,
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
]
