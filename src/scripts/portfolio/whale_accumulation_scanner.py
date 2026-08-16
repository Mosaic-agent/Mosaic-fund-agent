"""
Cross-AMC Institutional Whale Accumulation Scanner
====================================================
Scans market_data.mf_holdings for stocks where multiple independent AMCs
are simultaneously building positions (the "consensus signal").

Two outputs per scan:
  1. top_accumulators  — sorted by consensus_score (num_amcs × avg_delta_pp)
  2. fresh_entries     — stocks with zero weight N months ago, now held by
                         min_amcs or more distinct active funds

Usage (standalone):
    ALLOW_LOCAL_RUN=1 PYTHONPATH=. python src/scripts/portfolio/whale_accumulation_scanner.py
    ALLOW_LOCAL_RUN=1 PYTHONPATH=. \\
        python src/scripts/portfolio/whale_accumulation_scanner.py \\
        --amc dsp --months 6 --min-amcs 2 --save
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from src.db.pool import query_df

# ── Passive / fixed-income fund exclusion ─────────────────────────────────────
# Substring patterns in fund_name that identify index trackers, ETF wrappers,
# arbitrage funds, and fixed-income schemes — all ignored for conviction signals.
_PASSIVE_KEYWORDS: tuple[str, ...] = (
    "INDEX", "_ETF", "ETF_", "ARBITRAGE", "GILT", "LIQUID",
    "OVERNIGHT", "_DEBT", "DEBT_", "FMP", "FIXED_MATURITY",
    "CAPITAL_PROTECTION", "DYNAMIC_BOND", "CREDIT_RISK",
    "CORPORATE_BOND",
)


def _is_passive(fund_name: str) -> bool:
    fu = fund_name.upper()
    return any(kw in fu for kw in _PASSIVE_KEYWORDS)


# ── Debt-instrument name patterns ──────────────────────────────────────────────
# mf_holdings.asset_type is stamped per-fund at import time (the fund's own
# category), not per-holding — so equity-oriented funds that also carry a small
# G-Sec/NCD sleeve have those debt lines mislabeled asset_type='equity'. Filter
# them out by name pattern since the asset_type column can't be trusted here.
_DEBT_NAME_RE = re.compile(
    r"^[0-9]+\.[0-9]+%\s|Govt Stock|T-Bill|\(\d{2}/\d{2}/\d{4}\)"
)


def _is_debt_instrument(security_name: str) -> bool:
    return bool(_DEBT_NAME_RE.search(security_name))


# ── Company-suffix spelling normalization ─────────────────────────────────────
# Older BRAND_SCHEME importers (HDFC/ICICI/Kotak/DSP/Nippon/Bajaj/Quant/SBI) stamp
# security_name with the "Ltd" suffix; the newer AMFI-sourced importers (Abakkus,
# Axis, Canara Robeco, Helios, Invesco, Mirae Asset, Motilal Oswal) use "Limited"
# for the exact same companies (e.g. "Reliance Industries Ltd" vs "Reliance
# Industries Limited"). Without normalizing this, the same real-world stock is
# silently split into two separate consensus rows and can never show the full
# cross-AMC picture. Only the suffix token itself is swapped — trailing
# derivative/rights/partly-paid/expiry-dated suffixes (e.g. "... Limited Apr25",
# "... Limited - Partly Paid Up") are deliberately left as distinct strings since
# those are genuinely different instruments, not spelling variants of the same one.
_LIMITED_SUFFIX_RE = re.compile(r"\bLimited\b", re.IGNORECASE)


def _normalize_security_name(security_name: str) -> str:
    return _LIMITED_SUFFIX_RE.sub("Ltd", security_name).strip()


def _get_amc_group(fund_name: str) -> str:
    """Derive the parent AMC group from fund_name, which arrives in one of two
    conventions depending on which importer sourced it:

      1. BRAND_SCHEME_TYPE (e.g. HDFC_FLEXI_CAP, BAJAJ_FINSERV_LIQUID_FUND) — the
         brand is the first underscore-delimited token.
      2. "Human Readable Fund Name" (e.g. 'Axis Flexi Cap Fund', 'Canara Robeco Mid
         Cap Fund', 'Motilal Oswal Multicap Fund') — used by the Abakkus/Axis/Canara
         Robeco/Helios/Invesco/Mirae Asset/Motilal Oswal importers. This has no
         underscore, so the brand is the first space-delimited word instead.
         (A prior version always took fn.split("_")[0] regardless of convention,
         which for every space-separated name returned the ENTIRE uppercased fund
         name as a one-off "AMC group" — each such fund was bucketed alone and could
         never satisfy num_amcs >= min_amcs, silently excluding all 7 of those AMCs'
         holdings from every cross-AMC consensus scan.)

    No hardcoded per-AMC whitelist is used for either convention — the token itself is
    the group key — so newly-imported AMCs need no code change here to be counted.

    Historical Reliance Mutual Fund filings (pre-2019 rename) are folded into NIPPON so the
    same AMC's holdings aren't split across the brand change.
    """
    fn = fund_name.upper()
    if fn.startswith("RELIANCE"):
        return "NIPPON"
    if "_" in fn:
        return fn.split("_")[0]
    return fn.split(" ")[0]


# ── AMFI category flow context ────────────────────────────────────────────────

def _get_category_flow_context() -> dict[str, dict]:
    """
    Load the latest month's AMFI category flows, grouped by subcategory_group.

    Returns:
        {
            "Equity": {"net_flow_cr": 12345.6, "flow_pct_of_aum": 1.8,
                       "direction": "inflow", "report_month": "2026-06-01"},
            ...
        }
        Returns {} if amfi_category_flows is empty (fetcher not yet run).
    """
    try:
        from src.db.pool import query_df
        df_m = query_df("SELECT max(report_month) AS max_m FROM market_data.amfi_category_flows FINAL")
        if df_m is None or df_m.empty or pd.isna(df_m["max_m"].iloc[0]):
            return {}
        max_m_str = str(df_m["max_m"].iloc[0])[:10]

        df = query_df(f"""
            SELECT
                subcategory_group,
                SUM(net_flow_cr)     AS net_flow_cr,
                AVG(flow_pct_of_aum) AS flow_pct_of_aum
            FROM market_data.amfi_category_flows FINAL
            WHERE report_month = '{max_m_str}'
            GROUP BY subcategory_group
        """)
    except Exception:
        return {}

    if df is None or df.empty:
        return {}

    result = {}
    for _, row in df.iterrows():
        net = float(row["net_flow_cr"])
        result[str(row["subcategory_group"])] = {
            "net_flow_cr":    net,
            "flow_pct_of_aum": float(row["flow_pct_of_aum"]),
            "direction":      "inflow" if net >= 0 else "outflow",
            "report_month":   max_m_str,
        }
    return result


# ── Optional technical confirmation (RSI / drawdown / volume surge) ──────────

def _compute_technical_confirmation(isin_map: dict[str, str]) -> dict[str, dict]:
    """
    Bulk-download 1y OHLCV per ISIN via yfinance and compute RSI-14, drawdown from
    the 52-week high, and volume surge (latest / 20d avg volume) — the same
    technical-setup calculation dsp_opportunity_scanner.py already uses for DSP-only
    conviction picks, generalized here to any cross-AMC consensus pick.

    isin_map: {security_name: isin}. Returns {security_name: {rsi, drawdown_pct,
    volume_surge, ticker}} — securities with no usable price history are omitted;
    caller falls back to a neutral tech_score for those.
    """
    import yfinance as yf
    from src.scripts.portfolio.dsp_opportunity_scanner import _rsi

    isins = list(isin_map.values())
    if not isins:
        return {}

    try:
        df_prices = yf.download(isins, period="1y", group_by="ticker", progress=False)
    except Exception:
        return {}

    out: dict[str, dict] = {}
    for security_name, isin in isin_map.items():
        close_series = pd.Series(dtype=float)
        vol_series = pd.Series(dtype=float)
        has_data = False

        if len(isins) == 1:
            if not df_prices.empty:
                close_series = df_prices["Close"]
                vol_series = df_prices["Volume"]
                has_data = True
        elif isin in df_prices.columns.levels[0]:
            close_series = df_prices[isin]["Close"].dropna()
            vol_series = df_prices[isin]["Volume"].dropna()
            has_data = True

        if not has_data or len(close_series) <= 20:
            continue

        latest_close = float(close_series.iloc[-1])
        high_52w = float(close_series.max())
        drawdown_pct = float((latest_close - high_52w) / high_52w * 100) if high_52w > 0 else 0.0
        avg_vol_20d = float(vol_series.rolling(20).mean().iloc[-1])
        latest_vol = float(vol_series.iloc[-1])
        volume_surge = float(latest_vol / avg_vol_20d) if avg_vol_20d > 0 else 1.0

        ticker_symbol = isin
        try:
            ticker_symbol = yf.Ticker(isin).info.get("symbol", isin)
        except Exception:
            pass

        out[security_name] = {
            "rsi": _rsi(close_series),
            "drawdown_pct": drawdown_pct,
            "volume_surge": volume_surge,
            "ticker": ticker_symbol,
        }
    return out


# ── Core scan logic ───────────────────────────────────────────────────────────

def run_whale_scan(
    amc: str = "all",
    lookback_months: int = 3,
    min_amcs: int = 2,
    with_technicals: bool = False,
) -> dict[str, Any]:
    """
    Run the cross-AMC consensus accumulation scan.

    Parameters
    ----------
    amc : "all" | "dsp" | "nippon" | "bajaj" | "icici" | "quant"
        Restrict to a specific AMC group, or "all" for the full universe.
    lookback_months : int
        Number of months for MoM delta calculation (default 3 = latest vs 3 months ago).
    min_amcs : int
        Minimum distinct AMC groups a stock must be held by to qualify as
        consensus (default 2).
    with_technicals : bool
        When True, enrich the top 25 accumulators with RSI-14 / drawdown-from-52w-high /
        volume-surge (via yfinance) and a blended opportunity_score. Adds network latency
        (bulk yfinance download) — off by default.

    Returns
    -------
    dict with keys:
        as_of, prev_month, amc_filter, lookback_months, min_amcs,
        top_accumulators, fresh_entries, top_by_value
    """
    # ── 1. Fetch raw holdings (last lookback_months + 1 distinct months) ──────
    amc_clause = ""
    amc_upper = amc.strip().upper()
    if amc_upper != "ALL":
        amc_clause = f"AND fund_name LIKE '{amc_upper}%'"

    try:
        df_raw = query_df(
            f"""
            SELECT fund_name, as_of_month, security_name, asset_type, isin,
                   toFloat64(pct_of_nav) AS pct_of_nav,
                   toFloat64(market_value_cr) AS market_value_cr
            FROM market_data.mf_holdings FINAL
            WHERE as_of_month >= toStartOfMonth(
                      today() - INTERVAL {lookback_months + 1} MONTH
                  )
              AND asset_type IN ('Equity', 'equity', 'EQUITY')
              {amc_clause}
            ORDER BY as_of_month ASC
            """
        )
    except Exception as exc:
        return {"error": str(exc), "top_accumulators": [], "fresh_entries": [], "top_by_value": []}

    if df_raw.empty:
        return {"error": "No mf_holdings data found — run `import --category nippon dsp` first.",
                "top_accumulators": [], "fresh_entries": [], "top_by_value": []}

    df_raw["security_name"] = df_raw["security_name"].apply(_normalize_security_name)

    # ── 2. Assign AMC group + drop passive funds ──────────────────────────────
    df_raw["amc_group"] = df_raw["fund_name"].apply(_get_amc_group)
    df_raw = df_raw[~df_raw["fund_name"].apply(_is_passive)].copy()
    df_raw = df_raw[~df_raw["security_name"].apply(_is_debt_instrument)].copy()

    if df_raw.empty:
        return {"error": "No active equity holdings after passive fund filter.",
                "top_accumulators": [], "fresh_entries": [], "top_by_value": []}

    # ── ISIN lookup for optional technical confirmation (most common non-empty,
    # non-placeholder ISIN per security; mf_holdings occasionally stamps a synthetic
    # 'PH_'-prefixed ISIN for unlisted/non-tradeable lines) ───────────────────────
    _valid_isin = df_raw[
        df_raw["isin"].notna() & (df_raw["isin"] != "") & (~df_raw["isin"].str.startswith("PH_"))
    ]
    isin_lookup: dict[str, str] = (
        _valid_isin.groupby("security_name")["isin"]
        .agg(lambda s: s.value_counts().index[0])
        .to_dict()
    )

    df_raw["as_of_month"] = pd.to_datetime(df_raw["as_of_month"])

    # ── 3. Identify latest and comparison month (by calendar month) ──────────
    # Different AMCs snapshot on different calendar days (1st, 15th, month-end),
    # so comparing by *exact* as_of_month date starves the scan of cross-AMC
    # breadth whenever one family's snapshot date happens to be globally latest
    # (e.g. only BAJAJ_FINSERV_* funds report on the 15th while every other AMC
    # reports on the 1st/month-end — the exact-date max used to resolve to a
    # date where only one AMC group had any data at all).  Bucket to calendar
    # month instead so every AMC's most recent snapshot in that month counts.
    df_raw["report_month"] = df_raw["as_of_month"].values.astype("datetime64[M]")
    all_report_months = sorted(df_raw["report_month"].unique())
    latest_report_month = all_report_months[-1]
    target_prev = pd.Timestamp(latest_report_month) - pd.DateOffset(months=lookback_months)
    prev_report_month = min(all_report_months, key=lambda m: abs((pd.Timestamp(m) - target_prev).days))

    def _latest_per_fund(df: pd.DataFrame, report_month) -> pd.DataFrame:
        """Within a calendar month, keep each fund's most recent snapshot DATE
        (guards against rare double-imports/backfills landing in the same month) —
        ALL securities from that date, not a single collapsed row. (A prior
        `groupby(...).idxmax()` + `.loc[idx]` here silently kept only one
        arbitrary security per fund per month, discarding the rest of that
        fund's holdings from the scan entirely.)"""
        sub = df[df["report_month"] == report_month]
        max_dates = sub.groupby("fund_name")["as_of_month"].transform("max")
        return sub[sub["as_of_month"] == max_dates].copy()

    df_latest = _latest_per_fund(df_raw, latest_report_month)
    df_prev   = _latest_per_fund(df_raw, prev_report_month)

    latest_month = pd.Timestamp(latest_report_month)
    prev_month   = pd.Timestamp(prev_report_month)

    # ── 4. Aggregate per security_name × amc_group per month ─────────────────
    def _agg(df: pd.DataFrame) -> pd.DataFrame:
        return (
            df.groupby(["security_name", "amc_group"], as_index=False)
            .agg(pct=("pct_of_nav", "sum"), value_cr=("market_value_cr", "sum"))
        )

    lat = _agg(df_latest)
    prv = _agg(df_prev).rename(columns={"pct": "prev_pct", "value_cr": "prev_value_cr"})

    # ── 5. Merge and compute MoM delta ────────────────────────────────────────
    merged = lat.merge(
        prv[["security_name", "amc_group", "prev_pct"]],
        on=["security_name", "amc_group"],
        how="left",
    )
    merged["prev_pct"] = merged["prev_pct"].fillna(0.0)
    merged["delta_pp"] = merged["pct"] - merged["prev_pct"]
    merged["is_fresh"]  = merged["prev_pct"] == 0.0

    # ── 6. Aggregate per security_name across all AMCs ─────────────────────────
    active = merged[merged["pct"] > 0].copy()

    by_sec = (
        active.groupby("security_name", as_index=False)
        .agg(
            num_amcs=("amc_group", "nunique"),
            total_value_cr=("value_cr", "sum"),
            avg_delta_pp=("delta_pp", "mean"),
            amcs=("amc_group", lambda x: sorted(x.unique().tolist())),
        )
    )
    # consensus_score = num_amcs × avg_delta_pp (reward breadth + magnitude)
    by_sec["consensus_score"] = (
        by_sec["num_amcs"] * by_sec["avg_delta_pp"].clip(lower=0.0)
    )

    # Filter to min_amcs
    by_sec_filtered = by_sec[by_sec["num_amcs"] >= min_amcs].copy()
    by_sec_filtered = by_sec_filtered.sort_values("consensus_score", ascending=False)

    # ── 7. Fresh entries (zero-to-hero) ───────────────────────────────────────
    fresh_rows = merged[merged["is_fresh"] & (merged["pct"] > 0)].copy()
    fresh_by_sec = (
        fresh_rows.groupby("security_name", as_index=False)
        .agg(
            amcs_entered=("amc_group", lambda x: sorted(x.unique().tolist())),
            total_value_cr=("value_cr", "sum"),
            total_pct=("pct", "sum"),
        )
    )
    fresh_by_sec["num_amcs"] = fresh_by_sec["amcs_entered"].apply(len)
    fresh_by_sec = (
        fresh_by_sec[fresh_by_sec["num_amcs"] >= min_amcs]
        .sort_values("total_value_cr", ascending=False)
    )

    # ── 8. Top by raw market value (largest bets, any number of AMCs) ─────────
    top_by_value = (
        active.groupby("security_name", as_index=False)
        .agg(
            total_value_cr=("value_cr", "sum"),
            num_amcs=("amc_group", "nunique"),
            amcs=("amc_group", lambda x: sorted(x.unique().tolist())),
        )
        .sort_values("total_value_cr", ascending=False)
        .head(20)
    )

    # ── 9. Build per-security AMC breakup for detail view ────────────────────
    def _breakup(security: str) -> dict[str, dict]:
        rows = merged[merged["security_name"] == security]
        return {
            r["amc_group"]: {
                "pct_nav": round(float(r["pct"]), 3),
                "delta_pp": round(float(r["delta_pp"]), 3),
                "value_cr": round(float(r["value_cr"]), 2),
            }
            for _, r in rows[rows["pct"] > 0].iterrows()
        }

    # ── 9a. Load AMFI category flow context (optional enrichment) ─────────────
    flow_context = _get_category_flow_context()
    # Active equity funds → "Equity" subcategory; fall back to Hybrid if missing
    equity_ctx = flow_context.get("Equity") or flow_context.get("Hybrid") or {}
    flow_confirms_available = bool(equity_ctx)
    amfi_report_month = equity_ctx.get("report_month")

    accumulators: list[dict] = []
    for _, row in by_sec_filtered.head(25).iterrows():
        # category_flow_confirms: True when Equity category saw positive net inflows
        category_flow_confirms: bool | None = None
        flow_pct_of_aum: float | None = None
        if flow_confirms_available:
            category_flow_confirms = equity_ctx.get("direction") == "inflow"
            flow_pct_of_aum = equity_ctx.get("flow_pct_of_aum")

        accumulators.append({
            "security_name":          row["security_name"],
            "num_amcs":               int(row["num_amcs"]),
            "total_value_cr":         round(float(row["total_value_cr"]), 2),
            "avg_delta_pp":           round(float(row["avg_delta_pp"]), 3),
            "consensus_score":        round(float(row["consensus_score"]), 3),
            "amcs":                   row["amcs"],
            "breakup":                _breakup(row["security_name"]),
            "category_flow_confirms": category_flow_confirms,
            "flow_pct_of_aum":        flow_pct_of_aum,
            "amfi_report_month":      amfi_report_month,
            "isin":                   isin_lookup.get(row["security_name"]),
        })

    # ── 9b. Optional technical confirmation (RSI/drawdown/volume) ────────────────
    if with_technicals and accumulators:
        isin_map = {a["security_name"]: a["isin"] for a in accumulators if a.get("isin")}
        technicals = _compute_technical_confirmation(isin_map)
        for a in accumulators:
            t = technicals.get(a["security_name"])
            conviction_score = min(100.0, a["num_amcs"] * 25.0 + max(0.0, a["avg_delta_pp"]) * 40.0)
            rsi_val = t.get("rsi") if t else None
            dd_val = t.get("drawdown_pct") if t else None
            tech_score = 50.0
            if rsi_val is not None and dd_val is not None:
                tech_score = (
                    min(50.0, max(0.0, -dd_val * 1.5))
                    + min(50.0, max(0.0, 75.0 - rsi_val))
                )
            a["rsi"] = rsi_val
            a["drawdown_pct"] = dd_val
            a["volume_surge"] = t.get("volume_surge") if t else None
            a["ticker"] = t.get("ticker") if t else None
            a["opportunity_score"] = round(conviction_score * 0.5 + tech_score * 0.5, 1)

    fresh_entries: list[dict] = []
    for _, row in fresh_by_sec.head(20).iterrows():
        fresh_entries.append({
            "security_name":  row["security_name"],
            "num_amcs":       int(row["num_amcs"]),
            "amcs_entered":   row["amcs_entered"],
            "total_value_cr": round(float(row["total_value_cr"]), 2),
            "total_pct":      round(float(row["total_pct"]), 3),
        })

    top_val: list[dict] = []
    for _, row in top_by_value.iterrows():
        top_val.append({
            "security_name":  row["security_name"],
            "num_amcs":       int(row["num_amcs"]),
            "total_value_cr": round(float(row["total_value_cr"]), 2),
            "amcs":           row["amcs"],
        })

    return {
        "as_of":                latest_month.strftime("%Y-%m-%d"),
        "prev_month":           prev_month.strftime("%Y-%m-%d"),
        "amc_filter":           amc,
        "lookback_months":      lookback_months,
        "min_amcs":             min_amcs,
        "top_accumulators":     accumulators,
        "fresh_entries":        fresh_entries,
        "top_by_value":         top_val,
        "amfi_report_month":    amfi_report_month,
        "flow_confirms_available": flow_confirms_available,
        "with_technicals":      with_technicals,
    }


# ── Rich terminal report printer ──────────────────────────────────────────────

def print_whale_report(results: dict[str, Any], console=None, save: bool = False) -> None:
    """Print a rich terminal report. Optionally saves Markdown to output/."""
    try:
        from rich.console import Console
        from rich.table import Table
        from rich.panel import Panel
        from rich import box as rich_box
    except ImportError:
        print(results)
        return

    if console is None:
        console = Console()

    if "error" in results:
        console.print(f"[bold red]✗ Scan error:[/bold red] {results['error']}")
        return

    as_of = results["as_of"]
    prev  = results["prev_month"]
    amc   = results["amc_filter"].upper()
    nm    = results["lookback_months"]

    console.print(
        Panel(
            f"[bold]🐋 Institutional Whale Accumulation Scanner[/bold]\n"
            f"[dim]Latest: {as_of}  ·  Comparison: {prev}  ·  "
            f"AMC filter: {amc}  ·  min_amcs: {results['min_amcs']}[/dim]",
            border_style="cyan",
        )
    )

    # ── Section 1: Top Accumulators ───────────────────────────────────────────
    acc = results["top_accumulators"]
    has_flow_ctx = results.get("flow_confirms_available", False)
    amfi_month = results.get("amfi_report_month")
    if acc:
        title_str = "🔺 Top Accumulators (consensus_score = num_amcs × avg_delta_pp)"
        if amfi_month:
            title_str += f"  ·  AMFI flows: {amfi_month[:7]}"
        tbl = Table(
            title=title_str,
            box=rich_box.ROUNDED, show_header=True, header_style="bold magenta",
        )
        tbl.add_column("Security",      min_width=28, style="bold")
        tbl.add_column("Score",         justify="right", min_width=7)
        tbl.add_column("AMCs",          justify="center", min_width=5)
        tbl.add_column("AMC Groups",    min_width=20)
        tbl.add_column("Avg Δ (pp)",    justify="right", min_width=10)
        tbl.add_column("Total (₹ Cr)", justify="right", min_width=12)
        if has_flow_ctx:
            tbl.add_column("Flow Confirms?", justify="center", min_width=14)
            tbl.add_column("Cat %AUM",       justify="right",  min_width=9)
        has_tech = results.get("with_technicals", False)
        if has_tech:
            tbl.add_column("RSI-14",     justify="right", min_width=7)
            tbl.add_column("Drawdown %", justify="right", min_width=10)
            tbl.add_column("Vol Surge",  justify="right", min_width=9)
            tbl.add_column("Opp. Score", justify="right", min_width=9, style="bold green")

        for r in acc[:15]:
            delta_str = f"[green]{r['avg_delta_pp']:+.3f}[/green]" if r["avg_delta_pp"] >= 0 \
                        else f"[red]{r['avg_delta_pp']:+.3f}[/red]"
            row_data = [
                r["security_name"][:30],
                f"{r['consensus_score']:.3f}",
                str(r["num_amcs"]),
                ", ".join(r["amcs"]),
                delta_str,
                f"₹{r['total_value_cr']:,.2f}",
            ]
            if has_flow_ctx:
                confirms = r.get("category_flow_confirms")
                fpct = r.get("flow_pct_of_aum")
                flow_str = (
                    "[green]✅ Inflow[/green]" if confirms is True
                    else "[red]⚠ Outflow[/red]" if confirms is False
                    else "[dim]N/A[/dim]"
                )
                fpct_str = f"{fpct:+.2f}%" if fpct is not None else "—"
                row_data += [flow_str, fpct_str]
            if has_tech:
                rsi_v = r.get("rsi")
                dd_v = r.get("drawdown_pct")
                vol_v = r.get("volume_surge")
                score_v = r.get("opportunity_score")
                rsi_col = "green" if rsi_v is not None and rsi_v < 35 else "red" if rsi_v is not None and rsi_v > 70 else "white"
                dd_col = "green" if dd_v is not None and dd_v < -15 else "white"
                row_data += [
                    f"[{rsi_col}]{rsi_v:.1f}[/{rsi_col}]" if rsi_v is not None else "—",
                    f"[{dd_col}]{dd_v:+.1f}%[/{dd_col}]" if dd_v is not None else "—",
                    f"{vol_v:.2f}x" if vol_v is not None else "—",
                    f"{score_v:.1f}/100" if score_v is not None else "—",
                ]
            tbl.add_row(*row_data)
        console.print(tbl)
        if has_flow_ctx:
            console.print(
                "[dim]Flow Confirms = Equity category saw positive AMFI net inflows "
                "(from market_data.amfi_category_flows)[/dim]"
            )
        else:
            console.print(
                "[dim]ℹ Run [bold]python src/main.py import --category amfi_flows[/bold] "
                "to add category flow confirmation signal[/dim]"
            )
    else:
        console.print("[yellow]No consensus accumulators found (try reducing --min-amcs).[/yellow]")

    # ── Section 2: Fresh Entries ──────────────────────────────────────────────
    fresh = results["fresh_entries"]
    if fresh:
        ftbl = Table(
            title=f"🆕 Zero-to-Hero: Fresh Entries Last {nm}M (had 0% weight before)",
            box=rich_box.SIMPLE_HEAD, show_header=True, header_style="bold cyan",
        )
        ftbl.add_column("Security",       min_width=30, style="bold")
        ftbl.add_column("AMCs Entered",   min_width=20)
        ftbl.add_column("# AMCs",         justify="center", min_width=6)
        ftbl.add_column("Total (₹ Cr)",  justify="right", min_width=12)
        ftbl.add_column("Combined Wt %", justify="right", min_width=13)

        for r in fresh[:12]:
            ftbl.add_row(
                r["security_name"][:35],
                ", ".join(r["amcs_entered"]),
                str(r["num_amcs"]),
                f"₹{r['total_value_cr']:,.2f}",
                f"{r['total_pct']:.3f}%",
            )
        console.print(ftbl)
    else:
        console.print("[dim]No fresh entries found in this window.[/dim]")

    # ── Section 3: Biggest Institutional Bets ────────────────────────────────
    console.print("\n[bold]Top 10 by Aggregate Market Value (₹ Cr):[/bold]")
    vtbl = Table(box=rich_box.SIMPLE, show_header=True, header_style="bold")
    vtbl.add_column("Security",      min_width=30)
    vtbl.add_column("AMCs",          justify="center", min_width=5)
    vtbl.add_column("AMC Groups",    min_width=20)
    vtbl.add_column("Total (₹ Cr)", justify="right", min_width=12)
    for r in results["top_by_value"][:10]:
        vtbl.add_row(
            r["security_name"][:35],
            str(r["num_amcs"]),
            ", ".join(r["amcs"]),
            f"₹{r['total_value_cr']:,.2f}",
        )
    console.print(vtbl)

    # ── Save report ───────────────────────────────────────────────────────────
    if save:
        _save_markdown(results)


def _save_markdown(results: dict[str, Any]) -> None:
    """Save a Markdown summary of the scan to output/whale_accumulation_report.md."""
    has_tech = results.get("with_technicals", False)
    acc_header = "| Security | Score | AMCs | AMC Groups | Avg Δ (pp) | Total (₹ Cr) | Flow Confirms | Cat %AUM |"
    acc_sep    = "| :--- | ---: | ---: | :--- | ---: | ---: | :---: | ---: |"
    if has_tech:
        acc_header += " RSI-14 | Drawdown % | Vol Surge | Opp. Score |"
        acc_sep    += " ---: | ---: | ---: | ---: |"

    lines = [
        f"# 🐋 Whale Accumulation Report",
        f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  ",
        f"**Latest month:** {results['as_of']}  "
        f"| **Comparison month:** {results['prev_month']}  ",
        f"**AMC filter:** {results['amc_filter'].upper()}  "
        f"| **min_amcs:** {results['min_amcs']}",
        "",
        "---",
        "",
        "## 🔺 Top Accumulators (consensus_score)",
        "",
        acc_header,
        acc_sep,
    ]
    for r in results["top_accumulators"][:20]:
        confirms = r.get("category_flow_confirms")
        fpct = r.get("flow_pct_of_aum")
        flow_str = "✅" if confirms is True else ("⚠️" if confirms is False else "N/A")
        fpct_str = f"{fpct:+.2f}%" if fpct is not None else "—"
        row = (
            f"| {r['security_name']} | {r['consensus_score']:.3f} "
            f"| {r['num_amcs']} | {', '.join(r['amcs'])} "
            f"| {r['avg_delta_pp']:+.3f} | ₹{r['total_value_cr']:,.2f} "
            f"| {flow_str} | {fpct_str} |"
        )
        if has_tech:
            rsi_v = r.get("rsi")
            dd_v = r.get("drawdown_pct")
            vol_v = r.get("volume_surge")
            score_v = r.get("opportunity_score")
            rsi_str = f"{rsi_v:.1f}" if rsi_v is not None else "—"
            dd_str = f"{dd_v:+.1f}%" if dd_v is not None else "—"
            vol_str = f"{vol_v:.2f}x" if vol_v is not None else "—"
            score_str = f"{score_v:.1f}/100" if score_v is not None else "—"
            row += f" {rsi_str} | {dd_str} | {vol_str} | {score_str} |"
        lines.append(row)

    lines += ["", "---", "", "## 🆕 Zero-to-Hero Fresh Entries", "",
              "| Security | AMCs Entered | # AMCs | Total (₹ Cr) | Wt % |",
              "| :--- | :--- | ---: | ---: | ---: |"]
    for r in results["fresh_entries"][:15]:
        lines.append(
            f"| {r['security_name']} | {', '.join(r['amcs_entered'])} "
            f"| {r['num_amcs']} | ₹{r['total_value_cr']:,.2f} | {r['total_pct']:.3f}% |"
        )

    lines += ["", "---", "", "## 🏆 Largest Institutional Bets (by ₹ Value)", "",
              "| Security | AMCs | AMC Groups | Total (₹ Cr) |",
              "| :--- | ---: | :--- | ---: |"]
    for r in results["top_by_value"][:10]:
        lines.append(
            f"| {r['security_name']} | {r['num_amcs']} "
            f"| {', '.join(r['amcs'])} | ₹{r['total_value_cr']:,.2f} |"
        )

    out = Path("output/whale_accumulation_report.md")
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(lines))
    print(f"\nReport saved → {out.absolute()}")


# ── CLI entry point ───────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Cross-AMC Institutional Whale Accumulation Scanner"
    )
    parser.add_argument(
        "--amc", default="all",
        help="AMC group: all | dsp | nippon | bajaj | icici | quant (default: all)"
    )
    parser.add_argument(
        "--months", type=int, default=3,
        help="Lookback months for MoM delta (default: 3)"
    )
    parser.add_argument(
        "--min-amcs", type=int, default=2,
        help="Minimum distinct AMC groups (default: 2)"
    )
    parser.add_argument(
        "--save", action="store_true",
        help="Save Markdown report to output/whale_accumulation_report.md"
    )
    parser.add_argument(
        "--with-technicals", action="store_true",
        help="Enrich top accumulators with RSI/drawdown/volume-surge technical "
             "confirmation (adds yfinance latency)"
    )
    args = parser.parse_args()

    from rich.console import Console
    console = Console()
    results = run_whale_scan(
        amc=args.amc,
        lookback_months=args.months,
        min_amcs=args.min_amcs,
        with_technicals=args.with_technicals,
    )
    print_whale_report(results, console, save=args.save)


if __name__ == "__main__":
    main()
