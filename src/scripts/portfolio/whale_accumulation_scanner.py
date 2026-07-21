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


def _get_amc_group(fund_name: str) -> str:
    fn = fund_name.upper()
    if fn.startswith("DSP"):     return "DSP"
    if fn.startswith("NIPPON"):  return "NIPPON"
    if fn.startswith("BAJAJ"):   return "BAJAJ"
    if fn.startswith("ICICI"):   return "ICICI"
    if fn.startswith("QUANT"):   return "QUANT"
    return "OTHER"


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


# ── Core scan logic ───────────────────────────────────────────────────────────

def run_whale_scan(
    amc: str = "all",
    lookback_months: int = 3,
    min_amcs: int = 2,
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
            SELECT fund_name, as_of_month, security_name, asset_type,
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

    # ── 2. Assign AMC group + drop passive funds ──────────────────────────────
    df_raw["amc_group"] = df_raw["fund_name"].apply(_get_amc_group)
    df_raw = df_raw[~df_raw["fund_name"].apply(_is_passive)].copy()

    if df_raw.empty:
        return {"error": "No active equity holdings after passive fund filter.",
                "top_accumulators": [], "fresh_entries": [], "top_by_value": []}

    df_raw["as_of_month"] = pd.to_datetime(df_raw["as_of_month"])

    # ── 3. Identify latest and comparison month ───────────────────────────────
    all_months = sorted(df_raw["as_of_month"].unique())
    latest_month = all_months[-1]
    # Find the comparison month: closest to `lookback_months` ago
    target_prev = latest_month - pd.DateOffset(months=lookback_months)
    prev_month = min(all_months, key=lambda m: abs((m - target_prev).days))

    df_latest = df_raw[df_raw["as_of_month"] == latest_month].copy()
    df_prev   = df_raw[df_raw["as_of_month"] == prev_month].copy()

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
        })

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
        "| Security | Score | AMCs | AMC Groups | Avg Δ (pp) | Total (₹ Cr) | Flow Confirms | Cat %AUM |",
        "| :--- | ---: | ---: | :--- | ---: | ---: | :---: | ---: |",
    ]
    for r in results["top_accumulators"][:20]:
        confirms = r.get("category_flow_confirms")
        fpct = r.get("flow_pct_of_aum")
        flow_str = "✅" if confirms is True else ("⚠️" if confirms is False else "N/A")
        fpct_str = f"{fpct:+.2f}%" if fpct is not None else "—"
        lines.append(
            f"| {r['security_name']} | {r['consensus_score']:.3f} "
            f"| {r['num_amcs']} | {', '.join(r['amcs'])} "
            f"| {r['avg_delta_pp']:+.3f} | ₹{r['total_value_cr']:,.2f} "
            f"| {flow_str} | {fpct_str} |"
        )

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
    args = parser.parse_args()

    from rich.console import Console
    console = Console()
    results = run_whale_scan(
        amc=args.amc,
        lookback_months=args.months,
        min_amcs=args.min_amcs,
    )
    print_whale_report(results, console, save=args.save)


if __name__ == "__main__":
    main()
