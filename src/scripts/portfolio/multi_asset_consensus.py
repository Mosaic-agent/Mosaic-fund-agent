"""
scripts/portfolio/multi_asset_consensus.py
───────────────────────────────────────────
Cross-fund pattern detection across all multi-asset mutual funds in
`market_data.mf_holdings`.

Where `multi_asset_holdings_mom_yoy.py` analyses a single fund, this script
finds **consensus signals**: securities that multiple multi-asset funds are
simultaneously adding to or trimming from in the same month / over a year.

This is the institutional "smart money" overlap signal: if 4 of 7 multi-asset
funds raised their gold ETF exposure by ≥0.5 pct-pts in the same month, that
is a much stronger signal than any single fund acting alone.

Usage:
    # Latest month MoM consensus across all multi-asset funds (default)
    python src/scripts/portfolio/multi_asset_consensus.py

    # YoY (latest vs 12 months back) instead of MoM
    python src/scripts/portfolio/multi_asset_consensus.py --period yoy

    # Lower the minimum-fund threshold (default 2 funds)
    python src/scripts/portfolio/multi_asset_consensus.py --min-funds 3

    # Restrict to a specific asset class
    python src/scripts/portfolio/multi_asset_consensus.py --asset gold

    # Show more rows
    python src/scripts/portfolio/multi_asset_consensus.py --top 30
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import date
from typing import Optional

import pandas as pd

sys.path.append(os.getcwd())

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich import box

from src.db.pool import get_pool

console = Console()


# ──────────────────────────────────────────────────────────────────────────
# The 7 multi-asset funds tracked by whale_tracker.py.
# Kept here as the canonical roster for cross-fund analysis.
# ──────────────────────────────────────────────────────────────────────────

MULTI_ASSET_FUNDS = [
    {"label": "Nippon Multi Asset",     "filter": "scheme_code = 'RLMF806'"},
    {"label": "Nippon Multi Asset FoF", "filter": "scheme_code = 'RLMF811'"},
    {"label": "DSP Multi Asset",        "filter": "scheme_code = '152056'"},
    {"label": "DSP Multi Asset Omni",   "filter": "scheme_code = '154167'"},
    {"label": "Bajaj Multi Asset",      "filter": "scheme_code = '152639'"},
    {"label": "Quant Multi Asset",      "filter": "scheme_code = '120821'"},
    {"label": "ICICI Multi Asset",      "filter": "fund_name = 'ICICI_MULTI_ASSET'"},
]


# ──────────────────────────────────────────────────────────────────────────
# Snapshot fetching
# ──────────────────────────────────────────────────────────────────────────

def fund_month_list(fund_filter: str) -> list[date]:
    pool = get_pool()
    df = pool.query_df(
        f"""
        SELECT DISTINCT as_of_month
        FROM market_data.mf_holdings FINAL
        WHERE {fund_filter}
        ORDER BY as_of_month
        """
    )
    if df.empty:
        return []
    return [pd.to_datetime(m).date() for m in df["as_of_month"].tolist()]


def fund_snapshot(fund_filter: str, as_of_month: date,
                  asset: Optional[str]) -> pd.DataFrame:
    pool = get_pool()
    extra = f"AND asset_type = '{asset}'" if asset else ""
    df = pool.query_df(
        f"""
        SELECT
            security_name,
            any(asset_type)      AS sec_asset_type,
            sum(pct_of_nav)      AS pct_of_nav,
            sum(market_value_cr) AS market_value_cr
        FROM market_data.mf_holdings FINAL
        WHERE {fund_filter}
          AND as_of_month = '{as_of_month}'
          {extra}
        GROUP BY security_name
        """
    )
    if not df.empty and "sec_asset_type" in df.columns:
        df = df.rename(columns={"sec_asset_type": "asset_type"})
    return df


# ──────────────────────────────────────────────────────────────────────────
# Per-fund delta builder
# ──────────────────────────────────────────────────────────────────────────

def fund_deltas(label: str, fund_filter: str, period: str,
                asset: Optional[str]) -> pd.DataFrame:
    months = fund_month_list(fund_filter)
    if len(months) == 0:
        return pd.DataFrame()  # no data at all — skip

    curr = months[-1]

    if len(months) < 2:
        return pd.DataFrame()  # Cannot compute delta shifts without at least two months of history

    if period == "mom":
        prev = months[-2]
    else:  # yoy
        if len(months) < 13:
            # Fall back to oldest available month for pseudo-YoY
            prev = months[0]
        else:
            prev = months[-13]

    curr_snap = fund_snapshot(fund_filter, curr, asset)
    prev_snap = fund_snapshot(fund_filter, prev, asset)
    if curr_snap.empty and prev_snap.empty:
        return pd.DataFrame()

    if prev_snap.empty:
        merged = curr_snap.copy()
        merged["prev_pct"] = 0.0
    elif curr_snap.empty:
        merged = prev_snap.rename(columns={"pct_of_nav": "prev_pct"})
        merged["pct_of_nav"] = 0.0
    else:
        prev_slim = prev_snap[["security_name", "pct_of_nav"]].rename(
            columns={"pct_of_nav": "prev_pct"}
        )
        merged = curr_snap.merge(prev_slim, on="security_name", how="outer")
        merged["pct_of_nav"] = merged["pct_of_nav"].fillna(0.0)
        merged["prev_pct"]   = merged["prev_pct"].fillna(0.0)

    if "asset_type" not in merged.columns:
        merged["asset_type"] = ""
    merged["asset_type"] = merged["asset_type"].fillna("")
    merged["delta_pct"]  = merged["pct_of_nav"] - merged["prev_pct"]
    merged["fund_label"] = label
    merged["curr_month"] = curr
    merged["prev_month"] = prev
    return merged[[
        "fund_label", "security_name", "asset_type",
        "prev_pct", "pct_of_nav", "delta_pct", "curr_month", "prev_month",
    ]]


# ──────────────────────────────────────────────────────────────────────────
# Cross-fund aggregator
# ──────────────────────────────────────────────────────────────────────────

def _normalize_security_name(name: str) -> str:
    """
    Light normalisation so the same security under slightly different vendor
    labels aggregates together. Conservative — only strips trailing markers,
    extra whitespace, and case.
    """
    if not isinstance(name, str):
        return ""
    n = name.strip()
    for suffix in ("**", "*", " LTD.", " LTD", " LIMITED"):
        if n.upper().endswith(suffix):
            n = n[: -len(suffix)].rstrip()
    return n.upper()


def cross_fund_consensus(period: str, asset: Optional[str],
                         min_delta: float) -> tuple[pd.DataFrame, int, int, int]:
    """
    Returns:
        consensus_df       : one row per security with aggregated metrics
        n_funds_used       : how many funds contributed delta rows
        n_funds_skip       : how many funds had no data at all
        n_funds_baseline0  : how many funds contributed with prev=0 (single-month)
    """
    all_deltas: list[pd.DataFrame] = []
    used = skipped = baseline_zero = 0
    for f in MULTI_ASSET_FUNDS:
        df = fund_deltas(f["label"], f["filter"], period, asset)
        if df.empty:
            skipped += 1
            continue
        # Detect single-month funds (prev_month is None)
        if df["prev_month"].isnull().all():
            baseline_zero += 1
        used += 1
        all_deltas.append(df)

    if not all_deltas:
        return pd.DataFrame(), 0, skipped, baseline_zero

    combined = pd.concat(all_deltas, ignore_index=True)
    combined["security_key"] = combined["security_name"].map(_normalize_security_name)

    # Aggregate per security across funds
    grouped = combined.groupby("security_key").agg(
        canonical_name=("security_name",
                        lambda s: s.value_counts().idxmax()),  # most common variant
        asset_type=("asset_type",
                    lambda s: s.value_counts().idxmax() if len(s) else ""),
        n_funds_add=("delta_pct", lambda s: int((s >= min_delta).sum())),
        n_funds_trim=("delta_pct", lambda s: int((s <= -min_delta).sum())),
        n_funds_hold=("delta_pct", lambda s: int(((s > -min_delta) & (s < min_delta)).sum())),
        avg_delta=("delta_pct", "mean"),
        total_delta=("delta_pct", "sum"),
        funds_moving=("fund_label",
                      lambda s: ", ".join(sorted(set(s)))),
    ).reset_index(drop=True)

    grouped["net_funds"]      = grouped["n_funds_add"] - grouped["n_funds_trim"]
    grouped["consensus_size"] = grouped[["n_funds_add", "n_funds_trim"]].max(axis=1)
    return grouped, used, skipped, baseline_zero


# ──────────────────────────────────────────────────────────────────────────
# Asset-class consensus rotation
# ──────────────────────────────────────────────────────────────────────────

def asset_class_rotation(period: str) -> pd.DataFrame:
    """
    For each asset_type, count how many of the 7 funds increased vs decreased
    weight in the period — the asset-class equivalent of cross-fund consensus.
    """
    rows = []
    for f in MULTI_ASSET_FUNDS:
        months = fund_month_list(f["filter"])
        if len(months) == 0:
            continue
        curr = months[-1]

        if len(months) < 2:
            continue

        if period == "mom":
            prev = months[-2]
        else:
            prev = months[0] if len(months) < 13 else months[-13]

        pool = get_pool()
        df = pool.query_df(
            f"""
            SELECT as_of_month, asset_type, sum(pct_of_nav) AS weight_pct
            FROM market_data.mf_holdings FINAL
            WHERE {f['filter']}
              AND as_of_month IN ('{curr}', '{prev}')
            GROUP BY as_of_month, asset_type
            """
        )
        if df.empty:
            continue
        pivot = df.pivot_table(index="asset_type", columns="as_of_month",
                               values="weight_pct", fill_value=0.0)
        # Find the curr/prev columns regardless of pandas Timestamp coercion
        cols_sorted = sorted(pivot.columns, key=lambda c: pd.to_datetime(c))
        if len(cols_sorted) < 2:
            continue
        prev_col, curr_col = cols_sorted[0], cols_sorted[-1]
        for asset_type, prow in pivot.iterrows():
            rows.append({
                "fund": f["label"],
                "asset_type": asset_type,
                "prev_pct": prow[prev_col],
                "curr_pct": prow[curr_col],
                "delta": prow[curr_col] - prow[prev_col],
            })

    if not rows:
        return pd.DataFrame()

    raw = pd.DataFrame(rows)
    summary = raw.groupby("asset_type").agg(
        n_funds_seen=("fund", "nunique"),
        n_funds_add=("delta", lambda s: int((s >= 0.25).sum())),
        n_funds_trim=("delta", lambda s: int((s <= -0.25).sum())),
        avg_delta=("delta", "mean"),
    ).reset_index()
    summary["net_funds"] = summary["n_funds_add"] - summary["n_funds_trim"]
    summary = summary.sort_values("net_funds", ascending=False)
    return summary


# ──────────────────────────────────────────────────────────────────────────
# Overlap Analysis & Rendering
# ──────────────────────────────────────────────────────────────────────────

def portfolio_overlap(asset: Optional[str] = None) -> pd.DataFrame:
    """
    Find common holdings across all 7 multi-asset funds in their latest snapshot.
    Does not compute deltas, just shows absolute portfolio overlap.
    """
    all_snaps = []
    for f in MULTI_ASSET_FUNDS:
        months = fund_month_list(f["filter"])
        if not months:
            continue
        curr = months[-1]
        df = fund_snapshot(f["filter"], curr, asset)
        if not df.empty:
            df["fund_label"] = f["label"]
            all_snaps.append(df)
            
    if not all_snaps:
        return pd.DataFrame()
        
    combined = pd.concat(all_snaps, ignore_index=True)
    combined["security_key"] = combined["security_name"].map(_normalize_security_name)
    
    grouped = combined.groupby("security_key").agg(
        canonical_name=("security_name", lambda s: s.value_counts().idxmax()),
        asset_type=("asset_type", lambda s: s.value_counts().idxmax() if len(s) else ""),
        n_funds=("fund_label", "nunique"),
        avg_pct=("pct_of_nav", "mean"),
        total_mv_cr=("market_value_cr", "sum"),
        funds_holding=("fund_label", lambda s: ", ".join(sorted(set(s)))),
    ).reset_index(drop=True)
    
    grouped = grouped.sort_values(["n_funds", "avg_pct"], ascending=[False, False])
    return grouped


def render_portfolio_overlap(df: pd.DataFrame, top: int, min_funds: int) -> None:
    if df.empty:
        console.print("[yellow]No portfolio overlap data.[/yellow]")
        return
    flt = df[df["n_funds"] >= min_funds].head(top)
    if flt.empty:
        console.print(f"[dim]No overlap holdings at min_funds={min_funds}.[/dim]")
        return
        
    t = Table(title=f"Portfolio Overlap (Core Holdings) — shared by ≥{min_funds} funds", box=box.ROUNDED, header_style="bold cyan")
    t.add_column("Security", min_width=28, overflow="fold")
    t.add_column("Asset", width=8)
    t.add_column("# funds", justify="right", width=8)
    t.add_column("Avg Weight", justify="right", width=10)
    t.add_column("Total Value (Cr)", justify="right", width=15)
    t.add_column("Funds holding", min_width=20, overflow="fold")
    
    for _, r in flt.iterrows():
        t.add_row(
            r["canonical_name"],
            r["asset_type"] or "—",
            str(int(r["n_funds"])),
            f"{r['avg_pct']:.2f}%",
            f"₹{r['total_mv_cr']:.1f}",
            r["funds_holding"],
        )
    console.print(t)


# ──────────────────────────────────────────────────────────────────────────
# Rendering
# ──────────────────────────────────────────────────────────────────────────

def render_security_consensus(df: pd.DataFrame, kind: str, top: int,
                              min_funds: int, period_label: str) -> None:
    if df.empty:
        console.print(f"[yellow]No consensus {kind} signals.[/yellow]")
        return

    if kind == "add":
        flt = df[df["n_funds_add"] >= min_funds].copy()
        flt = flt.sort_values(["n_funds_add", "avg_delta"], ascending=[False, False])
        title    = f"Consensus ADDS ({period_label}) — held / increased by ≥{min_funds} funds"
        delta_color = "green"
        signal_col  = "n_funds_add"
    else:
        flt = df[df["n_funds_trim"] >= min_funds].copy()
        flt = flt.sort_values(["n_funds_trim", "avg_delta"], ascending=[False, True])
        title    = f"Consensus TRIMS ({period_label}) — reduced / exited by ≥{min_funds} funds"
        delta_color = "red"
        signal_col  = "n_funds_trim"

    flt = flt.head(top)
    if flt.empty:
        console.print(f"[dim]No {kind} signals at min_funds={min_funds}.[/dim]")
        return

    t = Table(title=title, box=box.ROUNDED, header_style=f"bold {delta_color}")
    t.add_column("Security", min_width=28, overflow="fold")
    t.add_column("Asset", width=8)
    t.add_column("# funds", justify="right", width=8)
    t.add_column("Avg Δ", justify="right", width=9)
    t.add_column("Net", justify="right", width=6)
    t.add_column("Funds moving", min_width=20, overflow="fold")

    for _, r in flt.iterrows():
        avg_str = f"{r['avg_delta']:+.2f}"
        net_str = f"{int(r['net_funds']):+d}"
        t.add_row(
            r["canonical_name"],
            r["asset_type"] or "—",
            str(int(r[signal_col])),
            f"[{delta_color}]{avg_str}[/{delta_color}]",
            net_str,
            r["funds_moving"],
        )
    console.print(t)


def render_asset_rotation(df: pd.DataFrame, period_label: str) -> None:
    if df.empty:
        console.print("[yellow]No asset-class rotation data.[/yellow]")
        return
    t = Table(
        title=f"Asset-Class Rotation ({period_label}) across multi-asset funds",
        box=box.ROUNDED,
        header_style="bold cyan",
    )
    t.add_column("Asset Type", style="bold")
    t.add_column("Funds seen", justify="right")
    t.add_column("# adding", justify="right")
    t.add_column("# trimming", justify="right")
    t.add_column("Net", justify="right")
    t.add_column("Avg Δ (pct-pts)", justify="right")
    for _, r in df.iterrows():
        net = int(r["net_funds"])
        net_color = "green" if net > 0 else ("red" if net < 0 else "dim")
        t.add_row(
            r["asset_type"],
            str(int(r["n_funds_seen"])),
            str(int(r["n_funds_add"])),
            str(int(r["n_funds_trim"])),
            f"[{net_color}]{net:+d}[/{net_color}]",
            f"{r['avg_delta']:+.2f}",
        )
    console.print(t)


# ──────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────

def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Cross-fund consensus pattern detector across the 7 multi-asset "
            "Indian mutual funds in mf_holdings."
        )
    )
    parser.add_argument("--period", choices=["mom", "yoy"], default="mom",
                        help="MoM (latest vs prev month) or YoY (vs 12 months back). Default: mom.")
    parser.add_argument("--min-funds", type=int, default=2,
                        help="Min number of funds moving the same way to count as consensus (default 2).")
    parser.add_argument("--min-delta", type=float, default=0.10,
                        help="Min |Δ pct-pts| per fund to count as an add/trim (default 0.10).")
    parser.add_argument("--asset", default="",
                        help="Restrict to a single asset_type (e.g. gold, equity, bond).")
    parser.add_argument("--top",  type=int, default=15,
                        help="Top N rows to display per side (default 15).")
    parser.add_argument("--no-rotation", action="store_true",
                        help="Skip the asset-class rotation summary.")
    args = parser.parse_args()

    period_label = "MoM" if args.period == "mom" else "YoY (12-mo)"
    asset = args.asset.strip().lower() or None

    console.print(Panel.fit(
        f"[bold cyan]Multi-Asset Consensus[/bold cyan]  ·  "
        f"period=[bold]{period_label}[/bold]  ·  "
        f"min_funds=[bold]{args.min_funds}[/bold]  ·  "
        f"min_delta=[bold]{args.min_delta:.2f}[/bold]"
        + (f"  ·  asset=[bold]{asset}[/bold]" if asset else ""),
        title="Cross-Fund Pattern Detection",
        border_style="cyan",
    ))

    # 1. Core Holdings Overlap (across all active funds)
    overlap = portfolio_overlap(asset)
    n_overlap_funds = 0
    for f in MULTI_ASSET_FUNDS:
        if fund_month_list(f["filter"]):
            n_overlap_funds += 1
            
    console.print(f"[dim]Funds analysed for holdings overlap: {n_overlap_funds}[/dim]")
    if not overlap.empty:
        render_portfolio_overlap(overlap, args.top, args.min_funds)
        console.print()

    # 2. Consensus Shifts (Adds & Trims - delta analysis)
    consensus, n_used, n_skipped, n_baseline0 = cross_fund_consensus(
        args.period, asset, args.min_delta
    )
    
    # Auto-adjust min_funds if number of funds with history is smaller than min_funds
    shift_min_funds = min(args.min_funds, max(1, n_used))
    
    status = f"[dim]Funds analysed for active shifts ({period_label}): {n_used}"
    if n_skipped:
        status += f"  ·  {n_skipped} skipped (no data)"
    status += "[/dim]"
    console.print(status)

    if not consensus.empty:
        render_security_consensus(consensus, "add",  args.top, shift_min_funds, period_label)
        console.print()
        render_security_consensus(consensus, "trim", args.top, shift_min_funds, period_label)
    else:
        console.print(f"[dim]No active shift consensus signals (insufficient multi-month history).[/dim]")

    if not args.no_rotation:
        console.print()
        rotation = asset_class_rotation(args.period)
        if not rotation.empty:
            render_asset_rotation(rotation, period_label)

    return 0


if __name__ == "__main__":
    sys.exit(main())
