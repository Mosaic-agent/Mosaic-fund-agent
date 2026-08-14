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
from src.tools.mf_sector_analyzer import classify_sector

console = Console()


# ──────────────────────────────────────────────────────────────────────────
# The 7 multi-asset funds tracked by whale_tracker.py.
# Kept here as the canonical roster for cross-fund analysis.
# ──────────────────────────────────────────────────────────────────────────

MULTI_ASSET_FUNDS = [
    {"label": "Nippon Multi Asset",     "filter": "fund_name LIKE 'NIPPON%MULTI_ASSET%' OR scheme_code = 'RLMF806'"},
    {"label": "Nippon Multi Asset FoF", "filter": "fund_name LIKE 'NIPPON%MULTI_ASSET%FOF' OR scheme_code = 'RLMF811'"},
    {"label": "DSP Multi Asset",        "filter": "fund_name = 'DSP_MULTI_ASSET' OR scheme_code = '152056'"},
    {"label": "DSP Multi Asset Omni",   "filter": "fund_name = 'DSP_MULTI_ASSET_OMNI_FOF' OR scheme_code = '154167'"},
    {"label": "Bajaj Multi Asset",      "filter": "fund_name = 'BAJAJ_MULTI_ASSET' OR scheme_code = '152639'"},
    {"label": "Quant Multi Asset",      "filter": "fund_name = 'QUANT_MULTI_ASSET' OR scheme_code = '120821'"},
    {"label": "ICICI Multi Asset",      "filter": "fund_name = 'ICICI_MULTI_ASSET' OR scheme_code IN ('120334', '120716')"},
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
        WHERE ({fund_filter})
        ORDER BY as_of_month
        """
    )
    if df.empty or "as_of_month" not in df.columns:
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
        WHERE ({fund_filter})
          AND as_of_month = '{as_of_month}'
          {extra}
        GROUP BY security_name
        """
    )
    if not df.empty and "sec_asset_type" in df.columns:
        df = df.rename(columns={"sec_asset_type": "asset_type"})
    elif df.empty:
        return pd.DataFrame(columns=["security_name", "asset_type", "pct_of_nav", "market_value_cr"])
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

def _rotation_streak(weight_series: pd.Series) -> tuple[int, str]:
    """
    weight_series: an asset_type's weight_pct indexed by as_of_month, ascending.
    Walks backward from the most recent month and counts consecutive
    month-over-month moves in the same direction. Returns (streak_months, direction).
    A single-month tick is streak=1 — only a run of several same-direction
    months counts as "persistent" downstream.
    """
    diffs = weight_series.diff().dropna().tolist()
    if not diffs:
        return 0, "flat"
    streak = 0
    direction = None
    for d in reversed(diffs):
        if d > 1e-9:
            sign = "up"
        elif d < -1e-9:
            sign = "down"
        else:
            break
        if direction is None:
            direction = sign
        elif sign != direction:
            break
        streak += 1
    return streak, direction or "flat"


def asset_class_rotation(lookback_months: int = 12, min_streak_months: int = 3) -> pd.DataFrame:
    """
    For each asset_type, score how many of the 7 multi-asset funds have been
    *persistently* rotating into vs. out of it over `lookback_months` — a fund
    with several consecutive same-direction monthly moves counts as persistent,
    distinct from one that ticked up or down in a single month and could
    reverse next month. This is the asset-class analogue of the smallcap
    cross-conviction persistence fix (see fetch_cross_conviction).
    """
    rows = []
    for f in MULTI_ASSET_FUNDS:
        months = fund_month_list(f["filter"])
        if len(months) < 2:
            continue
        window = months[-lookback_months:] if len(months) > lookback_months else months
        if len(window) < 2:
            continue

        pool = get_pool()
        month_list_sql = ", ".join(f"'{m}'" for m in window)
        df = pool.query_df(
            f"""
            SELECT as_of_month, asset_type, sum(pct_of_nav) AS weight_pct
            FROM market_data.mf_holdings FINAL
            WHERE {f['filter']}
              AND as_of_month IN ({month_list_sql})
            GROUP BY as_of_month, asset_type
            """
        )
        if df.empty:
            continue
        pivot = df.pivot_table(index="asset_type", columns="as_of_month",
                               values="weight_pct", fill_value=0.0)
        month_cols = sorted(pivot.columns, key=lambda c: pd.to_datetime(c))
        pivot = pivot[month_cols]

        for asset_type, prow in pivot.iterrows():
            series = prow.astype(float)
            streak_months, direction = _rotation_streak(series)
            rows.append({
                "fund": f["label"],
                "asset_type": asset_type,
                "latest_pct": float(series.iloc[-1]),
                "total_delta_window": float(series.iloc[-1] - series.iloc[0]),
                "streak_months": streak_months,
                "direction": direction,
            })

    if not rows:
        return pd.DataFrame()

    raw = pd.DataFrame(rows)
    raw["is_persistent_add"] = (raw["direction"] == "up") & (raw["streak_months"] >= min_streak_months)
    raw["is_persistent_trim"] = (raw["direction"] == "down") & (raw["streak_months"] >= min_streak_months)

    summary = raw.groupby("asset_type").agg(
        n_funds_seen=("fund", "nunique"),
        n_funds_persistent_add=("is_persistent_add", "sum"),
        n_funds_persistent_trim=("is_persistent_trim", "sum"),
        avg_streak_months=("streak_months", "mean"),
        avg_total_delta=("total_delta_window", "mean"),
    ).reset_index()
    summary["net_funds"] = summary["n_funds_persistent_add"] - summary["n_funds_persistent_trim"]
    summary = summary.sort_values(["net_funds", "avg_streak_months"], ascending=[False, False])
    return summary


# ──────────────────────────────────────────────────────────────────────────
# Sector rotation within each fund's equity sleeve
# ──────────────────────────────────────────────────────────────────────────

def fund_equity_sector_weights(fund_filter: str, months: list[date]) -> pd.DataFrame:
    """Monthly equity-sector weight_pct for one fund over the given months.

    asset_type is a literal mf_holdings column (used by asset_class_rotation),
    but there is no sector column — sector is derived per security via
    classify_sector (same keyword classifier mf_sector_analyzer.py uses),
    then weights are re-aggregated by (month, sector) in pandas.
    """
    pool = get_pool()
    month_list_sql = ", ".join(f"'{m}'" for m in months)
    df = pool.query_df(
        f"""
        SELECT as_of_month, security_name, sum(pct_of_nav) AS pct_of_nav
        FROM market_data.mf_holdings FINAL
        WHERE {fund_filter}
          AND as_of_month IN ({month_list_sql})
          AND lower(asset_type) = 'equity'
        GROUP BY as_of_month, security_name
        """
    )
    if df.empty:
        return pd.DataFrame()
    df["sector"] = df["security_name"].apply(classify_sector)
    return df.groupby(["as_of_month", "sector"], as_index=False)["pct_of_nav"].sum()


def sector_rotation(lookback_months: int = 12, min_streak_months: int = 3) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Persistence-ranked sector rotation *within the equity sleeve* of each
    multi-asset fund — the sector-level analogue of asset_class_rotation
    (which only sees equity/bond/gold/cash, not IT vs Banking vs Auto etc.
    within the equity portion).

    Returns:
        by_fund   : one row per (fund, sector) — the direct "is fund X
                    persistently rotating into/out of sector Y" answer.
        by_sector : cross-fund aggregate, same shape as asset_class_rotation's
                    summary — "which sectors are multi-asset funds as a group
                    persistently rotating into/out of".
    """
    fund_rows = []
    for f in MULTI_ASSET_FUNDS:
        months = fund_month_list(f["filter"])
        if len(months) < 2:
            continue
        window = months[-lookback_months:] if len(months) > lookback_months else months
        if len(window) < 2:
            continue

        weights = fund_equity_sector_weights(f["filter"], window)
        if weights.empty:
            continue
        pivot = weights.pivot_table(index="sector", columns="as_of_month",
                                    values="pct_of_nav", fill_value=0.0)
        month_cols = sorted(pivot.columns, key=lambda c: pd.to_datetime(c))
        pivot = pivot[month_cols]

        for sector, prow in pivot.iterrows():
            series = prow.astype(float)
            streak_months, direction = _rotation_streak(series)
            fund_rows.append({
                "fund": f["label"],
                "sector": sector,
                "latest_pct": float(series.iloc[-1]),
                "total_delta_window": float(series.iloc[-1] - series.iloc[0]),
                "streak_months": streak_months,
                "direction": direction,
                "is_persistent": streak_months >= min_streak_months,
            })

    if not fund_rows:
        return pd.DataFrame(), pd.DataFrame()

    by_fund = pd.DataFrame(fund_rows)
    by_fund = by_fund.sort_values(["fund", "streak_months"], ascending=[True, False]).reset_index(drop=True)

    by_fund["is_persistent_add"] = (by_fund["direction"] == "up") & by_fund["is_persistent"]
    by_fund["is_persistent_trim"] = (by_fund["direction"] == "down") & by_fund["is_persistent"]
    by_sector = by_fund.groupby("sector").agg(
        n_funds_seen=("fund", "nunique"),
        n_funds_persistent_add=("is_persistent_add", "sum"),
        n_funds_persistent_trim=("is_persistent_trim", "sum"),
        avg_streak_months=("streak_months", "mean"),
        avg_total_delta=("total_delta_window", "mean"),
    ).reset_index()
    by_sector["net_funds"] = by_sector["n_funds_persistent_add"] - by_sector["n_funds_persistent_trim"]
    by_sector = by_sector.sort_values(["net_funds", "avg_streak_months"], ascending=[False, False])

    return by_fund, by_sector


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


def render_asset_rotation(df: pd.DataFrame, lookback_months: int, min_streak_months: int) -> None:
    if df.empty:
        console.print("[yellow]No asset-class rotation data.[/yellow]")
        return
    t = Table(
        title=(
            f"Asset-Class Rotation — persistence-ranked "
            f"({lookback_months}mo lookback, ≥{min_streak_months}mo streak = persistent)"
        ),
        box=box.ROUNDED,
        header_style="bold cyan",
    )
    t.add_column("Asset Type", style="bold")
    t.add_column("Funds seen", justify="right")
    t.add_column("# persistent add", justify="right")
    t.add_column("# persistent trim", justify="right")
    t.add_column("Net", justify="right")
    t.add_column("Avg streak (mo)", justify="right")
    t.add_column("Avg Δ over window", justify="right")
    for _, r in df.iterrows():
        net = int(r["net_funds"])
        net_color = "green" if net > 0 else ("red" if net < 0 else "dim")
        t.add_row(
            r["asset_type"],
            str(int(r["n_funds_seen"])),
            str(int(r["n_funds_persistent_add"])),
            str(int(r["n_funds_persistent_trim"])),
            f"[{net_color}]{net:+d}[/{net_color}]",
            f"{r['avg_streak_months']:.1f}",
            f"{r['avg_total_delta']:+.2f}",
        )
    console.print(t)


def render_fund_sector_rotation(by_fund: pd.DataFrame, lookback_months: int, min_streak_months: int) -> None:
    if by_fund.empty:
        console.print("[yellow]No sector rotation data.[/yellow]")
        return
    persistent = by_fund[by_fund["is_persistent"]].sort_values(
        ["fund", "streak_months"], ascending=[True, False]
    )
    if persistent.empty:
        console.print(
            f"[dim]No fund shows a persistent (≥{min_streak_months}mo) sector rotation "
            f"over the last {lookback_months} months.[/dim]"
        )
        return
    t = Table(
        title=(
            f"Per-Fund Sector Rotation — persistent moves only "
            f"({lookback_months}mo lookback, ≥{min_streak_months}mo streak)"
        ),
        box=box.ROUNDED,
        header_style="bold magenta",
    )
    t.add_column("Fund", min_width=18)
    t.add_column("Sector", min_width=14)
    t.add_column("Direction", justify="center")
    t.add_column("Streak (mo)", justify="right")
    t.add_column("Latest wt %", justify="right")
    t.add_column("Δ over window", justify="right")
    for _, r in persistent.iterrows():
        color = "green" if r["direction"] == "up" else "red"
        arrow = "↑ IN" if r["direction"] == "up" else "↓ OUT"
        t.add_row(
            r["fund"], r["sector"],
            f"[{color}]{arrow}[/{color}]",
            str(int(r["streak_months"])),
            f"{r['latest_pct']:.2f}%",
            f"{r['total_delta_window']:+.2f}",
        )
    console.print(t)


def render_sector_rotation_summary(by_sector: pd.DataFrame, lookback_months: int, min_streak_months: int) -> None:
    if by_sector.empty:
        console.print("[yellow]No cross-fund sector rotation data.[/yellow]")
        return
    t = Table(
        title=(
            f"Cross-Fund Sector Rotation — persistence-ranked "
            f"({lookback_months}mo lookback, ≥{min_streak_months}mo streak = persistent)"
        ),
        box=box.ROUNDED,
        header_style="bold cyan",
    )
    t.add_column("Sector", style="bold")
    t.add_column("Funds seen", justify="right")
    t.add_column("# persistent add", justify="right")
    t.add_column("# persistent trim", justify="right")
    t.add_column("Net", justify="right")
    t.add_column("Avg streak (mo)", justify="right")
    t.add_column("Avg Δ over window", justify="right")
    for _, r in by_sector.iterrows():
        net = int(r["net_funds"])
        net_color = "green" if net > 0 else ("red" if net < 0 else "dim")
        t.add_row(
            r["sector"],
            str(int(r["n_funds_seen"])),
            str(int(r["n_funds_persistent_add"])),
            str(int(r["n_funds_persistent_trim"])),
            f"[{net_color}]{net:+d}[/{net_color}]",
            f"{r['avg_streak_months']:.1f}",
            f"{r['avg_total_delta']:+.2f}",
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
    parser.add_argument("--lookback-months", type=int, default=12,
                        help="Months of asset-class weight history to score rotation persistence over (default 12).")
    parser.add_argument("--min-streak-months", type=int, default=3,
                        help="Min consecutive same-direction months for a fund to count as 'persistent' (default 3).")
    parser.add_argument("--no-sectors", action="store_true",
                        help="Skip the per-fund equity sector rotation section.")
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
        rotation = asset_class_rotation(args.lookback_months, args.min_streak_months)
        if not rotation.empty:
            render_asset_rotation(rotation, args.lookback_months, args.min_streak_months)

    if not args.no_sectors:
        console.print()
        by_fund, by_sector = sector_rotation(args.lookback_months, args.min_streak_months)
        render_fund_sector_rotation(by_fund, args.lookback_months, args.min_streak_months)
        console.print()
        render_sector_rotation_summary(by_sector, args.lookback_months, args.min_streak_months)

    return 0


if __name__ == "__main__":
    sys.exit(main())
