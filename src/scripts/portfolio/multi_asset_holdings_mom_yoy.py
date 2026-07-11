"""
scripts/portfolio/multi_asset_holdings_mom_yoy.py
──────────────────────────────────────────────────
Holding-level MoM (Month-over-Month) and YoY (Year-over-Year) change analysis
for any multi-asset (or any) mutual fund stored in `market_data.mf_holdings`.

Unlike fund_mom_returns.py (which tracks NAV returns), this script tracks the
*portfolio composition* — what did the fund manager buy, sell, increase, or
trim — at the security and asset-class level.

Usage:
    # By exact fund_name (matches the value stored in mf_holdings)
    python src/scripts/portfolio/multi_asset_holdings_mom_yoy.py --fund DSP_MULTI_ASSET

    # By scheme code
    python src/scripts/portfolio/multi_asset_holdings_mom_yoy.py --scheme 152056

    # Fuzzy search; picks the first match (or prompts if multiple)
    python src/scripts/portfolio/multi_asset_holdings_mom_yoy.py --search "DSP Multi"

    # List all multi-asset funds with sufficient history and exit
    python src/scripts/portfolio/multi_asset_holdings_mom_yoy.py --list

    # Only show top N MoM movers (default 15)
    python src/scripts/portfolio/multi_asset_holdings_mom_yoy.py --fund DSP_MULTI_ASSET --top 20

    # Skip the YoY block (use when history < 12 months)
    python src/scripts/portfolio/multi_asset_holdings_mom_yoy.py --fund BAJAJ_MULTI_ASSET --no-yoy
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
# Fund resolution
# ──────────────────────────────────────────────────────────────────────────

def list_funds() -> pd.DataFrame:
    """Return all funds with ≥2 months of holdings history, sorted by depth."""
    pool = get_pool()
    return pool.query_df(
        """
        SELECT
            fund_name,
            scheme_code,
            count(DISTINCT as_of_month) AS n_months,
            min(as_of_month)            AS first_month,
            max(as_of_month)            AS last_month
        FROM market_data.mf_holdings FINAL
        GROUP BY fund_name, scheme_code
        HAVING n_months >= 2
        ORDER BY n_months DESC, fund_name
        """
    )


def resolve_fund(fund: Optional[str], scheme: Optional[str], search: Optional[str]) -> Optional[str]:
    """Return the canonical fund_name to analyze, or None if not resolvable."""
    pool = get_pool()

    if fund:
        df = pool.query_df(
            "SELECT DISTINCT fund_name FROM market_data.mf_holdings FINAL "
            f"WHERE fund_name = '{fund}'"
        )
        if df.empty:
            console.print(f"[red]No holdings found for fund_name='{fund}'[/red]")
            return None
        return df.iloc[0, 0]

    if scheme:
        df = pool.query_df(
            "SELECT DISTINCT fund_name FROM market_data.mf_holdings FINAL "
            f"WHERE scheme_code = '{scheme}' "
            "ORDER BY fund_name LIMIT 1"
        )
        if df.empty:
            console.print(f"[red]No fund found for scheme_code='{scheme}'[/red]")
            return None
        return df.iloc[0, 0]

    if search:
        df = pool.query_df(
            "SELECT DISTINCT fund_name FROM market_data.mf_holdings FINAL "
            f"WHERE fund_name ILIKE '%{search.upper().replace(' ', '%')}%' "
            "ORDER BY fund_name"
        )
        if df.empty:
            console.print(f"[red]No fund matched '{search}'[/red]")
            return None
        if len(df) > 1:
            console.print("[yellow]Multiple matches — picking the first:[/yellow]")
            for _, r in df.iterrows():
                console.print(f"  • {r['fund_name']}")
        return df.iloc[0, 0]

    return None


# ──────────────────────────────────────────────────────────────────────────
# Holdings snapshots & deltas
# ──────────────────────────────────────────────────────────────────────────

def get_snapshot(fund_name: str, as_of_month: date) -> pd.DataFrame:
    pool = get_pool()
    return pool.query_df(
        f"""
        SELECT
            security_name,
            asset_type,
            sum(pct_of_nav)      AS pct_of_nav,
            sum(market_value_cr) AS market_value_cr
        FROM market_data.mf_holdings FINAL
        WHERE fund_name = '{fund_name}' AND as_of_month = '{as_of_month}'
        GROUP BY security_name, asset_type
        """
    )


def get_month_list(fund_name: str) -> list[date]:
    pool = get_pool()
    df = pool.query_df(
        f"""
        SELECT DISTINCT as_of_month
        FROM market_data.mf_holdings FINAL
        WHERE fund_name = '{fund_name}'
        ORDER BY as_of_month
        """
    )
    if df.empty:
        return []
    # Coerce to plain date (may come back as pandas.Timestamp)
    return [pd.to_datetime(m).date() for m in df["as_of_month"].tolist()]


def diff_snapshots(curr: pd.DataFrame, prev: pd.DataFrame, label: str) -> pd.DataFrame:
    """Outer-join on security_name and compute weight delta."""
    if curr.empty:
        return pd.DataFrame()
    if prev.empty:
        merged = curr.copy()
        merged["prev_pct"] = 0.0
    else:
        prev_slim = prev[["security_name", "pct_of_nav"]].rename(
            columns={"pct_of_nav": "prev_pct"}
        )
        merged = curr.merge(prev_slim, on="security_name", how="outer")
        merged["pct_of_nav"] = merged["pct_of_nav"].fillna(0.0)
        merged["prev_pct"]   = merged["prev_pct"].fillna(0.0)
    merged["delta_pct"] = merged["pct_of_nav"] - merged["prev_pct"]
    merged["period"]    = label
    return merged


def classify_action(row: pd.Series) -> str:
    """Tag each row as new / exit / increase / trim / unchanged."""
    if row["prev_pct"] <= 0.001 and row["pct_of_nav"] > 0.001:
        return "🆕 NEW"
    if row["pct_of_nav"] <= 0.001 and row["prev_pct"] > 0.001:
        return "❌ EXIT"
    if row["delta_pct"] > 0.05:
        return "📈 ADD"
    if row["delta_pct"] < -0.05:
        return "📉 TRIM"
    return "─ same"


# ──────────────────────────────────────────────────────────────────────────
# Asset-class aggregates
# ──────────────────────────────────────────────────────────────────────────

def asset_class_summary(fund_name: str, months: list[date], targets: list[date]) -> pd.DataFrame:
    """Build a wide table of asset_type weight across the requested months."""
    pool = get_pool()
    target_strs = "','".join(str(m) for m in targets)
    df = pool.query_df(
        f"""
        SELECT
            as_of_month,
            asset_type,
            round(sum(pct_of_nav), 2) AS weight_pct
        FROM market_data.mf_holdings FINAL
        WHERE fund_name = '{fund_name}'
          AND as_of_month IN ('{target_strs}')
        GROUP BY as_of_month, asset_type
        ORDER BY as_of_month, asset_type
        """
    )
    if df.empty:
        return df
    return df.pivot(index="asset_type", columns="as_of_month", values="weight_pct").fillna(0.0)


# ──────────────────────────────────────────────────────────────────────────
# Rendering
# ──────────────────────────────────────────────────────────────────────────

def render_top_movers(diff_df: pd.DataFrame, n: int, period_label: str) -> None:
    if diff_df.empty:
        console.print(f"[yellow]No data for {period_label} movers.[/yellow]")
        return

    diff_df = diff_df.copy()
    diff_df["action"]    = diff_df.apply(classify_action, axis=1)
    diff_df["abs_delta"] = diff_df["delta_pct"].abs()
    movers = diff_df.sort_values("abs_delta", ascending=False).head(n)

    t = Table(
        title=f"Top {n} {period_label} Position Changes",
        box=box.ROUNDED,
        header_style="bold magenta",
    )
    t.add_column("Action", width=10)
    t.add_column("Security", min_width=30, overflow="fold")
    t.add_column("Asset", width=8)
    t.add_column("Prev %", justify="right", width=9)
    t.add_column("Curr %", justify="right", width=9)
    t.add_column("Δ pct-pts", justify="right", width=11)

    for _, r in movers.iterrows():
        delta = r["delta_pct"]
        delta_str = f"{delta:+.2f}"
        delta_style = "green" if delta > 0 else ("red" if delta < 0 else "dim")
        asset = (r.get("asset_type") or "—") if isinstance(r.get("asset_type"), str) else "—"
        t.add_row(
            r["action"],
            r["security_name"],
            asset,
            f"{r['prev_pct']:.2f}",
            f"{r['pct_of_nav']:.2f}",
            f"[{delta_style}]{delta_str}[/{delta_style}]",
        )
    console.print(t)


def render_asset_class(asset_pivot: pd.DataFrame, period_label: str) -> None:
    if asset_pivot.empty:
        return
    t = Table(
        title=f"Asset-Class Weight ({period_label})",
        box=box.ROUNDED,
        header_style="bold cyan",
    )
    t.add_column("Asset Type", style="bold")
    cols = list(asset_pivot.columns)
    for col in cols:
        t.add_column(str(col), justify="right")
    if len(cols) >= 2:
        t.add_column("Δ (curr − ref)", justify="right")

    for asset, row in asset_pivot.iterrows():
        cells = [asset]
        for col in cols:
            cells.append(f"{row[col]:.2f}%")
        if len(cols) >= 2:
            delta = row[cols[-1]] - row[cols[0]]
            cells.append(f"[{'green' if delta > 0 else ('red' if delta < 0 else 'dim')}]{delta:+.2f}[/]")
        t.add_row(*cells)
    console.print(t)


# ──────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────

def main() -> int:
    parser = argparse.ArgumentParser(
        description="MoM and YoY holdings analysis for multi-asset (or any) mutual funds."
    )
    parser.add_argument("--fund",   help="Exact fund_name in mf_holdings (e.g. DSP_MULTI_ASSET)")
    parser.add_argument("--scheme", help="scheme_code (e.g. 152056)")
    parser.add_argument("--search", help="Fuzzy fund_name search (e.g. 'DSP Multi')")
    parser.add_argument("--list",   action="store_true",
                        help="List all funds with ≥2 months of holdings and exit")
    parser.add_argument("--top",    type=int, default=15,
                        help="Top N MoM/YoY movers to show (default 15)")
    parser.add_argument("--no-yoy", action="store_true",
                        help="Skip the YoY block (use when history < 12 months)")
    args = parser.parse_args()

    if args.list:
        df = list_funds()
        if df.empty:
            console.print("[red]No funds with ≥2 months of holdings found.[/red]")
            return 1
        t = Table(title="Funds with holdings history", box=box.ROUNDED)
        t.add_column("fund_name", style="bold")
        t.add_column("scheme_code")
        t.add_column("months", justify="right")
        t.add_column("first")
        t.add_column("last")
        for _, r in df.iterrows():
            t.add_row(r["fund_name"], r["scheme_code"], str(r["n_months"]),
                      str(r["first_month"]), str(r["last_month"]))
        console.print(t)
        return 0

    fund_name = resolve_fund(args.fund, args.scheme, args.search)
    if not fund_name:
        console.print("[red]Provide --fund, --scheme, --search, or --list[/red]")
        return 1

    months = get_month_list(fund_name)
    if len(months) == 0:
        console.print(f"[red]No holdings data found for {fund_name}.[/red]")
        return 1

    if len(months) == 1:
        # Single month loaded — show snapshot, no MoM diff possible
        curr_month = months[0]
        console.print(Panel.fit(
            f"[bold cyan]{fund_name}[/bold cyan]  ·  "
            f"Only 1 month loaded: [bold]{curr_month}[/bold]\n"
            "[yellow]⚠ MoM/YoY comparison requires ≥2 months. "
            "Run the importer monthly to build history.[/yellow]",
            title="Multi-Asset Holdings — Snapshot Only",
            border_style="yellow",
        ))
        curr_snap = get_snapshot(fund_name, curr_month)
        if curr_snap.empty:
            console.print("[red]No holding rows returned for this month.[/red]")
            return 1
        t = Table(
            title=f"Current Holdings ({curr_month})",
            box=box.ROUNDED,
            header_style="bold magenta",
        )
        t.add_column("Security", min_width=30, overflow="fold")
        t.add_column("Asset", width=10)
        t.add_column("Weight %", justify="right", width=10)
        t.add_column("Value (₹Cr)", justify="right", width=12)
        snap_sorted = curr_snap.sort_values("pct_of_nav", ascending=False).head(args.top)
        for _, r in snap_sorted.iterrows():
            t.add_row(
                r["security_name"],
                str(r.get("asset_type") or "—"),
                f"{r['pct_of_nav']:.2f}%",
                f"{r['market_value_cr']:.1f}",
            )
        console.print(t)
        return 0

    curr_month = months[-1]
    prev_month = months[-2]
    yoy_month  = None
    if not args.no_yoy and len(months) >= 13:
        yoy_month = months[-13]

    console.print(Panel.fit(
        f"[bold cyan]{fund_name}[/bold cyan]  ·  "
        f"history: {months[0]} → {curr_month}  ({len(months)} months)",
        title="Multi-Asset Holdings Analysis",
        border_style="cyan",
    ))

    # MoM
    curr_snap = get_snapshot(fund_name, curr_month)
    prev_snap = get_snapshot(fund_name, prev_month)
    mom_diff  = diff_snapshots(curr_snap, prev_snap, "MoM")

    console.print()
    console.print(f"[bold]MoM[/bold]: [dim]{prev_month}[/dim] → [bold]{curr_month}[/bold]")
    render_top_movers(mom_diff, args.top, "MoM")

    # YoY
    if yoy_month:
        yoy_snap = get_snapshot(fund_name, yoy_month)
        yoy_diff = diff_snapshots(curr_snap, yoy_snap, "YoY")
        console.print()
        console.print(f"[bold]YoY[/bold]: [dim]{yoy_month}[/dim] → [bold]{curr_month}[/bold]")
        render_top_movers(yoy_diff, args.top, "YoY")
    elif not args.no_yoy:
        console.print(f"\n[yellow]YoY skipped — need ≥13 months of history (have {len(months)}).[/yellow]")

    # Asset-class roll-up
    console.print()
    target_months = [prev_month, curr_month] if not yoy_month else [yoy_month, prev_month, curr_month]
    asset_pivot   = asset_class_summary(fund_name, months, target_months)
    label         = "MoM" if not yoy_month else "YoY → MoM → Curr"
    render_asset_class(asset_pivot, label)

    return 0


if __name__ == "__main__":
    sys.exit(main())
