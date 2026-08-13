"""
scripts/portfolio/multi_asset_ascii_viz.py
───────────────────────────────────────
Terminal ASCII visualizations for the 7 multi-asset funds in
`market_data.mf_holdings`. Complements the tabular output of
`multi_asset_consensus.py` with two chart-style views:

  1. Fund x Asset-Class MoM weight-shift matrix — each fund's own latest
     two as_of_month snapshots, one row per fund, one column per asset
     class, with a text bar in each cell scaled to the largest |delta|
     in the matrix.

  2. Consensus-movers diverging bar chart — securities that >=2 funds
     moved (added to or trimmed from) in the same window, sorted by
     average delta, red/left bars for trims and green/right bars for adds.

Securities are joined across funds on ISIN, not security_name text —
AMCs spell the same company differently ("HDFC Bank Ltd" vs "HDFC Bank
Limited"), and joining on the raw name fragments one company into false
duplicates with opposite-looking bars.

Usage:
    python src/scripts/portfolio/multi_asset_ascii_viz.py
    python src/scripts/portfolio/multi_asset_ascii_viz.py --min-delta 0.25 --top 8
"""
from __future__ import annotations

import argparse
import os
import sys

import pandas as pd

sys.path.append(os.getcwd())

from rich import box
from rich.console import Console
from rich.table import Table

from src.db.pool import get_pool

console = Console()

# Canonical roster — kept identical to multi_asset_consensus.py's MULTI_ASSET_FUNDS.
FUNDS = [
    ("Nippon Multi Asset",     "scheme_code = 'RLMF806'"),
    ("Nippon Multi Asset FoF", "scheme_code = 'RLMF811'"),
    ("DSP Multi Asset",        "scheme_code = '152056'"),
    ("DSP Multi Asset Omni",   "scheme_code = '154167'"),
    ("Bajaj Multi Asset",      "scheme_code = '152639'"),
    ("Quant Multi Asset",      "scheme_code = '120821'"),
    ("ICICI Multi Asset",      "scheme_code = '120334'"),
]
ASSET_ORDER = ["equity", "gold", "bond", "cash", "other"]


def months_for(fund_filter: str) -> list:
    pool = get_pool()
    df = pool.query_df(
        f"SELECT DISTINCT as_of_month FROM market_data.mf_holdings FINAL "
        f"WHERE {fund_filter} ORDER BY as_of_month"
    )
    return [pd.to_datetime(m).date() for m in df["as_of_month"].tolist()]


def asset_class_snapshot(fund_filter: str, as_of_month) -> dict:
    pool = get_pool()
    df = pool.query_df(
        f"""
        SELECT lower(asset_type) AS asset_type, sum(pct_of_nav) AS pct
        FROM market_data.mf_holdings FINAL
        WHERE {fund_filter} AND as_of_month = '{as_of_month}'
        GROUP BY asset_type
        """
    )
    return dict(zip(df["asset_type"], df["pct"])) if not df.empty else {}


def security_snapshot(fund_filter: str, as_of_month) -> pd.DataFrame:
    pool = get_pool()
    return pool.query_df(
        f"""
        SELECT isin, any(security_name) AS security_name, sum(pct_of_nav) AS pct
        FROM market_data.mf_holdings FINAL
        WHERE {fund_filter} AND as_of_month = '{as_of_month}' AND isin != ''
        GROUP BY isin
        """
    )


def _bar(value: float, max_abs: float, width: int, up_char: str = "#", down_char: str = "-") -> str:
    n = min(width, int(round(abs(value) / max_abs * width))) if max_abs else 0
    return (up_char if value >= 0 else down_char) * n


def build_asset_class_matrix() -> pd.DataFrame:
    """One row per fund: its own latest-two-month asset-class pct_of_nav deltas.

    Columns: fund, prev, curr (as_of_month strings), then one column per
    ASSET_ORDER entry. Shared by both the ASCII renderer and the UI's chart
    view so the underlying (fund-lag-aware) computation lives in one place.
    """
    rows = []
    for label, fund_filter in FUNDS:
        months = months_for(fund_filter)
        if len(months) < 2:
            continue
        curr, prev = months[-1], months[-2]
        curr_snap, prev_snap = asset_class_snapshot(fund_filter, curr), asset_class_snapshot(fund_filter, prev)
        row = {"fund": label, "prev": str(prev), "curr": str(curr)}
        for asset in ASSET_ORDER:
            row[asset] = round(curr_snap.get(asset, 0.0) - prev_snap.get(asset, 0.0), 2)
        rows.append(row)
    return pd.DataFrame(rows)


def render_asset_class_matrix(console: Console = console) -> None:
    df = build_asset_class_matrix()
    if df.empty:
        console.print("[yellow]No funds with >=2 months of holdings history.[/yellow]")
        return

    mat = df.set_index("fund")
    max_abs = max(1.0, mat[ASSET_ORDER].abs().values.max())

    console.rule("[bold]1) Fund x Asset-Class MoM Weight Shift (pct-pts)[/bold]")
    table = Table(box=box.SQUARE, show_lines=True)
    table.add_column("Fund")
    table.add_column("Snapshot (prev -> curr)")
    for asset in ASSET_ORDER:
        table.add_column(asset.upper())
    for label, r in mat.iterrows():
        cells = []
        for asset in ASSET_ORDER:
            v = r[asset]
            color = "green" if v > 0 else ("red" if v < 0 else "white")
            cells.append(f"[{color}]{v:+.2f} {_bar(v, max_abs, 8)}[/{color}]")
        table.add_row(label, f"{r['prev']} -> {r['curr']}", *cells)
    console.print(table)
    console.print(
        "[dim]Bar length scaled to the largest single |delta| in this matrix. "
        "'#' = increase, '-' = decrease. Each fund compares its own two most-recent "
        "snapshots — funds lag each other, so rows are not all the same calendar month.[/dim]\n"
    )


def build_consensus_moves(min_delta: float = 0.10) -> pd.DataFrame:
    """Security-level moves aggregated across funds, matched on ISIN (not
    security_name text — AMCs spell the same company differently).

    Returns columns: isin, security, n_funds, avg_delta, net — filtered to
    n_funds >= 2, sorted by avg_delta ascending. Empty DataFrame if nothing
    qualifies.
    """
    sec_rows = []
    for label, fund_filter in FUNDS:
        months = months_for(fund_filter)
        if len(months) < 2:
            continue
        curr, prev = months[-1], months[-2]
        df_c, df_p = security_snapshot(fund_filter, curr), security_snapshot(fund_filter, prev)
        merged = pd.merge(df_c, df_p, on="isin", how="outer", suffixes=("_c", "_p"))
        merged["security_name"] = merged["security_name_c"].fillna(merged["security_name_p"])
        merged["pct_c"] = merged["pct_c"].fillna(0.0)
        merged["pct_p"] = merged["pct_p"].fillna(0.0)
        merged["delta"] = merged["pct_c"] - merged["pct_p"]
        merged = merged[merged["delta"].abs() >= min_delta]
        for _, r in merged.iterrows():
            sec_rows.append({"isin": r["isin"], "security": r["security_name"], "fund": label, "delta": r["delta"]})

    if not sec_rows:
        return pd.DataFrame(columns=["isin", "security", "n_funds", "avg_delta", "net"])

    agg = (
        pd.DataFrame(sec_rows)
        .groupby("isin")
        .agg(
            security=("security", "first"),
            n_funds=("fund", "nunique"),
            avg_delta=("delta", "mean"),
            net=("delta", lambda s: (s > 0).sum() - (s < 0).sum()),
        )
        .reset_index()
    )
    return agg[agg["n_funds"] >= 2].sort_values("avg_delta").reset_index(drop=True)


def render_consensus_bar_chart(min_delta: float, top_n: int, width: int, console: Console = console) -> None:
    agg = build_consensus_moves(min_delta=min_delta)

    console.rule("[bold]2) Consensus Movers - Diverging Bar Chart (avg Delta pct-pts, >=2 funds)[/bold]")
    if agg.empty:
        console.print("[yellow]No security moved by >=2 funds at this --min-delta threshold.[/yellow]")
        return

    half = max(1, top_n // 2)
    top = pd.concat([agg.head(half), agg.tail(half)]).drop_duplicates(subset="isin").sort_values("avg_delta")
    max_d = max(0.5, top["avg_delta"].abs().max())
    name_w = max(len(s) for s in top["security"])

    for _, r in top.iterrows():
        n = min(width, int(round(abs(r["avg_delta"]) / max_d * width)))
        if r["avg_delta"] >= 0:
            bar_str = f"[dim]{' ' * width}[/dim]|[green]{'#' * n}[/green]"
        else:
            bar_str = f"[red]{('-' * n).rjust(width)}[/red]|[dim]{' ' * width}[/dim]"
        console.print(
            f"{r['security']:<{name_w}}  {bar_str}  {r['avg_delta']:+.2f}  "
            f"({int(r['n_funds'])} funds, net {int(r['net']):+d})"
        )
    console.print(
        "[dim]Bars scaled to the largest |avg delta| among shown names. "
        "Left/red = trimmed, right/green = added. Joined on ISIN across funds.[/dim]"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="ASCII visualizations for multi-asset fund MoM patterns")
    parser.add_argument("--min-delta", type=float, default=0.10, help="Minimum |pct_of_nav delta| to count as a move (default 0.10)")
    parser.add_argument("--top", type=int, default=20, help="Total names shown in the consensus bar chart, split between top adds/trims (default 20)")
    parser.add_argument("--width", type=int, default=20, help="Bar width in characters for the consensus chart (default 20)")
    args = parser.parse_args()

    render_asset_class_matrix()
    render_consensus_bar_chart(min_delta=args.min_delta, top_n=args.top, width=args.width)


if __name__ == "__main__":
    main()
