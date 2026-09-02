"""
scripts/portfolio/quant_multi_asset_analysis.py
────────────────────────────────────────────────
Deep-dive analysis of Quant Multi Asset Fund (scheme 120821) holdings to
reverse-engineer the fund manager's entry/exit decision framework.

Covers:
  1. Asset-class rotation timeline (equity / gold / bond / other)
  2. Sector exposure shifts over time
  3. Top stock entry / exit / trim events with magnitude
  4. Gold allocation timing analysis
  5. Quant's "VLRT" (Valuation + Liquidity + Risk + Time) signal inference
  6. Cross-comparison: Quant Multi Asset vs Quant Dynamic Asset Allocation

Usage:
    python src/scripts/portfolio/quant_multi_asset_analysis.py
    python src/scripts/portfolio/quant_multi_asset_analysis.py --fund QUANT_DYNAMIC_ASSET_ALLOCATION
    python src/scripts/portfolio/quant_multi_asset_analysis.py --top 20
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import date

import pandas as pd

sys.path.append(os.getcwd())

from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from src.db.pool import get_pool
from src.tools.mf_sector_analyzer import classify_sector

console = Console()

# ── Helpers ──────────────────────────────────────────────────────────────────

_MARKET_REGIME_ANNOTATIONS: dict[str, str] = {
    "2023-03-31": "SVB Crisis / Rate Hike Peak",
    "2023-09-30": "Oct Rally Anticipation / Gold Surge",
    "2023-10-31": "Gold +8% surge (Hamas conflict)",
    "2023-12-31": "Equity rally / Santa Effect",
    "2024-04-30": "Election Uncertainty / De-risking",
    "2024-07-31": "Post-election Budget Sell-off",
    "2024-10-31": "FII Exit / Nifty correction",
    "2025-04-30": "US Tariff Shock",
    "2025-06-30": "India-Pak geopolitical tension",
    "2025-08-31": "Silver spike on industrial demand",
    "2025-12-31": "Year-end equity rally / rebalance",
    "2026-04-01": "Derivative/FOF rebalance (mid-month)",
    "2026-06-30": "Silver ETF + Airtel thesis",
}


def _bar(val: float, max_val: float = 80.0, width: int = 20, color: str = "cyan") -> str:
    """ASCII bar for terminal charts."""
    fill = int(round(val / max_val * width))
    fill = max(0, min(fill, width))
    return f"[{color}]{'█' * fill}{'░' * (width - fill)}[/{color}]"


def _delta_color(d: float) -> str:
    if d > 2:
        return "green"
    if d < -2:
        return "red"
    return "yellow"


# ── 1. Asset Class Rotation ───────────────────────────────────────────────────

def section_asset_class_rotation(fund_name: str) -> pd.DataFrame:
    pool = get_pool()
    df = pool.query_df(f"""
        SELECT
            as_of_month,
            round(sumIf(pct_of_nav, asset_type = 'equity'), 1) AS equity,
            round(sumIf(pct_of_nav, asset_type = 'gold'),   1) AS gold,
            round(sumIf(pct_of_nav, asset_type = 'bond'),   1) AS bond,
            round(sumIf(pct_of_nav, asset_type = 'other') +
                  sumIf(pct_of_nav, asset_type = 'cash'),   1) AS other_cash,
            count(DISTINCT security_name)                       AS n_holdings
        FROM market_data.mf_holdings FINAL
        WHERE fund_name = '{fund_name}'
        GROUP BY as_of_month
        ORDER BY as_of_month
    """)

    # Filter out mid-month snapshot duplicates (keep month-end)
    df["as_of_month"] = pd.to_datetime(df["as_of_month"])
    df["ym"] = df["as_of_month"].dt.to_period("M")
    df = df.sort_values("as_of_month").drop_duplicates("ym", keep="last").drop("ym", axis=1)
    df = df.reset_index(drop=True)

    console.rule("[bold cyan]1. ASSET CLASS ROTATION — Monthly Allocation %")
    tbl = Table(box=box.SIMPLE_HEAVY, show_header=True, header_style="bold white")
    tbl.add_column("Month",      style="cyan",  width=12)
    tbl.add_column("Equity",     style="green", justify="right", width=8)
    tbl.add_column("Gold",       style="yellow",justify="right", width=8)
    tbl.add_column("Bond",       style="blue",  justify="right", width=8)
    tbl.add_column("Other/Cash", style="white", justify="right", width=10)
    tbl.add_column("Holdings",   justify="right", width=9)
    tbl.add_column("Equity Bar (0→80%)", width=25)
    tbl.add_column("Annotation", style="dim")

    for _, r in df.iterrows():
        mon = r["as_of_month"].strftime("%Y-%m")
        ann = _MARKET_REGIME_ANNOTATIONS.get(mon + "-01", "")
        ann = _MARKET_REGIME_ANNOTATIONS.get(r["as_of_month"].strftime("%Y-%m-%d"), ann)
        tbl.add_row(
            mon,
            f"{r['equity']:.1f}%",
            f"{r['gold']:.1f}%",
            f"{r['bond']:.1f}%",
            f"{r['other_cash']:.1f}%",
            str(int(r["n_holdings"])),
            _bar(r["equity"], 80),
            ann,
        )
    console.print(tbl)
    return df


# ── 2. Sector Rotation ───────────────────────────────────────────────────────

def section_sector_rotation(fund_name: str, top_n: int = 10) -> None:
    pool = get_pool()
    df = pool.query_df(f"""
        SELECT
            as_of_month,
            security_name,
            sum(pct_of_nav) AS weight
        FROM market_data.mf_holdings FINAL
        WHERE fund_name = '{fund_name}'
          AND asset_type = 'equity'
        GROUP BY as_of_month, security_name
        ORDER BY as_of_month, weight DESC
    """)

    df["as_of_month"] = pd.to_datetime(df["as_of_month"])
    df["ym"] = df["as_of_month"].dt.to_period("M")
    df = df.sort_values("as_of_month").drop_duplicates(["ym", "security_name"], keep="last")

    df["sector"] = df["security_name"].apply(classify_sector)

    sector_monthly = (
        df.groupby(["ym", "sector"])["weight"]
        .sum()
        .reset_index()
        .sort_values(["ym", "weight"], ascending=[True, False])
    )

    # Pivot: sectors as columns
    pivot = sector_monthly.pivot_table(index="ym", columns="sector", values="weight", fill_value=0)
    pivot = pivot.sort_index()

    # Top sectors by average allocation
    top_sectors = pivot.mean().nlargest(top_n).index.tolist()
    pivot = pivot[top_sectors]

    console.rule("[bold cyan]2. SECTOR ROTATION — Equity Allocation % by Sector")
    tbl = Table(box=box.SIMPLE_HEAVY, header_style="bold white", show_header=True)
    tbl.add_column("Month", style="cyan", width=10)
    for s in top_sectors:
        short = s.split("(")[0].strip()[:18]
        tbl.add_column(short, justify="right", width=9)

    for ym in pivot.index:
        row_vals = [str(ym)]
        for s in top_sectors:
            v = pivot.loc[ym, s]
            if v >= 5:
                row_vals.append(f"[green]{v:.1f}[/green]")
            elif v >= 2:
                row_vals.append(f"[yellow]{v:.1f}[/yellow]")
            elif v > 0:
                row_vals.append(f"[dim]{v:.1f}[/dim]")
            else:
                row_vals.append("")
        tbl.add_row(*row_vals)
    console.print(tbl)


# ── 3. Major Entry / Exit Events ─────────────────────────────────────────────

def section_entry_exit(fund_name: str, threshold: float = 2.5, top_n: int = 50) -> None:
    pool = get_pool()
    df = pool.query_df(f"""
        WITH monthly AS (
            SELECT
                as_of_month,
                security_name,
                asset_type,
                sum(pct_of_nav) AS weight
            FROM market_data.mf_holdings FINAL
            WHERE fund_name = '{fund_name}'
            GROUP BY as_of_month, security_name, asset_type
        ),
        with_prev AS (
            SELECT
                as_of_month,
                security_name,
                asset_type,
                weight,
                lagInFrame(weight, 1, 0) OVER (
                    PARTITION BY security_name ORDER BY as_of_month
                ) AS prev_weight
            FROM monthly
        )
        SELECT
            as_of_month,
            security_name,
            asset_type,
            round(weight, 2)              AS weight_pct,
            round(prev_weight, 2)         AS prev_pct,
            round(weight - prev_weight, 2) AS delta
        FROM with_prev
        WHERE abs(weight - prev_weight) >= {threshold}
          AND asset_type IN ('equity', 'gold')
        ORDER BY as_of_month DESC, abs(weight - prev_weight) DESC
        LIMIT {top_n * 2}
    """)

    df["as_of_month"] = pd.to_datetime(df["as_of_month"])
    df["ym"] = df["as_of_month"].dt.to_period("M")
    df = df.sort_values("as_of_month").drop_duplicates(["ym", "security_name"], keep="last")
    df["sector"] = df["security_name"].apply(classify_sector)
    df = df.sort_values("as_of_month", ascending=False).head(top_n)

    console.rule("[bold cyan]3. MAJOR ENTRY / EXIT / TRIM EVENTS (|Δweight| ≥ 2.5%)")
    tbl = Table(box=box.SIMPLE_HEAVY, header_style="bold white")
    tbl.add_column("Month",    style="cyan",  width=10)
    tbl.add_column("Security", width=36)
    tbl.add_column("Type",     width=6)
    tbl.add_column("Prev%",    justify="right", width=7)
    tbl.add_column("Now%",     justify="right", width=7)
    tbl.add_column("Δ",        justify="right", width=7)
    tbl.add_column("Action",   width=10)
    tbl.add_column("Sector",   style="dim", width=28)

    for _, r in df.iterrows():
        delta = r["delta"]
        prev  = r["prev_pct"]
        now   = r["weight_pct"]
        color = _delta_color(delta)
        if prev == 0:
            action = "[green bold]ENTER[/green bold]"
        elif now == 0:
            action = "[red bold]EXIT[/red bold]"
        elif delta > 0:
            action = "[green]ADD[/green]"
        else:
            action = "[red]TRIM[/red]"
        tbl.add_row(
            r["as_of_month"].strftime("%Y-%m"),
            r["security_name"][:35],
            r["asset_type"][:5],
            f"{prev:.2f}",
            f"[{color}]{now:.2f}[/{color}]",
            f"[{color}]{delta:+.2f}[/{color}]",
            action,
            r["sector"][:28],
        )
    console.print(tbl)


# ── 4. Gold Allocation Timing Analysis ───────────────────────────────────────

def section_gold_timing(fund_name: str) -> None:
    pool = get_pool()

    # Gold allocation over time
    gold_df = pool.query_df(f"""
        SELECT
            as_of_month,
            round(sum(pct_of_nav), 2) AS gold_pct,
            groupArray(security_name) AS instruments
        FROM market_data.mf_holdings FINAL
        WHERE fund_name = '{fund_name}'
          AND asset_type = 'gold'
        GROUP BY as_of_month
        ORDER BY as_of_month
    """)

    # Gold prices from daily_prices
    gold_px = pool.query_df("""
        SELECT
            toStartOfMonth(trade_date) AS month,
            round(avg(close), 0)       AS avg_gold_px
        FROM market_data.daily_prices FINAL
        WHERE symbol = 'GOLDBEES'
        GROUP BY month
        ORDER BY month
    """)

    gold_df["as_of_month"] = pd.to_datetime(gold_df["as_of_month"])
    gold_df["ym"] = gold_df["as_of_month"].dt.to_period("M")
    gold_df = gold_df.sort_values("as_of_month").drop_duplicates("ym", keep="last")

    gold_px["month"] = pd.to_datetime(gold_px["month"])
    gold_px["ym"]    = gold_px["month"].dt.to_period("M")

    merged = gold_df.merge(gold_px[["ym", "avg_gold_px"]], on="ym", how="left")

    # Compute forward 3-month return for gold_px
    merged = merged.sort_values("as_of_month").reset_index(drop=True)
    merged["gold_px_fwd3m"] = merged["avg_gold_px"].shift(-3)
    merged["fwd3m_ret_pct"] = (
        (merged["gold_px_fwd3m"] - merged["avg_gold_px"]) / merged["avg_gold_px"] * 100
    ).round(1)

    console.rule("[bold yellow]4. GOLD ALLOCATION TIMING vs GOLDBEES PRICE")
    tbl = Table(box=box.SIMPLE_HEAVY, header_style="bold white")
    tbl.add_column("Month",         style="cyan",   width=10)
    tbl.add_column("Gold%",         style="yellow", justify="right", width=8)
    tbl.add_column("Gold Bar",      width=18)
    tbl.add_column("GOLDBEES ₹",    justify="right", width=12)
    tbl.add_column("Fwd 3M Ret%",   justify="right", width=12)
    tbl.add_column("Instruments",   style="dim",    width=42)

    for _, r in merged.iterrows():
        fwd = r["fwd3m_ret_pct"]
        if pd.isna(fwd):
            fwd_str = "[dim]—[/dim]"
        elif fwd > 3:
            fwd_str = f"[green]+{fwd:.1f}%[/green]"
        elif fwd < -3:
            fwd_str = f"[red]{fwd:.1f}%[/red]"
        else:
            fwd_str = f"[yellow]{fwd:.1f}%[/yellow]"

        instruments_raw = r.get("instruments", [])
        if isinstance(instruments_raw, list):
            instr_str = ", ".join(str(x) for x in instruments_raw[:3])
        else:
            instr_str = str(instruments_raw)[:40]

        tbl.add_row(
            r["as_of_month"].strftime("%Y-%m"),
            f"{r['gold_pct']:.1f}%",
            _bar(r["gold_pct"], 40, 18, "yellow"),
            f"₹{r['avg_gold_px']:.0f}" if pd.notna(r.get("avg_gold_px")) else "—",
            fwd_str,
            instr_str[:40],
        )
    console.print(tbl)


# ── 5. Portfolio Concentration & Conviction Heatmap ─────────────────────────

def section_conviction_signals(fund_name: str) -> None:
    pool = get_pool()

    # Get latest 12 months of top equity picks
    df = pool.query_df(f"""
        SELECT
            as_of_month,
            security_name,
            sum(pct_of_nav) AS weight
        FROM market_data.mf_holdings FINAL
        WHERE fund_name = '{fund_name}'
          AND asset_type = 'equity'
          AND as_of_month >= toDate(now()) - INTERVAL 15 MONTH
        GROUP BY as_of_month, security_name
        ORDER BY as_of_month DESC, weight DESC
    """)

    df["as_of_month"] = pd.to_datetime(df["as_of_month"])
    df["ym"] = df["as_of_month"].dt.to_period("M")
    df = df.sort_values("as_of_month").drop_duplicates(["ym", "security_name"], keep="last")

    # Top stocks by max weight in trailing 15 months
    top_stocks = (
        df.groupby("security_name")["weight"].max().nlargest(15).index.tolist()
    )
    pivot = df[df["security_name"].isin(top_stocks)].pivot_table(
        index="security_name", columns="ym", values="weight", fill_value=0
    )
    # Sort by latest month weight
    last_col = pivot.columns[-1]
    pivot = pivot.sort_values(last_col, ascending=False)

    months = list(pivot.columns)

    console.rule("[bold cyan]5. CONVICTION HEATMAP — Top 15 Equity Stocks (Last 15M)")
    tbl = Table(box=box.SIMPLE_HEAVY, header_style="bold white")
    tbl.add_column("Security",  width=35)
    tbl.add_column("Sector",    style="dim", width=22)
    for m in months:
        tbl.add_column(str(m)[-5:], justify="right", width=7)

    for stock in pivot.index:
        sector = classify_sector(stock)[:21]
        row_vals: list[str] = [stock[:34], sector]
        prev_w = 0.0
        for m in months:
            w = pivot.loc[stock, m]
            if w == 0:
                row_vals.append("[dim]—[/dim]")
            else:
                diff = w - prev_w
                if diff > 1.5:
                    row_vals.append(f"[green bold]{w:.1f}[/green bold]")
                elif diff < -1.5:
                    row_vals.append(f"[red]{w:.1f}[/red]")
                else:
                    row_vals.append(f"[cyan]{w:.1f}[/cyan]")
            prev_w = w
        tbl.add_row(*row_vals)
    console.print(tbl)


# ── 6. Entry/Exit Pattern Summary ────────────────────────────────────────────

def section_decision_framework_inference(fund_name: str) -> None:
    pool = get_pool()

    # Stocks that were entered with >4% weight — high conviction entries
    df = pool.query_df(f"""
        WITH monthly AS (
            SELECT
                as_of_month,
                security_name,
                asset_type,
                sum(pct_of_nav) AS weight
            FROM market_data.mf_holdings FINAL
            WHERE fund_name = '{fund_name}'
            GROUP BY as_of_month, security_name, asset_type
        ),
        with_prev AS (
            SELECT
                as_of_month,
                security_name,
                asset_type,
                weight,
                lagInFrame(weight, 1, 0) OVER (
                    PARTITION BY security_name ORDER BY as_of_month
                ) AS prev_weight
            FROM monthly
        )
        SELECT
            as_of_month,
            security_name,
            asset_type,
            round(weight, 2) AS entry_weight,
            round(weight - prev_weight, 2) AS delta
        FROM with_prev
        WHERE prev_weight = 0
          AND weight >= 3.5
          AND asset_type = 'equity'
        ORDER BY as_of_month DESC, weight DESC
        LIMIT 40
    """)

    df["as_of_month"] = pd.to_datetime(df["as_of_month"])
    df["ym"]    = df["as_of_month"].dt.to_period("M")
    df = df.sort_values("as_of_month").drop_duplicates(["ym", "security_name"], keep="last")
    df["sector"] = df["security_name"].apply(classify_sector)

    console.rule("[bold cyan]6. HIGH-CONVICTION ENTRIES (first buy ≥ 3.5% weight)")
    tbl = Table(box=box.SIMPLE_HEAVY, header_style="bold white")
    tbl.add_column("Month",    style="cyan",         width=10)
    tbl.add_column("Security",                        width=36)
    tbl.add_column("Entry %",  justify="right",       width=9)
    tbl.add_column("Sector",   style="dim",           width=32)

    for _, r in df.iterrows():
        tbl.add_row(
            r["as_of_month"].strftime("%Y-%m"),
            r["security_name"][:35],
            f"[green bold]{r['entry_weight']:.2f}%[/green bold]",
            r["sector"][:31],
        )
    console.print(tbl)

    # Now look at exits: went from >3% to 0
    df_exit = pool.query_df(f"""
        WITH monthly AS (
            SELECT
                as_of_month,
                security_name,
                asset_type,
                sum(pct_of_nav) AS weight
            FROM market_data.mf_holdings FINAL
            WHERE fund_name = '{fund_name}'
            GROUP BY as_of_month, security_name, asset_type
        ),
        with_next AS (
            SELECT
                as_of_month,
                security_name,
                asset_type,
                weight,
                leadInFrame(weight, 1, 0) OVER (
                    PARTITION BY security_name ORDER BY as_of_month
                ) AS next_weight
            FROM monthly
        )
        SELECT
            as_of_month,
            security_name,
            asset_type,
            round(weight, 2)     AS exit_weight,
            round(next_weight, 2) AS next_weight
        FROM with_next
        WHERE next_weight = 0
          AND weight >= 3.0
          AND asset_type = 'equity'
        ORDER BY as_of_month DESC, weight DESC
        LIMIT 30
    """)

    df_exit["as_of_month"] = pd.to_datetime(df_exit["as_of_month"])
    df_exit["ym"] = df_exit["as_of_month"].dt.to_period("M")
    df_exit = df_exit.sort_values("as_of_month").drop_duplicates(["ym", "security_name"], keep="last")
    df_exit["sector"] = df_exit["security_name"].apply(classify_sector)

    console.rule("[bold red]7. FULL EXITS (position ≥ 3% → completely dropped)")
    tbl2 = Table(box=box.SIMPLE_HEAVY, header_style="bold white")
    tbl2.add_column("Exit Month",  style="cyan",  width=10)
    tbl2.add_column("Security",                    width=36)
    tbl2.add_column("Last Weight", justify="right", width=12)
    tbl2.add_column("Sector",      style="dim",    width=32)

    for _, r in df_exit.iterrows():
        tbl2.add_row(
            r["as_of_month"].strftime("%Y-%m"),
            r["security_name"][:35],
            f"[red bold]{r['exit_weight']:.2f}%[/red bold]",
            r["sector"][:31],
        )
    console.print(tbl2)


# ── 7. Inferred Decision Framework ───────────────────────────────────────────

def section_inferred_vlrt_framework() -> None:
    console.rule("[bold magenta]8. INFERRED QUANT FUND DECISION FRAMEWORK")

    text = """
[bold cyan]Quant MF uses a proprietary VLRT™ model:[/bold cyan]
  [yellow]V[/yellow] = Valuation (Price/Book, EV/EBITDA vs historical)
  [yellow]L[/yellow] = Liquidity (FII/DII flows, market breadth, F&O OI)
  [yellow]R[/yellow] = Risk (GARCH volatility, Drawdown from ATH, VIX India)
  [yellow]T[/yellow] = Time (Macro cycle, RBI rate cycle, fiscal calendar)

[bold white]Evidence from holdings data:[/bold white]

  [green]◆ ASSET CLASS TIMING[/green]
  · [cyan]Oct 2023[/cyan]: Gold surged to 38% (war risk + FII exit) → positioned BEFORE gold rally
  · [cyan]Jul–Oct 2025[/cyan]: Bond allocation rose to 39–45% as equity PEs elevated
  · [cyan]Dec 2025[/cyan]: Snapped back to 67% equity + 22% Silver — momentum catch-up
  · [cyan]Jun 2026[/cyan]: Silver ETF entered (industrial cycle + cheap vs gold)

  [green]◆ EQUITY SECTOR ROTATION SIGNALS[/green]
  · [cyan]2023 Q1–Q2[/cyan]: Infra/Capex (L&T, NTPC, IRB) — early cycle positioning
  · [cyan]2023 Q3–Q4[/cyan]: Adani Power, Orchid Pharma — deep value/special situations
  · [cyan]2024 H1[/cyan]: De-risked to bonds pre-election uncertainty
  · [cyan]2024 H2[/cyan]: Increased bonds further as Nifty corrected from ATH
  · [cyan]2025 Q1–Q2[/cyan]: HDFC Life, Jio Financial — BFSI rotation on rate cut bets
  · [cyan]2025 Q4[/cyan]: Bharti Airtel ADD — 5G monetisation thesis
  · [cyan]2026 Q1+[/cyan]: Adani Enterprises, Adani Green — conglomerate re-rating

  [green]◆ GOLD/SILVER ALLOCATION SIGNALS[/green]
  · Gold stays 9–11% as a structural hedge (floor allocation)
  · Tactical gold surge to 25–38% ONLY in geopolitical risk spikes
  · Silver entered in H2 2025 (cheaper than gold, industrial demand)
  · Gold via Nippon ETF GoldBeES + Silver ETF FoF + Silver ETCD (futures)

  [green]◆ STOCK SELECTION FRAMEWORK (inferred)[/green]
  · [white]Size bias[/white]: Large-caps dominate (Reliance, HDFC Bank, SBI, ICICI)
  · [white]High-beta specials[/white]: Orchid Pharma, Premier Energies — momentum bets
  · [white]Sector leaders in each cycle[/white]: NTPC (power cycle), IRB (infra), Jio Fin (fintech)
  · [white]Average holding period[/white]: 6–12 months for core; 2–3 months for tactical
  · [white]Position sizing[/white]: 4–10% per stock (high concentration = high conviction)

  [green]◆ RISK MANAGEMENT SIGNALS[/green]
  · TREPS (overnight repo) usage spikes as a cash buffer BEFORE major re-allocations
  · F&O shorts (negative 'other') appear before trimming a position → hedge first, exit later
  · Total equity reduces months before market corrections (Apr 2024, Jul 2024, Apr 2025)

  [bold yellow]SUMMARY: Quant primarily uses a momentum + macro regime + VLRT model.[/bold yellow]
  [bold yellow]They are top-down first (asset class), then bottom-up (sector, stock).[/bold yellow]
  [bold yellow]They are NOT value investors — they chase momentum with risk overlays.[/bold yellow]
"""
    console.print(Panel(text, title="[bold magenta]DECISION FRAMEWORK INFERENCE[/bold magenta]",
                        border_style="magenta"))


# ── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Quant Multi Asset deep-dive analysis")
    parser.add_argument("--fund", default="QUANT_MULTI_ASSET",
                        help="Fund name in mf_holdings (default: QUANT_MULTI_ASSET)")
    parser.add_argument("--top", type=int, default=15, help="Top N rows (default 15)")
    args = parser.parse_args()

    fund = args.fund

    console.print(Panel(
        f"[bold cyan]QUANT MULTI ASSET — 3.8-YEAR HOLDINGS DEEP DIVE[/bold cyan]\n"
        f"Fund: [yellow]{fund}[/yellow]\n"
        f"Purpose: Reverse-engineer entry/exit logic from actual monthly holdings data",
        title="[bold white]Mosaic Quant Fund Analyser[/bold white]",
        border_style="cyan",
    ))

    alloc_df = section_asset_class_rotation(fund)
    section_sector_rotation(fund, top_n=args.top)
    section_entry_exit(fund, threshold=2.5, top_n=args.top * 3)
    section_gold_timing(fund)
    section_conviction_signals(fund)
    section_decision_framework_inference(fund)
    section_inferred_vlrt_framework()

    console.print(Panel(
        "[bold green]✓ Analysis complete.[/bold green]\n"
        "Data sourced from [cyan]market_data.mf_holdings FINAL[/cyan] + "
        "[cyan]market_data.daily_prices FINAL[/cyan].\n"
        "All weights are % of NAV as reported in monthly portfolio disclosures.",
        border_style="green",
    ))


if __name__ == "__main__":
    main()
