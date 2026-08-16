"""
scripts/whale_tracker.py
────────────────────────
Monitors "Whale" (Institutional) moves in core 2026 macro themes:
  1. Commodities (Gold/Silver)
  2. Electrification & Nuclear (NTPC, L&T, BHEL, etc.)
  3. Energy (ONGC, IOC, etc.)
  4. Infra & Real Assets (REITs, InvITs, Infra)
  5. AMC Style Archetypes, Asset Slices & Tactical Execution Profiles

Supports ALL AMCs with Multi-Asset Allocation funds stored in ClickHouse
(Nippon India, DSP, ICICI Prudential, Bajaj Finserv, Quant, Axis, Invesco,
Mirae Asset, Motilal Oswal, HDFC, Kotak Mahindra, SBI, Canara Robeco, etc.).

Usage:
  python src/scripts/market/whale_tracker.py              # Full institutional report
  python src/scripts/market/whale_tracker.py --archetypes  # AMC archetype & asset allocation matrix only
  python src/scripts/market/whale_tracker.py --amc dsp     # Filter by specific AMC
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional

# Add project root to sys.path
sys.path.append(os.getcwd())

from config.settings import settings
from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from src.tools.mf_multi_asset import get_all_multi_asset_funds, get_amc_archetype_profiles

# Key themes to track
THEME_KEYWORDS = {
    "🥈 Silver": ["Silver", "SILVERBEES"],
    "🥇 Gold": ["Gold", "GOLDBEES"],
    "⚛️ Nuclear/Grid": ["NTPC", "L&T", "Larsen", "BHEL", "Bharat Heavy", "Power Grid", "POWERGRID"],
    "🛢️ Energy": ["ONGC", "IOC", "Coal India", "BPCL", "HPCL", "GAIL", "Adani Green", "Premier Energies"],
    "🏗️ Infra": ["Larsen", "L&T", "Reliance", "Adani Ports", "NMDC", "REC", "PFC", "Embassy", "Mindspace", "Cube Highways"],
}


def get_fund_holdings(client: Any, query_filter: str, as_of_month: str) -> Dict[str, float]:
    """Fetch security_name -> sum(pct_of_nav) for a given fund filter and month."""
    query = f"""
    SELECT security_name, sum(pct_of_nav)
    FROM market_data.mf_holdings FINAL
    WHERE ({query_filter}) AND as_of_month = '{as_of_month}'
    GROUP BY security_name
    """
    res = client.query(query).result_rows
    return {row[0]: float(row[1]) for row in res}


def render_amc_archetypes(profiles: List[Dict[str, Any]], amc_filter: Optional[str] = None) -> None:
    console = Console()
    
    if amc_filter:
        profiles = [p for p in profiles if amc_filter.lower() in p["fund_label"].lower()]

    if not profiles:
        console.print("[yellow]No AMC profiles found matching filter.[/yellow]")
        return

    console.print("\n" + "=" * 80)
    console.print(Panel(
        "[bold cyan]🏛️ AMC MULTI-ASSET ALLOCATION & ARCHETYPE SCORECARD[/bold cyan]\n"
        "[dim]Comprehensive Asset Slices, Strategic Archetypes & Execution Styles across Indian AMCs[/dim]",
        border_style="cyan"
    ))

    t = Table(title="AMC Multi-Asset Allocations & Style Matrix", box=box.ROUNDED, show_header=True)
    t.add_column("AMC / Fund", style="bold magenta", min_width=22)
    t.add_column("AUM (₹ Cr)", justify="right")
    t.add_column("Equity %", justify="right")
    t.add_column("Gold %", justify="right")
    t.add_column("Silver %", justify="right")
    t.add_column("Debt %", justify="right")
    t.add_column("REITs %", justify="right")
    t.add_column("Cash %", justify="right")
    t.add_column("Strategic Archetype", style="bold green", min_width=26)
    t.add_column("Primary Differentiator", style="dim", min_width=30)

    for p in profiles:
        t.add_row(
            p["fund_label"],
            f"₹{p['total_aum_cr']:,.0f}",
            f"{p['equity_pct']:.1f}%",
            f"{p['gold_pct']:.1f}%",
            f"{p['silver_pct']:.1f}%",
            f"{p['debt_pct']:.1f}%",
            f"{p['reit_pct']:.1f}%",
            f"{p['cash_pct']:.1f}%",
            p["archetype"],
            p["key_differentiator"],
        )
    console.print(t)

    # Detailed Top Holdings per AMC
    console.print("\n" + "=" * 80)
    t_det = Table(title="AMC Top Equity Holdings & Execution Flavour", box=box.ROUNDED, show_header=True)
    t_det.add_column("AMC / Fund", style="bold cyan", min_width=22)
    t_det.add_column("Top Equity Holdings", min_width=45)
    t_det.add_column("Derivatives / Overlay Slice", justify="right")
    t_det.add_column("Latest Snapshot", justify="center")

    for p in profiles:
        eq_str = ", ".join(p["top_equities"]) if p["top_equities"] else "—"
        deriv_str = f"{p['derivatives_pct']:+.2f}%" if abs(p["derivatives_pct"]) > 0.01 else "—"
        t_det.add_row(
            p["fund_label"],
            eq_str,
            deriv_str,
            p["as_of_month"],
        )
    console.print(t_det)


def run_whale_tracker(amc_filter: Optional[str] = None, archetypes_only: bool = False, top_n: int = 15) -> None:
    console = Console()
    from src.db.pool import get_client
    client = get_client()

    whale_funds = get_all_multi_asset_funds(client)

    if amc_filter:
        whale_funds = [f for f in whale_funds if amc_filter.lower() in f["label"].lower()]

    if not archetypes_only:
        console.print(Panel(
            f"[bold cyan]🐋 Whale Tracker: Institutional Macro Moves[/bold cyan]\n"
            f"[dim]Tracking weight shifts across {len(whale_funds)} Multi-Asset Funds in ClickHouse "
            f"(Silver, Gold, Nuclear/Grid, Energy, Infra)[/dim]",
            border_style="cyan"
        ))

        # Composite aggregations for the Institutional Conviction Index
        composite_latest: Dict[str, float] = {}
        composite_prev: Dict[str, float] = {}
        fund_ownership: Dict[str, List[str]] = {}

        multi_month_count = 0
        single_month_count = 0

        for fund in whale_funds:
            fund_name = fund["name"]
            query_filter = fund["query_filter"]

            months_query = (
                f"SELECT DISTINCT as_of_month FROM market_data.mf_holdings FINAL "
                f"WHERE ({query_filter}) ORDER BY as_of_month DESC LIMIT 2"
            )
            months = [str(r[0]) for r in client.query(months_query).result_rows]

            if not months:
                continue

            latest_m = months[0]
            latest_h = get_fund_holdings(client, query_filter, latest_m)

            for sec, val in latest_h.items():
                composite_latest[sec] = composite_latest.get(sec, 0.0) + val
                if sec not in fund_ownership:
                    fund_ownership[sec] = []
                fund_ownership[sec].append(fund_name)

            if len(months) >= 2:
                multi_month_count += 1
                prev_m = months[1]
                prev_h = get_fund_holdings(client, query_filter, prev_m)

                for sec, val in prev_h.items():
                    composite_prev[sec] = composite_prev.get(sec, 0.0) + val

                table = Table(
                    title=f"{fund_name} (Changes: {prev_m} → {latest_m})",
                    box=box.ROUNDED,
                    show_header=True
                )
                table.add_column("Theme", style="dim")
                table.add_column("Security", style="magenta")
                table.add_column("Prev %", justify="right")
                table.add_column("Latest %", justify="right")
                table.add_column("Change", justify="right")

                has_changes = False
                all_securities = set(latest_h.keys()) | set(prev_h.keys())

                for sec in all_securities:
                    theme_label = None
                    for label, kws in THEME_KEYWORDS.items():
                        if any(kw.lower() in sec.lower() for kw in kws):
                            theme_label = label
                            break

                    if not theme_label:
                        continue

                    prev_val = prev_h.get(sec, 0.0)
                    curr_val = latest_h.get(sec, 0.0)
                    diff = curr_val - prev_val

                    if abs(diff) > 0.01:
                        has_changes = True
                        diff_str = f"{diff:+.2f}%"
                        if diff > 0.5:
                            style = "bold green"
                        elif diff < -0.5:
                            style = "bold red"
                        elif diff > 0:
                            style = "green"
                        elif diff < 0:
                            style = "red"
                        else:
                            style = "dim"

                        table.add_row(
                            theme_label,
                            sec,
                            f"{prev_val:.2f}%",
                            f"{curr_val:.2f}%",
                            f"[{style}]{diff_str}[/{style}]"
                        )

                if has_changes:
                    console.print(table)
                else:
                    console.print(f"[dim]  - {fund_name}: No significant changes in tracked themes ({prev_m} → {latest_m}).[/dim]")

            else:
                single_month_count += 1
                table = Table(
                    title=f"{fund_name} (Baseline: {latest_m} - 1st Month Ingested)",
                    box=box.ROUNDED,
                    show_header=True
                )
                table.add_column("Theme", style="dim")
                table.add_column("Security", style="magenta")
                table.add_column("Weight %", justify="right")

                has_themes = False
                for sec, curr_val in latest_h.items():
                    theme_label = None
                    for label, kws in THEME_KEYWORDS.items():
                        if any(kw.lower() in sec.lower() for kw in kws):
                            theme_label = label
                            break
                    if theme_label and curr_val > 0.10:
                        has_themes = True
                        table.add_row(theme_label, sec, f"{curr_val:.2f}%")

                if has_themes:
                    console.print(table)
                else:
                    console.print(f"[dim]  - {fund_name}: Initiated tracking at {latest_m} (no major theme weights > 0.10%).[/dim]")

        # ── 2. Composite Institutional Conviction Index ──────────────────────────────
        console.print("\n" + "=" * 80)
        console.print(Panel(
            f"[bold green]🐳 COMPOSITE INSTITUTIONAL CONVICTION INDEX (ALL {len(whale_funds)} FUNDS)[/bold green]\n"
            f"[dim]Aggregated multi-asset flows and cross-ownership conviction signals ({multi_month_count} funds with delta shifts, {single_month_count} baseline funds)[/dim]",
            border_style="green"
        ))

        # 2a. Theme Aggregations
        theme_latest = {}
        theme_prev = {}

        for label, kws in THEME_KEYWORDS.items():
            theme_latest[label] = 0.0
            theme_prev[label] = 0.0

            for sec in composite_latest:
                if any(kw.lower() in sec.lower() for kw in kws):
                    theme_latest[label] += composite_latest[sec]
            for sec in composite_prev:
                if any(kw.lower() in sec.lower() for kw in kws):
                    theme_prev[label] += composite_prev[sec]

        theme_table = Table(title="Unified Macro Theme Allocations Across All AMCs", box=box.ROUNDED, show_header=True)
        theme_table.add_column("Macro Theme", style="bold cyan")
        theme_table.add_column("Combined Prev Weight", justify="right")
        theme_table.add_column("Combined Latest Weight", justify="right")
        theme_table.add_column("Net Flow Change", justify="right")

        for theme, curr_val in theme_latest.items():
            prev_val = theme_prev.get(theme, 0.0)
            diff = curr_val - prev_val
            diff_str = f"{diff:+.2f}%"

            if diff > 0.5:
                style = "bold green"
            elif diff < -0.5:
                style = "bold red"
            elif diff > 0:
                style = "green"
            elif diff < 0:
                style = "red"
            else:
                style = "dim"

            theme_table.add_row(
                theme,
                f"{prev_val:.2f}%",
                f"{curr_val:.2f}%",
                f"[{style}]{diff_str}[/{style}]"
            )
        console.print(theme_table)

        # 2b. High Conviction Single-Name Equities (Cross-ownership >= 2 funds)
        exclude_kws = THEME_KEYWORDS["🥈 Silver"] + THEME_KEYWORDS["🥇 Gold"] + [
            "cash", "liquid", "treasury", "arbitrage", "mutual fund", "yield", "margin", "repo", "treps"
        ]

        equity_conviction = []
        for sec, funds in fund_ownership.items():
            if any(kw.lower() in sec.lower() for kw in exclude_kws):
                continue

            num_funds = len(funds)
            if num_funds >= 2:
                prev_val = composite_prev.get(sec, 0.0)
                curr_val = composite_latest.get(sec, 0.0)
                diff = curr_val - prev_val

                if num_funds >= 3 and diff > 0:
                    rating = "🔥 CORE CONVICTION"
                    r_style = "bold green"
                elif num_funds >= 2 and diff > 0.3:
                    rating = "📈 TACTICAL ADD"
                    r_style = "green"
                elif diff < -0.3:
                    rating = "⚠️ TRIMMING"
                    r_style = "bold red"
                else:
                    rating = "HOLDING"
                    r_style = "dim"

                equity_conviction.append({
                    "security": sec,
                    "num_funds": num_funds,
                    "prev_val": prev_val,
                    "curr_val": curr_val,
                    "diff": diff,
                    "rating": rating,
                    "r_style": r_style,
                    "funds_str": ", ".join(funds[:3]) + (f" +{len(funds)-3}" if len(funds) > 3 else "")
                })

        equity_conviction.sort(key=lambda x: (x["num_funds"], x["curr_val"]), reverse=True)

        equity_table = Table(title="High-Conviction Equity Cross-Ownership Across All AMCs", box=box.ROUNDED, show_header=True)
        equity_table.add_column("Security Name", style="magenta")
        equity_table.add_column("Funds Count", justify="center")
        equity_table.add_column("Combined Prev %", justify="right")
        equity_table.add_column("Combined Latest %", justify="right")
        equity_table.add_column("Net Change", justify="right")
        equity_table.add_column("Conviction Rating", justify="center")
        equity_table.add_column("Key Holding Funds", style="dim")

        for item in equity_conviction[:top_n]:
            diff = item["diff"]
            diff_str = f"{diff:+.2f}%"
            if diff > 0.3:
                d_style = "green"
            elif diff < -0.3:
                d_style = "red"
            else:
                d_style = "dim"

            equity_table.add_row(
                item["security"],
                str(item["num_funds"]),
                f"{item['prev_val']:.2f}%",
                f"{item['curr_val']:.2f}%",
                f"[{d_style}]{diff_str}[/{d_style}]",
                f"[{item['r_style']}]{item['rating']}[/{item['r_style']}]",
                item["funds_str"],
            )

        if equity_conviction:
            console.print(equity_table)
        else:
            console.print("[dim]  - No multi-fund cross-ownership detected in direct equities.[/dim]")

    # ── 3. AMC Multi-Asset Archetypes & Asset Slices ─────────────────────────
    profiles = get_amc_archetype_profiles(client)
    render_amc_archetypes(profiles, amc_filter)

    client.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Whale Tracker: Multi-Asset institutional flows, cross-ownership conviction & AMC archetype profiling."
    )
    parser.add_argument("--amc", type=str, default=None, help="Filter results by specific AMC (e.g. dsp, nippon, icici, quant, sbi)")
    parser.add_argument("--archetypes", action="store_true", help="Display only the AMC Multi-Asset Archetype & Asset Allocation Scorecard")
    parser.add_argument("--top", type=int, default=20, help="Top N equity cross-ownership holdings (default: 20)")
    args = parser.parse_args()

    run_whale_tracker(amc_filter=args.amc, archetypes_only=args.archetypes, top_n=args.top)


if __name__ == "__main__":
    main()
