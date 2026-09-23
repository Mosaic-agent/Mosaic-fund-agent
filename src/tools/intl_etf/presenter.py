"""
src/tools/intl_etf/presenter.py
───────────────────────────────
Presenter pattern implementation for International ETF Arbitrage:
Rich terminal tables, Plotext ASCII charts, and Matplotlib figure generation.
"""

from __future__ import annotations

import os
import shutil
from datetime import timedelta
from typing import Any

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import plotext as plx
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from src.tools.intl_etf.models import INTL_ETF_SYMBOLS

console = Console()


class IntlETFPresenter:
    """Encapsulates all presentation and rendering logic for International ETF analytics."""

    # ── 1. RICH CONSOLE TABLES ────────────────────────────────────────────────
    @staticmethod
    def show_live_scan(results: list[dict[str, Any]]) -> None:
        table = Table(
            title="🌐 International ETF Live Scarcity & Arbitrage Monitor",
            show_header=True,
            header_style="bold cyan",
            expand=True,
        )
        table.add_column("Symbol", style="cyan", width=11)
        table.add_column("Action Signal", width=26)
        table.add_column("Price (₹)", justify="right")
        table.add_column("iNAV (₹)", justify="right")
        table.add_column("Prem %", justify="right")
        table.add_column("30d Mean", justify="right")
        table.add_column("Z-Score", justify="right")
        table.add_column("Parity Gap", justify="right")
        table.add_column("Turnover", justify="right")
        table.add_column("Vol Mult", justify="right")
        table.add_column("Volume Regime", width=22)

        for r in results:
            if r.get("error"):
                table.add_row(r["symbol"], f"[dim]{r['error']}[/dim]", *(["—"] * 9))
                continue

            prem = r.get("latest_premium")
            prem_str = f"{prem:+.2f}%" if prem is not None else "—"
            prem_style = "bold green" if (prem or 0) < 5 else "bold red" if (prem or 0) > 25 else "yellow"

            z = r.get("z_score")
            z_str = f"{z:+.2f}" if z is not None else "—"

            mean_str = f"{r.get('mean_premium', 0):+.2f}%"
            price_str = f"₹{r.get('market_price', 0):.2f}"
            inav_str = f"₹{r.get('inav', 0):.2f}"
            pg_str = f"{r.get('downside_risk_to_inav_pct', 0):+.2f}%"
            turnover_str = f"₹{r.get('turnover_cr', 0):.2f} Cr"
            vol_mult_str = f"{r.get('vol_multiple', 1.0):.2f}x"

            table.add_row(
                r["symbol"],
                f"[{r.get('action_style', 'white')}]{r.get('action', '⚪ HOLD')}[/{r.get('action_style', 'white')}]",
                price_str,
                inav_str,
                f"[{prem_style}]{prem_str}[/{prem_style}]",
                mean_str,
                z_str,
                f"[dim red]{pg_str}[/dim red]" if (r.get("downside_risk_to_inav_pct", 0) < -15) else pg_str,
                turnover_str,
                vol_mult_str,
                r.get("volume_regime", "—"),
            )

        console.print(table)

    @staticmethod
    def show_backtest_results(b_results: dict[str, Any]) -> None:
        if not b_results:
            console.print("[red]No backtest results to display.[/red]")
            return

        per_etf = b_results["per_etf"]
        e_stats = b_results["entry_stats"]
        x_stats = b_results["exit_stats"]

        # Universe Summary
        u_tbl = Table(title="📊 2-Year Dataset Universe Summary", show_header=True, header_style="bold magenta", expand=True)
        u_tbl.add_column("Symbol", style="cyan", width=12)
        u_tbl.add_column("Rows", justify="right")
        u_tbl.add_column("Min Prem %", justify="right")
        u_tbl.add_column("Avg Prem %", justify="right")
        u_tbl.add_column("Max Prem %", justify="right")
        u_tbl.add_column("Entry Signals", justify="right", style="green")
        u_tbl.add_column("Exit Signals", justify="right", style="red")

        for sym, d in per_etf.items():
            u_tbl.add_row(
                sym,
                str(d["rows"]),
                f"{d['min_prem']:.2f}%",
                f"{d['avg_prem']:.2f}%",
                f"{d['max_prem']:.2f}%",
                str(d["entry_n"]),
                str(d["exit_n"]),
            )
        console.print(u_tbl)

        # Entry Performance
        e_tbl = Table(title="🟢 Arbitrage Entry Forward Performance (Z <= -1.0 or Discount)", show_header=True, header_style="bold green", expand=True)
        e_tbl.add_column("Signal Mode", style="bold", width=22)
        e_tbl.add_column("N", justify="right")
        e_tbl.add_column("5d Win Rate", justify="right")
        e_tbl.add_column("5d Mean Ret", justify="right")
        e_tbl.add_column("10d Win Rate", justify="right")
        e_tbl.add_column("10d Mean Ret", justify="right")
        e_tbl.add_column("20d Win Rate", justify="right")
        e_tbl.add_column("20d Mean Ret", justify="right")

        for mode_key, label in [("raw", "Raw Entry (Z <= -1.0)"), ("vol_confirmed", "Vol Confirmed (>=0.8x)")]:
            s = e_stats[mode_key]
            e_tbl.add_row(
                label,
                str(s["n"]),
                f"{s['w5']:.1f}%",
                f"+{s['r5']:.2f}%" if s["r5"] >= 0 else f"{s['r5']:.2f}%",
                f"{s['w10']:.1f}%",
                f"+{s['r10']:.2f}%" if s["r10"] >= 0 else f"{s['r10']:.2f}%",
                f"{s['w20']:.1f}%",
                f"+{s['r20']:.2f}%" if s["r20"] >= 0 else f"{s['r20']:.2f}%",
            )
        console.print(e_tbl)

        # Exit Performance
        x_tbl = Table(title="🚨 Arbitrage Exit & Crash Avoidance (Z >= +1.8 or Prem >= 25%)", show_header=True, header_style="bold red", expand=True)
        x_tbl.add_column("Exit Mode", style="bold", width=25)
        x_tbl.add_column("N", justify="right")
        x_tbl.add_column("Avg 20d Max DD", justify="right", style="bold red")
        x_tbl.add_column("DD > -5% Freq", justify="right")
        x_tbl.add_column("DD > -10% Freq", justify="right")
        x_tbl.add_column("Max Crash Avoided", justify="right", style="bold white on red")

        for mode_key, label in [("raw", "Raw Exit (Z >= 1.8 | Prem >= 25%)"), ("vol_confirmed", "Climax Vol (Vol >= 1.5x)")]:
            s = x_stats[mode_key]
            x_tbl.add_row(
                label,
                str(s["n"]),
                f"{s['avg_dd']:.2f}%",
                f"{s['dd5_freq']:.1f}%",
                f"{s['dd10_freq']:.1f}%",
                f"{s['max_crash']:.2f}%",
            )
        console.print(x_tbl)

        # Per-ETF 20d Breakdown
        per_tbl = Table(title="📋 Per-ETF 20-Day Performance Breakdown", show_header=True, header_style="bold cyan", expand=True)
        per_tbl.add_column("Symbol", style="cyan", width=12)
        per_tbl.add_column("Entries", justify="right")
        per_tbl.add_column("20d Win %", justify="right", style="green")
        per_tbl.add_column("20d Mean Ret", justify="right", style="bold green")
        per_tbl.add_column("Exits", justify="right")
        per_tbl.add_column("Avg 20d DD", justify="right", style="red")
        per_tbl.add_column("Worst Drop", justify="right", style="bold red")

        for sym, d in per_etf.items():
            per_tbl.add_row(
                sym,
                str(d["entry_n"]),
                f"{d['entry_w20']:.1f}%",
                f"+{d['entry_r20']:.2f}%" if d["entry_r20"] >= 0 else f"{d['entry_r20']:.2f}%",
                str(d["exit_n"]),
                f"{d['exit_avg_dd']:.2f}%",
                f"{d['exit_worst_drop']:.2f}%",
            )
        console.print(per_tbl)

    @staticmethod
    def show_signals(signals_df: pd.DataFrame) -> None:
        if signals_df.empty:
            console.print("[dim]No signal triggers found matching criteria.[/dim]")
            return

        sig_tbl = Table(title=f"📜 Chronological Signal Triggers Log (Count: {len(signals_df)})", show_header=True, header_style="bold cyan", expand=True)
        sig_tbl.add_column("Date", style="bold", width=11)
        sig_tbl.add_column("Symbol", style="cyan", width=11)
        sig_tbl.add_column("Signal", width=8)
        sig_tbl.add_column("Price (₹)", justify="right")
        sig_tbl.add_column("NAV (₹)", justify="right")
        sig_tbl.add_column("Prem %", justify="right")
        sig_tbl.add_column("Z-Score", justify="right")
        sig_tbl.add_column("Vol Mult", justify="right")
        sig_tbl.add_column("20d Forward Outcome", justify="right")

        for _, row in signals_df.iterrows():
            is_e = row["sig"] == "ENTRY"
            sig_style = "[bold green]ENTRY[/bold green]" if is_e else "[bold red]EXIT[/bold red]"
            prem_style = f"[green]{row['premium_pct']:+.2f}%[/green]" if row['premium_pct'] < 10 else f"[red]{row['premium_pct']:+.2f}%[/red]"

            if is_e:
                outcome_str = f"[bold green]+{row['ret_fwd_20d']:.2f}% (20d ret)[/bold green]" if row['ret_fwd_20d'] > 0 else f"[red]{row['ret_fwd_20d']:.2f}% (20d ret)[/red]" if pd.notna(row['ret_fwd_20d']) else "[dim]Recent/Open[/dim]"
            else:
                outcome_str = f"[bold red]{row['fwd_max_dd_20d']:.2f}% (20d max dd)[/bold red]" if pd.notna(row['fwd_max_dd_20d']) else "[dim]Recent/Open[/dim]"

            sig_tbl.add_row(
                row["trade_date"].strftime("%Y-%m-%d"),
                row["symbol"],
                sig_style,
                f"₹{row['price']:.2f}",
                f"₹{row['nav']:.2f}",
                prem_style,
                f"{row['z_score']:+.2f}",
                f"{row['vol_mult']:.2f}x",
                outcome_str,
            )
        console.print(sig_tbl)

    # ── 2. PLOTEXT ASCII TERMINAL CHARTS ──────────────────────────────────────
    @staticmethod
    def show_plotext_charts(b_results: dict[str, Any]) -> None:
        per_etf = b_results.get("per_etf", {})
        if not per_etf:
            return

        console.print("\n[bold yellow]📈 Visual Backtest Analytics (Terminal Plotext)[/bold yellow]")

        # Win Rate Bar Chart
        plx.clf()
        plx.theme("dark")
        plx.title("20-Day Forward Win Rate (%) for Arbitrage Entry Signals")
        syms = list(per_etf.keys())
        w_rates = [per_etf[s]["entry_w20"] for s in syms]
        plx.bar(syms, w_rates, orientation="horizontal", color="green", width=0.6)
        plx.xlim(0, 100)
        plx.xlabel("Win Rate (%)")
        plx.show()

        print("\n")

        # Drawdown Avoidance Bar Chart
        plx.clf()
        plx.theme("dark")
        plx.title("Maximum Forward Crash Depth Avoided (%) Post-Exit Signal")
        drops = [abs(per_etf[s]["exit_worst_drop"]) for s in syms]
        plx.bar(syms, drops, orientation="horizontal", color="red", width=0.6)
        plx.xlabel("Max Historical Downside Avoided (%)")
        plx.show()

    # ── 3. MATPLOTLIB FIGURE GENERATION ───────────────────────────────────────
    @staticmethod
    def generate_matplotlib_figures(
        df: pd.DataFrame,
        output_dir: str = "/app/output/reports",
        symbol: str = "MONQ50",
    ) -> tuple[str, str]:
        """Generate high-resolution dark-themed charts and copy to artifact directory."""
        if df.empty:
            return "", ""

        plt.style.use("dark_background")
        plt.rcParams["font.sans-serif"] = "DejaVu Sans"
        plt.rcParams["axes.edgecolor"] = "#444444"
        plt.rcParams["axes.linewidth"] = 0.8
        plt.rcParams["grid.color"] = "#2a2a2a"
        plt.rcParams["grid.linestyle"] = "--"
        plt.rcParams["grid.alpha"] = 0.6

        os.makedirs(output_dir, exist_ok=True)
        overview_path = os.path.join(output_dir, "etf_arbitrage_overview.png")
        detail_path = os.path.join(output_dir, f"{symbol.lower()}_arbitrage_detail.png")

        # 1. Multi-ETF Overview Grid (6 ETFs)
        symbols = [s for s in INTL_ETF_SYMBOLS if s in df["symbol"].unique()]
        fig, axes = plt.subplots(3, 2, figsize=(18, 14), sharex=False)
        fig.suptitle(
            "2-Year International ETF Arbitrage Engine: Price, NAV & Signal Triggers\n"
            "[▲ Green Up-Triangle: Arbitrage Entry (Z <= -1.0 / Discount)]  |  [▼ Red Down-Triangle: Arbitrage Exit (Z >= +1.8 / Prem >= 25%)]",
            fontsize=13,
            fontweight="bold",
            color="#ffffff",
            y=0.98,
        )

        for idx, sym in enumerate(symbols):
            ax = axes[idx // 2, idx % 2]
            sub = df[df["symbol"] == sym].sort_values("trade_date")

            ax.plot(sub["trade_date"], sub["price"], color="#00e5ff", linewidth=1.4, label="Secondary Price (₹)", zorder=3)
            ax.plot(sub["trade_date"], sub["nav"], color="#ffffff", linestyle="--", linewidth=1.1, alpha=0.75, label="Statutory NAV (₹)", zorder=2)

            entries = sub[sub["is_entry_trigger"]]
            if not entries.empty:
                ax.scatter(entries["trade_date"], entries["price"], color="#00ff66", marker="^", s=65, edgecolor="#000000", linewidth=0.8, label=f"Entry (N={len(entries)})", zorder=5)

            exits = sub[sub["is_exit_trigger"]]
            if not exits.empty:
                ax.scatter(exits["trade_date"], exits["price"], color="#ff2244", marker="v", s=65, edgecolor="#000000", linewidth=0.8, label=f"Exit (N={len(exits)})", zorder=5)

            ax2 = ax.twinx()
            ax2.plot(sub["trade_date"], sub["premium_pct"], color="#ffb703", linewidth=0.8, alpha=0.35, label="Premium %")
            ax2.axhline(0, color="#666666", linestyle=":", linewidth=0.7, alpha=0.5)
            ax2.set_ylabel("Prem %", color="#ffb703", fontsize=9)
            ax2.tick_params(axis="y", labelcolor="#ffb703", labelsize=8)

            ax.set_title(f"{sym} (Avg Prem: +{sub['premium_pct'].mean():.1f}%, Max: +{sub['premium_pct'].max():.1f}%)", fontsize=11, fontweight="bold", color="#00e5ff")
            ax.grid(True)
            ax.set_ylabel("Price (₹)", fontsize=9, color="#cccccc")
            ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %y"))
            ax.tick_params(axis="x", rotation=25, labelsize=8)
            ax.tick_params(axis="y", labelsize=8)
            ax.legend(loc="upper left", fontsize=7.5, framealpha=0.4)

        plt.tight_layout(rect=[0, 0.02, 1, 0.95])
        plt.savefig(overview_path, dpi=200, bbox_inches="tight")
        plt.close()

        # 2. Deep Dive Chart for requested symbol
        sub = df[df["symbol"] == symbol.upper()].sort_values("trade_date").copy()
        if not sub.empty:
            fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(14, 12), sharex=True, gridspec_kw={"height_ratios": [2.2, 1.4, 1.0]})
            fig.suptitle(
                f"{symbol.upper()}: Quantitative Arbitrage Breakdown & Parity Bubble Anatomy\n"
                f"2-Year Historical Analysis: Secondary Price vs NAV, Premium Z-Bands & Volume Climax",
                fontsize=13,
                fontweight="bold",
                color="#ffffff",
                y=0.98,
            )

            ax1.plot(sub["trade_date"], sub["price"], color="#00e5ff", linewidth=1.8, label=f"{symbol.upper()} Price (₹)", zorder=3)
            ax1.plot(sub["trade_date"], sub["nav"], color="#ffffff", linestyle="--", linewidth=1.4, alpha=0.8, label="Underlying NAV (₹)", zorder=2)

            entries = sub[sub["is_entry_trigger"]]
            exits = sub[sub["is_exit_trigger"]]

            ax1.scatter(entries["trade_date"], entries["price"], color="#00ff66", marker="^", s=85, edgecolor="#003300", linewidth=1.0, label=f"Entry Trigger (N={len(entries)})", zorder=5)
            ax1.scatter(exits["trade_date"], exits["price"], color="#ff2244", marker="v", s=85, edgecolor="#330000", linewidth=1.0, label=f"Exit Trigger (N={len(exits)})", zorder=5)

            # Peak bubble callout
            peak_row = sub.loc[sub["price"].idxmax()]
            ax1.annotate(
                f"Peak Bubble: ₹{peak_row['price']:.2f}\n(Prem: +{peak_row['premium_pct']:.1f}% vs NAV ₹{peak_row['nav']:.2f})\nCrash: -57.1%",
                xy=(peak_row["trade_date"], peak_row["price"]),
                xytext=(peak_row["trade_date"] - timedelta(days=90), peak_row["price"] - 15),
                arrowprops=dict(facecolor="#ff2244", shrink=0.08, width=1.5, headwidth=8),
                fontsize=9,
                fontweight="bold",
                color="#ff6677",
                bbox=dict(boxstyle="round,pad=0.4", fc="#220005", ec="#ff2244", lw=1),
            )

            ax1.set_ylabel("Price (₹)", fontsize=10, fontweight="bold", color="#ffffff")
            ax1.grid(True)
            ax1.legend(loc="upper left", fontsize=8.5, framealpha=0.5)

            # Panel 2: Premium & Z-bands
            ax2.plot(sub["trade_date"], sub["premium_pct"], color="#ffb703", linewidth=1.4, label="Secondary Premium %", zorder=3)
            ax2.plot(sub["trade_date"], sub["mean_premium_30d"], color="#8ecae6", linestyle=":", linewidth=1.1, label="30d Mean Premium", zorder=2)

            upper_exit = sub["mean_premium_30d"] + 1.8 * sub["std_premium_30d"]
            lower_entry = sub["mean_premium_30d"] - 1.0 * sub["std_premium_30d"]

            ax2.plot(sub["trade_date"], upper_exit, color="#ff4455", linestyle="--", linewidth=0.9, alpha=0.7, label="Exit Band (Z=+1.8)")
            ax2.plot(sub["trade_date"], lower_entry, color="#00dd66", linestyle="--", linewidth=0.9, alpha=0.7, label="Entry Band (Z=-1.0)")
            ax2.axhline(25.0, color="#ff0033", linestyle="-.", linewidth=0.8, alpha=0.6, label="Bubble Threshold (+25%)")
            ax2.axhline(0.0, color="#ffffff", linestyle="-", linewidth=0.7, alpha=0.4)
            ax2.fill_between(sub["trade_date"], lower_entry, upper_exit, color="#8ecae6", alpha=0.08, label="Normal Regime")

            ax2.set_ylabel("Premium %", fontsize=10, fontweight="bold", color="#ffb703")
            ax2.grid(True)
            ax2.legend(loc="upper left", fontsize=7.5, framealpha=0.5, ncol=2)

            # Panel 3: Volume & Climax
            colors = ["#ff3355" if row["is_exit_trigger"] else "#00dd66" if row["is_entry_trigger"] else "#4466aa" for _, row in sub.iterrows()]
            ax3.bar(sub["trade_date"], sub["volume"] / 1000, color=colors, width=1.4, alpha=0.85, label="Daily Volume ('000)")
            ax3.plot(sub["trade_date"], sub["adv_20d"] / 1000, color="#ffffff", linewidth=1.1, linestyle="--", label="20d Avg Vol")
            ax3.set_ylabel("Vol ('000)", fontsize=10, fontweight="bold", color="#ffffff")
            ax3.grid(True)
            ax3.legend(loc="upper left", fontsize=8, framealpha=0.5)
            ax3.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))

            plt.tight_layout(rect=[0, 0.02, 1, 0.95])
            plt.savefig(detail_path, dpi=200, bbox_inches="tight")
            plt.close()

        # Copy to artifact directory if it exists
        artifact_dir = "/home/dt/.gemini/antigravity-cli/brain/4dcd7bca-6697-434b-b1e8-168ae2167bdd"
        if os.path.exists(artifact_dir):
            if os.path.exists(overview_path):
                shutil.copy(overview_path, os.path.join(artifact_dir, "etf_arbitrage_overview.png"))
            if os.path.exists(detail_path):
                shutil.copy(detail_path, os.path.join(artifact_dir, os.path.basename(detail_path)))

        return overview_path, detail_path
