"""
src/scripts/portfolio/rolling_returns.py
────────────────────────────────────────
Rolling Return Distribution Engine for any Indian Mutual Fund.

Computes daily rolling 1Y, 3Y, and 5Y CAGR distributions, showing
probability of loss, median return, percentile bands, and consistency.

Usage:
  python src/scripts/portfolio/rolling_returns.py --scheme 152056
  python src/scripts/portfolio/rolling_returns.py --scheme 119212 --windows 1Y,3Y
"""

import argparse
import os
import sys
from datetime import datetime

import numpy as np
import pandas as pd
import requests
from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

MFAPI_BASE = "https://api.mfapi.in/mf"


def fetch_nav_history(scheme_code: str) -> tuple[str, pd.DataFrame]:
    """Fetch full NAV history from mfapi.in for any MF scheme."""
    try:
        resp = requests.get(f"{MFAPI_BASE}/{scheme_code}", timeout=15)
        if resp.status_code != 200:
            return f"Scheme {scheme_code}", pd.DataFrame()

        payload = resp.json()
        scheme_name = payload.get("meta", {}).get("scheme_name", f"Scheme {scheme_code}")

        raw = payload.get("data", [])
        if not raw:
            return scheme_name, pd.DataFrame()

        rows = []
        for entry in raw:
            try:
                date = datetime.strptime(entry["date"], "%d-%m-%Y").date()
                nav = float(entry["nav"])
                rows.append({"date": date, "nav": nav})
            except (ValueError, KeyError):
                continue

        df = pd.DataFrame(rows).sort_values("date").reset_index(drop=True)
        return scheme_name, df

    except Exception as e:
        return f"Scheme {scheme_code}", pd.DataFrame()


def calculate_rolling_returns(dates: np.ndarray, navs: np.ndarray, window_days: int) -> np.ndarray:
    """Compute rolling CAGR for a given window in trading days."""
    n = len(navs)
    if n <= window_days:
        return np.array([])

    returns = []
    for i in range(window_days, n):
        start_nav = navs[i - window_days]
        end_nav = navs[i]
        days_diff = (dates[i] - dates[i - window_days]).days
        if days_diff <= 0 or start_nav <= 0:
            continue
        cagr = (end_nav / start_nav) ** (365.0 / days_diff) - 1.0
        returns.append(cagr * 100.0)

    return np.array(returns)


def main():
    parser = argparse.ArgumentParser(description="Rolling Returns Distribution Engine")
    parser.add_argument("--scheme", type=str, required=True, help="AMFI scheme code (e.g. 152056, 119212)")
    parser.add_argument("--windows", type=str, default="1Y,3Y,5Y", help="Comma-separated windows (e.g. 1Y,3Y,5Y)")
    args = parser.parse_args()

    console = Console()
    windows_map = {"1Y": 252, "3Y": 756, "5Y": 1260}  # trading days
    requested = [w.strip() for w in args.windows.split(",")]

    with console.status("[bold green]Fetching NAV history from mfapi.in..."):
        scheme_name, df = fetch_nav_history(args.scheme)

    if df.empty:
        console.print(f"[bold red]No NAV data found for scheme {args.scheme}[/bold red]")
        return

    dates = df["date"].values
    navs = df["nav"].values

    # Convert dates to python date objects for diff calculations
    py_dates = pd.to_datetime(dates).date

    console.print(Panel(
        f"[bold blue]Fund:[/bold blue] {scheme_name}\n"
        f"[bold blue]Scheme Code:[/bold blue] {args.scheme}\n"
        f"[bold blue]Data Points:[/bold blue] {len(df)} days ({py_dates[0]} to {py_dates[-1]})",
        title="📈 Rolling Return Distribution Analysis",
        border_style="cyan",
        expand=False
    ))

    table = Table(box=box.ROUNDED, show_header=True, header_style="bold magenta")
    table.add_column("Window")
    table.add_column("# Obs", justify="right")
    table.add_column("Min", justify="right")
    table.add_column("10th %ile", justify="right")
    table.add_column("25th %ile", justify="right")
    table.add_column("Median", justify="right")
    table.add_column("Mean", justify="right")
    table.add_column("75th %ile", justify="right")
    table.add_column("90th %ile", justify="right")
    table.add_column("Max", justify="right")
    table.add_column("Std Dev", justify="right")
    table.add_column("% Neg", justify="right")
    table.add_column("% > 10%", justify="right")
    table.add_column("% > 15%", justify="right")

    plot_data = None
    plot_label = None

    for w in requested:
        if w not in windows_map:
            continue

        cagr = calculate_rolling_returns(py_dates, navs, windows_map[w])

        if len(cagr) == 0:
            table.add_row(w, "0", *["—"] * 12)
            continue

        if plot_data is None:
            plot_data = cagr
            plot_label = w

        n = len(cagr)
        pct_neg = float(np.mean(cagr < 0) * 100)
        pct_10 = float(np.mean(cagr > 10) * 100)
        pct_15 = float(np.mean(cagr > 15) * 100)

        table.add_row(
            w,
            f"{n:,}",
            f"{np.min(cagr):.1f}%",
            f"{np.percentile(cagr, 10):.1f}%",
            f"{np.percentile(cagr, 25):.1f}%",
            f"{np.median(cagr):.1f}%",
            f"{np.mean(cagr):.1f}%",
            f"{np.percentile(cagr, 75):.1f}%",
            f"{np.percentile(cagr, 90):.1f}%",
            f"{np.max(cagr):.1f}%",
            f"{np.std(cagr):.1f}%",
            f"[{'red' if pct_neg > 10 else 'green'}]{pct_neg:.1f}%[/]",
            f"{pct_10:.1f}%",
            f"{pct_15:.1f}%",
        )

    console.print(table)

    # ASCII histogram
    if plot_data is not None:
        try:
            import plotext as plt
            plt.clear_figure()
            plt.hist(plot_data.tolist(), bins=30)
            plt.title(f"{plot_label} Rolling CAGR Distribution (%) — {scheme_name}")
            plt.plotsize(80, 18)
            plt.show()
        except (ImportError, Exception):
            pass


if __name__ == "__main__":
    main()
