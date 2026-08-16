"""
src/scripts/portfolio/sip_backtester.py
───────────────────────────────────────
SIP Backtester with XIRR for any Indian Mutual Fund.

Simulates historical SIP performance, computing XIRR, total invested vs
current value, max drawdown, recovery time, and year-by-year breakdown.

Usage:
  python src/scripts/portfolio/sip_backtester.py --scheme 119212 --monthly 10000 --start 2020-01
  python src/scripts/portfolio/sip_backtester.py --scheme 152056 --monthly 25000 --start 2022-01 --stepup 10
"""

import argparse
import os
import sys
from datetime import datetime, date, timedelta

import numpy as np
import pandas as pd
import requests
from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

MFAPI_BASE = "https://api.mfapi.in/mf"


def fetch_nav_history(scheme_code: str) -> tuple[str, pd.DataFrame]:
    """Fetch full NAV history from mfapi.in."""
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
                d = datetime.strptime(entry["date"], "%d-%m-%Y").date()
                nav = float(entry["nav"])
                rows.append({"date": d, "nav": nav})
            except (ValueError, KeyError):
                continue

        df = pd.DataFrame(rows).sort_values("date").reset_index(drop=True)
        return scheme_name, df

    except Exception:
        return f"Scheme {scheme_code}", pd.DataFrame()


def xirr(cashflows: list[tuple[date, float]], guess: float = 0.1) -> float:
    """Compute XIRR using bisection method."""
    if not cashflows:
        return 0.0

    d0 = cashflows[0][0]

    def npv(rate):
        if rate <= -1:
            return float("inf")
        return sum(cf / (1 + rate) ** ((d - d0).days / 365.0) for d, cf in cashflows)

    lo, hi = -0.5, 5.0
    # Check boundaries
    if npv(lo) * npv(hi) > 0:
        return 0.0

    for _ in range(300):
        mid = (lo + hi) / 2
        if npv(mid) > 0:
            lo = mid
        else:
            hi = mid
        if abs(hi - lo) < 1e-9:
            break

    return (lo + hi) / 2


def main():
    parser = argparse.ArgumentParser(description="SIP Backtester with XIRR")
    parser.add_argument("--scheme", type=str, required=True, help="AMFI scheme code")
    parser.add_argument("--monthly", type=float, default=10000, help="Monthly SIP amount (default: 10000)")
    parser.add_argument("--start", type=str, default="2020-01", help="Start month YYYY-MM (default: 2020-01)")
    parser.add_argument("--stepup", type=float, default=0, help="Annual step-up percentage (default: 0)")
    args = parser.parse_args()

    console = Console()

    start_date = datetime.strptime(args.start + "-01", "%Y-%m-%d").date()

    with console.status("[bold green]Fetching NAV history from mfapi.in..."):
        scheme_name, df = fetch_nav_history(args.scheme)

    if df.empty:
        console.print(f"[bold red]No NAV data for scheme {args.scheme}[/bold red]")
        return

    # Filter to start date
    df = df[df["date"] >= start_date].reset_index(drop=True)
    if df.empty:
        console.print(f"[bold red]No NAV data after {start_date}[/bold red]")
        return

    nav_dates = df["date"].values
    nav_values = df["nav"].values
    py_dates = [pd.Timestamp(d).date() for d in nav_dates]

    # Build date→nav lookup
    nav_lookup = dict(zip(py_dates, nav_values))

    # Simulate SIP
    cumulative_units = 0.0
    cumulative_invested = 0.0
    cashflows: list[tuple[date, float]] = []
    sip_records = []  # (date, amount, nav, units, cum_units, cum_invested, value)
    peak_value = 0.0
    max_drawdown = 0.0
    max_dd_date = start_date
    underwater_start = None
    longest_underwater = 0
    current_underwater = 0

    monthly_amount = args.monthly
    months_since_stepup = 0

    # Iterate month by month
    current_month = start_date
    last_nav_date = py_dates[-1]

    while current_month <= last_nav_date:
        # Find first available NAV on or after the 1st of this month
        sip_date = None
        sip_nav = None
        for offset in range(0, 15):  # check up to 15 days into the month
            check_date = current_month + timedelta(days=offset)
            if check_date in nav_lookup:
                sip_date = check_date
                sip_nav = nav_lookup[check_date]
                break

        if sip_date and sip_nav and sip_nav > 0:
            units = monthly_amount / sip_nav
            cumulative_units += units
            cumulative_invested += monthly_amount
            cashflows.append((sip_date, -monthly_amount))

            current_value = cumulative_units * sip_nav

            # Track drawdown
            if current_value > peak_value:
                peak_value = current_value
                current_underwater = 0
            else:
                current_underwater += 1
                dd = (peak_value - current_value) / peak_value if peak_value > 0 else 0
                if dd > max_drawdown:
                    max_drawdown = dd
                    max_dd_date = sip_date
                longest_underwater = max(longest_underwater, current_underwater)

            sip_records.append({
                "date": sip_date,
                "amount": monthly_amount,
                "nav": sip_nav,
                "units": units,
                "cum_units": cumulative_units,
                "cum_invested": cumulative_invested,
                "value": current_value,
            })

        # Step-up: increase SIP amount annually
        months_since_stepup += 1
        if args.stepup > 0 and months_since_stepup >= 12:
            monthly_amount *= (1 + args.stepup / 100)
            months_since_stepup = 0

        # Next month
        if current_month.month == 12:
            current_month = date(current_month.year + 1, 1, 1)
        else:
            current_month = date(current_month.year, current_month.month + 1, 1)

    if not sip_records:
        console.print("[red]No SIP transactions could be executed.[/red]")
        return

    # Final valuation
    latest_nav = nav_values[-1]
    latest_date = py_dates[-1]
    final_value = cumulative_units * latest_nav
    cashflows.append((latest_date, final_value))

    # Compute XIRR
    xirr_rate = xirr(cashflows) * 100
    abs_return = ((final_value - cumulative_invested) / cumulative_invested) * 100 if cumulative_invested > 0 else 0
    tenure_months = len(sip_records)

    # Header
    console.print(Panel(
        f"[bold blue]Fund:[/bold blue] {scheme_name}\n"
        f"[bold blue]Scheme:[/bold blue] {args.scheme}\n"
        f"[bold blue]SIP:[/bold blue] ₹{args.monthly:,.0f}/month"
        + (f" (step-up {args.stepup}% p.a.)" if args.stepup > 0 else "")
        + f" from {start_date}",
        title="💰 SIP Backtester",
        border_style="cyan",
        expand=False,
    ))

    # Summary table
    t1 = Table(title="SIP Performance Summary", box=box.ROUNDED)
    t1.add_column("Metric", style="bold cyan")
    t1.add_column("Value", justify="right")
    t1.add_row("Total Invested", f"₹{cumulative_invested:,.0f}")
    t1.add_row("Current Value", f"[bold green]₹{final_value:,.0f}[/bold green]")
    t1.add_row("Absolute Return", f"{abs_return:+.1f}%")
    t1.add_row("XIRR", f"[bold {'green' if xirr_rate > 0 else 'red'}]{xirr_rate:.1f}%[/]")
    t1.add_row("Total Units", f"{cumulative_units:,.4f}")
    t1.add_row("Latest NAV", f"₹{latest_nav:.4f}")
    t1.add_row("SIP Tenure", f"{tenure_months} months")
    console.print(t1)

    # Risk metrics
    t2 = Table(title="Risk Metrics", box=box.ROUNDED)
    t2.add_column("Metric", style="bold cyan")
    t2.add_column("Value", justify="right")
    t2.add_row("Max Drawdown", f"[bold red]{max_drawdown * 100:.1f}%[/bold red]")
    t2.add_row("Max DD Date", str(max_dd_date))
    t2.add_row("Longest Underwater", f"{longest_underwater} months")
    console.print(t2)

    # Year-by-year breakdown
    if len(sip_records) > 12:
        t3 = Table(title="Year-by-Year Breakdown", box=box.ROUNDED)
        t3.add_column("Year", style="bold")
        t3.add_column("Invested", justify="right")
        t3.add_column("Value at Year-End", justify="right")
        t3.add_column("YoY Return", justify="right")

        df_sip = pd.DataFrame(sip_records)
        df_sip["year"] = pd.to_datetime(df_sip["date"]).dt.year

        prev_year_value = 0.0
        for yr, grp in df_sip.groupby("year"):
            year_invested = grp["amount"].sum()
            year_end_value = grp.iloc[-1]["value"]
            if prev_year_value > 0:
                yoy = ((year_end_value - prev_year_value - year_invested) / prev_year_value) * 100
            else:
                yoy = ((year_end_value - year_invested) / year_invested) * 100 if year_invested > 0 else 0

            style = "green" if yoy > 0 else "red"
            t3.add_row(
                str(yr),
                f"₹{year_invested:,.0f}",
                f"₹{year_end_value:,.0f}",
                f"[{style}]{yoy:+.1f}%[/{style}]",
            )
            prev_year_value = year_end_value

        console.print(t3)

    # Step-up comparison
    if args.stepup > 0:
        # Re-run without step-up for comparison
        no_stepup_units = 0.0
        no_stepup_invested = 0.0
        base_amount = args.monthly
        current_month = start_date
        while current_month <= last_nav_date:
            for offset in range(0, 15):
                check_date = current_month + timedelta(days=offset)
                if check_date in nav_lookup:
                    nav = nav_lookup[check_date]
                    if nav > 0:
                        no_stepup_units += base_amount / nav
                        no_stepup_invested += base_amount
                    break
            if current_month.month == 12:
                current_month = date(current_month.year + 1, 1, 1)
            else:
                current_month = date(current_month.year, current_month.month + 1, 1)

        no_stepup_value = no_stepup_units * latest_nav
        extra_gain = final_value - no_stepup_value

        console.print(Panel(
            f"Without Step-Up: ₹{no_stepup_value:,.0f} (invested ₹{no_stepup_invested:,.0f})\n"
            f"With {args.stepup}% Step-Up: ₹{final_value:,.0f} (invested ₹{cumulative_invested:,.0f})\n"
            f"[bold green]Extra Gain from Step-Up: ₹{extra_gain:,.0f}[/bold green]",
            title="Step-Up SIP Comparison",
            border_style="green",
        ))


if __name__ == "__main__":
    main()
