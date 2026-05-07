"""
scripts/portfolio/fund_mom_returns.py
──────────────────────────────────────
Month-over-Month NAV return analysis for any Indian mutual fund.

Usage:
    python src/scripts/portfolio/fund_mom_returns.py --scheme 152056
    python src/scripts/portfolio/fund_mom_returns.py --scheme 152056 --months 24
    python src/scripts/portfolio/fund_mom_returns.py --search "DSP Multi Asset"
"""

import sys
import os
import argparse
import requests
import pandas as pd
from datetime import datetime, timedelta

from rich.console import Console
from rich.table import Table
from rich.panel import Panel

console = Console()

MFAPI_BASE = "https://api.mfapi.in/mf"


def search_scheme(query: str) -> list[dict]:
    """Search for a mutual fund scheme by name."""
    try:
        resp = requests.get(f"{MFAPI_BASE}", timeout=10)
        if resp.status_code != 200:
            console.print(f"[red]Search API error: {resp.status_code}[/red]")
            return []
        all_schemes = resp.json()
        q = query.lower()
        return [s for s in all_schemes if q in s.get("schemeName", "").lower()]
    except Exception as e:
        console.print(f"[red]Search failed: {e}[/red]")
        return []


def fetch_nav_history(scheme_code: str, months: int = 12) -> tuple[str, pd.DataFrame]:
    """Fetch NAV history for a scheme code. Returns (scheme_name, df)."""
    try:
        resp = requests.get(f"{MFAPI_BASE}/{scheme_code}", timeout=15)
        if resp.status_code != 200:
            console.print(f"[red]API error {resp.status_code} for scheme {scheme_code}[/red]")
            return "", pd.DataFrame()

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

        cutoff = datetime.now().date() - timedelta(days=months * 31)
        df = df[df["date"] >= cutoff]

        return scheme_name, df

    except Exception as e:
        console.print(f"[red]Fetch failed: {e}[/red]")
        return "", pd.DataFrame()


def compute_monthly_returns(df: pd.DataFrame) -> pd.DataFrame:
    """Collapse daily NAV to month-end and compute MoM returns."""
    df["month"] = pd.to_datetime(df["date"]).dt.to_period("M")
    monthly = (
        df.groupby("month")
        .agg(nav=("nav", "last"))
        .reset_index()
    )
    monthly["prev_nav"] = monthly["nav"].shift(1)
    monthly["mom_pct"] = (
        (monthly["nav"] - monthly["prev_nav"]) / monthly["prev_nav"] * 100
    ).round(2)
    return monthly


def print_returns(scheme_name: str, monthly: pd.DataFrame, months: int) -> None:
    table = Table(title=f"{scheme_name}\nMonth-over-Month Returns (last {months}m)", show_lines=False)
    table.add_column("Month", style="dim", min_width=10)
    table.add_column("NAV (₹)", justify="right")
    table.add_column("MoM Return", justify="right", min_width=12)

    for _, row in monthly.iterrows():
        nav_str = f"₹{row['nav']:.2f}"
        if pd.isna(row["mom_pct"]):
            table.add_row(str(row["month"]), nav_str, "—")
        else:
            pct = row["mom_pct"]
            mom_str = f"{pct:+.2f}%"
            style = "green" if pct >= 0 else "red"
            table.add_row(str(row["month"]), nav_str, f"[{style}]{mom_str}[/{style}]")

    console.print(table)

    # Summary panel
    valid = monthly["mom_pct"].dropna()
    first_nav = monthly["nav"].iloc[0]
    last_nav = monthly["nav"].iloc[-1]
    total_return = (last_nav - first_nav) / first_nav * 100

    summary = (
        f"Period return : [bold]{total_return:+.2f}%[/bold]\n"
        f"Latest month  : [bold]{valid.iloc[-1]:+.2f}%[/bold]\n"
        f"Average MoM   : [bold]{valid.mean():+.2f}%[/bold]\n"
        f"Best month    : [green]{valid.max():+.2f}%[/green]  ({monthly.loc[valid.idxmax(), 'month']})\n"
        f"Worst month   : [red]{valid.min():+.2f}%[/red]  ({monthly.loc[valid.idxmin(), 'month']})\n"
        f"Positive months: [green]{(valid > 0).sum()}[/green] / {len(valid)}"
    )
    console.print(Panel(summary, title="Summary", border_style="blue"))


def main():
    parser = argparse.ArgumentParser(description="MF month-over-month return analyser")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--scheme", metavar="CODE", help="MFAPI scheme code (e.g. 152056)")
    group.add_argument("--search", metavar="NAME", help="Search by fund name and pick from results")
    parser.add_argument("--months", type=int, default=12, help="Lookback in months (default: 12)")
    args = parser.parse_args()

    scheme_code = args.scheme

    if args.search:
        results = search_scheme(args.search)
        if not results:
            console.print(f"[red]No schemes found matching '{args.search}'[/red]")
            sys.exit(1)
        console.print(f"\nFound [bold]{len(results)}[/bold] matching scheme(s):\n")
        for i, s in enumerate(results[:20]):
            console.print(f"  [{i}] {s['schemeCode']:>8}  {s['schemeName']}")
        if len(results) == 1:
            scheme_code = str(results[0]["schemeCode"])
        else:
            choice = console.input("\nEnter index to select: ").strip()
            scheme_code = str(results[int(choice)]["schemeCode"])

    console.print(f"\nFetching NAV data for scheme [bold]{scheme_code}[/bold]…")
    scheme_name, df = fetch_nav_history(scheme_code, args.months)

    if df.empty:
        console.print("[red]No NAV data returned.[/red]")
        sys.exit(1)

    monthly = compute_monthly_returns(df)
    print_returns(scheme_name, monthly, args.months)


if __name__ == "__main__":
    main()
