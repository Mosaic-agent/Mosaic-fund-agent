"""
src/scripts/portfolio/sif_performance_report.py
─────────────────────────────────────────────────
Portfolio report for all Specialized Investment Fund (SIF) schemes present in
market_data.mf_holdings — not hardcoded to any one AMC. A scheme is picked up
as "SIF" if its fund_name contains the word SIF, "Long Short"/"Long-Short"
(the SEBI SIF strategy hallmark), or "Specialized Investment Fund".

SIF schemes are not on AMFI/mfapi.in, so there is no NAV feed and therefore
no return figure to compute — this reports AUM growth and portfolio
composition from statutory holdings disclosures, not returns.

Usage:
    python src/scripts/portfolio/sif_performance_report.py
    python src/scripts/portfolio/sif_performance_report.py --top 10
    python src/scripts/portfolio/sif_performance_report.py --save
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime

sys.path.append(os.getcwd())

from rich import box
from rich.console import Console
from rich.table import Table

SIF_NAME_PATTERN = r"(?i)\bSIF\b|LONG[\s_-]?SHORT|SPECIALIZED INVESTMENT"

console = Console()


def find_sif_funds(client) -> list[tuple[str, str]]:
    df = client.query_df(
        f"""
        SELECT DISTINCT scheme_code, fund_name
        FROM market_data.mf_holdings FINAL
        WHERE match(fund_name, '{SIF_NAME_PATTERN}')
        ORDER BY scheme_code
        """
    )
    return list(df.itertuples(index=False, name=None))


def aum_by_month(client, scheme_codes: list[str]):
    codes = ",".join(f"'{c}'" for c in scheme_codes)
    return client.query_df(
        f"""
        SELECT scheme_code, as_of_month,
               sum(market_value_cr) AS total_aum_cr,
               sumIf(market_value_cr, asset_type = 'equity') AS equity_aum_cr,
               sumIf(market_value_cr, asset_type = 'bond') AS bond_aum_cr,
               sumIf(market_value_cr, asset_type = 'other') AS other_aum_cr,
               sum(pct_of_nav) AS pct_nav_captured
        FROM market_data.mf_holdings FINAL
        WHERE scheme_code IN ({codes})
        GROUP BY scheme_code, as_of_month
        ORDER BY scheme_code, as_of_month
        """
    )


def top_holdings_latest_month(client, scheme_codes: list[str], top_n: int):
    codes = ",".join(f"'{c}'" for c in scheme_codes)
    return client.query_df(
        f"""
        WITH latest AS (
            SELECT scheme_code, max(as_of_month) AS latest_month
            FROM market_data.mf_holdings FINAL
            WHERE scheme_code IN ({codes})
            GROUP BY scheme_code
        ),
        ranked AS (
            SELECT h.scheme_code, h.security_name, h.pct_of_nav, h.market_value_cr,
                   row_number() OVER (PARTITION BY h.scheme_code ORDER BY h.pct_of_nav DESC) AS rn
            FROM market_data.mf_holdings AS h FINAL
            INNER JOIN latest l ON h.scheme_code = l.scheme_code AND h.as_of_month = l.latest_month
            WHERE h.asset_type = 'equity'
        )
        SELECT scheme_code, security_name, round(pct_of_nav, 2) AS pct_of_nav,
               round(market_value_cr, 2) AS market_value_cr
        FROM ranked
        WHERE rn <= {top_n}
        ORDER BY scheme_code, pct_of_nav DESC
        """
    )


def run(top_n: int = 5, save: bool = False) -> None:
    from src.db.pool import get_client

    client = get_client()

    sif_funds = find_sif_funds(client)
    if not sif_funds:
        console.print("[yellow]No SIF-pattern funds found in market_data.mf_holdings.[/yellow]")
        return

    scheme_codes = [c for c, _ in sif_funds]
    name_by_code = dict(sif_funds)

    console.print(
        "[dim]No NAV feed exists for SIF schemes (not on AMFI/mfapi.in) — "
        "this is an AUM/holdings-composition report, not a returns report. "
        "\"% NAV Captured\" below is the fund's own disclosed holdings summed "
        "as a % of NAV — it can land under 100% (rounding, undisclosed "
        "residuals) or over 100% (long-short funds can carry gross long+short "
        "exposure above NAV even when net exposure is lower), so it reflects "
        "the source disclosure, not a parsing gap.[/dim]\n"
    )

    report: dict = {"generated_at": datetime.now().isoformat(), "funds": {}}

    # --- AUM growth summary ---
    aum_df = aum_by_month(client, scheme_codes)
    growth_table = Table(title="SIF AUM Growth", box=box.ROUNDED, header_style="bold cyan")
    for col in ["Fund", "Months", "First AUM (Cr)", "Latest AUM (Cr)", "AUM Growth"]:
        growth_table.add_column(col)

    trend_tables: list[Table] = []

    for code, grp in aum_df.groupby("scheme_code"):
        grp = grp.sort_values("as_of_month")
        first_aum = grp["total_aum_cr"].iloc[0]
        last_aum = grp["total_aum_cr"].iloc[-1]
        n_months = len(grp)
        growth_str = "n/a (1 disclosure)"
        if n_months > 1 and first_aum:
            growth_pct = (last_aum - first_aum) / first_aum * 100
            growth_str = f"{growth_pct:+.1f}%"
        growth_table.add_row(
            name_by_code[code],
            str(n_months),
            f"₹{first_aum:.1f}",
            f"₹{last_aum:.1f}",
            growth_str,
        )

        # --- Month-over-month trend (only worth showing for 2+ months) ---
        months_payload = []
        if n_months > 1:
            t_table = Table(
                title=f"{name_by_code[code]} — Month-over-Month AUM",
                box=box.ROUNDED,
                header_style="bold cyan",
            )
            for col in ["Month", "AUM (Cr)", "MoM Change", "% NAV Captured"]:
                t_table.add_column(col)
            prev_aum = None
            for _, row in grp.iterrows():
                aum = row["total_aum_cr"]
                mom = "—" if prev_aum is None or not prev_aum else f"{(aum - prev_aum) / prev_aum * 100:+.1f}%"
                t_table.add_row(
                    str(row["as_of_month"]),
                    f"₹{aum:.1f}",
                    mom,
                    f"{row['pct_nav_captured']:.1f}%",
                )
                months_payload.append({
                    "as_of_month": str(row["as_of_month"]),
                    "total_aum_cr": round(float(aum), 2),
                    "pct_nav_captured": round(float(row["pct_nav_captured"]), 2),
                })
                prev_aum = aum
            trend_tables.append(t_table)
        else:
            months_payload = [{
                "as_of_month": str(grp["as_of_month"].iloc[0]),
                "total_aum_cr": round(float(first_aum), 2),
                "pct_nav_captured": round(float(grp["pct_nav_captured"].iloc[0]), 2),
            }]

        report["funds"][code] = {
            "fund_name": name_by_code[code],
            "months": months_payload,
            "aum_growth_pct": None if growth_str.startswith("n/a") else round((last_aum - first_aum) / first_aum * 100, 2),
        }

    console.print(growth_table)
    for t in trend_tables:
        console.print(t)

    # --- Top holdings, latest disclosed month per fund ---
    holdings_df = top_holdings_latest_month(client, scheme_codes, top_n)
    for code, grp in holdings_df.groupby("scheme_code"):
        h_table = Table(
            title=f"{name_by_code[code]} — Top {top_n} Equity Holdings (latest disclosure)",
            box=box.ROUNDED,
            header_style="bold cyan",
        )
        for col in ["Security", "% NAV", "Value (Cr)"]:
            h_table.add_column(col)
        top_holdings_payload = []
        for _, row in grp.iterrows():
            h_table.add_row(row["security_name"], f"{row['pct_of_nav']:.2f}%", f"₹{row['market_value_cr']:.2f}")
            top_holdings_payload.append({
                "security_name": row["security_name"],
                "pct_of_nav": float(row["pct_of_nav"]),
                "market_value_cr": float(row["market_value_cr"]),
            })
        console.print(h_table)
        report["funds"].setdefault(code, {})["top_holdings"] = top_holdings_payload

    # --- Cross-fund conviction overlap ---
    overlap = (
        holdings_df.groupby("security_name")["scheme_code"]
        .nunique()
        .reset_index(name="fund_count")
        .query("fund_count > 1")
        .sort_values("fund_count", ascending=False)
    )
    overlap_payload = []
    if not overlap.empty:
        console.print("\n[bold]Cross-fund conviction (appears in top holdings of multiple SIF funds):[/bold]")
        for _, row in overlap.iterrows():
            console.print(f"  • {row['security_name']} — {row['fund_count']} funds")
            overlap_payload.append({"security_name": row["security_name"], "fund_count": int(row["fund_count"])})
    report["cross_fund_conviction"] = overlap_payload

    if save:
        out_dir = os.path.join("output")
        os.makedirs(out_dir, exist_ok=True)
        fname = f"sif_performance_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        out_path = os.path.join(out_dir, fname)
        with open(out_path, "w") as f:
            json.dump(report, f, indent=2, default=str)
        console.print(f"\n[dim]Saved to {out_path}[/dim]")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SIF (Specialized Investment Fund) AUM & holdings report")
    parser.add_argument("--top", type=int, default=5, help="Number of top equity holdings per fund (default: 5)")
    parser.add_argument("--save", action="store_true", help="Save report as JSON under output/")
    args = parser.parse_args()
    run(top_n=args.top, save=args.save)
