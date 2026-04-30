"""
scripts/import_all_icici.py
───────────────────────────
Import ICICI Prudential AMC fund holdings into market_data.mf_holdings
using the Morningstar sal-service API.

IMPORTANT — snapshot limitation
────────────────────────────────
The Morningstar API always returns the CURRENT live portfolio snapshot.
Unlike the DSP importer (which downloads monthly ZIP archives), there is
no way to retrieve historical monthly slices from this API.
Run this script once a month to build a forward-going time-series.

Usage
─────
    python scripts/import_all_icici.py              # import current snapshot
    python scripts/import_all_icici.py --dry-run    # parse & print, no DB insert
    python scripts/import_all_icici.py --test       # fetch one fund only, no DB insert

Run with PYTHONPATH set to the project root:
    PYTHONPATH=/path/to/ofin-agent python scripts/import_all_icici.py
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from datetime import datetime, date

import clickhouse_connect
import httpx
from rich.console import Console
from rich.progress import Progress

sys.path.append(os.getcwd())
from config.settings import settings

console = Console()
logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
logger = logging.getLogger(__name__)

# ── Morningstar API constants ─────────────────────────────────────────────────

_SAL_BASE = "https://api-global.morningstar.com/sal-service/v1"

# Public API key embedded in mstarpy and Morningstar's own JS bundles.
_API_KEY = "lstzFDEOhfFNMLikKa0am9mgEKLBl49T"

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "apikey": _API_KEY,
}

_PARAMS = {
    "clientId": "MDC",
    "version": "4.71.0",
    "premiumNum": "10000",
    "freeNum": "10000",
}

_REQUEST_DELAY: float = 1.5

# ── ICICI Prudential fund catalogue ──────────────────────────────────────────
# (amfi_scheme_code, fund_name, isin, morningstar_sec_id)
#
# Morningstar secIDs verified against api-global.morningstar.com/sal-service/v1/
# fund/portfolio/holding/v2/{secID}/data (all returning live holdings as of
# April 2026 investigation).
#
# AMFI scheme codes sourced from mfapi.in — update if AMFI renumbers a fund.

ICICI_FUNDS: list[tuple[str, str, str, str]] = [
    # scheme_code  fund_name                    isin             sec_id
    ("120716", "ICICI_MULTI_ASSET",        "INF109K015K4",  "F00000PE3K"),
    ("120586", "ICICI_BLUECHIP",           "INF109K01ZW2",  "F00000N9YF"),
    ("120505", "ICICI_VALUE_DISCOVERY",    "INF109K01XV0",  "F0000029OM"),
    ("120251", "ICICI_BAF",                "INF109K01ZN1",  "F00000N9YD"),
    ("120379", "ICICI_MIDCAP",             "INF109K01373",  "F0000029ON"),
    ("120828", "ICICI_SMALLCAP",           "INF109K01ZX0",  "F00000N9YG"),
    ("120593", "ICICI_TECH",               "INF109K01ZV4",  "F00000N9YE"),
    ("120380", "ICICI_INFRA",              "INF109K01375",  "F0000029OP"),
    ("148571", "ICICI_NIFTY50_INDEX",      "INF109KA1FH5",  "F00001485N"),
    ("120397", "ICICI_FMCG",              "INF109K01ZU6",  "F00000N9YC"),
    ("148570", "ICICI_COMMODITIES",        "INF109KA13N5",  "F00001485M"),
]

# ── Asset classification ──────────────────────────────────────────────────────

_GOLD_KW  = ("gold", "silver", "precious metal", "commodity etf")
_BOND_KW  = ("bond", "debt", "fixed income", "debenture", "ncd",
              "government", "gilt", "treasury", "paper", "deposit")
_CASH_KW  = ("cash", "money market", "liquid", "overnight", "repo", "treps")
_EQUITY_KW = ("stock", "equity", "share", "common", "preferred")


def _classify(type_id: str, security_name: str) -> str:
    combined = f"{type_id} {security_name}".lower()
    if any(k in combined for k in _GOLD_KW):
        return "gold"
    if any(k in combined for k in _BOND_KW):
        return "bond"
    if any(k in combined for k in _CASH_KW):
        return "cash"
    if any(k in combined for k in _EQUITY_KW):
        return "equity"
    tid = str(type_id).lower()
    if tid in ("stock", "equity", "e"):
        return "equity"
    if tid in ("bond", "fixed income", "fi", "b"):
        return "bond"
    if tid in ("cash", "c"):
        return "cash"
    return "other"


# ── Per-fund fetch ────────────────────────────────────────────────────────────

def _fetch_one_fund(
    scheme_code: str,
    fund_name: str,
    isin: str,
    sec_id: str,
    as_of_month: date,
) -> list[dict]:
    url = f"{_SAL_BASE}/fund/portfolio/holding/v2/{sec_id}/data"
    try:
        with httpx.Client(timeout=30, follow_redirects=True) as client:
            resp = client.get(url, headers=_HEADERS, params=_PARAMS)
            resp.raise_for_status()
            data = resp.json()
    except httpx.HTTPStatusError as exc:
        logger.warning(
            "Morningstar %d for %s (%s): %s",
            exc.response.status_code, fund_name, isin, exc,
        )
        return []
    except Exception as exc:
        logger.warning("Failed to fetch %s (%s): %s", fund_name, isin, exc)
        return []

    rows: list[dict] = []
    imported_at = datetime.now()

    for page_key in ("equityHoldingPage", "boldHoldingPage", "otherHoldingPage"):
        page_data = data.get(page_key)
        if not page_data:
            continue
        for h in page_data.get("holdingList", []):
            security_name: str = str(h.get("securityName") or "Unknown")

            try:
                pct_of_nav = float(h.get("weighting") or 0.0)
            except (TypeError, ValueError):
                pct_of_nav = 0.0

            type_id: str = str(h.get("holdingTypeId") or h.get("holdingType") or "")
            asset_type = _classify(type_id, security_name)

            holding_isin: str = str(h.get("isin") or h.get("secId") or "")

            try:
                mv_raw = float(h.get("marketValue") or 0.0)
                market_value_cr = round(mv_raw / 1e7, 4)
            except (TypeError, ValueError):
                market_value_cr = 0.0

            rows.append({
                "scheme_code":     scheme_code,
                "fund_name":       fund_name,
                "as_of_month":     as_of_month,
                "isin":            holding_isin or security_name[:20],
                "security_name":   security_name,
                "asset_type":      asset_type,
                "market_value_cr": market_value_cr,
                "pct_of_nav":      pct_of_nav,
                "imported_at":     imported_at,
            })

    pct_sum = sum(r["pct_of_nav"] for r in rows)
    color = "yellow" if pct_sum > 100 else "green"
    console.print(
        f"  [{color}]→ {fund_name}: {len(rows)} holdings, pct_sum={pct_sum:.1f}% "
        f"(month={as_of_month})[/{color}]"
    )
    return rows


# ── Main import ───────────────────────────────────────────────────────────────

def run_import(funds: list[tuple] | None = None, dry_run: bool = False) -> None:
    targets = funds or ICICI_FUNDS
    as_of_month = date.today().replace(day=1)

    console.print(
        f"[bold cyan]ICICI AMC import — {len(targets)} funds — snapshot date: "
        f"{as_of_month}[/bold cyan]"
    )

    client = None
    if not dry_run:
        client = clickhouse_connect.get_client(
            host=settings.clickhouse_host,
            port=settings.clickhouse_port,
            username=settings.clickhouse_user,
            password=settings.clickhouse_password,
            database=settings.clickhouse_database,
        )

    all_rows: list[dict] = []

    with Progress() as progress:
        task = progress.add_task("[cyan]Fetching ICICI holdings...", total=len(targets))
        for i, (scheme_code, fund_name, isin, sec_id) in enumerate(targets):
            progress.console.print(f"Fetching [bold]{fund_name}[/bold]...")
            rows = _fetch_one_fund(scheme_code, fund_name, isin, sec_id, as_of_month)
            all_rows.extend(rows)
            if i < len(targets) - 1:
                time.sleep(_REQUEST_DELAY)
            progress.advance(task)

    if dry_run:
        console.print(
            f"[bold blue]DRY RUN: {len(all_rows)} holdings parsed across "
            f"{len(targets)} funds — nothing inserted.[/bold blue]"
        )
        return

    if not all_rows:
        console.print("[red]✗ No holdings fetched.[/red]")
        if client:
            client.close()
        return

    insert_rows = [
        (
            r["scheme_code"], r["fund_name"],
            r["as_of_month"],
            r["isin"], r["security_name"], r["asset_type"],
            r["market_value_cr"], r["pct_of_nav"], r["imported_at"],
        )
        for r in all_rows
    ]

    client.insert(
        "market_data.mf_holdings",
        insert_rows,
        column_names=[
            "scheme_code", "fund_name", "as_of_month",
            "isin", "security_name", "asset_type",
            "market_value_cr", "pct_of_nav", "imported_at",
        ],
    )
    console.print(
        f"[bold green]✓ Inserted {len(insert_rows)} holdings for "
        f"{len(targets)} ICICI funds (month={as_of_month}).[/bold green]"
    )

    # Watermark: record the import date per fund so the CLI can skip re-fetching
    watermark_rows = [
        ["mf_holdings", fund_name, as_of_month]
        for _, fund_name, _, _ in targets
    ]
    client.insert(
        "market_data.import_watermarks",
        watermark_rows,
        column_names=["source", "symbol", "last_date"],
    )
    console.print(f"[dim]Watermark set for {len(watermark_rows)} funds.[/dim]")

    client.close()


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Import ICICI Prudential AMC fund holdings via Morningstar API"
    )
    parser.add_argument(
        "--test", action="store_true",
        help="Fetch only the first fund (ICICI_MULTI_ASSET), no DB insert",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Fetch all funds but skip DB insert",
    )
    args = parser.parse_args()

    if args.test:
        run_import(funds=ICICI_FUNDS[:1], dry_run=True)
    elif args.dry_run:
        run_import(dry_run=True)
    else:
        run_import()
