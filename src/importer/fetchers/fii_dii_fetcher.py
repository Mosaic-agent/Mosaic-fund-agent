"""
src/importer/fetchers/fii_dii_fetcher.py
─────────────────────────────────────────
Fetches daily FII and DII cash-market flow data from Sensibull's data backend.

Data source
───────────
  Sensibull oxide API — monthly cache endpoint:
    GET https://oxide.sensibull.com/v1/compute/cache/fii_dii_daily
        ?year_month=<YYYY-MonthName>   (optional; omit for current month)

  The response contains per-date cash-segment buy/sell/net figures for
  both FII and DII, pre-structured and clean.

  Available history: ~6 months of monthly buckets (rolling window).

Public API
──────────
    fetch_fii_dii(from_date: date | None = None) -> list[dict]

    Returns list[dict] with keys:
        trade_date        : date
        fii_gross_buy_cr  : float  (Rs Crore)
        fii_gross_sell_cr : float  (Rs Crore)
        fii_net_cr        : float  (Rs Crore)
        dii_gross_buy_cr  : float  (Rs Crore)
        dii_gross_sell_cr : float  (Rs Crore)
        dii_net_cr        : float  (Rs Crore)

    Rows are sorted by trade_date ascending.
    Empty list returned on any unrecoverable fetch error.
"""

from __future__ import annotations

import logging
import time
from calendar import month_name as _MONTH_NAMES
from datetime import date

import httpx

# Map month name → ordinal for comparison (January=1 … December=12)
_MONTH_ORD: dict[str, int] = {name: i for i, name in enumerate(_MONTH_NAMES) if name}


def _key_to_ord(key: str) -> tuple[int, int]:
    """Convert '2026-March' to (2026, 3) for chronological sorting/comparison."""
    try:
        year_str, mon_str = key.split("-", 1)
        return int(year_str), _MONTH_ORD.get(mon_str, 0)
    except ValueError:
        return (0, 0)

logger = logging.getLogger(__name__)

_SENSIBULL_DAILY   = "https://oxide.sensibull.com/v1/compute/cache/fii_dii_daily"
_SENSIBULL_MONTHLY = "https://oxide.sensibull.com/v1/compute/cache/fii_dii_cash"

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Referer": "https://web.sensibull.com/",
    "Origin": "https://web.sensibull.com",
}

# Polite delay between paginated month requests
_REQUEST_DELAY = 0.5


# -- Helpers ------------------------------------------------------------------

def _month_key(d: date) -> str:
    """Convert a date to Sensibull's year_month format: '2026-March'."""
    return f"{d.year}-{_MONTH_NAMES[d.month]}"


def _row_from_sensibull(trade_date: date, day_data: dict) -> dict | None:
    """
    Map one Sensibull day-entry to a canonical dict.

    Sensibull structure:
      {
        "cash": {
          "fii": {"buy": float, "sell": float, "buy_sell_difference": float, ...},
          "dii": {"buy": float, "sell": float, "buy_sell_difference": float, ...}
        },
        ...   (future/options data also present, ignored here)
      }
    """
    try:
        cash = day_data["cash"]
        fii = cash["fii"]
        dii = cash["dii"]
        return {
            "trade_date":        trade_date,
            "fii_gross_buy_cr":  float(fii["buy"]),
            "fii_gross_sell_cr": float(fii["sell"]),
            "fii_net_cr":        float(fii["buy_sell_difference"]),
            "dii_gross_buy_cr":  float(dii["buy"]),
            "dii_gross_sell_cr": float(dii["sell"]),
            "dii_net_cr":        float(dii["buy_sell_difference"]),
        }
    except (KeyError, TypeError, ValueError) as exc:
        logger.debug("Could not parse Sensibull entry for %s: %s", trade_date, exc)
        return None


# -- Row mappers -------------------------------------------------------------

def _fno_row_from_sensibull(trade_date: date, day_data: dict) -> dict | None:
    """
    Extract F&O participant OI data from a single Sensibull daily entry.

    Sensibull day_data sub-keys used:
        future.{fii,dii,pro,client}.quantity-wise.{net_oi, outstanding_oi, ...}
        option.{fii,dii,pro,client}.{call,put,overall_net_oi, overall_net_oi_change}
        nifty, banknifty, nifty_change_percent, banknifty_change_percent
    """
    try:
        fut  = day_data.get("future", {})
        opt  = day_data.get("option", {})

        def _qw(participant: str) -> dict:
            return fut.get(participant, {}).get("quantity-wise", {})

        def _opt(participant: str) -> dict:
            return opt.get(participant, {})

        fii_qw  = _qw("fii");  dii_qw  = _qw("dii")
        pro_qw  = _qw("pro");  cli_qw  = _qw("client")
        fii_opt = _opt("fii"); dii_opt = _opt("dii")
        pro_opt = _opt("pro"); cli_opt = _opt("client")

        return {
            "trade_date":                     trade_date,
            # Index futures
            "fii_fut_net_oi":                 float(fii_qw.get("net_oi", 0) or 0),
            "fii_fut_outstanding_oi":         float(fii_qw.get("outstanding_oi", 0) or 0),
            "fii_fut_nifty_net_oi":           float(fii_qw.get("nifty_net_oi", 0) or 0),
            "fii_fut_banknifty_net_oi":       float(fii_qw.get("banknifty_net_oi", 0) or 0),
            "dii_fut_net_oi":                 float(dii_qw.get("net_oi", 0) or 0),
            "dii_fut_outstanding_oi":         float(dii_qw.get("outstanding_oi", 0) or 0),
            "pro_fut_net_oi":                 float(pro_qw.get("net_oi", 0) or 0),
            "pro_fut_outstanding_oi":         float(pro_qw.get("outstanding_oi", 0) or 0),
            "client_fut_net_oi":              float(cli_qw.get("net_oi", 0) or 0),
            "client_fut_outstanding_oi":      float(cli_qw.get("outstanding_oi", 0) or 0),
            # Stock futures (may not always be present)
            "fii_fut_stock_net_oi":           0.0,
            "dii_fut_stock_net_oi":           0.0,
            "pro_fut_stock_net_oi":           0.0,
            "client_fut_stock_net_oi":        0.0,
            # Options
            "fii_opt_overall_net_oi":         float(fii_opt.get("overall_net_oi", 0) or 0),
            "fii_opt_overall_net_oi_change":  float(fii_opt.get("overall_net_oi_change", 0) or 0),
            "fii_opt_call_net_oi":            float((fii_opt.get("call") or {}).get("net_oi", 0) or 0),
            "fii_opt_put_net_oi":             float((fii_opt.get("put")  or {}).get("net_oi", 0) or 0),
            "dii_opt_overall_net_oi":         float(dii_opt.get("overall_net_oi", 0) or 0),
            "dii_opt_overall_net_oi_change":  float(dii_opt.get("overall_net_oi_change", 0) or 0),
            "pro_opt_overall_net_oi":         float(pro_opt.get("overall_net_oi", 0) or 0),
            "pro_opt_overall_net_oi_change":  float(pro_opt.get("overall_net_oi_change", 0) or 0),
            "client_opt_overall_net_oi":      float(cli_opt.get("overall_net_oi", 0) or 0),
            "client_opt_overall_net_oi_change": float(cli_opt.get("overall_net_oi_change", 0) or 0),
            # Market
            "nifty_close":        float(day_data.get("nifty", 0) or 0),
            "banknifty_close":    float(day_data.get("banknifty", 0) or 0),
            "nifty_change_pct":   float(day_data.get("nifty_change_percent", 0) or 0),
            "banknifty_change_pct": float(day_data.get("banknifty_change_percent", 0) or 0),
        }
    except (KeyError, TypeError, ValueError) as exc:
        logger.debug("Could not parse F&O entry for %s: %s", trade_date, exc)
        return None


# -- Fetch helpers ------------------------------------------------------------

def _fetch_month(client: httpx.Client, year_month: str | None = None) -> list[dict]:
    """
    Fetch one month of daily FII/DII data from Sensibull.

    Parameters
    ----------
    year_month : '2026-March' format, or None for the current month.

    Returns
    -------
    List of canonical dicts (trade_date, fii_*, dii_*).
    """
    params = {"year_month": year_month} if year_month else {}
    try:
        resp = client.get(_SENSIBULL_DAILY, params=params, timeout=15)
        resp.raise_for_status()
        payload = resp.json()
    except Exception as exc:
        logger.warning("Sensibull FII/DII fetch failed (year_month=%r): %s", year_month, exc)
        return []

    rows: list[dict] = []
    for date_str, day_data in payload.get("data", {}).items():
        try:
            trade_date = date.fromisoformat(date_str)
        except ValueError:
            logger.debug("Unrecognised date %r -- skipping", date_str)
            continue
        row = _row_from_sensibull(trade_date, day_data)
        if row:
            rows.append(row)

    return rows


def _available_months(client: httpx.Client) -> list[str]:
    """
    Return the list of available year_month keys from Sensibull
    (e.g. ['2025-October', '2025-November', ..., '2026-April']).
    """
    try:
        resp = client.get(_SENSIBULL_DAILY, timeout=15)
        resp.raise_for_status()
        return resp.json().get("key_list", [])
    except Exception as exc:
        logger.warning("Could not fetch Sensibull key_list: %s", exc)
        return []


# -- Public API ---------------------------------------------------------------

def fetch_fii_dii(from_date: date | None = None) -> list[dict]:
    """
    Fetch daily FII and DII cash-market flow data from Sensibull.

    Parameters
    ----------
    from_date : earliest trade_date to include.
                If None, only the current month is fetched.
                Pass a past date to fetch all available months back to that date.
                Sensibull retains ~6 months of history.

    Returns
    -------
    list[dict] with keys:
        trade_date, fii_gross_buy_cr, fii_gross_sell_cr, fii_net_cr,
        dii_gross_buy_cr, dii_gross_sell_cr, dii_net_cr

    Returns an empty list on unrecoverable errors.
    """
    all_rows: list[dict] = []

    try:
        with httpx.Client(headers=_HEADERS, follow_redirects=True) as client:
            if from_date is None:
                # Current month only
                all_rows = _fetch_month(client, year_month=None)
            else:
                # Get the list of available months, then fetch each needed one
                available = _available_months(client)
                if not available:
                    logger.warning(
                        "Sensibull returned empty key_list; falling back to current month"
                    )
                    all_rows = _fetch_month(client, year_month=None)
                else:
                    from_ord = _key_to_ord(_month_key(from_date))
                    # Filter to months >= from_date month (chronological comparison)
                    needed = [m for m in available if _key_to_ord(m) >= from_ord]
                    if not needed:
                        needed = [available[-1]]   # at least the latest month

                    for i, ym in enumerate(needed):
                        logger.debug("Fetching Sensibull FII/DII month: %s", ym)
                        batch = _fetch_month(client, year_month=ym)
                        all_rows.extend(batch)
                        if i < len(needed) - 1:
                            time.sleep(_REQUEST_DELAY)

    except Exception as exc:
        logger.error("Sensibull FII/DII fetch failed with unrecoverable error: %s", exc)
        return []

    # Deduplicate by trade_date (last-seen wins), then filter and sort
    by_date: dict[date, dict] = {}
    for row in all_rows:
        by_date[row["trade_date"]] = row
    all_rows = list(by_date.values())

    if from_date is not None:
        all_rows = [r for r in all_rows if r["trade_date"] >= from_date]

    all_rows.sort(key=lambda r: r["trade_date"])

    logger.info(
        "Sensibull FII/DII: fetched %d rows (%s -> %s)",
        len(all_rows),
        all_rows[0]["trade_date"] if all_rows else "-",
        all_rows[-1]["trade_date"] if all_rows else "-",
    )
    return all_rows


def fetch_fii_dii_monthly() -> list[dict]:
    """
    Fetch all available monthly FII/DII cash-market aggregate data.

    Endpoint: GET /v1/compute/cache/fii_dii_cash
    Returns ~92 months (Sep 2018 → present), one row per month.

    Returns
    -------
    list[dict] with keys:
        month_date, fii_buy_cr, fii_sell_cr, fii_net_cr,
        dii_buy_cr, dii_sell_cr, dii_net_cr, nifty_close, nifty_change_pct
    """
    try:
        with httpx.Client(headers=_HEADERS, follow_redirects=True) as client:
            resp = client.get(_SENSIBULL_MONTHLY, timeout=20)
            resp.raise_for_status()
            payload = resp.json()
    except Exception as exc:
        logger.error("Sensibull monthly FII/DII fetch failed: %s", exc)
        return []

    # Response is either {"data": {"YYYY-MM-DD": {...}}} or directly {"YYYY-MM-DD": {...}}
    raw = payload.get("data", payload)
    if not isinstance(raw, dict):
        logger.error("Unexpected monthly payload shape: %r", type(raw))
        return []

    rows: list[dict] = []
    for date_key, entry in raw.items():
        if not isinstance(entry, dict):
            continue
        try:
            month_date = date.fromisoformat(str(date_key))
        except ValueError:
            logger.debug("Skipping non-date monthly key: %r", date_key)
            continue
        try:
            rows.append({
                "month_date":      month_date,
                "fii_buy_cr":      float(entry.get("fii_buy", 0) or 0),
                "fii_sell_cr":     float(entry.get("fii_sell", 0) or 0),
                "fii_net_cr":      float(entry.get("fii_net", 0) or 0),
                "dii_buy_cr":      float(entry.get("dii_buy", 0) or 0),
                "dii_sell_cr":     float(entry.get("dii_sell", 0) or 0),
                "dii_net_cr":      float(entry.get("dii_net", 0) or 0),
                "nifty_close":     float(entry.get("nifty", 0) or 0),
                "nifty_change_pct": float(entry.get("nifty_change_percent", 0) or 0),
            })
        except (ValueError, TypeError) as exc:
            logger.debug("Skipping monthly row %r: %s", date_key, exc)

    rows.sort(key=lambda r: r["month_date"])
    logger.info(
        "Sensibull monthly FII/DII: fetched %d rows (%s -> %s)",
        len(rows),
        rows[0]["month_date"] if rows else "-",
        rows[-1]["month_date"] if rows else "-",
    )
    return rows


def fetch_fii_dii_fno(from_date: date | None = None) -> list[dict]:
    """
    Fetch daily F&O participant OI data from Sensibull.

    Uses the same daily endpoint but extracts the ``future`` and ``option``
    sub-keys rather than ``cash``.

    Parameters
    ----------
    from_date : earliest trade_date to include (same logic as fetch_fii_dii).

    Returns
    -------
    list[dict] with F&O OI fields (see _fno_row_from_sensibull for keys).
    """
    all_rows: list[dict] = []

    try:
        with httpx.Client(headers=_HEADERS, follow_redirects=True) as client:
            if from_date is None:
                months = [None]  # type: ignore[list-item]
            else:
                available = _available_months(client)
                if not available:
                    months = [None]  # type: ignore[list-item]
                else:
                    from_ord = _key_to_ord(_month_key(from_date))
                    needed   = [m for m in available if _key_to_ord(m) >= from_ord]
                    months   = needed or [available[-1]]  # type: ignore[assignment]

            for i, ym in enumerate(months):
                params = {"year_month": ym} if ym else {}
                try:
                    resp = client.get(_SENSIBULL_DAILY, params=params, timeout=15)
                    resp.raise_for_status()
                    payload = resp.json()
                except Exception as exc:
                    logger.warning("F&O fetch failed (year_month=%r): %s", ym, exc)
                    continue

                for date_str, day_data in payload.get("data", {}).items():
                    try:
                        trade_date = date.fromisoformat(date_str)
                    except ValueError:
                        continue
                    row = _fno_row_from_sensibull(trade_date, day_data)
                    if row:
                        all_rows.append(row)

                if i < len(months) - 1:
                    time.sleep(_REQUEST_DELAY)

    except Exception as exc:
        logger.error("Sensibull F&O fetch failed: %s", exc)
        return []

    # Deduplicate and filter
    by_date: dict[date, dict] = {}
    for row in all_rows:
        by_date[row["trade_date"]] = row
    all_rows = list(by_date.values())

    if from_date is not None:
        all_rows = [r for r in all_rows if r["trade_date"] >= from_date]

    all_rows.sort(key=lambda r: r["trade_date"])
    logger.info(
        "Sensibull F&O daily: fetched %d rows (%s -> %s)",
        len(all_rows),
        all_rows[0]["trade_date"] if all_rows else "-",
        all_rows[-1]["trade_date"] if all_rows else "-",
    )
    return all_rows


# -- Standalone runner --------------------------------------------------------

if __name__ == "__main__":
    """
    Run directly to fetch and (optionally) insert FII/DII data.

    Usage
    -----
    # Fetch current month -- dry-run, no DB write:
        python src/importer/fetchers/fii_dii_fetcher.py

    # Backfill from a specific date -- dry-run:
        python src/importer/fetchers/fii_dii_fetcher.py --from 2026-03-01

    # Fetch and INSERT into ClickHouse:
        python src/importer/fetchers/fii_dii_fetcher.py --insert

    # Backfill + insert:
        python src/importer/fetchers/fii_dii_fetcher.py --from 2026-03-01 --insert
    """
    import argparse
    import sys
    from pathlib import Path

    # Allow imports from project root
    sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

    from rich.console import Console
    from rich.table import Table
    from rich import box
    import logging as _logging

    _logging.basicConfig(
        level=_logging.INFO,
        format="%(levelname)s  %(name)s  %(message)s",
    )

    parser = argparse.ArgumentParser(
        description="Fetch FII/DII cash/F&O data from Sensibull and import to ClickHouse."
    )
    parser.add_argument(
        "--from",
        dest="from_date",
        metavar="YYYY-MM-DD",
        default=None,
        help="Earliest date to fetch (triggers historical backfill). "
             "Defaults to current month only.",
    )
    parser.add_argument(
        "--insert",
        action="store_true",
        default=False,
        help="Insert fetched rows into ClickHouse (uses config/settings.py). "
             "Without this flag the script runs as a dry-run.",
    )
    parser.add_argument(
        "--only",
        choices=["cash", "monthly", "fno", "all"],
        default="all",
        help="Which dataset to fetch: cash (daily), monthly, fno (daily F&O), or all (default).",
    )
    args = parser.parse_args()

    from_date_parsed: date | None = None
    if args.from_date:
        try:
            from_date_parsed = date.fromisoformat(args.from_date)
        except ValueError:
            print(f"Error: --from must be YYYY-MM-DD, got {args.from_date!r}")
            sys.exit(1)

    console = Console()
    fetch_all   = args.only == "all"
    do_cash     = fetch_all or args.only == "cash"
    do_monthly  = fetch_all or args.only == "monthly"
    do_fno      = fetch_all or args.only == "fno"

    console.print(
        f"[bold cyan]▶ FII/DII Importer[/bold cyan]  "
        f"from={from_date_parsed or 'recent'}  "
        f"tables={'cash+monthly+fno' if fetch_all else args.only}  "
        f"insert={args.insert}"
    )

    # ── 1. Daily cash flows ───────────────────────────────────────────────────
    cash_rows: list[dict] = []
    if do_cash:
        cash_rows = fetch_fii_dii(from_date=from_date_parsed)
        if cash_rows:
            t = Table(
                title=f"Daily Cash Flows ({len(cash_rows)} rows)",
                box=box.ROUNDED, header_style="bold magenta", show_lines=False,
            )
            t.add_column("Date",          justify="center")
            t.add_column("FII Buy (Cr)",  justify="right")
            t.add_column("FII Sell (Cr)", justify="right")
            t.add_column("FII Net (Cr)",  justify="right", style="bold")
            t.add_column("DII Buy (Cr)",  justify="right")
            t.add_column("DII Sell (Cr)", justify="right")
            t.add_column("DII Net (Cr)",  justify="right", style="bold")
            for r in cash_rows[-10:]:
                fii_net_style = "green" if r["fii_net_cr"] >= 0 else "red"
                dii_net_style = "green" if r["dii_net_cr"] >= 0 else "red"
                t.add_row(
                    str(r["trade_date"]),
                    f"{r['fii_gross_buy_cr']:,.0f}",
                    f"{r['fii_gross_sell_cr']:,.0f}",
                    f"[{fii_net_style}]{r['fii_net_cr']:+,.0f}[/{fii_net_style}]",
                    f"{r['dii_gross_buy_cr']:,.0f}",
                    f"{r['dii_gross_sell_cr']:,.0f}",
                    f"[{dii_net_style}]{r['dii_net_cr']:+,.0f}[/{dii_net_style}]",
                )
            console.print(t)
            if len(cash_rows) > 10:
                console.print(f"  [dim](showing last 10 of {len(cash_rows)} rows)[/dim]")
        else:
            console.print("[yellow]⚠ No daily cash rows returned.[/yellow]")

    # ── 2. Monthly aggregate (Sep 2018 → present) ────────────────────────────
    monthly_rows: list[dict] = []
    if do_monthly:
        monthly_rows = fetch_fii_dii_monthly()
        if monthly_rows:
            t2 = Table(
                title=f"Monthly Aggregate ({len(monthly_rows)} rows, "
                      f"{monthly_rows[0]['month_date']} → {monthly_rows[-1]['month_date']})",
                box=box.ROUNDED, header_style="bold blue", show_lines=False,
            )
            t2.add_column("Month",        justify="center")
            t2.add_column("FII Net (Cr)", justify="right", style="bold")
            t2.add_column("DII Net (Cr)", justify="right", style="bold")
            t2.add_column("Nifty",        justify="right")
            t2.add_column("Nifty Chg%",   justify="right")
            for r in monthly_rows[-12:]:
                fn = "green" if r["fii_net_cr"] >= 0 else "red"
                dn = "green" if r["dii_net_cr"] >= 0 else "red"
                nc = "green" if r["nifty_change_pct"] >= 0 else "red"
                t2.add_row(
                    str(r["month_date"])[:7],
                    f"[{fn}]{r['fii_net_cr']:+,.0f}[/{fn}]",
                    f"[{dn}]{r['dii_net_cr']:+,.0f}[/{dn}]",
                    f"{r['nifty_close']:,.0f}",
                    f"[{nc}]{r['nifty_change_pct']:+.1f}%[/{nc}]",
                )
            console.print(t2)
            if len(monthly_rows) > 12:
                console.print(f"  [dim](showing last 12 of {len(monthly_rows)} rows)[/dim]")
        else:
            console.print("[yellow]⚠ No monthly rows returned.[/yellow]")

    # ── 3. Daily F&O participant OI ───────────────────────────────────────────
    fno_rows: list[dict] = []
    if do_fno:
        fno_rows = fetch_fii_dii_fno(from_date=from_date_parsed)
        if fno_rows:
            t3 = Table(
                title=f"Daily F&O OI ({len(fno_rows)} rows)",
                box=box.ROUNDED, header_style="bold yellow", show_lines=False,
            )
            t3.add_column("Date",               justify="center")
            t3.add_column("FII Fut Net",         justify="right")
            t3.add_column("FII Opt OI",          justify="right")
            t3.add_column("FII Nifty OI Chg",    justify="right")
            t3.add_column("DII Fut Net",          justify="right")
            t3.add_column("Nifty",               justify="right")
            for r in fno_rows[-10:]:
                fn = "green" if r["fii_fut_net_oi"] >= 0 else "red"
                fo = "green" if r["fii_opt_overall_net_oi"] >= 0 else "red"
                t3.add_row(
                    str(r["trade_date"]),
                    f"[{fn}]{r['fii_fut_net_oi']:+,.0f}[/{fn}]",
                    f"[{fo}]{r['fii_opt_overall_net_oi']:+,.0f}[/{fo}]",
                    f"{r['fii_fut_nifty_net_oi']:+,.0f}",
                    f"{r['dii_fut_net_oi']:+,.0f}",
                    f"{r['nifty_close']:,.0f}",
                )
            console.print(t3)
            if len(fno_rows) > 10:
                console.print(f"  [dim](showing last 10 of {len(fno_rows)} rows)[/dim]")
        else:
            console.print("[yellow]⚠ No F&O rows returned.[/yellow]")

    total_rows = len(cash_rows) + len(monthly_rows) + len(fno_rows)
    if total_rows == 0:
        console.print("[bold red]✗ No data fetched at all -- Sensibull API may be down.[/bold red]")
        sys.exit(1)

    # -- Optional ClickHouse insert -------------------------------------------
    if args.insert:
        try:
            from config.settings import settings
            from src.importer.clickhouse import ClickHouseImporter

            ch = ClickHouseImporter(
                host=settings.clickhouse_host,
                port=settings.clickhouse_port,
                database=settings.clickhouse_database,
                username=settings.clickhouse_user,
                password=settings.clickhouse_password,
            )
            ch.ensure_schema()

            if cash_rows:
                n = ch.insert_fii_dii_flows(cash_rows)
                ch.set_watermark(
                    "nse_fii_dii", "MARKET",
                    max(r["trade_date"] for r in cash_rows),
                )
                console.print(f"[bold green]✓ {n} rows → market_data.fii_dii_flows[/bold green]")

            if monthly_rows:
                n = ch.insert_fii_dii_monthly(monthly_rows)
                console.print(f"[bold green]✓ {n} rows → market_data.fii_dii_monthly[/bold green]")

            if fno_rows:
                n = ch.insert_fii_dii_fno_daily(fno_rows)
                console.print(f"[bold green]✓ {n} rows → market_data.fii_dii_fno_daily[/bold green]")

            ch.close()
        except Exception as exc:
            console.print(f"[bold red]✗ ClickHouse insert failed:[/bold red] {exc}")
            raise
    else:
        console.print(
            "[dim]Dry-run — no data written. "
            "Pass [bold]--insert[/bold] to write to ClickHouse.[/dim]"
        )
