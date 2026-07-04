#!/usr/bin/env python3
"""
src/scripts/refit_premium_ou.py
────────────────────────────────
Nightly OU refit for ETF premium series.

For each symbol in DOMESTIC_ETF_SYMBOLS ∪ INTL_ETF_SYMBOLS:
  1. Pull last 90 days of EOD premiums from inav_snapshots
  2. Fit OU via ou_estimator.fit_ou()
  3. Upsert to market_data.premium_ou_state

Usage:
    python src/scripts/refit_premium_ou.py              # fit + save
    python src/scripts/refit_premium_ou.py --dry-run    # fit + print, no write
    python src/scripts/refit_premium_ou.py --lookback 60
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import date, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from rich.console import Console
from rich.table import Table

console = Console()


def main():
    parser = argparse.ArgumentParser(description="Nightly OU premium refit")
    parser.add_argument("--dry-run", action="store_true", help="Print results without writing to ClickHouse")
    parser.add_argument("--lookback", type=int, default=0, help="Override lookback days (0 = per-universe defaults: 90d domestic, 60d intl)")
    parser.add_argument("--symbols", type=str, default="", help="Comma-separated symbols (default: all)")
    args = parser.parse_args()

    from src.db.pool import get_pool
    from src.ml.ou_estimator import fit_ou, OUState
    from src.tools.domestic_etf_scanner import DOMESTIC_ETF_SYMBOLS
    from src.tools.premium_alerts import INTL_ETF_SYMBOLS

    pool = get_pool()

    # Per-universe lookback defaults: intl 60d (regime-shifted 2024), domestic 90d (stable)
    DOMESTIC_LOOKBACK = 90
    INTL_LOOKBACK = 60

    intl_set = set(INTL_ETF_SYMBOLS)

    if args.symbols:
        symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    else:
        symbols = list(dict.fromkeys(DOMESTIC_ETF_SYMBOLS + INTL_ETF_SYMBOLS))

    today = date.today().isoformat()

    results: list[tuple[str, OUState | None, str]] = []

    # Determine effective lookback per symbol
    def _lookback_for(sym: str) -> int:
        if args.lookback > 0:
            return args.lookback
        return INTL_LOOKBACK if sym in intl_set else DOMESTIC_LOOKBACK

    label = f"lookback={'per-universe (90d/60d)' if args.lookback == 0 else f'{args.lookback}d'}"
    console.print(f"\n[bold cyan]OU Premium Refit[/bold cyan]  {label}  symbols={len(symbols)}  {'DRY RUN' if args.dry_run else 'LIVE'}\n")

    for sym in symbols:
        try:
            lb = _lookback_for(sym)
            cutoff = (date.today() - timedelta(days=lb)).isoformat()
            rows = pool.query_df(
                f"""
                SELECT
                    toDate(snapshot_at) AS trade_date,
                    argMax(premium_discount_pct, snapshot_at) AS premium
                FROM market_data.inav_snapshots
                WHERE symbol = '{sym}'
                  AND snapshot_at >= toDateTime('{cutoff} 00:00:00')
                GROUP BY trade_date
                ORDER BY trade_date ASC
                """
            )

            if rows.empty or len(rows) < 10:
                results.append((sym, None, f"Only {len(rows)} daily obs"))
                continue

            premiums = rows["premium"].astype(float).tolist()
            state = fit_ou(premiums, dt=1.0)

            if state is None:
                results.append((sym, None, "OU fit failed (non-stationary or insufficient data)"))
                continue

            results.append((sym, state, "OK"))

            if not args.dry_run:
                pool.execute(
                    f"""
                    INSERT INTO market_data.premium_ou_state
                        (symbol, fit_date, theta, mu, sigma, half_life_days, n_obs, fit_r2)
                    VALUES
                        ('{sym}', '{today}', {state.theta}, {state.mu},
                         {state.sigma}, {state.half_life_days}, {state.n_obs}, {state.fit_r2})
                    """
                )

        except Exception as exc:
            results.append((sym, None, f"Error: {exc}"))

    # ── Display results ──────────────────────────────────────────────────────
    tbl = Table(title="OU Refit Results", show_header=True, header_style="bold magenta")
    tbl.add_column("Symbol", min_width=14, style="bold")
    tbl.add_column("θ (speed)", min_width=10, justify="right")
    tbl.add_column("μ (eq. prem %)", min_width=14, justify="right")
    tbl.add_column("σ", min_width=10, justify="right")
    tbl.add_column("Half-life (d)", min_width=13, justify="right")
    tbl.add_column("N obs", min_width=7, justify="right")
    tbl.add_column("R²", min_width=7, justify="right")
    tbl.add_column("Status", min_width=10)

    ok_count = 0
    for sym, state, status in results:
        if state:
            ok_count += 1
            tbl.add_row(
                sym,
                f"{state.theta:.4f}",
                f"{state.mu:.2f}%",
                f"{state.sigma:.4f}",
                f"{state.half_life_days:.1f}",
                str(state.n_obs),
                f"{state.fit_r2:.3f}",
                "[green]✓[/green]",
            )
        else:
            tbl.add_row(sym, "—", "—", "—", "—", "—", "—", f"[yellow]{status}[/yellow]")

    console.print(tbl)
    console.print(f"\n[bold]{'DRY RUN — ' if args.dry_run else ''}Fitted {ok_count}/{len(symbols)} symbols[/bold]\n")


if __name__ == "__main__":
    main()
