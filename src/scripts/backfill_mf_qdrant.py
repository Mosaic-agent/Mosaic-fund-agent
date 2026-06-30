"""
src/scripts/backfill_mf_qdrant.py
───────────────────────────────────
Backfill existing mf_holdings data from ClickHouse into Qdrant.

Strategy
────────
Only vectorizes the LATEST as_of_month per fund — historical months would
bloat Qdrant with stale profiles and are not needed for similarity queries
(we always want the most recent portfolio composition).

Collections populated
─────────────────────
  mf_holdings       — one point per (fund × security) for the latest month
  mf_fund_profiles  — one aggregated fingerprint per fund

Usage
─────
    python -m src.scripts.backfill_mf_qdrant           # all funds, batch 200
    python -m src.scripts.backfill_mf_qdrant --limit 5 # first 5 funds (demo)
    python -m src.scripts.backfill_mf_qdrant --dry-run # count only
"""

from __future__ import annotations

import argparse
import logging
import sys
import time

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)

_BATCH = 200   # holdings rows per embed call


def run(limit: int = 0, dry_run: bool = False) -> None:
    from src.db.pool import query_df
    from src.db.mf_vector import (
        _do_vectorize_holdings,
        _do_vectorize_profiles,
        _get_client,
        _HOLDINGS_COLLECTION,
        _PROFILES_COLLECTION,
    )

    # ── 1. Check Qdrant ───────────────────────────────────────────────────────
    client = _get_client()
    if client is None:
        log.error("Qdrant not reachable — is it running? (docker-compose up qdrant)")
        sys.exit(1)
    log.info("Qdrant connected")

    # ── 2. Fetch latest-month holdings from ClickHouse ────────────────────────
    log.info("Querying ClickHouse for latest-month holdings…")
    df = query_df("""
        SELECT
            fund_name, scheme_code, isin, security_name,
            asset_type, toFloat64(market_value_cr) AS market_value_cr,
            toFloat64(pct_of_nav) AS pct_of_nav, as_of_month
        FROM market_data.mf_holdings FINAL
        WHERE (fund_name, as_of_month) IN (
            SELECT fund_name, max(as_of_month)
            FROM market_data.mf_holdings FINAL
            GROUP BY fund_name
        )
        ORDER BY fund_name, pct_of_nav DESC
    """)

    if df.empty:
        log.warning("No mf_holdings data found in ClickHouse. Import fund data first.")
        return

    funds = df["fund_name"].unique().tolist()
    if limit:
        funds = funds[:limit]
        df = df[df["fund_name"].isin(funds)]

    total_rows = len(df)
    log.info("Found %d holdings across %d funds (latest month each)", total_rows, len(funds))

    if dry_run:
        log.info("[DRY RUN] Would vectorize %d holdings → %d fund profiles", total_rows, len(funds))
        print("\nFunds to process:")
        for fn in funds:
            month = df[df["fund_name"] == fn]["as_of_month"].iloc[0]
            n = (df["fund_name"] == fn).sum()
            print(f"  {fn:50s}  {str(month)[:7]}  {n:4d} holdings")
        return

    # ── 3. Vectorize in batches ───────────────────────────────────────────────
    rows_all = df.to_dict("records")

    log.info("Vectorizing %d holdings in batches of %d…", total_rows, _BATCH)
    t0 = time.time()
    batches_done = 0
    for start in range(0, len(rows_all), _BATCH):
        batch = rows_all[start: start + _BATCH]
        _do_vectorize_holdings(batch)
        batches_done += 1
        pct = min(100, (start + len(batch)) / total_rows * 100)
        log.info("  holdings %d/%d (%.0f%%)", start + len(batch), total_rows, pct)

    log.info("Holdings vectorized in %.1fs", time.time() - t0)

    # ── 4. Fund profiles (one per fund, fast) ─────────────────────────────────
    log.info("Building %d fund profiles…", len(funds))
    _do_vectorize_profiles(rows_all)
    log.info("Fund profiles done")

    # ── 5. Report collection sizes ────────────────────────────────────────────
    try:
        h_info = client.get_collection(_HOLDINGS_COLLECTION)
        p_info = client.get_collection(_PROFILES_COLLECTION)
        log.info(
            "Qdrant: %s → %d points | %s → %d points",
            _HOLDINGS_COLLECTION, h_info.points_count,
            _PROFILES_COLLECTION, p_info.points_count,
        )
    except Exception as e:
        log.debug("Could not read collection info: %s", e)

    log.info("Backfill complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill MF holdings into Qdrant")
    parser.add_argument("--limit", type=int, default=0,
                        help="Process only N funds (0 = all). Use 5 for a quick demo.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print what would be vectorized without touching Qdrant.")
    args = parser.parse_args()
    run(limit=args.limit, dry_run=args.dry_run)
