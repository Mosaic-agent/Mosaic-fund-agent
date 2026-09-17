"""
src/scripts/backfill_mf_qdrant.py
───────────────────────────────────
Backfill existing mf_holdings data from ClickHouse into Qdrant.

Strategy
────────
Only vectorizes the LATEST as_of_month per fund — historical months would
bloat Qdrant with stale profiles and are not needed for similarity queries
(we always want the most recent portfolio composition).

Incremental / Delta Strategy
────────────────────────────
By default, this script operates in delta mode:
1. Checks ClickHouse `market_data.import_watermarks` (source='qdrant_mf_holdings').
   - If watermarks are missing, automatically seeds them from existing Qdrant
     mf_fund_profiles to avoid re-embedding existing points.
2. Identifies only funds where latest `as_of_month` > watermark `last_date`
   (or funds never seen before).
3. If delta is empty, exits immediately (~0.1s).
4. When a fund rolls into a new month, prunes old month's points from
   `mf_holdings` in Qdrant to prevent cross-month accumulation.
5. Updates watermarks in `market_data.import_watermarks`.

Use `--force` to ignore watermarks and re-embed all funds.

Collections populated
─────────────────────
  mf_holdings       — one point per (fund × security) for the latest month
  mf_fund_profiles  — one aggregated fingerprint per fund

Usage
─────
    python -m src.scripts.backfill_mf_qdrant           # delta sync (only new/changed months)
    python -m src.scripts.backfill_mf_qdrant --force   # force re-vectorize all latest holdings
    python -m src.scripts.backfill_mf_qdrant --limit 5 # first 5 funds (demo)
    python -m src.scripts.backfill_mf_qdrant --dry-run # count only
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)

_BATCH = 200   # holdings rows per embed call
_SOURCE_WATERMARK = "qdrant_mf_holdings"


def _seed_watermarks_from_qdrant(client) -> int:
    """If import_watermarks has no records for qdrant_mf_holdings, seed it from Qdrant mf_fund_profiles."""
    from src.db.pool import acquire, query_df
    from src.db.mf_vector import _PROFILES_COLLECTION

    existing = query_df(
        f"SELECT count() as cnt FROM market_data.import_watermarks FINAL WHERE source = '{_SOURCE_WATERMARK}'"
    )
    if not existing.empty and existing["cnt"].iloc[0] > 0:
        return 0

    log.info("Watermark table has no entries for '%s'. Seeding from Qdrant profiles…", _SOURCE_WATERMARK)
    offset = None
    seen_funds: dict[str, str] = {}

    while True:
        try:
            points, offset = client.scroll(
                collection_name=_PROFILES_COLLECTION,
                limit=500,
                offset=offset,
                with_payload=True,
                with_vectors=False,
            )
            for p in points:
                fn = p.payload.get("fund_name")
                m = p.payload.get("as_of_month")
                if fn and m:
                    m_str = str(m)[:7]
                    if fn not in seen_funds or m_str > seen_funds[fn]:
                        seen_funds[fn] = m_str
            if offset is None:
                break
        except Exception as e:
            log.warning("Could not scroll profiles from Qdrant: %s", e)
            break

    if seen_funds:
        now = datetime.now()
        watermark_rows = []
        for fn, ym in seen_funds.items():
            try:
                d_val = datetime.strptime(f"{ym}-01", "%Y-%m-%d").date()
                watermark_rows.append((_SOURCE_WATERMARK, fn, d_val, now, "mf_holdings"))
            except Exception:
                continue

        with acquire() as ch:
            ch.insert(
                "market_data.import_watermarks",
                watermark_rows,
                column_names=["source", "symbol", "last_date", "updated_at", "dataset"],
            )
        log.info("Seeded %d fund watermarks into market_data.import_watermarks", len(watermark_rows))
        return len(watermark_rows)
    return 0


def run(limit: int = 0, dry_run: bool = False, fund_filter: str = "", force: bool = False) -> None:
    from src.db.pool import acquire, query_df
    from src.db.mf_vector import (
        _do_vectorize_holdings,
        _do_vectorize_profiles,
        _get_client,
        _HOLDINGS_COLLECTION,
        _PROFILES_COLLECTION,
    )
    from qdrant_client.http import models as q_models

    # ── 1. Check Qdrant ───────────────────────────────────────────────────────
    client = _get_client()
    if client is None:
        log.error("Qdrant not reachable — is it running? (docker-compose up qdrant)")
        sys.exit(1)
    log.info("Qdrant connected")

    # ── 2. Check Watermarks & Identify Delta Funds ────────────────────────────
    synced_map: dict[str, str] = {}
    if not force:
        _seed_watermarks_from_qdrant(client)
        watermark_df = query_df(f"""
            SELECT symbol AS fund_name, max(last_date) AS last_synced
            FROM market_data.import_watermarks FINAL
            WHERE source = '{_SOURCE_WATERMARK}'
            GROUP BY symbol
        """)
        if not watermark_df.empty:
            for _, r in watermark_df.iterrows():
                val = r["last_synced"]
                synced_map[r["fund_name"]] = str(val)[:7]

    log.info("Querying ClickHouse for latest-month disclosures per fund…")
    latest_funds_df = query_df("""
        SELECT fund_name, max(as_of_month) AS latest_month
        FROM market_data.mf_holdings FINAL
        GROUP BY fund_name
    """)

    if latest_funds_df.empty:
        log.warning("No mf_holdings data found in ClickHouse. Import fund data first.")
        return

    funds_to_process: list[tuple[str, any, str | None]] = []
    for _, r in latest_funds_df.iterrows():
        fn = r["fund_name"]
        m_val = r["latest_month"]
        m_str = str(m_val)[:7]
        if force or fn not in synced_map:
            funds_to_process.append((fn, m_val, None))
        elif m_str > synced_map[fn]:
            funds_to_process.append((fn, m_val, synced_map[fn]))

    if fund_filter:
        funds_to_process = [f for f in funds_to_process if fund_filter.lower() in f[0].lower()]

    if not funds_to_process:
        log.info("All funds are already up to date in Qdrant (delta=0). Nothing to do.")
        log.info("Use --force to re-vectorize all latest holdings.")
        return

    if limit:
        funds_to_process = funds_to_process[:limit]

    target_fund_names = [f[0] for f in funds_to_process]
    target_funds_set = set(target_fund_names)

    # ── 3. Fetch holdings for delta funds only ─────────────────────────────────
    log.info("Querying ClickHouse for holdings of %d delta funds…", len(target_fund_names))
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
    df = df[df["fund_name"].isin(target_funds_set)]

    total_rows = len(df)
    log.info("Found %d holdings across %d delta funds", total_rows, len(target_fund_names))

    if dry_run:
        log.info("[DRY RUN] Would vectorize %d holdings → %d fund profiles", total_rows, len(target_fund_names))
        print("\nFunds to process:")
        for fn, m_val, old_m in funds_to_process:
            n = (df["fund_name"] == fn).sum()
            status = f"UPDATE ({old_m} -> {str(m_val)[:7]})" if old_m else f"NEW ({str(m_val)[:7]})"
            print(f"  {fn:50s}  {status:25s}  {n:4d} holdings")
        return

    # ── 4. Prune old month points from Qdrant for updated funds ────────────────
    for fn, _, old_m in funds_to_process:
        if old_m:
            try:
                client.delete(
                    collection_name=_HOLDINGS_COLLECTION,
                    points_selector=q_models.Filter(
                        must=[
                            q_models.FieldCondition(key="fund_name", match=q_models.MatchValue(value=fn)),
                            q_models.FieldCondition(key="as_of_month", match=q_models.MatchValue(value=str(old_m)[:7])),
                        ]
                    ),
                )
            except Exception as e:
                log.debug("Could not prune old points for %s (%s): %s", fn, old_m, e)

    # ── 5. Vectorize in batches ───────────────────────────────────────────────
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

    # ── 6. Fund profiles (one per fund, fast) ─────────────────────────────────
    log.info("Building %d fund profiles…", len(target_fund_names))
    _do_vectorize_profiles(rows_all)
    log.info("Fund profiles done")

    # ── 7. Record updated watermarks in ClickHouse ─────────────────────────────
    now = datetime.now()
    watermark_rows = []
    for fn, m_val, _ in funds_to_process:
        try:
            d_val = datetime.strptime(str(m_val)[:10], "%Y-%m-%d").date()
            watermark_rows.append((_SOURCE_WATERMARK, fn, d_val, now, "mf_holdings"))
        except Exception:
            continue

    if watermark_rows:
        with acquire() as ch:
            ch.insert(
                "market_data.import_watermarks",
                watermark_rows,
                column_names=["source", "symbol", "last_date", "updated_at", "dataset"],
            )
        log.info("Recorded %d updated watermarks in market_data.import_watermarks", len(watermark_rows))

    # ── 8. Report collection sizes ────────────────────────────────────────────
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

    log.info("Delta sync complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill MF holdings into Qdrant (delta-aware)")
    parser.add_argument("--limit", type=int, default=0,
                        help="Process only N funds (0 = all eligible). Use 5 for a quick demo.")
    parser.add_argument("--filter", type=str, default="",
                        help="Filter funds matching this substring (e.g. 'QSIF' or 'DSP').")
    parser.add_argument("--force", action="store_true",
                        help="Ignore watermarks and force re-vectorize all funds.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print what would be vectorized without touching Qdrant.")
    args = parser.parse_args()
    run(limit=args.limit, dry_run=args.dry_run, fund_filter=args.filter, force=args.force)
