"""
src/importer/fetchers/nse_inav_fetcher.py
──────────────────────────────────────────
Fetches live iNAV snapshots for Indian ETFs from the NSE API.

The NSE ETF endpoint (https://www.nseindia.com/api/etf) returns all ETFs
with their current indicative NAV (iNAV) and last traded price.  It is
updated every ~15 seconds during market hours.

Since NSE does NOT provide historical iNAV, each call captures a single
timestamped snapshot.  Call repeatedly (e.g. every 15 min during market
hours via a cron job) to build a time series.

Returns a list of dicts suitable for ClickHouseImporter.insert_inav_snapshots().
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any

import httpx

logger = logging.getLogger(__name__)

_NSE_ETF_URL   = "https://www.nseindia.com/api/etf"
_NSE_WARMUP    = "https://www.nseindia.com/market-data/exchange-traded-funds-etf"
_NSE_HEADERS   = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/market-data/exchange-traded-funds-etf",
    "Connection": "keep-alive",
}
_TIMEOUT = 15


def _safe(val: Any, default: float = 0.0) -> float:
    try:
        return float(str(val).replace(",", ""))
    except (TypeError, ValueError):
        return default


def fetch_inav_snapshots(symbols: list[str]) -> list[dict[str, Any]]:
    """
    Fetch a live iNAV snapshot for each symbol in `symbols` from the NSE API,
    and merge/overwrite with live iNAV data from Nippon India AMC website for Nippon ETFs.

    Parameters
    ----------
    symbols : list of NSE symbols, e.g. ["GOLDBEES", "NIFTYBEES"]

    Returns
    -------
    list of dicts with keys:
        symbol, snapshot_at (datetime UTC), inav, market_price,
        premium_discount_pct, source
    """
    clean = {s.upper().replace(".NS", "") for s in symbols}
    snapshot_at = datetime.now(timezone.utc).replace(tzinfo=None)  # ClickHouse expects naive UTC

    nse_rows: list[dict[str, Any]] = []
    try:
        with httpx.Client(headers=_NSE_HEADERS, follow_redirects=True, timeout=_TIMEOUT) as client:
            # Warm up to get session cookies required by NSE
            client.get(_NSE_WARMUP, timeout=10)
            time.sleep(0.6)
            resp = client.get(_NSE_ETF_URL, timeout=_TIMEOUT)
            resp.raise_for_status()
            data = resp.json()

            etf_list: list[dict] = data.get("data", []) if isinstance(data, dict) else data
            if etf_list:
                for entry in etf_list:
                    sym = str(entry.get("symbol", "")).upper()
                    if sym not in clean:
                        continue

                    raw_nav  = entry.get("nav") or entry.get("iNav")
                    raw_ltp  = entry.get("ltP") or entry.get("lastPrice")

                    if raw_nav is None:
                        continue

                    inav         = _safe(raw_nav)
                    market_price = _safe(raw_ltp) if raw_ltp is not None else inav
                    prem_disc    = ((market_price - inav) / inav * 100) if inav else 0.0

                    if sym == "SILVERCASE":
                        # Verify and correct duplicate GOLDCASE iNAV glitch
                        goldcase_entry = next((e for e in etf_list if str(e.get("symbol", "")).upper() == "GOLDCASE"), None)
                        silverbees_entry = next((e for e in etf_list if str(e.get("symbol", "")).upper() == "SILVERBEES"), None)
                        if goldcase_entry and silverbees_entry:
                            raw_gold_nav = goldcase_entry.get("nav") or goldcase_entry.get("iNav")
                            raw_silverbees_nav = silverbees_entry.get("nav") or silverbees_entry.get("iNav")
                            if raw_gold_nav and raw_silverbees_nav:
                                gold_inav = _safe(raw_gold_nav)
                                silverbees_inav = _safe(raw_silverbees_nav)
                                if abs(inav - gold_inav) < 1e-6 and silverbees_inav > 0:
                                    corrected_inav = round(silverbees_inav * 0.106127, 4)
                                    inav = corrected_inav
                                    prem_disc = ((market_price - inav) / inav * 100) if inav else 0.0

                    nse_rows.append({
                        "symbol":               sym,
                        "snapshot_at":          snapshot_at,
                        "inav":                 inav,
                        "market_price":         market_price,
                        "premium_discount_pct": round(prem_disc, 4),
                        "source":               "NSE",
                    })
    except Exception as exc:
        logger.warning("NSE iNAV fetch failed: %s", exc)

    # Fetch live iNAVs from Nippon AMC website for Nippon-managed ETFs
    from src.data_importer.fetchers.nippon_inav_fetcher import fetch_inav_nippon, NIPPON_SYMBOL_MAP
    nippon_symbols = [s for s in clean if s in NIPPON_SYMBOL_MAP]
    nippon_rows: list[dict[str, Any]] = []
    if nippon_symbols:
        try:
            nippon_rows = fetch_inav_nippon(nippon_symbols)
        except Exception as exc:
            logger.warning("Nippon AMC iNAV fetch failed: %s", exc)

    # Fetch live iNAVs from Zerodha AMC API for Zerodha-managed ETFs
    from src.data_importer.fetchers.zerodha_inav_fetcher import fetch_inav_zerodha, ZERODHA_SYMBOLS
    zerodha_symbols = [s for s in clean if s in ZERODHA_SYMBOLS]
    zerodha_rows: list[dict[str, Any]] = []
    if zerodha_symbols:
        try:
            zerodha_rows = fetch_inav_zerodha(zerodha_symbols)
        except Exception as exc:
            logger.warning("Zerodha AMC iNAV fetch failed: %s", exc)

    # Fetch live iNAVs from Mirae AMC API for Mirae-managed ETFs
    from src.data_importer.fetchers.mirae_inav_fetcher import fetch_inav_mirae, MIRAE_SYMBOLS
    mirae_symbols = [s for s in clean if s in MIRAE_SYMBOLS]
    mirae_rows: list[dict[str, Any]] = []
    if mirae_symbols:
        try:
            mirae_rows = fetch_inav_mirae(mirae_symbols)
        except Exception as exc:
            logger.warning("Mirae AMC iNAV fetch failed: %s", exc)

    # Fetch live iNAVs from Motilal Oswal AMC API for MON100, MONQ50
    from src.data_importer.fetchers.motilal_inav_fetcher import fetch_inav_motilal, MOTILAL_SYMBOLS
    motilal_symbols = [s for s in clean if s in MOTILAL_SYMBOLS]
    motilal_rows: list[dict[str, Any]] = []
    if motilal_symbols:
        try:
            motilal_rows = fetch_inav_motilal(motilal_symbols)
        except Exception as exc:
            logger.warning("Motilal AMC iNAV fetch failed: %s", exc)

    # Merge: NSE base, then overwrite with AMC-specific rows (higher accuracy)
    merged: dict[str, dict[str, Any]] = {}
    for r in nse_rows:
        merged[r["symbol"]] = r
    for r in nippon_rows:
        merged[r["symbol"]] = r
    for r in zerodha_rows:
        merged[r["symbol"]] = r
    for r in mirae_rows:
        merged[r["symbol"]] = r
    for r in motilal_rows:
        merged[r["symbol"]] = r

    final_rows = list(merged.values())

    from src.utils.ist import utc_to_ist
    _snap_ist = utc_to_ist(snapshot_at).strftime("%Y-%m-%d %H:%M:%S IST")
    logger.info(
        "Combined iNAV: captured %d snapshot(s) at %s "
        "(NSE: %d, Nippon: %d, Zerodha: %d, Mirae: %d, Motilal: %d)",
        len(final_rows), _snap_ist,
        len(nse_rows), len(nippon_rows), len(zerodha_rows), len(mirae_rows), len(motilal_rows),
    )
    return final_rows


def _is_market_open() -> bool:
    """Return True if NSE is currently in its normal trading session (IST 09:15–15:30)."""
    from datetime import datetime, timezone, timedelta
    ist = datetime.now(timezone(timedelta(hours=5, minutes=30)))
    if ist.weekday() >= 5:          # Saturday / Sunday
        return False
    open_t  = ist.replace(hour=9,  minute=15, second=0, microsecond=0)
    close_t = ist.replace(hour=15, minute=30, second=0, microsecond=0)
    return open_t <= ist <= close_t


def get_latest_inav(
    symbol: str,
    max_age_days: int = 4,
    max_age_minutes: int | None = None,
    store_to_db: bool = True,
) -> dict[str, Any] | None:
    """
    Return the latest iNAV snapshot for *symbol*.

    During market hours (IST 09:15–15:30) freshness is evaluated against
    *max_age_minutes* (default 2 min) — iNAV updates every ~15 s, so data
    older than that is treated as stale and triggers a live fetch.
    Outside market hours the coarser *max_age_days* threshold applies.

    Resolution order:
      1. ClickHouse ``inav_snapshots`` — if row is fresh enough
      2. Kite iNAV instrument           — true live AMC iNAV (source: kite_live)
      3. Nippon / Zerodha / Mirae / Motilal AMC — live AMC APIs where available
         (source: nippon_amc_live / zerodha_amc_live / mirae_amc_live / motilal_amc_live)
      4. NSE /api/etf fallback          — PREVIOUS DAY's declared NAV only
         (source: nse_prev_nav). Note: for US/HK-market ETFs (MON100, MONQ50,
         MAFANG, MASPTOP50, MAHKTECH) this is the correct reference — the
         underlying market is closed during Indian trading hours, so there is
         no intraday iNAV to compute.
         Stores the fresh snapshot to DB when ``store_to_db=True``.

    Returns a dict with keys:
        symbol, premium_discount_pct, inav, market_price, snapshot_at, source
    or None if all sources return nothing.
    """
    sym = symbol.strip().upper().replace(".NS", "")

    # During market hours use a tight minute-level window; otherwise use days.
    if max_age_minutes is None:
        max_age_minutes = 2       # default: 2 min — iNAV updates every 15s, keep data fresh
    if _is_market_open():
        age_clause = f"snapshot_at >= now() - INTERVAL {max_age_minutes} MINUTE"
        threshold_desc = f"{max_age_minutes} min"
    else:
        age_clause = f"snapshot_at >= now() - INTERVAL {max_age_days} DAY"
        threshold_desc = f"{max_age_days} days"

    # ── 1. Try ClickHouse ─────────────────────────────────────────────────
    try:
        from src.db.pool import get_pool
        pool = get_pool()
        rows_db = pool.query_df(
            f"""
            SELECT premium_discount_pct, inav, market_price, snapshot_at
            FROM market_data.inav_snapshots FINAL
            WHERE symbol = '{sym}'
              AND {age_clause}
            ORDER BY snapshot_at DESC
            LIMIT 1
            """
        )
        if not rows_db.empty:
            r = rows_db.iloc[0]
            logger.debug("get_latest_inav: %s found in DB (within %s)", sym, threshold_desc)
            return {
                "symbol":               sym,
                "premium_discount_pct": float(r["premium_discount_pct"]),
                "inav":                 float(r["inav"]),
                "market_price":         float(r["market_price"]),
                "snapshot_at":          r["snapshot_at"],
                "source":               "db",
            }
        logger.info(
            "get_latest_inav: %s DB snapshot older than %s — fetching live",
            sym, threshold_desc,
        )
    except Exception as exc:
        logger.warning("get_latest_inav: DB lookup failed for %s: %s", sym, exc)

    # ── 2. Kite iNAV instrument (true live iNAV when Kite is configured) ─
    # Kite exposes iNAV as non-tradeable NSE instruments (GOLDBEINAV, SILVERINAV…)
    # These are the real AMC-published intraday iNAV values, updated every ~15s.
    try:
        from src.data_importer.fetchers.kite_inav_fetcher import fetch_inav_kite
        kite_rows = fetch_inav_kite([sym])
        if kite_rows:
            row = kite_rows[0]
            if store_to_db:
                try:
                    from config.settings import settings
                    from src.data_importer.clickhouse import ClickHouseImporter
                    ch = ClickHouseImporter(
                        host=settings.clickhouse_host, port=settings.clickhouse_port,
                        database=settings.clickhouse_database,
                        username=settings.clickhouse_user, password=settings.clickhouse_password,
                    )
                    try:
                        ch.insert_inav_snapshots([row])
                    finally:
                        ch.close()
                except Exception:
                    pass
            return {
                "symbol":               sym,
                "premium_discount_pct": row["premium_discount_pct"],
                "inav":                 row["inav"],
                "market_price":         row["market_price"],
                "snapshot_at":          row["snapshot_at"],
                "source":               "kite_live",
            }
    except Exception as exc:
        logger.debug("get_latest_inav: Kite iNAV unavailable for %s: %s", sym, exc)

    # ── 2.5 Nippon AMC Live iNAV (fallback for Nippon-managed ETFs) ──
    try:
        from src.data_importer.fetchers.nippon_inav_fetcher import NIPPON_SYMBOL_MAP
        if sym in NIPPON_SYMBOL_MAP:
            from src.data_importer.fetchers.nippon_inav_fetcher import fetch_inav_nippon
            nippon_rows = fetch_inav_nippon([sym])
            if nippon_rows:
                row = nippon_rows[0]
                if store_to_db:
                    try:
                        from config.settings import settings
                        from src.data_importer.clickhouse import ClickHouseImporter
                        ch = ClickHouseImporter(
                            host=settings.clickhouse_host, port=settings.clickhouse_port,
                            database=settings.clickhouse_database,
                            username=settings.clickhouse_user, password=settings.clickhouse_password,
                        )
                        try:
                            ch.insert_inav_snapshots([row])
                            logger.info("get_latest_inav: stored fresh Nippon AMC snapshot for %s", sym)
                        finally:
                            ch.close()
                    except Exception:
                        pass
                return {
                    "symbol":               sym,
                    "premium_discount_pct": row["premium_discount_pct"],
                    "inav":                 row["inav"],
                    "market_price":         row["market_price"],
                    "snapshot_at":          row["snapshot_at"],
                    "source":               "nippon_amc_live",
                }
    except Exception as exc:
        logger.debug("get_latest_inav: Nippon AMC live fetch failed for %s: %s", sym, exc)

    # ── 2.7 Zerodha AMC Live iNAV (fallback for Zerodha-managed ETFs) ──
    try:
        from src.data_importer.fetchers.zerodha_inav_fetcher import ZERODHA_SYMBOLS
        if sym in ZERODHA_SYMBOLS:
            from src.data_importer.fetchers.zerodha_inav_fetcher import fetch_inav_zerodha
            zerodha_rows = fetch_inav_zerodha([sym])
            if zerodha_rows:
                row = zerodha_rows[0]
                if store_to_db:
                    try:
                        from config.settings import settings
                        from src.data_importer.clickhouse import ClickHouseImporter
                        ch = ClickHouseImporter(
                            host=settings.clickhouse_host, port=settings.clickhouse_port,
                            database=settings.clickhouse_database,
                            username=settings.clickhouse_user, password=settings.clickhouse_password,
                        )
                        try:
                            ch.insert_inav_snapshots([row])
                            logger.info("get_latest_inav: stored fresh Zerodha AMC snapshot for %s", sym)
                        finally:
                            ch.close()
                    except Exception:
                        pass
                return {
                    "symbol":               sym,
                    "premium_discount_pct": row["premium_discount_pct"],
                    "inav":                 row["inav"],
                    "market_price":         row["market_price"],
                    "snapshot_at":          row["snapshot_at"],
                    "source":               "zerodha_amc_live",
                }
    except Exception as exc:
        logger.debug("get_latest_inav: Zerodha AMC live fetch failed for %s: %s", sym, exc)

    # ── 2.9 Mirae AMC Live iNAV (fallback for Mirae-managed ETFs) ──
    try:
        from src.data_importer.fetchers.mirae_inav_fetcher import MIRAE_SYMBOLS
        if sym in MIRAE_SYMBOLS:
            from src.data_importer.fetchers.mirae_inav_fetcher import fetch_inav_mirae
            mirae_rows = fetch_inav_mirae([sym])
            if mirae_rows:
                row = mirae_rows[0]
                if store_to_db:
                    try:
                        from config.settings import settings
                        from src.data_importer.clickhouse import ClickHouseImporter
                        ch = ClickHouseImporter(
                            host=settings.clickhouse_host, port=settings.clickhouse_port,
                            database=settings.clickhouse_database,
                            username=settings.clickhouse_user, password=settings.clickhouse_password,
                        )
                        try:
                            ch.insert_inav_snapshots([row])
                            logger.info("get_latest_inav: stored fresh Mirae AMC snapshot for %s", sym)
                        finally:
                            ch.close()
                    except Exception:
                        pass
                return {
                    "symbol":               sym,
                    "premium_discount_pct": row["premium_discount_pct"],
                    "inav":                 row["inav"],
                    "market_price":         row["market_price"],
                    "snapshot_at":          row["snapshot_at"],
                    "source":               "mirae_amc_live",
                }
    except Exception as exc:
        logger.debug("get_latest_inav: Mirae AMC live fetch failed for %s: %s", sym, exc)

    # ── 2.95 Motilal Oswal AMC Live iNAV (MON100, MONQ50) ──
    try:
        from src.data_importer.fetchers.motilal_inav_fetcher import MOTILAL_SYMBOLS
        if sym in MOTILAL_SYMBOLS:
            from src.data_importer.fetchers.motilal_inav_fetcher import fetch_inav_motilal
            motilal_rows = fetch_inav_motilal([sym])
            if motilal_rows:
                row = motilal_rows[0]
                if store_to_db:
                    try:
                        from config.settings import settings
                        from src.data_importer.clickhouse import ClickHouseImporter
                        ch = ClickHouseImporter(
                            host=settings.clickhouse_host, port=settings.clickhouse_port,
                            database=settings.clickhouse_database,
                            username=settings.clickhouse_user, password=settings.clickhouse_password,
                        )
                        try:
                            ch.insert_inav_snapshots([row])
                            logger.info("get_latest_inav: stored fresh Motilal AMC snapshot for %s", sym)
                        finally:
                            ch.close()
                    except Exception:
                        pass
                return {
                    "symbol":               sym,
                    "premium_discount_pct": row["premium_discount_pct"],
                    "inav":                 row["inav"],
                    "market_price":         row["market_price"],
                    "snapshot_at":          row["snapshot_at"],
                    "source":               "motilal_amc_live",
                }
    except Exception as exc:
        logger.debug("get_latest_inav: Motilal AMC live fetch failed for %s: %s", sym, exc)

    # ── 4. Fallback: NSE /api/etf (returns PREVIOUS DAY's declared NAV) ──
    # NSE's public ETF API (/api/etf) only exposes the prior day's AMC-declared NAV.
    # The /api/quote-equity endpoint (true live iNAV) is Akamai-protected.
    # For US/HK-market ETFs (MON100, MONQ50, MAFANG, MASPTOP50, MAHKTECH) the
    # overseas market is CLOSED during Indian trading hours — the Motilal API
    # returns the prior-day close adjusted for current USDINR (step 2.95 above).
    logger.info("get_latest_inav: %s not in DB or stale — calling NSE API (prev-day NAV)", sym)
    live_rows = fetch_inav_snapshots([sym])
    if not live_rows:
        logger.warning("get_latest_inav: NSE API returned nothing for %s", sym)
        return None

    row = live_rows[0]

    # ── 3. Persist snapshot so next caller finds it in DB ─────────────────
    if store_to_db:
        try:
            from config.settings import settings
            from src.data_importer.clickhouse import ClickHouseImporter
            ch = ClickHouseImporter(
                host=settings.clickhouse_host,
                port=settings.clickhouse_port,
                database=settings.clickhouse_database,
                username=settings.clickhouse_user,
                password=settings.clickhouse_password,
            )
            try:
                ch.insert_inav_snapshots([row])
                logger.info("get_latest_inav: stored fresh snapshot for %s", sym)
            finally:
                ch.close()
        except Exception as exc:
            logger.warning("get_latest_inav: failed to store snapshot for %s: %s", sym, exc)

    return {
        "symbol":               sym,
        "premium_discount_pct": row["premium_discount_pct"],
        "inav":                 row["inav"],
        "market_price":         row["market_price"],
        "snapshot_at":          row["snapshot_at"],
        "source":               "nse_prev_nav",
    }
