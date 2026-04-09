"""
src/tools/premium_alerts.py
────────────────────────────
Scarcity Premium Alert engine for international ETFs listed on the NSE.

Background
──────────
The RBI imposes a $7 billion industry-wide limit on overseas MF investments.
This creates a structural premium on international ETFs (MAFANG, HNGSNGBEES,
etc.) that rarely reverts to zero in a bull market.

Strategy: trade the *volatility of the premium*, not the premium level itself.
  • When the premium dips well below its historical mean → likely reversion → BUY
  • When the premium is near or above its mean → avoid / hold

Signal logic
────────────
  z_score = (latest_premium − mean_30d) / std_30d

  z ≤ −1.5   → 🟢 SCREAMING BUY   (deep dip, premium likely to snap back)
  z ≤ −1.0   → 🟡 GOOD ENTRY       (moderate dip, favourable risk/reward)
  otherwise  → 🔴 NO ACTION        (premium near or above average)

Public API
──────────
    check_premium_alerts(
        ch_client,
        symbols           = INTL_ETF_SYMBOLS,
        lookback_days     = 30,
        z_threshold       = -1.5,
        good_entry_threshold = -1.0,
        min_snapshots     = 5,
    ) -> list[dict]

Return schema (one dict per symbol)
────────────────────────────────────
  {
    "symbol":          str,
    "latest_premium":  float | None,   # % — positive = premium over iNAV
    "mean_premium":    float | None,   # rolling mean over lookback_days
    "std_premium":     float | None,   # rolling std  over lookback_days
    "z_score":         float | None,
    "n_snapshots":     int,
    "action":          str,            # signal label
    "action_style":    str,            # Rich markup colour class
    "error":           str | None,
  }
"""
from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Any

log = logging.getLogger(__name__)

# International ETFs affected by the RBI overseas investment cap
INTL_ETF_SYMBOLS: list[str] = [
    "MAFANG",       # Mirae Asset NYSE FANG+ ETF
    "HNGSNGBEES",   # Nippon Hang Seng BeES
    "MAHKTECH",     # Mirae Asset Hang Seng TECH ETF
    "MON100",       # Motilal Oswal NASDAQ 100 ETF
    "MASPTOP50",    # Mirae Asset S&P 500 Top 50 ETF
]

_MIN_SNAPSHOTS_DEFAULT = 5


def check_premium_alerts(
    ch_client: Any,
    symbols: list[str] | None = None,
    lookback_days: int = 30,
    z_threshold: float = -1.5,
    good_entry_threshold: float = -1.0,
    min_snapshots: int = _MIN_SNAPSHOTS_DEFAULT,
) -> list[dict[str, Any]]:
    """
    Compute iNAV premium Z-scores for each symbol and generate action signals.

    Parameters
    ----------
    ch_client             : clickhouse_connect client (already connected)
    symbols               : NSE symbols to scan (default: INTL_ETF_SYMBOLS)
    lookback_days         : historical window for mean/std calculation
    z_threshold           : z ≤ this → SCREAMING BUY
    good_entry_threshold  : z ≤ this (and > z_threshold) → GOOD ENTRY
    min_snapshots         : minimum hourly buckets required to compute std

    Returns
    -------
    list of result dicts sorted by z_score ascending (best opportunities first);
    symbols with errors or insufficient data are appended at the end.
    """
    import statistics

    if symbols is None:
        symbols = INTL_ETF_SYMBOLS

    cutoff = (date.today() - timedelta(days=lookback_days)).isoformat()
    results: list[dict[str, Any]] = []

    for sym in symbols:
        result: dict[str, Any] = {
            "symbol":         sym,
            "latest_premium": None,
            "mean_premium":   None,
            "std_premium":    None,
            "z_score":        None,
            "n_snapshots":    0,
            "action":         "⚠ Insufficient Data",
            "action_style":   "dim",
            "error":          None,
        }

        try:
            # ── Historical premium: deduplicated into hourly buckets ───────────
            hist_rows = ch_client.query(f"""
                SELECT
                    toStartOfHour(snapshot_at)                AS hour_bucket,
                    argMax(premium_discount_pct, snapshot_at) AS premium
                FROM market_data.inav_snapshots
                WHERE symbol = '{sym}'
                  AND snapshot_at >= toDateTime('{cutoff} 00:00:00')
                GROUP BY hour_bucket
                ORDER BY hour_bucket ASC
            """).result_rows

            n = len(hist_rows)
            result["n_snapshots"] = n

            if n < min_snapshots:
                result["error"] = f"Only {n} snapshots (need ≥ {min_snapshots})"
                results.append(result)
                continue

            premiums = [float(r[1]) for r in hist_rows]
            mean_prem = statistics.mean(premiums)
            std_prem  = statistics.stdev(premiums) if n >= 2 else 0.0

            result["mean_premium"] = round(mean_prem, 4)
            result["std_premium"]  = round(std_prem, 4)

            if std_prem < 1e-8:
                result["error"] = "Std ≈ 0 (premium is perfectly flat)"
                results.append(result)
                continue

            # ── Latest premium: most recent snapshot regardless of date ────────
            latest_rows = ch_client.query(f"""
                SELECT argMax(premium_discount_pct, snapshot_at) AS premium
                FROM market_data.inav_snapshots
                WHERE symbol = '{sym}'
            """).result_rows

            if not latest_rows or latest_rows[0][0] is None:
                result["error"] = "No snapshot found"
                results.append(result)
                continue

            latest_prem = float(latest_rows[0][0])
            result["latest_premium"] = round(latest_prem, 4)

            # ── Z-score and action signal ─────────────────────────────────────
            z = (latest_prem - mean_prem) / std_prem
            result["z_score"] = round(z, 3)

            if z <= z_threshold:
                result["action"]       = "🟢 SCREAMING BUY"
                result["action_style"] = "bold green"
            elif z <= good_entry_threshold:
                result["action"]       = "🟡 GOOD ENTRY"
                result["action_style"] = "bold yellow"
            else:
                result["action"]       = "🔴 NO ACTION"
                result["action_style"] = "red"

        except Exception as exc:
            result["error"]        = str(exc)
            result["action"]       = "❌ Error"
            result["action_style"] = "bold red"
            log.warning("premium_alerts error for %s: %s", sym, exc)

        results.append(result)

    # Sort: actionable signals (lowest z) first; no-data / errors last
    def _sort_key(r: dict) -> tuple:
        z = r["z_score"]
        return (0, z) if z is not None else (1, 0.0)

    results.sort(key=_sort_key)
    return results
