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


def _remove_outliers(values: list[float], iqr_multiplier: float = 3.0) -> tuple[list[float], int]:
    """Return (cleaned_values, n_removed) using the IQR fence method."""
    if len(values) < 4:
        return values, 0
    sorted_v = sorted(values)
    n = len(sorted_v)
    q1 = sorted_v[n // 4]
    q3 = sorted_v[(3 * n) // 4]
    iqr = q3 - q1
    lo = q1 - iqr_multiplier * iqr
    hi = q3 + iqr_multiplier * iqr
    cleaned = [v for v in values if lo <= v <= hi]
    return cleaned, len(values) - len(cleaned)

log = logging.getLogger(__name__)

# International ETFs affected by the RBI overseas investment cap
INTL_ETF_SYMBOLS: list[str] = [
    "MAFANG",
    "HNGSNGBEES",
    "MON100",
    "MASPTOP50",
    "MAHKTECH",
    "MONQ50",
]

_MIN_SNAPSHOTS_DEFAULT = 5

# ── Cost / tax constants (all intl ETFs are equity post-Budget July 2024) ────
ROUND_TRIP_COST_PCT: float = 0.10  # brokerage + STT + exchange + stamp
STCG_EQUITY_RATE: float = 0.208    # 20% + 4% cess
LTCG_EQUITY_RATE: float = 0.130    # 12.5% + cess


def _apply_cost_tax_filter(result: dict[str, Any]) -> None:
    """Enrich result with net P&L after cost and tax (all intl ETFs = equity)."""
    rev = result.get("expected_reversion_pct")
    if rev is None:
        return
    stcg = STCG_EQUITY_RATE
    ltcg = LTCG_EQUITY_RATE
    gross = abs(rev)
    net_stcg = gross * (1 - stcg) - ROUND_TRIP_COST_PCT
    net_ltcg = gross * (1 - ltcg) - ROUND_TRIP_COST_PCT
    breakeven = ROUND_TRIP_COST_PCT / (1 - stcg) if stcg < 1 else float("inf")
    result["expected_gross_pct"]        = round(gross, 4)
    result["net_pnl_stcg_pct"]          = round(net_stcg, 4)
    result["net_pnl_ltcg_pct"]          = round(net_ltcg, 4)
    result["breakeven_gross_pct"]       = round(breakeven, 4)
    result["is_profitable_after_costs"] = net_stcg > 0


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
            "symbol":               sym,
            "latest_premium":       None,
            "mean_premium":         None,
            "std_premium":          None,
            "z_score":              None,
            "raw_z_score":          None,
            "n_snapshots":          0,
            "n_outliers_removed":   0,
            "action":               "⚠ Insufficient Data",
            "action_style":         "dim",
            "tax_class":            "equity",
            "expected_reversion_pct": None,
            "ou_available":         False,
            "theta":                None,
            "ou_mu":                None,
            "half_life_days":       None,
            "prob_revert_10d":      None,
            "expected_premium_5d":  None,
            "expected_premium_10d": None,
            "horizon_days":         10,
            "error":                None,
        }

        try:
            # ── Latest premium: DB first, NSE API fallback ─────────────────
            from src.importer.fetchers.nse_inav_fetcher import get_latest_inav
            live = get_latest_inav(sym, max_age_days=7, store_to_db=True)
            if live is None:
                result["error"] = "No snapshot found in DB or NSE API"
                results.append(result)
                continue

            latest_prem = live["premium_discount_pct"]
            result["latest_premium"] = round(latest_prem, 4)
            result["inav_source"]    = live["source"]  # "db", "kite_live", "nippon_amc_live", "zerodha_amc_live", "mirae_amc_live", "motilal_amc_live", or "nse_prev_nav"

            # ── Historical premium: deduplicated into hourly buckets ───────────
            hist_rows = ch_client.query(
                """
                SELECT
                    toStartOfHour(snapshot_at)                AS hour_bucket,
                    argMax(premium_discount_pct, snapshot_at) AS premium
                FROM market_data.inav_snapshots
                WHERE symbol = {sym:String}
                  AND snapshot_at >= toDateTime({cutoff:String})
                GROUP BY hour_bucket
                ORDER BY hour_bucket ASC
                """,
                parameters={"sym": sym, "cutoff": f"{cutoff} 00:00:00"},
            ).result_rows

            n = len(hist_rows)
            result["n_snapshots"] = n

            if n < min_snapshots:
                result["error"] = f"Only {n} snapshots (need ≥ {min_snapshots})"
                results.append(result)
                continue

            premiums = [float(r[1]) for r in hist_rows]
            premiums, n_removed = _remove_outliers(premiums)
            result["n_outliers_removed"] = n_removed
            if n_removed:
                log.warning("%s: removed %d outlier snapshot(s) from premium history", sym, n_removed)

            if len(premiums) < min_snapshots:
                result["n_snapshots"] = n
                result["error"] = f"Only {len(premiums)} clean snapshots after removing {n_removed} outlier(s) (need ≥ {min_snapshots})"
                results.append(result)
                continue

            mean_prem = statistics.mean(premiums)
            std_prem  = statistics.stdev(premiums) if len(premiums) >= 2 else 0.0

            result["mean_premium"] = round(mean_prem, 4)
            result["std_premium"]  = round(std_prem, 4)

            if std_prem < 1e-8:
                # Flat premium — market holiday or no intraday movement.
                result["action"]       = "⚪ FLAT PREMIUM"
                result["action_style"] = "dim"
                result["error"]        = "Spread is constant — likely a market holiday or stale iNAV"
                results.append(result)
                continue

            # ── Z-score and action signal ─────────────────────────────────────
            result["n_snapshots"] = len(premiums)  # count after outlier removal
            z = (latest_prem - mean_prem) / std_prem
            result["z_score"] = round(z, 3)
            result["raw_z_score"] = round(z, 3)

            if z <= z_threshold:
                result["action"]       = "🟢 SCREAMING BUY"
                result["action_style"] = "bold green"
            elif z <= good_entry_threshold:
                result["action"]       = "🟡 GOOD ENTRY"
                result["action_style"] = "bold yellow"
            else:
                result["action"]       = "🔴 NO ACTION"
                result["action_style"] = "red"

            # ── OU-adjusted expected reversion (with graceful fallback) ───────
            from src.db.repository import MarketDataRepository
            from src.db.pool import get_pool as _get_ou_pool
            from src.ml.ou_estimator import expected_reversion, expected_premium, prob_revert

            ou = MarketDataRepository(_get_ou_pool()).ou_state(sym)
            if ou is not None:
                fit_age = (date.today() - date.fromisoformat(ou["fit_date"])).days
                if fit_age <= 7:
                    result["ou_available"]          = True
                    result["theta"]                 = ou["theta"]
                    result["ou_mu"]                 = ou["mu"]
                    result["half_life_days"]        = ou["half_life_days"]
                    result["expected_reversion_pct"] = round(
                        expected_reversion(latest_prem, ou["theta"], ou["mu"], 10), 4
                    )
                    result["expected_premium_5d"]   = round(
                        expected_premium(latest_prem, ou["theta"], ou["mu"], 5), 4
                    )
                    result["expected_premium_10d"]  = round(
                        expected_premium(latest_prem, ou["theta"], ou["mu"], 10), 4
                    )
                    result["prob_revert_10d"]       = round(
                        prob_revert(latest_prem, ou["theta"], ou["mu"], ou["sigma"], ou["mu"], 10), 4
                    )
                else:
                    # OU state stale — fall back to naive
                    result["expected_reversion_pct"] = round(mean_prem - latest_prem, 4)
            else:
                # No OU state — fall back to naive
                result["expected_reversion_pct"] = round(mean_prem - latest_prem, 4)

            # ── Cost / tax filter ─────────────────────────────────────────────
            _apply_cost_tax_filter(result)

            # Downgrade signal if OU says trade is unprofitable after costs
            if result.get("ou_available") and not result.get("is_profitable_after_costs", True):
                if result["action"] in ("🟢 SCREAMING BUY", "🟡 GOOD ENTRY"):
                    result["action"]       = "⚪ UNPROFITABLE"
                    result["action_style"] = "dim"

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
