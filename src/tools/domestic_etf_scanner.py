"""
src/tools/domestic_etf_scanner.py
───────────────────────────────────
Premium / Discount scanner for domestic Indian ETFs.

Background
──────────
Unlike international ETFs, domestic ETFs are **not** constrained by the RBI
$7B overseas cap.  Arbitrage desks actively close mispricing, so premiums and
discounts are smaller and more transient.  The signal is therefore
inverted compared to international ETFs:

  • Large PREMIUM  → ETF is overpriced vs underlying → potential SELL / avoid
  • Large DISCOUNT → ETF is cheap vs underlying       → potential BUY

Signal logic
────────────
  z_score = (latest_premium − mean_Nd) / std_Nd

  Premium alerts (ETF is expensive):
    z ≥ +1.5   → 🔴 HIGH PREMIUM   (consider waiting or selling)
    z ≥ +1.0   → 🟡 MILD PREMIUM   (monitor)

  Discount alerts (ETF is cheap):
    z ≤ −1.5   → 🟢 GOOD DISCOUNT  (potential entry vs direct stock)
    z ≤ −1.0   → 🟡 MILD DISCOUNT  (monitor)

  otherwise  → ⚪ FAIR VALUE

Public API
──────────
    scan_domestic_etfs(
        ch_client,
        symbols       = DOMESTIC_ETF_SYMBOLS,
        lookback_days = 30,
        z_high        = +1.5,
        z_low         = -1.5,
        min_snapshots = 5,
    ) -> list[dict]

Return schema (one dict per symbol)
────────────────────────────────────
  {
    "symbol":          str,
    "latest_premium":  float | None,
    "mean_premium":    float | None,
    "std_premium":     float | None,
    "z_score":         float | None,
    "n_snapshots":     int,
    "signal":          str,
    "signal_style":    str,            # Rich markup colour
    "error":           str | None,
  }
"""
from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Any

log = logging.getLogger(__name__)

# ── Tax classification (Budget July 23 2024) ────────────────────────────────
# equity    → STCG 20.8%  (20% base + 4% cess); LTCG 13.0% (12.5% + cess)
# commodity → taxed at income-tax slab rate  (Gold, Silver ETFs)
# debt      → taxed at income-tax slab rate  (Liquid, Gilt ETFs)
ETF_TAX_CLASS: dict[str, str] = {
    "GOLDBEES":   "commodity",
    "SILVERBEES": "commodity",
    "LIQUIDBEES": "debt",
    "LIQUIDCASE": "debt",
    "GILT5YBEES": "debt",
}
# Symbols not in the map above default to "equity"

# Estimated round-trip transaction cost as % of trade value (conservative):
#   brokerage ~0.05% + STT sell 0.001% + exchange/SEBI ~0.005% + stamp duty 0.015%
ROUND_TRIP_COST_PCT: float = 0.10

# Post-July-2024 STCG rates (inclusive of 4% cess)
STCG_EQUITY_RATE: float = 0.208   # 20% + cess
STCG_SLAB_20_RATE: float = 0.208  # 20% slab + cess
STCG_SLAB_30_RATE: float = 0.312  # 30% slab + cess

# LTCG rates (> 12 months holding; ₹1.25 L/year exemption)
LTCG_EQUITY_RATE: float = 0.130   # 12.5% + cess
LTCG_COMMODITY_RATE: float = 0.208  # 20% + cess (indexed)

# Domestic ETFs ranked by liquidity (avg daily volume)
DOMESTIC_ETF_SYMBOLS: list[str] = [
    # ── Broad market ──────────────────────────────────────────────────────────
    "NIFTYBEES",    # #1  Nifty 50             ~1.5 Cr shares/day  AUM ₹60,796 Cr
    "SILVERBEES",   # #3  Physical Silver       ~4.2 Cr shares/day  AUM ₹31,712 Cr
    "GOLDBEES",     # #4  Physical Gold         ~4.2 Cr shares/day  AUM ₹59,007 Cr
    "LIQUIDBEES",   # #5  Liquid / Cash         ~58L shares/day     AUM ₹11,903 Cr
    "LIQUIDCASE",   # #2  1D Rate Liquid        ~96L shares/day     AUM  ₹8,529 Cr
    "CPSEETF",      # #6  PSU / Govt            ~25L shares/day     AUM ₹60,188 Cr
    "BANKBEES",     # #7  Banking               ~18L shares/day     AUM ₹10,724 Cr
    "ITBEES",       # #8  Technology / IT       ~15L shares/day     AUM ₹23,086 Cr
    "JUNIORBEES",   # #9  Nifty Next 50         ~12L shares/day     AUM  ₹7,088 Cr
    "SETFNIF50",    # #10 Nifty 50 Institutional ~8L shares/day     AUM ₹2,05,595 Cr
    "MID150BEES",   # #11 Mid Cap (Nifty 150)    ~6L shares/day     AUM  ₹4,200 Cr
    "PSUBNKBEES",   # #12 PSU Banks              ~5L shares/day     AUM  ₹3,950 Cr
    "ICICIB22",     # #13 Bharat 22              ~4L shares/day     AUM ₹21,692 Cr
    "MONIFTY500",   # #14 Multi-Cap Nifty 500    ~3L shares/day     AUM  ₹2,740 Cr
    "GILT5YBEES",   # #16 Govt Securities 5Y   ~1.5L shares/day     AUM  ₹3,201 Cr
    "PHARMABEES",   # #17 Pharma               ~1.2L shares/day     AUM  ₹2,450 Cr
    "AUTOBEES",     # #18 Automobile             ~1L shares/day     AUM  ₹2,120 Cr
    "FMCGIETF",     # #19 FMCG (NSE: FMCGIETF) ~90k shares/day     AUM  ₹2,510 Cr
    "SMALL250",     # #20 Small Cap              ~85k shares/day    AUM  ₹2,280 Cr
    # ── Additional Nifty 50 trackers ─────────────────────────────────────────
    "HDFCNIFTY",    # Nifty 50 (HDFC AMC)
]

_MIN_SNAPSHOTS_DEFAULT = 5

# ── OU + cost/tax helpers ────────────────────────────────────────────────────

_DEFAULT_HORIZON_DAYS = 10  # forward-looking window for OU expected reversion


def _get_stcg_rate(tax_class: str) -> float:
    """Return the applicable STCG rate for a given tax classification."""
    if tax_class == "equity":
        return STCG_EQUITY_RATE
    # commodity and debt: slab rate — use 30% + cess as conservative default
    return STCG_SLAB_30_RATE


def _get_ltcg_rate(tax_class: str) -> float:
    """Return the applicable LTCG rate for a given tax classification."""
    if tax_class == "equity":
        return LTCG_EQUITY_RATE
    return LTCG_COMMODITY_RATE


def _apply_cost_tax_filter(result: dict[str, Any]) -> None:
    """
    Enrich a scanner result dict with net P&L after cost and tax.

    Adds:
      - expected_gross_pct    : raw OU (or naive) expected reversion
      - net_pnl_stcg_pct     : net after round-trip costs + STCG
      - net_pnl_ltcg_pct     : net after round-trip costs + LTCG
      - breakeven_gross_pct  : minimum gross gain to be STCG-profitable
      - is_profitable_after_costs : True if net_pnl_stcg_pct > 0
    """
    rev = result.get("expected_reversion_pct")
    if rev is None:
        return

    tax_class = result.get("tax_class", "equity")
    stcg = _get_stcg_rate(tax_class)
    ltcg = _get_ltcg_rate(tax_class)

    gross = abs(rev)
    net_stcg = gross * (1 - stcg) - ROUND_TRIP_COST_PCT
    net_ltcg = gross * (1 - ltcg) - ROUND_TRIP_COST_PCT
    breakeven = ROUND_TRIP_COST_PCT / (1 - stcg) if stcg < 1 else float("inf")

    result["expected_gross_pct"]         = round(gross, 4)
    result["net_pnl_stcg_pct"]           = round(net_stcg, 4)
    result["net_pnl_ltcg_pct"]           = round(net_ltcg, 4)
    result["breakeven_gross_pct"]        = round(breakeven, 4)
    result["is_profitable_after_costs"]  = net_stcg > 0


def scan_domestic_etfs(
    ch_client: Any,
    symbols: list[str] | None = None,
    lookback_days: int = 30,
    z_high: float = 1.5,
    z_low: float = -1.5,
    z_mild_high: float = 1.0,
    z_mild_low: float = -1.0,
    min_snapshots: int = _MIN_SNAPSHOTS_DEFAULT,
) -> list[dict[str, Any]]:
    """
    Compute iNAV premium/discount Z-scores for domestic ETFs and classify signals.

    Parameters
    ----------
    ch_client     : clickhouse_connect client (already connected)
    symbols       : NSE symbols to scan (default: DOMESTIC_ETF_SYMBOLS)
    lookback_days : historical window for mean/std calculation
    z_high        : z ≥ this → HIGH PREMIUM alert
    z_low         : z ≤ this → GOOD DISCOUNT alert
    z_mild_high   : z ≥ this (< z_high) → MILD PREMIUM
    z_mild_low    : z ≤ this (> z_low)  → MILD DISCOUNT
    min_snapshots : minimum hourly buckets required

    Returns
    -------
    list of result dicts sorted by z_score descending (highest premium first),
    with discounts last; insufficient/error rows appended at end.
    """
    import statistics
    from collections import defaultdict

    if symbols is None:
        symbols = DOMESTIC_ETF_SYMBOLS

    cutoff = (date.today() - timedelta(days=lookback_days)).isoformat()
    results: list[dict[str, Any]] = []

    # ── Batch query 1: latest premium for all symbols in one round-trip ──────
    in_list = ", ".join(f"'{s}'" for s in symbols)
    try:
        _latest = ch_client.query(
            f"SELECT symbol, argMax(premium_discount_pct, snapshot_at) AS premium "
            f"FROM market_data.inav_snapshots "
            f"WHERE symbol IN ({in_list}) "
            f"GROUP BY symbol"
        ).result_rows
        latest_map: dict[str, float] = {
            row[0]: float(row[1]) for row in _latest if row[1] is not None
        }
    except Exception as exc:
        log.warning("domestic_etf_scanner batch-latest query failed: %s", exc)
        latest_map = {}

    # ── Batch query 2: historical hourly premiums for all symbols ────────────
    try:
        _hist = ch_client.query(
            f"SELECT symbol, toStartOfHour(snapshot_at) AS hour_bucket, "
            f"       argMax(premium_discount_pct, snapshot_at) AS premium "
            f"FROM market_data.inav_snapshots "
            f"WHERE symbol IN ({in_list}) "
            f"  AND snapshot_at >= toDateTime('{cutoff} 00:00:00') "
            f"GROUP BY symbol, hour_bucket "
            f"ORDER BY symbol, hour_bucket ASC"
        ).result_rows
        hist_map: dict[str, list[float]] = defaultdict(list)
        for row in _hist:
            hist_map[row[0]].append(float(row[2]))
    except Exception as exc:
        log.warning("domestic_etf_scanner batch-hist query failed: %s", exc)
        hist_map = defaultdict(list)

    for sym in symbols:
        result: dict[str, Any] = {
            "symbol":                 sym,
            "latest_premium":         None,
            "mean_premium":           None,
            "std_premium":            None,
            "z_score":                None,
            "raw_z_score":            None,
            "n_snapshots":            0,
            "signal":                 "⚠ Insufficient Data",
            "signal_style":           "dim",
            "tax_class":              ETF_TAX_CLASS.get(sym.upper(), "equity"),
            "expected_reversion_pct": None,
            "ou_available":           False,
            "theta":                  None,
            "ou_mu":                  None,
            "half_life_days":         None,
            "prob_revert_10d":        None,
            "expected_premium_5d":    None,
            "expected_premium_10d":   None,
            "horizon_days":           _DEFAULT_HORIZON_DAYS,
            "error":                  None,
        }

        try:
            if sym not in latest_map:
                result["error"] = "No snapshot found"
                results.append(result)
                continue

            latest_prem = latest_map[sym]
            result["latest_premium"] = round(latest_prem, 4)

            premiums = hist_map.get(sym, [])
            n = len(premiums)
            result["n_snapshots"] = n

            if n < min_snapshots:
                result["error"] = f"Only {n} snapshots (need ≥ {min_snapshots})"
                results.append(result)
                continue
            mean_prem = statistics.mean(premiums)
            std_prem  = statistics.stdev(premiums) if n >= 2 else 0.0

            result["mean_premium"] = round(mean_prem, 4)
            result["std_premium"]  = round(std_prem, 4)

            if std_prem < 1e-8:
                # Flat premium — market holiday or no intraday movement.
                result["signal"]       = "⚪ FLAT PREMIUM"
                result["signal_style"] = "dim"
                result["error"]        = "Spread is constant — likely a market holiday or stale iNAV"
                results.append(result)
                continue

            # ── Z-score and signal classification ────────────────────────────
            z = (latest_prem - mean_prem) / std_prem
            result["z_score"] = round(z, 3)
            result["raw_z_score"] = round(z, 3)

            if z >= z_high:
                result["signal"]       = "🔴 HIGH PREMIUM"
                result["signal_style"] = "bold red"
            elif z >= z_mild_high:
                result["signal"]       = "🟡 MILD PREMIUM"
                result["signal_style"] = "bold yellow"
            elif z <= z_low:
                result["signal"]       = "🟢 GOOD DISCOUNT"
                result["signal_style"] = "bold green"
            elif z <= z_mild_low:
                result["signal"]       = "🟡 MILD DISCOUNT"
                result["signal_style"] = "bold yellow"
            else:
                result["signal"]       = "⚪ FAIR VALUE"
                result["signal_style"] = "dim"

            # ── OU-adjusted expected reversion (with graceful fallback) ───────
            from src.db.repository import MarketDataRepository
            from src.db.pool import get_pool as _get_ou_pool
            from src.ml.ou_estimator import expected_reversion, expected_premium, prob_revert

            ou = MarketDataRepository(_get_ou_pool()).ou_state(sym)
            if ou is not None:
                from datetime import datetime
                fit_age = (date.today() - date.fromisoformat(ou["fit_date"])).days
                if fit_age <= 7:
                    result["ou_available"]        = True
                    result["theta"]               = ou["theta"]
                    result["ou_mu"]               = ou["mu"]
                    result["half_life_days"]      = ou["half_life_days"]
                    result["expected_reversion_pct"] = round(
                        expected_reversion(latest_prem, ou["theta"], ou["mu"], _DEFAULT_HORIZON_DAYS), 4
                    )
                    result["expected_premium_5d"]  = round(
                        expected_premium(latest_prem, ou["theta"], ou["mu"], 5), 4
                    )
                    result["expected_premium_10d"] = round(
                        expected_premium(latest_prem, ou["theta"], ou["mu"], 10), 4
                    )
                    result["prob_revert_10d"]      = round(
                        prob_revert(latest_prem, ou["theta"], ou["mu"], ou["sigma"], ou["mu"], 10), 4
                    )
                else:
                    # OU state is stale — fall back to naive
                    result["expected_reversion_pct"] = round(mean_prem - latest_prem, 4)
            else:
                # No OU state available — fall back to naive
                result["expected_reversion_pct"] = round(mean_prem - latest_prem, 4)

            # ── Cost / tax filter ─────────────────────────────────────────────
            _apply_cost_tax_filter(result)

            # Downgrade signal if OU says trade is unprofitable after costs
            if result.get("ou_available") and not result.get("is_profitable_after_costs", True):
                if result["signal"] == "🟢 GOOD DISCOUNT":
                    result["signal"]       = "⚪ UNPROFITABLE"
                    result["signal_style"] = "dim"

        except Exception as exc:
            result["error"]        = str(exc)
            result["signal"]       = "❌ Error"
            result["signal_style"] = "bold red"
            log.warning("domestic_etf_scanner error for %s: %s", sym, exc)

        results.append(result)

    # Sort: highest premium first (most overpriced → most discounted), errors last
    def _sort_key(r: dict) -> tuple:
        z = r["z_score"]
        return (0, -(z if z is not None else 0.0)) if z is not None else (1, 0.0)

    results.sort(key=_sort_key)
    return results


def log_signals_to_db(results: list[dict[str, Any]], ch_client: Any, source: str = "domestic_scanner") -> int:
    """
    Write scanner results to premium_signal_log for paper-trade tracking.

    Returns the number of rows logged.
    """
    from datetime import date as _date
    today_str = _date.today().isoformat()
    logged = 0
    for r in results:
        if r.get("z_score") is None:
            continue
        try:
            ch_client.execute(
                f"INSERT INTO market_data.premium_signal_log "
                f"(as_of, symbol, current_prem, ou_mu, half_life_days, "
                f"expected_reversion_pct, net_pnl_stcg_pct, action, "
                f"ou_available, is_profitable_after_costs, signal_source) VALUES "
                f"('{today_str}', '{r['symbol']}', "
                f"{r.get('latest_premium') or 0}, "
                f"{r.get('ou_mu') or 0}, "
                f"{r.get('half_life_days') or 0}, "
                f"{r.get('expected_reversion_pct') or 0}, "
                f"{r.get('net_pnl_stcg_pct') or 0}, "
                f"'{r.get('signal', '')}', "
                f"{1 if r.get('ou_available') else 0}, "
                f"{1 if r.get('is_profitable_after_costs') else 0}, "
                f"'{source}')"
            )
            logged += 1
        except Exception as exc:
            log.warning("Failed to log signal for %s: %s", r.get("symbol"), exc)
    return logged
