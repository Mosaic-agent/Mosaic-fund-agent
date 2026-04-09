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

    if symbols is None:
        symbols = DOMESTIC_ETF_SYMBOLS

    cutoff = (date.today() - timedelta(days=lookback_days)).isoformat()
    results: list[dict[str, Any]] = []

    for sym in symbols:
        result: dict[str, Any] = {
            "symbol":                 sym,
            "latest_premium":         None,
            "mean_premium":           None,
            "std_premium":            None,
            "z_score":                None,
            "n_snapshots":            0,
            "signal":                 "⚠ Insufficient Data",
            "signal_style":           "dim",
            "tax_class":              ETF_TAX_CLASS.get(sym.upper(), "equity"),
            "expected_reversion_pct": None,
            "error":                  None,
        }

        try:
            # ── Historical premium in hourly buckets ──────────────────────────
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

            # ── Latest premium ────────────────────────────────────────────────
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

            # ── Z-score and signal classification ────────────────────────────
            z = (latest_prem - mean_prem) / std_prem
            result["z_score"] = round(z, 3)

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

            # Expected mean-reversion gain (positive → upside for discount signals)
            result["expected_reversion_pct"] = round(mean_prem - latest_prem, 4)

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
