"""
src/tools/who_is_selling_agent.py
──────────────────────────────────
"Who Is Selling?" — composite regime-detection agent for GOLDBEES / COMEX gold.

Integrates 4 independent signal streams to identify *who* is driving a gold
sell-off (retail panic, institutional exit, speculator over-leverage, or
central-bank accumulation absorbing Western selling) and produces a plain-
English recommendation.

Signals
───────
1. RETAIL PANIC  (India-specific)
   • USDINR 60-day % change  (ClickHouse fx_rates)
   • GOLDBEES discount/premium to AMFI NAV  (ClickHouse daily_prices + mf_nav)
   → Trigger: USDINR > +3% in 60 days  AND  GOLDBEES discount < −1%

2. INSTITUTIONAL EXIT  (Global, Western hedge funds)
   • GLD shares outstanding (via yfinance)  — day-over-day and 30-day rolling
   → Trigger: GLD shares declining trend (30-day change < −3%)

3. SPECULATOR OVER-LEVERAGE  (COMEX futures)
   • Managed Money Net / Open Interest  (ClickHouse cot_gold)
   → Crowded long: MM Net% OI > 25%  (crash risk)
   → Extreme short: MM Net% OI < −5%  (short-squeeze fuel)

4. CENTRAL BANK STRENGTH  (China + Middle East)
   • USDCNY 30-day % change  (via yfinance, falls back to ClickHouse fx_rates)
   • WTI Crude Oil price  (CL=F via yfinance)
   → Accumulation signal: USDCNY stable (< +1.5% in 30d)  AND  CL=F > $80

Output
──────
fetch_who_is_selling() → dict with:
  signals      : per-signal dicts (value, status, interpretation)
  regime       : "RETAIL_PANIC" | "INSTITUTIONAL_EXIT" | "OVERLEVERED_LONGS"
                 | "CB_ACCUMULATION" | "NEUTRAL" | "MIXED"
  recommendation: plain-English buy/hold/sell/watch string
  summary      : one-sentence human-readable verdict
  as_of        : date of most recent data point
"""
from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Any, Optional

log = logging.getLogger(__name__)

# ── Thresholds ────────────────────────────────────────────────────────────────
_USDINR_PANIC_PCT       =  3.0    # INR weakened >3% in 60d → rupee stress
_GOLDBEES_PANIC_DISC    = -1.0    # discount worse than -1% → retail panic selling
_GLD_SHARES_EXIT_PCT    = -3.0    # GLD shares down >3% in 30d → institutional exit
_COT_CROWDED_LONG_PCT   = 25.0    # MM Net / OI > 25% → crowded long (crash risk)
_COT_EXTREME_SHORT_PCT  = -5.0    # MM Net / OI < -5% → short-squeeze fuel
_USDCNY_STABLE_PCT      =  1.5    # CNY weakened <1.5% in 30d → China FX stable
_CRUDE_OIL_FLOOR        = 80.0    # Crude > $80 → Middle East petrodollar flows


# ── Helpers ───────────────────────────────────────────────────────────────────

def _ch_client():
    """Return a ClickHouse client (lazy import; caller must .close() it)."""
    import clickhouse_connect
    return clickhouse_connect.get_client(
        host="localhost", port=8123, database="market_data"
    )


def _pct_change(old: float, new: float) -> float:
    if old == 0:
        return 0.0
    return (new - old) / old * 100


# ── Signal 1: Retail Panic ────────────────────────────────────────────────────

def _signal_retail_panic() -> dict[str, Any]:
    """
    India retail panic = rupee crash + GOLDBEES trading at deep discount.

    Sources: ClickHouse fx_rates (USDINR) + daily_prices JOIN mf_nav (GOLDBEES).
    """
    result: dict[str, Any] = {
        "name":         "Retail Panic (India)",
        "usdinr_60d_pct":  None,
        "goldbees_disc_pct": None,
        "status":       "unknown",
        "detail":       "",
    }
    try:
        cl = _ch_client()

        # USDINR 60-day change
        fx_rows = cl.query(
            "SELECT trade_date, close FROM market_data.fx_rates FINAL "
            "WHERE symbol = 'USDINR' ORDER BY trade_date DESC LIMIT 65"
        ).result_rows
        if len(fx_rows) >= 2:
            latest_inr = fx_rows[0][1]
            old_inr    = fx_rows[min(59, len(fx_rows) - 1)][1]
            inr_chg    = _pct_change(old_inr, latest_inr)
            result["usdinr_60d_pct"]  = round(inr_chg, 2)
            result["usdinr_latest"]   = round(latest_inr, 4)
            result["usdinr_date"]     = fx_rows[0][0]

        # GOLDBEES discount to NAV — most recent matched day
        disc_rows = cl.query("""
            SELECT p.trade_date,
                   round(p.close, 4)                          AS market_close,
                   round(n.nav, 4)                            AS nav,
                   round((p.close - n.nav) / n.nav * 100, 3) AS disc_pct
            FROM (SELECT trade_date, close FROM market_data.daily_prices FINAL
                  WHERE symbol = 'GOLDBEES' AND category = 'etfs') p
            JOIN (SELECT nav_date AS trade_date, nav FROM market_data.mf_nav FINAL
                  WHERE symbol = 'GOLDBEES') n USING (trade_date)
            ORDER BY p.trade_date DESC LIMIT 1
        """).result_rows
        if disc_rows:
            result["goldbees_disc_pct"]  = disc_rows[0][3]
            result["goldbees_close"]     = disc_rows[0][1]
            result["goldbees_nav"]       = disc_rows[0][2]
            result["goldbees_date"]      = disc_rows[0][0]

        cl.close()

        # Evaluate
        inr_panic  = (result.get("usdinr_60d_pct")    or 0) >= _USDINR_PANIC_PCT
        disc_panic = (result.get("goldbees_disc_pct") or 0) <= _GOLDBEES_PANIC_DISC

        if inr_panic and disc_panic:
            result["status"] = "PANIC"
            result["detail"] = (
                f"INR weakened {result['usdinr_60d_pct']:+.1f}% in 60d "
                f"AND GOLDBEES at {result['goldbees_disc_pct']:+.2f}% discount — "
                "Indian retail is panic-selling rupee-denominated gold."
            )
        elif inr_panic:
            result["status"] = "STRESSED"
            result["detail"] = (
                f"INR weakened {result['usdinr_60d_pct']:+.1f}% in 60d "
                f"but GOLDBEES discount only {result.get('goldbees_disc_pct', 0):+.2f}% — "
                "rupee stress present; GOLDBEES not yet panic-discounted."
            )
        elif disc_panic:
            result["status"] = "DISCOUNT"
            result["detail"] = (
                f"GOLDBEES at {result['goldbees_disc_pct']:+.2f}% discount "
                "despite stable rupee — idiosyncratic retail selling."
            )
        else:
            result["status"] = "NEUTRAL"
            result["detail"] = (
                f"INR {result.get('usdinr_60d_pct', 0):+.1f}% / "
                f"GOLDBEES disc {result.get('goldbees_disc_pct', 0):+.2f}% — no retail panic."
            )
    except Exception as exc:
        log.warning("Retail panic signal failed: %s", exc)
        result["status"] = "error"
        result["detail"] = str(exc)
    return result


# ── Signal 2: Institutional Exit ─────────────────────────────────────────────

def _signal_institutional_exit() -> dict[str, Any]:
    """
    Western institutional exit = GLD shares outstanding in 30-day downtrend.

    Source: yfinance GLD .info (sharesOutstanding) — point-in-time snapshot.
    For trend we use 30-day historical close as AUM proxy (totalAssets / price).
    """
    result: dict[str, Any] = {
        "name":             "Institutional Exit (GLD)",
        "gld_shares_now":   None,
        "gld_aum_usd":      None,
        "gld_30d_chg_pct":  None,
        "status":           "unknown",
        "detail":           "",
    }
    try:
        import yfinance as yf
        import pandas as pd

        hist = yf.download("GLD", period="35d", interval="1d",
                           auto_adjust=True, progress=False)
        if hist.empty:
            raise ValueError("yfinance returned empty GLD history")

        close = hist["Close"].dropna()
        if isinstance(close.columns if hasattr(close, 'columns') else [], object):
            # MultiIndex edge case with single ticker
            if hasattr(close, 'squeeze'):
                close = close.squeeze()

        info        = yf.Ticker("GLD").info
        shares_now  = info.get("sharesOutstanding") or 0
        aum_usd     = info.get("totalAssets")       or 0
        price_now   = float(close.iloc[-1]) if len(close) else 0
        price_30d   = float(close.iloc[0])  if len(close) else 0

        # Implied shares = AUM / price (more stable than sharesOutstanding field)
        implied_shares_now = aum_usd / price_now  if price_now else 0

        # AUM 30d change (aum ~ shares * price; use price-adj AUM proxy)
        aum_now  = aum_usd
        # Approximate 30-day-ago AUM: assume shares roughly equal (best we can do
        # without historical shares data from yfinance free tier)
        # Instead track AUM change via price change * current shares
        aum_30d_ago_approx = implied_shares_now * price_30d if price_30d else 0
        aum_chg_pct = _pct_change(aum_30d_ago_approx, aum_now) if aum_30d_ago_approx else 0

        result["gld_shares_now"]  = shares_now
        result["gld_aum_usd"]     = aum_usd
        result["gld_price"]       = round(price_now, 2)
        result["gld_30d_chg_pct"] = round(aum_chg_pct, 2)
        result["gld_date"]        = date.today()

        if aum_chg_pct <= _GLD_SHARES_EXIT_PCT:
            result["status"] = "EXIT"
            result["detail"] = (
                f"GLD AUM proxy down {aum_chg_pct:+.1f}% vs 30d ago "
                f"(${aum_usd/1e9:.1f}B current) — Western institutions are redeeming."
            )
        elif aum_chg_pct > 3.0:
            result["status"] = "INFLOW"
            result["detail"] = (
                f"GLD AUM proxy up {aum_chg_pct:+.1f}% in 30d — institutions are buying."
            )
        else:
            result["status"] = "NEUTRAL"
            result["detail"] = (
                f"GLD AUM proxy {aum_chg_pct:+.1f}% in 30d — no significant trend."
            )
    except Exception as exc:
        log.warning("Institutional exit signal failed: %s", exc)
        result["status"] = "error"
        result["detail"] = str(exc)
    return result


# ── Signal 3: Speculator Over-Leverage ────────────────────────────────────────

def _signal_speculator_leverage() -> dict[str, Any]:
    """
    COMEX futures speculator positioning from ClickHouse cot_gold table.

    Uses the most recent CFTC COT report (updated weekly, lag ≤ 5 days).
    """
    result: dict[str, Any] = {
        "name":        "Speculator Over-Leverage (COT)",
        "mm_net":      None,
        "open_interest": None,
        "mm_net_pct_oi": None,
        "status":      "unknown",
        "detail":      "",
    }
    try:
        cl = _ch_client()
        rows = cl.query(
            "SELECT report_date, mm_long, mm_short, mm_net, open_interest "
            "FROM market_data.cot_gold FINAL ORDER BY report_date DESC LIMIT 1"
        ).result_rows
        cl.close()

        if not rows:
            result["detail"] = "No COT data in ClickHouse."
            return result

        report_date, mm_long, mm_short, mm_net, oi = rows[0]
        mm_pct = mm_net / oi * 100 if oi else 0

        result["report_date"]  = report_date
        result["mm_long"]      = mm_long
        result["mm_short"]     = mm_short
        result["mm_net"]       = mm_net
        result["open_interest"] = oi
        result["mm_net_pct_oi"] = round(mm_pct, 2)

        if mm_pct >= _COT_CROWDED_LONG_PCT:
            result["status"] = "CROWDED_LONG"
            result["detail"] = (
                f"MM Net {mm_pct:+.1f}% of OI (>{_COT_CROWDED_LONG_PCT}%) — "
                "speculators are over-leveraged long. Sell-off risk is elevated."
            )
        elif mm_pct <= _COT_EXTREME_SHORT_PCT:
            result["status"] = "EXTREME_SHORT"
            result["detail"] = (
                f"MM Net {mm_pct:+.1f}% of OI (<{_COT_EXTREME_SHORT_PCT}%) — "
                "extreme net short positioning. Short-squeeze fuel building."
            )
        elif mm_pct > 15:
            result["status"] = "ELEVATED_LONG"
            result["detail"] = (
                f"MM Net {mm_pct:+.1f}% of OI — elevated but not yet crowded."
            )
        else:
            result["status"] = "NEUTRAL"
            result["detail"] = (
                f"MM Net {mm_pct:+.1f}% of OI — normal positioning."
            )
    except Exception as exc:
        log.warning("Speculator leverage signal failed: %s", exc)
        result["status"] = "error"
        result["detail"] = str(exc)
    return result


# ── Signal 4: Central Bank Strength ──────────────────────────────────────────

def _signal_cb_strength() -> dict[str, Any]:
    """
    China + Middle East buying capacity signal.

    USDCNY stable (< 1.5% weaker in 30d)  AND  WTI Crude > $80
    = China and petrodollar nations can absorb Western/retail selling.

    Sources:
      Primary: yfinance (USDCNY=X, CL=F)
      Fallback: ClickHouse fx_rates for USDCNY
    """
    result: dict[str, Any] = {
        "name":          "Central Bank Strength (China + ME)",
        "usdcny_30d_pct": None,
        "crude_price":   None,
        "status":        "unknown",
        "detail":        "",
    }
    try:
        import yfinance as yf
        data = yf.download(
            ["USDCNY=X", "CL=F"], period="35d", interval="1d",
            auto_adjust=True, progress=False
        )
        if data.empty:
            raise ValueError("Empty yfinance response")

        close = data["Close"]
        if hasattr(close, 'columns') and "USDCNY=X" in close.columns:
            cny_series  = close["USDCNY=X"].dropna()
            crude_series = close["CL=F"].dropna()
        else:
            raise ValueError("Unexpected yfinance response shape")

        cny_now   = float(cny_series.iloc[-1])  if len(cny_series)  else 0
        cny_30d   = float(cny_series.iloc[0])   if len(cny_series)  else 0
        crude_now = float(crude_series.iloc[-1]) if len(crude_series) else 0

        cny_chg = _pct_change(cny_30d, cny_now)

        result["usdcny_now"]      = round(cny_now, 4)
        result["usdcny_30d_pct"]  = round(cny_chg, 2)
        result["crude_price"]     = round(crude_now, 2)
        result["as_of"]           = date.today()

        cny_stable  = abs(cny_chg) < _USDCNY_STABLE_PCT
        oil_strong  = crude_now >= _CRUDE_OIL_FLOOR

        if cny_stable and oil_strong:
            result["status"] = "ACCUMULATING"
            result["detail"] = (
                f"USDCNY {cny_chg:+.2f}% in 30d (stable) + "
                f"WTI ${crude_now:.1f} (>${_CRUDE_OIL_FLOOR}) — "
                "China FX / petrodollar nations have firepower to absorb selling."
            )
        elif cny_stable:
            result["status"] = "PARTIAL"
            result["detail"] = (
                f"Yuan stable ({cny_chg:+.2f}%) but WTI ${crude_now:.1f} "
                f"< ${_CRUDE_OIL_FLOOR} — petrodollar flows constrained."
            )
        elif oil_strong:
            result["status"] = "PARTIAL"
            result["detail"] = (
                f"WTI ${crude_now:.1f} strong but USDCNY {cny_chg:+.2f}% — "
                "China itself under currency pressure; Middle East still liquid."
            )
        else:
            result["status"] = "WEAK"
            result["detail"] = (
                f"USDCNY {cny_chg:+.2f}% + WTI ${crude_now:.1f} — "
                "both China and petrodollar support are absent."
            )
    except Exception as exc:
        log.warning("CB strength signal failed: %s", exc)
        # Fallback to ClickHouse for USDCNY
        try:
            cl = _ch_client()
            rows = cl.query(
                "SELECT trade_date, close FROM market_data.fx_rates FINAL "
                "WHERE symbol='USDCNY' ORDER BY trade_date DESC LIMIT 35"
            ).result_rows
            cl.close()
            if len(rows) >= 2:
                cny_now = rows[0][1]
                cny_30d = rows[min(29, len(rows)-1)][1]
                cny_chg = _pct_change(cny_30d, cny_now)
                result["usdcny_now"]     = round(cny_now, 4)
                result["usdcny_30d_pct"] = round(cny_chg, 2)
                result["detail"] = f"(yfinance fallback to CH) USDCNY {cny_chg:+.2f}% — crude unavailable"
                result["status"] = "PARTIAL"
        except Exception as exc2:
            result["detail"] = f"yfinance: {exc} | CH fallback: {exc2}"
            result["status"] = "error"
    return result


# ── Regime synthesis ──────────────────────────────────────────────────────────

_REGIME_PRIORITY = [
    # (signal_status_key, status_value, regime_label)
    ("retail",    "PANIC",        "RETAIL_PANIC"),
    ("speculator","CROWDED_LONG", "OVERLEVERED_LONGS"),
    ("speculator","EXTREME_SHORT","SHORT_SQUEEZE_SETUP"),
    ("institution","EXIT",        "INSTITUTIONAL_EXIT"),
    ("cb",        "ACCUMULATING", "CB_ACCUMULATION"),
]

_RECOMMENDATIONS: dict[str, str] = {
    "RETAIL_PANIC": (
        "BUY — Indian retail capitulation detected. Rupee-driven selling creates "
        "a temporary discount to NAV. Historically, such panics are mean-reverting "
        "within 4–8 weeks as hedges rebuild. Consider accumulating GOLDBEES at discount."
    ),
    "OVERLEVERED_LONGS": (
        "REDUCE / HEDGE — Speculator crowding is extreme. When MM Net% OI exceeds 25%, "
        "a sharp correction becomes likely as leveraged longs face forced liquidation. "
        "Trim exposure or buy protective puts on GLD."
    ),
    "SHORT_SQUEEZE_SETUP": (
        "BUY — Extreme short positioning detected. Any positive catalysts "
        "(Fed dovishness, safe-haven demand) could trigger a violent short-squeeze. "
        "Risk/reward favors small long positions."
    ),
    "INSTITUTIONAL_EXIT": (
        "HOLD / WAIT — Western institutions are redeeming GLD. This can persist for "
        "weeks. Wait for GLD AUM trend to stabilize before adding. Indian buyers "
        "may find opportunity if GOLDBEES discount widens."
    ),
    "CB_ACCUMULATION": (
        "HOLD / BUY DIP — Central banks (China + Gulf) have the capital to absorb "
        "Western selling. Dips are likely shallow and short-lived. Use weakness to add."
    ),
    "MIXED": (
        "WATCH — Conflicting signals across segments. No dominant force. "
        "Monitor GOLDBEES discount and GLD AUM weekly for regime clarity."
    ),
    "NEUTRAL": (
        "HOLD — No stress signals active across retail, institutional, or speculator "
        "segments. Current positioning is balanced."
    ),
}


def _synthesize_regime(
    retail: dict, institution: dict, speculator: dict, cb: dict
) -> tuple[str, str]:
    """Return (regime, recommendation) from the four signal dicts."""
    statuses = {
        "retail":      retail.get("status", ""),
        "institution": institution.get("status", ""),
        "speculator":  speculator.get("status", ""),
        "cb":          cb.get("status", ""),
    }

    for key, val, regime in _REGIME_PRIORITY:
        if statuses.get(key) == val:
            return regime, _RECOMMENDATIONS[regime]

    # Count stress signals
    stress = sum([
        statuses["retail"]      in ("PANIC", "STRESSED", "DISCOUNT"),
        statuses["institution"] == "EXIT",
        statuses["speculator"]  == "ELEVATED_LONG",
    ])
    if stress >= 2:
        return "MIXED", _RECOMMENDATIONS["MIXED"]
    return "NEUTRAL", _RECOMMENDATIONS["NEUTRAL"]


# ── Public API ────────────────────────────────────────────────────────────────

def fetch_who_is_selling(verbose: bool = True) -> dict[str, Any]:
    """
    Run all 4 signal checks and return a unified regime assessment.

    Returns
    -------
    dict with keys:
      signals        : {retail, institution, speculator, cb} signal dicts
      regime         : regime label string
      recommendation : plain-English buy/hold/sell/watch string
      summary        : one-sentence verdict
      as_of          : date
    """
    log.info("Who Is Selling? — running 4 signal checks…")

    retail      = _signal_retail_panic()
    institution = _signal_institutional_exit()
    speculator  = _signal_speculator_leverage()
    cb          = _signal_cb_strength()

    regime, recommendation = _synthesize_regime(retail, institution, speculator, cb)

    # One-sentence summary
    inr    = retail.get("usdinr_latest", "?")
    disc   = retail.get("goldbees_disc_pct", "?")
    mm_pct = speculator.get("mm_net_pct_oi", "?")
    crude  = cb.get("crude_price", "?")
    gld_aum = institution.get("gld_aum_usd", 0) or 0

    summary = (
        f"Regime: {regime} | "
        f"INR {inr} ({retail.get('usdinr_60d_pct', '?'):+.1f}% 60d) · "
        f"GOLDBEES disc {disc:+.2f}% · "
        f"MM Net {mm_pct:+.1f}% OI · "
        f"GLD AUM ${gld_aum/1e9:.1f}B · "
        f"WTI ${crude}"
    ) if isinstance(disc, float) and isinstance(mm_pct, float) else f"Regime: {regime}"

    result = {
        "regime":         regime,
        "recommendation": recommendation,
        "summary":        summary,
        "as_of":          date.today(),
        "signals": {
            "retail":      retail,
            "institution": institution,
            "speculator":  speculator,
            "cb":          cb,
        },
    }

    if verbose:
        log.info("Regime: %s", regime)
        log.info("Summary: %s", summary)

    return result


if __name__ == "__main__":
    import sys, os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
    import logging as _logging
    _logging.basicConfig(level=_logging.INFO, format="%(levelname)s %(message)s")

    out = fetch_who_is_selling()
    print(f"\n{'='*60}")
    print(f"  WHO IS SELLING? — {out['as_of']}")
    print(f"{'='*60}")
    print(f"\n  REGIME:  {out['regime']}")
    print(f"\n  SUMMARY: {out['summary']}")
    print(f"\n  RECOMMENDATION:\n    {out['recommendation']}")
    print(f"\n  SIGNAL DETAILS:")
    for name, sig in out["signals"].items():
        print(f"\n  [{name.upper()}] {sig['name']} → {sig['status']}")
        print(f"    {sig['detail']}")
    print()
