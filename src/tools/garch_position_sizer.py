"""
src/tools/garch_position_sizer.py
──────────────────────────────────
End-to-end GARCH-based position sizer for any NSE/BSE instrument.

Pipeline
────────
  1. Fetch 2 years of daily OHLCV from yfinance (Ticker.history)
  2. Compute log-returns + robust Z-scores for regime signals
  3. Fit GARCH(1,1) with Student-t innovations → σ_t (conditional vol)
  4. Compute EMA(50) → trend filter flag
  5. Classify regime from GARCH residuals (lightweight — no Isolation Forest)
  6. Call compute_position_weight() from risk_governor

Results for steps 3–5 are cached in output/.cache/garch_<SYMBOL>.json
with a 1-hour TTL so repeated calls within an analysis run cost nothing.

Public API
──────────
    size_position(symbol, exchange, composite_score, regime_override)
        -> RiskDecision | None
"""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path

import numpy as np
import pandas as pd

from src.ml.anomaly import fit_garch_residuals, robust_zscore
from src.tools.risk_governor import RiskDecision, compute_position_weight, vol_target_for

logger = logging.getLogger(__name__)

_CACHE_DIR   = Path("output/.cache")
_CACHE_TTL_S = 3600   # 1 hour
_EMA_WINDOW  = 50
_MIN_ROWS    = 120    # ~6 months — minimum for a stable GARCH fit


# ── Helpers ───────────────────────────────────────────────────────────────────

def _yahoo_symbol(symbol: str, exchange: str) -> str:
    suffix = ".BO" if exchange.upper() == "BSE" else ".NS"
    return f"{symbol}{suffix}"


def _cache_path(symbol: str) -> Path:
    return _CACHE_DIR / f"garch_{symbol.upper()}.json"


def _load_cache(symbol: str) -> dict | None:
    path = _cache_path(symbol)
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text())
        if time.time() - data.get("_ts", 0) < _CACHE_TTL_S:
            return data
    except Exception:
        pass
    return None


def _save_cache(symbol: str, payload: dict) -> None:
    _CACHE_DIR.mkdir(parents=True, exist_ok=True)
    payload["_ts"] = time.time()
    try:
        _cache_path(symbol).write_text(json.dumps(payload))
    except Exception:
        pass


def _fetch_ohlcv(symbol: str, exchange: str) -> pd.DataFrame | None:
    """Download 2 years of daily OHLCV via yfinance. Returns None on failure."""
    try:
        import yfinance as yf
    except ImportError:
        logger.error("yfinance not installed; cannot run risk governor")
        return None

    ticker_sym = _yahoo_symbol(symbol, exchange)
    try:
        hist = yf.Ticker(ticker_sym).history(period="2y")
        if hist.empty or len(hist) < _MIN_ROWS:
            logger.warning(
                "Only %d rows for %s (need %d) — skipping GARCH",
                len(hist), ticker_sym, _MIN_ROWS,
            )
            return None
        df = hist.reset_index()[["Date", "Open", "High", "Low", "Close", "Volume"]]
        df.columns = ["trade_date", "open", "high", "low", "close", "volume"]
        # Strip timezone so pd.to_datetime comparisons stay simple
        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.tz_localize(None)
        return df.sort_values("trade_date").reset_index(drop=True)
    except Exception as exc:
        logger.warning("yfinance fetch failed for %s: %s", ticker_sym, exc)
        return None


# ── Lightweight regime classification (no Isolation Forest / COT / FX) ────────

def _classify_regime_lite(df: pd.DataFrame) -> str:
    """
    Regime label using only GARCH residual Z and robust price Z.

    Replicates the shock-priority rules from anomaly.classify_regime() but
    drops the Isolation Forest confidence multiplier and cross-asset features,
    which are unavailable at per-holding analysis time.

    Priority (same as full pipeline):
        ⚡ Flash Crash  — low price Z, high residual Z   (GARCH shock without trend)
        🔥 Vol Breakout — high price Z, high residual Z
        🧨 Blow-off Top — high price Z, thin volume, positive day
        📈 Strong Trend — high price Z only
        ✅ Normal
    """
    if "z_resid_abs" not in df.columns or "z_robust" not in df.columns:
        return "✅ Normal"

    # 80th-percentile thresholds match full classify_regime()
    z_med   = float(df["z_robust"].abs().quantile(0.80))
    res_med = float(df["z_resid_abs"].quantile(0.80))

    row    = df.iloc[-1]
    hi_z   = abs(float(row.get("z_robust",   0.0))) > z_med
    hi_res = float(row.get("z_resid_abs", 0.0))     > res_med

    if not hi_z and hi_res:
        return "⚡ Flash Crash / Black Swan (EXIT)"
    if hi_z and hi_res:
        return "🔥 Volatile Breakout"
    if hi_z:
        if "z_volume" in df.columns:
            vol_lo = float(df["z_volume"].abs().quantile(0.20))
            if (
                abs(float(row.get("z_volume", 0.0))) < vol_lo
                and float(row.get("daily_return", 0.0)) > 0
            ):
                return "🧨 Blow-off Top (Weak)"
        return "📈 Strong Trend (HODL)"
    return "✅ Normal"


# ── Public API ────────────────────────────────────────────────────────────────

def size_position(
    symbol: str,
    exchange: str = "NSE",
    composite_score: float | None = None,
    regime_override: str | None = None,
) -> RiskDecision | None:
    """
    Compute volatility-targeted position weight for any NSE/BSE instrument.

    Parameters
    ----------
    symbol          : NSE/BSE trading symbol e.g. "GOLDBEES", "INFY"
    exchange        : "NSE" or "BSE"
    composite_score : Quant scorecard 0–100 (None = not available)
    regime_override : If provided, skips GARCH regime classification

    Returns
    -------
    RiskDecision — fully auditable sizing decision with all intermediate steps.
    None         — if fewer than 120 trading days of history are available.
    """
    # ── Cache hit: reuse previously fitted GARCH vol + regime ────────────────
    cached = _load_cache(symbol)
    if cached:
        logger.debug("GARCH cache hit for %s (age < 1 h)", symbol)
        return compute_position_weight(
            garch_annual_vol_pct = cached["garch_annual_vol_pct"],
            regime               = regime_override or cached["regime"],
            composite_score      = composite_score,
            vol_target_pct       = vol_target_for(symbol),
            price_below_ema50    = cached["price_below_ema50"],
        )

    # ── Step 1: OHLCV ────────────────────────────────────────────────────────
    df = _fetch_ohlcv(symbol, exchange)
    if df is None:
        return None

    # ── Step 2: Returns + robust Z-scores ────────────────────────────────────
    df["log_return"]   = np.log(df["close"] / df["close"].shift(1))
    df["daily_return"] = df["close"].pct_change() * 100
    df["range_pct"]    = (df["high"] - df["low"]) / df["close"] * 100

    df["z_return"] = robust_zscore(df["daily_return"].fillna(0))
    df["z_range"]  = robust_zscore(df["range_pct"])
    df["z_robust"] = (df["z_return"].abs() + df["z_range"]) / 2.0
    df["z_volume"] = robust_zscore(df["volume"].fillna(0))

    # ── Step 3: GARCH(1,1) with Student-t innovations ────────────────────────
    try:
        df, _ = fit_garch_residuals(df)
    except Exception as exc:
        logger.warning("GARCH fit failed for %s: %s", symbol, exc)
        return None

    latest_garch_vol = float(df["garch_vol"].dropna().iloc[-1])

    # ── Step 4: EMA(50) trend filter ─────────────────────────────────────────
    df["ema50"]       = df["close"].ewm(span=_EMA_WINDOW, adjust=False).mean()
    price_below_ema50 = bool(df["close"].iloc[-1] < df["ema50"].iloc[-1])

    # ── Step 5: Lightweight regime classification ─────────────────────────────
    regime = regime_override or _classify_regime_lite(df)

    # ── Persist GARCH outputs for 1-hour reuse ────────────────────────────────
    _save_cache(symbol, {
        "garch_annual_vol_pct": latest_garch_vol,
        "regime":               regime,
        "price_below_ema50":    price_below_ema50,
    })

    # ── Step 6: Compute position weight ──────────────────────────────────────
    return compute_position_weight(
        garch_annual_vol_pct = latest_garch_vol,
        regime               = regime,
        composite_score      = composite_score,
        vol_target_pct       = vol_target_for(symbol),
        price_below_ema50    = price_below_ema50,
    )
