"""
src/tools/quant_scorecard.py
─────────────────────────────
Composite Gold Score engine for GOLDBEES.

Produces a 0–100 weighted score across four quantitative pillars:

  Macro    (30%) — DXY level + Real Yield trend
  Flows    (30%) — COT speculator positioning (mm_net / open_interest)
  Valuation(20%) — GOLDBEES iNAV premium / discount
  Momentum (20%) — LightGBM 5-day expected return

Scoring rules
─────────────
  Macro:
    • dxy_score:   100 if DXY ≤ 100, 0 if DXY ≥ 110  (linear between)
    • yield_score: 100 if real_yield_5d_delta ≤ −0.10,
                   0 if real_yield_5d_delta ≥ +0.10  (linear between)
    • macro_score = (dxy_score + yield_score) / 2

  Flows:
    • cot_pct = mm_net / open_interest × 100
    • flows_score: 100 if cot_pct ≤ 15%, 0 if ≥ 25%  (linear between)

  Valuation:
    • 100 if iNAV discount > +0.50%
    •   0 if iNAV premium  > +0.50%
    •  50 at parity (linear between)
    • Flips sign: positive premium_discount_pct means market > iNAV (premium),
      so valuation is low when premium is high.

  Momentum:
    • 100 if expected_return_pct ≥ +1.0%
    •   0 if expected_return_pct ≤ −1.0%  (linear between)

Public API
──────────
    compute_gold_scorecard(
        ch_host, ch_port, ch_user, ch_pass, ch_database
    ) -> dict

Return schema
─────────────
  {
    "composite_score":  float | None,   # 0–100
    "macro_score":      float | None,
    "flows_score":      float | None,
    "valuation_score":  float | None,
    "momentum_score":   float | None,
    "signals": {
        "dxy_level":          float | None,
        "real_yield_level":   float | None,   # latest US10Y − 2.5
        "real_yield_delta5":  float | None,   # 5-day change in real yield
        "cot_pct_oi":         float | None,
        "inav_disc_pct":      float | None,   # negative = discount, positive = premium
        "lgbm_return_pct":    float | None,
    },
    "goldbees_prices": pd.DataFrame,    # trade_date, close — 90 days
    "dxy_prices":      pd.DataFrame,    # trade_date, close — 90 days
    "as_of":           date | None,
    "error":           str | None,      # non-None if partial data only
  }

All pillars degrade gracefully — a missing data source contributes NaN and
the composite is computed from the available pillars only (re-weighted).
"""
from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Any

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)

# ── Scoring constants ──────────────────────────────────────────────────────────
_DXY_LOW      = 100.0    # below here → full bull score
_DXY_HIGH     = 110.0    # above here → zero bull score

_YIELD_D5_LOW  = -0.10   # 5-day real yield drop  → full bull score
_YIELD_D5_HIGH = +0.10   # 5-day real yield rise  → zero bull score

_COT_LOW  = 20.0         # % of OI → oversold → full bull score
_COT_HIGH = 35.0         # % of OI → crowded  → zero bull score

_DISC_HIGH  = +0.50      # discount > 0.5% → full valuation score (note: disc = negative pct)
_PREM_HIGH  = +0.50      # premium > 0.5% → zero valuation score

_MOM_HIGH = +1.0         # LightGBM pred ≥ +1% → full momentum score
_MOM_LOW  = -1.0         # LightGBM pred ≤ −1% → zero momentum score

# Pillar weights
_W_MACRO      = 0.30
_W_FLOWS      = 0.30
_W_VALUATION  = 0.20
_W_MOMENTUM   = 0.20

_INFLATION_CONST = 2.5   # fixed inflation proxy for real yield = US10Y − 2.5%

_LOOKBACK_DAYS = 90  # days of price history returned for rolling correlation


def _clamp01(value: float, low: float, high: float) -> float:
    """Linear interpolation between [low, high] → [1.0, 0.0], clamped to [0, 1]."""
    if high == low:
        return 0.5
    raw = (high - value) / (high - low)
    return float(np.clip(raw, 0.0, 1.0))


def _fetch_dxy_tnx(lookback_days: int = _LOOKBACK_DAYS + 10) -> pd.DataFrame:
    """
    Fetch DXY (DX-Y.NYB) and US 10Y yield (^TNX) from Yahoo Finance.

    Returns a DataFrame with columns: trade_date, dxy_close, us10y_close.
    Degrades gracefully to an empty DataFrame on failure.
    """
    try:
        import yfinance as yf
        from datetime import date as _date
        start = (_date.today() - timedelta(days=lookback_days)).isoformat()
        end   = (_date.today() + timedelta(days=1)).isoformat()

        dxy_raw = yf.download(
            "DX-Y.NYB", start=start, end=end,
            auto_adjust=True, progress=False, timeout=None,
        )
        tnx_raw = yf.download(
            "^TNX", start=start, end=end,
            auto_adjust=True, progress=False, timeout=None,
        )

        pieces: list[pd.DataFrame] = []
        if not dxy_raw.empty:
            d = dxy_raw[["Close"]].copy()
            # yfinance may return MultiIndex columns even for a single ticker
            if isinstance(d.columns, pd.MultiIndex):
                d.columns = ["dxy_close"]
            else:
                d.columns = ["dxy_close"]
            pieces.append(d)
        if not tnx_raw.empty:
            t = tnx_raw[["Close"]].copy()
            if isinstance(t.columns, pd.MultiIndex):
                t.columns = ["us10y_close"]
            else:
                t.columns = ["us10y_close"]
            pieces.append(t)

        if not pieces:
            return pd.DataFrame(columns=["trade_date", "dxy_close", "us10y_close"])

        df = pieces[0]
        for extra in pieces[1:]:
            df = df.join(extra, how="outer")

        df = df.reset_index().rename(columns={"Date": "trade_date", "index": "trade_date"})
        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.tz_localize(None).dt.normalize()
        return df.sort_values("trade_date").reset_index(drop=True)

    except Exception as exc:
        log.warning("DXY/TNX fetch failed: %s", exc)
        return pd.DataFrame(columns=["trade_date", "dxy_close", "us10y_close"])


def _score_macro(dxy_series: pd.Series, tnx_series: pd.Series) -> tuple[float | None, dict]:
    """
    Score the Macro pillar.

    Parameters
    ----------
    dxy_series : daily DXY close, time-ordered
    tnx_series : daily US10Y close, time-ordered

    Returns (score 0–100, signals_dict)
    """
    signals: dict[str, Any] = {
        "dxy_level": None,
        "real_yield_level": None,
        "real_yield_delta5": None,
    }
    scores: list[float] = []

    # DXY sub-score
    if not dxy_series.dropna().empty:
        dxy_latest = float(dxy_series.dropna().iloc[-1])
        signals["dxy_level"] = round(dxy_latest, 2)
        dxy_score = _clamp01(dxy_latest, _DXY_LOW, _DXY_HIGH) * 100
        scores.append(dxy_score)
    else:
        log.debug("DXY data unavailable for macro score")

    # Real Yield sub-score
    if not tnx_series.dropna().empty:
        ry = tnx_series.dropna() - _INFLATION_CONST
        ry_latest = float(ry.iloc[-1])
        signals["real_yield_level"] = round(ry_latest, 3)

        if len(ry) >= 6:
            ry_delta5 = float(ry.iloc[-1] - ry.iloc[-6])
            signals["real_yield_delta5"] = round(ry_delta5, 4)
            yield_score = _clamp01(ry_delta5, _YIELD_D5_LOW, _YIELD_D5_HIGH) * 100
            scores.append(yield_score)
        else:
            log.debug("Not enough TNX history for real yield delta")
    else:
        log.debug("TNX data unavailable for macro score")

    if not scores:
        return None, signals

    macro_score = float(np.mean(scores))
    return round(macro_score, 1), signals


def _score_flows(cot_pct_oi: float | None) -> float | None:
    """Score the Flows pillar from COT mm_net/OI %."""
    if cot_pct_oi is None:
        return None
    return round(_clamp01(cot_pct_oi, _COT_LOW, _COT_HIGH) * 100, 1)


def _score_valuation(premium_discount_pct: float | None) -> float | None:
    """
    Score the Valuation pillar from iNAV premium/discount %.

    premium_discount_pct > 0 means market > iNAV (premium, negative for valuation).
    premium_discount_pct < 0 means discount (positive for valuation).
    """
    if premium_discount_pct is None:
        return None
    # Flip sign: discount (negative pct) means cheap → high score
    disc = -premium_discount_pct
    return round(_clamp01(-disc, -_DISC_HIGH, _PREM_HIGH) * 100, 1)


def _score_momentum(expected_return_pct: float | None) -> float | None:
    """Score the Momentum pillar from LightGBM expected return %."""
    if expected_return_pct is None:
        return None
    return round(_clamp01(expected_return_pct, _MOM_LOW, _MOM_HIGH) * 100, 1)


def _composite(pillar_scores: dict[str, float | None]) -> float | None:
    """
    Compute weighted composite from available pillars, re-normalising weights
    so the output stays on the 0–100 scale even when data is partial.
    """
    weight_map = {
        "macro":      _W_MACRO,
        "flows":      _W_FLOWS,
        "valuation":  _W_VALUATION,
        "momentum":   _W_MOMENTUM,
    }
    total_w = 0.0
    weighted_sum = 0.0
    for key, score in pillar_scores.items():
        if score is not None:
            w = weight_map.get(key, 0.0)
            total_w += w
            weighted_sum += w * score

    if total_w == 0.0:
        return None
    return round(weighted_sum / total_w, 1)


# ── Public API ─────────────────────────────────────────────────────────────────

def compute_gold_scorecard(
    ch_host: str = "localhost",
    ch_port: int = 8123,
    ch_user: str = "default",
    ch_pass: str = "",
    ch_database: str = "market_data",
) -> dict[str, Any]:
    """
    Compute the full Composite Gold Score for GOLDBEES.

    Returns a dict — see module docstring for full schema.
    Degrades gracefully: any missing data source contributes NaN pillars;
    the composite is re-weighted from available pillars only.
    """
    import clickhouse_connect

    errors: list[str] = []
    signals: dict[str, Any] = {
        "dxy_level":         None,
        "real_yield_level":  None,
        "real_yield_delta5": None,
        "cot_pct_oi":        None,
        "inav_disc_pct":     None,
        "lgbm_return_pct":   None,
    }
    macro_score:      float | None = None
    flows_score:      float | None = None
    valuation_score:  float | None = None
    momentum_score:   float | None = None
    goldbees_prices   = pd.DataFrame()
    dxy_prices        = pd.DataFrame()
    as_of: date | None = None

    # ── 1. Fetch DXY + TNX from Yahoo Finance ─────────────────────────────────
    macro_df = _fetch_dxy_tnx(_LOOKBACK_DAYS + 10)
    if not macro_df.empty:
        dxy_col  = macro_df["dxy_close"]  if "dxy_close"   in macro_df.columns else pd.Series(dtype=float)
        tnx_col  = macro_df["us10y_close"] if "us10y_close" in macro_df.columns else pd.Series(dtype=float)
        macro_score, macro_signals = _score_macro(dxy_col, tnx_col)
        signals.update(macro_signals)

        # Retain DXY price series for rolling correlation chart
        if "dxy_close" in macro_df.columns:
            dxy_prices = (
                macro_df[["trade_date", "dxy_close"]]
                .rename(columns={"dxy_close": "close"})
                .dropna()
                .tail(_LOOKBACK_DAYS)
                .reset_index(drop=True)
            )
    else:
        errors.append("DXY/TNX fetch failed — macro pillar unavailable")

    # ── 2. Query ClickHouse ────────────────────────────────────────────────────
    try:
        client = clickhouse_connect.get_client(
            host=ch_host, port=ch_port,
            username=ch_user, password=ch_pass,
            connect_timeout=10,
        )

        # ── Flows: latest COT mm_net / open_interest ──────────────────────────
        try:
            cot_row = client.query("""
                SELECT mm_net, open_interest
                FROM market_data.cot_gold
                ORDER BY report_date DESC
                LIMIT 1
            """).result_rows
            if cot_row:
                mm_net, oi = float(cot_row[0][0]), float(cot_row[0][1])
                if oi > 0:
                    cot_pct = mm_net / oi * 100
                    signals["cot_pct_oi"] = round(cot_pct, 2)
                    flows_score = _score_flows(cot_pct)
                else:
                    errors.append("COT open_interest is zero")
            else:
                errors.append("COT table empty — flows pillar unavailable")
        except Exception as exc:
            errors.append(f"COT query failed: {exc}")
            log.warning("COT score error: %s", exc)

        # ── Valuation: latest iNAV premium/discount ───────────────────────────
        try:
            inav_row = client.query("""
                SELECT premium_discount_pct, toDate(snapshot_at) AS snap_date
                FROM market_data.inav_snapshots
                WHERE symbol = 'GOLDBEES'
                ORDER BY snapshot_at DESC
                LIMIT 1
            """).result_rows
            if inav_row:
                disc_pct = float(inav_row[0][0])
                signals["inav_disc_pct"] = round(disc_pct, 3)
                valuation_score = _score_valuation(disc_pct)
                if as_of is None and inav_row[0][1]:
                    as_of = inav_row[0][1]
            else:
                errors.append("iNAV snapshots empty — valuation pillar unavailable")
        except Exception as exc:
            errors.append(f"iNAV query failed: {exc}")
            log.warning("iNAV score error: %s", exc)

        # ── Momentum: latest LightGBM prediction ─────────────────────────────
        try:
            ml_row = client.query("""
                SELECT expected_return_pct, as_of
                FROM market_data.ml_predictions
                ORDER BY as_of DESC
                LIMIT 1
            """).result_rows
            if ml_row:
                ret_pct = float(ml_row[0][0])
                signals["lgbm_return_pct"] = round(ret_pct, 3)
                momentum_score = _score_momentum(ret_pct)
                if as_of is None and ml_row[0][1]:
                    as_of = ml_row[0][1]
            else:
                errors.append("ml_predictions empty — momentum pillar unavailable")
        except Exception as exc:
            errors.append(f"ML predictions query failed: {exc}")
            log.warning("Momentum score error: %s", exc)

        # ── GOLDBEES price history for rolling correlation chart ───────────────
        try:
            cutoff = (date.today() - timedelta(days=_LOOKBACK_DAYS)).isoformat()
            gb_rows = client.query(f"""
                SELECT trade_date, argMax(close, imported_at) AS close
                FROM market_data.daily_prices
                WHERE symbol = 'GOLDBEES' AND category = 'etfs'
                  AND trade_date >= toDate('{cutoff}')
                GROUP BY trade_date
                ORDER BY trade_date ASC
            """).result_rows
            if gb_rows:
                goldbees_prices = pd.DataFrame(gb_rows, columns=["trade_date", "close"])
                goldbees_prices["trade_date"] = pd.to_datetime(goldbees_prices["trade_date"])
                if as_of is None and len(goldbees_prices):
                    as_of = goldbees_prices["trade_date"].iloc[-1].date()
        except Exception as exc:
            errors.append(f"GOLDBEES prices query failed: {exc}")
            log.warning("GOLDBEES prices error: %s", exc)

        client.close()

    except Exception as exc:
        errors.append(f"ClickHouse connection failed: {exc}")
        log.error("Scorecard ClickHouse error: %s", exc)

    # ── 3. Composite ──────────────────────────────────────────────────────────
    composite = _composite({
        "macro":     macro_score,
        "flows":     flows_score,
        "valuation": valuation_score,
        "momentum":  momentum_score,
    })

    return {
        "composite_score": composite,
        "macro_score":     macro_score,
        "flows_score":     flows_score,
        "valuation_score": valuation_score,
        "momentum_score":  momentum_score,
        "signals":         signals,
        "goldbees_prices": goldbees_prices,
        "dxy_prices":      dxy_prices,
        "as_of":           as_of,
        "error":           "; ".join(errors) if errors else None,
    }
