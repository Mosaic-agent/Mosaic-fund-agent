"""
src/tools/quant_scorecard.py
─────────────────────────────
Composite Quant Score engine for Gold (GOLDBEES) and Silver (SILVERBEES).

Produces a 0–100 weighted score across four quantitative pillars:

  Macro    (30%) — DXY level + Real Yield trend  [+ Gold-Silver Ratio for silver]
  Flows    (30%) — COT speculator positioning (mm_net / open_interest)
  Valuation(20%) — ETF iNAV premium / discount
  Momentum (20%) — LightGBM 5-day expected return  [SI=F momentum for silver]

Scoring rules — Gold
─────────────────────
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

  Momentum:
    • 100 if expected_return_pct ≥ +1.0%
    •   0 if expected_return_pct ≤ −1.0%  (linear between)

Scoring rules — Silver (additional / modified)
───────────────────────────────────────────────
  Macro also includes Gold-Silver Ratio (GSR):
    • gsr_score:   100 if GSR ≥ 90 (silver cheap), 0 if GSR ≤ 55  (linear between)
    • macro_score = average of (dxy_score, yield_score, gsr_score)

  Flows (COT code 084, SILVER COMEX — fetched live from CFTC TXT):
    • flows_score: 100 if cot_pct ≤ 20%, 0 if ≥ 35%  (silver less speculative than gold)

  Momentum (SI=F 5-day realised return, wider band due to higher vol):
    • 100 if return ≥ +2.0%
    •   0 if return ≤ −2.0%  (linear between)

Public API
──────────
    compute_gold_scorecard(ch_host, ch_port, ch_user, ch_pass, ch_database) -> dict
    compute_silver_scorecard(ch_host, ch_port, ch_user, ch_pass, ch_database) -> dict

Return schema (both functions)
─────────────────────────────
  {
    "composite_score":  float | None,   # 0–100
    "macro_score":      float | None,
    "flows_score":      float | None,
    "valuation_score":  float | None,
    "momentum_score":   float | None,
    "signals":          dict,
    "<etf>_prices":     pd.DataFrame,   # trade_date, close — 90 days
    "dxy_prices":       pd.DataFrame,   # trade_date, close — 90 days
    "as_of":            date | None,
    "error":            str | None,
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

# Staleness guards — queries returning data older than these limits degrade gracefully
_COT_MAX_AGE_DAYS   = 14   # COT is weekly (Tuesday); 14 days allows one missed release
_INAV_MAX_AGE_DAYS  = 2    # iNAV is intraday; >2 days = importer broken
_ML_MAX_AGE_DAYS    = 3    # ML prediction; 3 days covers Fri→Mon weekend gap


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

    Convention: premium_discount_pct > 0 → market above iNAV (premium = expensive, bearish).
                premium_discount_pct < 0 → market below iNAV (discount = cheap, bullish).

    Score:  −0.5 % (deep discount) → 100   (buy cheap)
             0.0 %  (parity)        →  50
            +0.5 % (deep premium)  →   0   (avoid expensive)

    _clamp01(value, low, high) = (high − value) / (high − low), clamped to [0, 1].
    With low = −_DISC_HIGH = −0.5 and high = +_PREM_HIGH = +0.5:
        raw = (0.5 − pct) / 1.0   → rises as pct falls (discount grows).
    This is equivalent to the original disc = −pct / _clamp01(pct, −DISC, +PREM)
    but without the confusing double-negation.
    """
    if premium_discount_pct is None:
        return None
    return round(_clamp01(premium_discount_pct, -_DISC_HIGH, _PREM_HIGH) * 100, 1)


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
            cot_row = client.query(f"""
                SELECT mm_net, open_interest, report_date
                FROM market_data.cot_gold
                WHERE report_date >= today() - INTERVAL {_COT_MAX_AGE_DAYS} DAY
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
                errors.append(
                    f"COT data is stale (no row within {_COT_MAX_AGE_DAYS}d) "
                    "— flows pillar unavailable"
                )
        except Exception as exc:
            errors.append(f"COT query failed: {exc}")
            log.warning("COT score error: %s", exc)

        # ── Valuation: latest iNAV premium/discount ───────────────────────────
        try:
            inav_row = client.query(f"""
                SELECT premium_discount_pct, toDate(snapshot_at) AS snap_date
                FROM market_data.inav_snapshots
                WHERE symbol = 'GOLDBEES'
                  AND snapshot_at >= now() - INTERVAL {_INAV_MAX_AGE_DAYS} DAY
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
                errors.append(
                    f"iNAV data is stale (no row within {_INAV_MAX_AGE_DAYS}d) "
                    "— valuation pillar unavailable"
                )
        except Exception as exc:
            errors.append(f"iNAV query failed: {exc}")
            log.warning("iNAV score error: %s", exc)

        # ── Momentum: latest LightGBM prediction ─────────────────────────────
        try:
            ml_row = client.query(f"""
                SELECT expected_return_pct, as_of
                FROM market_data.ml_predictions
                WHERE as_of >= today() - INTERVAL {_ML_MAX_AGE_DAYS} DAY
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
                errors.append(
                    f"ML prediction is stale (no row within {_ML_MAX_AGE_DAYS}d) "
                    "— momentum pillar unavailable"
                )
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


# ══════════════════════════════════════════════════════════════════════════════
# Silver scorecard
# ══════════════════════════════════════════════════════════════════════════════

# Silver-specific scoring constants
_SILVER_COT_LOW   = 20.0   # % of OI below → less crowded → bullish
_SILVER_COT_HIGH  = 35.0   # % of OI above → crowded → bearish

_GSR_HIGH = 90.0   # gold-silver ratio ≥ 90 → silver very cheap → 100 score
_GSR_LOW  = 55.0   # gold-silver ratio ≤ 55 → silver expensive → 0 score

_SILVER_MOM_HIGH  = +2.0   # 5-day return ≥ +2% → full momentum score
_SILVER_MOM_LOW   = -2.0   # 5-day return ≤ −2% → zero momentum score

_CFTC_CUR_URL     = "https://www.cftc.gov/dea/newcot/f_disagg.txt"
_UA_HEADERS       = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"}
_CFTC_COLS_NEEDED = [
    "Market_and_Exchange_Names", "As_of_Date_In_Form_YYMMDD", "Report_Date_as_YYYY-MM-DD",
    "CFTC_Contract_Market_Code", "CFTC_Market_Code", "CFTC_Region_Code",
    "CFTC_Commodity_Code", "Open_Interest_All",
    "Prod_Merc_Positions_Long_All", "Prod_Merc_Positions_Short_All",
    "Swap_Positions_Long_All", "Swap__Positions_Short_All", "Swap__Positions_Spread_All",
    "M_Money_Positions_Long_All", "M_Money_Positions_Short_All", "M_Money_Positions_Spread_All",
    "Other_Rept_Positions_Long_All", "Other_Rept_Positions_Short_All", "Other_Rept_Positions_Spread_All",
    "Tot_Rept_Positions_Long_All", "Tot_Rept_Positions_Short_All",
    "NonRept_Positions_Long_All", "NonRept_Positions_Short_All",
]


def _fetch_silver_cot_live() -> dict[str, Any] | None:
    """
    Fetch the latest Silver COT row directly from the CFTC weekly TXT.

    Returns dict with keys: report_date, mm_net, open_interest — or None on failure.
    CFTC market name for standard silver: 'SILVER - COMMODITY'  (code 084).
    """
    import io
    import requests

    try:
        resp = requests.get(_CFTC_CUR_URL, headers=_UA_HEADERS, timeout=30)
        resp.raise_for_status()
        df_raw = pd.read_csv(io.StringIO(resp.text), header=None, low_memory=False)
        n = min(len(_CFTC_COLS_NEEDED), len(df_raw.columns))
        rename = {i: _CFTC_COLS_NEEDED[i] for i in range(n)}
        df_raw = df_raw.rename(columns=rename)

        silver_df = df_raw[
            df_raw["Market_and_Exchange_Names"].str.match(r"^SILVER - COMMODITY", na=False)
        ]
        if silver_df.empty:
            log.warning("No 'SILVER - COMMODITY' rows found in CFTC TXT")
            return None

        row = silver_df.sort_values("Report_Date_as_YYYY-MM-DD", ascending=False).iloc[0]
        mm_long  = int(row["M_Money_Positions_Long_All"]  or 0)
        mm_short = int(row["M_Money_Positions_Short_All"] or 0)
        oi       = int(row["Open_Interest_All"]           or 0)
        rdate    = pd.to_datetime(str(row["Report_Date_as_YYYY-MM-DD"])[:10]).date()
        return {
            "report_date":   rdate,
            "mm_net":        mm_long - mm_short,
            "open_interest": oi,
        }
    except Exception as exc:
        log.warning("Silver COT live fetch failed: %s", exc)
        return None


def _fetch_gold_silver_ratio() -> float | None:
    """
    Fetch the current Gold-Silver Ratio via Yahoo Finance (GC=F / SI=F).
    Returns the ratio or None on failure.
    """
    try:
        import yfinance as yf
        from datetime import date as _date
        start = (_date.today() - timedelta(days=5)).isoformat()
        end   = (_date.today() + timedelta(days=1)).isoformat()

        gc = yf.download("GC=F", start=start, end=end, auto_adjust=True, progress=False)
        si = yf.download("SI=F", start=start, end=end, auto_adjust=True, progress=False)

        if gc.empty or si.empty:
            return None

        # Handle MultiIndex columns
        gc_close = gc["Close"].iloc[-1]
        si_close = si["Close"].iloc[-1]
        if isinstance(gc_close, pd.Series):
            gc_close = gc_close.iloc[0]
        if isinstance(si_close, pd.Series):
            si_close = si_close.iloc[0]

        if float(si_close) == 0:
            return None
        return round(float(gc_close) / float(si_close), 2)
    except Exception as exc:
        log.warning("GSR fetch failed: %s", exc)
        return None


def _fetch_silver_momentum(lookback_days: int = _LOOKBACK_DAYS + 10) -> float | None:
    """
    Compute 5-day realised return of Silver futures (SI=F) from Yahoo Finance.
    Returns the percentage return or None on failure.
    """
    try:
        import yfinance as yf
        from datetime import date as _date
        start = (_date.today() - timedelta(days=lookback_days)).isoformat()
        end   = (_date.today() + timedelta(days=1)).isoformat()

        si_raw = yf.download("SI=F", start=start, end=end, auto_adjust=True, progress=False)
        if si_raw.empty or len(si_raw) < 6:
            return None

        closes = si_raw["Close"]
        if isinstance(closes, pd.DataFrame):
            closes = closes.iloc[:, 0]
        closes = closes.dropna()
        if len(closes) < 6:
            return None

        ret5 = float((closes.iloc[-1] - closes.iloc[-6]) / closes.iloc[-6] * 100)
        return round(ret5, 3)
    except Exception as exc:
        log.warning("Silver momentum fetch failed: %s", exc)
        return None


def _score_macro_silver(
    dxy_series: pd.Series,
    tnx_series: pd.Series,
    gsr: float | None,
) -> tuple[float | None, dict]:
    """
    Score the Macro pillar for silver (DXY + Real Yield + Gold-Silver Ratio).
    """
    signals: dict[str, Any] = {
        "dxy_level":         None,
        "real_yield_level":  None,
        "real_yield_delta5": None,
        "gsr":               None,
    }
    scores: list[float] = []

    # DXY sub-score (same as gold)
    if not dxy_series.dropna().empty:
        dxy_latest = float(dxy_series.dropna().iloc[-1])
        signals["dxy_level"] = round(dxy_latest, 2)
        scores.append(_clamp01(dxy_latest, _DXY_LOW, _DXY_HIGH) * 100)

    # Real Yield sub-score (same as gold)
    if not tnx_series.dropna().empty:
        ry = tnx_series.dropna() - _INFLATION_CONST
        signals["real_yield_level"] = round(float(ry.iloc[-1]), 3)
        if len(ry) >= 6:
            ry_delta5 = float(ry.iloc[-1] - ry.iloc[-6])
            signals["real_yield_delta5"] = round(ry_delta5, 4)
            scores.append(_clamp01(ry_delta5, _YIELD_D5_LOW, _YIELD_D5_HIGH) * 100)

    # Gold-Silver Ratio sub-score
    if gsr is not None:
        signals["gsr"] = gsr
        # High GSR = silver cheap vs gold = bullish for silver
        gsr_score = _clamp01(gsr, _GSR_LOW, _GSR_HIGH)  # 1.0 when gsr=HIGH, 0.0 when gsr=LOW
        scores.append(gsr_score * 100)

    if not scores:
        return None, signals
    return round(float(np.mean(scores)), 1), signals


def compute_silver_scorecard(
    ch_host: str = "localhost",
    ch_port: int = 8123,
    ch_user: str = "default",
    ch_pass: str = "",
    ch_database: str = "market_data",
) -> dict[str, Any]:
    """
    Compute the full Composite Silver Score for SILVERBEES.

    Pillars:
      Macro    (30%) — DXY + Real Yield + Gold-Silver Ratio (GSR)
      Flows    (30%) — COT silver mm_net / OI (live CFTC TXT, code 084)
      Valuation(20%) — SILVERBEES iNAV premium / discount from ClickHouse
      Momentum (20%) — SI=F 5-day realised return (or ml_predictions if available)

    Degrades gracefully — missing pillars are excluded and weights re-normalised.
    """
    import clickhouse_connect

    errors: list[str] = []
    signals: dict[str, Any] = {
        "dxy_level":         None,
        "real_yield_level":  None,
        "real_yield_delta5": None,
        "gsr":               None,
        "cot_pct_oi":        None,
        "cot_report_date":   None,
        "inav_disc_pct":     None,
        "momentum_return_pct": None,
    }
    macro_score:     float | None = None
    flows_score:     float | None = None
    valuation_score: float | None = None
    momentum_score:  float | None = None
    silverbees_prices = pd.DataFrame()
    dxy_prices        = pd.DataFrame()
    as_of: date | None = None

    # ── 1. Macro: DXY + TNX + GSR ────────────────────────────────────────────
    macro_df = _fetch_dxy_tnx(_LOOKBACK_DAYS + 10)
    gsr = _fetch_gold_silver_ratio()

    if not macro_df.empty:
        dxy_col = macro_df["dxy_close"]   if "dxy_close"   in macro_df.columns else pd.Series(dtype=float)
        tnx_col = macro_df["us10y_close"] if "us10y_close" in macro_df.columns else pd.Series(dtype=float)
        macro_score, macro_signals = _score_macro_silver(dxy_col, tnx_col, gsr)
        signals.update(macro_signals)

        if "dxy_close" in macro_df.columns:
            dxy_prices = (
                macro_df[["trade_date", "dxy_close"]]
                .rename(columns={"dxy_close": "close"})
                .dropna()
                .tail(_LOOKBACK_DAYS)
                .reset_index(drop=True)
            )
    else:
        if gsr is not None:
            signals["gsr"] = gsr
            gsr_score = _clamp01(gsr, _GSR_LOW, _GSR_HIGH) * 100
            macro_score = round(gsr_score, 1)
        errors.append("DXY/TNX fetch failed — macro uses GSR only")

    # ── 2. Flows: Silver COT from CFTC live TXT ───────────────────────────────
    cot = _fetch_silver_cot_live()
    if cot is not None:
        oi = cot["open_interest"]
        if oi > 0:
            cot_pct = cot["mm_net"] / oi * 100
            signals["cot_pct_oi"]      = round(cot_pct, 2)
            signals["cot_report_date"] = str(cot["report_date"])
            flows_score = round(_clamp01(cot_pct, _SILVER_COT_LOW, _SILVER_COT_HIGH) * 100, 1)
        else:
            errors.append("Silver COT open_interest is zero")
    else:
        errors.append("Silver COT fetch failed — flows pillar unavailable")

    # ── 3. ClickHouse: iNAV + price history ───────────────────────────────────
    try:
        client = clickhouse_connect.get_client(
            host=ch_host, port=ch_port,
            username=ch_user, password=ch_pass,
            connect_timeout=10,
        )

        # Valuation: SILVERBEES iNAV premium/discount
        try:
            inav_row = client.query(f"""
                SELECT premium_discount_pct, toDate(snapshot_at) AS snap_date
                FROM market_data.inav_snapshots
                WHERE symbol = 'SILVERBEES'
                  AND snapshot_at >= now() - INTERVAL {_INAV_MAX_AGE_DAYS} DAY
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
                errors.append(
                    f"SILVERBEES iNAV data is stale (no row within {_INAV_MAX_AGE_DAYS}d) "
                    "— valuation unavailable"
                )
        except Exception as exc:
            errors.append(f"SILVERBEES iNAV query failed: {exc}")

        # Momentum: try ml_predictions for silver first, else use SI=F realised return
        try:
            ml_row = client.query(f"""
                SELECT expected_return_pct, as_of
                FROM market_data.ml_predictions
                WHERE symbol = 'SILVERBEES'
                  AND as_of >= today() - INTERVAL {_ML_MAX_AGE_DAYS} DAY
                ORDER BY as_of DESC
                LIMIT 1
            """).result_rows
            if ml_row:
                ret_pct = float(ml_row[0][0])
                signals["momentum_return_pct"] = round(ret_pct, 3)
                momentum_score = round(
                    _clamp01(ret_pct, _SILVER_MOM_LOW, _SILVER_MOM_HIGH) * 100, 1
                )
                if as_of is None and ml_row[0][1]:
                    as_of = ml_row[0][1]
            else:
                raise ValueError("no ml_predictions row for SILVERBEES")
        except Exception:
            # Fallback: SI=F 5-day realised return
            ret5 = _fetch_silver_momentum()
            if ret5 is not None:
                signals["momentum_return_pct"] = ret5
                momentum_score = round(
                    _clamp01(ret5, _SILVER_MOM_LOW, _SILVER_MOM_HIGH) * 100, 1
                )
            else:
                errors.append("Silver momentum fetch failed — momentum pillar unavailable")

        # SILVERBEES price history for chart
        try:
            cutoff = (date.today() - timedelta(days=_LOOKBACK_DAYS)).isoformat()
            sb_rows = client.query(f"""
                SELECT trade_date, argMax(close, imported_at) AS close
                FROM market_data.daily_prices
                WHERE symbol = 'SILVERBEES' AND category = 'etfs'
                  AND trade_date >= toDate('{cutoff}')
                GROUP BY trade_date
                ORDER BY trade_date ASC
            """).result_rows
            if sb_rows:
                silverbees_prices = pd.DataFrame(sb_rows, columns=["trade_date", "close"])
                silverbees_prices["trade_date"] = pd.to_datetime(silverbees_prices["trade_date"])
                if as_of is None and len(silverbees_prices):
                    as_of = silverbees_prices["trade_date"].iloc[-1].date()
        except Exception as exc:
            errors.append(f"SILVERBEES prices query failed: {exc}")

        client.close()

    except Exception as exc:
        errors.append(f"ClickHouse connection failed: {exc}")
        log.error("Silver scorecard ClickHouse error: %s", exc)

    # ── 4. Composite ──────────────────────────────────────────────────────────
    composite = _composite({
        "macro":     macro_score,
        "flows":     flows_score,
        "valuation": valuation_score,
        "momentum":  momentum_score,
    })

    return {
        "composite_score":  composite,
        "macro_score":      macro_score,
        "flows_score":      flows_score,
        "valuation_score":  valuation_score,
        "momentum_score":   momentum_score,
        "signals":          signals,
        "silverbees_prices": silverbees_prices,
        "dxy_prices":       dxy_prices,
        "as_of":            as_of,
        "error":            "; ".join(errors) if errors else None,
    }


# ══════════════════════════════════════════════════════════════════════════════
# Copper scorecard
# ══════════════════════════════════════════════════════════════════════════════
#
# Copper is an industrial metal — macro pillar uses DXY + USDCNY (China demand
# proxy) instead of safe-haven real-yield signals.  No Indian ETF exists so
# there is no iNAV/valuation pillar; weights are redistributed to macro and
# momentum.
#
# Pillars:
#   Macro    (35%) — DXY (inverse) + USDCNY (lower CNY/USD = strong China = bullish)
#   Flows    (30%) — COT copper (CFTC code 085, "COPPER- #1") from live f_disagg.txt
#   Momentum (35%) — HG=F 5D + 20D realised return blend
#
# Scoring rules:
#   DXY:     100 if ≤ 100, 0 if ≥ 110  (same as gold)
#   USDCNY:  100 if ≤ 7.00 (strong CNY), 0 if ≥ 7.50  (weak CNY = bearish for copper)
#   COT:     mm_net/OI ≤ 20% → 100; ≥ 35% → 0
#   Momentum:return ≥ +3% → 100; ≤ -3% → 0  (wider band — copper is more volatile)
# ──────────────────────────────────────────────────────────────────────────────

_COPPER_COT_LOW  = 20.0
_COPPER_COT_HIGH = 35.0

_USDCNY_BULL = 7.00   # strong CNY (low rate) → full bull score
_USDCNY_BEAR = 7.50   # weak CNY (high rate)  → zero bull score

_COPPER_MOM_HIGH = +3.0   # HG=F 5/20D return ≥ +3% → full score
_COPPER_MOM_LOW  = -3.0   # HG=F 5/20D return ≤ −3% → zero score

# Copper pillar weights (no valuation pillar)
_W_COPPER_MACRO    = 0.35
_W_COPPER_FLOWS    = 0.30
_W_COPPER_MOMENTUM = 0.35


def _fetch_usdcny(lookback_days: int = 10) -> float | None:
    """Fetch latest USD/CNY rate from Yahoo Finance (USDCNY=X)."""
    try:
        import yfinance as yf
        from datetime import date as _date
        start = (_date.today() - timedelta(days=lookback_days)).isoformat()
        end   = (_date.today() + timedelta(days=1)).isoformat()
        raw = yf.download("USDCNY=X", start=start, end=end,
                          auto_adjust=True, progress=False)
        if raw.empty:
            return None
        closes = raw["Close"]
        if isinstance(closes, pd.DataFrame):
            closes = closes.iloc[:, 0]
        val = closes.dropna().iloc[-1]
        return round(float(val), 4)
    except Exception as exc:
        log.warning("USDCNY fetch failed: %s", exc)
        return None


def _fetch_copper_cot_live() -> dict[str, Any] | None:
    """
    Fetch the latest Copper COT row from the CFTC weekly TXT.
    CFTC market name: 'COPPER- #1 - COMMODITY EXCHANGE INC.'  (code 085).
    Returns dict with report_date, mm_net, open_interest — or None on failure.
    """
    import io
    import requests

    try:
        resp = requests.get(_CFTC_CUR_URL, headers=_UA_HEADERS, timeout=30)
        resp.raise_for_status()
        df_raw = pd.read_csv(io.StringIO(resp.text), header=None, low_memory=False)
        n = min(len(_CFTC_COLS_NEEDED), len(df_raw.columns))
        df_raw = df_raw.rename(columns={i: _CFTC_COLS_NEEDED[i] for i in range(n)})

        copper_df = df_raw[
            df_raw["Market_and_Exchange_Names"].str.match(r"^COPPER-", na=False)
        ]
        if copper_df.empty:
            log.warning("No 'COPPER-' rows found in CFTC TXT")
            return None

        row = copper_df.sort_values("Report_Date_as_YYYY-MM-DD", ascending=False).iloc[0]
        mm_long  = int(row["M_Money_Positions_Long_All"]  or 0)
        mm_short = int(row["M_Money_Positions_Short_All"] or 0)
        oi       = int(row["Open_Interest_All"]           or 0)
        rdate    = pd.to_datetime(str(row["Report_Date_as_YYYY-MM-DD"])[:10]).date()
        return {"report_date": rdate, "mm_net": mm_long - mm_short, "open_interest": oi}
    except Exception as exc:
        log.warning("Copper COT live fetch failed: %s", exc)
        return None


def _fetch_copper_momentum(lookback_days: int = _LOOKBACK_DAYS + 10) -> dict[str, Any]:
    """
    Fetch HG=F (copper futures) price history and compute 5D + 20D returns.
    Returns dict with ret5, ret20, last_price, copper_prices DataFrame.
    """
    try:
        import yfinance as yf
        from datetime import date as _date
        start = (_date.today() - timedelta(days=lookback_days)).isoformat()
        end   = (_date.today() + timedelta(days=1)).isoformat()
        raw = yf.download("HG=F", start=start, end=end,
                          auto_adjust=True, progress=False)
        if raw.empty or len(raw) < 6:
            return {}

        closes = raw["Close"]
        if isinstance(closes, pd.DataFrame):
            closes = closes.iloc[:, 0]
        closes = closes.dropna()

        last  = float(closes.iloc[-1])
        ret5  = float((closes.iloc[-1] - closes.iloc[max(0, len(closes)-6)]) /
                      closes.iloc[max(0, len(closes)-6)] * 100) if len(closes) >= 6 else None
        ret20 = float((closes.iloc[-1] - closes.iloc[max(0, len(closes)-21)]) /
                      closes.iloc[max(0, len(closes)-21)] * 100) if len(closes) >= 21 else None

        prices_df = (
            raw[["Close"]].rename(columns={"Close": "close"})
            .reset_index().rename(columns={"Date": "trade_date"})
            .tail(_LOOKBACK_DAYS).reset_index(drop=True)
        )
        if isinstance(prices_df["close"], pd.DataFrame):
            prices_df["close"] = prices_df["close"].iloc[:, 0]
        prices_df["trade_date"] = pd.to_datetime(prices_df["trade_date"]).dt.tz_localize(None)

        return {"ret5": ret5, "ret20": ret20, "last_price": last, "prices": prices_df}
    except Exception as exc:
        log.warning("Copper momentum fetch failed: %s", exc)
        return {}


def _composite_copper(macro: float | None, flows: float | None,
                      momentum: float | None) -> float | None:
    """Re-weighted composite for copper (no valuation pillar)."""
    weight_map = {
        "macro":    _W_COPPER_MACRO,
        "flows":    _W_COPPER_FLOWS,
        "momentum": _W_COPPER_MOMENTUM,
    }
    pairs = [("macro", macro), ("flows", flows), ("momentum", momentum)]
    total_w, weighted_sum = 0.0, 0.0
    for key, score in pairs:
        if score is not None:
            w = weight_map[key]
            total_w += w
            weighted_sum += w * score
    if total_w == 0.0:
        return None
    return round(weighted_sum / total_w, 1)


def compute_copper_scorecard() -> dict[str, Any]:
    """
    Compute the Composite Copper Score for HG=F (COMEX copper futures).

    No ClickHouse dependency — all data from Yahoo Finance + CFTC live TXT.
    Degrades gracefully if any pillar fails.
    """
    errors: list[str] = []
    signals: dict[str, Any] = {
        "dxy_level":         None,
        "usdcny":            None,
        "cot_pct_oi":        None,
        "cot_report_date":   None,
        "ret_5d":            None,
        "ret_20d":           None,
        "last_price":        None,
    }
    macro_score:    float | None = None
    flows_score:    float | None = None
    momentum_score: float | None = None
    copper_prices   = pd.DataFrame()
    dxy_prices      = pd.DataFrame()
    as_of: date | None = date.today()

    # ── 1. Macro: DXY + USDCNY ───────────────────────────────────────────────
    macro_df = _fetch_dxy_tnx(_LOOKBACK_DAYS + 10)
    usdcny   = _fetch_usdcny()

    macro_scores: list[float] = []

    if not macro_df.empty and "dxy_close" in macro_df.columns:
        dxy_series = macro_df["dxy_close"].dropna()
        if not dxy_series.empty:
            dxy_latest = float(dxy_series.iloc[-1])
            signals["dxy_level"] = round(dxy_latest, 2)
            macro_scores.append(_clamp01(dxy_latest, _DXY_LOW, _DXY_HIGH) * 100)

        dxy_prices = (
            macro_df[["trade_date", "dxy_close"]]
            .rename(columns={"dxy_close": "close"})
            .dropna()
            .tail(_LOOKBACK_DAYS)
            .reset_index(drop=True)
        )
    else:
        errors.append("DXY fetch failed")

    if usdcny is not None:
        signals["usdcny"] = usdcny
        # Lower USDCNY = stronger CNY = bullish for copper
        macro_scores.append(_clamp01(usdcny, _USDCNY_BULL, _USDCNY_BEAR) * 100)
    else:
        errors.append("USDCNY fetch failed")

    if macro_scores:
        macro_score = round(float(np.mean(macro_scores)), 1)
    else:
        errors.append("Macro pillar unavailable")

    # ── 2. Flows: Copper COT from CFTC live TXT ──────────────────────────────
    cot = _fetch_copper_cot_live()
    if cot is not None:
        oi = cot["open_interest"]
        if oi > 0:
            cot_pct = cot["mm_net"] / oi * 100
            signals["cot_pct_oi"]      = round(cot_pct, 2)
            signals["cot_report_date"] = str(cot["report_date"])
            flows_score = round(_clamp01(cot_pct, _COPPER_COT_LOW, _COPPER_COT_HIGH) * 100, 1)
        else:
            errors.append("Copper COT open_interest is zero")
    else:
        errors.append("Copper COT fetch failed — flows pillar unavailable")

    # ── 3. Momentum: HG=F price returns ──────────────────────────────────────
    mom_data = _fetch_copper_momentum()
    if mom_data:
        ret5  = mom_data.get("ret5")
        ret20 = mom_data.get("ret20")
        signals["ret_5d"]    = round(ret5,  2) if ret5  is not None else None
        signals["ret_20d"]   = round(ret20, 2) if ret20 is not None else None
        signals["last_price"] = round(mom_data["last_price"], 4)
        copper_prices = mom_data.get("prices", pd.DataFrame())

        # Blend 5D and 20D returns
        mom_scores = []
        if ret5  is not None: mom_scores.append(_clamp01(ret5,  _COPPER_MOM_LOW, _COPPER_MOM_HIGH) * 100)
        if ret20 is not None: mom_scores.append(_clamp01(ret20, _COPPER_MOM_LOW, _COPPER_MOM_HIGH) * 100)
        if mom_scores:
            momentum_score = round(float(np.mean(mom_scores)), 1)
    else:
        errors.append("Copper momentum fetch failed — momentum pillar unavailable")

    composite = _composite_copper(macro_score, flows_score, momentum_score)

    return {
        "composite_score": composite,
        "macro_score":     macro_score,
        "flows_score":     flows_score,
        "valuation_score": None,   # no ETF — not applicable
        "momentum_score":  momentum_score,
        "signals":         signals,
        "copper_prices":   copper_prices,
        "dxy_prices":      dxy_prices,
        "as_of":           as_of,
        "error":           "; ".join(errors) if errors else None,
    }
