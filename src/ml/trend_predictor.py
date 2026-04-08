"""
src/ml/trend_predictor.py
──────────────────────────
LightGBM 5-day forward return predictor for GOLDBEES.

Feature-to-Forward-Target strategy — the "Who Is Selling?" signals become
alpha factors with *soft* thresholds learned from data, not hard IF/THEN rules.

Alpha factors
─────────────
  f_logret1            Daily log return    ln(P_t/P_{t-1})            stationarity fix
  f_goldbees_logret5   Log Momentum        5-day log return
  f_goldbees_logret20  Log Momentum        20-day log return
  f_ema_cross39        EMA signal          EMA(3)/EMA(9)−1            short-term trend
  f_ema_cross920       EMA signal          EMA(9)/EMA(20)−1           medium trend
  f_ma_ratio           Mean-Reversion      close / 20-day SMA
  f_atr14_pct          Volatility regime   ATR(14) / close × 100
  f_hvol10             Historical vol      10-day log-return σ × √252
  f_cot_pct_oi         COT Leverage        mm_net / open_interest × 100
  f_spread_pct         Retail Spread       (price − nav) / nav × 100
  f_spread_delta5      Spread Momentum     5-day change in retail spread
  f_aum_mom_30d        AUM Momentum        30-day log % Δ of GLD total assets
  f_usdinr_vol14       Currency Stress     14-day USDINR log-return vol × 100
  f_usdinr_60d         INR Trend           60-day USDINR log % change
  f_dxy_proxy          DXY proxy           −(5-day USDINR log return × 100)
  f_gold_logret5       COMEX momentum      5-day log return of COMEX GOLD
  f_dxy_logret5        Real DXY            5-day DXY log return  (DX-Y.NYB via yfinance)
  f_dxy_logret20       Real DXY            20-day DXY log return
  f_us10y_level        US 10Y yield        absolute yield level  (rate-regime signal)
  f_us10y_delta5       US 10Y yield shock  5-day change in yield (^TNX via yfinance)
  f_real_yield         Real Yield proxy    f_us10y_level − 2.5% inflation constant
  f_real_yield_delta5  Real Yield trend    5-day change in real yield
  f_gsr                Gold-Silver Ratio   gold_close / silver_close  (risk-on/off regime)
  f_gsr_zscore         GSR z-score         rolling 60-day z-score of f_gsr
  f_month_sin/cos      Seasonality         cyclical month encoding (wedding / CNY / Q4)
  f_dow                Day-of-week         0=Mon → 1=Fri (Friday effect)
  f_fii_net_5d         FII Flow            5-day rolling sum of FII net cash flows (₹ Cr)
  f_dii_net_5d         DII Flow            5-day rolling sum of DII net cash flows (₹ Cr)
  f_inst_net_momentum  Inst. Impulse       5-day combined FII+DII net flow

Target: ln(close[t + horizon] / close[t])  — stationary log return
  (converted back to % for display)

Training: TimeSeriesSplit expanding-window walk-forward (gap = 2× horizon).
  min_train_size ensures early folds always have sufficient history.
Model:    LGBMRegressor with L1+L2 regularisation + early stopping.
  Early stopping uses the last 15% of each training fold as an internal
  validation set — test fold is never touched so there is no leakage.
Metrics:  CV R² per fold  +  Hit Ratio (directional accuracy)

Public API
──────────
    run_trend_prediction(horizon, n_splits, verbose) → dict
"""
from __future__ import annotations

import logging
from datetime import date
from typing import Any

import numpy as np
import pandas as pd

__all__ = [
    "build_master_table",
    "engineer_features",
    "label_forward_return",
    "fit_walk_forward",
    "run_trend_prediction",
]

log = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────────
_HORIZON   = 5     # forward return horizon in trading days
_N_SPLITS  = 5     # TimeSeriesSplit folds
_GAP       = 10    # gap = 2× horizon: prevents target-feature overlap at fold boundaries
_MIN_ROWS  = 120   # minimum clean rows required to train

# Regime thresholds (on predicted return %)
_BUY_THRESH         =  1.5
_WATCH_LONG_THRESH  =  0.5
_WATCH_SHORT_THRESH = -0.5
_SELL_THRESH        = -1.5

# ── ClickHouse SQL ─────────────────────────────────────────────────────────────
# CROSS JOIN + argMax forward-fills weekly COT onto daily dates (same validated
# pattern as the Global Anomaly Index chart — no experimental flags needed).
_MASTER_SQL = """
    SELECT
        p.trade_date  AS trade_date,
        p.close       AS goldbees_close,
        n.nav         AS goldbees_nav,
        f.close       AS usdinr,
        aum.aum_usd   AS gld_aum_usd,
        cot.mm_net    AS cot_mm_net,
        cot.oi        AS cot_oi,
        gold.gold_close AS gold_close,
        fii.fii_net_cr  AS fii_net_cr,
        fii.dii_net_cr  AS dii_net_cr
    FROM (
        SELECT trade_date, close
        FROM market_data.daily_prices FINAL
        WHERE symbol = 'GOLDBEES' AND category = 'etfs'
    ) p
    LEFT JOIN (
        SELECT nav_date AS trade_date, nav
        FROM market_data.mf_nav FINAL
        WHERE symbol = 'GOLDBEES'
    ) n ON p.trade_date = n.trade_date
    LEFT JOIN (
        SELECT trade_date, close
        FROM market_data.fx_rates FINAL
        WHERE symbol = 'USDINR'
    ) f ON p.trade_date = f.trade_date
    LEFT JOIN (
        SELECT trade_date, aum_usd
        FROM market_data.etf_aum FINAL
        WHERE symbol = 'GLD'
    ) aum ON p.trade_date = aum.trade_date
    LEFT JOIN (
        SELECT
            d.trade_date,
            argMax(c.mm_net,        c.report_date) AS mm_net,
            argMax(c.open_interest, c.report_date) AS oi
        FROM (
            SELECT DISTINCT trade_date
            FROM market_data.daily_prices FINAL
            WHERE symbol = 'GOLDBEES' AND category = 'etfs'
        ) d
        CROSS JOIN market_data.cot_gold c
        WHERE c.report_date <= d.trade_date
        GROUP BY d.trade_date
    ) cot ON p.trade_date = cot.trade_date
    LEFT JOIN (
        SELECT trade_date, close AS gold_close
        FROM market_data.daily_prices FINAL
        WHERE symbol = 'GOLD' AND category = 'commodities'
    ) gold ON p.trade_date = gold.trade_date
    LEFT JOIN (
        SELECT trade_date, close AS silver_close
        FROM market_data.daily_prices FINAL
        WHERE symbol = 'SILVER' AND category = 'commodities'
    ) silver ON p.trade_date = silver.trade_date
    LEFT JOIN (
        SELECT trade_date, fii_net_cr, dii_net_cr
        FROM market_data.fii_dii_flows FINAL
    ) fii ON p.trade_date = fii.trade_date
    ORDER BY p.trade_date ASC
"""


# ── Macro series helper ────────────────────────────────────────────────────────

def _fetch_macro_series(start_date: str, end_date: str) -> pd.DataFrame:
    """
    Fetch real DXY (DX-Y.NYB) and US 10-year yield (^TNX) from Yahoo Finance.

    Both are primary global gold price drivers:
      DXY   — Dollar strength: gold and DXY move inversely ~70% of the time.
      US10Y — Real rate proxy: rising yields are a consistent headwind for gold.

    Returns a DataFrame indexed by trade_date with columns dxy_close, us10y_close.
    Degrades gracefully to an empty DataFrame on any fetch failure.
    """
    try:
        import yfinance as yf
        dxy_raw = yf.download(
            "DX-Y.NYB", start=start_date, end=end_date,
            auto_adjust=True, progress=False,
        )
        tnx_raw = yf.download(
            "^TNX", start=start_date, end=end_date,
            auto_adjust=True, progress=False,
        )
        pieces: list[pd.DataFrame] = []
        if not dxy_raw.empty:
            dxy = dxy_raw[["Close"]].copy()
            dxy.columns = ["dxy_close"]
            pieces.append(dxy)
        if not tnx_raw.empty:
            tnx = tnx_raw[["Close"]].copy()
            tnx.columns = ["us10y_close"]
            pieces.append(tnx)
        if not pieces:
            return pd.DataFrame(columns=["trade_date", "dxy_close", "us10y_close"])
        df_macro = pieces[0]
        for extra in pieces[1:]:
            df_macro = df_macro.join(extra, how="outer")
        df_macro = df_macro.reset_index().rename(columns={"Date": "trade_date"})
        df_macro["trade_date"] = pd.to_datetime(df_macro["trade_date"]).dt.tz_localize(None)
        return df_macro.sort_values("trade_date").reset_index(drop=True)
    except Exception as exc:
        log.warning("Macro series (DXY/TNX) fetch failed — features will be NaN: %s", exc)
        return pd.DataFrame(columns=["trade_date", "dxy_close", "us10y_close"])


# ── Step 1: Data assembly ──────────────────────────────────────────────────────

def build_master_table(ch_client) -> pd.DataFrame:
    """
    Pull and join all signal sources from ClickHouse into one flat table.

    GLD AUM is sparse (daily snapshots; most rows are NaN) — forward-filled
    from the closest available value so LightGBM receives a usable signal.
    Also merges real DXY + US 10Y yield from Yahoo Finance (degrades gracefully
    to NaN features if the fetch fails or the network is unavailable).
    """
    df = ch_client.query_df(_MASTER_SQL)
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    df = df.sort_values("trade_date").reset_index(drop=True)
    df["gld_aum_usd"]   = df["gld_aum_usd"].replace(0, np.nan).ffill()
    # Forward-fill NAV: AMFI publishes end-of-day; latest trading day may have
    # no NAV yet. ffill carries the last known NAV forward (same-day approximation).
    df["goldbees_nav"]  = df["goldbees_nav"].replace(0, np.nan).ffill()

    # Merge real DXY + US 10Y yield — primary global gold price drivers
    start    = str(df["trade_date"].min().date())
    end      = str((df["trade_date"].max() + pd.Timedelta(days=3)).date())
    df_macro = _fetch_macro_series(start, end)
    if not df_macro.empty:
        df = df.merge(df_macro, on="trade_date", how="left")
        df["dxy_close"]   = df["dxy_close"].ffill()
        df["us10y_close"] = df["us10y_close"].ffill()

    log.info(
        "Master table: %d rows, %s → %s",
        len(df),
        df["trade_date"].min().date(),
        df["trade_date"].max().date(),
    )
    return df


# ── Step 2: Feature engineering ───────────────────────────────────────────────

def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform raw columns into stationary alpha factors.

    All feature columns are prefixed with ``f_`` for automatic selection.
    Input must have: goldbees_close, goldbees_nav, usdinr, gld_aum_usd,
                     cot_mm_net, cot_oi.
    Optional (silently skipped if absent): gold_close.
    """
    df = df.copy().sort_values("trade_date").reset_index(drop=True)

    p   = df["goldbees_close"].replace(0, np.nan)
    lnp = np.log(p)  # log price — differences give stationary log returns

    # ── 1. Stationary log-return features (fix level bias) ───────────────────
    df["f_logret1"]           = lnp.diff(1)
    df["f_goldbees_logret5"]  = lnp.diff(5)
    df["f_goldbees_logret20"] = lnp.diff(20)

    # ── 2. EMA crossover signals (trend vs mean-reversion regime) ────────────
    ema3  = p.ewm(span=3,  adjust=False).mean()
    ema9  = p.ewm(span=9,  adjust=False).mean()
    ema20 = p.ewm(span=20, adjust=False).mean()
    df["f_ema_cross39"]  = ema3  / ema9.replace(0, np.nan)  - 1
    df["f_ema_cross920"] = ema9  / ema20.replace(0, np.nan) - 1

    # ── 3. Mean-reversion: close / 20-day SMA ────────────────────────────────
    df["f_ma_ratio"] = p / p.rolling(20).mean()

    # ── 4. Volatility regime features ────────────────────────────────────────
    # ATR(14) as % of close — real range when OHLC present, logret proxy otherwise
    if "goldbees_high" in df.columns and "goldbees_low" in df.columns:
        h  = df["goldbees_high"].replace(0, np.nan)
        lo = df["goldbees_low"].replace(0, np.nan)
        tr = pd.concat([
            h - lo,
            (h - p.shift(1)).abs(),
            (lo - p.shift(1)).abs(),
        ], axis=1).max(axis=1)
        df["f_atr14_pct"] = tr.rolling(14).mean() / p * 100
    else:
        df["f_atr14_pct"] = df["f_logret1"].abs().rolling(14).mean() * 100

    # 10-day historical volatility (annualised)
    df["f_hvol10"] = df["f_logret1"].rolling(10).std() * np.sqrt(252)

    # ── 5. COT leverage — speculator over-positioning ────────────────────────
    df["f_cot_pct_oi"] = df["cot_mm_net"] / df["cot_oi"].replace(0, np.nan) * 100

    # ── 6. Retail spread — GOLDBEES premium/discount to AMFI NAV ─────────────
    nav_safe = df["goldbees_nav"].replace(0, np.nan)
    df["f_spread_pct"]    = (p - nav_safe) / nav_safe * 100
    df["f_spread_delta5"] = df["f_spread_pct"].diff(5)

    # ── 7. AUM momentum — 30-day log % change in GLD total assets ────────────
    aum_safe = df["gld_aum_usd"].replace(0, np.nan)
    df["f_aum_mom_30d"] = np.log(aum_safe / aum_safe.shift(30)) * 100

    # ── 8. Currency stress + trend ────────────────────────────────────────────
    inr_safe   = df["usdinr"].replace(0, np.nan)
    inr_logret = np.log(inr_safe / inr_safe.shift(1))
    df["f_usdinr_vol14"] = inr_logret.rolling(14).std() * 100
    df["f_usdinr_60d"]   = np.log(inr_safe / inr_safe.shift(60)) * 100
    # DXY proxy: USDINR up = Dollar strong = headwind for gold → negate
    df["f_dxy_proxy"] = -np.log(inr_safe / inr_safe.shift(5)) * 100

    # ── 9. COMEX Gold momentum ────────────────────────────────────────────────
    if "gold_close" in df.columns:
        gc = df["gold_close"].replace(0, np.nan)
        df["f_gold_logret5"] = np.log(gc / gc.shift(5)) * 100

    # ── 10. Real DXY — Dollar Index (replaces the USDINR proxy when available) ─
    if "dxy_close" in df.columns:
        dxy = df["dxy_close"].replace(0, np.nan)
        df["f_dxy_logret5"]  = np.log(dxy / dxy.shift(5))  * 100
        df["f_dxy_logret20"] = np.log(dxy / dxy.shift(20)) * 100

    # ── 11. US 10Y yield — real rate proxy (core gold headwind / tailwind) ────
    if "us10y_close" in df.columns:
        y10 = df["us10y_close"].replace(0, np.nan)
        df["f_us10y_level"]  = y10           # absolute level signals rate regime
        df["f_us10y_delta5"] = y10.diff(5)   # 5-day yield shock

    # ── 12. Real Yield proxy — US 10Y minus fixed inflation constant ────────────
    # Rising real yields are a persistent headwind for non-yielding gold.
    # Fixed 2.5% inflation constant is the Fed's asymmetric target mid-point.
    if "us10y_close" in df.columns:
        df["f_real_yield"]        = df["f_us10y_level"] - 2.5
        df["f_real_yield_delta5"] = df["f_real_yield"].diff(5)

    # ── 13. Gold-Silver Ratio — risk-on / risk-off regime indicator ────────────
    # GSR above 80+: silver under-performs → risk-off / recession fears → gold bid.
    # GSR below 60:  silver outperforms  → industrial demand / risk-on rally.
    # z-score normalises the ratio to a stationary signal.
    if "silver_close" in df.columns and "gold_close" in df.columns:
        sc = df["silver_close"].replace(0, np.nan)
        gc = df["gold_close"].replace(0, np.nan)
        df["f_gsr"]        = gc / sc
        df["f_gsr_zscore"] = (
            (df["f_gsr"] - df["f_gsr"].rolling(60).mean())
            / df["f_gsr"].rolling(60).std().replace(0, np.nan)
        )

    # ── 14. Calendar seasonality (cyclical encoding) ──────────────────────────
    # Known gold seasonals: Indian wedding season (Oct–Nov), Chinese New Year
    # (Jan–Feb), Q4 institutional rebalancing, Friday option-expiry effect.
    df["f_month_sin"] = np.sin(2 * np.pi * df["trade_date"].dt.month / 12)
    df["f_month_cos"] = np.cos(2 * np.pi * df["trade_date"].dt.month / 12)
    df["f_dow"]       = df["trade_date"].dt.dayofweek / 4.0   # 0=Mon → 1=Fri

    # ── 13. Institutional flow features (FII / DII) ───────────────────────────
    # FII and DII net flows are major NSE market drivers.  Using rolling 5-day
    # sums makes the signal stationary and reduces single-day noise.
    if "fii_net_cr" in df.columns and "dii_net_cr" in df.columns:
        fii_net = df["fii_net_cr"].fillna(0.0)
        dii_net = df["dii_net_cr"].fillna(0.0)
        df["f_fii_net_5d"]         = fii_net.rolling(5).sum()
        df["f_dii_net_5d"]         = dii_net.rolling(5).sum()
        df["f_inst_net_momentum"]   = (fii_net + dii_net).rolling(5).sum()

    return df


# ── Step 3: Target labeling ───────────────────────────────────────────────────

def label_forward_return(df: pd.DataFrame, horizon: int = _HORIZON) -> pd.DataFrame:
    """
    Add the forward log-return target (stationary, eliminates level bias).

        target = ln(close[t + horizon] / close[t])

    Using log returns avoids the non-stationary level problem that causes
    negative R² when the model's intercept drifts from the true mean.
    The last `horizon` rows will have NaN targets and are excluded from training.
    They are kept so the latest row can still be used for live prediction.
    """
    df = df.copy()
    p_safe = df["goldbees_close"].replace(0, np.nan)
    df["target"] = np.log(p_safe.shift(-horizon) / p_safe)
    return df


# ── Step 4: Walk-forward training ─────────────────────────────────────────────

def fit_walk_forward(
    df: pd.DataFrame,
    n_splits: int = _N_SPLITS,
    gap: int = _GAP,
) -> tuple[Any, pd.DataFrame, list[float], list[float], pd.DataFrame, list[str]]:
    """
    Train LGBMRegressor with expanding-window TimeSeriesSplit walk-forward CV.

    Parameters
    ----------
    df       : DataFrame with all f_* columns and 'target'
    n_splits : number of CV folds
    gap      : rows between train end and test start (prevents look-ahead)

    Returns
    -------
    model         : LGBMRegressor fit on the last fold's full train set
    fi_df         : feature importances averaged over last 3 folds
    cv_r2_scores  : list of out-of-sample R² per fold
    cv_hit_ratios : list of directional accuracy (hit ratio) per fold
    df_clean      : training-eligible rows
    feature_cols  : list of feature names used
    """
    try:
        import lightgbm as lgb
    except (ImportError, OSError) as exc:
        if "libomp" in str(exc) or "lightgbm" in str(exc).lower():
            import os, sys
            # macOS: inject the Homebrew libomp path so dlopen can find it
            libomp_path = "/opt/homebrew/opt/libomp/lib"
            current = os.environ.get("DYLD_LIBRARY_PATH", "")
            if libomp_path not in current:
                os.environ["DYLD_LIBRARY_PATH"] = (
                    f"{libomp_path}:{current}" if current else libomp_path
                )
            # Force a fresh dlopen attempt by removing cached failed import
            sys.modules.pop("lightgbm", None)
            try:
                import lightgbm as lgb  # noqa: F811
            except (ImportError, OSError) as exc2:
                raise OSError(
                    "LightGBM could not load libomp.dylib.\n"
                    "Fix: brew install libomp\n"
                    "If the error persists, add to ~/.zshrc:\n"
                    "  export DYLD_LIBRARY_PATH=/opt/homebrew/opt/libomp/lib:$DYLD_LIBRARY_PATH"
                ) from exc2
        else:
            raise ImportError(
                "lightgbm is not installed. Run: .venv/bin/pip install lightgbm"
            ) from exc

    from sklearn.model_selection import TimeSeriesSplit

    feature_cols = sorted(c for c in df.columns if c.startswith("f_"))

    # Non-NaN target required; LightGBM handles missing feature values natively.
    # Keep rows where at least half the features are present so sparse columns
    # (f_aum_mom_30d, f_gold_logret5) don't eliminate every row.
    df_clean = df.dropna(subset=["target"]).copy()
    min_features_required = max(1, len(feature_cols) // 2)
    feature_coverage = df_clean[feature_cols].notna().sum(axis=1)
    df_clean = df_clean[feature_coverage >= min_features_required].reset_index(drop=True)

    if len(df_clean) < _MIN_ROWS:
        raise ValueError(
            f"Only {len(df_clean)} clean rows available — need ≥ {_MIN_ROWS}. "
            "Run: mosaic import --category etfs mf cot"
        )

    X = df_clean[feature_cols]   # DataFrame preserves feature names in LightGBM
    y = df_clean["target"].values

    # min_train_size: ensure early folds have enough history to train on
    min_train_size = max(_MIN_ROWS // (n_splits + 1), 60)
    tscv = TimeSeriesSplit(n_splits=n_splits, gap=gap)
    models: list[Any]   = []
    scores: list[float] = []
    hit_ratios: list[float] = []

    for train_idx, test_idx in tscv.split(X):
        if len(train_idx) < min_train_size:
            continue  # skip folds where training set is too thin

        # Reserve the last 15% of the training window as an internal validation
        # set for early stopping.  The held-out test fold is never used here —
        # no hyperparameter leakage.  Fall back to full fit when fold is tiny.
        n_train  = len(train_idx)
        val_size = max(int(n_train * 0.15), 20)
        use_es   = val_size < n_train
        if use_es:
            train_part = train_idx[: n_train - val_size]
            val_part   = train_idx[n_train - val_size :]
        else:
            train_part = train_idx

        m = lgb.LGBMRegressor(
            n_estimators      = 800,   # high ceiling; early stopping decides actual rounds
            learning_rate     = 0.03,
            max_depth         = 4,
            num_leaves        = 15,
            subsample         = 0.8,
            colsample_bytree  = 0.7,
            min_child_samples = 15,
            reg_alpha         = 0.15,   # L1 — prunes noisy features
            reg_lambda        = 0.15,   # L2 — shrinks coefficients
            random_state      = 42,
            verbose           = -1,
        )
        if use_es:
            m.fit(
                X.iloc[train_part], y[train_part],
                eval_set=[(X.iloc[val_part], y[val_part])],
                callbacks=[
                    lgb.early_stopping(stopping_rounds=50, verbose=False),
                    lgb.log_evaluation(period=0),
                ],
            )
        else:
            m.fit(X.iloc[train_idx], y[train_idx])

        y_pred = m.predict(X.iloc[test_idx])
        y_true = y[test_idx]
        scores.append(float(m.score(X.iloc[test_idx], y_true)))
        # Hit ratio: fraction of days where predicted direction == actual
        hit_ratios.append(float(np.mean(np.sign(y_pred) == np.sign(y_true))))
        models.append(m)

    if not models:
        raise ValueError(
            "All CV folds were skipped (too few rows per fold). "
            "Reduce n_splits or import more data."
        )

    # Average importances over the last k folds for stability
    k = min(3, len(models))
    avg_imp = np.mean(
        [m.feature_importances_ for m in models[-k:]], axis=0
    ).tolist()
    fi_df = (
        pd.DataFrame({"feature": feature_cols, "importance": avg_imp})
        .sort_values("importance", ascending=False)
        .reset_index(drop=True)
    )

    log.info(
        "Walk-forward: %d folds, R²mean=%.4f, hit_mean=%.3f",
        len(models), np.mean(scores), np.mean(hit_ratios),
    )
    return models[-1], fi_df, scores, hit_ratios, df_clean, feature_cols


# ── Step 5: Public API ────────────────────────────────────────────────────────

def run_trend_prediction(
    horizon: int = _HORIZON,
    n_splits: int = _N_SPLITS,
    verbose: bool = True,
    ch_host: str = "localhost",
    ch_port: int = 8123,
    ch_database: str = "market_data",
    ch_user: str = "default",
    ch_password: str = "",
) -> dict[str, Any]:
    """
    End-to-end LightGBM trend predictor.

    Connects to ClickHouse, assembles the master table, engineers features,
    trains with walk-forward CV, and predicts the expected return for the
    *next* `horizon` trading days from today.

    Returns
    -------
    dict with keys:
      expected_return_pct  float   model's predicted forward return %
      confidence_low/high  float   heuristic ±band based on CV R² spread
      regime_signal        str     BUY | WATCH_LONG | HOLD | WATCH_SHORT | SELL
      regime_rationale     str     plain-English explanation
      feature_importances  DataFrame  (feature, importance) sorted desc
      cv_r2_scores         list[float]
      cv_r2_mean           float
      n_training_rows      int
      as_of                date
      horizon_days         int
    """
    import clickhouse_connect

    client = clickhouse_connect.get_client(
        host=ch_host, port=ch_port, database=ch_database,
        username=ch_user, password=ch_password,
    )
    try:
        df_raw = build_master_table(client)
    finally:
        client.close()

    df_feat    = engineer_features(df_raw)
    df_labeled = label_forward_return(df_feat, horizon=horizon)

    model, fi_df, scores, hit_ratios, df_clean, feature_cols = fit_walk_forward(
        df_labeled, n_splits=n_splits, gap=_GAP
    )

    # Predict on the latest row that has sufficient feature coverage.
    df_feat_recent   = df_feat[feature_cols].copy()
    coverage_pred    = df_feat_recent.notna().sum(axis=1)
    df_pred_eligible = df_feat_recent[coverage_pred >= (len(feature_cols) // 2)]
    if df_pred_eligible.empty:
        raise ValueError(
            "No recent rows with sufficient feature coverage for prediction. "
            "Run: mosaic import --category etfs mf cot"
        )
    latest_row  = df_pred_eligible.iloc[[-1]]
    pred_logret = float(model.predict(latest_row)[0])   # log return (stationary)
    pred        = (np.exp(pred_logret) - 1) * 100        # convert to % for display

    # Confidence band widens when CV scores are inconsistent
    cv_std    = float(np.std(scores))
    band_half = max(abs(pred) * (1.0 + cv_std), 0.3)

    # Regime classification
    if pred >= _BUY_THRESH:
        regime    = "BUY"
        rationale = (
            f"Model expects +{pred:.2f}% over {horizon}d. "
            "All signals pointing constructive — consider adding on dips."
        )
    elif pred >= _WATCH_LONG_THRESH:
        regime    = "WATCH_LONG"
        rationale = (
            f"Model expects +{pred:.2f}% over {horizon}d. "
            "Mild upside expected — hold current position."
        )
    elif pred >= _WATCH_SHORT_THRESH:
        regime    = "HOLD"
        rationale = (
            f"Model expects {pred:+.2f}% over {horizon}d. "
            "No strong edge in either direction."
        )
    elif pred >= _SELL_THRESH:
        regime    = "WATCH_SHORT"
        rationale = (
            f"Model expects {pred:.2f}% over {horizon}d. "
            "Mild downside risk — reduce aggressive long exposure."
        )
    else:
        regime    = "SELL"
        rationale = (
            f"Model expects {pred:.2f}% over {horizon}d. "
            "Strong negative signal — consider reducing GOLDBEES holdings."
        )

    result: dict[str, Any] = {
        "expected_return_pct": round(pred, 3),
        "confidence_low":      round(pred - band_half, 3),
        "confidence_high":     round(pred + band_half, 3),
        "regime_signal":       regime,
        "regime_rationale":    rationale,
        "feature_importances": fi_df,
        "cv_r2_scores":        [round(s, 4) for s in scores],
        "cv_r2_mean":          round(float(np.mean(scores)), 4),
        "cv_hit_ratios":       [round(h, 4) for h in hit_ratios],
        "cv_hit_ratio_mean":   round(float(np.mean(hit_ratios)), 4),
        "n_training_rows":     len(df_clean),
        "horizon_days":        horizon,
        "as_of":               date.today(),
    }
    # ── Persist prediction to ClickHouse + JSONL fallback ────────────────────
    import json, pathlib

    _pred_row = {
        "as_of":               result["as_of"],
        "horizon_days":        result["horizon_days"],
        "expected_return_pct": result["expected_return_pct"],
        "confidence_low":      result["confidence_low"],
        "confidence_high":     result["confidence_high"],
        "regime_signal":       result["regime_signal"],
        "cv_r2_mean":          result["cv_r2_mean"],
        "n_training_rows":     result["n_training_rows"],
        "goldbees_close":      round(float(df_feat["goldbees_close"].iloc[-1]), 4),
    }

    # ClickHouse — create table if missing, then upsert
    try:
        import clickhouse_connect as _cc
        _ch = _cc.get_client(
            host=ch_host, port=ch_port, database=ch_database,
            username=ch_user, password=ch_password,
        )
        _ch.command("""
            CREATE TABLE IF NOT EXISTS market_data.ml_predictions (
                as_of                Date,
                horizon_days         UInt8,
                expected_return_pct  Float64,
                confidence_low       Float64,
                confidence_high      Float64,
                regime_signal        String,
                cv_r2_mean           Float64,
                n_training_rows      UInt32,
                goldbees_close       Float64,
                created_at           DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(created_at)
            ORDER BY (as_of, horizon_days)
        """)
        _ch.insert(
            "market_data.ml_predictions",
            [[
                _pred_row["as_of"], _pred_row["horizon_days"],
                _pred_row["expected_return_pct"], _pred_row["confidence_low"],
                _pred_row["confidence_high"], _pred_row["regime_signal"],
                _pred_row["cv_r2_mean"], _pred_row["n_training_rows"],
                _pred_row["goldbees_close"],
            ]],
            column_names=[
                "as_of", "horizon_days", "expected_return_pct",
                "confidence_low", "confidence_high", "regime_signal",
                "cv_r2_mean", "n_training_rows", "goldbees_close",
            ],
        )
        _ch.close()
        log.info("Prediction logged to market_data.ml_predictions")
    except Exception as _e:
        log.warning("Could not write prediction to ClickHouse: %s", _e)

    # JSONL fallback — git-trackable, one line per (as_of, horizon_days)
    _log_path = pathlib.Path(__file__).parents[2] / "predictions_log.jsonl"
    _existing: set = set()
    if _log_path.exists():
        for _line in _log_path.read_text().splitlines():
            try:
                _e2 = json.loads(_line)
                _existing.add((_e2.get("as_of"), _e2.get("horizon_days")))
            except json.JSONDecodeError:
                pass
    _entry = {k: str(v) if isinstance(v, date) else v for k, v in _pred_row.items()}
    if (_entry["as_of"], _entry["horizon_days"]) not in _existing:
        with _log_path.open("a") as _f:
            _f.write(json.dumps(_entry) + "\n")
    if verbose:
        log.info(
            "Prediction: %+.3f%% in %dd | regime=%s | CV R²=%.4f | hit=%.3f",
            pred, horizon, regime, result["cv_r2_mean"], result["cv_hit_ratio_mean"],
        )
    return result


# ── CLI smoke-test ────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    import os
    import logging as _logging

    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
    _logging.basicConfig(level=_logging.INFO, format="%(levelname)s %(message)s")

    out = run_trend_prediction()
    print(f"\n{'='*62}")
    print(f"  LGBM TREND PREDICTOR — {out['as_of']}")
    print(f"{'='*62}")
    print(f"  Expected {out['horizon_days']}-day return : {out['expected_return_pct']:+.3f}%")
    print(f"  Confidence band        : [{out['confidence_low']:+.3f}%, {out['confidence_high']:+.3f}%]")
    print(f"  Regime signal          : {out['regime_signal']}")
    print(f"\n  {out['regime_rationale']}")
    print(f"\n  CV R² per fold : {out['cv_r2_scores']}")
    print(f"  CV R² mean     : {out['cv_r2_mean']}")
    print(f"  Hit ratio mean : {out['cv_hit_ratio_mean']:.1%}  (>52% = useful edge)")
    print(f"  Training rows  : {out['n_training_rows']}")
    print(f"\n  Feature importances (top 5):")
    max_imp = out["feature_importances"]["importance"].max()
    for _, row in out["feature_importances"].head(5).iterrows():
        bar = "█" * max(1, int(row["importance"] / max_imp * 20))
        print(f"    {row['feature']:25s}  {bar}  {row['importance']:.1f}")
    print()
