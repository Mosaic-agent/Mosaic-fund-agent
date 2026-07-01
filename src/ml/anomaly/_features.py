"""Feature engineering for the anomaly detection pipeline."""
from __future__ import annotations

import logging

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)


def robust_zscore(s: pd.Series, window: int = 30) -> pd.Series:
    """
    MAD-based rolling robust Z-score.
    Formula: 0.6745 × (x − rolling_median) / rolling_MAD
    The constant 0.6745 makes the scale consistent with σ for Gaussian data.
    """
    rolling_med = s.rolling(window=window, min_periods=window // 2).median()
    rolling_mad = (s - rolling_med).abs().rolling(window=window, min_periods=window // 2).median()
    return 0.6745 * (s - rolling_med) / (rolling_mad + 1e-10)


def repair_decimal_glitches(df: pd.DataFrame) -> pd.DataFrame:
    """
    Detects and repairs decimal scaling errors (e.g. 10x or 100x shift for 1-2 days)
    on the fly.
    """
    if df.empty or len(df) < 5:
        return df

    df = df.copy()
    close = df["close"].values

    n = len(df)
    for i in range(1, n - 1):
        c_prev = close[i - 1]
        c_curr = close[i]
        c_next = close[i + 1]
        if c_prev <= 0 or c_curr <= 0 or c_next <= 0:
            continue

        ratio_down = c_curr / c_prev
        ratio_up = c_next / c_curr

        # Check for 1-day glitches
        for factor in [10.0, 100.0]:
            tol = 0.15
            if (abs(ratio_down - 1.0/factor) < tol/factor and abs(ratio_up - factor) < tol) or \
               (abs(ratio_down - factor) < tol and abs(ratio_up - 1.0/factor) < tol/factor):
                mult = factor if ratio_down < 1.0 else 1.0 / factor
                df.loc[i, ["open", "high", "low", "close"]] *= mult
                close = df["close"].values
                log.debug("Repaired single-day decimal scaling glitch on index %d (%s) for factor %f", i, df.iloc[i]["trade_date"], factor)
                break

        # Check for 2-day glitches
        if i < n - 2:
            c_next2 = close[i + 2]
            if c_next2 <= 0:
                continue
            ratio_down = c_curr / c_prev
            ratio_flat = c_next / c_curr
            ratio_up = c_next2 / c_next

            if abs(ratio_flat - 1.0) < 0.15:
                for factor in [10.0, 100.0]:
                    tol = 0.15
                    if (abs(ratio_down - 1.0/factor) < tol/factor and abs(ratio_up - factor) < tol) or \
                       (abs(ratio_down - factor) < tol and abs(ratio_up - 1.0/factor) < tol/factor):
                        mult = factor if ratio_down < 1.0 else 1.0 / factor
                        df.loc[i, ["open", "high", "low", "close"]] *= mult
                        df.loc[i+1, ["open", "high", "low", "close"]] *= mult
                        close = df["close"].values
                        log.debug("Repaired 2-day decimal scaling glitch on index %d-%d (%s) for factor %f", i, i+1, df.iloc[i]["trade_date"], factor)
                        break
    return df


def build_features(df: pd.DataFrame, rf_lags: int = 5) -> pd.DataFrame:
    """
    Add engineered features to a daily OHLCV DataFrame (sorted ascending by trade_date).

    Added columns: daily_return, log_return, range_pct.
    Input must have: trade_date, open, high, low, close, volume.
    Returns a new DataFrame — does NOT mutate the input.
    """
    # First repair decimal glitches on the fly to clean up noise
    df = repair_decimal_glitches(df)

    df = df.copy().sort_values("trade_date").reset_index(drop=True)
    df["daily_return"] = df["close"].pct_change() * 100

    # Yield protection: avoid negative or zero values in log return
    close_val = df["close"].values
    prev_close = df["close"].shift(1).values

    # Calculate log ratio with absolute value protection and safety epsilon
    with np.errstate(divide="ignore", invalid="ignore"):
        ratio = np.abs(close_val) / (np.abs(prev_close) + 1e-10)
        ratio[ratio <= 0] = 1.0
        df["log_return"] = np.log(ratio)

    df["range_pct"] = (df["high"] - df["low"]) / df["close"] * 100
    df["vol_lag1"]  = df["volume"].shift(1)
    return df
