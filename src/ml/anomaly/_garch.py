"""GARCH(1,1) residual fitting for the anomaly pipeline."""
from __future__ import annotations

import warnings

import numpy as np
import pandas as pd

from ._features import robust_zscore

_GARCH_CACHE: dict = {}


def fit_garch_residuals(
    df: pd.DataFrame,
) -> tuple[pd.DataFrame, float]:
    """
    Fit GARCH(1,1) with Student-t innovations on log-returns.
    Applies caching to prevent redundant MLE fits on the same dataset.
    """
    if df.empty:
        return df, 0.0

    last_row = df.iloc[-1]
    cache_key = (len(df), str(last_row["trade_date"]), float(last_row["close"]))
    if cache_key in _GARCH_CACHE:
        cached_cols, loglik = _GARCH_CACHE[cache_key]
        df_out = df.copy()
        for col in cached_cols.columns:
            if col != "trade_date":
                df_out[col] = cached_cols[col].values
        return df_out, loglik

    try:
        from arch import arch_model  # type: ignore[import]
    except ImportError as exc:
        raise ImportError(
            "arch library required: pip install arch>=6.3.0"
        ) from exc

    df = df.copy()
    returns = df["log_return"].dropna() * 100  # arch works in % scale

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        am  = arch_model(returns, vol="Garch", p=1, q=1, dist="t", rescale=False)
        res = am.fit(disp="off", show_warning=False)

    # arch returns vectors aligned to the *non-NaN* log_return rows (N-1 values)
    # We need to map them back to the full df index (N rows).
    valid_idx = df.index[df["log_return"].notna()]   # integer positions of valid rows

    cond_vol_pct = res.conditional_volatility.values   # (N-1,) daily σ in %
    cond_vol     = cond_vol_pct / 100                  # (N-1,) daily σ in log-return scale
    fitted_ret   = res.resid.values / 100              # (N-1,) GARCH-fitted log-returns

    # ── Annualised conditional volatility ────────────────────────────────────
    df["garch_vol"] = np.nan
    df.loc[valid_idx, "garch_vol"] = cond_vol_pct * np.sqrt(252)

    # ── rf_pred: close[t-1] × exp(fitted_ret[t]) for chart backward-compat ──
    prev_close_valid = df["close"].shift(1).values[valid_idx]  # (N-1,)
    df["rf_pred"] = np.nan
    df.loc[valid_idx, "rf_pred"] = prev_close_valid * np.exp(fitted_ret)

    # ── GARCH ±1σ / ±2σ price bands for chart ────────────────────────────────
    close_valid = df["close"].values[valid_idx]
    df["garch_band_1s"] = np.nan
    df["garch_band_2s"] = np.nan
    df.loc[valid_idx, "garch_band_1s"] = close_valid * (np.exp(cond_vol) - 1)
    df.loc[valid_idx, "garch_band_2s"] = close_valid * (np.exp(2 * cond_vol) - 1)

    # ── Standardised residuals e_t = r_t / σ_t  (the proper anomaly score) ──
    logret_valid = df["log_return"].values[valid_idx]   # (N-1,)
    std_resid_valid = logret_valid / cond_vol           # (N-1,)
    df["residual"] = np.nan
    df.loc[valid_idx, "residual"] = std_resid_valid

    df["z_resid"]     = robust_zscore(df["residual"].fillna(0))
    df["z_resid_abs"] = df["z_resid"].abs()

    loglik = float(res.loglikelihood)

    # Cache the computed GARCH columns
    cols_to_cache = [
        "trade_date", "garch_vol", "rf_pred", "garch_band_1s",
        "garch_band_2s", "residual", "z_resid", "z_resid_abs"
    ]
    _GARCH_CACHE[cache_key] = (df[cols_to_cache].copy(), loglik)

    return df, loglik
