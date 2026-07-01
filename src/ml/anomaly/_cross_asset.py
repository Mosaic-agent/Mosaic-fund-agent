"""Cross-asset feature injection (COT + USDINR) for the anomaly pipeline."""
from __future__ import annotations

import numpy as np
import pandas as pd


def _inject_cross_asset(
    df: pd.DataFrame,
    df_cot: pd.DataFrame | None,
    df_fx: pd.DataFrame | None,
) -> pd.DataFrame:
    """
    Left-join COT and USDINR features onto the OHLCV DataFrame.

    df_cot columns expected : report_date, mm_net, open_interest
    df_fx  columns expected : symbol, trade_date, close
    """
    df = df.copy()

    # ── COT speculator crowding (weekly → daily forward-fill) ──────────────
    if df_cot is not None and len(df_cot) > 10:
        cot = df_cot[["report_date", "mm_net", "open_interest"]].copy()
        cot = cot.rename(columns={"report_date": "trade_date"})
        cot["cot_pct_oi"] = cot["mm_net"] / (cot["open_interest"] + 1e-6) * 100
        cot["trade_date"] = pd.to_datetime(cot["trade_date"])
        df["trade_date"]  = pd.to_datetime(df["trade_date"])
        df = df.merge(cot[["trade_date", "cot_pct_oi"]], on="trade_date", how="left")
        df["cot_pct_oi"]  = df["cot_pct_oi"].ffill().fillna(0.0)

    # ── USDINR dollar-stress features ─────────────────────────────────────
    if df_fx is not None and len(df_fx) > 10:
        usdinr = df_fx[df_fx["symbol"] == "USDINR"][["trade_date", "close"]].copy()
        usdinr = usdinr.sort_values("trade_date").reset_index(drop=True)
        usdinr["usdinr_logret"] = np.log(usdinr["close"] / usdinr["close"].shift(1))
        usdinr["usdinr_vol14"]  = (
            usdinr["usdinr_logret"]
            .rolling(14, min_periods=7)
            .std() * np.sqrt(252) * 100
        )
        usdinr["trade_date"] = pd.to_datetime(usdinr["trade_date"])
        df = df.merge(
            usdinr[["trade_date", "usdinr_logret", "usdinr_vol14"]],
            on="trade_date", how="left",
        )
        df[["usdinr_logret", "usdinr_vol14"]] = (
            df[["usdinr_logret", "usdinr_vol14"]].fillna(0.0)
        )

    return df
