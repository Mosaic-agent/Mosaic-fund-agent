"""
src/tools/weight_checkpoint.py
────────────────────────────────
ClickHouse I/O for the weight_checkpoints table.

Three responsibilities:
  1. save_checkpoints()    — bulk insert decisions
  2. evaluate_methods()    — JOIN checkpoints to daily_prices to get realised returns
  3. latest_decisions()    — most recent decision per method (for CLI display)

The evaluation query avoids maintaining a 'realised_return' column in the table.
Realised returns are computed on demand via an ASOF JOIN to daily_prices so the
data is always derived from the authoritative price series, not a cached value
that could go stale.
"""
from __future__ import annotations

import logging
from datetime import date, timedelta

import pandas as pd

logger = logging.getLogger(__name__)


def _client():
    """Build a ClickHouseImporter connected with settings from .env."""
    from src.importer.clickhouse import ClickHouseImporter
    return ClickHouseImporter()


def save_checkpoints(decisions: list[dict]) -> int:
    """
    Bulk insert weight decisions into market_data.weight_checkpoints.

    Each dict in `decisions` matches the insert_weight_checkpoints() schema:
        as_of, symbol, method, recommended_weight, regime, rationale
        (+ optional: expected_return_pct, expected_vol_pct, garch_vol_pct,
           composite_score, cv_r2, price_below_ema50, horizon_days)

    Returns number of rows inserted.
    """
    ch = _client()
    ch.ensure_schema()
    return ch.insert_weight_checkpoints(decisions)


def evaluate_methods(
    symbol:       str = "GOLDBEES",
    since_days:   int = 90,
    horizon_days: int = 5,
) -> pd.DataFrame:
    """
    Compute realised performance for each method in weight_checkpoints.

    For every checkpoint row with as_of >= today - since_days and
    as_of + horizon_days <= today (so the forward window has closed),
    we JOIN to daily_prices to get entry and exit close prices, then
    compute weighted log returns and aggregate by method.

    Returns a DataFrame with columns:
        method, n, total_return_pct, ann_return_pct, ann_vol_pct,
        sharpe, hit_ratio_pct, avg_weight
    """
    from src.db.pool import get_pool as _get_ch_pool

    since_date = (date.today() - timedelta(days=since_days)).isoformat()
    # Only evaluate rows whose horizon has closed
    max_as_of  = (date.today() - timedelta(days=horizon_days)).isoformat()

    # Fetch checkpoints and prices separately, join in Python to avoid
    # ClickHouse 24.x inequality-join restriction in ON clauses.
    try:
        _pool = _get_ch_pool()
        chk = _pool.query_df(f"""
            SELECT as_of, method, recommended_weight,
                   toInt32(horizon_days) AS horizon_days
            FROM market_data.weight_checkpoints FINAL
            WHERE symbol = '{symbol}'
              AND as_of >= toDate('{since_date}')
              AND as_of <= toDate('{max_as_of}')
            ORDER BY as_of
        """)
        prices = _pool.query_df(f"""
            SELECT trade_date, argMax(close, imported_at) AS close
            FROM market_data.daily_prices
            WHERE symbol = '{symbol}' AND category = 'etfs'
            GROUP BY trade_date
            ORDER BY trade_date
        """)
    except Exception as exc:
        logger.error("evaluate_methods failed: %s", exc)
        return pd.DataFrame()

    if chk.empty or prices.empty:
        return pd.DataFrame()

    import numpy as np

    chk["as_of"]         = pd.to_datetime(chk["as_of"])
    prices["trade_date"] = pd.to_datetime(prices["trade_date"])
    price_map            = prices.set_index("trade_date")["close"].to_dict()
    price_idx            = pd.DatetimeIndex(prices["trade_date"].sort_values())

    def _find_exit_price(exit_target: pd.Timestamp) -> float:
        idx = price_idx.searchsorted(exit_target, side="left")
        if idx >= len(price_idx):
            return float("nan")
        return float(price_map.get(price_idx[idx], float("nan")))

    rows_out = []
    for _, row in chk.iterrows():
        entry_price = price_map.get(row["as_of"], float("nan"))
        exit_target = row["as_of"] + pd.Timedelta(days=int(row["horizon_days"]))
        exit_price  = _find_exit_price(exit_target)
        if pd.isna(entry_price) or pd.isna(exit_price) or entry_price == 0:
            continue
        log_ret     = float(np.log(exit_price / entry_price))
        weighted_ret = float(row["recommended_weight"]) * log_ret
        rows_out.append({
            "method":             row["method"],
            "recommended_weight": float(row["recommended_weight"]),
            "log_ret":            log_ret,
            "weighted_ret":       weighted_ret,
        })

    if not rows_out:
        return pd.DataFrame()

    ev = pd.DataFrame(rows_out)
    ann_factor = _TRADING_DAYS / horizon_days
    result = (
        ev.groupby("method")
        .apply(lambda g: pd.Series({
            "n":               len(g),
            "total_return_pct": round(g["weighted_ret"].sum() * 100, 2),
            "ann_return_pct":   round(g["weighted_ret"].mean() * ann_factor * 100, 2),
            "ann_vol_pct":      round(g["weighted_ret"].std() * (ann_factor ** 0.5) * 100, 2),
            "avg_weight_pct":   round(g["recommended_weight"].mean() * 100, 1),
            "hit_ratio_pct":    round((g["weighted_ret"] > 0).mean() * 100, 1),
        }), include_groups=False)
        .reset_index()
        .sort_values("method")
    )
    result["sharpe"] = (
        result["ann_return_pct"] / result["ann_vol_pct"].replace(0, float("nan"))
    ).round(2)
    return result[["method", "n", "total_return_pct", "ann_return_pct",
                   "ann_vol_pct", "sharpe", "hit_ratio_pct", "avg_weight_pct"]]


def latest_decisions(symbol: str = "GOLDBEES") -> dict[str, dict]:
    """
    Return the most recent checkpoint row per method for a given symbol.
    Used by the CLI `risk` command display.

    Returns dict keyed by method name, value is the row dict.
    """
    from src.db.pool import get_pool as _get_ch_pool

    _pool = _get_ch_pool()
    df = _pool.query_df(f"""
        SELECT *
        FROM market_data.weight_checkpoints FINAL
        WHERE symbol = '{symbol}'
        ORDER BY as_of DESC
        LIMIT 4
    """)
    if df.empty:
        return {}
    result: dict[str, dict] = {}
    for _, row in df.iterrows():
        method = row["method"]
        if method not in result:
            result[method] = row.to_dict()
    return result


_TRADING_DAYS = 252
