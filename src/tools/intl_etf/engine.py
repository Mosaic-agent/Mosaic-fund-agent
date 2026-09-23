"""
src/tools/intl_etf/engine.py
────────────────────────────
Facade orchestrator for International ETF Quantitative Arbitrage:
Live Scarcity Scanner, Historical 2-Year Backtesting, Signal Tracking, and Visualizations.
"""

from __future__ import annotations

import logging
import os
import shutil
import statistics
from datetime import date, timedelta
from typing import Any, Optional

import numpy as np
import pandas as pd

from src.db.pool import get_pool
from src.tools.intl_etf.models import (
    INTL_ETF_SYMBOLS,
    LiveSnapshot,
    ArbitrageSignal,
)
from src.tools.intl_etf.classifier import ArbitrageClassificationStrategy

log = logging.getLogger(__name__)


def _remove_outliers(values: list[float], iqr_multiplier: float = 3.0) -> tuple[list[float], int]:
    """Return (cleaned_values, n_removed) using the IQR fence method."""
    if len(values) < 4:
        return values, 0
    sorted_v = sorted(values)
    n = len(sorted_v)
    q1 = sorted_v[n // 4]
    q3 = sorted_v[(3 * n) // 4]
    iqr = q3 - q1
    lo = q1 - iqr_multiplier * iqr
    hi = q3 + iqr_multiplier * iqr
    cleaned = [v for v in values if lo <= v <= hi]
    return cleaned, len(values) - len(cleaned)


class IntlETFEngine:
    """Unified Facade for International ETF Scarcity Arbitrage."""

    def __init__(self, symbols: list[str] | None = None) -> None:
        self.symbols = symbols or INTL_ETF_SYMBOLS
        self.classifier = ArbitrageClassificationStrategy()

    def get_client(self) -> Any:
        return get_pool().get_client()

    # ── 1. LIVE ARBITRAGE SCANNER ─────────────────────────────────────────────
    def scan_live(
        self,
        lookback_days: int = 30,
        z_threshold: float = -1.5,
        good_entry_threshold: float = -1.0,
        min_snapshots: int = 5,
    ) -> list[dict[str, Any]]:
        """
        Compute live iNAV premium Z-scores and generate bidirectional arbitrage signals.
        """
        client = self.get_client()
        cutoff = (date.today() - timedelta(days=lookback_days)).isoformat()
        results: list[dict[str, Any]] = []

        for sym in self.symbols:
            item = LiveSnapshot(symbol=sym).model_dump()

            try:
                # Latest live iNAV: DB or live AMC/NSE API
                from src.importer.fetchers.nse_inav_fetcher import get_latest_inav
                live = get_latest_inav(sym, max_age_days=7, store_to_db=True)
                if live is None:
                    item["error"] = "No snapshot found in DB or NSE API"
                    results.append(item)
                    continue

                latest_prem = float(live["premium_discount_pct"])
                item["latest_premium"] = round(latest_prem, 4)
                item["inav_source"] = live.get("source")

                # Historical premium: deduplicated hourly buckets
                hist_rows = client.query(
                    """
                    SELECT
                        toStartOfHour(snapshot_at)                AS hour_bucket,
                        argMax(premium_discount_pct, snapshot_at) AS premium
                    FROM market_data.inav_snapshots
                    WHERE symbol = {sym:String}
                      AND snapshot_at >= toDateTime({cutoff:String})
                    GROUP BY hour_bucket
                    ORDER BY hour_bucket ASC
                    """,
                    parameters={"sym": sym, "cutoff": f"{cutoff} 00:00:00"},
                ).result_rows

                n = len(hist_rows)
                item["n_snapshots"] = n

                if n < min_snapshots:
                    # Fallback to daily_prices JOIN mf_nav
                    nav_rows = client.query(
                        """
                        SELECT
                            p.trade_date,
                            ((p.close - n.nav) / n.nav) * 100 AS premium
                        FROM market_data.daily_prices p FINAL
                        JOIN market_data.mf_nav n FINAL
                          ON p.symbol = n.symbol AND p.trade_date = n.nav_date
                        WHERE p.symbol = {sym:String}
                          AND p.trade_date >= toDate({cutoff:String})
                        ORDER BY p.trade_date ASC
                        """,
                        parameters={"sym": sym, "cutoff": cutoff},
                    ).result_rows

                    if len(nav_rows) >= min_snapshots:
                        premiums = [float(r[1]) for r in nav_rows]
                        item["history_source"] = "daily_prices_mf_nav"
                    else:
                        item["error"] = f"Only {n} snapshots (need >= {min_snapshots})"
                        results.append(item)
                        continue
                else:
                    premiums = [float(r[1]) for r in hist_rows]
                    item["history_source"] = "inav_snapshots"

                premiums, n_removed = _remove_outliers(premiums)
                if len(premiums) < min_snapshots:
                    item["error"] = f"Insufficient clean data after outlier removal"
                    results.append(item)
                    continue

                mean_prem = statistics.mean(premiums)
                std_prem = statistics.stdev(premiums) if len(premiums) >= 2 else 0.0
                item["mean_premium"] = round(mean_prem, 4)
                item["std_premium"] = round(std_prem, 4)

                # Live price & NAV
                live_price = float(live.get("market_price") or 0.0)
                live_inav = float(live.get("inav") or 0.0)
                item["market_price"] = live_price
                item["inav"] = live_inav
                item["downside_risk_to_inav_pct"] = self.classifier.calculate_parity_gap(live_price, live_inav)

                # Volume profile (20d ADV & turnover)
                vol_rows = client.query(
                    """
                    SELECT 
                        argMax(volume, trade_date) as latest_vol,
                        argMax(close, trade_date) as latest_close,
                        round(argMax(volume * close, trade_date) / 10000000, 2) as turnover_cr,
                        round(avg(volume), 0) as avg_vol_20d,
                        round(argMax(volume, trade_date) / nullif(avg(volume), 0), 2) as vol_multiple
                    FROM (
                        SELECT trade_date, close, volume
                        FROM market_data.daily_prices FINAL
                        WHERE symbol = {sym:String} AND trade_date >= today() - 35
                        ORDER BY trade_date DESC
                    )
                    """,
                    parameters={"sym": sym},
                ).result_rows

                if vol_rows and vol_rows[0][0] is not None:
                    v_latest, _, v_turnover, v_avg20, v_mult = vol_rows[0]
                    item["latest_volume"] = float(v_latest or 0.0)
                    item["turnover_cr"] = float(v_turnover or 0.0)
                    item["avg_vol_20d"] = float(v_avg20 or 0.0)
                    item["vol_multiple"] = float(v_mult or 1.0)

                # Volume regime
                item["volume_regime"] = self.classifier.classify_volume_regime(
                    item["vol_multiple"], item["turnover_cr"], latest_prem
                )

                # Z-Score & Arbitrage Classification
                if std_prem > 1e-8:
                    z = (latest_prem - mean_prem) / std_prem
                    item["z_score"] = round(z, 3)
                    action, style, sig_enum = self.classifier.classify_signal(
                        latest_prem, z, z_threshold, good_entry_threshold
                    )
                    item["action"] = action
                    item["action_style"] = style
                    item["arbitrage_signal"] = sig_enum
                else:
                    item["action"] = "⚪ FLAT PREMIUM"
                    item["action_style"] = "dim"
                    item["arbitrage_signal"] = ArbitrageSignal.HOLD.value

                # OU Reversion modeling
                try:
                    from src.db.repository import MarketDataRepository
                    from src.db.pool import get_pool as _get_ou_pool
                    from src.ml.ou_estimator import expected_reversion, prob_revert

                    ou = MarketDataRepository(_get_ou_pool()).ou_state(sym)
                    if ou and ou.is_stationary and ou.half_life_days and ou.half_life_days > 0:
                        exp_rev = expected_reversion(latest_prem, ou.mu, ou.theta, horizon_days=10)
                        item["expected_reversion_pct"] = round(exp_rev, 4)
                        item["half_life_days"] = round(ou.half_life_days, 1)
                        item["prob_revert_10d"] = round(prob_revert(latest_prem, ou.mu, ou.theta, ou.sigma, 10), 3)

                        # Cost & Tax Filter
                        tax_info = self.classifier.apply_cost_tax_filter(exp_rev)
                        item.update(tax_info)
                except Exception:
                    pass

            except Exception as exc:
                log.exception("Error processing ETF %s", sym)
                item["error"] = str(exc)

            results.append(item)

        # Sort: best entry opportunities (lowest Z) first
        valid = [r for r in results if r.get("z_score") is not None]
        invalid = [r for r in results if r.get("z_score") is None]
        valid.sort(key=lambda x: x["z_score"])
        return valid + invalid

    # ── 2. HISTORICAL DATASET & ENRICHMENT ────────────────────────────────────
    def fetch_historical_dataset(self, years: float = 2.0) -> pd.DataFrame:
        """
        Fetch and enrich historical secondary prices, NAVs, rolling Z-scores,
        volume multiples, forward returns, and forward drawdowns.
        """
        client = self.get_client()
        start_date = (date.today() - timedelta(days=int(years * 365.25))).isoformat()

        sql = """
        SELECT
            p.symbol,
            p.trade_date,
            p.close AS price,
            p.volume,
            n.nav,
            ((p.close - n.nav) / n.nav) * 100 AS premium_pct
        FROM market_data.daily_prices p FINAL
        JOIN market_data.mf_nav n FINAL
          ON p.symbol = n.symbol AND p.trade_date = n.nav_date
        WHERE p.symbol IN {symbols:Array(String)}
          AND p.trade_date >= toDate({start_date:String})
          AND p.close > 0 AND n.nav > 0
        ORDER BY p.symbol, p.trade_date ASC
        """

        df = client.query_df(sql, parameters={"symbols": self.symbols, "start_date": start_date})
        if df.empty:
            return pd.DataFrame()

        df["trade_date"] = pd.to_datetime(df["trade_date"])
        df["price"] = df["price"].astype(float)
        df["volume"] = df["volume"].astype(float)
        df["nav"] = df["nav"].astype(float)
        df["premium_pct"] = df["premium_pct"].astype(float)

        dfs = []
        for sym, grp in df.groupby("symbol"):
            g = grp.sort_values("trade_date").copy().reset_index(drop=True)

            # Rolling 30d baseline
            g["mean_premium_30d"] = g["premium_pct"].rolling(30, min_periods=10).mean()
            g["std_premium_30d"] = g["premium_pct"].rolling(30, min_periods=10).std()
            g["z_score"] = (g["premium_pct"] - g["mean_premium_30d"]) / g["std_premium_30d"].replace(0, np.nan)

            # Rolling 20d volume ADV & multiple
            g["adv_20d"] = g["volume"].rolling(20, min_periods=5).mean()
            g["vol_mult"] = g["volume"] / g["adv_20d"].replace(0, np.nan)

            # Forward returns
            g["ret_fwd_5d"] = (g["price"].shift(-5) - g["price"]) / g["price"] * 100
            g["ret_fwd_10d"] = (g["price"].shift(-10) - g["price"]) / g["price"] * 100
            g["ret_fwd_20d"] = (g["price"].shift(-20) - g["price"]) / g["price"] * 100

            # Forward 20d max drawdown
            prices = g["price"].values
            n = len(prices)
            dd_list = []
            for i in range(n):
                if i + 1 < n:
                    window_end = min(i + 21, n)
                    min_future = np.min(prices[i + 1:window_end])
                    dd_list.append((min_future - prices[i]) / prices[i] * 100)
                else:
                    dd_list.append(np.nan)
            g["fwd_max_dd_20d"] = dd_list

            # Signals
            g["is_entry"] = (g["z_score"] <= -1.0) | (g["premium_pct"] < -0.5)
            g["is_entry_vol"] = g["is_entry"] & (g["vol_mult"] >= 0.8)

            g["is_exit"] = (g["z_score"] >= 1.8) | (g["premium_pct"] >= 25.0)
            g["is_exit_vol"] = g["is_exit"] & (g["vol_mult"] >= 1.5)

            g["sig"] = "HOLD"
            g.loc[g["is_entry"], "sig"] = "ENTRY"
            g.loc[g["is_exit"], "sig"] = "EXIT"

            g["prev_sig"] = g["sig"].shift(1)
            g["is_entry_trigger"] = (g["sig"] == "ENTRY") & (g["prev_sig"] != "ENTRY")
            g["is_exit_trigger"] = (g["sig"] == "EXIT") & (g["prev_sig"] != "EXIT")
            g["is_trigger"] = (g["sig"] != "HOLD") & (g["sig"] != g["prev_sig"])

            dfs.append(g)

        return pd.concat(dfs, ignore_index=True)

    # ── 3. BACKTEST PERFORMANCE AGGREGATION ──────────────────────────────────
    def backtest(self, years: float = 2.0) -> dict[str, Any]:
        """Run statistical evaluation of forward win rates and drawdown avoidance."""
        df = self.fetch_historical_dataset(years=years)
        if df.empty:
            return {}

        entry_sub = df[df["is_entry"] & df["ret_fwd_20d"].notna()]
        entry_vol_sub = df[df["is_entry_vol"] & df["ret_fwd_20d"].notna()]

        exit_sub = df[df["is_exit"] & df["fwd_max_dd_20d"].notna()]
        exit_vol_sub = df[df["is_exit_vol"] & df["fwd_max_dd_20d"].notna()]

        # Aggregate Entry stats
        entry_stats = {
            "raw": {
                "n": len(entry_sub),
                "w5": (entry_sub["ret_fwd_5d"] > 0).mean() * 100 if len(entry_sub) else 0,
                "r5": entry_sub["ret_fwd_5d"].mean() if len(entry_sub) else 0,
                "w10": (entry_sub["ret_fwd_10d"] > 0).mean() * 100 if len(entry_sub) else 0,
                "r10": entry_sub["ret_fwd_10d"].mean() if len(entry_sub) else 0,
                "w20": (entry_sub["ret_fwd_20d"] > 0).mean() * 100 if len(entry_sub) else 0,
                "r20": entry_sub["ret_fwd_20d"].mean() if len(entry_sub) else 0,
            },
            "vol_confirmed": {
                "n": len(entry_vol_sub),
                "w5": (entry_vol_sub["ret_fwd_5d"] > 0).mean() * 100 if len(entry_vol_sub) else 0,
                "r5": entry_vol_sub["ret_fwd_5d"].mean() if len(entry_vol_sub) else 0,
                "w10": (entry_vol_sub["ret_fwd_10d"] > 0).mean() * 100 if len(entry_vol_sub) else 0,
                "r10": entry_vol_sub["ret_fwd_10d"].mean() if len(entry_vol_sub) else 0,
                "w20": (entry_vol_sub["ret_fwd_20d"] > 0).mean() * 100 if len(entry_vol_sub) else 0,
                "r20": entry_vol_sub["ret_fwd_20d"].mean() if len(entry_vol_sub) else 0,
            },
        }

        # Aggregate Exit stats
        exit_stats = {
            "raw": {
                "n": len(exit_sub),
                "avg_dd": exit_sub["fwd_max_dd_20d"].mean() if len(exit_sub) else 0,
                "dd5_freq": (exit_sub["fwd_max_dd_20d"] <= -5.0).mean() * 100 if len(exit_sub) else 0,
                "dd10_freq": (exit_sub["fwd_max_dd_20d"] <= -10.0).mean() * 100 if len(exit_sub) else 0,
                "max_crash": exit_sub["fwd_max_dd_20d"].min() if len(exit_sub) else 0,
            },
            "vol_confirmed": {
                "n": len(exit_vol_sub),
                "avg_dd": exit_vol_sub["fwd_max_dd_20d"].mean() if len(exit_vol_sub) else 0,
                "dd5_freq": (exit_vol_sub["fwd_max_dd_20d"] <= -5.0).mean() * 100 if len(exit_vol_sub) else 0,
                "dd10_freq": (exit_vol_sub["fwd_max_dd_20d"] <= -10.0).mean() * 100 if len(exit_vol_sub) else 0,
                "max_crash": exit_vol_sub["fwd_max_dd_20d"].min() if len(exit_vol_sub) else 0,
            },
        }

        # Per-ETF Breakdown
        per_etf = {}
        for sym, grp in df.groupby("symbol"):
            e_sub = grp[grp["is_entry"] & grp["ret_fwd_20d"].notna()]
            x_sub = grp[grp["is_exit"] & grp["fwd_max_dd_20d"].notna()]
            per_etf[sym] = {
                "rows": len(grp),
                "min_prem": grp["premium_pct"].min(),
                "avg_prem": grp["premium_pct"].mean(),
                "max_prem": grp["premium_pct"].max(),
                "entry_n": len(e_sub),
                "entry_w20": (e_sub["ret_fwd_20d"] > 0).mean() * 100 if len(e_sub) else 0,
                "entry_r20": e_sub["ret_fwd_20d"].mean() if len(e_sub) else 0,
                "entry_max_g": e_sub["ret_fwd_20d"].max() if len(e_sub) else 0,
                "entry_max_l": e_sub["ret_fwd_20d"].min() if len(e_sub) else 0,
                "exit_n": len(x_sub),
                "exit_avg_dd": x_sub["fwd_max_dd_20d"].mean() if len(x_sub) else 0,
                "exit_dd5_freq": (x_sub["fwd_max_dd_20d"] <= -5.0).mean() * 100 if len(x_sub) else 0,
                "exit_worst_drop": x_sub["fwd_max_dd_20d"].min() if len(x_sub) else 0,
            }

        return {
            "dataset": df,
            "entry_stats": entry_stats,
            "exit_stats": exit_stats,
            "per_etf": per_etf,
        }

    # ── 4. CHRONOLOGICAL SIGNAL HISTORY ──────────────────────────────────────
    def get_signal_history(
        self,
        years: float = 2.0,
        symbol: str | None = None,
        limit: int = 50,
    ) -> pd.DataFrame:
        """Extract chronological trigger event log with forward outcomes."""
        df = self.fetch_historical_dataset(years=years)
        if df.empty:
            return pd.DataFrame()

        triggers = df[df["is_trigger"]].copy()
        if symbol:
            triggers = triggers[triggers["symbol"] == symbol.upper()]

        return triggers.sort_values("trade_date", ascending=False).head(limit)
