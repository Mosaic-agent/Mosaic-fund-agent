import logging
from dataclasses import dataclass
from typing import Any
from datetime import date as dt_date
from src.commands.base import Command

logger = logging.getLogger(__name__)

@dataclass
class RiskCommand(Command):
    symbol: str
    save: bool
    evaluate: bool
    since_days: int
    blend: float

    def execute(self) -> dict[str, Any]:
        from src.db.pool import get_pool
        _ch_pool = get_pool()

        # ── Evaluate mode ─────────────────────────────────────────────────────────
        if self.evaluate:
            from src.tools.weight_checkpoint import evaluate_methods
            df = evaluate_methods(symbol=self.symbol, since_days=self.since_days)
            return {
                "evaluate": True,
                "df": df
            }

        # ── Fetch live inputs ─────────────────────────────────────────────────────
        from src.tools.risk_governor import compute_position_weight, vol_target_for

        today = dt_date.today()

        # 1. Latest ML prediction from ClickHouse
        pred_df = _ch_pool.query_df("""
            SELECT expected_return_pct, confidence_low, confidence_high,
                   cv_r2_mean, regime_signal, horizon_days
            FROM market_data.ml_predictions FINAL
            ORDER BY as_of DESC LIMIT 1
        """)

        if pred_df.empty:
            raise RuntimeError("No ML predictions found. Run signals or trend predictor first.")

        pred = pred_df.iloc[0]
        exp_ret   = float(pred["expected_return_pct"])
        conf_low  = float(pred["confidence_low"])
        conf_high = float(pred["confidence_high"])
        cv_r2     = float(pred["cv_r2_mean"])
        ml_regime = str(pred["regime_signal"])
        horizon   = int(pred["horizon_days"])

        # 2. Latest GARCH vol + regime from anomaly pipeline
        price_df = _ch_pool.query_df(f"""
            SELECT trade_date,
                   toFloat64(argMax(close, imported_at)) AS close
            FROM market_data.daily_prices
            WHERE symbol = '{self.symbol}' AND category = 'etfs'
            GROUP BY trade_date ORDER BY trade_date ASC
        """)

        garch_vol_pct = vol_target_for(self.symbol)
        regime = "✅ Normal"
        price_below_ema50 = False

        if not price_df.empty:
            try:
                import pandas as _pd
                from src.ml.anomaly import run_composite_anomaly
                price_df["trade_date"] = _pd.to_datetime(price_df["trade_date"])
                price_df_full = _pd.DataFrame({
                    "trade_date": price_df["trade_date"],
                    "open": price_df["close"], "high": price_df["close"],
                    "low": price_df["close"],  "close": price_df["close"],
                    "volume": 0,
                })
                df_res, _, _ = run_composite_anomaly(price_df_full)
                last = df_res.dropna(subset=["garch_vol"]).iloc[-1]
                garch_vol_pct = float(last["garch_vol"])
                regime = str(last["regime"])
                close_series = price_df["close"]
                ema50 = close_series.ewm(span=50, adjust=False).mean()
                price_below_ema50 = bool(close_series.iloc[-1] < ema50.iloc[-1])
            except Exception as exc:
                logger.warning("GARCH computation failed, using vol target: %s", exc)

        # ── Compute all method weights ─────────────────────────────────────────────
        from src.tools.adaptive_kelly import compute_kelly_weight, compute_blended_weight

        vol_target = vol_target_for(self.symbol)
        rg_dec = compute_position_weight(
            garch_annual_vol_pct=garch_vol_pct,
            regime=regime,
            vol_target_pct=vol_target,
            price_below_ema50=price_below_ema50,
        )
        kelly_dec = compute_kelly_weight(
            expected_return_pct=exp_ret,
            confidence_low_pct=conf_low,
            confidence_high_pct=conf_high,
            horizon_days=horizon,
            cv_r2=cv_r2,
            garch_annual_vol_pct=garch_vol_pct,
        )
        blended_w   = compute_blended_weight(rg_dec.final_weight, kelly_dec.final_weight, self.blend)
        blended_30  = compute_blended_weight(rg_dec.final_weight, kelly_dec.final_weight, 0.3)

        rows_data = [
            ("rg",          rg_dec.final_weight,  rg_dec.tier,
             f"inverse-vol × regime × trend"),
            ("kelly",       kelly_dec.final_weight, "—",
             f"μ/σ²  raw={kelly_dec.raw_kelly:.1f}×  haircut={kelly_dec.confidence_haircut:.0%}"),
            (f"blended_{int(self.blend*100)}", blended_w, "—",
             f"{int((1-self.blend)*100)}% RG + {int(self.blend*100)}% Kelly"),
            ("blended_30",  blended_30, "—",
             "70% RG + 30% Kelly (conservative)"),
        ]

        # ── Save checkpoints ──────────────────────────────────────────────────────
        saved_count = 0
        if self.save:
            from src.tools.weight_checkpoint import save_checkpoints
            checkpoint_rows = [
                {
                    "as_of": today, "symbol": self.symbol, "method": "rg",
                    "recommended_weight": rg_dec.final_weight,
                    "garch_vol_pct": garch_vol_pct, "regime": regime,
                    "price_below_ema50": int(price_below_ema50),
                    "horizon_days": horizon,
                    "rationale": f"vol={garch_vol_pct:.1f}% regime_mult={rg_dec.regime_mult:.0%} trend={rg_dec.trend_mult:.0%}",
                },
                {
                    "as_of": today, "symbol": self.symbol, "method": "kelly",
                    "recommended_weight": kelly_dec.final_weight,
                    "expected_return_pct": exp_ret, "expected_vol_pct": kelly_dec.implied_vol_pct,
                    "garch_vol_pct": garch_vol_pct, "regime": regime,
                    "price_below_ema50": int(price_below_ema50),
                    "cv_r2": cv_r2, "horizon_days": horizon,
                    "rationale": f"raw_kelly={kelly_dec.raw_kelly:.2f} frac={kelly_dec.fractional_kelly:.2f} haircut={kelly_dec.confidence_haircut:.0%}",
                },
                {
                    "as_of": today, "symbol": self.symbol, "method": f"blended_{int(self.blend*100)}",
                    "recommended_weight": blended_w,
                    "expected_return_pct": exp_ret, "expected_vol_pct": kelly_dec.implied_vol_pct,
                    "garch_vol_pct": garch_vol_pct, "regime": regime,
                    "price_below_ema50": int(price_below_ema50),
                    "cv_r2": cv_r2, "horizon_days": horizon,
                    "rationale": f"rg={rg_dec.final_weight:.0%} kelly={kelly_dec.final_weight:.0%} blend={self.blend:.0%}",
                },
                {
                    "as_of": today, "symbol": self.symbol, "method": "blended_30",
                    "recommended_weight": blended_30,
                    "expected_return_pct": exp_ret, "expected_vol_pct": kelly_dec.implied_vol_pct,
                    "garch_vol_pct": garch_vol_pct, "regime": regime,
                    "price_below_ema50": int(price_below_ema50),
                    "cv_r2": cv_r2, "horizon_days": horizon,
                    "rationale": f"rg={rg_dec.final_weight:.0%} kelly={kelly_dec.final_weight:.0%} blend=30%",
                },
            ]
            saved_count = save_checkpoints(checkpoint_rows)

        return {
            "evaluate": False,
            "today": today,
            "garch_vol_pct": garch_vol_pct,
            "regime": regime,
            "price_below_ema50": price_below_ema50,
            "exp_ret": exp_ret,
            "conf_low": conf_low,
            "conf_high": conf_high,
            "cv_r2": cv_r2,
            "rows_data": rows_data,
            "saved_count": saved_count
        }
