"""
GOLDBEES pipeline report — single-script, minimal queries.
Run: python src/scripts/goldbees_report.py
Output: formatted recommendation block, ~20 lines.
"""
import sys
import os
_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(_ROOT, "src"))
sys.path.insert(0, _ROOT)

from db.pool import query_df, get_pool
from db.repository import MarketDataRepository


def main():
    pool = get_pool()
    repo = MarketDataRepository(pool)

    # 1. Latest ML prediction
    ml = query_df("""
        SELECT expected_return_pct, prob_up, regime_signal, cv_auc_mean,
               confidence_low, confidence_high, as_of
        FROM market_data.ml_predictions FINAL
        ORDER BY as_of DESC LIMIT 1
    """).iloc[0]

    # 2. Weight checkpoints (rg + kelly + blended)
    wdf = query_df("""
        SELECT method, recommended_weight, garch_vol_pct, regime
        FROM market_data.weight_checkpoints FINAL
        WHERE symbol = 'GOLDBEES'
        ORDER BY as_of DESC LIMIT 4
    """).set_index("method")

    # 3. iNAV premium (dict: symbol -> premium_pct)
    inav, _ = repo.inav_latest_and_history(["GOLDBEES"])
    inav_prem = inav.get("GOLDBEES", None)

    # 4. Price vs EMA50
    df_ohlcv = repo.ohlcv("GOLDBEES", "etfs").sort_values("trade_date")
    df_ohlcv["ema50"] = df_ohlcv["close"].ewm(span=50, adjust=False).mean()
    last = df_ohlcv.iloc[-1]
    price   = last["close"]
    ema50   = last["ema50"]
    ema_dir = "ABOVE" if price >= ema50 else "BELOW"

    # 5. Signal composite for anomaly flag
    sig = query_df("""
        SELECT anomaly_flag, composite_score, action
        FROM market_data.signal_composite FINAL
        WHERE etf_symbol = 'GOLDBEES'
        ORDER BY as_of DESC LIMIT 1
    """).iloc[0]

    # Pull weight values safely
    rg_w   = wdf.loc["rg",         "recommended_weight"] if "rg"         in wdf.index else float("nan")
    kel_w  = wdf.loc["kelly",      "recommended_weight"] if "kelly"      in wdf.index else float("nan")
    bl50_w = wdf.loc["blended_50", "recommended_weight"] if "blended_50" in wdf.index else float("nan")
    bl30_w = wdf.loc["blended_30", "recommended_weight"] if "blended_30" in wdf.index else float("nan")
    garch_vol = wdf["garch_vol_pct"].iloc[0] if not wdf.empty else float("nan")
    regime    = wdf["regime"].iloc[0]         if not wdf.empty else "N/A"

    skill = round(float(ml["cv_auc_mean"]) - 0.5, 3)
    inav_str = f"{inav_prem:+.2f}%" if inav_prem is not None else "N/A"
    inav_alert = "  ⚠️  PREMIUM > +5% — do not enter" if (inav_prem or 0) > 5 else "  ✅ No premium alert"

    print(f"""
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  GOLDBEES  |  {ml['as_of']}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Regime        : {regime:<18}  GARCH vol: {garch_vol:.1f}%
  Price vs EMA50: ₹{price:.2f} vs ₹{ema50:.2f}  →  {ema_dir}
  iNAV Premium  : {inav_str}{inav_alert}
  Anomaly       : {sig['anomaly_flag']}

  ML Signal
  ─────────
  Probability up   : {float(ml['prob_up']):.4f}
  Expected return  : {float(ml['expected_return_pct']):+.2f}%  (5-day)
  Confidence band  : [{float(ml['confidence_low']):.2f}%, {float(ml['confidence_high']):.2f}%]
  Model AUC        : {float(ml['cv_auc_mean']):.3f}  (skill: {skill:+.3f})
  Regime signal    : {ml['regime_signal']}

  Position Weights
  ─────────────────
  Rule-based (RG)  : {rg_w*100:.1f}%
  Kelly only       : {kel_w*100:.1f}%
  Blended 50/50    : {bl50_w*100:.1f}%   ← recommended
  Blended 70/30    : {bl30_w*100:.1f}%

  Composite score  : {float(sig['composite_score']):.0f}/100  →  {sig['action']}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━""")


if __name__ == "__main__":
    main()
