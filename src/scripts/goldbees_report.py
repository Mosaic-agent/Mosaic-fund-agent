"""
GOLDBEES pipeline report — single-script, minimal queries.
Run: python src/scripts/goldbees_report.py
Output: formatted recommendation block, ~20 lines.
"""
import sys
import os
import warnings
warnings.filterwarnings("ignore", category=UserWarning)
_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(_ROOT, "src"))
sys.path.insert(0, _ROOT)

# Guard: Ensure script is running inside the Docker container to prevent local package import errors
if not os.environ.get('RUNNING_IN_DOCKER') and not os.path.exists('/.dockerenv') and os.environ.get('ALLOW_LOCAL_RUN') != '1':
    print("=================================================================", file=sys.stderr)
    print(" ERROR: This script must be run inside the Docker container", file=sys.stderr)
    print(" to ensure that all required dependencies are loaded correctly.", file=sys.stderr)
    print("-----------------------------------------------------------------", file=sys.stderr)
    print(" Please use the wrapper script instead:", file=sys.stderr)
    print(f"   ./mosaic.sh src/scripts/goldbees_report.py", file=sys.stderr)
    print(" (On Windows, use 'mosaic.bat' instead)", file=sys.stderr)
    print("-----------------------------------------------------------------", file=sys.stderr)
    print(" If you are a developer and want to bypass this check, set:", file=sys.stderr)
    print("   export ALLOW_LOCAL_RUN=1  (or set it in your environment)", file=sys.stderr)
    print("=================================================================", file=sys.stderr)
    sys.exit(1)

from db.pool import query_df, get_pool
from db.repository import MarketDataRepository


def get_llm_recommendation(
    regime: str, garch_vol: float, price: float, ema50: float, ema_dir: str,
    inav_prem: float | None, anomaly: str, prob_up: float, expected_return: float,
    auc: float, skill: float, regime_signal: str, rg_w: float, kel_w: float,
    bl50_w: float, bl30_w: float, composite_score: float, action: str
) -> str:
    """Generate an intelligent recommendation using the configured LLM."""
    try:
        from config.settings import settings
        from langchain_core.messages import SystemMessage, HumanMessage

        # Build LLM
        llm = None
        if not settings.llm_local_disabled:
            provider = settings.llm_provider.lower()
            if settings.llm_base_url:
                from langchain_openai import ChatOpenAI
                llm = ChatOpenAI(
                    model=settings.llm_model,
                    base_url=settings.llm_base_url,
                    api_key=settings.openai_api_key or "local",
                    temperature=0.2,
                    max_tokens=256,
                )
            elif provider == "anthropic":
                from langchain_anthropic import ChatAnthropic
                llm = ChatAnthropic(
                    model=settings.llm_model,
                    api_key=settings.anthropic_api_key,
                    temperature=0.2,
                    max_tokens=256,
                    extra_headers={"anthropic-beta": "prompt-caching-2024-07-31"},
                )
            else:
                from langchain_openai import ChatOpenAI
                llm = ChatOpenAI(
                    model=settings.llm_model,
                    api_key=settings.openai_api_key,
                    temperature=0.2,
                    max_tokens=256,
                )

        if llm is None and settings.llm_cloud_provider:
            provider = settings.llm_cloud_provider.strip().lower()
            if provider == "anthropic":
                from langchain_anthropic import ChatAnthropic
                llm = ChatAnthropic(
                    model=settings.llm_cloud_model,
                    api_key=settings.anthropic_api_key,
                    temperature=0.2,
                    max_tokens=256,
                    extra_headers={"anthropic-beta": "prompt-caching-2024-07-31"},
                )
            else:
                from langchain_openai import ChatOpenAI
                llm = ChatOpenAI(
                    model=settings.llm_cloud_model,
                    api_key=settings.openai_api_key,
                    temperature=0.2,
                    max_tokens=256,
                )

        if llm is None:
            return f"LLM not configured. Recommended weight: {bl50_w*100:.1f}% based on {action} composite action."

        inav_desc = f"{inav_prem:+.2f}%" if inav_prem is not None else "N/A"
        prompt = (
            f"Write a concise, 1-2 sentence actionable investment recommendation for GOLDBEES ETF based on these quant inputs:\n"
            f"- Market Regime: {regime}\n"
            f"- GARCH Volatility: {garch_vol:.1f}%\n"
            f"- Price vs EMA50: ₹{price:.2f} vs ₹{ema50:.2f} ({ema_dir} EMA50)\n"
            f"- iNAV Premium: {inav_desc}\n"
            f"- Anomaly Flag: {anomaly}\n"
            f"- ML Predictor: Prob Up={prob_up:.4f}, Expected 5-Day Return={expected_return:+.2f}%, AUC={auc:.3f} (Skill={skill:+.3f}), Signal={regime_signal}\n"
            f"- Sizing Weights: Risk Governor={rg_w*100:.1f}%, Kelly Only={kel_w*100:.1f}%, Recommended Blended (50/50)={bl50_w*100:.1f}%, Conservative (70/30)={bl30_w*100:.1f}%\n"
            f"- Composite Score: {composite_score:.0f}/100, Action={action}\n\n"
            f"Synthesise this data into a clear investment action (e.g. BUY, ACCUMULATE, HOLD, TRIM, or AVOID) with rationale, mentioning the recommended weight size ({bl50_w*100:.1f}%). "
            f"Never invent numbers. Do not output any markdown formatting (no bold/italics), just a plain text sentence."
        )

        res = llm.invoke([
            SystemMessage(content="You are a professional quantitative strategist for Indian ETF markets."),
            HumanMessage(content=prompt),
        ])
        return str(res.content).strip()
    except Exception as exc:
        return f"Error generating recommendation: {exc}"


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

    rec = get_llm_recommendation(
        regime=regime,
        garch_vol=garch_vol,
        price=price,
        ema50=ema50,
        ema_dir=ema_dir,
        inav_prem=inav_prem,
        anomaly=sig['anomaly_flag'],
        prob_up=float(ml['prob_up']),
        expected_return=float(ml['expected_return_pct']),
        auc=float(ml['cv_auc_mean']),
        skill=skill,
        regime_signal=ml['regime_signal'],
        rg_w=rg_w,
        kel_w=kel_w,
        bl50_w=bl50_w,
        bl30_w=bl30_w,
        composite_score=float(sig['composite_score']),
        action=sig['action']
    )

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

  Recommendation: {rec}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━""")


if __name__ == "__main__":
    main()
