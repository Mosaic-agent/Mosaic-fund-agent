"""
src/mcp_server.py
──────────────────
MCP server exposing the GOLDBEES investment pipeline as structured tools.

Compatible with:
  - Claude Code  (registered in .mcp.json)
  - Gemini CLI   (registered in ~/.gemini/settings.json)
  - Any MCP-compliant client (Cursor, Windsurf, etc.)

Tools exposed
─────────────
  run_pipeline          Full pipeline: predict → Kelly → RG blend → save checkpoint
  get_latest_signal     Latest stored prediction + recommended weight (no recompute)
  evaluate_performance  Realised hit-ratio / MAE from stored checkpoints vs prices
  import_data           Refresh market data from upstream sources

Transport: stdio (default for CLI tools)

Run directly:
  /Users/dhiraj.thakur/project/ofin-agent/.venv_new/bin/python3 src/mcp_server.py

Or via MCP client config:
  command: /Users/dhiraj.thakur/project/ofin-agent/.venv_new/bin/python3
  args:    ["src/mcp_server.py"]
  cwd:     /Users/dhiraj.thakur/project/ofin-agent
"""
from __future__ import annotations

import json
import logging
import os
import sys
from datetime import date
from pathlib import Path
from typing import Any

# ── Bootstrap project root so all src.* imports resolve ───────────────────────
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))
os.chdir(_PROJECT_ROOT)

import mcp.server.stdio
from mcp.server import Server
from mcp.server.models import InitializationOptions
from mcp.types import (
    TextContent,
    Tool,
)

from src.utils.logging_setup import setup_logging
setup_logging(log_level="WARNING")
log = logging.getLogger(__name__)

app = Server("ofin-goldbees-pipeline")

# ── Tool definitions ───────────────────────────────────────────────────────────

@app.list_tools()
async def list_tools() -> list[Tool]:
    return [
        Tool(
            name="run_pipeline",
            description=(
                "Run the full GOLDBEES investment pipeline:\n"
                "1. Fetch latest ML prediction (trains LightGBM classifier)\n"
                "2. Compute Kelly-optimal position weight\n"
                "3. Blend with rule-based Risk Governor (vol-targeting + regime)\n"
                "4. Save decision checkpoint to DB for later evaluation\n\n"
                "Returns: probability up, expected return, Kelly weight, blended weight, regime.\n\n"
                "IMPORTANT: Show the 'display_report' field verbatim to the user. "
                "Do NOT invent composite scores, macro ratings, sentiment scores, or "
                "any metric not present in the JSON response — this pipeline does not "
                "produce them. The recommended weight is 'blended_50', not 'rg'."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "save": {
                        "type": "boolean",
                        "description": "Persist recommendation to weight_checkpoints table (default: true)",
                        "default": True,
                    },
                    "blend": {
                        "type": "number",
                        "description": "Kelly blend fraction 0-1 (0=pure RG, 1=pure Kelly, default 0.5)",
                        "default": 0.5,
                    },
                },
                "required": [],
            },
        ),
        Tool(
            name="get_latest_signal",
            description=(
                "Return the most recent stored prediction and weight recommendation "
                "without retraining. Fast — reads from DB only.\n\n"
                "Returns: last prediction date, prob_up, expected return %, "
                "recommended weights per method, regime signal."
            ),
            inputSchema={
                "type": "object",
                "properties": {},
                "required": [],
            },
        ),
        Tool(
            name="evaluate_performance",
            description=(
                "Evaluate realised accuracy of past ML predictions by joining "
                "to actual GOLDBEES prices at the forward horizon.\n\n"
                "Returns: hit ratio (directional accuracy), MAE, RMSE, "
                "per-row predicted vs actual for recent predictions."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "rows": {
                        "type": "integer",
                        "description": "Number of recent predictions to show (default 10)",
                        "default": 10,
                    },
                },
                "required": [],
            },
        ),
        Tool(
            name="import_data",
            description=(
                "Refresh market data from upstream sources (NSE/Yahoo/CFTC).\n"
                "Run this before run_pipeline if data might be stale.\n\n"
                "Categories: etfs (price+NAV), mf (MF NAV), cot (CFTC gold positioning), "
                "fii_dii (institutional flows). Default: all four."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "categories": {
                        "type": "string",
                        "description": "Comma-separated categories to import (default: 'etfs,mf,cot,fii_dii')",
                        "default": "etfs,mf,cot,fii_dii",
                    },
                },
                "required": [],
            },
        ),
    ]


# ── Tool handlers ──────────────────────────────────────────────────────────────

@app.call_tool()
async def call_tool(name: str, arguments: dict[str, Any]) -> list[TextContent]:
    try:
        if name == "run_pipeline":
            result = await _run_pipeline(
                save=arguments.get("save", True),
                blend=float(arguments.get("blend", 0.5)),
            )
        elif name == "get_latest_signal":
            result = await _get_latest_signal()
        elif name == "evaluate_performance":
            result = await _evaluate_performance(rows=int(arguments.get("rows", 10)))
        elif name == "import_data":
            result = await _import_data(categories=arguments.get("categories", "etfs,mf,cot,fii_dii"))
        else:
            result = {"error": f"Unknown tool: {name}"}
    except Exception as exc:
        log.exception("Tool %s failed", name)
        result = {"error": str(exc), "tool": name}

    return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]


# ── Implementation ─────────────────────────────────────────────────────────────

async def _run_pipeline(save: bool = True, blend: float = 0.5) -> dict:
    """
    Full pipeline:
      1. Run LightGBM trend predictor (trains on full history)
      2. Compute Risk Governor weight (GARCH vol + regime + trend)
      3. Compute Kelly weight (prob_up × GARCH vol)
      4. Blend RG + Kelly
      5. Optionally save checkpoint to DB
    """
    from config.settings import settings
    from src.ml.trend_predictor import run_trend_prediction
    from src.tools.risk_governor import compute_position_weight, vol_target_for
    from src.tools.adaptive_kelly import compute_kelly_weight, compute_blended_weight
    from src.tools.weight_checkpoint import save_checkpoints, latest_decisions
    import numpy as np
    import pandas as pd

    # ── Step 1: ML prediction ────────────────────────────────────────────────
    ml = run_trend_prediction(
        verbose=False,
        ch_host=settings.clickhouse_host,
        ch_port=settings.clickhouse_port,
        ch_database=settings.clickhouse_database,
        ch_user=settings.clickhouse_user,
        ch_password=settings.clickhouse_password,
    )

    # ── Step 2: Fetch GARCH vol + regime from anomaly detector ───────────────
    from src.db.pool import get_pool as _get_ch_pool
    _ch_pool = _get_ch_pool()
    price_df = _ch_pool.query_df("""
        SELECT trade_date,
               toFloat64(argMax(open,   imported_at)) AS open,
               toFloat64(argMax(high,   imported_at)) AS high,
               toFloat64(argMax(low,    imported_at)) AS low,
               toFloat64(argMax(close,  imported_at)) AS close,
               toFloat64(argMax(volume, imported_at)) AS volume
        FROM market_data.daily_prices
        WHERE symbol = 'GOLDBEES' AND category = 'etfs'
        GROUP BY trade_date ORDER BY trade_date ASC
    """)

    price_df["trade_date"] = pd.to_datetime(price_df["trade_date"])

    from src.ml.anomaly import run_composite_anomaly
    df_res, _, _ = run_composite_anomaly(price_df)
    last = df_res.iloc[-1]

    garch_vol_pct    = float(last.get("garch_vol", vol_target_for("GOLDBEES")))
    regime           = str(last.get("regime", "NORMAL"))
    latest_close     = float(last["close"])
    ema50            = float(price_df["close"].ewm(span=50, adjust=False).mean().iloc[-1])
    price_below_ema50 = latest_close < ema50

    vol_target = vol_target_for("GOLDBEES")

    # ── Step 3: Risk Governor weight ────────────────────────────────────────
    rg_dec = compute_position_weight(
        garch_annual_vol_pct=garch_vol_pct,
        regime=regime,
        vol_target_pct=vol_target,
        price_below_ema50=price_below_ema50,
    )

    # ── Step 4: Kelly weight ─────────────────────────────────────────────────
    kelly_dec = compute_kelly_weight(
        expected_return_pct  = ml["expected_return_pct"],
        confidence_low_pct   = ml["confidence_low"],
        confidence_high_pct  = ml["confidence_high"],
        horizon_days         = ml["horizon_days"],
        cv_r2                = ml["cv_r2_mean"],
        garch_annual_vol_pct = garch_vol_pct,
    )

    # ── Step 5: Blend ────────────────────────────────────────────────────────
    blended_w  = compute_blended_weight(rg_dec.final_weight, kelly_dec.final_weight, blend)
    blended_30 = compute_blended_weight(rg_dec.final_weight, kelly_dec.final_weight, 0.3)

    # ── Step 6: Save checkpoint ───────────────────────────────────────────────
    if save:
        rows = [
            {
                "as_of": date.today(), "symbol": "GOLDBEES", "method": "rg",
                "recommended_weight": rg_dec.final_weight, "horizon_days": ml["horizon_days"],
                "regime": regime, "rationale": f"vol={garch_vol_pct:.1f}% ema50={'below' if price_below_ema50 else 'above'}",
                "garch_vol_pct": garch_vol_pct, "expected_return_pct": None,
                "cv_r2": None, "composite_score": None,
            },
            {
                "as_of": date.today(), "symbol": "GOLDBEES", "method": "kelly",
                "recommended_weight": kelly_dec.final_weight, "horizon_days": ml["horizon_days"],
                "regime": regime, "rationale": f"prob_up={ml['prob_up']:.3f} auc={ml['cv_auc_mean']:.3f}",
                "garch_vol_pct": garch_vol_pct, "expected_return_pct": ml["expected_return_pct"],
                "cv_r2": ml["cv_r2_mean"], "composite_score": None,
            },
            {
                "as_of": date.today(), "symbol": "GOLDBEES", "method": "blended_50",
                "recommended_weight": blended_w, "horizon_days": ml["horizon_days"],
                "regime": regime, "rationale": f"rg={rg_dec.final_weight:.2f} kelly={kelly_dec.final_weight:.2f} blend=50%",
                "garch_vol_pct": garch_vol_pct, "expected_return_pct": ml["expected_return_pct"],
                "cv_r2": ml["cv_r2_mean"], "composite_score": None,
            },
        ]
        n_saved = save_checkpoints(rows)

    result = {
        "as_of":                date.today().isoformat(),
        "symbol":               "GOLDBEES",
        "latest_close":         round(latest_close, 2),
        "regime":               regime,
        "garch_vol_pct":        round(garch_vol_pct, 2),
        "price_vs_ema50":       "below" if price_below_ema50 else "above",
        "ml": {
            "prob_up":           ml["prob_up"],
            "expected_return_pct": ml["expected_return_pct"],
            "confidence_band":   [ml["confidence_low"], ml["confidence_high"]],
            "regime_signal":     ml["regime_signal"],
            "cv_auc":            ml["cv_auc_mean"],
            "cv_skill":          ml["cv_r2_mean"],
            "hit_ratio":         ml["cv_hit_ratio_mean"],
            "n_training_rows":   ml["n_training_rows"],
        },
        "weights": {
            "rg":          round(rg_dec.final_weight, 4),
            "kelly":       round(kelly_dec.final_weight, 4),
            "blended_50":  round(blended_w, 4),
            "blended_30":  round(blended_30, 4),
        },
        "kelly_detail": {
            "raw_kelly":    kelly_dec.raw_kelly,
            "haircut":      kelly_dec.confidence_haircut,
            "sigma_source": kelly_dec.sigma_source,
            "alerts":       kelly_dec.alerts,
        },
        "checkpoint_saved": save,
        "recommendation": _summarise(ml, blended_w, regime),
        "_display_instructions": (
            "Show the 'display_report' field verbatim. "
            "Do NOT add scores, ratings, or metrics not present in this JSON. "
            "The pipeline has no composite score, macro score, or sentiment score."
        ),
    }
    result["display_report"] = _format_pipeline_report(result)
    return result


async def _get_latest_signal() -> dict:
    """Read the latest stored recommendation from DB — no retraining."""
    from src.tools.weight_checkpoint import latest_decisions
    from src.db.pool import get_pool as _get_ch_pool

    decisions = latest_decisions("GOLDBEES")

    last_pred = _get_ch_pool().query_df("""
        SELECT as_of, expected_return_pct, prob_up, cv_auc_mean,
               regime_signal, confidence_low, confidence_high
        FROM market_data.ml_predictions FINAL
        ORDER BY as_of DESC LIMIT 1
    """)

    pred_row = last_pred.iloc[0].to_dict() if not last_pred.empty else {}

    return {
        "last_prediction_date": str(pred_row.get("as_of", "N/A")),
        "regime_signal":        pred_row.get("regime_signal", "N/A"),
        "prob_up":              pred_row.get("prob_up", None),
        "expected_return_pct":  pred_row.get("expected_return_pct", None),
        "cv_auc":               pred_row.get("cv_auc_mean", None),
        "confidence_band": [
            pred_row.get("confidence_low", None),
            pred_row.get("confidence_high", None),
        ],
        "weights": {
            method: {
                "recommended_weight": round(float(row["recommended_weight"]), 4),
                "as_of":              str(row["as_of"]),
                "rationale":          str(row.get("rationale", "")),
            }
            for method, row in decisions.items()
        },
        "note": "Run 'run_pipeline' to refresh with today's data.",
    }


async def _evaluate_performance(rows: int = 10) -> dict:
    """Evaluate realised accuracy: predictions vs actual GOLDBEES prices."""
    import numpy as np
    import pandas as pd
    from src.db.pool import get_pool as _get_ch_pool

    _pool = _get_ch_pool()
    try:
        preds = _pool.query_df("""
            SELECT as_of, horizon_days, expected_return_pct, regime_signal, goldbees_close AS start_price
            FROM market_data.ml_predictions FINAL
            ORDER BY as_of ASC
        """)
        prices = _pool.query_df("""
            SELECT trade_date, argMax(close, imported_at) AS close
            FROM market_data.daily_prices
            WHERE symbol = 'GOLDBEES' AND category = 'etfs'
            GROUP BY trade_date ORDER BY trade_date ASC
        """)
    except Exception as exc:
        return {"error": str(exc)}

    preds["as_of"]        = pd.to_datetime(preds["as_of"])
    prices["trade_date"]  = pd.to_datetime(prices["trade_date"])
    price_idx = pd.DatetimeIndex(prices["trade_date"])
    price_map = prices.set_index("trade_date")["close"].to_dict()

    results = []
    for _, row in preds.iterrows():
        start_price = float(row["start_price"])
        if start_price <= 0:
            continue
        horizon    = int(row["horizon_days"])
        entry_pos  = price_idx.searchsorted(row["as_of"], side="left")
        exit_pos   = entry_pos + horizon
        if exit_pos >= len(price_idx):
            continue
        end_price       = float(price_map[price_idx[exit_pos]])
        actual_logret   = float(np.log(end_price / start_price) * 100)
        predicted       = float(row["expected_return_pct"])
        pred_sign       = int(np.sign(predicted))
        actual_sign     = int(np.sign(actual_logret))
        hit = None if pred_sign == 0 else int(pred_sign == actual_sign)
        results.append({
            "as_of":     row["as_of"].date().isoformat(),
            "regime":    row["regime_signal"],
            "predicted": round(predicted, 3),
            "actual":    round(actual_logret, 3),
            "error":     round(actual_logret - predicted, 3),
            "hit":       hit,
        })

    if not results:
        return {"status": "no_data", "message": "No realised predictions yet — horizon hasn't closed."}

    df = pd.DataFrame(results)
    directional = df[df["hit"].notna()]
    hit_ratio = float(directional["hit"].mean()) if not directional.empty else None
    mae  = float(df["error"].abs().mean())
    rmse = float(np.sqrt((df["error"] ** 2).mean()))

    return {
        "summary": {
            "n_total":           len(df),
            "n_directional":     len(directional),
            "hit_ratio":         round(hit_ratio, 3) if hit_ratio is not None else None,
            "mae_pct":           round(mae, 3),
            "rmse_pct":          round(rmse, 3),
            "interpretation":    (
                f"Model called direction correctly {hit_ratio:.0%} of the time"
                if hit_ratio is not None else "No directional calls yet"
            ),
        },
        "recent": df.tail(rows).to_dict(orient="records"),
    }


async def _import_data(categories: str = "etfs,mf,cot,fii_dii") -> dict:
    """Run the data import pipeline for the given categories."""
    import subprocess
    cat_list = [c.strip() for c in categories.split(",") if c.strip()]
    cmd = [
        sys.executable, "src/main.py", "import",
        "--category", ",".join(cat_list),
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, cwd=str(_PROJECT_ROOT))
    return {
        "status":     "ok" if proc.returncode == 0 else "error",
        "categories": cat_list,
        "stdout":     proc.stdout[-2000:] if proc.stdout else "",
        "stderr":     proc.stderr[-500:]  if proc.stderr else "",
        "returncode": proc.returncode,
    }


def _summarise(ml: dict, blended_w: float, regime: str) -> str:
    """One-line plain-English recommendation."""
    pct     = blended_w * 100
    sig     = ml["regime_signal"]
    prob_up = ml["prob_up"]
    ret     = ml["expected_return_pct"]
    auc     = ml["cv_auc_mean"]
    if auc <= 0.5 or ml["cv_r2_mean"] <= 0:
        return (
            f"Model has no directional skill (AUC={auc:.3f}) — "
            f"Kelly contribution suppressed. RG-only position: {pct:.0f}%."
        )
    direction = "up" if prob_up > 0.5 else "down"
    return (
        f"Model is {prob_up:.0%} confident GOLDBEES goes {direction} "
        f"({ret:+.2f}% expected over 5d, AUC={auc:.3f}). "
        f"Recommended position: {pct:.0f}% of allocation ({sig})."
    )


def _format_pipeline_report(result: dict) -> str:
    """
    Pre-formatted display report — return this verbatim to the user.
    Prevents the LLM from inventing scores or metrics not in the pipeline.
    """
    ml  = result["ml"]
    w   = result["weights"]
    kd  = result["kelly_detail"]
    sep = "━" * 46

    alerts = ""
    if kd["alerts"]:
        alerts = "\n  ⚠  Kelly alerts\n"
        for a in kd["alerts"]:
            alerts += f"     • {a}\n"

    saved = "✓ Checkpoint saved to DB" if result["checkpoint_saved"] else "— Dry run (not saved)"

    return f"""
{sep}
  GOLDBEES  |  {result['as_of']}
{sep}
  Close         : ₹{result['latest_close']}
  Regime        : {result['regime']}
  GARCH vol     : {result['garch_vol_pct']}%   (target 15%)
  Price vs EMA50: {result['price_vs_ema50']} {'⬆' if result['price_vs_ema50'] == 'above' else '⬇'}

  ML Signal  (these are the ONLY model outputs — no other scores exist)
  ──────────────────────────────────────────────────
  Probability up    : {ml['prob_up']:.1%}
  Expected return   : {ml['expected_return_pct']:+.3f}%  (5-day log return)
  Confidence band   : [{ml['confidence_band'][0]:+.2f}%, {ml['confidence_band'][1]:+.2f}%]
  Regime signal     : {ml['regime_signal']}
  Model AUC         : {ml['cv_auc']:.4f}   (0.5 = random, >0.55 = useful)
  AUC skill score   : {ml['cv_skill']:+.4f}  (≤0 disables Kelly)
  Hit ratio (CV)    : {ml['hit_ratio']:.1%}
  Training rows     : {ml['n_training_rows']:,}

  Position Weights  (blended_50 is the recommended weight)
  ──────────────────────────────────────────────────
  Rule-based RG     : {w['rg']:.1%}
  Kelly only        : {w['kelly']:.1%}
  Blended 50/50  ★  : {w['blended_50']:.1%}   ← USE THIS
  Blended 70/30     : {w['blended_30']:.1%}

  Kelly detail: σ from {kd['sigma_source']}, haircut {kd['haircut']:.0%}, raw Kelly {kd['raw_kelly']:.2f}×
{alerts}
  {saved}
{sep}
  RECOMMENDATION: {result['recommendation']}
{sep}

⚠ GROUNDING NOTE: The numbers above are the complete pipeline output.
  Do NOT add composite scores, macro ratings, sentiment scores, or
  any metrics not shown here — they are not produced by this pipeline.
""".strip()


# ── Entry point ────────────────────────────────────────────────────────────────

async def main():
    async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
        await app.run(
            read_stream,
            write_stream,
            InitializationOptions(
                server_name="ofin-goldbees-pipeline",
                server_version="1.0.0",
                capabilities=app.get_capabilities(
                    notification_options=None,
                    experimental_capabilities={},
                ),
            ),
        )


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
