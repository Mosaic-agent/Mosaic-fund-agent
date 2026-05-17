---
name: goldbees-pipeline
description: Run the full GOLDBEES ML pipeline (LightGBM prob_up → Kelly weight → Risk Governor blend) and show the recommendation. Supports --no-save (dry run), --evaluate (past-call accuracy), and --latest (read last stored signal). Use when the user asks "run pipeline", "today's GOLDBEES signal", "what should I do with GOLDBEES", or invokes /goldbees-pipeline.
---

# Skill: GOLDBEES Pipeline

Run the full GOLDBEES investment pipeline and show a recommendation.

## Trigger

Use this skill when the user asks:
- "Run the pipeline"
- "Today's GOLDBEES signal"
- "What should I do with GOLDBEES?"
- "/goldbees-pipeline" (with or without flags)

## What it does

1. Fetches latest ML prediction (LightGBM classifier → prob_up)
2. Computes Kelly-optimal position weight
3. Blends with rule-based Risk Governor (GARCH vol + regime)
4. Saves checkpoint to DB
5. Shows recommended position size

## Usage

```
/goldbees-pipeline
/goldbees-pipeline --no-save        (dry run, don't persist to DB)
/goldbees-pipeline --evaluate       (show realised accuracy of past calls)
/goldbees-pipeline --latest         (read last stored signal, no retraining)
```

## Steps to execute

Parse any flags from the user's message, then:

**Default / no flags:**
Use the `run_pipeline` MCP tool with `save: true`.
Present the result as:
- Regime + GARCH vol
- ML signal: prob_up, expected return, AUC
- Weights table: RG / Kelly / Blended-50 / Blended-30
- One-line recommendation

**--no-save:**
Use `run_pipeline` with `save: false`.

**--evaluate:**
Use `evaluate_performance` with `rows: 15`.
Show hit ratio, MAE, RMSE and recent predictions table.

**--latest:**
Use `get_latest_signal`.
Show last stored signal without retraining.

## Output format

```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  GOLDBEES  |  {date}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Regime      : {regime}   GARCH vol: {garch_vol}%
  Price vs EMA50: {above/below}

  ML Signal
  ─────────
  Probability up   : {prob_up}
  Expected return  : {expected_return}%  (5-day)
  Confidence band  : [{low}%, {high}%]
  Model AUC        : {auc}  (skill: {skill})

  Position Weights
  ─────────────────
  Rule-based (RG)  : {rg}%
  Kelly only       : {kelly}%
  Blended 50/50    : {blended_50}%   ← recommended
  Blended 70/30    : {blended_30}%

  Recommendation: {one-line summary}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
```

## Grounding (do NOT invent)

When the tool returns a `display_report` field, show it **verbatim**. Do not invent:
- Composite scores like "69/100"
- Macro/sentiment/flow sub-scores
- "ACCUMULATE" / "STRONG BUY" labels — use `regime_signal` as-is (BUY / WATCH_LONG / HOLD / WATCH_SHORT / SELL)
- The recommended weight is `weights.blended_50`, **not** `weights.rg`
