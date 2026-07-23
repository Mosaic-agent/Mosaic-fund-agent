---
name: goldbees-pipeline-agent
description: Run the full GOLDBEES ML pipeline (LightGBM prob_up → Kelly weight → Risk Governor blend) and show the recommendation. Supports --no-save (dry run), --evaluate (past-call accuracy), and --latest (read last stored signal). Use when the user asks "run pipeline", "today's GOLDBEES signal", "what should I do with GOLDBEES", or invokes /goldbees-pipeline.
tools:
  - run_command
  - view_file
  - search_web
  - read_url_content
  - grep_search
  - list_dir
model: inherit
temperature: 0.1
max_turns: 8
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

Parse any flags from the user's message, then run the pre-baked report script —
**do not probe imports or query ClickHouse interactively**:

**Default / no flags / --latest:**
```bash
python src/scripts/goldbees_report.py
```
Capture stdout and display verbatim. That's it — one Bash call, no exploration needed.
All queries are pre-baked: ml_predictions, weight_checkpoints, inav, ohlcv, signal_composite.

**--evaluate:**
```bash
python src/main.py signals --verbose 2>&1 | grep -A 20 "GOLDBEES"
```
Show hit ratio from the composite score table output.

**--no-save:**
Same as default — `goldbees_report.py` is read-only (does not write to DB).

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

Let the LLM format the data and reports (e.g., into clean Markdown tables or structured panels) to ensure maximum readability and a premium layout. The LLM must still use the exact numbers returned by the tools verbatim (no self-calculations or metric inventions). Do not invent:
- Composite scores like "69/100"
- Macro/sentiment/flow sub-scores
- "ACCUMULATE" / "STRONG BUY" labels — use `regime_signal` as-is (BUY / WATCH_LONG / HOLD / WATCH_SHORT / SELL)
- The recommended weight is `weights.blended_50`, **not** `weights.rg`
