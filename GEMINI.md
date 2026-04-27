# GEMINI.md — ofin-agent project instructions

This file is read automatically by Gemini CLI when working in this project.

## Project Overview

ofin-agent is a quantitative investment platform for Indian equity/commodity ETFs.
The core pipeline runs LightGBM classification → Kelly position sizing → Risk Governor blend
for GOLDBEES (gold ETF).

## Critical: Grounding Rules — DO NOT Hallucinate

The pipeline produces a **specific, fixed set of outputs**. You MUST NOT invent
metrics, scores, or analysis beyond what the tools return.

### What the pipeline DOES produce:
- `prob_up` — probability the ETF goes up (from LightGBM classifier, 0–1)
- `expected_return_pct` — predicted 5-day log return (%)
- `confidence_band` — [low%, high%] quantile bounds
- `regime_signal` — one of: BUY / WATCH_LONG / HOLD / WATCH_SHORT / SELL
- `cv_auc` — model AUC (0.5 = random, >0.55 = useful signal)
- `cv_skill` — AUC − 0.5 (≤0 means no skill, Kelly disabled)
- `hit_ratio` — directional accuracy from walk-forward CV
- `weights.rg` — Rule-based Risk Governor weight
- `weights.kelly` — Kelly-optimal weight
- `weights.blended_50` — **recommended weight** (50% RG + 50% Kelly)
- `weights.blended_30` — conservative blend (70% RG + 30% Kelly)

### What the pipeline DOES NOT produce:
- ❌ Composite scores (e.g. "69/100") — do not invent these
- ❌ Macro signal scores (e.g. "100/100") — do not invent these
- ❌ Sentiment scores (e.g. "71/100") — do not invent these
- ❌ Flow signal scores (e.g. "72/100") — do not invent these
- ❌ "ACCUMULATE" / "STRONG BUY" labels — use the regime_signal as-is
- ❌ The RG weight (91%) is NOT the recommendation — blended_50 is

### Display rule:
When a tool returns a `display_report` field, show it **verbatim** without modification.
Do not reformat, reinterpret, or add to it.

## MCP Tools Available

`run_pipeline`, `get_latest_signal`, `evaluate_performance`, and `import_data`
are **MCP tools** registered under the `ofin-pipeline` server.
They are NOT files, scripts, or shell commands — call them directly as tools.

Do NOT use FindFiles, shell, or search to locate them.
Do NOT enter Plan Mode to decide whether to call them — just call them.

| Tool | Call when user says |
|---|---|
| `run_pipeline` | "run pipeline", "today's signal", "what should I do with GOLDBEES" |
| `get_latest_signal` | "latest signal", "last recommendation", "--latest" |
| `evaluate_performance` | "evaluate", "how accurate", "hit ratio", "--evaluate" |
| `import_data` | "refresh data", "update prices", "import" |

## Correct Workflow

```
User: "run_pipeline" or "what should I do with GOLDBEES today?"
→ Call MCP tool: run_pipeline (save: true)
→ Show display_report field verbatim — do not modify it
→ Answer follow-up questions using only the returned JSON values
```

```
User: "is the model accurate?" or "evaluate performance"
→ Call MCP tool: evaluate_performance (rows: 15)
→ Report hit_ratio, MAE, RMSE exactly as returned
→ Do not editorialize beyond the numbers
```

## Macro Scanner Output

The `macro` command output contains:
- Active theme names and headlines (from Google News RSS — text only)
- ETF net scores (integer article counts, NOT price forecasts)
- A **Quant Overlay panel** at the bottom with live DB numbers

Rules:
- Net scores are article-counts. Do NOT convert them to % return forecasts.
- Only cite specific prices or flows if they appear in the **Quant Overlay panel**.
- Do NOT add commodity prices (WTI, gold spot) from training data — they change daily.
- Do NOT add FII flow amounts unless shown in the Quant Overlay.
- Score ≥ +16 = strong bullish | +8 to +15 = moderate | ≤ -16 = strong bearish.

## Number Sources

All market data comes from ClickHouse (live DB). If a number is in the tool
response, it came from the DB. Do not substitute numbers from training data or
general knowledge — gold prices, FII flows, USDINR etc. change daily.
