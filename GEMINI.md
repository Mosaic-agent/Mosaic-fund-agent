# GEMINI.md — Mosaic-agent project instructions

This file is read automatically by Gemini CLI when working in this project.

## Project Overview

Mosaic-agent is a quantitative investment platform for Indian equity/commodity ETFs.
The core pipeline runs LightGBM classification → Kelly position sizing → Risk Governor blend
for GOLDBEES (gold ETF).

### CLI Commands
```bash
python src/main.py analyze --max 3          # analyse portfolio (limit 3)
python src/main.py analyze                  # full live portfolio (Zerodha login required)
python src/main.py ask "question"           # ReAct LLM Q&A over portfolio
python src/main.py import --category etfs,stocks,mf,fii_dii,cot,fx_rates
python src/main.py import --full            # full backfill (ignores watermarks)
python src/main.py import --dry-run
python src/main.py signals --save --verbose # composite signal aggregator
python src/main.py macro --max 3            # macro theme scanner
python src/main.py comex                    # COMEX pre-market gold/silver/copper
python src/main.py etf-news --max 3 --save  # ETF news sentiment
python src/main.py premium-alerts           # iNAV premium/discount alerts
python src/main.py ui                       # Streamlit at localhost:8501
python src/main.py config                   # show masked config
```

### Key Scripts (run from project root)
```bash
python src/scripts/portfolio/fund_mom_returns.py --scheme 152056   # MoM NAV returns
python src/scripts/portfolio/portfolio_health_check.py
python src/scripts/portfolio/inr_hedge_report.py
python src/scripts/dsp/import_all_dsp_equity.py                    # DSP holdings import
python src/scripts/dsp/import_latest_dsp.py                        # latest month only
python src/scripts/fund_imports/run.py <icici|nippon|all>
python src/scripts/market/analyze_news_trend.py
python src/ml/trend_predictor.py                                   # LightGBM forecast
```

### Setup & Docker
```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
brew install libomp          # macOS — required by LightGBM
cp .env.example .env         # fill in API keys
docker compose up clickhouse -d
docker compose up            # full stack (clickhouse + ui + app)
```

### Tests
```bash
python tests/test_tools.py
python tests/test_quant_signals.py
python tests/test_news_sentiment.py
python tests/test_cache.py
python tests/test_inav_cli.py
python tests/test_macro_theme_agent.py
python tests/_test_importer.py      # requires live ClickHouse
python tests/_validate_ml.py
python tests/_backtest_anomaly.py
```

### Key Module Layers
| Layer | Path | Role |
|-------|------|------|
| CLI | `src/main.py` | Typer commands; entry point |
| Agents | `src/agents/` | LangChain/LangGraph orchestrators |
| Analyzers | `src/analyzers/` | `asset_analyzer` (per-holding), `portfolio_analyzer` (aggregate) |
| Tools | `src/tools/` | Pure functions returning dict/DataFrame |
| Importer | `src/importer/` | Delta-sync pipeline: fetchers → ClickHouse |
| DB Pool | `src/db/pool.py` | Thread-safe `CHPool` singleton (`get_pool()`) |
| ML | `src/ml/` | LightGBM 5-day forecast, GARCH + Isolation Forest anomaly |
| Models | `src/models/portfolio.py` | Pydantic: `Holding`, `Portfolio`, `Sentiment` |
| Config | `config/settings.py` | Pydantic `BaseSettings`; all settings from `.env` |
| UI | `src/ui/app.py` | Streamlit hub (5 tabs over ClickHouse) |

**13 fetchers:** `yfinance`, `mfapi`, `cot` (CFTC Socrata), `nse_inav`, `fii_dii`,
`imf_reserves`, `etf_aum`, `mf_holdings` (Morningstar), `fx_rates`, `nse_quote`,
`yahoo_snapshot`, `expert_tweets`, plus news tools.

**LLM config:** `LLM_PROVIDER` (`openai` or `anthropic`) + `LLM_MODEL`.
Set `LLM_BASE_URL` to an OpenAI-compatible endpoint for local inference (Ollama, LM Studio).

### Architecture (analyze command flow)
```
CLI → PortfolioAgent.run()
  → KiteMCPClient          (Zerodha auth via mcp.kite.trade)
  → _parse_holdings()      (raw Kite → List[Holding])
  → asset_analyzer         (per holding: yfinance + earnings + news)
  → portfolio_analyzer     (LangGraph ReAct LLM scoring)
  → output/                (JSON + HTML reports)
```

### ClickHouse Tables (database: market_data)
`daily_prices`, `mf_nav`, `mf_holdings`, `fii_dii_flows`, `fii_dii_monthly`,
`cot_gold`, `cb_gold_reserves`, `etf_aum`, `inav_snapshots`, `fx_rates`,
`ml_predictions`, `signal_composite`, `news_articles`, `import_watermarks`.
All use `ReplacingMergeTree` — always query with `FINAL` to deduplicate.

### Important Patterns
- **Tool registration:** Tools are `@tool`-decorated function lists (`YAHOO_TOOLS`, `NEWS_TOOLS`). `ALL_TOOLS` in `portfolio_agent.py` is the union passed to `create_react_agent`.
- **Scraping fallbacks:** Screener.in is primary for earnings; BSE/Yahoo Finance are fallbacks. `fake-useragent` rotates user-agents.
- **Caching:** NewsAPI/COMEX responses cached to `output/.cache/` with 1-hour TTL.
- **Output files:** Reports written to `./output/` as JSON/HTML.
- **Scripts subdirs:** `dsp/` (DSP AMC import + analysis), `fund_imports/` (factory-pattern AMC importers), `etf/` (ETF comparison, CAGR, risk), `ml/` (prediction backfill/eval), `portfolio/` (health checks, opportunity scan, MoM returns), `market/` (macro, FII/DII, metals, sentiment), `db/` (ClickHouse backup/restore/sanity).

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

## User Context

Dhiraj's wife is a treasurer at a major Indian AMC. Her domain expertise shaped the
platform's fund/ETF signal design, macro theme mapping, and institutional flow interpretation.
When explaining financial concepts, assume strong domain knowledge — no need to define
basic MF, ETF, or institutional flow terms.

## Persistent Rules (Claude Memory — apply here too)

These rules are enforced across all Claude sessions in this project. Gemini should
follow the same norms for consistency:

### No LLM Calculations
**Never compute any number inside your response.** All numeric work — returns,
ratios, aggregations, scores, Kelly fractions — must be computed by Python or SQL
and passed in as pre-computed results. The LLM (Gemini or Claude) is only allowed
to narrate results that already exist in the tool/script output.

- ✅ Run `python src/scripts/portfolio/fund_mom_returns.py --scheme 152056`, then summarise
- ✅ Run a ClickHouse query, then interpret the result rows
- ❌ "The average MoM return is approximately X%" — if you computed that yourself, do not state it
- ❌ Deriving PE ratios, CAGR, Kelly weights, or any financial metric from training knowledge

### Commit Style
Never add a `Co-Authored-By:` trailer to git commit messages. Write clean,
single-author commit messages only.

### Verify Dilution Before Flagging Promoter Sale
A drop in promoter % is **not** the same as a promoter sale. Before concluding
"promoter sell-down" from a shareholding-pattern drop, check whether the
denominator (total shares outstanding) expanded in the same/prior quarter:
- **QIP** (qualified institutional placement) — search "[company] QIP [year]"
- **Preferential allotment** to investors
- **Rights issue** / **bonus issue** (bonus doesn't dilute %, watch for confusion)
- **ESOP exercise** (small dilution)
- **M&A share issuance** (acquirer paying in shares)

A promoter-% drop with **unchanged absolute share count** = dilution, not sale.
A promoter-% drop with **lower absolute share count** = actual sale (red flag).
Screener.in's shareholding history shows % only — cross-reference with the
annual report's "Equity Capital" line for total shares outstanding to confirm.

*Why this rule exists:* I once read a 4.59% drop in Techno Electric promoter
holding (Q1FY25 61.52% → Sep-24 56.93%) as a sell signal. It turned out to be
dilution from a ₹1,250 Cr QIP in July 2024 — fresh equity issued at ₹1,440/share
to institutions. The promoter's absolute share count was unchanged.

### DSP Active-Fund Holdings = Highest-Conviction Single-Name Signal
DSP active-fund holdings in `market_data.mf_holdings` are the primary
institutional-quality signal for single-name Indian equity research. Cross-fund
ownership (same name held by 2+ active DSP funds for 24+ months) is the
strongest possible long-term conviction marker.

**Active funds (meaningful signal):**
`DSP_SMALL_CAP`, `DSP_MID_CAP`, `DSP_LARGE_AND_MID_CAP`, `DSP_FLEXI_CAP`,
`DSP_MULTICAP`, `DSP_FOCUSED`, `DSP_VALUE`, `DSP_TIGER`, `DSP_BUSINESS_CYCLE`,
`DSP_ELSS_TAX_SAVER`, `DSP_HEALTHCARE`, `DSP_BANKING_FINANCIAL_SERVICES`,
`DSP_QUANT`.

**Passive funds (weak signal — index tracking, ignore):**
`DSP_NIFTY_*_INDEX`, `DSP_NIFTY_*_ETF`, `DSP_BSE_*_ETF`,
`DSP_*_QUALITY_50_INDEX`.

**How to apply:**
- Query `market_data.mf_holdings FINAL` filtered by `fund_name LIKE 'DSP%'`,
  ordered by `market_value_cr DESC` — get latest position and `months_held`.
- MoM `pct_of_nav` change is the conviction *trend*: rising = manager adding,
  falling = trimming.
- For "find me an opportunity" prompts, cross-reference top adds (Jan vs Apr
  2026 deltas) against technical setups (RSI, drawdown, volume) — the
  intersection is the highest-quality idea pool.
- Holdings data extends back to Jun 2022; reliable from Sep 2023 onwards
  (62-fund coverage).
- When interpreting any holding % change, apply the QIP/dilution check above
  before drawing a sale/sell-down conclusion.

## Claude Code Skills Available in This Project

When working alongside Claude Code (`claude` CLI), these slash commands (skills)
are available. Reference them when suggesting a workflow step:

| Skill | Trigger | What it does |
|-------|---------|-------------|
| `/goldbees-pipeline` | "run goldbees", "pipeline signal" | Full GOLDBEES ML pipeline end-to-end |
| `/commit` | "commit this" | Stages and commits with a well-formed message |
| `/review` | "review PR #N" | Multi-agent pull request review |
| `/security-review` | "security review" | Reviews pending branch changes for vulnerabilities |
| `/simplify` | "simplify this code" | Reviews changed code for quality/efficiency |
| `/schedule` | "schedule this daily" | Creates a cron-scheduled remote agent routine |
| `/loop` | "loop every 5m" | Runs a prompt or command on a recurring interval |
| `/update-config` | "allow X", "add hook for Y" | Edits Claude Code settings.json/hooks |
| `/init` | "init CLAUDE.md" | Generates CLAUDE.md documentation for the repo |

Claude Code also has **persistent project memory** at:
`.claude/projects/-Users-dhiraj-thakur-project-ofin-agent/memory/`

Current memory entries:
- `user_background.md` — AMC domain context
- `feedback_no_llm_calculations.md` — all numeric work in code, never in LLM
- `feedback_no_coauthor.md` — no Co-Authored-By in commits
- `feedback_qip_dilution_check.md` — verify QIP/dilution before flagging promoter sale
- `project_dsp_holdings_signal.md` — DSP active-fund cross-ownership is the primary institutional signal

## Number Sources

All market data comes from ClickHouse (live DB). If a number is in the tool
response, it came from the DB. Do not substitute numbers from training data or
general knowledge — gold prices, FII flows, USDINR etc. change daily.

## DSP Fund Holdings — ClickHouse Schema

`market_data.mf_holdings` contains DSP fund portfolio disclosures.

**Columns:** `scheme_code`, `fund_name`, `as_of_month` (Date), `isin`,
`security_name`, `asset_type`, `market_value_cr` (Float64), `pct_of_nav` (Float64), `imported_at`.

**Coverage:** 62 DSP funds, Sep 2023 – Mar 2026 (31 months). Top 10 funds go back to Jun 2022.

**Key fund_name values:**
`DSP_MULTI_ASSET`, `DSP_MULTI_ASSET_OMNI_FOF`, `DSP_MID_CAP`, `DSP_SMALL_CAP`,
`DSP_FLEXI_CAP`, `DSP_LARGE_CAP`, `DSP_LARGE_AND_MID_CAP`, `DSP_TIGER`,
`DSP_ELSS_TAX_SAVER`, `DSP_NRNEF`, `DSP_AGGRESSIVE_HYBRID`,
`DSP_DYNAMIC_ASSET_ALLOCATION`, `DSP_QUANT`, `DSP_VALUE`, `DSP_HEALTHCARE`,
`DSP_ARBITRAGE`, `DSP_EQUITY_SAVINGS`, `DSP_MULTICAP`, `DSP_BUSINESS_CYCLE`,
`DSP_NIFTY_50_INDEX`, `DSP_NIFTY_50_ETF`, `DSP_NIFTY_NEXT_50_INDEX`,
`DSP_NIFTY_MIDCAP_150_INDEX`, `DSP_NIFTY_SMALLCAP_250_INDEX`, `DSP_NIFTY_500_INDEX`,
`DSP_GOLD_ETF`, `DSP_SILVER_ETF`, `DSP_NIFTY_BANK_ETF`, `DSP_NIFTY_IT_ETF`,
`DSP_MSCI_INDIA_ETF`, and 33 more index/ETF variants.

**Correct query pattern:**
```sql
SELECT fund_name, as_of_month, security_name, pct_of_nav, market_value_cr, asset_type
FROM market_data.mf_holdings FINAL
WHERE fund_name = 'DSP_MULTI_ASSET'
  AND as_of_month = (SELECT max(as_of_month) FROM market_data.mf_holdings FINAL
                     WHERE fund_name = 'DSP_MULTI_ASSET')
ORDER BY pct_of_nav DESC;
```

**NEVER use** `weight_pct` or `name` — those columns do not exist.

## DSP NAV / Return Analysis

To get month-over-month NAV returns for any DSP (or any Indian MF) fund:

```bash
# By MFAPI scheme code
python src/scripts/portfolio/fund_mom_returns.py --scheme 152056

# By name search (interactive picker)
python src/scripts/portfolio/fund_mom_returns.py --search "DSP Multi Asset"

# Custom lookback (default 12 months)
python src/scripts/portfolio/fund_mom_returns.py --scheme 152056 --months 24
```

**Key DSP scheme codes (Direct Growth plans):**
| Fund | Scheme Code |
|------|-------------|
| DSP Multi Asset Allocation | 152056 |
| DSP Multi Asset Omni FoF | 154167 |
| DSP Mid Cap | 119071 |
| DSP Small Cap | 119212 |
| DSP Flexi Cap | 119076 |
| DSP Large Cap | 119250 |
| DSP Large & Mid Cap | 119218 |
| DSP ELSS Tax Saver | 119242 |
| DSP TIGER | 119247 |
| DSP Dynamic Asset Allocation | 126393 |
| DSP Quant | 147306 |
| DSP Value | 148595 |
| DSP Healthcare | 145454 |

The script fetches live NAV from mfapi.in and computes MoM returns. It does NOT read from ClickHouse — it pulls fresh data each run.
