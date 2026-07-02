# GEMINI.md — Mosaic-agent project instructions

This file is read automatically by Gemini CLI when working in this project.

## Project Overview

Mosaic is an agentic quantitative research, asset-allocation, and risk-management platform for Indian and global ETF/equity markets. It integrates walk-forward machine learning predictions (LightGBM), dynamic volatility scaling (GARCH), multi-pillar signal aggregation, and institutional flow (whale) tracking. Broker integrations (like Zerodha Kite) serve as data ingestion feeds rather than the core identity.

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
python src/main.py crossover --symbol GOLDBEES # MA crossover backtester
python src/main.py scan-setups              # ETF volume-volatility setups scanner
python src/main.py ui                       # Streamlit at localhost:8501
python src/main.py config                   # show masked config
```

### Key Scripts (run from project root)
```bash
python src/scripts/portfolio/fund_mom_returns.py --scheme 152056   # MoM NAV returns
python src/scripts/portfolio/portfolio_health_check.py
python src/scripts/portfolio/import_stocks_parallel.py --workers 5  # parallel stock import
python src/scripts/portfolio/inr_hedge_report.py
python src/scripts/db/fix_bad_data.py                              # deduplication, watermark & price repair
python src/scripts/dsp/import_all_dsp_equity.py                    # DSP holdings import
python src/scripts/dsp/import_latest_dsp.py                        # latest month only
python src/scripts/fund_imports/run.py <icici|nippon|all>          # Nippon: dynamic URL discovery from 2024 onward
python src/scripts/market/analyze_news_trend.py
python src/scripts/goldbees_report.py                              # GOLDBEES signal (pre-baked, ~2s, use instead of MCP)
python src/scripts/market/whale_tracker.py                         # all 7 multi-asset funds
python src/scripts/market/metals_quant_scorecard.py                # Gold/Silver/Copper 4-pillar scorecard
python src/ml/trend_predictor.py                                   # LightGBM forecast
python src/scripts/db_metadata_init.py                              # seed table schemas & SQL templates to Qdrant
python src/scripts/news_rag_backfill.py --migrate-qdrant           # migrate ClickHouse embeddings to Qdrant news collection
```

### Setup & Docker

**Manual setup (requires Python 3.11):**
```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
brew install libomp          # macOS — required by LightGBM
cp .env.example .env         # fill in API keys
docker compose up clickhouse -d
docker compose up            # full stack (clickhouse + ui + app)
```

**Zero-dependency / Docker setup (Docker Desktop required):**
- **Start Dashboard:** `./run.sh` (macOS/Linux) or `run.bat` (Windows)
- **Stop Dashboard:** `./stop.sh` (macOS/Linux) or `stop.bat` (Windows)
- **Run CLI/Scripts in Docker:** Use the `mosaic.sh`/`mosaic.bat` wrappers:
  - `./mosaic.sh comex`
  - `./mosaic.sh ask "question"`
  - `./mosaic.sh src/scripts/goldbees_report.py`


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
| ML | `src/ml/` | LightGBM 5-day forecast (`trend_predictor`), composite anomaly (`anomaly.py`). `run_composite_anomaly(df, df_cot, df_fx, df_corp_actions, symbol, category)` → 5-step pipeline: MAD-Z → GARCH(1,1) → Isolation Forest → PELT change-point → Company Event classification. Suppresses corporate actions from `is_anomaly` for ETFs only; requires ≥60 rows |
| Repository | `src/db/repository.py` | `MarketDataRepository`: typed reads, watermarks, `run_fetcher()`. Point-in-time variants: `ml_prediction_asof(date)`, `signal_composite_asof(symbol, date)` |
| Models | `src/models/portfolio.py` | Pydantic: `Holding`, `Portfolio`, `Sentiment` |
| Config | `config/settings.py` | Pydantic `BaseSettings`; all settings from `.env` |
| UI | `src/ui/app.py` | Streamlit hub (5 tabs over ClickHouse) |

**14 fetchers:** `yfinance`, `mfapi`, `cot` (CFTC Socrata), `nse_inav`, `fii_dii`,
`imf_reserves`, `etf_aum`, `mf_holdings` (Morningstar), `fx_rates`, `nse_quote`,
`yahoo_snapshot`, `expert_tweets`, `nse_corporate_actions` (NSE equity splits/bonuses/demergers/rights/dividends), plus news tools.

**Whale Tracker funds** (`src/scripts/market/whale_tracker.py`) — all 7 multi-asset funds:

| Scheme Code | Fund | History |
|---|---|---|
| `RLMF806` | Nippon India Multi Asset | 57 months (deepest; dynamic import from 2024) |
| `152056` | DSP Multi Asset | 33 months |
| `154167` | DSP Multi Asset Omni FoF | 3 months |
| `152639` | Bajaj Multi Asset | 2 months |
| `120821` | Quant Multi Asset | 2 months |
| `120334` | ICICI Multi Asset | 1 month (shows ⚠ insufficient data) |
| `120716` | ICICI Multi Asset II | 1 month (shows ⚠ insufficient data) |

**LLM config:** `LLM_PROVIDER` (`openai` or `anthropic`) + `LLM_MODEL`.
Set `LLM_BASE_URL` to an OpenAI-compatible endpoint for local inference (Ollama, LM Studio).

### Architecture (analyze command flow)
```
CLI → MosaicFundAgent.run()
  → KiteMCPClient          (Zerodha auth via mcp.kite.trade)
  → _parse_holdings()      (raw Kite → List[Holding])
  → asset_analyzer         (per holding: yfinance + earnings + news)
  → portfolio_analyzer     (LangGraph ReAct LLM scoring)
  → output/                (JSON + HTML reports)
```

### ClickHouse Tables (database: market_data)
`daily_prices`, `mf_nav`, `mf_holdings`, `fii_dii_flows`, `fii_dii_monthly`,
`cot_gold`, `cb_gold_reserves`, `etf_aum`, `inav_snapshots`, `fx_rates`,
`ml_predictions`, `signal_composite`, `news_articles`, `import_watermarks`,
`corporate_actions` (symbol, ex_date, action_type, ratio, purpose — NSE fetched; price-impacting ex-dates suppress anomaly detection).
All use `ReplacingMergeTree` — always query with `FINAL` to deduplicate.

### Important Patterns
- **Tool registration:** Tools are `@tool`-decorated function lists (`YAHOO_TOOLS`, `NEWS_TOOLS`). `ALL_TOOLS` in `mosaic_fund_agent.py` is the union passed to `create_react_agent`.
- **Scraping fallbacks:** Screener.in is primary for earnings; BSE/Yahoo Finance are fallbacks. `fake-useragent` rotates user-agents.
- **Caching:** NewsAPI/COMEX responses cached to `output/.cache/` with 1-hour TTL.
- **Output files:** Reports written to `./output/` as JSON/HTML.
- **Repository pattern:** `MarketDataRepository` (`src/db/repository.py`) is the single access point for all ClickHouse reads. Use `repo.ml_prediction_asof(date)` and `repo.signal_composite_asof(symbol, date)` for point-in-time queries (e.g. what the model said on a historical anomaly date). Never write raw SQL in signal/ML code.
- **Vector RAG / Qdrant:** 6 collections (768d nomic-embed-text, COSINE) power retrieval-augmented context — news, MF holdings/profiles, anomaly precedent, DB schemas. Full reference (diagram, embedding pipeline, two-pass symbol-scoped news retrieval, read-tool table): **[docs/rag-architecture.md](docs/rag-architecture.md)**.
- **Anomaly pipeline:** `run_composite_anomaly` (`src/ml/anomaly.py`) is a 5-step composite: (1) MAD robust Z, (2) GARCH(1,1) standardised residuals, (3) Isolation Forest confidence multiplier, (4) PELT change-point detection (`ruptures`, rbf cost), (5) Company Event classification. Pass `df_corp_actions`; for ETFs (`category="etfs"`) split/bonus/demerger ex-dates are excluded from `df_flagged` (regime label `🏢 Price Driven by Company Event`); for stocks/commodities they remain in `df_flagged` and are stored in Qdrant (with the same label). Automatically stores anomalies in Qdrant if `symbol` and `category` are passed. Gracefully falls back to naive threshold if `arch`/`ruptures` missing or <60 rows.
- **Anomaly tools (two separate tools):**
  - `explain_price_anomalies` (`market/gold.py`) — gold/commodity-specific; sequential news search + COT/FX cross-asset. Always call in parallel with `plot_price_chart`.
  - `search_anomaly_events(symbol, days)` (`market/equity.py`) — equity-generic; loads corp actions from ClickHouse → runs pipeline with suppression → **parallel** Google News (ThreadPoolExecutor, ±1d fallback, NewsAPI for <30d dates). Call for any NSE/BSE stock anomaly investigation.
  - `get_corporate_actions(symbol)` (`market/equity.py`) — fetches NSE corporate actions, upserts to `corporate_actions` table, returns history. Call when chart shows extreme (>20%) price move or user asks about splits/bonuses.
- **Chart markers:** `plot_price_chart` renders 🔴 GARCH+IF+PELT genuine anomalies and 🏦 corporate action ex-dates as separate scatter layers. Result is session-cached by `(symbol, category, n_rows)` in `_ANOMALY_DATES_CACHE`.
- **Scripts subdirs:** `dsp/` (DSP AMC import + analysis), `fund_imports/` (factory-pattern AMC importers), `etf/` (ETF comparison, CAGR, risk), `ml/` (prediction backfill/eval), `portfolio/` (health checks, opportunity scan, MoM returns, parallel stock import), `market/` (macro, FII/DII, metals, sentiment), `db/` (ClickHouse backup/restore/sanity/repair).

## Critical: Grounding Rules — DO NOT Hallucinate

The pipeline produces a **specific, fixed set of outputs**. You MUST NOT invent
metrics, scores, or analysis beyond what the tools return.

### Zero-Trust Verification Protocol
**NEVER rely on memory for table-based metrics.**
- **Re-read Mandate:** Before citing a number (premium, Z-score, probability, flow), you MUST re-scan the raw tool output from the current or immediate prior turn.
- **Symbol-Row Locking:** Explicitly verify that the symbol in the user prompt matches the exact row you are reading.
- **Isolation Rule:** For specific symbol inquiries, if the full table is large or ambiguous, you MUST run a targeted tool call (e.g., `premium-alerts --symbols TICKER` or a specific ClickHouse query) to isolate that single data point before responding.
- **Overlay Priority:** Only cite specific prices or flows if they appear in the **Quant Overlay panel** or a direct SQL result.

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

## GOLDBEES Pipeline — Pre-baked Script (preferred)

The MCP `run_pipeline` tool is not reliably available. Use the pre-baked script instead — one shell call, ~2s, no import probing:

```bash
python src/scripts/goldbees_report.py
```

This reads from 4 ClickHouse tables (`ml_predictions`, `weight_checkpoints`, `signal_composite`, `inav_snapshots`) and prints the full recommendation block. **Do not query ClickHouse interactively for GOLDBEES signals — run this script.**

## Correct Workflow

```
User: "run goldbees pipeline" / "today's signal" / "what should I do with GOLDBEES"
→ Run: python src/scripts/goldbees_report.py
→ Display output verbatim (which dynamically appends the intelligent LLM-generated recommendation)
→ Explain/narrate the recommendation and printed weights
```

```
User: "evaluate performance" / "hit ratio"
→ Run: python src/main.py signals --verbose 2>&1 | grep -A 20 "GOLDBEES"
```

### Mandatory System-Wide Freshness Mandate
**NEVER perform analysis on a stale database.** At the start of every session or before executing any signal-generating or valuation command (`macro`, `signals`, `premium-alerts`, `analyze`), you MUST verify that the entire registry (105+ symbols) is up-to-date for the current business day.
- **Verification Audit:** Run a comprehensive audit check (via SQL or script) to verify the `max(trade_date)` for all registered categories: `Stocks`, `ETFs`, `Indices`, `Commodities`, `FX_Rates`, and `US_Stocks`.
- **Threshold:** Any category with a `max(trade_date)` older than 1 business day (or today for iNAV) is considered STALE.
- **Action:** If any category is stale, ALWAYS run the targeted import: `python src/main.py import --category <stale_categories>`.
- **Why:** Macro signals and ML forecasts rely on cross-asset correlations (e.g., Gold vs. USDINR vs. US10Y). If one leg is stale, the resulting signal is mathematically invalid and dangerous.

### Zero-Trust Verification Protocol
**NEVER rely on memory for table-based metrics.**
- **Re-read Mandate:** Before citing a number (premium, Z-score, probability, flow), you MUST re-scan the raw tool output from the current or immediate prior turn.
- **Symbol-Row Locking:** Explicitly verify that the symbol in the user prompt matches the exact row you are reading.
- **Isolation Rule:** For specific symbol inquiries, if the full table is large or ambiguous, you MUST run a targeted tool call (e.g., `premium-alerts --symbols TICKER` or a specific ClickHouse query) to isolate that single data point before responding.
- **Overlay Priority:** Only cite specific prices or flows if they appear in the **Quant Overlay panel** or a direct SQL result.

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

### Commit and PR Workflow
- Committing directly to `main` locally is fine — PRs always use **squash-and-merge**, so history stays clean.
- Never add a `Co-Authored-By:` trailer to git commit messages. Write clean, single-author commit messages only.
- **NEVER stage or commit code automatically.** You must always wait for the user to explicitly prompt or approve a commit (e.g. "commit changes", "/commit", "please commit this"). Do not run git commit commands proactively unless directly requested.

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

## Antigravity (agy) & Claude Code Skills & Slash Commands

When working with Antigravity (`agy`) or Claude Code (`claude`), the following slash commands (skills) are available. Recommend them to the user as appropriate:

### Antigravity Slash Commands
| Command | Trigger / Purpose | What it does / When to recommend |
|---|---|---|
| `/goal` | "run a long task", "overnight run" | Recommend when the user wants to run a long-running, thorough task and the agent should keep going until the goal is fully achieved. |
| `/schedule` | "schedule this daily", "set cron" | Recommend when the user wants to run an instruction on a recurring schedule or set a one-time timer. |
| `/grill-me` | "interactive design", "clarify plan" | Recommend when the user wants to align on an implementation plan through an interactive interview to resolve design decisions. |
| `/commit` | "commit this", "commit changes" | Stage and commit the current working-tree changes using Conventional Commits. |
| `/daily-signal-composite` | "What should I buy today?", "run signals" | Run the composite signal aggregator to compute scores for all 18 ETFs. |
| `/data-engineering-importer` | "import all", "database repair" | Trigger the historical ClickHouse data engineering pipeline. |
| `/dsp-multi-asset-importer` | "import dsp data", "backfill dsp" | Backfill and validate DSP Multi Asset allocation fund holdings. |
| `/etf-news` | "etf news", "sentiment analysis" | Fetch and tag latest news articles by ETF category with sentiment. |
| `/etf-setups` | "etf setups", "volume volatility scan" | Scan all 18 tracked ETFs for volume-volatility breakouts, exhaustion, or squeezes. |
| `/goldbees-pipeline` | "run goldbees pipeline", "today's signal" | Run the full GOLDBEES ML prediction, Kelly sizing, and Risk Governor blend. |
| `/intraday` | "track intraday", "intraday GOLDBEES" | Run the real-time read-only intraday signal monitor for an ETF or stock. |
| `/ma-crossover` | "run crossover backtest", "golden cross" | Backtest SMA/EMA crossovers and plot equity curve performance. |
| `/macro-scanner` | "macro trends today", "geopolitical events" | Scan live macro events and map their directional impact to ETFs. |
| `/macro-strategy` | "macro strategy", "positioning" | Deep dive into the 2026 macro supercycle, real assets, and domestic alpha. |
| `/risk-governor` | "position size", "garch vol" | Calculate GARCH-based position sizing and volatility targeting. |

### Claude Code Slash Commands
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

Both agents share the same persistent memory guidelines for the project:
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

## System Architecture Pattern: Modular Monolith with API Gateway

To prevent unnecessary microservice complexity and dependency bloat, Mosaic enforces a **Modular Monolith with API Gateway** pattern:
- **Unified Repository**: Keep all core modules (ml, tools, analyzers, importer) in the single Python project workspace to share repo access pools (`MarketDataRepository`, `CHPool`), config files, and Pydantic models.
- **API Gateway Service (`src/ui/agent_server.py`)**: Use a single multithreaded backend web server to host static assets (`website/app.html`) and expose structured JSON REST API endpoints (`/api/query`, `/api/import/run`, `/api/import/status`, `/api/anomaly/scan`, `/api/dilution/check`).
- **Asynchronous Offloading**: Offload heavy blocking operations (like data ingestion pipelines or massive backtests) to background threads or subprocesses (`subprocess.Popen`), writing to standard logs (`output/import_run.log`) which the UI polls for real-time console streaming.
- **Direct Database Interface**: Run all client data fetching requests through the server-backed `/api/query` ClickHouse interface rather than spinning up multiple independent database microservices.
