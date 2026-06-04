# Architecture

> Last updated: 2026-06-04

Mosaic Fund Agent is a multi-source financial intelligence platform for Indian equity and commodity markets. It ingests market data into ClickHouse, scores assets across six independent signal pillars, runs ML forecasting and anomaly detection, and surfaces actionable recommendations via CLI, scripts, and a Streamlit UI.

---

## Design Philosophy

The codebase is built on four patterns that keep it extensible and testable. Understanding these four is enough to navigate the whole system:

| Pattern | Where | What it buys you |
|---|---|---|
| **Repository** | `src/db/repository.py` | One typed access point for every ClickHouse read. `FINAL` dedup, return shapes, and the `run_fetcher()` loop live here — no raw SQL scattered across signal/ML code. |
| **Adapter** | `src/importer/fetchers/` | Every data source implements one `Fetcher` ABC (`fetch → validate → insert`). Add a source = one class + one registry entry. |
| **Strategy** | `src/agents/signal_sources.py` | Each composite-score pillar is a `SignalSource` subclass. Add a pillar = one class + one list append; the aggregator runs them all in parallel. |
| **Observer** | `src/events/bus.py` | A live import publishes one `DataImportedEvent`; cache-invalidation, ML re-prediction, signal refresh, and sanity checks all react independently. |

Two cross-cutting rules:

- **No LLM calculations** — every number is computed in Python/SQL; the LLM only narrates pre-computed results.
- **Thin tools, fat logic** — `@tool` wrappers stay small (validate args, call a function, format output); business logic lives in `src/ml/`, `src/db/`, or dedicated tool modules so it's unit-testable without the agent loop.

See [§ Design Patterns](#design-patterns) at the end for the full reference (code, method tables, and the multi-agent orchestrator).

---

## High-Level Data Flow

```
External Data Sources
        │
        ▼
  Fetcher Adapters (src/importer/fetchers/adapters.py)
        │  Fetcher ABC: fetch() → validate() → insert()
        ▼
  MarketDataRepository.run_fetcher()  (src/db/repository.py)
        │  watermark check → insert → set_watermark → EventBus.publish()
        ▼
  EventBus  (src/events/bus.py)
        │  DataImportedEvent → async observers
        ├──▶  ModelCacheInvalidator   (sync) → clears stale .joblib
        ├──▶  MLPredictionObserver    (async) → re-runs LightGBM
        ├──▶  SignalAggregatorObserver(async) → refreshes composite scores
        └──▶  SanityCheckObserver     (async) → anomaly validation
        │
        ▼
  ClickHouse  (market_data database — 26 tables)
        │
        ├──▶  MarketDataRepository reads  ← typed queries, consistent FINAL
        ├──▶  Signal Sources  (src/agents/signal_sources.py)  ← Strategy pattern
        ├──▶  ML  (src/ml/)               ← LightGBM forecast + anomaly
        ├──▶  Tools (src/tools/)          ← real-time signals per asset
        ├──▶  Agents (src/agents/)        ← orchestrated multi-tool workflows
        │
        ▼
  Output
  ├── CLI tables (Rich console)
  ├── JSON reports  (output/)
  └── Streamlit UI  (localhost:8501)
```

---

## Directory Map

```
config/
  settings.py             Pydantic settings — LLM, ClickHouse, API keys, market constants

src/
  main.py                 Typer CLI — 13 commands (analyze, import, signals, macro, …)
  agents/
    signal_aggregator.py  Composite score orchestrator — parallel Strategy sources
    signal_sources.py     Strategy pattern: SignalSource ABC + 5 pillars + GARCHAnomalySource
  analyzers/              Per-asset and portfolio-level enrichment
  clients/
    mcp_client.py         Zerodha Kite MCP (JSON-RPC 2.0)
  db/
    pool.py               Thread-safe CHPool singleton
    repository.py         MarketDataRepository — typed reads + run_fetcher + events
  events/
    bus.py                EventBus singleton, DataImportedEvent, Observer ABC
    observers.py          4 post-import hooks (cache, ML, signals, sanity)
  formatters/             JSON / HTML report rendering
  importer/
    base_fetcher.py       Fetcher ABC — Adapter interface for all data sources
    cli.py                run_import() — delta-sync orchestrator
    clickhouse.py         Schema DDL, bulk inserts, watermark management
    registry.py           Symbol catalogs (stocks, ETFs, commodities, indices, FX)
    fetchers/
      adapters.py         Fetcher adapters + FETCHER_REGISTRY
      <name>_fetcher.py   One file per external data source
  ml/
    trend_predictor.py    LightGBM 5-day predictor + joblib model cache
    anomaly.py            Composite anomaly (Z + GARCH + Isolation Forest + _IF_CACHE)
  models/                 Pydantic data schemas
  tools/                  @tool wrappers + standalone signal functions
    skills_tools.py       General-purpose tools (query, import, iNAV, deep-dive) + SKILLS_TOOLS list
    runners.py            Thin shell-command runners (run_goldbees_pipeline, run_macro_scanner, …)
    market/gold.py        Gold/GARCH domain tools (explain_price_anomalies, run_risk_governor_analysis)
    _subprocess.py        Shared subprocess helpers (no project imports — breaks circular deps)
    chart_tools.py        plotext terminal charts (price, signal, GARCH vol, MACD, …)
    <domain>.py           Per-domain signal functions (quant_scorecard, inav_fetcher, comex_fetcher, …)
  ui/
    app.py                Streamlit 5-tab data hub
  utils/                  Caching, symbol mapping, report loading, and terminal markdown renderer (markdown_renderer.py)

scripts/                  Standalone runnable analysis scripts (including goldbees_report.py)
docs/                     This documentation
skills/                   Gemini / Claude skill definitions
data-engineering-importer/  Data pipeline reference (importer guide + schema)
tests/
docker-compose.yml
```

---

## Importers

All importers are **watermark-based delta-sync**: each fetcher reads `import_watermarks.(source, symbol).last_date`, fetches only new rows, and writes back the watermark after a successful insert. Safe for repeated runs — `ReplacingMergeTree` handles duplicate dates.

For `stocks` and `us_stocks` categories, the import manager redirects standard sequential download to a high-concurrency pipeline ([parallel_importer.py](file:///Users/dhiraj.thakur/project/ofin-agent/src/importer/parallel_importer.py)). This pipeline concurrently processes each stock symbol using a thread pool (capped at 5 workers) to fetch prices, quarterly earnings, insider transactions, and valuation snapshots. Requests are staggered using random jitter delays to bypass Yahoo Finance rate-limiting defenses (401 Unauthorized).

| Fetcher | External Source | ClickHouse Table(s) | Cadence |
|---|---|---|---|
| `yfinance_fetcher` | Yahoo Finance | `daily_prices` | Daily |
| `mfapi_fetcher` | MFAPI.in (AMFI) | `mf_nav` | Daily |
| `cot_fetcher` | CFTC Socrata API + ZIP archives | `cot_gold` | Weekly (Fri) |
| `nse_inav_fetcher` | NSE website | `inav_snapshots` | Every 15 min (market hours) |
| `fii_dii_fetcher` | Sensibull oxide API | `fii_dii_flows`, `fii_dii_monthly`, `fii_dii_fno_daily` | Daily |
| `imf_reserves_fetcher` | WGC Goldhub (primary) + World Bank WDI REST API (fallback) | `cb_gold_reserves` | Monthly |
| `etf_aum_fetcher` | Yahoo Finance | `etf_aum` | Daily |
| `mf_holdings_fetcher` | Morningstar (mstarpy) | `mf_holdings` | Monthly |
| `fx_rates_fetcher` | Yahoo Finance | `fx_rates` | Daily |
| `worldbank_macro_fetcher` | World Bank WDI API | `macro_indicators` | Annual |
| `imf_weo_fetcher` | IMF DataMapper API | `macro_indicators` | Semi-annual |
| `nse_quote_fetcher` | NSE Direct API | `daily_prices` | Intraday (Post-close) |
| `earnings_fetcher` | Yahoo Finance | `stock_earnings` | Quarterly |
| `insider_fetcher` | Yahoo Finance | `stock_insider_trades` | Daily |
| `valuation_fetcher` | Yahoo Finance | `stock_valuation` | Daily |
| `fund_imports/` (factory) | AMC Websites (ICICI, Nippon) | `mf_holdings` | Monthly |
| News tools | NewsAPI + Google News RSS | `news_articles` | Twice daily |
| `signal_aggregator` | Reads ClickHouse | `signal_composite` | Daily / on-demand |
| `trend_predictor` | Reads ClickHouse | `ml_predictions` | Daily after close |

### Symbol Registry (`src/importer/registry.py`)

| Category | Count | Examples |
|---|---|---|
| Stocks | 50 | RELIANCE, TCS, HDFCBANK, INFY, ICICIBANK |
| ETFs | 30+ | GOLDBEES, SILVERBEES, NIFTYBEES, BANKBEES, ITBEES, CPSEETF |
| Commodities | 7 | GOLD (GC=F), SILVER (SI=F), COPPER (HG=F), CRUDEOIL, NGAS, PLATINUM, PALLADIUM |
| Indices | 10 | NIFTY50, SENSEX, BANKNIFTY, SP500, NASDAQ, US10Y, DXY |
| FX Pairs | 5 | USDINR, USDCNY, USDAED, USDSAR, USDKWD |
| MF Schemes | 12 | GOLDBEES (140088), SILVERBEES (149758), NIFTYBEES (140084) |
| MF Holdings Watchlist | 4 | DSP Multi Asset, Quant Multi Asset, ICICI Multi Asset, Bajaj Multi Asset |

---

## ClickHouse Tables

Database: `market_data`. All tables use `ReplacingMergeTree` for idempotent re-imports.

| Table | Key Columns | Purpose |
|---|---|---|
| `daily_prices` | symbol, category, trade_date, OHLCV, imported_at | OHLCV time series — stocks, ETFs, commodities, indices, FX |
| `mf_nav` | symbol, scheme_code, nav_date, nav | MF / ETF daily NAV (AMFI via MFAPI.in) |
| `inav_snapshots` | symbol, snapshot_at, inav, market_price, premium_discount_pct | ETF iNAV vs market price (intraday) |
| `cot_gold` | report_date, mm_long, mm_short, mm_net, open_interest | CFTC COT Gold positioning (weekly) |
| `cb_gold_reserves` | ref_period, country_code, reserves_tonnes | Central bank gold holdings (quarterly) |
| `etf_aum` | trade_date, symbol, aum_usd, implied_tonnes | ETF assets under management (daily) |
| `fx_rates` | symbol, trade_date, OHLC | Currency pair daily OHLC |
| `ml_predictions` | as_of, horizon_days, expected_return_pct, confidence_low/high, regime_signal, cv_r2_mean | LightGBM 5-day return forecasts for GOLDBEES |
| `mf_holdings` | scheme_code, as_of_month, isin, security_name, market_value_cr, pct_of_nav | Mutual fund portfolio compositions (monthly) |
| `fii_dii_flows` | trade_date, fii_net_cr, dii_net_cr | Daily FII/DII cash-market net flows (₹ Crore) |
| `fii_dii_monthly` | month_date, fii_net_cr, dii_net_cr, nifty_close | Monthly FII/DII aggregate + Nifty context |
| `fii_dii_fno_daily` | trade_date, fii_fut_net_oi, fii_opt_overall_net_oi | Daily F&O participant OI (futures + options) |
| `signal_composite` | as_of, etf_symbol, macro_score, sentiment_score, valuation_score, flow_score, ml_score, composite_score, action | Multi-pillar ETF scores 0–100 with BUY/HOLD/SELL action |
| `news_articles` | fetched_at, title, source, sentiment, etfs_impacted, category, impact_tier | ETF-tagged news + macro events |
| `import_watermarks` | source, symbol, last_date | Delta-sync state tracking |
| `weight_checkpoints` | as_of, symbol, method, recommended_weight, regime | Position-sizing decisions (Kelly / Risk Governor) |
| `stock_earnings` | symbol, earnings_date, eps_actual, surprise_pct | US stock quarterly earnings |
| `stock_insider_trades` | symbol, insider_name, transaction_date, shares | US stock insider buying/selling |
| `stock_valuation` | symbol, snapshot_date, trailing_pe, forward_pe, roe | US stock valuation & fundamentals snapshot |
| `user_holdings` | tradingsymbol, isin, quantity, average_price, pnl | Zerodha Kite CNC holdings backup |
| `user_profile` | user_id, user_name, broker | Zerodha Kite user profile backup |
| `user_margins` | segment, cash, available_balance | Zerodha Kite account margins backup |
| `user_positions` | tradingsymbol, product, quantity, average_price | Zerodha Kite live positions backup |
| `user_orders` | order_id, status, tradingsymbol, quantity | Zerodha Kite order history backup |
| `macro_indicators` | ref_year, country_code, indicator_code, value, source | World Bank/IMF macro indicators (GDP, CPI, etc.) |

---

## Tools (`src/tools/`)

Most tools are standalone functions returning a dict/DataFrame — no DB writes, no side effects — callable independently or composed inside agents. They group into three kinds:

- **Domain signal functions** — the bulk: real computation per asset (quant scorecard, iNAV, COMEX, …).
- **Runners** (`runners.py`) — thin `@tool` wrappers over CLI scripts via subprocess; zero business logic.
- **Gold/GARCH domain** (`market/gold.py`) — `explain_price_anomalies`, `run_risk_governor_analysis`; real logic kept out of the junk-drawer.

`SKILLS_TOOLS` in `skills_tools.py` is the single canonical list; it re-exports from `runners.py` and `market/gold.py` so existing imports keep working. Shared subprocess helpers live in `_subprocess.py` to avoid circular deps.

| Tool | File | Signal Produced |
|---|---|---|
| **Explain Price Anomalies** | `market/gold.py` | GARCH-detected anomaly dates → regime + Final Z + news correlation + COMEX/COT context + forward ML/signal lookup |
| **Risk Governor Analysis** | `market/gold.py` | GARCH vol-targeted position weight for GOLDBEES (inverse-vol × regime × score gate) |
| **Quant Scorecard** | `quant_scorecard.py` | Gold + Silver 4-pillar 0–100 composite scores (Macro / Flows / Valuation / Momentum) |
| **Macro Event Scanner** | `macro_event_scanner.py` | 8 macro themes → per-ETF impact direction (+1 / -1) + conviction, sourced from live news |
| **iNAV Fetcher** | `inav_fetcher.py` | Live iNAV, market price, premium/discount % for any NSE ETF |
| **COMEX Fetcher** | `comex_fetcher.py` | Pre-market signals for XAU, XAG, XPT, XPD, HG vs prior close |
| **Who Is Selling** | `who_is_selling_agent.py` | FII / DII / Retail sell-off attribution and flow signals |
| **Premium Alerts** | `premium_alerts.py` | ETF iNAV premium/discount threshold breach alerts |
| **Domestic ETF Scanner** | `domestic_etf_scanner.py` | Z-score valuation + flow + momentum per ETF |
| **Market Context** | `market_context.py` | Live Nifty/BankNifty levels + market regime for LLM prompts |
| **News Search** | `news_search.py` | GNews RSS articles + keyword sentiment for any symbol |
| **NewsAPI Search** | `newsapi_search.py` | NewsAPI.org articles from premium Indian financial publications |
| **Earnings Scraper** | `earnings_scraper.py` | Quarterly results from Screener.in / Yahoo Finance |
| **Historic iNAV** | `historic_inav.py` | Historical iNAV snapshots for ETFs |
| **Valuation Alerts** | `valuation_alerts.py` | P/E, yield, P/B ratio threshold crossings |
| **Summarization** | `summarization.py` | LLM-generated risk and sentiment summaries per holding |
| **Chart Tools** | `chart_tools.py` | plotext terminal charts — price (with 🔴 anomaly markers), signal scores, GARCH vol, MACD |
| **Zerodha MCP Tools** | `zerodha_mcp_tools.py` | Holdings, positions, orders via Zerodha Kite MCP |

### Quant Scorecard Pillars (`quant_scorecard.py`)

**Gold (GOLDBEES):**
| Pillar | Weight | Signal Sources | Scoring |
|---|---|---|---|
| Macro | 30% | DXY (yfinance) + US10Y yield | DXY ≤ 100 → 100; ≥ 110 → 0. Real yield 5D delta ≤ −0.10 → 100 |
| Flows | 30% | COT gold (ClickHouse `cot_gold`) | mm_net/OI ≤ 20% → 100; ≥ 35% → 0 |
| Valuation | 20% | iNAV snapshot (`inav_snapshots`) | Discount > 0.5% → 100; Premium > 0.5% → 0 |
| Momentum | 20% | LightGBM (`ml_predictions`) | Return ≥ +1% → 100; ≤ −1% → 0 |

**Silver (SILVERBEES) — additional signals:**
| Pillar | Weight | Signal Sources | Scoring |
|---|---|---|---|
| Macro | 30% | DXY + US10Y + Gold-Silver Ratio (yfinance) | GSR ≥ 90 → 100 (silver cheap); ≤ 55 → 0 |
| Flows | 30% | CFTC live TXT (`SILVER - COMMODITY`, code 084) | mm_net/OI ≤ 20% → 100; ≥ 35% → 0 |
| Valuation | 20% | SILVERBEES iNAV (`inav_snapshots`) | Same as gold |
| Momentum | 20% | SI=F 5-day realised return (yfinance fallback) | Return ≥ +2% → 100; ≤ −2% → 0 |

---

## ML (`src/ml/`)

### TrendPredictor (`trend_predictor.py`)

- **Target:** 5-day forward log return for GOLDBEES
- **Algorithm:** LightGBM with `TimeSeriesSplit` walk-forward cross-validation
- **25+ alpha features:**
  - Momentum: logret1, logret5, logret20, EMA crosses
  - Mean-reversion: price/MA ratio
  - Volatility: ATR, historical vol
  - Macro: DXY, USD/INR, US 10Y yield
  - Market microstructure: COT leverage ratio, iNAV spread
  - Flows: FII/DII 5-day rolling net
  - Seasonality: month sin/cos, day-of-week encoding
- **Output written to:** `ml_predictions` — expected_return_pct, confidence bounds, cv_r2_mean, regime_signal

### AnomalyDetector (`anomaly.py`)

Three-stage composite pipeline — public API: `run_composite_anomaly(df, df_cot=None, df_fx=None)`:

1. **Robust Z-score** — MAD-based, resistant to fat tails in gold returns
2. **GARCH(1,1) standardised residuals** — isolates true price shocks from routine volatility clustering
3. **Isolation Forest** — cross-asset feature confirmation (USDINR, COT crowding)

Requires full OHLCV (`open/high/low/close/volume`) and ≥60 rows. Returns `(df_result, df_flagged, garch_loglik)` where `df_result` carries per-date `regime`, `final_z`, `garch_vol`, `cot_pct_oi`, `usdinr_logret`.

Regime labels: `⚡ Flash Crash / Black Swan`, `🔥 Volatile Breakout`, `⚠️ Crowded Long (Squeeze Risk)`, `🧨 Blow-off Top (Weak)`, `📈 Strong Trend (HODL)`, `✅ Normal`.

Fire rate ~5% (vs. Random Forest's spurious 21% prior to GARCH replacement).

### explain_price_anomalies (`src/tools/skills_tools.py`)

The **anomaly explanation tool** bridges the ML pipeline with news/event correlation:

1. Fetches full OHLCV history (ClickHouse → yfinance fallback)
2. Loads COT (`cot_gold` — gold only) + USDINR FX as cross-asset features
3. Runs `run_composite_anomaly` on full history; filters flagged dates to the `days` window
4. Per anomaly date: surfaces GARCH **regime** + **Final Z** in the summary table and detail section
5. Queries `search_financial_news` per date; flags neutral-news + large-move divergence (policy surprise signal)
6. Calls `ml_prediction_asof` + `signal_composite_asof` (repo) to show what the ML model and composite signal said *on* that date — enabling confirmed vs. contradicted signal analysis
7. Appends COMEX futures price chart (GC=F / SI=F for gold/silver) and GARCH vol chart

Graceful fallback: if <60 rows or `arch` missing, falls back to naive `max(2.0, 2.5×std)` threshold — report always renders, regime columns show `—`.

Cross-asset loading pattern (copied from `src/ui/app.py`):
```python
# COT (gold only): SELECT report_date, mm_net, open_interest FROM market_data.cot_gold
# FX (always):     SELECT symbol, trade_date, toFloat64(close) AS close
#                  FROM market_data.fx_rates FINAL WHERE symbol = 'USDINR'
```

---

## Agents (`src/agents/`)

Agents orchestrate multiple tools into complete workflows using LangGraph / LangChain.

### MosaicFundAgent (`mosaic_fund_agent.py`)
Full Zerodha portfolio intelligence workflow.

```
Auth (Kite MCP) → Fetch Holdings
    ↓
Parallel enrichment per holding:
  Yahoo Finance (prices, metrics)
  + News (NewsAPI + GNews)
  + Earnings (Screener.in / Yahoo)
    ↓
Per-asset LLM scoring
    ↓
Portfolio aggregation + LLM summary
    ↓
JSON report
```

Entry: `run_full_analysis()` | Ad-hoc: `ask(question)` via ReAct loop

### ComexAgent (`comex_agent.py`)
Pre-market commodity signals for XAU, XAG, XPT, XPD, HG.

- **Local LLM path:** direct call to `get_comex_signals()` (avoids tool loop)
- **Cloud LLM path:** LangGraph with loop guard (max 2 tool calls)
- Signal thresholds: ±0.3% = neutral, ±1% = strong

### NewsSentimentAgent (`news_sentiment_agent.py`)
Multi-source news sentiment for any stock or ETF.

- Sources: NewsAPI.org + Google News RSS (gnews)
- Single-call design via `collate_news_sentiment()` to prevent tool loops
- Output: overall_sentiment (POSITIVE / NEUTRAL / NEGATIVE), per-article scores, deduplicated

### SignalAggregator (`signal_aggregator.py`)
Composite ETF signal — 6 pillars → 0–100 score → BUY / ACCUMULATE / HOLD / TRIM / AVOID

Uses the **Strategy pattern**: each pillar is a `SignalSource` subclass in `signal_sources.py`. The aggregator loops over `score_sources` and an `AnomalySource`; all run in parallel via `ThreadPoolExecutor`. Signal aggregator wall-clock: ~9 s (was 79 s before parallelisation).

| Pillar | Class | Weight | Source |
|---|---|---|---|
| Macro | `MacroSignalSource` | 25% | `macro_event_scanner` → net signal across 8 themes |
| Sentiment | `SentimentSignalSource` | 15% | `news_articles` table — pos/neg ratio last 7 days |
| Valuation | `ValuationSignalSource` | 15% | `domestic_etf_scanner` — iNAV Z-score premium/discount |
| Flow | `FlowSignalSource` | 25% | `fii_dii_flows` — 5D net; equity ETFs benefit, safe-haven inverse |
| ML | `MLSignalSource` | 15% | `ml_predictions` — LightGBM expected return (GOLDBEES only; others neutral) |
| Anomaly | `GARCHAnomalySource` | 5% | `anomaly.py` — Flash Crash boost / Blow-off dampener |

Adding a 7th pillar: subclass `SignalSource`, implement `collect(repo)`, append to `score_sources` list in `run_signal_aggregation()`.

Covers 18 core ETFs. Output optionally written to `signal_composite` table via `--save`.

> **Planned 7th pillar — DSP Smart Money (pending):** MoM delta of DSP Multi Asset gold/equity allocation (`mf_holdings` table) as a contrarian tactical signal. Source: `scripts/dsp_quant_strategy_analyzer.py`. GSR correlation R=0.68 identified as primary driver of DSP allocation shifts.

---

## Scripts (`scripts/`)

Standalone scripts that run analyses against the live database and print Rich console output.

| Script | Purpose |
|---|---|
| `metals_quant_scorecard.py` | Run Gold + Silver quant scorecards side-by-side |
| `opportunity_scan.py` | Cross-asset DB scan — momentum, drawdown, RSI, iNAV, flows → ranked opportunity table |
| `gold_quant_scorecard.py` | Gold-only 4-pillar scorecard (GOLDBEES) |
| `fii_pattern_check.py` | FII historical buying/selling pattern analysis |
| `import_dsp_history.py` | One-time ETL backfill: 31-month DSP Multi Asset holdings (Sep 2023–Mar 2026) from DSP website ZIPs into `mf_holdings`; writes watermark on completion |
| `dsp_quant_strategy_analyzer.py` | Reverse-engineer DSP's trading rules by correlating monthly allocation deltas against Mosaic quant signals (DXY, COT, iNAV, GSR, ML). Identifies GSR as primary tactical lever (R=0.68). |
| `whale_tracker.py` | Large FII/DII flow detection and alerts |

---

## CLI Commands (`src/main.py`)

| Command | Purpose | Writes To |
|---|---|---|
| `analyze` | Full portfolio analysis (Zerodha → enrich → score → report) | JSON |
| `import` | Sync market data to ClickHouse (delta or full) | ClickHouse |
| `signals` | Run SignalAggregator for 18 ETFs | Console (+ DB with `--save`) |
| `macro` | Run macro event scanner (8 themes) | Console (+ DB with `--save`) |
| `etf-news` | ETF-specific news scanner | Console (+ DB with `--save`) |
| `comex` | Pre-market commodity signals | Console |
| `who-is-selling` | FII/DII/Retail flow analysis | Console |
| `premium-alerts` | iNAV premium/discount threshold alerts | Console |
| `news SYMBOL` | Multi-source sentiment for a symbol | Console |
| `ask "question"` | Free-form ReAct agent with tool access | Console |
| `config` | Show current settings (API keys masked) | Console |

---

## Configuration (`config/settings.py`)

All settings are loaded from `.env`. See [docs/configuration.md](configuration.md) for full reference.

| Group | Key Settings |
|---|---|
| LLM | `llm_provider` (openai/anthropic), `llm_model`, `llm_base_url` (local), `llm_context_window` |
| API Keys | `openai_api_key`, `anthropic_api_key`, `newsapi_key`, `gold_api_key` |
| Zerodha | `kite_mcp_url`, `kite_api_key`, `kite_api_secret` |
| ClickHouse | `clickhouse_host`, `clickhouse_port`, `clickhouse_database`, `clickhouse_user/password` |
| Caching | `comex_cache_ttl_seconds` (3600), `newsapi_cache_ttl_seconds` (3600) |
| Market | `nse_suffix` (.NS), `market_timezone` (Asia/Kolkata), `market_open` (09:15), `market_close` (15:30) |
| App | `output_dir`, `log_level`, `news_articles_per_stock`, `news_lookback_days` |

---

## Design Patterns

### 1. Watermark Delta Sync
Every fetcher checks `import_watermarks.(source, symbol).last_date` before fetching. Only rows after `last_date - overlap_days` are fetched and inserted. `ReplacingMergeTree` deduplicates on re-import. Use `--full` to bypass watermarks.

### 2. Repository Pattern (`src/db/repository.py`)
`MarketDataRepository` is the single access point for all ClickHouse reads. Centralises `FINAL` usage, typed return shapes, and the `run_fetcher()` orchestration loop. All signal sources and ML code read through the repo — never raw SQL strings scattered across files.

Key read methods:

| Method | Returns | Table |
|---|---|---|
| `fii_dii_5d()` | `(fii_net, dii_net)` floats | `fii_dii_flows` |
| `ohlcv(symbol, category)` | Full OHLCV DataFrame | `daily_prices` |
| `latest_close(symbols, category)` | `{symbol: close}` | `daily_prices` |
| `latest_ml_prediction()` | Latest ML row dict | `ml_predictions` |
| `ml_prediction_asof(as_of)` | ML row dict ≤ date, or None | `ml_predictions` |
| `latest_signal_composite(symbols)` | `{symbol: {score, action, flag}}` | `signal_composite` |
| `signal_composite_asof(symbol, as_of)` | Signal row dict ≤ date, or None | `signal_composite` |
| `inav_latest_and_history(symbols)` | Latest iNAV + 30d history | `inav_snapshots` |
| `run_fetcher(fetcher)` | `FetchResult` | Orchestrates watermark + insert + events |

The `*_asof(date)` variants enable **point-in-time queries** — used by `explain_price_anomalies` to surface what the ML model and composite signal said on each anomaly date, without contaminating the report with future information.

### 3. Agent Architecture (Multi-Agent Orchestrator)

> **Full reference:** [docs/agent-architecture.md](agent-architecture.md) — intent routing, sub-agent catalogue, middleware (tracer + budget), mandatory rules, request flows, observability queries.

The platform employs a **Multi-Agent Orchestrator** pattern. A main `MosaicFundAgent` uses an **LLM-based Intent Router** (`src/agents/intent_router.py`, with regex fallback in `src/agents/sub_agents.py`) to delegate user queries to 10 specialised sub-agents. Every sub-agent invocation auto-attaches **TracingCallbackHandler** (→ `agent_traces` table) and **BudgetCallbackHandler** (20 calls / 30k tokens / 180s wall-clock). All agent prompts include the **NO_LLM_CALC_RULE** — the LLM may never compute any number; all numeric work must come from tool output.

| Sub-Agent | Purpose | Tools |
|---|---|---|
| **DeepDiveSubAgent** | US stock SEC filings (10-K/10-Q), XBRL, peer valuation | ~6 |
| **IndianEquityResearchSubAgent** | NSE/BSE company research (price, earnings, cashflow, holdings, news) | ~15 |
| **SignalSubAgent** | ETF composite scores, ML prediction, Kelly weights, GARCH vol | ~14 |
| **MacroSubAgent** | COMEX pre-market, FII/DII flows, macro themes | ~10 |
| **NewsSubAgent** | News headlines and sentiment per stock/ETF | ~5 |
| **CodeSubAgent** | Ad-hoc Python execution and ClickHouse queries | ~5 |
| **DatabaseSubAgent** | ClickHouse schema, watermarks, data freshness | ~5 |
| **IntlETFSubAgent** | International ETFs (MAFANG, Hang Seng, Nasdaq 100) | ~8 |
| **ResearchSubAgent** | Multi-domain cross-asset research | ~30 |

### 4. Local LLM & Gemma Integration
Mosaic is optimized for both cloud (OpenAI/Anthropic) and **Local LLM** execution via **Ollama**. It specifically supports **Gemma 4** (customized as `mosaic-gemma4`) with several architectural adaptations:

*   **Compact Prompting:** For local models with limited context windows (e.g., 4k–8k tokens), the agent switches to high-density, compact system prompts to preserve space for market data.
*   **Direct-to-LLM Fallback:** When a model's context is too low to support a full ReAct tool-calling loop, the system bypasses the agent and passes raw data tables directly to the model for one-shot summarization.
*   **Structured Table Injection:** Instead of relying on the LLM to "query" the database via tools, the orchestrator pre-fetches relevant tables and injects them into the prompt, allowing models like Gemma to focus on analysis rather than orchestration.
*   **Tool-Calling-Free Path:** For smaller models that struggle with complex JSON-RPC tool schemas, the orchestrator provides a flattened data path that reduces "hallucination risk" in function selection.

### 5. Terminal Charting & Visual Intelligence
The platform features a native **Terminal Charting** engine (in `src/tools/chart_tools.py`) that enables the agent to provide visual context directly in the CLI.

*   **Plotext Integration:** Uses `plotext` to render high-resolution ASCII/Unicode charts (line, bar, grouped bar) within the terminal. These are rendered inside Rich panels with preserved ANSI color codes.
*   **Dynamic Scaling:** Charts are automatically sized based on the user's terminal width and height, ensuring a clean responsive layout in any console.
*   **Specialized Quant Visuals:** Includes tools for plotting:
    *   **Price & NAV Trends:** Historical price action with normalized indexing for multi-symbol comparison.
    *   **Pillar Breakdowns:** Grouped bar charts showing the contribution of individual pillars (Macro, Flows, etc.) to an ETF's composite score.
    *   **Whale Tracking:** Bar charts of FII/DII daily net flows and fund portfolio weights.
*   **Unicode Sparklines:** Uses compact Unicode sparklines (`▁▂▃▄▅▆▇█`) to embed 10–20 days of trend history directly into text summaries and table headers, providing instant visual momentum context without full-sized charts.

### 6. Strategy Pattern (`src/agents/signal_sources.py`)
`SignalSource` ABC defines `collect(repo) -> dict[etf, float]`. Each signal pillar is a subclass. The aggregator holds a `score_sources: list[SignalSource]` — adding a new pillar = one class + one list append. All sources run in parallel via `ThreadPoolExecutor`.

### 7. Adapter Pattern (`src/importer/base_fetcher.py`)
`Fetcher` ABC defines `fetch() / validate() / insert() / max_date()`. Each external data source is a concrete subclass registered in `FETCHER_REGISTRY`. `repo.run_fetcher(fetcher)` handles watermarks, dry-run, and event publishing — the fetcher only knows its data.

### 8. Observer Pattern (`src/events/`)
`EventBus` fires `DataImportedEvent` after every live `run_fetcher()` insert. Observers subscribe once; the import pipeline has zero knowledge of ML retraining or signal refresh. Built-in observers: `ModelCacheInvalidator` (sync), `MLPredictionObserver`, `SignalAggregatorObserver`, `SanityCheckObserver` (all async, daemon threads).

### 9. Graceful Pillar Degradation
Every signal pillar degrades to neutral 50 (not 0) when its data source is unavailable. The composite score remains valid across all 18 ETFs — missing data does not penalise the composite.

### 10. LLM-Required Scoring
All agent scoring paths require a configured LLM. LLM provider (OpenAI / Anthropic / local via OpenAI-compatible endpoint) is selected at runtime via `llm_provider` setting. Set `LLM_BASE_URL` for local inference with Ollama or LM Studio.

### 11. Tool Loop Protection
- ComexAgent uses a direct function call for local LLMs (avoids ReAct loop overhead)
- NewsSentimentAgent uses a single `collate_news_sentiment()` call (not a tool loop)
- LangGraph agents have explicit loop guards (`max_iterations=2`)

### 12. iNAV Arbitrage Detection
NSE iNAV snapshots are captured every 15 minutes during market hours. `premium_discount_pct > +0.5%` triggers a premium alert; `< −0.25%` flags a discount opportunity. The SILVERBEES / GOLDBEES premium spread is a direct input to the quant scorecard valuation pillar.
NSE iNAV snapshots are captured every 15 minutes during market hours. `premium_discount_pct > +0.5%` triggers a premium alert; `< −0.25%` flags a discount opportunity. The SILVERBEES / GOLDBEES premium spread is a direct input to the quant scorecard valuation pillar.

---

## External Data Sources

| Source | Auth Required | Quota | Used For |
|---|---|---|---|
| Yahoo Finance (yfinance) | No | Soft rate limits | OHLCV, ETF AUM, FX, indices, GSR, silver momentum |
| Google News RSS (gnews) | No | None | Macro themes, ETF news |
| CFTC Socrata API | No | None | COT gold + silver positioning |
| CFTC direct TXT/ZIP | No | None | Silver COT (live `f_disagg.txt`) |
| NSE website | No | Soft | Live iNAV snapshots |
| Sensibull oxide API | No | None | FII/DII daily + monthly + F&O OI |
| WGC Goldhub API | No | None | Central bank gold reserves (annual, primary) |
| World Bank WDI REST API | No | None | Central bank gold reserves (historic gap-fill) |
| MFAPI.in | No | None | MF / ETF NAV history |
| Morningstar (mstarpy) | No | None | MF portfolio holdings |
| NewsAPI.org | `newsapi_key` | 100 req/day (free) | Premium Indian financial news |
| gold-api.com | `gold_api_key` | Strict daily quota | COMEX live spot (with 1h cache) |
| Zerodha Kite MCP | Optional (hosted endpoint) | None | Live portfolio holdings + positions |
| OpenAI / Anthropic | `openai_api_key` / `anthropic_api_key` | Pay-per-token | LLM scoring, summaries, ReAct agent |
