<div align="center">

# 🪙 Mosaic

### Your quantitative co-pilot for Indian & global markets

**ML-driven alpha · institutional flow tracking · volatility-aware position sizing — all on a private, local-first data lake.**

![Python](https://img.shields.io/badge/python-3.11+-blue) ![ClickHouse](https://img.shields.io/badge/storage-ClickHouse-yellow) ![LLM](https://img.shields.io/badge/LLM-OpenAI%20%7C%20Anthropic%20%7C%20Ollama-green) ![License](https://img.shields.io/badge/license-Apache%202.0-lightgrey)

</div>

---

## Why Mosaic?

Serious market decisions need three things that rarely live in one place: **clean cross-asset data**, **models that quantify edge and risk**, and **context to explain *why* something moved**. Retail tools give you charts. Terminals give you data feeds. Neither tells you *"GOLDBEES jumped 5.7% on a GARCH 'Volatile Breakout' regime, the news was a neutral import-duty change, and the ML model had already flagged WATCH_LONG the day before."*

Mosaic closes that gap. It continuously syncs **13+ data sources** into a **local ClickHouse data lake** (your data never leaves your machine), then layers on:

- **Signals** — a 6-pillar composite score (0–100) across macro, flows, valuation, sentiment, ML, and volatility
- **Risk** — GARCH(1,1) volatility regimes feeding inverse-vol + Kelly position sizing
- **Explanation** — anomaly detection that correlates price shocks with news, COMEX futures, COT positioning, and what the models predicted next
- **Conversation** — ask plain-English questions; a guild of specialised LLM agents route, research, and answer

Whether you're a **retail investor** wanting a visual dashboard, a **trader** watching pre-market commodities, or a **quant** backtesting volatility regimes — Mosaic turns scattered market data into clear, risk-adjusted decisions you can interrogate.

> **Not financial advice.** This is a personal research tool. Always verify before acting on any output. Licensed under the [Apache License 2.0](LICENSE).

---

## How It Works

```
                          ┌─────────────────────────────────────────────┐
   13+ DATA SOURCES       │            LOCAL CLICKHOUSE LAKE             │
   ─────────────────      │   daily_prices · mf_holdings · fii_dii      │
   Yahoo · NSE · CFTC     │   cot_gold · fx_rates · ml_predictions      │
   MFAPI · Morningstar ──▶│   signal_composite · inav_snapshots         │──┐
   Zerodha Kite · NewsAPI │   ReplacingMergeTree · watermark delta-sync │  │
   World Bank · IMF       └─────────────────────────────────────────────┘  │
                                                                            │
        ┌───────────────────────────────────────────────────────────────────┘
        │
        ▼
   ┌──────────────┐   ┌──────────────┐   ┌──────────────┐   ┌──────────────┐
   │  SIGNALS     │   │  ML FORECAST │   │  VOLATILITY  │   │  ANOMALY     │
   │  6 pillars   │   │  LightGBM 5d │   │  GARCH(1,1)  │   │  GARCH + IF  │
   │  → 0–100     │   │  + quantile  │   │  + Kelly     │   │  + news +    │
   │  composite   │   │  confidence  │   │  sizing      │   │  COMEX/COT   │
   └──────┬───────┘   └──────┬───────┘   └──────┬───────┘   └──────┬───────┘
          └──────────────────┴─────────┬────────┴──────────────────┘
                                       ▼
              ┌─────────────────────────────────────────────┐
              │   MULTI-AGENT ORCHESTRATOR (LangGraph ReAct)  │
              │   Intent Router → specialised sub-agent guild │
              └─────────────────────────────────────────────┘
                                       ▼
        CLI  ·  Streamlit Dashboard  ·  Conversational "ask"  ·  MCP tools
```

**The flow:** data lands in ClickHouse via watermark-based delta sync → quant pipelines (signals, ML, volatility, anomaly) read through a single typed repository → an LLM intent router dispatches your question to the right specialist agent → you get a narrated, numerically-grounded answer in your terminal, the web hub, or via Claude/Gemini MCP tools.

> **Numbers are never hallucinated.** Every figure an agent reports is computed in Python or SQL first; the LLM only narrates pre-computed results.

---

## See It In Action

Ask why a holding moved — and get a forensic answer, not a guess:

```bash
python src/main.py ask "explain GOLDBEES anomalies over the last 30 days"
```

```
🔍 Price Anomaly & News Correlation Report: GOLDBEES
Detected 1 anomaly date in the last 30 days (GARCH composite, Final Z > 2.5):

│ Date       │ Return  │ 20d Vol │ Volume (Spike) │ Regime              │ Final Z │
│ 2026-05-13 │ +5.72%  │ 1.94%   │ 10.39 Cr (3.6x)│ 🔥 Volatile Breakout │ +3.81   │

📰 News on 2026-05-13:  "Government doubles import duty on gold & silver…"  (NEUTRAL)
   ⚠️ Divergence: neutral sentiment on a high-magnitude move → policy surprise

📡 What the models said on this date:
   ML (5d): WATCH_LONG | prob_up=64% | expected_return=+1.8% → continuation
   Composite: BUY (score 71) → ✅ signal confirmed the shock

📈 Correlated COMEX futures (GC=F) + GARCH volatility charts appended…
```

One command pulls the price shock, classifies its **GARCH regime**, finds the **news** that caused it, flags the **neutral-news-but-big-move divergence**, and tells you **what the ML model and composite signal predicted the day before** — all grounded in your local data.

---

## 🎯 Key Capabilities & Value Pillars

### 1. Tactical Asset Allocation & Decision Intelligence
*   **6-Pillar Composite Scoring:** Automatically scores 18+ ETFs from `0 to 100` by analyzing Macro Trends, Capital Flows, Valuation, News Sentiment, ML Forecasts, and Volatility Regimes.
*   **GOLDBEES Pipeline:** Integrates 5-day walk-forward predictive returns with dynamic target allocation sizing.

### 2. Multi-Market Equity Research Hub
*   **🇮🇳 Indian Equity Research:** Automates deep-dives for any NSE/BSE stock—extracting 3-year cash flows, quarterly results, promoter/FII shareholding patterns with QoQ deltas, mutual fund cross-ownership (e.g., DSP AMC conviction), news sentiment, and automatic price charts.
*   **🇺🇸 US Equity Research:** Performs SEC filing research—parsing SEC 10-K/10-Q filing text, XBRL financials, executive compensation, corporate recruitment metrics (Workday job scrapers), and peer multiples.
*   **🌍 International ETF "Scarcity Premium" Tracker:** Tracks international ETFs (like Nasdaq 100, S&P 500, or Hang Seng ETFs) and flags SEBI-mandated AMC investment limit inflows and premium distortions.

### 3. Volatility Management & Capital Preservation
*   **Smart Sizing (Risk Governor):** Dynamically scales position weights using continuous inverse-volatility scaling blended with the Kelly Criterion, protecting capital during high-stress regimes.
*   **Market Anomaly Detection:** Utilizes standard GARCH(1,1) residuals and Isolation Forests to classify true market volatility regimes (e.g., "Flash Crash", "Blow-off Top").
*   **Risk Governor Parameter Optimizer:** Features a grid-search parameter sweep script to tune volatility targets, trend filter windows/multipliers, and drawdown brakes to maximize historical Sharpe ratios.

### 4. Institutional Flow & "Whale" Tracking
*   **AMC Smart Money Tracker:** Scrapes and analyzes monthly portfolio disclosures across major Indian AMCs (DSP, Nippon India, ICICI Prudential) to reverse-engineer professional fund conviction.
*   **FII/DII & COT Flows:** Monitors daily Cash + F&O participant flows alongside global metal speculator commitments (CFTC COT).

### 5. Real-Time Price & Fair Value Telemetry
*   **iNAV Fair-Value Watch:** Compares live exchange prices against the true underlying Net Asset Value (iNAV) of ETFs to alert on premium/discount markups.
*   **COMEX Pre-Market Intel:** Analyzes global metal markets (Gold, Silver, Copper, Platinum, Palladium) to prepare commodity strategies before Indian markets open.

### 6. Tax, Currency, & Portfolio Health
*   **Tax-Aware Portfolios:** Incorporates latest Indian tax structures (e.g., Budget 2024 FoF rules) to optimize post-tax returns.
*   **INR Currency Hedging:** Suggests currency-hedging strategies by analyzing how USDINR affects real-asset valuations.
*   **Broker Sync & Backups:** Backs up Zerodha Kite holdings, margins, and historical order books to a local ClickHouse database.

---

## 🤖 The Agent System: A Specialized Analyst Guild

Rather than passing raw user prompts to a single LLM with dozens of tools, Mosaic employs a **deterministic Intent Router & Multi-Agent Orchestrator** to route queries to dedicated sub-agents wrapping a LangChain/LangGraph ReAct loop with a domain-specific system prompt and a curated tool subset:

*   **💼 Portfolio Orchestrator (`MosaicFundAgent`):** Accesses Zerodha Kite accounts to inspect holdings, calculate portfolio-level sector exposure, enrich positions with news/earnings, and compile risk summaries.
*   **🇮🇳 Indian Equity Analyst (`IndianEquityResearchSubAgent`):** Orchestrates multi-tool parallel lookups for NSE/BSE stocks (Yahoo multiples, momentum, cash flows, promoter trends, and mutual fund conviction).
*   **🇺🇸 US Equity Analyst (`DeepDiveSubAgent`):** Targeted SEC filing analyst querying SEC EDGAR for 10-K/10-Q filings, XBRL financials, executive pay, and Workday hiring trends.
*   **📈 Volatility & Quant Agent (`SignalSubAgent`):** Manages GOLDBEES forecasts, walk-forward ML evaluation metrics, and computes risk-governed weights.
*   **🌍 International ETF Arbitrage Agent (`IntlETFSubAgent`):** Specializes in tracking overseas ETFs and analyzing SEBI inflow constraints to discover scarcity premiums.
*   **🌐 Macro & Flow Agent (`MacroSubAgent`):** Maps macro news themes (rates, inflation, trade wars) to ETF directions, details pre-market COMEX indicators, and breaks down FII/DII flow dynamics.
*   **💻 Code & DB Developer (`CodeSubAgent`):** Writes and runs local Python scripts or queries raw ClickHouse SQL to answer ad-hoc database questions.

*Mosaic is optimized to run locally via **Ollama** (specifically customized **Gemma 4** models) using high-density compact prompts and structured table injection to avoid context drift and token exhaustion.*

---

## 📐 Quantitative & Infrastructure Spec (For Quant Engineers)

### 1. Database & Pipeline Engineering (ClickHouse Data Lake)
*   **Deduplicated Storage:** Core tables (`daily_prices`, `mf_holdings`, `fii_dii_flows`) leverage ClickHouse's `ReplacingMergeTree` engines for idempotent inserts.
*   **Watermark Delta-Sync:** Ingestion pipelines utilize watermark-based delta syncing (tracking `max(trade_date)`) to protect API limits.
*   **Cross-Asset Freshness Guard:** Enforces a system-wide freshness guard verifying synchronization across all 105+ symbols (Indices, Gold/Silver, FX Rates, US Stocks) before allowing execution of cross-asset mathematical models.

### 2. Machine Learning Predictive Pipeline
*   **Model Architecture:** 5-day walk-forward LightGBM regression/classification models.
*   **Feature Set:** Generates 25+ features including multi-timeframe price momentum, mean reversion oscillators, USDINR/DXY currency spreads, CFTC COT positioning, and seasonality.
*   **Uncertainty Quantification:** Leverages quantile regression to construct 80% confidence intervals (`confidence_band`) around expected log returns.
*   **Model Diagnostics:** Evaluates performance via walk-forward Cross-Validation (CV) metrics (AUC and hit ratio).

### 3. Volatility & Anomaly Pipelines
*   **GARCH Volatility Scaling:** Estimates conditional volatility ($\sigma_t$) using a **GARCH(1,1)** model with Student-t innovations to capture fat-tailed asset return distributions.
*   **Regime Classification:** Standardized GARCH residuals are passed alongside macro indicators to a cross-asset **Isolation Forest** model to detect volatility anomaly regimes.
*   **Risk Governor Sizing:** Combines continuous inverse-volatility scaling ($w_t = \frac{\sigma_{\text{target}}}{\sigma_t}$) with the **Kelly Criterion** to determine blended portfolio weights.

---

## 💻 Choose Your Experience

### 🟢 Non-Technical (The Web Hub & Agent)
*   **Visual Dashboard:** Run the Streamlit Web UI ([src/ui/app.py](src/ui/app.py)) with one click to view charts, run data syncs, and check signal summaries visually.
*   **Conversational Assistant:** Ask questions in plain English (e.g., *"Am I overexposed to IT?"*, *"Which gold ETFs have the lowest premium?"*, *"Analyze Roku's R&D spend trend"*) and get immediate, written answers.

### 👑 Intermediate (The Command Line & Reports)
*   **Pre-Built Reports:** Generate terminal summaries with high-resolution text charts and unicode trend sparklines.
*   **Targeted CLI Commands:** Run direct commands like `python src/main.py comex` or `python src/main.py signals`.

---

## Quick Start

### One-Click Setup (Non-Technical)

If you are not a developer or want to avoid setting up Python and installing libraries manually, you can run the entire stack (ClickHouse database, Streamlit Web Dashboard, and agents) with **Docker Desktop**:

1. Install **[Docker Desktop](https://www.docker.com/products/docker-desktop/)** and make sure it is running.
2. Double-click **[run.sh](run.sh)** (macOS/Linux) or **[run.bat](run.bat)** (Windows).
   - *First-run only:* This creates a `.env` file in the project folder. Open it in a text editor to add your API keys (like OpenAI, Zerodha login, etc.).
3. Once configured, run the script again. It will automatically build the image, start the containers, and open the Web Dashboard at **http://localhost:8501** in your browser.
4. To run individual CLI commands or scripts without installing Python locally, use the **[mosaic.sh](mosaic.sh)** (macOS/Linux) or **[mosaic.bat](mosaic.bat)** (Windows) wrappers:
   - For standard CLI commands:
     ```bash
     ./mosaic.sh comex
     ./mosaic.sh ask "am I overexposed to IT?"
     ./mosaic.sh analyze --max 3
     ```
   - For specific Python scripts:
     ```bash
     ./mosaic.sh src/scripts/goldbees_report.py
     ```
5. To stop the application, double-click **[stop.sh](stop.sh)** (macOS/Linux) or **[stop.bat](stop.bat)** (Windows).

See the detailed **[Docker Setup Guide](docs/docker_install_guide.md)** for platform-specific installation steps.

### Developer Install (Manual)


```bash
git clone https://github.com/Mosaic-agent/Mosaic-fund-agent.git
cd Mosaic-fund-agent
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

**macOS — OpenMP for LightGBM:**
```bash
brew install libomp
```

### Configure

```bash
cp .env.example .env   # fill in your keys
```

Minimum keys to get started:
```
OPENAI_API_KEY=sk-...
NEWSAPI_KEY=...
GOLD_API_KEY=...
```

See [docs/configuration.md](docs/configuration.md) for all options including local LLM setup.

### Start ClickHouse

```bash
docker compose up clickhouse -d
python src/main.py config   # verify connection
```

---

## Usage

### Portfolio analysis

```bash
python src/main.py analyze               # live portfolio
python src/main.py analyze --max 3       # test with 3 holdings
```
### Ask a question

```bash
python src/main.py ask "which holdings have the worst news sentiment?"
python src/main.py ask "am I overexposed to IT sector?"
python src/main.py ask "which ETFs are trading at a premium?"
```

### Full Command Reference

| Command | Description |
|---|---|
| `analyze` | Full Zerodha portfolio analysis + JSON/HTML reports |
| `ask "..."` | Free-form ReAct agent Q&A (Auto-routes to 5 sub-agents) |
| `signals` | Composite ETF signal aggregator (0–100 scores) |
| `macro` | Geopolitical & Macro theme scanner |
| `etf-news` | Category-tagged ETF news sentiment |
| `risk` | GARCH-based position sizing & Risk Governor blend |
| `premium-alerts` | Live iNAV premium/discount threshold alerts |
| `comex` | COMEX pre-market signals (XAU, XAG, XPT, XPD, HG) |
| `who-is-selling` | FII/DII/Retail flow attribution |
| `import` | Sync market data to ClickHouse (delta or full) |
| `news SYMBOL` | Multi-source sentiment for a specific ticker |
| `config` | Show current settings (API keys masked) |

---

## Local LLM & Gemma (Ollama)

Mosaic-agent is optimized for local execution using **Ollama**.

1.  **Pull Gemma 4:**
    *   For general hardware: `ollama pull gemma4:latest`
    *   For macOS (Apple Silicon, 16GB+ Unified Memory): `ollama pull gemma4:12b-mlx` (Requires Ollama version `v0.30.3` or higher)
2.  **Create Mosaic Model:** `ollama create mosaic-gemma4 -f ollama/Modelfile`
3.  **Configure `.env`:**
    ```env
    LLM_PROVIDER=openai
    LLM_MODEL=gemma4:12b-mlx  # Or mosaic-gemma4
    LLM_BASE_URL=http://localhost:11434/v1
    ```
The orchestrator automatically detects low-context local models and switches to **high-density compact prompts** or **direct-data-injection** paths to maintain accuracy without cloud dependencies.

---

## Data Hub & Streamlit UI

```bash
docker compose up -d                     # ClickHouse + UI together
# or
python src/main.py ui                    # UI only (ClickHouse must already be running)
```

Open **http://localhost:8501**.

### Broker backups

```bash
python scripts/save_portfolio_holdings.py  # backup CNC holdings
python scripts/backup_zerodha_account.py   # backup profile, margins, and orders
```

### Import data

```bash
python src/main.py import --category commodities
python src/main.py import --category etfs --category mf
python src/main.py import --category fii_dii    # FII/DII flows
```

### Custom Date Range Charts & Imports
Both the EOD import (`import_symbol_data`) and charting (`plot_price_chart`) tools support custom date ranges (years, year ranges, months, explicit dates). The dynamic routing planner automatically extracts these parameters and injects them as start/end date hints:
```bash
# E.g. Ask the agent to import custom periods (defaults to Shoonya for stocks/ETFs)
python src/main.py ask "import GOLDBEES 2019"

# E.g. Ask the agent to plot custom periods
python src/main.py ask "show goldbees price of 2026 to 2019"
```

*Note: For the `stocks` and `us_stocks` categories, the importer automatically switches to a high-concurrency mode running parallel symbol fetches (up to 5 concurrent workers) that pull prices, earnings, insider trades, and valuations. This parallel run is staggered with random jitter delays to avoid rate-limiting blocks. You can also invoke the parallel script directly:*
```bash
python src/scripts/portfolio/import_stocks_parallel.py --workers 5
```

> [!IMPORTANT]  
> **Mandatory Data Freshness:** Quantitative signals (Macro, ML, Composite) rely on cross-asset correlations (e.g., Gold vs USDINR vs US10Y). If any category is stale, the signals are mathematically invalid. **Always run an import at the start of your session.**

### Institutional Whale Tracking (AMC Disclosures)

Mosaic features specialized data engineering to backfill and track high-conviction institutional disclosures:

- **DSP Smart Money:** 31-month history for 60+ DSP funds; reverse-engineered tactical pivot signals.
- **Nippon India:** Dynamic URL discovery for multi-asset and equity holdings (2024–present).
- **ICICI Prudential:** Direct integration for multi-asset and index fund portfolio tracking.

```bash
# Import all DSP history (Sep 2023–Mar 2026)
python scripts/import_dsp_history.py

# Run Whale Tracker (detects FII/DII flow patterns across 7 multi-asset funds)
python src/scripts/market/whale_tracker.py
```

### DSP Multi Asset historical backfill

```bash
# Validate parsing (no DB writes)
python scripts/import_dsp_history.py --dry-run

# Import all 31 months (Sep 2023–Mar 2026) into mf_holdings
python scripts/import_dsp_history.py

# Reverse-engineer DSP's quant strategy (requires ClickHouse)
python scripts/dsp_quant_strategy_analyzer.py
```

See [docs/import-schema.md](docs/import-schema.md) for all categories, the full ClickHouse schema, and recommended cron schedules.

---

## Documentation

| Doc | What's in it |
|---|---|
| [docs/user_guide.md](docs/user_guide.md) | **Getting Started User Guide** — tutorials, CLI commands, database explorers, offline setups |
| [docs/architecture.md](docs/architecture.md) | Full system architecture — data flow, agents, tools, ML, ClickHouse schema, design patterns |
| [docs/agent-architecture.md](docs/agent-architecture.md) | Agent orchestration — intent routing, 10 sub-agents, tracing, budget middleware, mandatory rules |
| [docs/import-schema.md](docs/import-schema.md) | All import categories, ClickHouse tables, cron schedule |
| [docs/data-sources.md](docs/data-sources.md) | APIs and data sources used |
| [docs/anomaly-detection.md](docs/anomaly-detection.md) | GARCH(1,1) anomaly pipeline — regimes, cross-asset IF, Risk Governor integration |
| [docs/ml-forecast.md](docs/ml-forecast.md) | LightGBM 25-feature set, quantile CI, COT lag fix, regime signals |
| [docs/configuration.md](docs/configuration.md) | All `.env` settings |
| [docs/db-management.md](docs/db-management.md) | ClickHouse backup/restore strategy, retention policies, monitoring queries, maintenance schedule |
| [docs/gemini-prompts.md](docs/gemini-prompts.md) | 20 ready-to-use Gemini CLI prompts |

---

## Latest Reports

- **[INR Hedge & Taxation Report](https://htmlpreview.github.io/?https://github.com/Mosaic-agent/Mosaic-fund-agent/blob/main/output/inr_hedge_report.html)** — Comprehensive analysis of INR hedging strategies and current FoF taxation rules (Budget 2024 updates).

---

## Project Structure

```
config/settings.py              Pydantic settings (LLM, ClickHouse, API keys, market constants)
src/
  main.py                       CLI — 13 commands (analyze, import, signals, macro, comex, …)
  agents/
    mosaic_fund_agent.py          Zerodha portfolio → enrich → LLM score → JSON report
    comex_agent.py              Pre-market commodity signals (XAU, XAG, XPT, XPD, HG)
    news_sentiment_agent.py     Multi-source news sentiment (NewsAPI + GNews)
    signal_aggregator.py        6-pillar composite ETF scores 0–100 → BUY/HOLD/SELL
  analyzers/                    asset_analyzer, portfolio_analyzer
  clients/mcp_client.py         Zerodha Kite MCP (JSON-RPC 2.0)
  importer/
    cli.py                      run_import() — delta-sync entry point
    clickhouse.py               Schema DDL, bulk inserts, watermark management
    registry.py                 Symbol catalogs (50 stocks, 30+ ETFs, 7 commodities, …)
    fetchers/                   One file per external data source
  ml/
    trend_predictor.py          LightGBM 5-day return predictor (25 alpha features, quantile CI)
    anomaly.py                  Robust Z + GARCH(1,1) Student-t + cross-asset Isolation Forest
  tools/
    quant_scorecard.py          Gold + Silver 4-pillar quant scores (0–100) with stale-data guards
    risk_governor.py            Inverse-vol position sizing w=vol_target/σ_t + regime overrides
    macro_event_scanner.py      8 macro themes → ETF impact maps from live news
    inav_fetcher.py             Live ETF iNAV + premium/discount %
    comex_fetcher.py            COMEX pre-market signals
    who_is_selling_agent.py     FII/DII/Retail sell-off attribution
    premium_alerts.py           iNAV premium/discount threshold alerts
    domestic_etf_scanner.py     ETF valuation + flow + momentum scanner
    market_context.py           Live Nifty/BankNifty levels for LLM prompts
    (+ news_search, earnings_scraper, summarization, valuation_alerts, …)
  utils/
    markdown_renderer.py        Beautiful terminal Markdown renderer using rich.table.Table
    llm_cache.py                SQLite-backed LLM response cache
    ist.py, symbol_mapper.py    Utility helpers
  ui/app.py                     Streamlit data hub (Import / Query / Explorer / Kite Dashboard)
  scripts/
    goldbees_report.py          GOLDBEES investment pipeline report with LLM recommendation
    portfolio/import_stocks_parallel.py  High-concurrency parallel stock data importer
    portfolio/optimize_risk_governor.py  Risk Governor parameter grid-search optimizer
    db/fix_bad_data.py          Deduplication, watermark alignment, and invalid data repair script
scripts/
  save_portfolio_holdings.py         Backup CNC holdings to ClickHouse
  backup_zerodha_account.py          Backup profile, margins, and orders
  metals_quant_scorecard.py          Gold + Silver quant scorecards
  opportunity_scan.py                Cross-asset DB opportunity scanner
  fii_pattern_check.py               FII historical pattern analysis
  gold_quant_scorecard.py            Gold-only scorecard
  import_dsp_history.py             DSP Multi Asset 31-month ETL backfill
  dsp_quant_strategy_analyzer.py    Reverse-engineer DSP's GSR-based tactical rules
tests/
docker-compose.yml
```

See [docs/architecture.md](docs/architecture.md) for the full architecture including data flow, all ClickHouse tables, agent internals, ML pipeline details, and design patterns.

---

## Tests

```bash
python tests/test_tools.py          # unit tests (no API keys for 10/11)
python tests/_test_importer.py      # integration (requires ClickHouse)
```

---

## Gemini & Antigravity (agy) CLI

Gemini CLI and Antigravity CLI are configured — agents and skills are in `.gemini/` (Gemini) and `.agents/` (Antigravity).

```bash
cd ~/project/Mosaic-fund-agent
gemini  # For Gemini CLI
agy     # For Antigravity (agy) CLI
```

| Agent / Skill | Trigger |
|---|---|
| `@macro-strategy-agent` | Baton Pass thesis, whale tracking, 2026 themes |
| `daily-signal-composite` | "What should I buy today?" |
| `risk-governor` | "How much GOLDBEES should I hold?" |
| `macro-scanner` | "Run the macro scanner" |
| `etf-news` | "Latest GOLDBEES news" |

---

## Known Limitations

- **NewsAPI free tier:** 100 req/day — top holdings by weight are prioritised
- **iNAV:** NSE API live only 9:15 AM – 3:30 PM IST
- **LightGBM:** Requires ≥ 120 clean training rows; CV R² improves as history accumulates
- **Local LLMs:** Models < 30B struggle with multi-turn orchestration
- **Anomaly detection:** Requires ≥ 60 rows per symbol — run an import first
- **GARCH:** Requires ≥ 30 rows for rolling MAD initialisation; first ~30 rows have NaN bands
