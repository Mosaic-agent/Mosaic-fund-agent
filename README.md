# Mosaic Fund Agent

Ask your Zerodha portfolio a question. Get an actual answer.

Pulls live holdings from Zerodha Kite, enriches each position with news, results, and iNAV data, scores risk and sentiment via LLM, and outputs a terminal report and JSON export.

A built-in **Streamlit data hub** lets you import and store historical market data in ClickHouse, run arbitrary SQL, and run composite anomaly detection and ML forecasting — no code required.

> **Not financial advice.** This is a personal research tool. Always verify before acting on any output.

Licensed under the [Apache License 2.0](LICENSE).

---

## Features

- **Portfolio analysis** — per-holding risk scores, news sentiment, quarterly results, sector breakdown
- **ETF premium/discount** — live iNAV vs market price for every ETF holding
- **COMEX pre-market signals** — Gold, Silver, Copper, Platinum, Palladium vs previous close
- **Who Is Selling?** — institutional sell-off attribution (retail panic / institutional exit / speculator crowding)
- **LightGBM 5-day forecast** — walk-forward predictor with 25 alpha features + quantile regression 80% CI
- **GARCH(1,1) anomaly detection** — conditional volatility residuals + cross-asset Isolation Forest (COT + USDINR)
- **Risk Governor** — continuous inverse-vol position sizing (`w = vol_target / σ_t`) with regime + score overrides
- **Quant Scorecard** — Gold + Silver 4-pillar scores (Macro/Flows/Valuation/Momentum) with stale-data guards
- **DSP Smart Money tracker** — 31-month allocation history for DSP Multi Asset Fund; reverse-engineered GSR-based tactical pivot signal (R=0.68)
- **FII/DII institutional flows** — daily cash + monthly (Sep 2018→present) + F&O participant OI
- **Streamlit UI** — import, SQL explorer, charts, anomaly detection, and **🪁 Kite Dashboard**
- **Zerodha Account Backup** — persist personal holdings, margins, profile, and orders to ClickHouse for historical tracking
- **Gemini & Antigravity (agy) CLI agents** — macro strategy agent + 4 skills discoverable from `.gemini/` and `.agents/`

---

## Quick Start

### One-Click Setup (Non-Technical)

If you are not a developer or want to avoid setting up Python and installing libraries manually, you can run the entire stack (ClickHouse database, Streamlit Web Dashboard, and agents) with **Docker Desktop**:

1. Install **[Docker Desktop](https://www.docker.com/products/docker-desktop/)** and make sure it is running.
2. Double-click **[run.sh](file:///home/dt/project/Mosaic-fund-agent/run.sh)** (macOS/Linux) or **[run.bat](file:///home/dt/project/Mosaic-fund-agent/run.bat)** (Windows).
   - *First-run only:* This creates a `.env` file in the project folder. Open it in a text editor to add your API keys (like OpenAI, Zerodha login, etc.).
3. Once configured, run the script again. It will automatically build the image, start the containers, and open the Web Dashboard at **http://localhost:8501** in your browser.
4. To run individual CLI commands or scripts without installing Python locally, use the **[mosaic.sh](file:///home/dt/project/Mosaic-fund-agent/mosaic.sh)** (macOS/Linux) or **[mosaic.bat](file:///home/dt/project/Mosaic-fund-agent/mosaic.bat)** (Windows) wrappers:
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
5. To stop the application, double-click **[stop.sh](file:///home/dt/project/Mosaic-fund-agent/stop.sh)** (macOS/Linux) or **[stop.bat](file:///home/dt/project/Mosaic-fund-agent/stop.bat)** (Windows).

See the detailed **[Docker Setup Guide](file:///home/dt/project/Mosaic-fund-agent/docs/docker_install_guide.md)** for platform-specific installation steps.

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

### Other commands

```bash
python src/main.py comex                 # COMEX pre-market signals
python src/main.py news GOLDBEES         # news for a single symbol
python src/main.py config               # show current settings (masked)
```

### Zerodha Account Backup (to ClickHouse)

```bash
python scripts/save_portfolio_holdings.py  # backup CNC holdings
python scripts/backup_zerodha_account.py  # backup profile, margins, and orders
```

---

## Data Hub & Streamlit UI

```bash
docker compose up -d                     # ClickHouse + UI together
# or
python src/main.py ui                    # UI only (ClickHouse must already be running)
```

Open **http://localhost:8501**.

### Import data

```bash
python src/main.py import --category commodities
python src/main.py import --category etfs --category mf
python src/main.py import --category fii_dii    # FII/DII flows

# Full backfill (ignores watermarks)
python src/main.py import --category stocks --full

# Preview without writing
python src/main.py import --category etfs --dry-run
```

Subsequent runs are **delta-synced** — only new data is fetched.

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
| [docs/architecture.md](docs/architecture.md) | Full system architecture — data flow, agents, tools, ML, ClickHouse schema, design patterns |
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
  ui/app.py                     Streamlit data hub (Import / Query / Explorer / Kite Dashboard)
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
