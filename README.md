<div align="center">

# 🪙 Mosaic

### Quantitative co-pilot for Indian & global markets

**[mosaic-agent.github.io/Mosaic-fund-agent](https://mosaic-agent.github.io/Mosaic-fund-agent/)**

**ML-driven alpha · institutional flow tracking · volatility-aware position sizing — on a private, local-first data lake.**

![Python](https://img.shields.io/badge/python-3.11+-blue) ![ClickHouse](https://img.shields.io/badge/storage-ClickHouse-yellow) ![LLM](https://img.shields.io/badge/LLM-OpenAI%20%7C%20Anthropic%20%7C%20Ollama-green) ![License](https://img.shields.io/badge/license-Apache%202.0-lightgrey)

</div>

---

Mosaic syncs **13+ data sources** into a local ClickHouse lake, then layers ML forecasting, GARCH anomaly detection, composite ETF signals, and a multi-agent LLM guild on top. Every number an agent reports is computed in Python or SQL first — the LLM only narrates pre-computed results.

```
13+ sources → ClickHouse lake → signals / ML / GARCH / anomaly → LangGraph agents → CLI / dashboard
```

---

## Quick Start

### Docker (recommended)

```bash
# 1. Start everything and open the dashboard
./run.sh           # macOS/Linux
run.bat            # Windows
# → Dashboard: http://localhost:8501
# → Reports:   http://localhost:8502

# 2. Run CLI commands without installing Python
./mosaic.sh ask "what is my riskiest holding?"
./mosaic.sh analyze --max 3
./mosaic.sh comex
```

### Developer install

```bash
git clone https://github.com/Mosaic-agent/Mosaic-fund-agent.git
cd Mosaic-fund-agent
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
brew install libomp          # macOS — required by LightGBM
cp .env.example .env         # add your API keys
docker compose up clickhouse -d
python src/main.py config    # verify connection
```

Minimum keys to get started:
```
OPENAI_API_KEY=sk-...
NEWSAPI_KEY=...
GOLD_API_KEY=...
```

---

## Commands

| Command | Description |
|---|---|
| `analyze` | Full Zerodha portfolio analysis + reports |
| `ask "..."` | Free-form ReAct Q&A — routes to specialist agents |
| `signals` | Composite ETF scores (0–100) across 6 pillars |
| `macro` | Geopolitical & macro theme scanner |
| `comex` | COMEX pre-market signals (XAU, XAG, XPT, XPD, HG) |
| `etf-news` | Category-tagged ETF news sentiment |
| `risk` | GARCH-based position sizing & Kelly blend |
| `premium-alerts` | Live iNAV premium/discount alerts |
| `import` | Delta-sync market data to ClickHouse |
| `correlate --symbol X` | Map price anomalies to FX shocks, macro events, filings |
| `config` | Show current settings (keys masked) |

---

## Local LLM (Ollama)

```bash
ollama pull gemma4:latest
ollama create mosaic-gemma4 -f ollama/Modelfile
```

```env
LLM_PROVIDER=openai
LLM_MODEL=mosaic-gemma4
LLM_BASE_URL=http://localhost:11434/v1
```

---

## What's Inside

| Layer | Key files |
|---|---|
| CLI | `src/main.py` — 13 Typer commands |
| Agents | `src/agents/` — MosaicFundAgent + 7 specialist sub-agents |
| Signals | `src/agents/signal_sources.py` — 6-pillar composite (macro, flows, valuation, sentiment, ML, volatility) |
| ML | `src/ml/trend_predictor.py` — LightGBM 5-day forecast; `src/ml/anomaly.py` — GARCH + Isolation Forest |
| Correlation | `src/ml/correlation.py` — anomaly-to-event attribution (FX, macro, filings) |
| Importer | `src/importer/` — 14 fetcher adapters, watermark delta-sync, EventBus |
| Repository | `src/db/repository.py` — all ClickHouse reads, point-in-time variants |
| UI | `src/ui/app.py` — Streamlit dashboard (12 tabs) |
| Risk | `src/tools/risk_governor.py` — inverse-vol + Kelly position sizing |

---

## Documentation

| Doc | Contents |
|---|---|
| [docs/architecture.md](docs/architecture.md) | Full system architecture, data flow, ClickHouse schema |
| [docs/agent-architecture.md](docs/agent-architecture.md) | Intent routing, sub-agent guild, tracing, budget middleware |
| [docs/import-schema.md](docs/import-schema.md) | All import categories, tables, cron schedule |
| [docs/ml-forecast.md](docs/ml-forecast.md) | LightGBM features, quantile CI, walk-forward CV |
| [docs/anomaly-detection.md](docs/anomaly-detection.md) | GARCH(1,1) pipeline, regime classification, Risk Governor |
| [docs/configuration.md](docs/configuration.md) | All `.env` settings |
| [docs/user_guide.md](docs/user_guide.md) | Tutorials, CLI reference, offline setup |

---

## Tests

```bash
python -m pytest tests/test_tools.py tests/test_quant_signals.py   # unit (no keys needed)
python tests/_test_importer.py                                       # integration (needs ClickHouse)
```

---

## Known Limitations

- NewsAPI free tier: 100 req/day — top holdings prioritised
- iNAV: NSE API live only 09:15–15:30 IST
- LightGBM: requires ≥120 clean rows; accuracy improves as history accumulates
- GARCH anomaly: requires ≥60 rows per symbol — run an import first
- Local LLMs <30B struggle with multi-turn orchestration

> **Not financial advice.** Personal research tool. Always verify before acting. [Apache 2.0](LICENSE).
