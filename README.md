<div align="center">

# 🪙 Mosaic

### Your AI research desk for Indian markets

**[mosaic-agent.github.io/Mosaic-fund-agent](https://mosaic-agent.github.io/Mosaic-fund-agent/)**

Signals · Smart-money tracking · Live portfolio intelligence — on your own machine.

![Free & Open Source](https://img.shields.io/badge/free%20%26%20open%20source-Apache%202.0-lightgrey) ![Runs Locally](https://img.shields.io/badge/runs%20locally-no%20cloud%20required-green) ![Zerodha](https://img.shields.io/badge/broker-Zerodha%20connected-blue) ![Python](https://img.shields.io/badge/python-3.11+-blue) ![Qdrant](https://img.shields.io/badge/vector_DB-Qdrant-dc244c)

</div>

---

## What you get

```
> mosaic ask "Should I buy GOLDBEES today?"

GOLDBEES Signal: BUY  ·  Composite score: 68/100
  ✅ Macro: DXY weak, real yields falling  (+)
  ✅ FII: Net buyers ₹1,840 Cr last 5 days  (+)
  ✅ ML forecast: +0.9% expected return (5-day)
  ⚠️  iNAV premium: +0.3% — slight overvaluation
Kelly weight: 7%   Blended recommended: 8%
```

```
> mosaic ask "What is DSP buying this month?"

DSP Multi Asset — MoM changes (Jun 2026)
  📈 ADDED   Muthoot Finance  +0.41%  (3 other funds agree ✓)
  📈 ADDED   SBI Life          +0.28%
  📉 TRIMMED Coal India        −0.33%
  ❌ EXITED  Mankind Pharma
Cross-fund consensus: Gold allocation up across all 7 funds

> mosaic ask "Which funds hold HDFC Bank?"

Funds holding HDFC Bank Ltd (equity) — Qdrant similarity search
  DSP_AGGRESSIVE_HYBRID    12.80% NAV  ₹2,341Cr  2026-05  sim=0.97
  DSP_BSE_SENSEX_ETF       12.80% NAV  ₹ 441Cr   2026-05  sim=0.97
  ICICI_BLUECHIP            8.20% NAV  ₹6,820Cr  2026-05  sim=0.95
  ICICI_MULTI_ASSET         5.20% NAV  ₹1,230Cr  2026-05  sim=0.92
  + 6 more funds
```

```
> mosaic ask "Why did ADANIENT drop 8% last Tuesday?"

Price anomaly detected: −8.3% on 2026-06-17 (GARCH Z-score: 3.1)
News correlation: Hindenburg-style short report circulated at 09:47 IST
FX: USDINR moved +0.4% same day — partial macro overlay
Corporate actions: None on record (not a split/bonus)
```

```
🚨 Live Monitor — during market hours (Slack alert)

NIFTY BANK — price break (z=−4.8) @ 13:45 IST
Price: 56,595   Volume: 45,231 (baseline ~1,050)
📰 Trump says US-Iran ceasefire is "over" after fresh strikes — Reuters
```

---

## Who is this for?

**🎯 ETF traders** tracking GOLDBEES, SILVERBEES, NIFTYBEES, BANKBEES
> "I want a daily signal — not gut feel."
> Mosaic scores 18 NSE ETFs across 6 pillars every day and tells you the recommended weight.

**🏦 Mutual fund watchers** following DSP, Nippon, Bajaj, Quant, ICICI
> "I want to know what smart money is doing before the market prices it in."
> Mosaic tracks 7 multi-asset funds month-by-month and surfaces cross-fund consensus.

**📊 Zerodha users** who want more than Kite's basic charts
> "I want to know if my portfolio is actually at risk, not just whether it's green today."
> Connect your live Zerodha portfolio. Mosaic analyzes each holding and flags concentration risk.

**🔍 Stock researchers** covering NSE/BSE companies
> "I spend 3 hours pulling data before I can even start analyzing."
> Ask in plain English. Mosaic fetches price history, MF holdings, quarterly results, news, and anomalies in parallel — and synthesises it in seconds.

---

## Ask it anything

```bash
mosaic ask "Is this a good time to add to SILVERBEES?"
mosaic ask "Which mutual funds hold Muthoot Finance?"
mosaic ask "Show me FII/DII flows this week"
mosaic ask "What's the macro picture for gold right now?"
mosaic ask "Deep dive on HDFC Bank — earnings, holdings, risks"
mosaic ask "Am I over-concentrated in financials?"
mosaic ask "Which funds have gold or commodity exposure?"
mosaic ask "Find multi-asset funds similar to DSP_MULTI_ASSET"
mosaic ask "What historical crashes looked like this GOLDBEES anomaly?"
mosaic research "Comprehensive research on Zomato"
```

---

## Your data stays yours

Everything runs on **your own machine**. No portfolio data, no trades, no questions are sent to any cloud service.

- Prices and signals live in a local **ClickHouse** database on your laptop
- LLM calls go to whichever provider you configure (OpenAI, Anthropic, or a local Ollama model — your choice)
- Zerodha connection is read-only via the official Kite MCP — Mosaic cannot place orders

---

## Quick Start

### Docker (5 minutes)

```bash
git clone https://github.com/Mosaic-agent/Mosaic-fund-agent.git
cd Mosaic-fund-agent
cp .env.example .env        # add your API keys (see below)
./run.sh                    # macOS/Linux  |  run.bat on Windows
```

Open **http://localhost:8501** — dashboard is ready.

**Minimum keys needed:**
```env
OPENAI_API_KEY=sk-...       # or ANTHROPIC_API_KEY
NEWSAPI_KEY=...             # free at newsapi.org
GOLD_API_KEY=...            # free at gold-api.com
```

### First things to do

```bash
./mosaic.sh import --category etfs,stocks,fii_dii   # sync market data (~5 min)
./mosaic.sh signals --save                           # today's ETF signals
./mosaic.sh ask "what should I know about gold today?"
```

---

## Use without Zerodha

Zerodha is optional. You can use every signal, research, and charting feature without connecting a broker account. Only the `analyze` portfolio command requires a live Zerodha connection.

---

## Run with a local AI (no API key)

```bash
ollama pull gemma4:latest
ollama create mosaic-gemma4 -f ollama/Modelfile
```

```env
LLM_PROVIDER=openai
LLM_MODEL=mosaic-gemma4
LLM_BASE_URL=http://localhost:11434/v1
```

All signals, charts, and data imports work without any LLM. The AI is only needed for natural-language Q&A and research reports.

---

## Daily workflow

| What you want | Command |
|---|---|
| Today's buy/sell signal for 18 ETFs | `signals` |
| Pre-market gold/silver/crude outlook | `comex` |
| FII/DII flow summary | `ask "show me institutional flows"` |
| Is my ETF trading at a premium? | `premium-alerts` |
| Is this intl ETF's premium cheap or rich right now? | `ask "OU premium chart for MAFANG"` |
| What's the macro news moving markets? | `macro` |
| Deep research on any Indian stock | `research "RELIANCE"` |
| Full Zerodha portfolio health check | `analyze` |
| What happened to a stock? | `ask "why did X drop last week?"` |
| Live anomaly + news alerts during market hours | `python src/agents/live_monitor.py` |

---

## What's tracked

- **18 NSE ETFs** — GOLDBEES, SILVERBEES, NIFTYBEES, BANKBEES, HNGSNGBEES, MAFANG, MON100, and more
- **50+ NSE stocks** — blue-chips and mid-caps across sectors
- **7 multi-asset MFs** — DSP, Nippon, Bajaj, Quant, ICICI with 24+ months of holdings history
- **13 data sources** — Yahoo Finance, NSE, CFTC COT, AMFI NAVs, FII/DII flows, COMEX metals, IMF/World Bank macro, and more
- **Corporate actions** — splits, bonuses, demergers automatically detected
- **Qdrant vector memory** — 5 collections: anomaly history (`market_anomalies`), MF holding similarity (`mf_holdings`, `mf_fund_profiles`), news RAG (`news_articles`), DB schema (`clickhouse_metadata`); enables semantic search across 22k+ fund positions and all flagged anomaly dates

---

## Documentation

| | |
|---|---|
| [User Guide](docs/user_guide.md) | Tutorials, example questions, offline setup |
| [Configuration](docs/configuration.md) | All `.env` settings explained |
| [Architecture](docs/architecture.md) | How it works under the hood |
| [Agent Architecture](docs/agent-architecture.md) | AI routing and workflow design |
| [Anomaly Detection](docs/anomaly-detection.md) | 5-step pipeline, Correlation Engine, Qdrant integration |
| [OU Mean-Reversion](docs/ou-mean-reversion.md) | Ornstein-Uhlenbeck premium strategy for international ETFs — buy/sell zones, forward path |

---

<div align="center">

**Not financial advice.** Personal research tool — always verify before acting.

[Apache 2.0 License](LICENSE) · [Report an issue](https://github.com/Mosaic-agent/Mosaic-fund-agent/issues)

</div>
