# AGENTS.md - Mosaic Operational Reference & Instructions

This file is read automatically by Codex when working in this project.

## Project Overview

Mosaic is an agentic quantitative research, asset-allocation, and risk-management platform for Indian and global ETF/equity markets. It integrates walk-forward machine learning predictions (LightGBM), dynamic volatility scaling (GARCH), multi-pillar signal aggregation, and institutional flow (whale) tracking.

### Essential CLI Commands
```bash
python src/main.py ask "question"           # ReAct LLM Q&A / sub-agent dispatch
python src/main.py analyze                  # Full live portfolio (Zerodha login required)
python src/main.py import --category etfs,stocks,mf,fii_dii,cot,fx_rates
python src/main.py signals --save --verbose # Composite signal aggregator
python src/main.py smallcap --amc all       # Multi-AMC Small Cap pattern & accumulation analyzer
python src/main.py macro --max 3            # Macro theme scanner
python src/main.py comex                    # COMEX pre-market gold/silver/copper
python src/main.py ui                       # Streamlit hub at localhost:8501
```

### Key Scripts
```bash
python src/scripts/goldbees_report.py                              # Pre-baked GOLDBEES signal (~2s)
python src/scripts/portfolio/smallcap_pattern_analyzer.py --amc all  # Multi-AMC Small Cap accumulation & conviction analyzer
python src/scripts/portfolio/run_stock_quant_workflow.py BAJFINANCE  # Stock ASCII chart, anomalies & MF holdings
python src/scripts/portfolio/fund_mom_returns.py --scheme 152056   # MoM NAV returns

python src/scripts/market/whale_tracker.py                         # All 7 multi-asset funds
python src/scripts/dsp/import_all_dsp_equity.py                    # DSP holdings import
python src/scripts/db/fix_bad_data.py                              # Deduplication & price repair
```

### Key Module Layers
| Layer | Path | Role |
|-------|------|------|
| CLI | `src/main.py` | Typer commands; entry point |
| Agents | `src/agents/` | LangChain/LangGraph orchestrators & routing |
| Declarative | `src/agents/declarative/` | Configuration-driven YAML playbooks (`config/agents/*.yaml`) & runner |
| Analyzers | `src/analyzers/` | `asset_analyzer` (per-holding), `portfolio_analyzer` (aggregate) |
| Tools | `src/tools/` | Pure functions returning dict/DataFrame |
| Importer | `src/data_importer/` | Delta-sync pipeline: fetchers → ClickHouse |
| DB Pool | `src/db/pool.py` | Thread-safe `CHPool` singleton (`get_pool()`) |
| ML | `src/ml/` | LightGBM 5-day forecast (`trend_predictor`), composite anomaly (`anomaly.py`) |
| Repository | `src/db/repository.py` | `MarketDataRepository`: typed reads, watermarks |
| Models | `src/models/portfolio.py` | Pydantic: `Holding`, `Portfolio`, `Sentiment` |
| Config | `config/settings.py` | Pydantic `BaseSettings`; all settings from `.env` |
| UI | `src/ui/app.py` | Streamlit hub (5 tabs over ClickHouse) |

---

## Persistent Rules & Domain Mandates

### 1. No LLM Calculations
**NEVER calculate, estimate, or derive numbers** (percentages, averages, sums, PE, CAGR, Kelly fractions) within your internal reasoning. ALL numeric work MUST be computed by Python/SQL tools. Narrate tool output verbatim without modification.

### 2. Zero-Trust Verification Protocol
- **Re-read Mandate:** Re-scan raw tool output before citing any number.
- **Symbol-Row Locking:** Explicitly verify that the symbol in the user prompt matches the exact row you are reading.
- **Quant Overlay Priority:** Only cite specific prices or flows if they appear in the Quant Overlay panel or a direct SQL result.

### 3. Verify Dilution Before Flagging Promoter Sale
A drop in promoter % is **not** the same as a promoter sale. Check whether total shares outstanding expanded in the same/prior quarter via QIP, preferential allotment, rights/bonus issue, ESOP exercise, or M&A.
- Promoter-% drop with **unchanged share count** = dilution, not sale.
- Promoter-% drop with **lower share count** = actual sale (red flag).

### 4. DSP Active-Fund Holdings = Highest Conviction Signal
DSP active-fund holdings in `market_data.mf_holdings` are the primary single-name Indian equity signal. Cross-fund ownership (held by 2+ active DSP funds for 24+ months) is the strongest long-term conviction marker.
- **Active funds:** `DSP_SMALL_CAP`, `DSP_MID_CAP`, `DSP_LARGE_AND_MID_CAP`, `DSP_FLEXI_CAP`, `DSP_MULTICAP`, `DSP_FOCUSED`, `DSP_VALUE`, `DSP_TIGER`, `DSP_BUSINESS_CYCLE`, `DSP_ELSS_TAX_SAVER`, `DSP_HEALTHCARE`, `DSP_BANKING_FINANCIAL_SERVICES`, `DSP_QUANT`.
- **Passive funds (ignore):** `DSP_NIFTY_*_INDEX`, `DSP_NIFTY_*_ETF`, `DSP_BSE_*_ETF`, `DSP_*_QUALITY_50_INDEX`.

### 5. Grounding & Pipeline Recommendation Rules
- The recommended GOLDBEES position weight is ALWAYS `weights.blended_50` (50% Risk Governor + 50% Kelly), NOT `weights.rg`.
- Do NOT invent composite scores or labels (like "ACCUMULATE" / "STRONG BUY") — use `regime_signal` as-is.

### 6. Commit Workflow
- Write clean, single-author commit messages. **NEVER add `Co-Authored-By:` trailers**.
- **NEVER stage or commit code automatically**. Always wait for explicit user prompt (`/commit`, "commit this").

---

## ClickHouse Schema & Documentation References

- **Database:** `market_data` (ReplacingMergeTree tables; always query with `FINAL`).
- **Core Tables:** `daily_prices`, `mf_nav`, `mf_holdings`, `fii_dii_flows`, `fii_dii_monthly`, `cot_gold`, `cb_gold_reserves`, `etf_aum`, `inav_snapshots`, `fx_rates`, `ml_predictions`, `signal_composite`, `news_articles`, `import_watermarks`, `corporate_actions`, `amfi_category_flows`.
- **Full Architecture & Details:**
  - System & Data Pipelines: [docs/architecture.md](docs/architecture.md)
  - Agent Orchestration & Playbooks: [docs/agent-architecture.md](docs/agent-architecture.md)
  - Anomaly Detection Pipeline: [docs/anomaly-detection.md](docs/anomaly-detection.md)
  - Vector RAG & Qdrant Collections: [docs/rag-architecture.md](docs/rag-architecture.md)
