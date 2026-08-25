# GEMINI.md - Mosaic Operational Reference & Instructions

This file is read automatically by Gemini CLI when working in this project.

> Kept content-identical to [AGENTS.md](AGENTS.md) (read by Codex) except for this header — if you edit one, mirror the change in the other.

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
python src/scripts/portfolio/fund_mom_returns.py --search "<name>"  # resolve scheme code by fund name (any AMC)

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

### 7. No Web Search Mandate
- **NEVER use web search (`search_web` or web search tools)**. Rely strictly on local codebase, ClickHouse database, Python/SQL tools, and direct tool outputs.

### 8. Fund NAV Lookup & Expense Ratio
- For "what's fund X's scheme code / NAV returns" questions, use `src/scripts/portfolio/fund_mom_returns.py` directly: `--search "<name>"` resolves the AMFI scheme code (any AMC, not just DSP), `--scheme <CODE> --months N` computes MoM returns.
- NAV is fetched **live** from mfapi.in every run — no ClickHouse import exists or is needed for actively-managed AMC schemes (only a fixed ETF/index watchlist is imported into `market_data.mf_nav`).
- **Expense ratio is not available anywhere** — mfapi.in's scheme metadata has no such field, and nothing in this codebase tracks TER. Do not substitute a number from training knowledge (see Rule 1) — state it's unavailable.

### 9. Always Use Color in Console & Reports
- ALWAYS use rich colors, ANSI escape sequences, or Rich library console styling in terminal outputs, scripts, and reports (e.g., green for gains/freshness, red for losses/anomalies, cyan/bold for headers, yellow for warnings, diff code blocks for heatmaps). Ensure console reports and markdown heatmaps are visually distinct and easy to read.

### 10. Always Render Graphs & Charts
- Whenever analyzing technical patterns, stock/ETF moves, price/volume trends, moving average setups, macro transmissions, or anomaly regimes, ALWAYS include terminal ASCII/Unicode charts (via plotext/rich), box-and-arrow transmission grids, and Mermaid diagrams alongside data tables for maximum visual clarity. Never omit the graph.

### 11. Mandatory Data Provenance & Source Grounding
- To guarantee data authenticity and ensure numbers are never made up or estimated, ALWAYS explicitly cite the exact data source, database table (e.g., `market_data.daily_prices FINAL`, `market_data.fii_dii_flows FINAL`, `market_data.mf_holdings FINAL`), filing/API origin, and watermark timestamp for all presented figures and tables. Include a dedicated Data Provenance Audit block where appropriate.

### 12. Always Render Institutional Strategy Fitment Guide
- Whenever comparing schemes, mutual funds, asset allocation strategies, AIF/SIFs, or portfolio approaches, ALWAYS include a dedicated "## 🗺️ 4. Institutional Strategy Fitment Guide" section with a visual Mermaid decision flowchart mapping investor objectives, risk appetite, time horizon, and market regime to the recommended fund/strategy.

---

## ClickHouse Schema & Documentation References

- **Database:** `market_data` (ReplacingMergeTree tables; always query with `FINAL`).
- **Core Tables:** `daily_prices`, `mf_nav`, `mf_holdings`, `fii_dii_flows`, `fii_dii_monthly`, `cot_gold`, `cb_gold_reserves`, `etf_aum`, `inav_snapshots`, `fx_rates`, `ml_predictions`, `signal_composite`, `news_articles`, `import_watermarks`, `corporate_actions`, `amfi_category_flows`, `bulk_block_deals` (37 tables total — full list in [docs/import-schema.md](docs/import-schema.md#clickhouse-schema)).
- **Full Architecture & Details:**
  - System & Data Pipelines: [docs/architecture.md](docs/architecture.md)
  - Agent Orchestration & Playbooks: [docs/agent-architecture.md](docs/agent-architecture.md)
  - Anomaly Detection Pipeline: [docs/anomaly-detection.md](docs/anomaly-detection.md)
  - Vector RAG & Qdrant Collections: [docs/rag-architecture.md](docs/rag-architecture.md)
