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

### 10. Always Render Built-in Terminal Graphs & Charts
- Whenever analyzing technical patterns, stock/ETF moves, price/volume trends, moving average setups, macro transmissions, or anomaly regimes, ALWAYS include terminal ASCII/Unicode charts (via plotext/rich) and top-down box-and-arrow transmission grids alongside data tables for maximum visual clarity. Never omit the graph.

### 11. Mandatory Data Provenance & Source Grounding
- To guarantee data authenticity and ensure numbers are never made up or estimated, ALWAYS explicitly cite the exact data source, database table (e.g., `market_data.daily_prices FINAL`, `market_data.fii_dii_flows FINAL`, `market_data.mf_holdings FINAL`), filing/API origin, and watermark timestamp for all presented figures and tables. Include a dedicated Data Provenance Audit block where appropriate.

### 12. Always Render Institutional Strategy Fitment Guide
- Whenever comparing schemes, mutual funds, asset allocation strategies, AIF/SIFs, or portfolio approaches, ALWAYS include a dedicated "## 🗺️ Institutional Strategy Fitment Guide" section with a top-down Unicode box-drawing decision flowchart or Rich tree mapping investor objectives, risk appetite, time horizon, and market regime to the recommended fund/strategy.

### 13. Drop Mermaid — Strictly Use Built-in Console Visualizations
- DO NOT use raw Mermaid code blocks (```mermaid / graph TD```) in console responses as terminal shells cannot render JavaScript SVGs.
- ALWAYS use the project's native built-in terminal visual tools:
  1. **Top-Down Box-and-Arrow Grids** (`┌──┐`, `│`, `└──┘`, `▲`, `▼`, `──►`) for transmission workflows and decision flowcharts.
  2. **Rich Colored Trees & Panels** for hierarchical structures, breakdowns, and portfolio trees.
  3. **Plotext 2D Terminal Charts** for price trends, volume spikes, and coordinate graphs.
  4. **Diff Code Blocks** (`diff` syntax with `+` in green, `-` in red) for Month-over-Month (MoM) shifts, additions, and trims.

### 14. Zero-Synthetic-Labeling & Attribution Integrity Protocol
- **Never Synthesize Status Labels for Missing Data**: If a security's price or ISIN cannot be resolved, emit `[UNRESOLVED_SECURITY: isin=...]` and report the unpriced weight percentage. NEVER invent narrative tags ('Unlisted', 'Pre-IPO', 'Delisted', 'Private Placement') to paper over pipeline lookup gaps. Always resolve via `src.tools.company_resolver.resolve_isin()`.
- **Minimum Coverage Gate**: If unpriced holdings exceed 5% of an asset sleeve's weight, SUPPRESS aggregate portfolio metrics (weighted drawdown, beta, barbell ratios) rather than computing them over partial data and fabricating narrative.
- **Attribution Conservation Law**: Before attributing returns to an asset sleeve, execute the contribution equation $\sum (w_i \times r_i) \approx \Delta \text{NAV}$. Never credit high fund returns to low-yielding cash or debt ballast.
- **Drawdown Rigor**: At an All-Time High, acknowledge that 0.00% current drawdown is a definition, not an analysis. Shift focus to full-period Maximum Historical Drawdown and forward drawdown capacity / single-stock concentration stress testing.

### 15. Regulatory-First Triangulation & Cross-Metric Sanity Protocol
To prevent compounding reporting failures in mutual funds, SIFs, and alternative strategies, strictly enforce the following gates before presenting performance, AUM, or portfolio metrics:

- **Source Hierarchy (Tier 1 vs Tier 2)**: Statutory SEBI monthly portfolio filings (`.xlsx`/`.xml`) and AMFI portal disclosures are Tier-1 Authoritative Ground Truth. Marketing factsheets (PDFs), brochures, and web scrapes are Tier-2 unverified collateral. NEVER cite a Tier-2 figure without cross-validating against Tier-1. If they conflict, report the Tier-1 regulatory figure and explicitly disclose the marketing variance.
- **Arithmetic Invariant Gate ($\Delta \text{NAV}$ Sanity Check)**: Before publishing any return table, verify:
  $$\frac{\text{NAV}_{\text{current}} - \text{NAV}_{\text{NFO}}}{\text{NAV}_{\text{NFO}}} \times 100 \approx \text{Reported Since-Inception Return}$$
  If $|\text{Reported Return} - \text{Implied Return}| > 1.0\%$, SUPPRESS the numbers and emit `[ARITHMETIC_MISMATCH: Reported=X%, Implied by NAV=Y%. Source figures contradictory — suppressed.]`.
- **Temporal Date-Locking (One-Date Rule)**: Every metric in a single comparison row (NAV, 1M, 3M, Since Inception, AUM, Beta) MUST share the exact same `as_of_date`. NEVER splice an interim live web NAV with a prior month-end factsheet return.
- **AUM Disambiguation (Regulatory Net AUM vs Gross Strategy Notional)**: Always explicitly qualify whether a fund size is **Audited Net AUM** ($\text{Units} \times \text{NAV}$ filed with AMFI) or **Gross Strategy Notional** (long equity + gross derivative notional cited in marketing brochures). If an AMC brochure cites a figure $>1.2\times$ the AMFI AAUM, append a mandatory disclosure flag.
- **Metric-Label Locking**: NEVER extract financial ratios (Beta, Sharpe, Alpha) from unstructured narrative paragraphs without verifying the exact label key. Do not conflate net exposure percentages with portfolio beta.

### 16. Stock & ETF Price Authority: Always Use NSE or Shoonya & Run via Persistent Service
- **Price Authority**: For Indian stock and ETF prices, ALWAYS use NSE or Shoonya as the authoritative source of truth. NEVER rely on Yahoo Finance or fall back to AMC declared NAV for secondary market prices (which severely corrupts ETF premium/discount calculations, particularly for international ETFs subject to RBI scarcity caps).
- **Container Execution Mandate**: NEVER use host-local Python/uv to execute tasks or pipelines (`uv run`). Always run everything inside Docker.
- **Persistent Service Execution Mandate**: NEVER spawn transient one-off containers (`docker run --rm` or `docker compose run --rm`) for ad-hoc commands or scripts. ALWAYS maintain the `mosaic` container running persistently as a background service (`docker compose up -d mosaic`) and execute tasks via `docker compose exec [-T] mosaic <cmd>` or `./mosaic.sh <cmd>` for sub-second, zero-teardown execution.

### 17. Delta-First Refresh Protocol ("Refresh Only Delta")
Whenever the user asks to "refresh", "make db fresh", "update database", "refresh only delta", or invokes `/db-freshness`, the system MUST execute a strictly delta-only update across data, vector, and signal layers without redundant historical re-fetches:
1. **Targeted Freshness Audit First**: Run `./mosaic.sh src/scripts/db/audit_freshness.py` to identify exclusively stale categories. NEVER re-import categories that are already FRESH.
2. **Targeted ClickHouse Delta Sync**: Sync ONLY the identified stale or in-session categories from their last watermark (`./mosaic.sh import --category <stale_categories> --source nse`). During active market hours when base tables are fresh, refresh live intraday ETF iNAV (`./mosaic.sh import --category inav`).
3. **Delta Vector DB Sync**: Run Qdrant synchronization (`./mosaic.sh python -m src.scripts.backfill_mf_qdrant`) strictly in delta mode (default `delta=0` check; NEVER pass `--force`).
4. **Signal & Quant Delta Aggregation**: Run composite signal generator (`./mosaic.sh signals --save`) to refresh multi-pillar composite scores over the newly updated delta prices and intraday iNAV spreads.
5. **Freshness Verification Audit**: Run `./mosaic.sh src/scripts/db/audit_freshness.py` to confirm 100% FRESH and SYNCED status across all layers.

### 18. ETF Premium/Discount Authority: AMC iNAV Precedence Over NSE EOD Feed
- **Never Use NSE EOD Feed for Premium vs Discount**: NEVER consider or use the NSE EOD feed (e.g. static declared NAV or lagged EOD prices from NSE `/api/etf` or `daily_prices`) to measure ETF premium vs discount when a live iNAV feed is available directly through the AMC (Nippon India, Zerodha, Mirae, Motilal, etc.).
- **Static vs Live Prohibition**: The static `nav` field in NSE feeds represents historical/prior-day declared NAV, not real-time intraday iNAV. Measuring secondary market price against NSE static NAV produces corrupted, phantom premium/discount figures.
- **Synchronous Market Price & iNAV**: Always pair live real-time indicative NAV (iNAV) published directly by the AMC with the real-time live secondary market Last Traded Price (LTP). Never compare a live AMC iNAV against an EOD closing price or an NSE static NAV.

---

## ClickHouse Schema & Documentation References

- **Database:** `market_data` (ReplacingMergeTree tables; always query with `FINAL`).
- **Core Tables:** `daily_prices`, `mf_nav`, `mf_holdings`, `fii_dii_flows`, `fii_dii_monthly`, `cot_gold`, `cb_gold_reserves`, `etf_aum`, `inav_snapshots`, `fx_rates`, `ml_predictions`, `signal_composite`, `news_articles`, `import_watermarks`, `corporate_actions`, `amfi_category_flows`, `bulk_block_deals` (37 tables total — full list in [docs/import-schema.md](docs/import-schema.md#clickhouse-schema)).
- **Full Architecture & Details:**
  - System & Data Pipelines: [docs/architecture.md](docs/architecture.md)
  - Agent Orchestration & Playbooks: [docs/agent-architecture.md](docs/agent-architecture.md)
  - Anomaly Detection Pipeline: [docs/anomaly-detection.md](docs/anomaly-detection.md)
  - Vector RAG & Qdrant Collections: [docs/rag-architecture.md](docs/rag-architecture.md)
  - Dynamic Asset Allocation (VLRT v3): [docs/vlrt-v3.md](docs/vlrt-v3.md)
