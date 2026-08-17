---
name: mf-tracker-agent
description: Complete Mutual Fund (MF) research, portfolio disclosures, AMC importers, AMC-specific bullish small-cap stock picks, DSP active-fund cross-ownership conviction signals, small-cap & mid-cap cross-ownership screening, AMFI category flows, MoM NAV returns, multi-asset institutional Whale Tracking, cross-fund multi-asset consensus (smart-money overlap), and full-universe cross-AMC whale accumulation scanning with optional RSI/drawdown/volume-surge technical confirmation, across all supported AMCs. Trigger when the user asks "track mf holdings", "whale tracker", "mf whale tracker", "dsp holdings", "fund holdings", "amfi flows", "fund returns", "small cap cross ownership", "mid cap conviction", "amc bullish small cap", "multi asset consensus", "what are multi asset funds buying", "trend across multi asset funds", "smart money overlap", "cross-amc consensus buys", "institutional accumulation", "which consensus buys are technically attractive", "whale accumulation scan", or invokes /mf-tracker.
tools:
  - run_command
  - view_file
  - search_web
  - read_url_content
  - grep_search
  - list_dir
model: inherit
temperature: 0.1
max_turns: 25
---

# Complete Mutual Fund (MF) & AMC Research Suite (`/mf-tracker`)

This skill is the single unified reference and execution guide for **ALL Mutual Fund capabilities** across Indian AMCs (DSP, Nippon India, ICICI Prudential, Quant, HDFC, Bajaj Finserv, AMFI category flows) with dedicated tools for **Sector Allocations**, **MoM/YoY Sector Rotation**, **Investment Thesis Rationale**, **100% Exhaustive Stock Shift Audits**, and **Master Executive CLI Dashboards**.

---

## 🛠️ Registered Mutual Fund Analysis Tools (`MosaicFundAgent`)

| Tool Name | Module | What it Does |
| :--- | :--- | :--- |
| `smallcap` | `src/scripts/portfolio/smallcap_pattern_analyzer.py` | Multi-AMC Small Cap pattern & accumulation analyzer (CLI: `python src/main.py smallcap --amc <all\|dsp\|nippon\|hdfc\|quant>`) |
| `run_mf_concentration_risk` | `src/scripts/portfolio/concentration_risk.py` | Latest equity-sleeve HHI, effective stock count, named top-three holdings, concentration alerts, and sector weights for any fund or all multi-asset funds |
| `analyze_mf_sectors` | `src/tools/mf_sector_analyzer.py` | Single AMC sector breakdown (% AUM, ₹ Cr value, active stock count) & multi-AMC comparative matrix |
| `detect_amc_sector_rotation` | `src/tools/mf_sector_rotation.py` | MoM / YoY sector weight shifts (% AUM), capital deltas (₹ Cr), rotation status tags |
| `run_multi_asset_consensus` | `src/scripts/portfolio/multi_asset_consensus.py` | Cross-fund "smart money overlap" — consensus adds/trims held or moved by ≥2 of the 7 tracked multi-asset funds, plus persistence-ranked asset-class and sector rotation |
| `scan_whale_accumulation` | `src/scripts/portfolio/whale_accumulation_scanner.py` (`src/tools/whale_tools.py`) | Cross-AMC consensus scan across the FULL active-equity universe (15+ AMC groups, not just multi-asset funds) — consensus_score, zero-to-hero fresh entries, and optional RSI/drawdown/volume-surge technical confirmation (`with_technicals=True`) |
| `get_whale_consensus` | `src/tools/whale_tools.py` | Single-stock lookup: which AMCs hold it, weight, MoM delta, and months-held conviction tenure, across the same full active-equity universe |
| `explain_rotation_thesis` | `src/tools/mf_rotation_thesis.py` | Macro, fundamental, and valuation investment thesis explaining WHY an AMC rotated |
| `audit_exhaustive_stock_shifts` | `src/tools/mf_sector_rotation.py` | 100% complete audit of ALL stock additions (+₹ Cr) and subtractions (-₹ Cr) with zero data loss |
| `display_master_amc_dashboard` | `src/tools/cli_ux_dashboard.py` | Master Executive CLI Dashboard (Left-to-Right Horizontal Sector Flow + Side-by-Side Stock Ledger) |


---

## 📊 Fund Concentration Risk & Top Holdings

Use this report for any fund category, including small-cap and multi-asset schemes. It evaluates the latest disclosure available for the selected fund, normalizes weights within its equity sleeve, and renders the named top-three holdings alongside HHI, top-three/top-ten concentration, single-stock alerts, and sector concentration.

```bash
# Exact fund name
python src/scripts/portfolio/concentration_risk.py --fund DSP_SMALL_CAP

# AMFI scheme code
python src/scripts/portfolio/concentration_risk.py --scheme 120821

# Cross-fund multi-asset comparison
python src/scripts/portfolio/concentration_risk.py --all
```

The Gemini MF agent should call `run_mf_concentration_risk` for requests such as “is this fund diversified?”, “show concentration risk”, or “what are the top holdings?”. Provide exactly one selector: `fund`, `scheme_code`, or `all_funds=True`.

---

## 🏛️ 1. AMC-Specific Bullish Small-Cap Stock Pick Queries

Finds the single highest-conviction small-cap stock picks (% NAV allocation > 3.5%) for individual active small-cap funds:

```sql
SELECT 
    fund_name AS AMC_Fund,
    security_name AS Company,
    pct_of_nav AS NAV_Weight_Pct,
    round(market_value_cr, 2) AS Holding_Value_Cr,
    as_of_month AS Disclosed_Month
FROM market_data.mf_holdings FINAL
WHERE fund_name IN ('DSP_SMALL_CAP', 'QUANT_SMALL_CAP', 'NIPPON_INDIA_SMALL_CAP', 'BAJAJ_FINSERV_SMALL_CAP_FUND')
  AND as_of_month = (SELECT max(as_of_month) FROM market_data.mf_holdings WHERE fund_name LIKE '%SMALL%')
  AND lower(asset_type) = 'equity'
ORDER BY fund_name ASC, pct_of_nav DESC;
```

### Top Bullish Pick Benchmarks by AMC:
* **DSP Small Cap (`DSP_SMALL_CAP`):** #1 Pick = **Thangamayil Jewellery Ltd** (**4.81% NAV / ₹945 Cr**), #2 Kirloskar Oil Engines (**4.23% NAV**), #3 Lumax Auto Tech (**4.17% NAV**).
* **Quant Small Cap (`QUANT_SMALL_CAP`):** #1 Pick = **HFCL Ltd** (**6.03% NAV / ₹2,035 Cr**), #2 RBL Bank (**5.46% NAV**), #3 Adani Power (**3.97% NAV**).
* **Bajaj Finserv Small Cap (`BAJAJ_FINSERV_SMALL_CAP_FUND`):** #1 Pick = **Rubicon Research Ltd** (**4.18% NAV / ₹92.5 Cr**), #2 Schaeffler India (**3.77% NAV**), #3 Timken India (**3.63% NAV**).

---

## 🎯 2. Small-Cap & Mid-Cap Cross-Ownership Conviction Screening

Cross-fund ownership (same stock held by 2+ active Small/Mid-Cap funds for multiple consecutive months) is the **highest-conviction long-term alpha marker** in Indian equities.

### High-Conviction Small-Cap Cross-Ownership Query (<100ms MV)
Finds small-cap stocks held by 2+ active funds with expanding total allocation:
```sql
SELECT 
    s.security_name,
    s.isin,
    s.as_of_month,
    s.active_funds_count,
    s.total_market_val_cr
FROM market_data.mf_holding_summaries s FINAL
WHERE s.as_of_month = (SELECT max(as_of_month) FROM market_data.mf_holding_summaries)
  AND s.active_funds_count >= 2
  AND s.total_market_val_cr >= 20.0
ORDER BY s.active_funds_count DESC, s.total_market_val_cr DESC
LIMIT 25;
```

### Full Fund Breakdown for Small/Mid-Cap Stock (e.g., Thangamayil / Bectorfood)
```sql
SELECT 
    fund_name,
    as_of_month,
    pct_of_nav,
    market_value_cr,
    holding_shares
FROM market_data.mf_holdings FINAL
WHERE (security_name LIKE '%THANGAMAYIL%' OR security_name LIKE '%BECTOR%')
  AND fund_name NOT LIKE '%INDEX%'
  AND fund_name NOT LIKE '%ETF%'
ORDER BY as_of_month DESC, market_value_cr DESC;
```

---

## 🏛️ 3. AMC Holdings Importers (Canonical & Factory Layers)

### Canonical Importer Layer (`amc_holdings_fetcher.py`)
```bash
python src/main.py import --category dsp          # DSP equity & multi-asset
python src/main.py import --category nippon       # Nippon India (dynamic URL discovery from 2024)
python src/main.py import --category icici        # ICICI Prudential multi-asset
python src/main.py import --category icici-index  # ICICI Index funds
python src/main.py import --category quant        # Quant active funds
python src/main.py import --category bajaj        # Bajaj Finserv funds
python src/main.py import --category abakkus      # Abakkus Mutual Fund (Flexi Cap, Small Cap, Liquid)
python src/main.py import --category helios       # Helios Mutual Fund (Small Cap, Flexi Cap, Mid Cap, BAF, etc.)
python src/main.py import --category invesco      # Invesco Mutual Fund (Smallcap, Contra, Multi Asset, Flexi Cap, etc.)
python src/main.py import --category canara       # Canara Robeco Mutual Fund (Small Cap, Flexi Cap, Value, Multi Cap, etc.)
python src/main.py import --category mirae        # Mirae Asset Mutual Fund (Large Cap, Small Cap, Midcap, Multi Asset, etc.)
python src/main.py import --category axis         # Axis Mutual Fund (Small Cap, Midcap, Multi Asset, Flexi Cap, etc.)
python src/main.py import --category motilal      # Motilal Oswal Mutual Fund (Small Cap, Midcap, Flexi Cap, Nasdaq 100, etc.)
python src/main.py import --category amfi         # AMFI category flows & AUM
```

### Factory Import Scripts Direct CLI (`src/scripts/fund_imports/`)
```bash
python src/scripts/fund_imports/run.py dsp          # DSP holdings backfill
python src/scripts/fund_imports/run.py nippon       # Nippon holdings backfill
python src/scripts/fund_imports/run.py icici        # ICICI holdings backfill
python src/scripts/fund_imports/run.py quant        # Quant holdings backfill
python src/scripts/fund_imports/run.py bajaj        # Bajaj holdings backfill
python src/scripts/fund_imports/run.py abakkus      # Abakkus holdings backfill
python src/scripts/fund_imports/run.py helios       # Helios holdings backfill
python src/scripts/fund_imports/run.py invesco      # Invesco holdings backfill
python src/scripts/fund_imports/run.py canara       # Canara Robeco holdings backfill
python src/scripts/fund_imports/run.py mirae        # Mirae Asset holdings backfill
python src/scripts/fund_imports/run.py axis         # Axis Mutual Fund holdings backfill
python src/scripts/fund_imports/run.py motilal      # Motilal Oswal Mutual Fund holdings backfill
python src/scripts/fund_imports/run.py amfi         # AMFI category-wise net flows
python src/scripts/fund_imports/run.py all          # Run all registered AMC importers
```

### Specialized DSP Scripts (`src/scripts/dsp/`)
```bash
python src/scripts/dsp/import_all_dsp_equity.py   # Full DSP holdings backfill (2022-2026)
python src/scripts/dsp/import_latest_dsp.py       # Latest month disclosures only
```

---

## 🐳 4. Institutional Whale Tracker (All AMCs with Multi-Asset Funds)

Runs the multi-asset fund allocation scanner and single-name cross-ownership conviction index across **all AMCs with Multi-Asset Allocation funds** (19+ schemes including Nippon India, DSP, ICICI Prudential, Quant, Bajaj Finserv, Axis, Invesco, Mirae Asset, Motilal Oswal, HDFC, Kotak Mahindra, SBI, Canara Robeco, etc.):
```bash
python src/scripts/market/whale_tracker.py
```

### Tracked Multi-Asset Funds Matrix (Exhaustive & Dynamic Discovery):
| AMC / Provider | Fund Name | Scheme Identifier |
| :--- | :--- | :--- |
| **Nippon India** | Nippon India Multi Asset Fund / FoF | `RLMF806`, `RLMF811` |
| **DSP** | DSP Multi Asset Allocation / Omni FoF / Dynamic Asset Allocation | `152056`, `154167`, `126393` |
| **Quant** | Quant Multi Asset Fund / Dynamic Asset Allocation | `120821`, `120833` |
| **Bajaj Finserv** | Bajaj Finserv Multi Asset Allocation | `152639` |
| **ICICI Prudential** | ICICI Prudential Multi Asset Fund | `120334`, `120716` |
| **Axis** | Axis Multi Asset Allocation / Active FoF | `AXIS_MULTI_ASSET_ALLOCATION` |
| **Invesco** | Invesco India Multi Asset Allocation | `INVESCO_MULTI_ASSET_ALLOCATION` |
| **Mirae Asset** | Mirae Asset Multi Asset Allocation / Allocator FoF | `MIRAE_MULTI_ASSET_ALLOCATION` |
| **Motilal Oswal** | Motilal Oswal Asset Allocation FoF (Conservative / Aggressive) | `MOTILAL_ASSET_ALLOCATION_*` |
| **HDFC** | HDFC Multi-Asset Allocation | `HDFC_MULTI_ASSET` / `119047` |
| **Kotak Mahindra** | Kotak Multi Asset Allocation | `152064` / `KOTAK_MULTI_ASSET` |
| **SBI** | SBI Multi Asset Allocation | `119843` / `SBI_MULTI_ASSET` |
| **Canara Robeco** | Canara Robeco Multi Asset Allocation | `CANARA_MULTI_ASSET_ALLOCATION` |

---

## 🤝 4b. Cross-Fund Multi-Asset Consensus (Smart-Money Overlap)

While the Whale Tracker (above) tracks predefined macro *themes* (gold/silver/nuclear/infra), this tool finds **consensus signals** — securities and asset classes that *multiple* multi-asset funds across all AMCs are simultaneously adding to or trimming in the same window.

```bash
# Latest month MoM consensus across all multi-asset funds (default)
python src/scripts/portfolio/multi_asset_consensus.py

# YoY (latest vs 12 months back) instead of MoM
python src/scripts/portfolio/multi_asset_consensus.py --period yoy

# Lower the minimum-fund threshold (default 2 funds)
python src/scripts/portfolio/multi_asset_consensus.py --min-funds 3

# Restrict to one asset class (equity / gold / bond / cash / other)
python src/scripts/portfolio/multi_asset_consensus.py --asset gold

# Show more rows
python src/scripts/portfolio/multi_asset_consensus.py --top 30
```

Outputs, in order: (1) Portfolio Overlap — core holdings shared by ≥2 funds across all AMCs, (2) Consensus ADDS / TRIMS for the chosen period, (3) Asset-Class Rotation persistence table (12mo lookback, ≥3mo streak = persistent), (4) Per-Fund and Cross-Fund Sector Rotation persistence tables.

**Data-lag caveat — always check before calling a month "July" or "latest":** funds do not disclose on identical cadences. Verify per-fund `as_of_month` first:
```sql
SELECT fund_name, max(as_of_month) AS latest, count(DISTINCT as_of_month) AS n_months
FROM market_data.mf_holdings FINAL
WHERE fund_name ILIKE '%multi%asset%' OR scheme_code IN ('RLMF806','RLMF811','152056','154167','152639','120821','120334','120716','152064','119843')
GROUP BY fund_name;
```

Agent tool wrapper: `run_multi_asset_consensus` (`src/tools/runners.py`, registered on the MF sub-agent — see `src/agents/sub_agents/mf.py`).

### ASCII chart views of the same data

`multi_asset_consensus.py` prints Rich data tables; for an actual chart-style read (bar/heatmap, not rows), use:
```bash
python src/scripts/portfolio/multi_asset_ascii_viz.py
python src/scripts/portfolio/multi_asset_ascii_viz.py --min-delta 0.25 --top 10
```
Renders (1) a Fund x Asset-Class MoM weight-shift matrix with an in-cell text bar per fund/asset-class pair, and (2) a diverging ASCII bar chart of consensus movers (securities ≥2 funds moved, joined on **ISIN** — not `security_name` text, since AMCs spell the same company inconsistently, e.g. "HDFC Bank Ltd" vs "HDFC Bank Limited", which fragments one company into false duplicates if joined on the raw name). Same data-lag caveat as §4b applies — each fund's row compares its own two most-recent snapshots, which are not all the same calendar month.

---

## 🐋 4c. Cross-AMC Whale Accumulation Scanner (Full Active-Equity Universe + Technical Confirmation)

Unlike §4/§4b (scoped to the 19 tracked multi-asset funds), this scans **every active
equity fund across 15+ AMC groups** (ABAKKUS, AXIS, BAJAJ, CANARA, DSP, HDFC, HELIOS,
ICICI, INVESCO, KOTAK, MIRAE, MOTILAL, NIPPON, QUANT, SBI) for stocks where multiple
independent AMCs are simultaneously building positions — the broadest cross-AMC
consensus signal in this codebase.

```bash
# Full-universe scan, default 3-month lookback, min 2 AMCs
python src/scripts/portfolio/whale_accumulation_scanner.py

# Restrict to one AMC group, longer lookback, lower the consensus threshold
python src/scripts/portfolio/whale_accumulation_scanner.py --amc dsp --months 6 --min-amcs 3

# Add RSI-14 / drawdown-from-52w-high / volume-surge technical confirmation per
# accumulator (via yfinance) and a blended opportunity_score — turns "N AMCs are
# buying X" into "N AMCs are buying X and it's technically confirmed". Adds
# noticeable latency (bulk yfinance download of up to 25 candidates) — only ask
# for this when technical/price confirmation is actually wanted.
python src/scripts/portfolio/whale_accumulation_scanner.py --with-technicals

# Save a Markdown report to output/whale_accumulation_report.md
python src/scripts/portfolio/whale_accumulation_scanner.py --save
```

Outputs, in order: (1) Top Accumulators ranked by `consensus_score = num_amcs ×
avg_delta_pp` (plus RSI/Drawdown/Vol Surge/Opp. Score columns when
`--with-technicals` is passed), (2) Zero-to-Hero fresh entries (stocks that had 0%
weight before and are now held by `min_amcs`+ AMCs), (3) largest institutional bets
by aggregate ₹ value.

**Two data-quality fixes baked into this scanner that matter if you're debugging an
unexpectedly narrow AMC list in the output:**
- `security_name` is normalized for the "Ltd" vs "Limited" company-suffix spelling
  difference between importers (older BRAND_SCHEME importers use "Ltd"; the newer
  Abakkus/Axis/Canara Robeco/Helios/Invesco/Mirae Asset/Motilal Oswal importers use
  "Limited" for the same companies) before grouping — otherwise the same real stock
  silently splits into two consensus rows and looks like less cross-AMC overlap than
  there really is.
- "Latest" and "comparison" snapshot dates are resolved **independently per fund**,
  not via one shared calendar-month bucket — different AMCs' importers stamp their
  monthly disclosure differently (e.g. one AMC's latest row dated the 1st of the next
  month vs another's dated the actual month-end, a day apart in reality), so a shared
  bucket would silently exclude whichever AMCs' latest row lands in an earlier
  calendar month.

Agent tools: `scan_whale_accumulation(amc, lookback_months, min_amcs, with_technicals)`
and `get_whale_consensus(symbol)` (`src/tools/whale_tools.py`, registered on the MF
sub-agent — see `src/agents/sub_agents/mf.py`). Ask for these (rather than
`run_multi_asset_consensus` / `run_whale_tracker`) when the question is about the full
AMC universe or wants technical confirmation, e.g. "what is everyone accumulating",
"which consensus buys are also technically attractive", "is DSP or Nippon buying
<stock>".

---

## 📈 5. MoM NAV Returns Analysis

Compute Month-over-Month NAV returns for any Direct Growth scheme using `mfapi`:
```bash
python src/scripts/portfolio/fund_mom_returns.py --scheme <SCHEME_CODE>
```

### DSP Direct Growth Scheme Codes:
| Fund Name | Scheme Code | Fund Category |
| :--- | :---: | :--- |
| **DSP Multi Asset Allocation** | `152056` | Multi Asset |
| **DSP Multi Asset Omni FoF** | `154167` | Multi Asset FoF |
| **DSP Mid Cap** | `119071` | Mid Cap |
| **DSP Small Cap** | `119212` | Small Cap |
| **DSP Flexi Cap** | `119076` | Flexi Cap |
| **DSP Large Cap** | `119250` | Large Cap |
| **DSP Large & Mid Cap** | `119218` | Large & Mid Cap |
| **DSP ELSS Tax Saver** | `119242` | ELSS / Tax Saver |
| **DSP TIGER** | `119247` | Infrastructure / Thematic |
| **DSP Dynamic Asset Allocation** | `126393` | Balanced Advantage |
| **DSP Quant** | `147306` | Factor / Quant |
| **DSP Value** | `148595` | Value |
| **DSP Healthcare** | `145454` | Pharma / Healthcare |

### Any Fund by Name (not just DSP)

Don't know the scheme code? Search by name first — works for any AMC/fund, not just the DSP table above:
```bash
python src/scripts/portfolio/fund_mom_returns.py --search "<fund name>"   # lists matching scheme codes (e.g. Direct vs Regular Plan), then pick one
python src/scripts/portfolio/fund_mom_returns.py --scheme <CODE> --months <N>
```
Agent tool: `run_fund_mom_returns(scheme_code, search_query, months=12)` (`src/tools/runners.py`).

**NAV is always fetched live from mfapi.in** — no ClickHouse import step exists or is needed for actively-managed AMC schemes (only a fixed ETF/index watchlist gets imported into `market_data.mf_nav`, see `MF_SCHEME_CODES` in `src/data_importer/registry.py`).

**Expense ratio is NOT available** from this tool or anywhere else in the codebase — mfapi.in's scheme metadata only has `fund_house`/`scheme_type`/`scheme_category`/`scheme_code`/`scheme_name`/ISINs. Don't invent a number from training knowledge; say it's unavailable (see `docs/CLAUDE.md` grounding rules).

---

## 🔍 6. DSP Active-Fund Conviction Signal (Single-Name Research)

DSP active-fund holdings in `market_data.mf_holdings` are the primary institutional single-name conviction signal.

### Active Funds (Meaningful Signal):
`DSP_SMALL_CAP`, `DSP_MID_CAP`, `DSP_LARGE_AND_MID_CAP`, `DSP_FLEXI_CAP`, `DSP_MULTICAP`, `DSP_FOCUSED`, `DSP_VALUE`, `DSP_TIGER`, `DSP_BUSINESS_CYCLE`, `DSP_ELSS_TAX_SAVER`, `DSP_HEALTHCARE`, `DSP_BANKING_FINANCIAL_SERVICES`, `DSP_QUANT`.

### Passive Funds (Ignore — Index Tracking):
`DSP_NIFTY_*_INDEX`, `DSP_NIFTY_*_ETF`, `DSP_BSE_*_ETF`, `DSP_*_QUALITY_50_INDEX`.

---

## ⚡ 7. Ultra-Fast Pre-Aggregated Queries (<100ms)

Use `market_data.mf_holding_summaries` Materialized View for instant aggregate stock counts and totals:
```sql
SELECT 
    security_name,
    isin,
    as_of_month,
    active_funds_count,
    total_market_val_cr
FROM market_data.mf_holding_summaries FINAL 
WHERE security_name LIKE '%THANGAMAYIL%'
ORDER BY as_of_month DESC;
```

---

## 📊 8. AMFI Category Flows & Sector AUM Tracking

`market_data.amfi_category_flows` contains monthly category-level gross purchases, redemptions, net flows, and closing AUM.

```sql
SELECT 
    report_month,
    category_name,
    subcategory_group,
    net_flow_cr,
    closing_aum_cr,
    flow_pct_of_aum
FROM market_data.amfi_category_flows FINAL
WHERE category_name LIKE '%Small Cap%' OR category_name LIKE '%Multi Asset%'
ORDER BY report_month DESC
LIMIT 12;
```

---

## 📌 Critical Mandates & User Rules

1. **Verify Dilution Before Flagging Promoter Sale:** A drop in promoter % is **not** a sell signal if total shares outstanding expanded (QIP, preferential allotment, bonus, ESOPs). Always verify whether the denominator expanded before drawing a sale conclusion.
2. **DSP Active Cross-Ownership = Highest Conviction:** 2+ active DSP funds holding a stock for 24+ months is the strongest single-name conviction signal in Mosaic.
3. **No LLM Math:** Never compute percentages, CAGR, or totals in internal reasoning — always read pre-computed output rows from ClickHouse SQL or script outputs verbatim.
