---
name: mf-tracker
description: Complete Mutual Fund (MF) research, portfolio disclosures, AMC importers, DSP active-fund cross-ownership conviction signals, AMFI category flows, MoM NAV returns, and multi-asset institutional Whale Tracking across all supported AMCs. Trigger when the user asks "track mf holdings", "whale tracker", "mf whale tracker", "dsp holdings", "fund holdings", "amfi flows", "fund returns", or invokes /mf-tracker.
---

# Complete Mutual Fund (MF) & AMC Research Suite (`/mf-tracker`)

This skill is the single unified reference and execution guide for **ALL Mutual Fund capabilities** across Indian AMCs (DSP, Nippon India, ICICI Prudential, Quant, Bajaj Finserv, AMFI category flows).

---

## 🏛️ 1. AMC Holdings Importers (Canonical & Factory Layers)

### Canonical Importer Layer (`amc_holdings_fetcher.py`)
```bash
python src/main.py import --category dsp          # DSP equity & multi-asset
python src/main.py import --category nippon       # Nippon India (dynamic URL discovery from 2024)
python src/main.py import --category icici        # ICICI Prudential multi-asset
python src/main.py import --category icici-index  # ICICI Index funds
python src/main.py import --category quant        # Quant active funds
python src/main.py import --category bajaj        # Bajaj Finserv funds
python src/main.py import --category amfi         # AMFI category flows & AUM
```

### Factory Import Scripts Direct CLI (`src/scripts/fund_imports/`)
```bash
python src/scripts/fund_imports/run.py dsp          # DSP holdings backfill
python src/scripts/fund_imports/run.py nippon       # Nippon holdings backfill
python src/scripts/fund_imports/run.py icici        # ICICI holdings backfill
python src/scripts/fund_imports/run.py quant        # Quant holdings backfill
python src/scripts/fund_imports/run.py bajaj        # Bajaj holdings backfill
python src/scripts/fund_imports/run.py amfi         # AMFI category-wise net flows
python src/scripts/fund_imports/run.py all          # Run all registered AMC importers
```

### Specialized DSP Scripts (`src/scripts/dsp/`)
```bash
python src/scripts/dsp/import_all_dsp_equity.py   # Full DSP holdings backfill (2022-2026)
python src/scripts/dsp/import_latest_dsp.py       # Latest month disclosures only
```

---

## 🐳 2. Institutional Whale Tracker (7 Multi-Asset Funds)

Runs the multi-asset fund allocation scanner and single-name cross-ownership conviction index:
```bash
python src/scripts/market/whale_tracker.py
```

### Tracked Multi-Asset Funds Matrix:
| Scheme Code | Fund Name | History Depth |
| :--- | :--- | :--- |
| `RLMF806` | **Nippon India Multi Asset** | 57 Months (Deepest; dynamic import from 2024) |
| `152056` | **DSP Multi Asset Allocation** | 33 Months |
| `154167` | **DSP Multi Asset Omni FoF** | 3 Months |
| `152639` | **Bajaj Finserv Multi Asset** | 2 Months |
| `120821` | **Quant Multi Asset** | 2 Months |
| `120334` | **ICICI Prudential Multi Asset** | 1 Month |
| `120716` | **ICICI Prudential Multi Asset II** | 1 Month |

---

## 📈 3. MoM NAV Returns Analysis

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

---

## 🔍 4. DSP Active-Fund Conviction Signal (Single-Name Research)

DSP active-fund holdings in `market_data.mf_holdings` are the primary institutional single-name conviction signal.

### Active Funds (Meaningful Signal):
`DSP_SMALL_CAP`, `DSP_MID_CAP`, `DSP_LARGE_AND_MID_CAP`, `DSP_FLEXI_CAP`, `DSP_MULTICAP`, `DSP_FOCUSED`, `DSP_VALUE`, `DSP_TIGER`, `DSP_BUSINESS_CYCLE`, `DSP_ELSS_TAX_SAVER`, `DSP_HEALTHCARE`, `DSP_BANKING_FINANCIAL_SERVICES`, `DSP_QUANT`.

### Passive Funds (Ignore — Index Tracking):
`DSP_NIFTY_*_INDEX`, `DSP_NIFTY_*_ETF`, `DSP_BSE_*_ETF`, `DSP_*_QUALITY_50_INDEX`.

### ClickHouse Query Pattern for Active DSP Cross-Ownership:
```sql
SELECT 
    security_name,
    isin,
    count(DISTINCT fund_name) AS active_dsp_funds,
    sum(market_value_cr) AS total_market_val_cr,
    max(as_of_month) AS latest_month
FROM market_data.mf_holdings FINAL
WHERE fund_name LIKE 'DSP%'
  AND fund_name NOT LIKE '%INDEX%'
  AND fund_name NOT LIKE '%ETF%'
GROUP BY security_name, isin
ORDER BY total_market_val_cr DESC
LIMIT 20;
```

---

## ⚡ 5. Ultra-Fast Pre-Aggregated Queries (<100ms)

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

## 📊 6. AMFI Category Flows & Sector AUM Tracking

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
