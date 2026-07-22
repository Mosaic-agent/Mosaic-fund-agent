---
name: mf-tracker
description: Track Indian Mutual Fund (MF) portfolio disclosures, DSP active-fund cross-ownership conviction signals, and multi-asset institutional Whale Tracking across 7 multi-asset allocation funds. Trigger when the user asks "track mf holdings", "whale tracker", "mf whale tracker", "dsp holdings", "fund holdings", or invokes /mf-tracker.
---

# Mutual Fund & Institutional Whale Tracker (`/mf-tracker`)

This skill provides institutional-grade research, portfolio disclosure tracking, and multi-asset Whale Tracking over ClickHouse table `market_data.mf_holdings`.

---

## 🚀 Quick Execution Commands

### 1. Run Institutional Whale Tracker (7 Multi-Asset Funds)
Runs the multi-asset fund allocation scanner and single-name cross-ownership conviction index:
```bash
python src/scripts/market/whale_tracker.py
```

### 2. Import AMC Holdings
Imports disclosures for specific AMCs or all registered funds:
```bash
python src/main.py import --category dsp        # DSP equity & multi-asset
python src/main.py import --category nippon     # Nippon India multi-asset
python src/main.py import --category quant      # Quant active funds
python src/main.py import --category icici      # ICICI Prudential
python src/scripts/fund_imports/run.py all      # Run all registered AMC importers
```

### 3. Query DSP Active-Fund Conviction Signal (Single-Name Research)
DSP active-fund holdings are the primary institutional single-name conviction marker:
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

### 4. Fast Aggregate Totals (<100ms Query)
Query the pre-aggregated Materialized View for ultra-fast holding counts and totals:
```sql
SELECT * 
FROM market_data.mf_holding_summaries FINAL 
WHERE security_name LIKE '%THANGAMAYIL%'
ORDER BY as_of_month DESC;
```

---

## 🐳 Tracked Multi-Asset "Whale" Funds

| Scheme Code | Fund Name | Disclosed History |
| :--- | :--- | :--- |
| `RLMF806` | **Nippon India Multi Asset** | 57 Months (Deepest) |
| `152056` | **DSP Multi Asset Allocation** | 33 Months |
| `154167` | **DSP Multi Asset Omni FoF** | 3 Months |
| `152639` | **Bajaj Finserv Multi Asset** | 2 Months |
| `120821` | **Quant Multi Asset** | 2 Months |
| `120334` | **ICICI Prudential Multi Asset** | 1 Month |
| `120716` | **ICICI Prudential Multi Asset II** | 1 Month |

---

## 📌 Rules & Domain Guidance

1. **Verify Dilution Before Flagging Promoter Sale:** A drop in promoter % is **not** a sell signal if total shares outstanding expanded (QIP, preferential allotment, bonus, ESOP). Always cross-reference shareholding % with total equity capital before drawing conclusions.
2. **DSP Active Funds = Primary Conviction:** Cross-ownership across 2+ active DSP funds for 24+ months is the strongest single-name long-term conviction signal in Mosaic.
3. **No LLM Math:** Never compute percentages, CAGR, or totals in internal reasoning — always read pre-computed output rows from ClickHouse SQL or script outputs verbatim.
