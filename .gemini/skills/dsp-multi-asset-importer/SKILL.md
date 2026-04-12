---
name: dsp-multi-asset-importer
description: Backfill and validate historical holdings for DSP Multi Asset Allocation Fund (scheme 152056) into market_data.mf_holdings. Use when user asks to import DSP data, check DSP holdings, re-run the backfill, validate month coverage, or fix missing months.
---

# Skill: DSP Multi Asset Importer

Manages the 31-month historical backfill (Sep 2023 – Mar 2026) of DSP Multi Asset Allocation Fund holdings into ClickHouse `market_data.mf_holdings`.

## Trigger

Use this skill when the user asks:
- "Import DSP Multi Asset history"
- "Run the DSP backfill"
- "Check if DSP data is complete"
- "How many months of DSP holdings do we have?"
- "Which DSP months are missing?"
- "Validate DSP holdings data"
- "Re-import a specific DSP month"
- "What is DSP holding in gold / equity / bonds?"

---

## Commands

### Validate parsing only (no DB write)
```bash
# Single month — fast smoke test
python scripts/import_dsp_history.py --test

# All 31 months — full dry-run
python scripts/import_dsp_history.py --dry-run
```

### Full import (writes to ClickHouse)
```bash
python scripts/import_dsp_history.py
```

---

## Validation Queries

### Coverage check — how many months are loaded
```sql
SELECT
    as_of_month,
    count() AS n_holdings,
    round(sum(pct_of_nav), 1) AS pct_total,
    round(sum(market_value_cr), 0) AS aum_cr
FROM market_data.mf_holdings FINAL
WHERE fund_name = 'DSP_MULTI_ASSET'
GROUP BY as_of_month
ORDER BY as_of_month;
```

Expected: 31 rows (2023-09-30 → 2026-03-31). Holdings grow from ~23 (new fund) to ~91 over time.

### Asset allocation as of latest month
```sql
SELECT
    asset_type,
    count() AS n,
    round(sum(pct_of_nav), 2) AS weight_pct
FROM market_data.mf_holdings FINAL
WHERE fund_name = 'DSP_MULTI_ASSET'
  AND as_of_month = (SELECT max(as_of_month) FROM market_data.mf_holdings FINAL WHERE fund_name = 'DSP_MULTI_ASSET')
GROUP BY asset_type
ORDER BY weight_pct DESC;
```

### Month-over-month allocation shift (gold trend)
```sql
SELECT
    as_of_month,
    sumIf(pct_of_nav, asset_type = 'gold')    AS gold_pct,
    sumIf(pct_of_nav, asset_type = 'equity')  AS equity_pct,
    sumIf(pct_of_nav, asset_type = 'bond')    AS bond_pct,
    sumIf(pct_of_nav, asset_type = 'cash')    AS cash_pct
FROM market_data.mf_holdings FINAL
WHERE fund_name = 'DSP_MULTI_ASSET'
GROUP BY as_of_month
ORDER BY as_of_month;
```

### Top holdings by weight (latest month)
```sql
SELECT security_name, isin, asset_type, pct_of_nav, market_value_cr
FROM market_data.mf_holdings FINAL
WHERE fund_name = 'DSP_MULTI_ASSET'
  AND as_of_month = (SELECT max(as_of_month) FROM market_data.mf_holdings FINAL WHERE fund_name = 'DSP_MULTI_ASSET')
ORDER BY pct_of_nav DESC
LIMIT 15;
```

---

## Data Model

| Field | Detail |
|---|---|
| `scheme_code` | `152056` — AMFI code for DSP Multi Asset Allocation Fund (Direct) |
| `fund_name` | `DSP_MULTI_ASSET` |
| `as_of_month` | Month-end date — first available is `2023-09-30` (NFO date) |
| `isin` | 12-char ISIN; commodities use `GOLD_ETCD_DSP` / `SILVER_ETCD_DSP` |
| `asset_type` | `equity` / `bond` / `gold` / `cash` / `other` |
| `market_value_cr` | Market value in ₹ Crores (source is Rs. Lakhs ÷ 100) |
| `pct_of_nav` | % of NAV in percentage-point form (e.g. 4.41, not 0.0441) |

## Why pct_sum < 100%

TREPS (Reverse Repo) and Net Receivables/Payables have no ISIN and are intentionally excluded. Expected pct_sum per month:
- 2023-09: ~55% (65% in TREPS — new fund)
- 2024+: 90–97% (fully deployed)

## Source

Script: `scripts/import_dsp_history.py`
Plan: `plan.md`
