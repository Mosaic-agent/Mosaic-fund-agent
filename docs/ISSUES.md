# Project Issues & Feature Requests

## [ISSUE-001] Track Mutual Fund Inflow Status (Open/Blocked)

**Status:** Proposed
**Priority:** Medium
**Created:** 2026-05-03

### Problem
Indian Mutual Funds and ETFs that invest in international securities (e.g., Mirae Asset NYSE FANG+ ETF, Motilal Oswal Nasdaq 100 ETF) frequently reach SEBI-mandated investment limits ($7B industry-wide / $1B per AMC). When this happens, AMCs block "Fresh Inflows" (lumpsum subscriptions) while potentially allowing SIPs to continue.

The `ofin-agent` pipeline might recommend a "BUY" for an ETF like `MAFANG`, but the user might find it blocked in their brokerage (Zerodha/Kite).

### Proposed Solution

1.  **Registry Update:** Modify `src/importer/registry.py` to include an `inflow_status` map for international ETFs.
    ```python
    MF_INFLOW_STATUS = {
        "MAFANG": "BLOCKED",
        "MON100": "SIP_ONLY",
        "MASPTOP50": "OPEN",
    }
    ```

2.  **Schema Change:** Add `inflow_status` to the `market_data.etf_metadata` table in ClickHouse.

3.  **UI/CLI Integration:** 
    - Update `src/agents/signal_aggregator.py` to check this status.
    - Display a warning in `run_pipeline` output: 
      `⚠️  WARNING: MAFANG is currently BLOCKED for fresh inflows.`

4.  **Automation:** Implement a scraper for AMC "Notice to Unitholders" or check if `mfapi.in` or `nseindia.com` provides a "Suspended" flag for these symbols.

---
