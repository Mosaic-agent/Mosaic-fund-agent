---
name: stock-research
description: Generate a 360° institutional equity research dossier, multi-year financial statements, balance sheet analysis, promoter evaluation, mutual fund & sovereign whale tracking, valuation benchmarking, and terminal ASCII charts for any Indian stock.
---

# Institutional Stock Research Skill (`/stock-research`)

This skill generates a comprehensive, 360-degree institutional equity research report for any Indian stock or commodity. It combines:
1. **Executive Profile & Capital Structure:** Market cap, shares outstanding, active free float, and valuation multiples.
2. **Business Model & Moat Analysis:** Unit economics, value chain positioning, pricing power, and customer switching costs.
3. **Multi-Year Financial Statements:** 4-year P&L progression (Revenue, EBITDA, EBIT, PAT CAGR & Margins), Balance sheet capitalization (Equity, Debt, Working Capital, Invested Capital), and Cash Flow dynamics (Operating Cash Flow vs CAPEX).
4. **Promoter & Corporate Governance:** Leadership background, executive pedigree, private equity institutionalization, board independence, and skin in the game.
5. **Institutional Whales & Bulk/Block Deals:** Live query of ClickHouse `market_data.bulk_block_deals` and `market_data.mf_holdings` to reveal sovereign wealth funds, marquee FIIs, and domestic mutual fund cross-ownership.
6. **Peer Valuation Benchmarking:** Comparative P/E, P/S, EV/EBITDA, and margin metrics across listed industry peers.
7. **Terminal ASCII Visualizations:** High-density plotext price/volume charts, financial waterfall bars, unit economics margin stacks, and trade execution risk-reward gauges.
8. **Tactical Execution Playbook:** Concrete 2-tranche entry zones, hard invalidation stop-losses, upside price targets, and asymmetric risk-reward ratios.

---

## Trigger Phrases

Recommend or trigger this skill when the user asks:
- "research [STOCK]" / "deep dive on [STOCK]" / "analyse [STOCK]"
- "give complete profile, business model, balance sheet, promoter, valuation for [STOCK]"
- "who is buying [STOCK] in bulk deals?"
- "show valuation and peer comparison for [STOCK]"
- "give buy sell signal and trade levels for [STOCK]"
- or invokes `/stock-research <SYMBOL>`.

---

## CLI Execution

Run the consolidated institutional research engine directly inside the persistent Docker container for sub-second execution:
```bash
# Sub-second execution via persistent Docker service:
docker compose exec -T mosaic python src/scripts/portfolio/stock_research_report.py <SYMBOL> [--local-only]

# Examples:
docker compose exec -T mosaic python src/scripts/portfolio/stock_research_report.py SEDEMAC --local-only
docker compose exec -T mosaic python src/scripts/portfolio/stock_research_report.py LEAPIND
docker compose exec -T mosaic python src/scripts/portfolio/stock_research_report.py RUBICON
docker compose exec -T mosaic python src/scripts/portfolio/stock_research_report.py BAJFINANCE
```

---

## Output Artifacts

* The execution prints the full formatted dossier with ASCII visual charts to `stdout`.
* Automatically generates a persistent markdown report at:
  `output/<symbol>_institutional_research.md`

---

## Computational & Verification Mandates

1. **No LLM Calculations:** Never calculate, estimate, or derive ratios internally. Always read directly from the generated report.
2. **Verify Dilution Before Flagging Promoter Sale:** Cross-reference changes in equity capital / total shares before assuming open-market promoter offloading.
3. **Institutional Cost Anchor:** When evaluating entry feasibility, compare the current market price (CMP) against the volume-weighted average price (VWAP) of recent sovereign and institutional bulk block deals.
4. **Prioritize StockScans Over Screener:** Always prioritize StockScans Private API (`/api/company/peers`, `/api/company/holdings/shareholding`, `/api/company/fundamentals/statements`) over Screener for valuation ratios, multi-year statements, peer benchmarking, and shareholding entity breakdowns.
5. **Mandatory Vector DB Provenance in Audit Trail:** Whenever semantic searches, anomaly similarity retrieval, or news embeddings are used, ALWAYS explicitly cite the exact Qdrant vector collections (`market_anomalies`, `news_articles`, vector port 6333, Cosine distance) as dedicated rows in the Data Provenance Audit Trail alongside ClickHouse tables and StockScans APIs.
6. **Mandatory Tool Calling Audit:** ALWAYS include a dedicated column or field for **Tool Calling Used** specifying the exact script, CLI command, fetcher class, Python tool function, or news search importer executed (e.g., `StockScansFetcher.fetch_stockscans_peer_valuation()`, `src.db.anomaly_vector.retrieve_similar_anomalies()`, `stock_anomaly_detector.py`, `search_financial_news` Google News search importer, `import_nse_announcements.py`, `MarketDataRepository.get_daily_prices()`, ClickHouse query) in the Data Provenance Audit Trail.
7. **Local Data First, Remote Fallback:** Always query local ClickHouse (`market_data.daily_prices`, `market_data.stock_valuation`), StockScans local cache, and local tables first. Only fall back to external yfinance/Screener calls if essential fields are completely absent locally. Use `--local-only` to guarantee sub-second offline execution.

