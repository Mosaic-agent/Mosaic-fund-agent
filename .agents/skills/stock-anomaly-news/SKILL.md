---
name: stock-anomaly-news
description: Run a comprehensive price anomaly detection and qualitative news/event correlation report for any Indian stock or commodity.
---

# Stock Anomaly & News Correlation Skill

This skill runs the 5-step GARCH-composite anomaly detection pipeline (GARCH conditional volatility + Isolation Forest + PELT change-point detection) on any Indian stock or commodity, filters out corporate actions (splits, bonuses, mergers), and performs parallel news and event attribution searches (including semantic RAG scoring via Ollama) to explain the causes of price shocks.

## Trigger Phrases

Recommend or use this skill when the user asks:
- "run anomaly analysis on [STOCK]"
- "explain the price shocks for [STOCK]"
- "find news on [STOCK] anomaly dates"
- "correlate [STOCK] price moves with macro events"
- "investigate what happened to [STOCK] in the last year"

## CLI Execution

Run the consolidated script directly inside the persistent Docker container for sub-second execution:
```bash
# Sub-second execution via persistent Docker service:
docker compose exec -T mosaic python src/scripts/market/stock_anomaly_detector.py --symbol <SYMBOL> --days <LOOKBACK_DAYS> [--local-only]

# Or via mosaic wrapper:
./mosaic.sh src/scripts/market/stock_anomaly_detector.py --symbol <SYMBOL> --days <LOOKBACK_DAYS> [--local-only]
```

### Options:
- `--symbol` (required): NSE/BSE trading symbol (e.g., `INFY`, `RELIANCE`, `TCS`).
- `--days` (optional): Lookback window in calendar days (default `180`).
- `--local-only` (optional): Rely strictly on local ClickHouse (`market_data.news_articles`, `market_data.bulk_block_deals`, `corporate_actions`) and skip remote network scrapers/RSS feeds.
- `--z-threshold` (optional): Raw GARCH residual Z-score cutoff (default `5.0`).
- `--contamination` (optional): Isolation Forest outlier contamination rate (default `0.01`).

## Output Files & ASCII Visualizations

The script generates a comprehensive markdown report in the output directory:
- Report Path: `output/<symbol>_anomaly_report.md`
- Visual timeline plots are saved to `output/reports/<SYMBOL>_correlation_timeline.png`
- Lead-lag grids are saved to `output/reports/<SYMBOL>_lead_lag_grid.png`

### Terminal ASCII & ANSI Red Chart Integration
For interactive CLI responses or inline summary reports:
1. **Mandatory Red Anomaly Highlighting**: Statistical price/volume anomalies (GARCH residual shocks, volume institutional blocks) MUST be highlighted in **RED**:
   - In ANSI terminal charts, use bright red escape codes (`\033[91;1m▲\033[0m` / `\033[91;1m▼\033[0m`).
   - In plotext scatter overlays, pass `color="red", marker="🔴"` (or `marker="sd"`).
   - In Markdown reports, present anomaly dates inside a ````diff ```` block using `-` so all anomaly lines render in bright red across all Markdown viewers.
2. Call `plot_price_chart(symbol, days)` or `render_sparkline(prices)` from `src.tools.chart_tools` to render ASCII candlestick charts with built-in red anomaly overlay points (`🔴`).

## Framework for Interpreting Results

### 1. Statistical Correlation Strength (Cohen Convention)
* **Strong**: $|r| \ge 0.5$ — High co-movement, clear linear dependency.
* **Moderate**: $0.3 \le |r| < 0.5$ — Noticeable relationship, some shared variance.
* **Weak**: $|r| < 0.3$ — Minimal linear relationship (e.g. currency spot dependency).

### 2. Statistical Significance Boundaries (p-value)
* **Significant ($p < 0.05$)**: The relationship is statistically valid.
* **Not Significant ($p \ge 0.05$)**: The correlation cannot be distinguished from noise.

### 3. Event-Attribution Confidence Scores
* **HIGH Confidence (Score $\ge 70.0$)**: Immediate, direct causal transmission (e.g. major corporate filing or policy ex-date).
* **MODERATE Confidence (Score $40.0$ to $69.9$)**: Probable driver, backed by tight timing and news frequency.
* **LOW Confidence (Score $< 40.0$)**: Plausible correlation, but either lagged, indirect, or from a general macro announcement.

---

## Data Provenance & Vector DB Mandate
1. **Mandatory Qdrant Vector Collection Rows**: Whenever semantic searches, anomaly similarity retrieval (`retrieve_similar_anomalies`), or news embeddings are used, ALWAYS explicitly cite the exact Qdrant vector collections (`market_anomalies`, `news_articles`, vector port 6333, Cosine distance) as dedicated rows in the Data Provenance Audit Trail alongside ClickHouse tables and StockScans APIs.
2. **Prioritize StockScans Over Screener**: Always prioritize StockScans Private API (`StockScansFetcher`) over Screener for fundamental, shareholding, insider, and concall data.
3. **Mandatory Tool Calling Audit**: ALWAYS include a dedicated column or field for **Tool Calling Used** specifying the exact script, CLI command, fetcher class, Python tool function, or news search importer executed (e.g., `src.db.anomaly_vector.retrieve_similar_anomalies()`, `StockScansFetcher`, `stock_anomaly_detector.py`, `search_financial_news` Google News search importer, `import_nse_announcements.py`, ClickHouse query) in the Data Provenance Audit Trail.
4. **Local Data First, Remote Fallback:** Always query local ClickHouse tables (`market_data.news_articles`, `market_data.bulk_block_deals`, `corporate_actions`) first before initiating external network requests (e.g. Google News RSS or broker logins). If local attribution events exist, skip network calls entirely for sub-second offline execution. Use `--local-only` to strictly enforce local data attribution.
