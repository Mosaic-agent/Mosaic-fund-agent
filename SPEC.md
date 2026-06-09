# Mosaic Import Specification

## Stock and ETF Data Sources

- Before an agent imports a stock, ETF, or specific symbol, it must reuse a
  saved source selected within the previous 24 hours. If none exists, ask:
  1. Shoonya
  2. NSE
  3. yfinance
- A selected source is stored in `market_data.agent_preferences` under
  `market_import_data_source`. Selecting again refreshes the 24-hour TTL.
- Agent import tools must not choose a source when neither the request nor the
  unexpired DB preference supplies one.
- `import_symbol_data` and `run_data_engineering_importer` return
  `DATA_SOURCE_REQUIRED` when a stock/ETF import has no `data_source`.
- The CLI accepts `--source shoonya|nse|yfinance`. For stock/ETF categories, it
  reuses the unexpired DB preference or prompts for choices 1-3.
- The selected source controls price fetching and the price watermark namespace.
