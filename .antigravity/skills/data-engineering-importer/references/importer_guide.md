# Importer Workflow Guide

The importer is responsible for populating ClickHouse with historical and daily data.

## CLI Commands
Run the importer via the main CLI:
```bash
python -m src.main import [CATEGORIES...] [--lookback DAYS] [--full] [--dry-run]
```
- `CATEGORIES`: One or more of `stocks`, `etfs`, `commodities`, `indices`, `mf`, `nse_eod`, `inav`, `cot`, `cb_reserves`, `etf_aum`, `fx_rates`, `mf_holdings`, or `all`.
- `--lookback`: Default 3650 days (~10 years) for first-time imports.
- `--full`: Ignore watermarks and re-fetch history.
- `--dry-run`: Log what would be done without writing to ClickHouse.

## Delta-Sync Strategy
1. **Read Watermark**: Check `market_data.import_watermarks` for `(source, symbol)`.
2. **Calculate Start Date**: `from_date = watermark - 3 days` (3-day overlap to catch corrections/weekends).
3. **Fetch & Insert**: Fetch data from `from_date` to `today`.
4. **Update Watermark**: Update `import_watermarks` with the `max(trade_date)` from the inserted data.

## Adding a New Data Source
1. **Define Table**: Add DDL to `src/importer/clickhouse.py`.
2. **Implement Fetcher**: Create a new file in `src/importer/fetchers/`.
3. **Register Source**: Update `src/importer/registry.py` with new symbols or categories.
4. **Update CLI**: Wire the new fetcher into `src/importer/cli.py`.

## Data Sources & APIs
- **yfinance**: OHLCV for most instruments.
- **MFAPI.in**: Mutual fund and ETF NAVs.
- **NSE Quote API**: Live/EOD prices directly from NSE.
- **CFTC**: COT positioning data (scraped or CSV).
- **IMF**: Gold reserves via API.
- **Morningstar (mstarpy)**: MF portfolio holdings.
