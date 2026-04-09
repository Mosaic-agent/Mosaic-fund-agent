# Skill: ETF-Signals-XLSX

Exports ETF signals, FII/DII flows, momentum data, ML predictions, and buy/sell recommendations to a formatted Excel workbook.

## Trigger

Use this skill when the user asks:
- "Export ETF signals to Excel"
- "Create an Excel sheet with today's analysis"
- "Save the buy/sell table to a spreadsheet"
- "Give me the FII/DII data in Excel"

## Workbook Structure

### Sheet 1: ETF Signals
Columns: Symbol | Last Price | 1d% | 5d% | 20d% | Macro Score | ETF News Sentiment | NAV Spread% | Action | Conviction

### Sheet 2: FII_DII_Flows
Columns: Date | FII Net (Cr) | DII Net (Cr) | Net (Cr) | FII Signal | DII Signal
- Last 20 trading days
- Conditional formatting: red for FII sell days, green for buy days
- 5-day and 20-day cumulative totals at bottom

### Sheet 3: ML_Predictions
Columns: As Of | Horizon | Expected Return% | Confidence Low | Confidence High | Regime Signal | CV R² | Hit Ratio | GOLDBEES Close

### Sheet 4: Macro_Themes
Columns: Theme | Conviction | Events Detected | GOLDBEES Impact | NIFTYBEES Impact | BANKBEES Impact | ITBEES Impact

### Sheet 5: Buy_Sell_Summary
The final actionable table: Symbol | Action | Conviction | Key Reason | Entry Zone | Risk

## How to Generate

```python
import clickhouse_connect, openpyxl
from config.settings import settings

# Connect to ClickHouse and pull data
client = clickhouse_connect.get_client(
    host=settings.clickhouse_host, port=settings.clickhouse_port,
    username=settings.clickhouse_user, password=settings.clickhouse_password,
)

# Query ETF momentum, FII/DII flows, ML predictions
# Use openpyxl to build the workbook with color coding
# Blue = hardcoded inputs, Black = formulas, Green = cross-sheet links
```

## File Naming

```
output/etf_signals_YYYYMMDD.xlsx
```

## Color Coding (Industry Standard)

- Blue text (0,0,255): Hardcoded inputs
- Black text: Formulas
- Green background: BUY signals
- Red background: SELL signals
- Yellow background: HOLD / Watch

## Dependencies

- `openpyxl` (already in requirements.txt)
- `anthropic-skills:xlsx` skill loaded
- ClickHouse running

## Source Data

- `market_data.daily_prices` — ETF OHLCV
- `market_data.fii_dii_flows` — FII/DII daily flows
- `market_data.ml_predictions` — LightGBM forecasts
- `market_data.inav_snapshots` — NAV premium/discount
