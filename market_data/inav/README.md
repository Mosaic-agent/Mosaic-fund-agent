# iNAV CSV Store

Local flat-file store for indicative NAV (iNAV) snapshots.

## Layout

```
market_data/inav/
├── {SYMBOL}/
│   ├── {YYYY}/
│   │   ├── {MM}/
│   │   │   ├── {DD}.csv
│   │   │   └── ...
```

Example:

```
market_data/inav/GOLDBEES/2026/08/04.csv
market_data/inav/SILVERBEES/2026/08/04.csv
```

## File schema

Each CSV has this header (matches `market_data.inav_snapshots` in ClickHouse):

```
snapshot_at,inav,market_price,premium_discount_pct,source
```

- `snapshot_at`: UTC ISO-8601 timestamp of the fetch
- `inav`: indicative NAV in ₹
- `market_price`: last traded price in ₹
- `premium_discount_pct`: `(market_price − inav) / inav × 100`
- `source`: `NSE` | `Yahoo` | `Zerodha` | `Mirae` | `Motilal` | `Nippon`

## Usage

```python
from data_importer.inav_csv import write_inav_rows, read_inav

# Write
rows = [
    {"symbol": "GOLDBEES", "snapshot_at": "2026-08-04T09:15:00",
     "inav": 82.15, "market_price": 82.30, "premium_discount_pct": 0.18, "source": "NSE"},
]
write_inav_rows(rows)

# Read one symbol between dates
df = read_inav("GOLDBEES", start="2026-08-01", end="2026-08-31")
```
