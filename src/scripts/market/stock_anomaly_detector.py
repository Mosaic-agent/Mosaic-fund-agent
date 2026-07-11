import sys
import argparse
from pathlib import Path
from datetime import date, timedelta
import pandas as pd
import numpy as np

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent))

from src.tools.market.equity import search_anomaly_events
from src.ml.anomaly import run_composite_anomaly
from src.ml.correlation.service import CorrelationService
from src.scripts.market.ou_regime_backtest import _load_from_clickhouse

def draw_ascii_chart(df: pd.DataFrame, symbol: str, anomaly_dates: set, width: int = 80, height: int = 15) -> str:
    df = df.sort_values("date").reset_index(drop=True)
    n_rows = len(df)
    if n_rows == 0:
        return "No data to plot"
        
    indices = np.linspace(0, n_rows - 1, width, dtype=int)
    sampled_dates = df["date"].iloc[indices].tolist()
    sampled_vals = df["price"].iloc[indices].tolist()
    
    min_val = min(sampled_vals)
    max_val = max(sampled_vals)
    val_range = max_val - min_val if max_val != min_val else 1.0
    
    grid = [[" " for _ in range(width)] for _ in range(height)]
    
    for col_idx, val in enumerate(sampled_vals):
        row_idx = int(((val - min_val) / val_range) * (height - 1))
        row_idx = max(0, min(height - 1, row_idx))
        
        dt_str = sampled_dates[col_idx].strftime("%Y-%m-%d")
        if dt_str in anomaly_dates:
            row_df = df[df["date"] == sampled_dates[col_idx]]
            ret = 0.0
            if not row_df.empty:
                idx = row_df.index[0]
                if idx > 0:
                    ret = (df.loc[idx, "price"] / df.loc[idx-1, "price"] - 1.0) * 100.0
            marker = "▲" if ret >= 0 else "▼"
            grid[height - 1 - row_idx][col_idx] = marker
        else:
            grid[height - 1 - row_idx][col_idx] = "●"
            
    chart_lines = []
    start_date_str = sampled_dates[0].strftime("%Y-%m-%d")
    end_date_str = sampled_dates[-1].strftime("%Y-%m-%d")
    
    chart_lines.append("=" * (width + 12))
    chart_lines.append(f" 📈 {symbol} EOD Price Trend ({start_date_str} to {end_date_str}) | ▲ Positive Shock | ▼ Negative Shock")
    chart_lines.append("=" * (width + 12))
    for r in range(height):
        cur_val = max_val - (r / (height - 1)) * val_range
        val_label = f"₹{cur_val:7.2f} | "
        row_str = "".join(grid[r])
        chart_lines.append(f"{val_label}{row_str}")
    chart_lines.append(" " * 9 + "+" + "-" * width)
    timeline_labels = f"Timeline: {start_date_str}" + " " * (width - len(start_date_str) - len(end_date_str)) + end_date_str
    chart_lines.append(" " * 10 + timeline_labels)
    chart_lines.append("=" * (width + 12))
    return "\n".join(chart_lines)

def main():
    parser = argparse.ArgumentParser(description="Generic Stock Price Anomaly & Event Correlation Detector")
    parser.add_argument("--symbol", type=str, required=True, help="NSE/BSE stock symbol (e.g. INFY, RELIANCE, TCS)")
    parser.add_argument("--days", type=int, default=180, help="Lookback window in days (default 180)")
    parser.add_argument("--z-threshold", type=float, default=5.0, help="Anomaly Z-score threshold (default 5.0)")
    parser.add_argument("--contamination", type=float, default=0.01, help="Isolation Forest contamination (default 0.01)")
    
    args = parser.parse_args()
    symbol = args.symbol.upper().strip()
    
    print(f"=== Loading Price Data for {symbol} ===")
    end_dt = date.today()
    start_dt = end_dt - timedelta(days=730) # Fetch 2 years of history for GARCH stability
    
    from src.db.pool import query_df
    try:
        sql = """
            SELECT
                toDate(trade_date) AS trade_date,
                toFloat64(argMax(open,   imported_at)) AS open,
                toFloat64(argMax(high,   imported_at)) AS high,
                toFloat64(argMax(low,    imported_at)) AS low,
                toFloat64(argMax(close,  imported_at)) AS close,
                toFloat64(argMax(volume, imported_at)) AS volume
            FROM market_data.daily_prices FINAL
            WHERE symbol = %(sym)s
              AND trade_date BETWEEN %(start)s AND %(end)s
            GROUP BY trade_date
            ORDER BY trade_date ASC
        """
        df_prices = query_df(sql, {"sym": symbol, "start": str(start_dt), "end": str(end_dt)})
    except Exception as exc:
        print(f"Warning: ClickHouse fetch failed: {exc}")
        df_prices = pd.DataFrame()
        
    if df_prices.empty:
        print("ClickHouse daily_prices empty. Falling back to yfinance...")
        try:
            import yfinance as yf
            hist = yf.Ticker(f"{symbol}.NS").history(start=start_dt, end=end_dt)
            if not hist.empty:
                df_prices = hist.reset_index()[["Date", "Open", "High", "Low", "Close", "Volume"]]
                df_prices.columns = ["trade_date", "open", "high", "low", "close", "volume"]
                df_prices["trade_date"] = pd.to_datetime(df_prices["trade_date"]).dt.date
        except Exception as exc:
            print(f"Error: yfinance fetch failed: {exc}")
            
    if df_prices is None or df_prices.empty:
        print(f"Error: Could not load price data for {symbol}.")
        return
        
    df_prices = df_prices.sort_values("trade_date").reset_index(drop=True)
    df_prices["price"] = df_prices["close"]
    df_prices["trade_date"] = pd.to_datetime(df_prices["trade_date"])
    
    print(f"Loaded {len(df_prices)} daily rows. Running GARCH composite anomaly detection...")
    
    # Run composite anomaly pipeline programmatically
    try:
        from src.ml.correlation.event_registry import EventRegistry
        registry = EventRegistry()
        df_corp = registry.load_corp_actions(symbol)
    except Exception:
        df_corp = None
        
    df_anomaly_res, df_flagged, _ = run_composite_anomaly(
        df_prices,
        df_corp_actions=df_corp,
        z_threshold=args.z_threshold,
        contamination=args.contamination,
        symbol=symbol,
        store=False
    )
    
    # Filter df_flagged to the requested lookback window
    cutoff_date = pd.to_datetime(date.today() - timedelta(days=args.days))
    df_flagged = df_flagged[pd.to_datetime(df_flagged["trade_date"]) >= cutoff_date]
    
    anomaly_dates = set(pd.to_datetime(df_flagged["trade_date"]).dt.strftime("%Y-%m-%d").tolist())
    print(f"Flagged {len(anomaly_dates)} anomalies in the last {args.days} days: {sorted(list(anomaly_dates))}")
    
    # Run correlation service
    print("Running event correlation service...")
    service = CorrelationService()
    findings = service.find_correlations(symbol, df_prices, lookback_days=max(365, args.days))
    
    # Map findings by date
    findings_by_date = {}
    for f in findings:
        dt_str = pd.to_datetime(f.anomaly_date).strftime("%Y-%m-%d")
        findings_by_date[dt_str] = f
        
    # Generate Google News search report
    print("Running Google News search agent...")
    news_report = search_anomaly_events.invoke({
        "symbol": symbol,
        "days": args.days,
        "z_threshold": args.z_threshold,
        "contamination": args.contamination
    })
    
    # Load daily prices for ASCII chart rendering
    df_chart = df_prices[pd.to_datetime(df_prices["trade_date"]) >= cutoff_date].copy()
    df_chart = df_chart.rename(columns={"trade_date": "date"})
    
    ascii_chart = ""
    if not df_chart.empty:
        ascii_chart = draw_ascii_chart(df_chart, symbol, anomaly_dates, width=80, height=15)
        
    # Build unified anomaly news table
    table_rows = []
    sorted_anom_list = sorted(list(anomaly_dates), reverse=True)
    for dt_str in sorted_anom_list:
        row_df = df_prices[df_prices["trade_date"].astype(str).str.startswith(dt_str)]
        if row_df.empty:
            continue
        idx = row_df.index[0]
        close_price = row_df.iloc[0]["price"]
        
        daily_ret = 0.0
        if idx > 0:
            daily_ret = (close_price / df_prices.loc[idx-1, "price"] - 1.0) * 100.0
            
        ret_str = f"{daily_ret:+.2f}%"
        
        # Get Z-score from flagged anomalies
        anom_row = df_flagged[df_flagged["trade_date"].astype(str).str.startswith(dt_str)]
        z_val = "N/A"
        if not anom_row.empty:
            # Check for final_z
            if "final_z" in anom_row.columns:
                z_val = f"{anom_row.iloc[0]['final_z']:+.2f}"
                
        # Look up correlation finding
        finding = findings_by_date.get(dt_str)
        if finding:
            conf_level = f"{finding.correlation_score:.1f} ({finding.confidence})"
            event_trigger = finding.event.label
            reasoning = finding.explanation.replace("\n", " ")
        else:
            conf_level = "N/A (Unattributed)"
            event_trigger = "No macro/corporate event mapped"
            reasoning = "Anomaly occurred without temporally overlapping corporate announcements or macro shocks."
            
        table_rows.append(
            f"| **{dt_str}** | ₹{close_price:.2f} | {ret_str} | {z_val} | {conf_level} | {event_trigger} | {reasoning} |"
        )
        
    table_hdr = [
        "| Anomaly Date | Close Price | Daily Return | GARCH Z-Score | Confidence Score (Level) | Event Trigger | Causal Reasoning & Correlation |",
        "| :--- | :---: | :---: | :---: | :---: | :--- | :--- |"
    ]
    table_str = "\n".join(table_hdr + table_rows)
    
    # 5. Assemble final report
    report_md = f"""# 🔬 {symbol} Price Anomaly & Event Correlation Report

This report presents a unified view of all price anomalies and their underlying macro-geopolitical, regulatory, or corporate catalysts over the last {args.days} days.

---

### 📈 EOD Price Trend & Anomaly Markers (ASCII)
`▲` represents a Positive Anomaly Shock; `▼` represents a Negative Anomaly Shock; `●` represents a normal trading day.

```text
{ascii_chart}
```

---

### 🎯 Consolidated Anomaly & News Correlation Table
This table lists all detected anomalies, matching news events, and their economic/arbitrage reasoning:

{table_str}

---

### 🔍 Detailed Date-by-Date News Correlation
{news_report}

---

### 📏 Quantitative Strength & Confidence Frameworks

To contextualize these findings, the platform evaluates the strength of both numeric and event-based relationships using the following boundaries:

#### A. Linear Correlation Strength (Cohen Convention)
* **Strong**: $|r| \\ge 0.5$ — High co-movement, clear linear dependency.
* **Moderate**: $0.3 \\le |r| < 0.5$ — Noticeable relationship, some shared variance.
* **Weak**: $|r| < 0.3$ — Minimal linear relationship.

#### B. Statistical Significance Boundaries (p-value)
* **Statistically Significant ($p < 0.05$)**: The relationship has a less than 5% probability of being a random occurrence.
* **Not Statistically Significant ($p \\ge 0.05$)**: The correlation is statistically indistinguishable from noise.

#### C. Event-Attribution Confidence Scores
* **HIGH Confidence (Score $\\ge 70.0$)**: Immediate, direct causal transmission (e.g. policy ex-date).
* **MODERATE Confidence (Score $40.0$ to $69.9$)**: Probable driver, backed by tight timing and news frequency.
* **LOW Confidence (Score $< 40.0$)**: Plausible correlation, but either lagged, indirect, or from a general macro announcement.
"""
    
    # Write report to output directory
    report_path = Path("output") / f"{symbol.lower()}_anomaly_report.md"
    report_path.write_text(report_md)
    
    print(f"\n✅ Detailed report successfully written to: {report_path}")
    print("Run this file on your host to view the full details!")

if __name__ == "__main__":
    main()
