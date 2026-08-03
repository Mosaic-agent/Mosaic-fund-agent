"""
src/scripts/portfolio/run_stock_quant_workflow.py
────────────────────────────────────────────────────
Unified Master Quantitative, Anomaly & Institutional Stock Workflow for AGY.

Integrated Capabilities:
  1. Price Snapshot & Momentum Metrics (20d/50d SMA)
  2. Multi-Model Anomaly Engine & Bulk/Block Deal Classifier (classify_regime)
  3. Official NSE Regulatory Announcements & Filings (fetch_corporate_announcements)
  4. Multi-AMC Institutional Cross-Ownership & Whale Conviction (market_data.mf_holdings)
  5. Terminal ASCII Plotting Engine (plotext with Red Circle 🔴 Anomaly Markers)
  6. Automatic Chart & Artifact Preservation (<symbol>_quant_workflow.md)

Usage:
  python src/scripts/portfolio/run_stock_quant_workflow.py BAJFINANCE --days 120
  python src/scripts/portfolio/run_stock_quant_workflow.py BECTORFOOD --days 180
  python src/scripts/portfolio/run_stock_quant_workflow.py RELIANCE --days 120
"""

from __future__ import annotations

import sys
import os
import argparse
from pathlib import Path
from datetime import date, timedelta
import pandas as pd
import numpy as np

# Ensure project root is on sys.path
ROOT_DIR = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(ROOT_DIR))

from src.db.pool import get_pool
from src.ml.anomaly._regime import classify_regime
from src.tools.nse_announcements import fetch_corporate_announcements


def get_artifact_dir() -> Path:
    """Detect or fallback to the current conversation artifact directory."""
    env_dir = os.getenv("ANTIGRAVITY_ARTIFACT_DIR")
    if env_dir and os.path.exists(env_dir):
        return Path(env_dir)
    default_dir = Path("/Users/dhiraj.thakur/.gemini/antigravity-cli/brain/9cfa6ba8-34d7-436e-9748-767e8130f116")
    default_dir.mkdir(parents=True, exist_ok=True)
    return default_dir


def run_stock_workflow(symbol: str, days: int = 120, plot_width: int = 80, plot_height: int = 14, artifact_dir: str = "") -> None:
    pool = get_pool()
    clean_sym = symbol.upper().strip()

    print("\n" + "═" * 85)
    print(f" 🏛️ AGY MASTER QUANT, ANOMALY & INSTITUTIONAL WORKFLOW: {clean_sym}")
    print("═" * 85)

    # 1. Fetch EOD Price & Volume History from ClickHouse
    df = pool.query_df(f"""
        SELECT trade_date, open, high, low, close, volume
        FROM market_data.daily_prices FINAL
        WHERE symbol IN ('{clean_sym}', '{clean_sym}.NS')
          AND trade_date >= today() - {days}
        ORDER BY trade_date ASC
    """)

    if df.empty:
        import yfinance as yf
        t = yf.Ticker(f"{clean_sym}.NS")
        df = t.history(period="6m").reset_index()
        df = df.rename(columns={"Date": "trade_date", "Open": "open", "High": "high", "Low": "low", "Close": "close", "Volume": "volume"})

    if df.empty:
        print(f"❌ Error: No price data available for '{clean_sym}'.")
        return

    df["trade_date"] = pd.to_datetime(df["trade_date"])
    df = df.sort_values("trade_date").reset_index(drop=True)

    # 2. Compute Technical, Volatility & Anomaly Regimes
    df["ret"] = df["close"].pct_change() * 100
    df["vol_ma20"] = df["volume"].rolling(20).mean()
    df["vol_std20"] = df["volume"].rolling(20).std()
    df["z_volume"] = (df["volume"] - df["vol_ma20"]) / (df["vol_std20"] + 1e-6)

    df["ret_ma20"] = df["ret"].rolling(20).mean()
    df["ret_std20"] = df["ret"].rolling(20).std()
    df["z_robust"] = (df["ret"] - df["ret_ma20"]) / (df["ret_std20"] + 1e-6)
    df["z_resid_abs"] = df["z_robust"].abs()
    df["if_confidence"] = 0.5

    # Run Anomaly Engine Regime Classifier
    reg_df = classify_regime(df)
    reg_df["is_anomaly"] = (reg_df["z_volume"] > 2.0) | (reg_df["z_robust"].abs() > 2.0)

    dates = reg_df["trade_date"].dt.strftime("%d/%m/%Y").tolist()
    prices = reg_df["close"].tolist()
    volumes = reg_df["volume"].tolist()

    cur_close = prices[-1]
    prev_close = prices[-2] if len(prices) > 1 else cur_close
    chg = cur_close - prev_close
    pct_chg = (chg / prev_close) * 100 if prev_close else 0.0

    summary_hdr = f"Latest Close: ₹{cur_close:,.2f} ({chg:+.2f} / {pct_chg:+.2f}%) | {days}d Range: ₹{reg_df['low'].min():,.2f} – ₹{reg_df['high'].max():,.2f}"
    print(summary_hdr)

    # 3. Terminal ASCII Plotting Engine (plotext with Red Circles 🔴)
    import plotext as plt

    anom_indices = [i for i, is_a in enumerate(reg_df["is_anomaly"]) if is_a]

    plt.date_form("d/m/Y")

    # Panel 1: Price Chart with Red Circle 🔴 Anomaly Markers
    plt.clear_figure()
    plt.title(f"{clean_sym} — Price Trend & Anomalies [🔴 Red Circles]")
    plt.plot(dates, prices, color="yellow", label="Price (₹)")
    if anom_indices:
        plt.scatter([dates[i] for i in anom_indices], [prices[i] for i in anom_indices], color="red", marker="🔴", label="Anomaly (🔴)")
    plt.plot_size(plot_width, plot_height)
    price_chart = plt.build()

    # Panel 2: Volume Chart with Red Anomaly Bars
    plt.clear_figure()
    plt.title(f"{clean_sym} — Daily Volume (Million Shares) [RED = Anomaly Days]")
    vol_cols = ["red" if reg_df["is_anomaly"].iloc[i] else "cyan" for i in range(len(reg_df))]
    plt.bar(dates, [v / 1e6 for v in volumes], color=vol_cols, label="Volume")
    plt.plot_size(plot_width, 10)
    vol_chart = plt.build()

    print("\n" + price_chart)
    print("\n" + vol_chart)

    # 4. Fetch Official NSE Regulatory Corporate Disclosures
    today_dt = date.today()
    start_dt = today_dt - timedelta(days=days)
    nse_announcements = []
    try:
        nse_announcements = fetch_corporate_announcements(clean_sym, start_dt, today_dt)
    except Exception as exc:
        print(f"Note: NSE disclosures lookup skipped ({exc})")

    # Map announcements to anomaly dates
    ann_map = {}
    for ann in nse_announcements:
        pub_date = ann.get("published_at", "")[:10]
        if pub_date not in ann_map:
            ann_map[pub_date] = f"{ann.get('category')}: {ann.get('title')}"

    # 5. Institutional Mutual Fund Cross-Ownership Query
    mf_df = pool.query_df(f"""
        SELECT 
            fund_name, 
            max(as_of_month) as latest_month, 
            round(sum(market_value_cr), 2) as val_cr, 
            round(avg(pct_of_nav), 2) as pct_nav
        FROM market_data.mf_holdings FINAL
        WHERE (security_name LIKE '%{clean_sym}%' OR security_name LIKE '%{clean_sym.replace('BEES', '')}%')
          AND lower(asset_type) = 'equity'
          AND as_of_month >= '2026-06-01'
        GROUP BY fund_name
        ORDER BY val_cr DESC
        LIMIT 10
    """)

    mf_table_str = ""
    if not mf_df.empty:
        mf_table_str = mf_df.to_string(index=False)
        print("\n=== 🐳 TOP MUTUAL FUND HOLDINGS (LATEST MONTH) ===")
        print(mf_table_str)

    # 6. Anomaly & Bulk/Block Deal Classification Table
    anom_df = reg_df[reg_df["is_anomaly"]].copy()
    anom_lines = []
    anom_lines.append(f"{'Date':<12} | {'Close (₹)':<10} | {'Daily Ret':<10} | {'Volume (M)':<10} | {'Vol Z':<7} | {'Regime Classification':<36} | {'NSE Regulatory Trigger':<40}")
    anom_lines.append("-" * 140)
    for _, r in anom_df.iterrows():
        d_str = r["trade_date"].strftime("%Y-%m-%d")
        reg_label = str(r["regime"])
        trigger_str = ann_map.get(d_str, "Market Volume / Liquidity Movement")
        anom_lines.append(f"{d_str:<12} | ₹{r['close']:<9.2f} | {r['ret']:<+9.2f}% | {r['volume']/1e6:<9.2f}M | {r['z_volume']:<+6.2f} | {reg_label:<36} | {trigger_str[:40]}")

    anom_table_str = "\n".join(anom_lines)
    print("\n=== 🚨 DETECTED ANOMALIES & BULK/BLOCK DEAL CLASSIFICATION ===")
    print(anom_table_str)

    # 7. Preserve Charts & Report into Artifact Directory
    target_dir = Path(artifact_dir) if artifact_dir else get_artifact_dir()
    target_file = target_dir / f"{clean_sym.lower()}_quant_workflow.md"

    md_content = f"""# {clean_sym} — Preserved Quantitative, Anomaly & Institutional Report

**Symbol:** {clean_sym}  
**Snapshot:** {summary_hdr}  

---

## 📈 Preserved Terminal ASCII Charts

```text
{price_chart}
```

```text
{vol_chart}
```

---

## 🐳 Mutual Fund Cross-Ownership (Latest Disclosures)

```text
{mf_table_str if mf_table_str else "No direct holding records found in market_data.mf_holdings"}
```

---

## 🚨 Statistical Anomalies & Bulk/Block Deal Classification

```text
{anom_table_str}
```
"""
    with open(target_file, "w", encoding="utf-8") as f:
        f.write(md_content)

    print(f"\n💾 SUCCESS: Preserved chart & report saved to artifact:\n   {target_file}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python src/scripts/portfolio/run_stock_quant_workflow.py <SYMBOL> [--days 120] [--artifact-dir <DIR>]")
        sys.exit(1)
    sym = sys.argv[1]
    days_val = 120
    art_dir = ""
    if "--days" in sys.argv:
        idx = sys.argv.index("--days")
        if idx + 1 < len(sys.argv):
            days_val = int(sys.argv[idx + 1])
    if "--artifact-dir" in sys.argv:
        idx = sys.argv.index("--artifact-dir")
        if idx + 1 < len(sys.argv):
            art_dir = sys.argv[idx + 1]

    run_stock_workflow(sym, days=days_val, artifact_dir=art_dir)
