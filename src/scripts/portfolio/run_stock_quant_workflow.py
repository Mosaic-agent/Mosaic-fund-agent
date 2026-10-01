"""
src/scripts/portfolio/run_stock_quant_workflow.py
────────────────────────────────────────────────────
Unified Master Quantitative, Anomaly & Institutional Stock Workflow for AGY.

Integrated Capabilities:
  1. Price Snapshot & Momentum Metrics (20d/50d SMA)
  2. Multi-Model Anomaly Engine & Bulk/Block Deal Classifier (classify_regime)
  3. Official NSE Regulatory Announcements & Filings (fetch_corporate_announcements)
  4. Multi-AMC Institutional Cross-Ownership & Whale Conviction (market_data.mf_holdings),
     keyed on ISIN (resolved via amfi_market_cap, then security_symbol_map) and
     restricted to ACTIVE funds -- index / ETF / arbitrage funds are excluded.
  5. Terminal ASCII Plotting Engine (plotext with Red Circle 🔴 Anomaly Markers)
  6. Automatic Chart & Artifact Preservation (<symbol>_quant_workflow.md)

Usage:
  python src/scripts/portfolio/run_stock_quant_workflow.py BAJFINANCE --days 120
  python src/scripts/portfolio/run_stock_quant_workflow.py BECTORFOOD --days 180
  python src/scripts/portfolio/run_stock_quant_workflow.py RELIANCE --days 120

Flags are scanned positionally, so they must be space-separated
(`--days 180`, never `--days=180`).

Reports default to /app/output in the container (<repo>/output on the host);
override with --artifact-dir or $ANTIGRAVITY_ARTIFACT_DIR.

Price authority: EOD comes from market_data.daily_prices (NSE/Shoonya). Only
61 stocks are covered locally; for anything else the script falls back to
yfinance and prints a loud UNVERIFIED banner. Import the symbol rather than
trusting that fallback.
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
from src.data_importer.tool_fetchers.shoonya_tools import fetch_and_calculate_obi, format_order_book_ascii



def get_artifact_dir() -> Path:
    """Resolve the report output directory.

    Order: ANTIGRAVITY_ARTIFACT_DIR env -> /app/output (bind-mounted into the
    mosaic container) -> <repo>/output.

    The previous default was a hardcoded ~/.gemini/antigravity-cli path. Only
    src/ and output/ are bind-mounted, so that path was created *inside* the
    container and every report was silently lost on teardown while the script
    printed a host-looking path and "SUCCESS".
    """
    env_dir = os.getenv("ANTIGRAVITY_ARTIFACT_DIR")
    if env_dir and os.path.exists(env_dir):
        return Path(env_dir)
    for cand in (Path("/app/output"), ROOT_DIR / "output"):
        if cand.exists():
            return cand
    fallback = ROOT_DIR / "output"
    fallback.mkdir(parents=True, exist_ok=True)
    return fallback


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

    price_source = "market_data.daily_prices FINAL (NSE/Shoonya)"

    if df.empty:
        price_source = "yfinance / Yahoo — UNVERIFIED"
        print(
            "\n" + "!" * 85
            + f"\n⚠️  PRICE AUTHORITY WARNING — '{clean_sym}' has NO rows in"
              " market_data.daily_prices.\n"
              "    Falling back to yfinance (Yahoo). This BREAKS the NSE/Shoonya\n"
              "    price-authority rule. Yahoo OHLC for Indian small caps is routinely\n"
              "    stale, split-unadjusted or simply wrong, and unadjusted splits show\n"
              "    up as fake 80% drawdowns.\n"
              "    Treat EVERY number below as UNVERIFIED. Import the symbol first:\n"
              f"       ./mosaic.sh import --category stocks --source nse\n"
            + "!" * 85 + "\n"
        )
        import yfinance as yf
        start = (date.today() - timedelta(days=days)).isoformat()
        t = yf.Ticker(f"{clean_sym}.NS")
        df = t.history(start=start).reset_index()
        df = df.rename(columns={"Date": "trade_date", "Open": "open", "High": "high", "Low": "low", "Close": "close", "Volume": "volume"})

    if df.empty:
        print(f"❌ Error: No price data available for '{clean_sym}'.")
        return

    # Absent data triggers the Yahoo banner above, but STALE local data is just
    # as misleading: GODIGIT sits in daily_prices with a watermark of
    # 2026-09-19, so its last close reads as current when it is ~2 weeks old.
    _latest_bar = pd.to_datetime(df["trade_date"]).max().date()
    _lag_days = (date.today() - _latest_bar).days
    if _lag_days > 5:
        print(
            f"\n⚠️  STALE PRICE WARNING: latest bar for '{clean_sym}' is {_latest_bar}"
            f" ({_lag_days} calendar days old).\n"
            "    Every price, return and drawdown below is as-of that date, NOT today."
            "\n    Refresh before acting:  ./mosaic.sh import --category stocks --source nse\n"
        )
        price_source += f" [STALE: as-of {_latest_bar}, {_lag_days}d lag]"

    print(f"Price source: {price_source}")

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

    # 3b. Live Order Book Imbalance (OBI) & Market Microstructure via Shoonya
    obi_panel_str = ""
    try:
        obi_data = fetch_and_calculate_obi(clean_sym)
        if obi_data:
            obi_panel_str = format_order_book_ascii(obi_data)
            print("\n" + obi_panel_str)
    except Exception as exc:
        pass

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
            cat = str(ann.get("category") or "").strip()
            title = str(ann.get("title") or "").strip()
            if ":" in title:
                specific = title.split(":", 1)[1].strip()
            else:
                specific = title or cat
            if not specific or specific.lower() in ("nse_announcements", "general updates", "updates"):
                specific = cat if cat.lower() not in ("nse_announcements", "general updates", "updates") else "Official NSE Disclosure"
            ann_map[pub_date] = specific

    # 5. Institutional Mutual Fund Cross-Ownership Query (ISIN-keyed)
    #
    # NEVER match the ticker against security_name. mf_holdings stores company
    # names ("Nuvama Wealth Management Limited"), ClickHouse LIKE is
    # case-sensitive, so '%NUVAMA%' matched 0 rows for a stock held by 29
    # funds -- and the section then printed nothing at all, making heavy
    # institutional ownership indistinguishable from none. A loose
    # case-insensitive name match is worse: '%nuvama%' also pulls in bonds of
    # affiliated entities under unrelated ISINs. ISIN is the only safe key.
    isin_df = pool.query_df(f"""
        SELECT isin FROM market_data.amfi_market_cap FINAL
        WHERE nse_symbol = '{clean_sym}'
          AND period_end_date = (SELECT max(period_end_date) FROM market_data.amfi_market_cap FINAL)
        LIMIT 1
    """)
    if isin_df.empty:
        isin_df = pool.query_df(f"""
            SELECT isin FROM market_data.security_symbol_map FINAL
            WHERE symbol = '{clean_sym}' AND isin != '' LIMIT 1
        """)

    resolved_isin = str(isin_df["isin"].iloc[0]) if not isin_df.empty else ""
    # Each fund is taken at ITS OWN latest disclosed month, never at a single
    # global max(as_of_month) for the ISIN. AMC filing calendars are ragged: for
    # GODIGIT, 14 funds had filed 2026-08-31 while ICICI_MULTI_ASSET had already
    # filed 2026-10-01. Pinning to the ISIN-wide max month returned that ONE
    # fund (Rs 35.8 Cr) and silently dropped the other 16 (Rs ~1,494 Cr).
    mf_df = pd.DataFrame()
    if resolved_isin:
        mf_df = pool.query_df(f"""
            SELECT fund_name,
                   argMax(as_of_month, as_of_month) AS latest_month,
                   round(argMax(market_value_cr, as_of_month), 2) AS val_cr,
                   round(argMax(pct_of_nav, as_of_month), 2) AS pct_nav
            FROM market_data.mf_holdings FINAL
            WHERE isin = '{resolved_isin}'
              AND lower(asset_type) = 'equity'
              AND lower(fund_name) NOT LIKE '%index%'
              AND lower(fund_name) NOT LIKE '%etf%'
              AND lower(fund_name) NOT LIKE '%arbitrage%'
            GROUP BY fund_name
            ORDER BY val_cr DESC
            LIMIT 25
        """)

    print("\n=== 🐳 ACTIVE-FUND CROSS-OWNERSHIP (latest disclosed month, ISIN-keyed) ===")
    if not resolved_isin:
        mf_table_str = (
            f"[UNRESOLVED_SECURITY: symbol={clean_sym}] no ISIN found in"
            " amfi_market_cap or security_symbol_map."
            " Ownership was NOT checked -- this is a lookup gap, not an absence of holders."
        )
        print(f"⚠️  {mf_table_str}")
    elif mf_df.empty:
        mf_table_str = (
            f"ISIN {resolved_isin} resolved, but zero ACTIVE-fund equity holdings on the"
            " latest disclosed month (index / ETF / arbitrage funds excluded)."
        )
        print(f"ℹ️  {mf_table_str}")
    else:
        mf_df["latest_month"] = pd.to_datetime(mf_df["latest_month"])
        newest = mf_df["latest_month"].max()
        oldest = mf_df["latest_month"].min()
        # Mark funds whose last filing lags the newest one by more than ~2
        # months: they may have exited rather than simply not filed yet.
        mf_df["stale"] = np.where(
            (newest - mf_df["latest_month"]).dt.days > 62, "<- STALE/possible exit", ""
        )
        mf_df["latest_month"] = mf_df["latest_month"].dt.date

        header = (
            f"ISIN {resolved_isin} | {len(mf_df)} active funds |"
            f" total ₹{mf_df['val_cr'].sum():,.1f} Cr"
            f" | disclosures {oldest.date()} .. {newest.date()}"
        )
        notes = []
        if oldest != newest:
            notes.append(
                "⚠️  RAGGED DISCLOSURE: AMC filing months differ across these funds,"
                " so the total is NOT a single-date figure. Each row is that fund's"
                " own latest filing."
            )
        n_stale = int((mf_df["stale"] != "").sum())
        if n_stale:
            notes.append(
                f"⚠️  {n_stale} fund(s) last filed >62d before the newest disclosure"
                " and may have EXITED -- verify before citing as current holders."
            )
        mf_table_str = header + "\n\n" + mf_df.to_string(index=False)
        if notes:
            mf_table_str += "\n\n" + "\n".join(notes)
        print(mf_table_str)

    # 6. Anomaly & Bulk/Block Deal Classification Table
    anom_df = reg_df[reg_df["is_anomaly"]].copy()
    anom_lines = []
    anom_lines.append(f"{'Date':<12} | {'Close (₹)':<10} | {'Daily Ret':<10} | {'Volume (M)':<10} | {'Vol Z':<7} | {'Regime Classification':<36} | {'NSE Regulatory Trigger'}")
    anom_lines.append("-" * 140)
    for _, r in anom_df.iterrows():
        d_str = r["trade_date"].strftime("%Y-%m-%d")
        reg_label = str(r["regime"])
        trigger_str = ann_map.get(d_str, "Market Volume / Liquidity Movement")
        anom_lines.append(f"{d_str:<12} | ₹{r['close']:<9.2f} | {r['ret']:<+9.2f}% | {r['volume']/1e6:<9.2f}M | {r['z_volume']:<+6.2f} | {reg_label:<36} | {trigger_str}")

    anom_table_str = "\n".join(anom_lines)
    print("\n=== 🚨 DETECTED ANOMALIES & BULK/BLOCK DEAL CLASSIFICATION ===")
    print(anom_table_str)

    # 7. Preserve Charts & Report into Artifact Directory
    target_dir = Path(artifact_dir) if artifact_dir else get_artifact_dir()
    target_file = target_dir / f"{clean_sym.lower()}_quant_workflow.md"

    obi_md_section = ""
    if obi_panel_str:
        obi_md_section = f"""---

## ⚖️ Live Order Book Microstructure & Queue Depth (Shoonya OBI)

```text
{obi_panel_str}
```
"""

    md_content = f"""# {clean_sym} — Preserved Quantitative, Anomaly & Institutional Report

**Symbol:** {clean_sym}  
**Snapshot:** {summary_hdr}  
**Price source:** {price_source}  

---

## 📈 Preserved Terminal ASCII Charts

```text
{price_chart}
```

```text
{vol_chart}
```

{obi_md_section}---

## 🐳 Active-Fund Cross-Ownership (latest disclosed month, ISIN-keyed)

```text
{mf_table_str}
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
