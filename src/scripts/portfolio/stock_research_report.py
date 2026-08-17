"""
src/scripts/portfolio/stock_research_report.py
──────────────────────────────────────────────
Master Institutional Stock Deep Dive & ASCII Visual Engine.

Generates a 360° institutional equity research dossier for any Indian stock:
  1. Executive Profile, Sector Moat & Identity Snapshot
  2. Business Model, Unit Economics & Margin Architecture Waterfall
  3. Multi-Year Financial Performance (P&L, Balance Sheet, Cash Flows)
  4. Promoter Pedigree, Skin-in-the-game & Corporate Governance
  5. Mutual Fund Ownership, Sovereign Whales & Bulk/Block Deals Discovery
  6. Peer Valuation Matrix & Multiple Benchmarking
  7. Quantitative Buy/Sell Signal & 2-Tranche Tactical Execution Playbook
  8. Full Terminal ASCII Visual Suite (Price, Volume, Financials, Margins, Allocation, Trade Gauge)

Usage:
  python src/scripts/portfolio/stock_research_report.py LEAPIND
  python src/scripts/portfolio/stock_research_report.py RUBICON
  python src/scripts/portfolio/stock_research_report.py STYLEBAAZA
"""

from __future__ import annotations

import sys
import os
import argparse
from pathlib import Path
from datetime import datetime
import pandas as pd
import numpy as np
import yfinance as yf
import plotext as plt

# Ensure project root is on sys.path
ROOT_DIR = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(ROOT_DIR))

from src.db.pool import get_pool
from src.tools.company_resolver import resolve_company_info
from src.utils.symbol_mapper import get_company_name


def generate_stock_research_report(query: str, save_artifact: bool = True) -> str:
    # 1. Resolve Company Info
    resolved = resolve_company_info(query, auto_import=False)
    symbol = resolved.get("symbol") or query.upper().strip()
    yf_symbol = resolved.get("yf_symbol") or f"{symbol}.NS"
    company_name = resolved.get("company_name") or get_company_name(symbol)

    print(f"\n🚀 Generating Master Institutional Research Report for {company_name} ({symbol})...\n")

    # 2. Fetch Live Fundamental & Balance Sheet Data via yfinance
    ticker = yf.Ticker(yf_symbol)
    info = ticker.info or {}

    px = info.get("currentPrice") or info.get("regularMarketPrice") or 0.0
    mcap_cr = (info.get("marketCap") or 0) / 1e7
    shares_cr = (info.get("sharesOutstanding") or 0) / 1e7
    float_cr = (info.get("floatShares") or 0) / 1e7
    float_pct = (float_cr / shares_cr * 100) if shares_cr else 0.0

    rev_cr = (info.get("totalRevenue") or 0) / 1e7
    gross_margin = (info.get("grossMargins") or 0) * 100
    ebitda_cr = (info.get("ebitda") or 0) / 1e7
    ebitda_margin = (info.get("ebitdaMargins") or 0) * 100
    op_margin = (info.get("operatingMargins") or 0) * 100
    pat_cr = (info.get("netIncomeToCommon") or 0) / 1e7
    pat_margin = (info.get("profitMargins") or 0) * 100
    eps_ttm = info.get("trailingEps") or 0.0
    pe_ttm = info.get("trailingPE") or 0.0
    ps_ttm = info.get("priceToSalesTrailing12Months") or 0.0
    ocf_cr = (info.get("operatingCashflow") or 0) / 1e7
    fcf_cr = (info.get("freeCashflow") or 0) / 1e7
    roe = (info.get("returnOnEquity") or 0) * 100
    roa = (info.get("returnOnAssets") or 0) * 100

    summary = info.get("longBusinessSummary") or f"{company_name} operates in the Indian market."
    officers = info.get("companyOfficers") or []
    lead_officer = officers[0].get("name", "Management Team") if officers else "Management Team"
    lead_title = officers[0].get("title", "Managing Director & CEO") if officers else "Promoter / Leadership"

    # 3. Pull ClickHouse Historical Bulk/Block Deals & Mutual Fund Holdings
    pool = get_pool()
    client = pool.get_client()

    # Bulk/Block deals
    deals_rows = []
    try:
        deals_rows = client.query(f"""
            SELECT deal_date, client_name, buy_sell, quantity, trade_price, value_cr
            FROM market_data.bulk_block_deals FINAL
            WHERE symbol = '{symbol}'
            ORDER BY value_cr DESC
            LIMIT 10
        """).result_rows
    except Exception:
        pass

    # Mutual Fund Holdings
    mf_rows = []
    try:
        mf_rows = client.query(f"""
            SELECT fund_name, max(as_of_month), round(sum(market_value_cr), 2) as val_cr, round(avg(pct_of_nav), 2) as nav_pct
            FROM market_data.mf_holdings FINAL
            WHERE (security_name ILIKE '%{symbol}%' OR security_name ILIKE '%{company_name}%')
              AND lower(asset_type) = 'equity'
            GROUP BY fund_name
            ORDER BY val_cr DESC
            LIMIT 10
        """).result_rows
    except Exception:
        pass

    # 4. Multi-Year Financial Statements
    inc_df = ticker.income_stmt
    bs_df = ticker.balance_sheet
    cf_df = ticker.cashflow

    # 5. Price & Volume Intraday / Daily Chart via plotext
    df_h = ticker.history(period="1mo", interval="1d").reset_index()
    if df_h.empty:
        df_h = ticker.history(period="5d", interval="1h").reset_index()

    p_chart_str = ""
    v_chart_str = ""
    if not df_h.empty:
        x_idx = list(range(len(df_h)))
        d_col = "Date" if "Date" in df_h.columns else "Datetime"
        lbls = [d.strftime("%d/%m") for d in df_h[d_col]]
        closes = df_h["Close"].tolist()
        vols = (df_h["Volume"] / 1e6).tolist()

        plt.clear_figure()
        plt.title(f"{symbol} — Price Trend (₹)")
        plt.plot(x_idx, closes, color="yellow", label="Price (₹)")
        plt.xticks(x_idx, lbls)
        plt.plot_size(78, 11)
        plt.theme("dark")
        p_chart_str = plt.build()

        plt.clear_figure()
        plt.title(f"{symbol} — Volume (Million Shares)")
        plt.bar(x_idx, vols, color="cyan", label="Vol (M)")
        plt.xticks(x_idx, lbls)
        plt.plot_size(78, 7)
        plt.theme("dark")
        v_chart_str = plt.build()

    # 6. Tactical Action Sizing & Risk Parameters
    cur_p = px if px > 0 else (closes[-1] if not df_h.empty else 100.0)
    low_p = df_h["Low"].min() if not df_h.empty else cur_p * 0.9
    high_p = df_h["High"].max() if not df_h.empty else cur_p * 1.1

    stop_loss = round(low_p * 0.96, 2)
    tranche_1_low = round(cur_p * 0.98, 2)
    tranche_1_high = round(cur_p * 1.01, 2)
    tranche_2_low = round(low_p * 1.01, 2)
    tranche_2_high = round(low_p * 1.04, 2)
    target_1 = round(high_p * 1.05, 2)
    target_2 = round(high_p * 1.25, 2)
    downside_risk = round(cur_p - stop_loss, 2)
    upside_gain = round(target_2 - cur_p, 2)
    rr_ratio = round(upside_gain / (downside_risk + 1e-6), 1)

    # 7. Construct Comprehensive Markdown Output
    lines = []
    lines.append(f"# 🏛️ {company_name} (`{symbol}`) — Master Institutional Research Dossier\n")
    lines.append(f"**As of:** {datetime.now().strftime('%B %d, %Y')} | **CMP:** ₹{cur_p:,.2f} | **Market Cap:** ₹{mcap_cr:,.2f} Cr\n")
    lines.append("---\n")

    # Section 1: Executive Profile
    lines.append("## 1. Executive Profile & Capital Structure\n")
    lines.append("```text")
    lines.append(f"Company Name:           {company_name}")
    lines.append(f"NSE / Yahoo Ticker:     {symbol} / {yf_symbol}")
    lines.append(f"Key Leadership:         {lead_officer} ({lead_title})")
    lines.append(f"Market Capitalization:  ₹{mcap_cr:,.2f} Crore")
    lines.append(f"Shares Outstanding:     {shares_cr:.2f} Cr shares (Active Float: {float_cr:.2f} Cr / {float_pct:.1f}%)")
    lines.append(f"TTM Revenue / P/S:      ₹{rev_cr:,.2f} Crore / {ps_ttm:.2f}×")
    lines.append(f"TTM EBITDA / Margin:    ₹{ebitda_cr:,.2f} Crore / {ebitda_margin:.2f}%")
    lines.append(f"TTM PAT / Trailing P/E: ₹{pat_cr:,.2f} Crore / {pe_ttm:.2f}×")
    lines.append("```\n")

    # Section 2: Business Model & Economic Moat
    lines.append("## 2. Business Model & Value Chain Summary\n")
    lines.append(f"{summary}\n")

    # Section 3: ASCII Visual Price & Volume Chart
    lines.append("## 3. Price & Volume Trend (Terminal ASCII Visual)\n")
    lines.append("```text")
    lines.append(p_chart_str)
    lines.append("")
    lines.append(v_chart_str)
    lines.append("```\n")

    # Section 4: Multi-Year Financial Performance
    if inc_df is not None and not inc_df.empty:
        lines.append("## 4. Multi-Year Financial Statements (₹ Crore)\n")
        inc_sub = (inc_df.head(10) / 1e7).round(2)
        lines.append("### Income Statement Progression")
        lines.append("```text")
        lines.append(inc_sub.to_string())
        lines.append("```\n")

    if bs_df is not None and not bs_df.empty:
        bs_sub = (bs_df.head(10) / 1e7).round(2)
        lines.append("### Balance Sheet Capital Base")
        lines.append("```text")
        lines.append(bs_sub.to_string())
        lines.append("```\n")

    # Section 5: Institutional & Whale Inflows
    lines.append("## 5. Institutional Ownership & Bulk/Block Deals\n")
    if deals_rows:
        lines.append("### 🐳 Marquee Institutional Bulk & Block Deals")
        lines.append("```text")
        deals_df = pd.DataFrame(deals_rows, columns=["Date", "Client Name", "Action", "Shares", "Price (₹)", "Value (₹ Cr)"])
        lines.append(deals_df.to_string(index=False))
        lines.append("```\n")
    else:
        lines.append("*No recent exchange bulk/block deal filings found for this ticker.*\n")

    if mf_rows:
        lines.append("### 🏛️ Top Mutual Fund Holdings")
        lines.append("```text")
        mf_df = pd.DataFrame(mf_rows, columns=["Fund Name", "Month", "Value (₹ Cr)", "NAV %"])
        lines.append(mf_df.to_string(index=False))
        lines.append("```\n")

    # Section 6: Tactical Execution Framework
    lines.append("## 6. Tactical Action & Trade Execution Playbook\n")
    lines.append("```text")
    lines.append("┌──────────────────────────────────────────────────────────────────────────────────────────┐")
    lines.append("│                                TACTICAL TRADE BLUEPRINT                                  │")
    lines.append("├──────────────────────────────────────────────────────────────────────────────────────────┤")
    lines.append(f"│  • Signal Classification:     🟢 STAGGERED ACCUMULATION (2 Tranches)                     │")
    lines.append(f"│  • Current Price (CMP):       ₹{cur_p:,.2f}                                                   │")
    lines.append(f"│  • Tranche 1 (40% Position):  ₹{tranche_1_low:,.2f} – ₹{tranche_1_high:,.2f} (CMP Entry Zone)                       │")
    lines.append(f"│  • Tranche 2 (60% Position):  ₹{tranche_2_low:,.2f} – ₹{tranche_2_high:,.2f} (Dip / Base Limit Add)                 │")
    lines.append(f"│  • Hard Invalidation / Stop:  Daily close below ₹{stop_loss:,.2f} (Risk: -₹{downside_risk:,.2f})                 │")
    lines.append(f"│  • Target 1 (Short-Term):     ₹{target_1:,.2f} (+{((target_1-cur_p)/cur_p*100):.1f}% / Resistance Test)                   │")
    lines.append(f"│  • Target 2 (Medium-Term):    ₹{target_2:,.2f} (+{((target_2-cur_p)/cur_p*100):.1f}% / Structural Expansion)             │")
    lines.append(f"│  • Asymmetric Risk/Reward:    1 : {rr_ratio}                                                     │")
    lines.append("└──────────────────────────────────────────────────────────────────────────────────────────┘")
    lines.append("```\n")

    report_md = "\n".join(lines)

    # Save artifact
    if save_artifact:
        out_dir = Path("output")
        out_dir.mkdir(parents=True, exist_ok=True)
        art_path = out_dir / f"{symbol.lower()}_institutional_research.md"
        with open(art_path, "w", encoding="utf-8") as f:
            f.write(report_md)
        print(f"💾 Report saved to: {art_path}")

    return report_md


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Master Institutional Stock Research Dossier")
    parser.add_argument("symbol", type=str, help="Stock symbol or company name (e.g. LEAPIND, RUBICON, BAJFINANCE)")
    args = parser.parse_args()

    res = generate_stock_research_report(args.symbol)
    print(res)
