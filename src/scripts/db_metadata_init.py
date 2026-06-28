"""
src/scripts/db_metadata_init.py
────────────────────────────────
Seeds table schemas and SQL query templates into Qdrant for semantic metadata RAG.
"""

import sys
import os

# Set project root in path
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, PROJECT_ROOT)

import logging
from src.db.db_metadata_rag import index_metadata_points

logging.basicConfig(level=logging.INFO)

METADATA = [
    # ── 1. Table Schemas ──────────────────────────────────────────────────────
    {
        "type": "table_schema",
        "name": "market_data.daily_prices",
        "description": "Daily stock and ETF prices, close rates, high/low points, open price, and volume traded on NSE or BSE.",
        "content": "Columns: symbol (String), category (String: 'etfs' or 'stocks'), trade_date (Date), open (Float64), high (Float64), low (Float64), close (Float64), volume (Float64), imported_at (DateTime)"
    },
    {
        "type": "table_schema",
        "name": "market_data.mf_holdings",
        "description": "Morningstar mutual fund portfolio disclosures for Indian AMCs (like DSP, Nippon, Bajaj, ICICI). Contains stock holdings, security name, ISIN, percentage of NAV, and market value in crores.",
        "content": "Columns: scheme_code (String), fund_name (String), as_of_month (Date), isin (String), security_name (String), asset_type (String), market_value_cr (Float64), pct_of_nav (Float64), imported_at (DateTime)"
    },
    {
        "type": "table_schema",
        "name": "market_data.mf_nav",
        "description": "Daily NAV (Net Asset Value) histories for Indian mutual funds (pulled from mfapi.in).",
        "content": "Columns: scheme_code (String), nav_date (Date), nav (Float64), imported_at (DateTime)"
    },
    {
        "type": "table_schema",
        "name": "market_data.fii_dii_flows",
        "description": "Daily FII (Foreign Institutional Investors) and DII (Domestic Institutional Investors) net buying/selling flows in Indian equity cash markets (in crore rupees).",
        "content": "Columns: trade_date (Date), fii_net_cr (Float64), dii_net_cr (Float64), imported_at (DateTime)"
    },
    {
        "type": "table_schema",
        "name": "market_data.fii_dii_fno_daily",
        "description": "Daily FII and DII derivatives (F&O) open interest (OI) positioning including index futures net OI, options call net OI, and options put net OI.",
        "content": "Columns: trade_date (Date), fii_fut_net_oi (Float64), fii_opt_call_net_oi (Float64), fii_opt_put_net_oi (Float64), imported_at (DateTime)"
    },
    {
        "type": "table_schema",
        "name": "market_data.signal_composite",
        "description": "Composite quantitative signals, ranking metrics, and allocation regimes for all 18 tracked Indian ETFs.",
        "content": "Columns: as_of (Date), etf_symbol (String), composite_score (Float64), action (String: 'BUY'/'HOLD'/'SELL' etc.), regime_signal (String), imported_at (DateTime)"
    },
    {
        "type": "table_schema",
        "name": "market_data.ml_predictions",
        "description": "Machine learning prediction checkpoints (expected return percentage, up probability, confidence bands) from walk-forward LightGBM models.",
        "content": "Columns: as_of (Date), etf_symbol (String), prob_up (Float64), expected_return_pct (Float64), confidence_low (Float64), confidence_high (Float64), cv_auc (Float64), hit_ratio (Float64), imported_at (DateTime)"
    },
    {
        "type": "table_schema",
        "name": "market_data.weight_checkpoints",
        "description": "Recommended allocation weights (Rule-Based Risk Governor, Kelly Optimal, and Blended 50/50 recommended weights) and GARCH volatility.",
        "content": "Columns: as_of (Date), symbol (String), recommended_weight (Float64), weight_rg (Float64), weight_kelly (Float64), garch_vol_pct (Float64), imported_at (DateTime)"
    },
    {
        "type": "table_schema",
        "name": "market_data.inav_snapshots",
        "description": "iNAV (Intraday Net Asset Value) premium/discount snapshots for commodities (like GOLDBEES, SILVBEES) and other ETFs, comparing market price with intraday net value.",
        "content": "Columns: symbol (String), snapshot_at (DateTime), inav (Float64), market_price (Float64), premium_discount_pct (Float64), imported_at (DateTime)"
    },
    {
        "type": "table_schema",
        "name": "market_data.cot_gold",
        "description": "CFTC Commitments of Traders (COT) net positioning for gold futures, tracking Managed Money long/short interest and open interest.",
        "content": "Columns: report_date (Date), mm_long (Float64), mm_short (Float64), mm_net (Float64), open_interest (Float64), imported_at (DateTime)"
    },
    {
        "type": "table_schema",
        "name": "market_data.fx_rates",
        "description": "USDINR and other foreign exchange rates close histories.",
        "content": "Columns: trade_date (Date), symbol (String: e.g. 'USDINR=X'), close (Float64), imported_at (DateTime)"
    },
    {
        "type": "table_schema",
        "name": "market_data.corporate_actions",
        "description": "Corporate action events for NSE listed equities (bonus shares, demergers, stock splits, dividends, rights issues). used to suppress false positive price anomalies.",
        "content": "Columns: symbol (String), ex_date (Date), action_type (String), ratio (String), purpose (String), imported_at (DateTime)"
    },
    {
        "type": "table_schema",
        "name": "market_data.stock_earnings",
        "description": "Company earnings disclosures, EPS (Earnings Per Share) estimates, actual EPS, and surprise percentages.",
        "content": "Columns: symbol (String), earnings_date (Date), eps_estimate (Float64), eps_actual (Float64), surprise_pct (Float64), imported_at (DateTime)"
    },
    {
        "type": "table_schema",
        "name": "market_data.stock_insider_trades",
        "description": "Insider trading transaction disclosures for company promoters and management (selling/buying shares, relation, transaction value).",
        "content": "Columns: symbol (String), transaction_date (Date), insider_name (String), relation (String), transaction_type (String), shares (Float64), value (Float64), imported_at (DateTime)"
    },
    {
        "type": "table_schema",
        "name": "market_data.stock_valuation",
        "description": "Key fundamental valuations for Indian stocks (Market Capitalisation, trailing PE ratio, forward PE ratio, Price-to-Book, Return on Equity, margins, FCF).",
        "content": "Columns: symbol (String), snapshot_date (Date), market_cap (Float64), trailing_pe (Float64), forward_pe (Float64), price_to_book (Float64), return_on_equity (Float64), profit_margin (Float64), free_cashflow (Float64), imported_at (DateTime)"
    },
    {
        "type": "table_schema",
        "name": "market_data.news_articles",
        "description": "Scraped financial news articles tagged with sentiment scores, url, publisher source, category, and impact tier.",
        "content": "Columns: fetched_at (DateTime), published_at (String), source_type (String), fetch_source (String), category (String), etfs_impacted (String), sentiment (String: POSITIVE/NEUTRAL/NEGATIVE), impact_tier (String), title (String), description (String), source (String), url (String), embedding (Array(Float32))"
    },

    # ── 2. SQL Query Templates ────────────────────────────────────────────────
    {
        "type": "sql_template",
        "name": "latest_etf_price",
        "description": "Fetch the most recent closing price for an ETF or stock.",
        "content": "SELECT trade_date, close FROM market_data.daily_prices FINAL WHERE symbol = 'GOLDBEES' AND category = 'etfs' ORDER BY trade_date DESC LIMIT 5"
    },
    {
        "type": "sql_template",
        "name": "mutual_fund_holdings_search",
        "description": "Search mutual fund portfolios (like DSP active funds) that hold a particular stock.",
        "content": "SELECT fund_name, as_of_month, pct_of_nav, market_value_cr FROM market_data.mf_holdings FINAL WHERE security_name ILIKE '%Reliance%' ORDER BY as_of_month DESC, pct_of_nav DESC LIMIT 20"
    },
    {
        "type": "sql_template",
        "name": "fii_dii_net_flows_trend",
        "description": "Fetch daily net FII and DII cash flows to analyze institutional trends.",
        "content": "SELECT trade_date, fii_net_cr, dii_net_cr FROM market_data.fii_dii_flows FINAL ORDER BY trade_date DESC LIMIT 10"
    },
    {
        "type": "sql_template",
        "name": "etf_signal_composites",
        "description": "Fetch composite scores and buying/selling signals for ETFs.",
        "content": "SELECT as_of, etf_symbol, composite_score, action FROM market_data.signal_composite FINAL WHERE as_of = (SELECT max(as_of) FROM market_data.signal_composite FINAL) ORDER BY composite_score DESC"
    },
    {
        "type": "sql_template",
        "name": "inav_premium_discount_snapshot",
        "description": "Fetch the latest premium or discount snapshot for an ETF relative to its iNAV.",
        "content": "SELECT symbol, snapshot_at, inav, market_price, premium_discount_pct FROM market_data.inav_snapshots FINAL WHERE symbol = 'GOLDBEES' ORDER BY snapshot_at DESC LIMIT 5"
    },
    {
        "type": "sql_template",
        "name": "stock_valuation_fundamentals",
        "description": "Fetch trailing PE, Return on Equity (ROE), and Market Cap valuations for a specific stock.",
        "content": "SELECT snapshot_date, market_cap, trailing_pe, return_on_equity FROM market_data.stock_valuation FINAL WHERE symbol = 'RELIANCE' ORDER BY snapshot_date DESC LIMIT 1"
    },
    {
        "type": "sql_template",
        "name": "promoter_insider_trades",
        "description": "Fetch insider buying or selling transactions by promoters or key directors.",
        "content": "SELECT transaction_date, insider_name, transaction_type, shares, value FROM market_data.stock_insider_trades FINAL WHERE symbol = 'RELIANCE' ORDER BY transaction_date DESC LIMIT 10"
    },
    {
        "type": "sql_template",
        "name": "cot_futures_positioning",
        "description": "Fetch managed money long, short, and net futures positioning for commodities like gold.",
        "content": "SELECT report_date, mm_long, mm_short, mm_net, open_interest FROM market_data.cot_gold FINAL ORDER BY report_date DESC LIMIT 10"
    }
]

def main():
    print("Initializing ClickHouse metadata index in Qdrant...")
    index_metadata_points(METADATA)
    print("Done.")

if __name__ == "__main__":
    main()
