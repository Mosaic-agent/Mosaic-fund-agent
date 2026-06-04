import os
import sys
import json
import logging
from datetime import date, datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import numpy as np
import pandas as pd
from curl_cffi import requests as crequests
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich import box

# Ensure project root is on sys.path
_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.insert(0, os.path.join(_ROOT, "src"))
sys.path.insert(0, _ROOT)

from config.settings import settings
from src.db.pool import get_pool, query_df

console = Console()
log = logging.getLogger(__name__)

CACHE_PATH = os.path.join(_ROOT, "data", "isin_ticker_cache.json")

def load_cache():
    if os.path.exists(CACHE_PATH):
        try:
            with open(CACHE_PATH, "r") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def save_cache(cache):
    os.makedirs(os.path.dirname(CACHE_PATH), exist_ok=True)
    with open(CACHE_PATH, "w") as f:
        json.dump(cache, f, indent=2)

def resolve_isin(isin, company_name):
    if isin in ("IN9040A01032", "IN9002A01016") or "margin" in company_name.lower() or "clearing" in company_name.lower():
        return None
    try:
        url = f"https://query2.finance.yahoo.com/v1/finance/search?q={isin}"
        r = crequests.get(url, impersonate="chrome110", timeout=10)
        if r.status_code == 200:
            data = r.json()
            quotes = data.get("quotes", [])
            if quotes:
                symbol = quotes[0].get("symbol")
                if symbol and (symbol.endswith(".NS") or symbol.endswith(".BO")):
                    return symbol
    except Exception as e:
        log.warning(f"Failed to resolve ISIN {isin} using raw search: {e}")
    return None

def fetch_constituent_data(c, session, db_valuations, should_scrape_valuation):
    ticker = c["ticker"]
    symbol_key = ticker.replace(".NS", "").replace(".BO", "")
    
    stats = {
        "symbol": ticker,
        "pe": None,
        "pb": None,
        "price": None,
        "close": None,
        "sma50": None,
        "sma200": None
    }
    
    # 1. Valuation Fallback from DB
    if symbol_key in db_valuations:
        pe_db, pb_db = db_valuations[symbol_key]
        if pe_db > 0:
            stats["pe"] = pe_db
        if pb_db > 0:
            stats["pb"] = pb_db

    # 2. Fetch history
    try:
        url = f"https://query2.finance.yahoo.com/v8/finance/chart/{ticker}?range=260d&interval=1d"
        r = session.get(url, impersonate="chrome110", timeout=10)
        if r.status_code == 200:
            res_data = r.json()
            if "chart" in res_data and res_data["chart"]["result"]:
                result = res_data["chart"]["result"][0]
                
                # Fetch raw close prices
                quote = result.get("indicators", {}).get("quote", [{}])[0]
                close_prices = quote.get("close", [])
                
                # Try to use adjusted close if available to account for splits/dividends
                adjclose_data = result.get("indicators", {}).get("adjclose")
                if adjclose_data and len(adjclose_data) > 0:
                    adj_prices = adjclose_data[0].get("adjclose", [])
                    adj_prices = [p for p in adj_prices if p is not None]
                    if len(adj_prices) == len([cl for cl in close_prices if cl is not None]):
                        close_prices = adj_prices
                
                # Clean close_prices (filter None)
                close_prices = [p for p in close_prices if p is not None]
                
                if close_prices:
                    stats["price"] = close_prices[-1]
                    if len(close_prices) >= 2:
                        stats["close"] = close_prices[-2]
                    if len(close_prices) >= 50:
                        stats["sma50"] = sum(close_prices[-50:]) / 50
                    if len(close_prices) >= 200:
                        stats["sma200"] = sum(close_prices[-200:]) / 200
    except Exception as e:
        log.warning(f"Error fetching history for {ticker}: {e}")

    # 3. Scrape valuation (PE/PB) from quote page if requested
    if should_scrape_valuation:
        try:
            url = f"https://finance.yahoo.com/quote/{ticker}"
            r = session.get(url, impersonate="chrome110", timeout=10)
            if r.status_code == 200:
                text = r.text
                import re
                pe_match = re.search(r'\\\\\\\"trailingPE\\\\\\\":\{\\\\\\\"raw\\\\\\\":([0-9.-]+)', text)
                pb_match = re.search(r'\\\\\\\"priceToBook\\\\\\\":\{\\\\\\\"raw\\\\\\\":([0-9.-]+)', text)
                
                if pe_match:
                    stats["pe"] = float(pe_match.group(1))
                if pb_match:
                    stats["pb"] = float(pb_match.group(1))
        except Exception as e:
            log.warning(f"Error scraping valuation for {ticker}: {e}")

    return stats

def ensure_tables():
    ch = get_pool().get_client()
    try:
        ch.command("""
            CREATE TABLE IF NOT EXISTS market_data.index_indicators (
                trade_date       Date,
                index_symbol     String,
                pe_ratio         Float64,
                pb_ratio         Float64,
                pct_above_50dma  Float64,
                pct_above_200dma Float64,
                ad_ratio         Float64,
                created_at       DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(created_at)
            ORDER BY (trade_date, index_symbol)
        """)
    finally:
        ch.close()

def save_index_indicators(trade_date, index_symbol, pe, pb, pct_50, pct_200, ad_ratio):
    ensure_tables()
    ch = get_pool().get_client()
    try:
        from datetime import date, datetime
        if isinstance(trade_date, str):
            dt = datetime.strptime(trade_date, "%Y-%m-%d").date()
        elif isinstance(trade_date, datetime):
            dt = trade_date.date()
        else:
            dt = trade_date
        ch.insert(
            "market_data.index_indicators",
            [[dt, index_symbol, float(pe), float(pb), float(pct_50), float(pct_200), float(ad_ratio)]],
            column_names=["trade_date", "index_symbol", "pe_ratio", "pb_ratio", "pct_above_50dma", "pct_above_200dma", "ad_ratio"]
        )
    finally:
        ch.close()

def run_index_analysis(fund_name, label, max_workers=20):
    console.print(f"[yellow]Analyzing constituents of {label} ({fund_name})...[/yellow]")
    df = query_df(f"""
        SELECT security_name, isin, pct_of_nav 
        FROM market_data.mf_holdings FINAL
        WHERE fund_name = '{fund_name}'
          AND as_of_month = (SELECT max(as_of_month) FROM market_data.mf_holdings FINAL WHERE fund_name = '{fund_name}')
          AND asset_type = 'equity'
        ORDER BY pct_of_nav DESC
    """)
    if df.empty:
        console.print(f"[red]No constituents found for {fund_name}.[/red]")
        return None
        
    cache = load_cache()
    resolved_tickers = {}
    missing_isins = []
    
    for _, row in df.iterrows():
        isin = row["isin"]
        name = row["security_name"]
        if isin in cache:
            resolved_tickers[isin] = cache[isin]
        else:
            missing_isins.append((isin, name))
            
    if missing_isins:
        console.print(f"Resolving {len(missing_isins)} new ISINs parallelly...")
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_map = {executor.submit(resolve_isin, isin, name): isin for isin, name in missing_isins}
            for future in as_completed(future_map):
                isin = future_map[future]
                ticker = future.result()
                if ticker:
                    resolved_tickers[isin] = ticker
                    cache[isin] = ticker
        save_cache(cache)
        
    constituents = []
    total_weight = 0.0
    for _, row in df.iterrows():
        isin = row["isin"]
        weight = float(row["pct_of_nav"])
        ticker = resolved_tickers.get(isin)
        if ticker:
            constituents.append({"name": row["security_name"], "isin": isin, "weight": weight, "ticker": ticker})
            total_weight += weight

    # Sort constituents by weight descending to prioritize scraping of top holdings
    constituents = sorted(constituents, key=lambda x: -x["weight"])

    # Load existing valuations from ClickHouse DB
    db_valuations = {}
    try:
        db_df = query_df("""
            SELECT symbol, trailing_pe, price_to_book
            FROM market_data.stock_valuation FINAL
            WHERE trailing_pe > 0 AND price_to_book > 0
        """)
        for _, row in db_df.iterrows():
            db_valuations[str(row["symbol"])] = (float(row["trailing_pe"]), float(row["price_to_book"]))
    except Exception as e:
        log.warning(f"Failed to load valuations from DB: {e}")

    # Set scraping limits: scrape all for Nifty 50, top 120 for others (e.g. Nifty 500)
    max_scrape = 52 if '50' in label and '500' not in label else 120
    
    session = crequests.Session()
    ticker_stats = {}
    
    # Fetch historical prices and scrape quote page in parallel
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {}
        for idx, c in enumerate(constituents):
            should_scrape = (idx < max_scrape)
            future = executor.submit(fetch_constituent_data, c, session, db_valuations, should_scrape)
            future_map[future] = c["ticker"]
            
        for future in as_completed(future_map):
            ticker = future_map[future]
            stats = future.result()
            ticker_stats[ticker] = stats
            
    # Computations
    valid_pe_weights = []
    valid_pe_values = []
    valid_pb_weights = []
    valid_pb_values = []
    
    above_50dma = 0
    above_200dma = 0
    valid_dma_count = 0
    
    advances = 0
    declines = 0
    
    for c in constituents:
        stats = ticker_stats.get(c["ticker"])
        if not stats or stats.get("price") is None:
            continue
            
        weight = c["weight"]
        price = stats["price"]
        prev_close = stats["close"]
        pe = stats["pe"]
        pb = stats["pb"]
        sma50 = stats["sma50"]
        sma200 = stats["sma200"]
        
        if price is not None and prev_close is not None:
            if price > prev_close:
                advances += 1
            elif price < prev_close:
                declines += 1
                
        if price is not None and sma50 is not None:
            valid_dma_count += 1
            if price > sma50:
                above_50dma += 1
            if sma200 is not None and price > sma200:
                above_200dma += 1
                
        if pe is not None and pe > 0:
            valid_pe_weights.append(weight)
            valid_pe_values.append(pe)
        if pb is not None and pb > 0:
            valid_pb_weights.append(weight)
            valid_pb_values.append(pb)
            
    sum_pe_weight = sum(valid_pe_weights)
    weighted_harmonic_pe = sum_pe_weight / sum(w / val for w, val in zip(valid_pe_weights, valid_pe_values)) if valid_pe_weights else float("nan")
    
    sum_pb_weight = sum(valid_pb_weights)
    weighted_harmonic_pb = sum_pb_weight / sum(w / val for w, val in zip(valid_pb_weights, valid_pb_values)) if valid_pb_weights else float("nan")
    
    ad_ratio = advances / declines if declines > 0 else advances
    pct_above_50 = (above_50dma / valid_dma_count) * 100 if valid_dma_count > 0 else 0.0
    pct_above_200 = (above_200dma / valid_dma_count) * 100 if valid_dma_count > 0 else 0.0
    
    return {
        "pe": weighted_harmonic_pe,
        "pb": weighted_harmonic_pb,
        "pct_above_50": pct_above_50,
        "pct_above_200": pct_above_200,
        "advances": advances,
        "declines": declines,
        "ad_ratio": ad_ratio,
        "coverage": total_weight
    }

def print_sector_rotation():
    sectors = {
        "IT": "ITBEES.NS",
        "Banking": "BANKBEES.NS",
        "Auto": "AUTOBEES.NS",
        "Pharma": "PHARMABEES.NS",
        "PSU / Capex": "CPSEETF.NS",
        "FMCG": "FMCGIETF.NS"
    }
    
    console.print("\n[bold cyan]📊 Sector Rotation Ranking (1-Month Performance)[/bold cyan]")
    tbl = Table(box=box.SIMPLE, show_header=True)
    tbl.add_column("Sector", style="white")
    tbl.add_column("ETF Ticker", style="dim")
    tbl.add_column("Price Now", justify="right")
    tbl.add_column("1M Return %", justify="right")
    tbl.add_column("3M Return %", justify="right")
    
    sector_data = []
    session = crequests.Session()
    for sec_name, ticker in sectors.items():
        try:
            url = f"https://query2.finance.yahoo.com/v8/finance/chart/{ticker}?range=4mo&interval=1d"
            r = session.get(url, impersonate="chrome110", timeout=10)
            if r.status_code == 200:
                result = r.json().get("chart", {}).get("result", [{}])[0]
                quote = result.get("indicators", {}).get("quote", [{}])[0]
                close_prices = quote.get("close", [])
                
                # Filter out None values
                close_prices = [p for p in close_prices if p is not None]
                
                if close_prices:
                    close_now = close_prices[-1]
                    close_1m = close_prices[-21] if len(close_prices) >= 21 else close_now
                    close_3m = close_prices[-63] if len(close_prices) >= 63 else close_now
                    
                    ret_1m = ((close_now / close_1m) - 1) * 100
                    ret_3m = ((close_now / close_3m) - 1) * 100
                    
                    sector_data.append({
                        "sector": sec_name,
                        "ticker": ticker,
                        "price": close_now,
                        "ret_1m": ret_1m,
                        "ret_3m": ret_3m
                    })
        except Exception as e:
            log.warning(f"Error fetching sector rotation for {ticker}: {e}")
            
    if sector_data:
        sector_data = sorted(sector_data, key=lambda x: -x["ret_1m"])
        for s in sector_data:
            col_1m = "green" if s["ret_1m"] >= 0 else "red"
            col_3m = "green" if s["ret_3m"] >= 0 else "red"
            tbl.add_row(
                s["sector"],
                s["ticker"],
                f"₹{s['price']:.2f}",
                f"[{col_1m}]{s['ret_1m']:+.2f}%[/{col_1m}]",
                f"[{col_3m}]{s['ret_3m']:+.2f}%[/{col_3m}]"
            )
        console.print(tbl)

def print_fii_positioning():
    console.print("\n[bold cyan]📉 FII Derivatives & Index Futures Stance[/bold cyan]")
    df = query_df("""
        SELECT 
            trade_date,
            nifty_close,
            fii_fut_net_oi,
            fii_fut_outstanding_oi,
            fii_fut_nifty_net_oi,
            fii_fut_banknifty_net_oi
        FROM market_data.fii_dii_fno_daily FINAL
        ORDER BY trade_date DESC 
        LIMIT 5
    """)
    if df.empty:
        console.print("[dim]FII derivatives data unavailable in ClickHouse.[/dim]")
        return
        
    tbl = Table(box=box.SIMPLE, show_header=True)
    tbl.add_column("Trade Date", style="dim")
    tbl.add_column("Nifty Close", justify="right")
    tbl.add_column("FII Net Index Futures OI", justify="right")
    tbl.add_column("Leverage Stance", justify="right")
    tbl.add_column("Nifty Fut Net Δ (Daily)", justify="right")
    
    for _, r in df.iterrows():
        net_oi = float(r["fii_fut_net_oi"])
        total_oi = float(r["fii_fut_outstanding_oi"])
        ratio = (net_oi / total_oi * 100) if total_oi > 0 else 0.0
        
        stance = f"[{'green' if ratio >= 0 else 'red'}]{ratio:+.1f}%[/{'green' if ratio >= 0 else 'red'}]"
        net_oi_str = f"[{'green' if net_oi >= 0 else 'red'}]{net_oi:+.0f}[/{'green' if net_oi >= 0 else 'red'}]"
        nifty_net = f"{r['fii_fut_nifty_net_oi']:+.0f}"
        
        tbl.add_row(
            str(r["trade_date"]),
            f"{r['nifty_close']:,.2f}",
            net_oi_str,
            stance,
            nifty_net
        )
    console.print(tbl)

def print_usdinr_trend():
    console.print("\n[bold cyan]💱 USDINR Rupee Stress Indicator[/bold cyan]")
    df = query_df("""
        WITH history AS (
            SELECT 
                trade_date,
                close AS rate,
                avg(close) OVER (ORDER BY trade_date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) AS sma50
            FROM market_data.fx_rates FINAL
            WHERE symbol = 'USDINR'
        )
        SELECT trade_date, rate, sma50, round((rate/sma50 - 1)*100, 2) AS dev
        FROM history
        ORDER BY trade_date DESC 
        LIMIT 5
    """)
    if df.empty:
        console.print("[dim]USDINR exchange rate data unavailable in ClickHouse.[/dim]")
        return
        
    tbl = Table(box=box.SIMPLE, show_header=True)
    tbl.add_column("Trade Date", style="dim")
    tbl.add_column("USDINR Close", justify="right")
    tbl.add_column("50-Day SMA", justify="right")
    tbl.add_column("Deviation %", justify="right")
    tbl.add_column("Stress Stance", justify="center")
    
    for _, r in df.iterrows():
        dev = float(r["dev"])
        col = "red" if dev > 0.5 else "green" if dev < -0.5 else "dim"
        status = "⚠️ Weak Rupee (Stress)" if dev > 0.2 else "🟢 Stable / Strong"
        
        tbl.add_row(
            str(r["trade_date"]),
            f"₹{r['rate']:.2f}",
            f"₹{r['sma50']:.2f}",
            f"[{col}]{dev:+.2f}%[/{col}]",
            status
        )
    console.print(tbl)

def print_gold_etf_flows():
    console.print("\n[bold cyan]🥇 Gold ETF Tonnage Whale Flow (SPDR GLD)[/bold cyan]")
    df = query_df("""
        WITH current_aum AS (
            SELECT symbol, implied_tonnes AS tonnes_now, trade_date
            FROM market_data.etf_aum FINAL
            WHERE symbol = 'GLD'
        ),
        past_aum AS (
            SELECT symbol, implied_tonnes AS tonnes_prev, trade_date AS prev_date
            FROM market_data.etf_aum FINAL
            WHERE symbol = 'GLD'
        )
        SELECT 
            c.trade_date,
            c.tonnes_now,
            p.tonnes_prev,
            round(c.tonnes_now - p.tonnes_prev, 2) AS monthly_tonnes_flow,
            round(((c.tonnes_now / p.tonnes_prev) - 1) * 100, 2) AS monthly_change_pct
        FROM current_aum c
        JOIN past_aum p ON c.symbol = p.symbol AND p.prev_date = subtractDays(c.trade_date, 30)
        ORDER BY c.trade_date DESC
        LIMIT 5
    """)
    if df.empty:
        # Fallback to simple monthly comparison
        df = query_df("""
            SELECT trade_date, implied_tonnes AS tonnes_now,
                   implied_tonnes - any(implied_tonnes) OVER (ORDER BY trade_date ROWS BETWEEN 20 PRECEDING AND 20 PRECEDING) AS monthly_tonnes_flow
            FROM market_data.etf_aum FINAL
            WHERE symbol = 'GLD'
            ORDER BY trade_date DESC
            LIMIT 5
        """)
        if df.empty:
            console.print("[dim]Gold ETF flow data unavailable in ClickHouse.[/dim]")
            return
            
    tbl = Table(box=box.SIMPLE, show_header=True)
    tbl.add_column("Trade Date", style="dim")
    tbl.add_column("GLD Implied Tonnes", justify="right")
    tbl.add_column("30D Net Flow (Tonnes)", justify="right")
    tbl.add_column("Whale Buy/Sell Stance", justify="center")
    
    for _, r in df.iterrows():
        flow = float(r.get("monthly_tonnes_flow", 0.0))
        tonnes = float(r.get("tonnes_now", r.get("implied_tonnes", 0.0)))
        
        col = "green" if flow >= 0 else "red"
        stance = "🐋 Whale Buying" if flow > 5 else "Trim / Neutral" if flow >= -5 else "🐋 Whale Selling"
        
        tbl.add_row(
            str(r["trade_date"]),
            f"{tonnes:,.2f} t",
            f"[{col}]{flow:+.2f} t[/{col}]",
            stance
        )
    console.print(tbl)

def main():
    console.print(Panel(
        "[bold cyan]🔍 Quantitative Macro & Market Breadth Scorecard[/bold cyan]\n"
        "[dim]Dynamic Constituent Valuation, Index Breadth, and Macro Indicator Overlay[/dim]",
        border_style="cyan"
    ))
    
    # 1. Nifty 50 constituents calculation
    n50 = run_index_analysis('DSP_NIFTY_50_INDEX', 'Nifty 50', max_workers=20)
    # 2. Nifty 500 constituents calculation (takes a bit longer, limited workers)
    n500 = run_index_analysis('DSP_NIFTY_500_INDEX', 'Nifty 500', max_workers=30)
    
    # Render Breadth & Valuations
    tbl = Table(title="Calculated Index Valuation & Breadth", show_header=True, header_style="bold cyan")
    tbl.add_column("Index", style="white")
    tbl.add_column("Weighted P/E", justify="right")
    tbl.add_column("Weighted P/B", justify="right")
    tbl.add_column("Above 50 DMA", justify="right")
    tbl.add_column("Above 200 DMA", justify="right")
    tbl.add_column("A/D Stats", justify="right")
    tbl.add_column("AD Ratio", justify="right")
    
    today_dt = date.today()
    
    if n50:
        tbl.add_row(
            "Nifty 50",
            f"{n50['pe']:.2f}",
            f"{n50['pb']:.2f}",
            f"{n50['pct_above_50']:.1f}%",
            f"{n50['pct_above_200']:.1f}%",
            f"{n50['advances']} A / {n50['declines']} D",
            f"{n50['ad_ratio']:.2f}"
        )
        save_index_indicators(today_dt, "NIFTY50", n50['pe'], n50['pb'], n50['pct_above_50'], n50['pct_above_200'], n50['ad_ratio'])
        
    if n500:
        tbl.add_row(
            "Nifty 500",
            f"{n500['pe']:.2f}",
            f"{n500['pb']:.2f}",
            f"{n500['pct_above_50']:.1f}%",
            f"{n500['pct_above_200']:.1f}%",
            f"{n500['advances']} A / {n500['declines']} D",
            f"{n500['ad_ratio']:.2f}"
        )
        save_index_indicators(today_dt, "NIFTY500", n500['pe'], n500['pb'], n500['pct_above_50'], n500['pct_above_200'], n500['ad_ratio'])
        
    console.print(tbl)
    
    # 3. Sector Rotation
    print_sector_rotation()
    
    # 4. FII futures stance
    print_fii_positioning()
    
    # 5. USDINR trend stress
    print_usdinr_trend()
    
    # 6. Gold ETF flows
    print_gold_etf_flows()
    
    console.print(
        "\n[dim]Note: P/E and P/B are computed as weighted harmonic means to match index standards. "
        "Dynamic stats resolved parallelly from yfinance via ISIN map. Index indicators persisted to ClickHouse.[/dim]"
    )

if __name__ == "__main__":
    main()
