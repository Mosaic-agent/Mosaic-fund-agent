import logging
from datetime import date, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from src.importer.clickhouse import ClickHouseImporter
from src.importer.fetchers.yfinance_fetcher import fetch_ohlcv
from src.importer.fetchers.earnings_fetcher import fetch_earnings
from src.importer.fetchers.insider_fetcher import fetch_insider_trades
from src.importer.fetchers.valuation_fetcher import fetch_valuation

logger = logging.getLogger(__name__)

def import_single_stock(symbol: str, ticker: str, category: str, lookback_days: int, full_reimport: bool, clickhouse_config: dict, dry_run: bool = False) -> dict:
    """
    Import price and other relevant data (earnings, insider, valuation) for a single stock.
    """
    logger.info("Starting parallel import for %s (%s) | %s (dry_run=%s)", symbol, ticker, category, dry_run)
    
    ch = ClickHouseImporter(
        host=clickhouse_config.get("host", "localhost"),
        port=clickhouse_config.get("port", 8123),
        database=clickhouse_config.get("database", "market_data"),
        username=clickhouse_config.get("username", "default"),
        password=clickhouse_config.get("password", ""),
    )
    
    today = date.today()
    from_date = today - timedelta(days=lookback_days)
    if not full_reimport and not dry_run:
        wm = ch.get_watermark("yfinance", symbol)
        if wm:
            from_date = wm - timedelta(days=3)  # 3 days overlap

    results = {
        "symbol": symbol,
        "prices_inserted": 0,
        "earnings_inserted": 0,
        "insider_inserted": 0,
        "valuation_inserted": 0,
        "error": None
    }
    
    try:
        import time
        import random
        
        # Initial jitter sleep to stagger the start times of the 5 parallel worker threads
        time.sleep(random.uniform(0.1, 1.2))
        
        # 1. Prices
        prices = fetch_ohlcv([(symbol, ticker)], category, from_date, today)
        if prices:
            if not dry_run:
                inserted_prices = ch.insert_prices(prices)
                results["prices_inserted"] = inserted_prices
                max_date = max(r["trade_date"] for r in prices)
                ch.set_watermark("yfinance", symbol, max_date)
            else:
                results["prices_inserted"] = len(prices)
            
        # Spacing delay between endpoints
        time.sleep(random.uniform(0.3, 1.0))
        
        # 2. Earnings
        earnings = fetch_earnings([(symbol, ticker)])
        if earnings:
            if not dry_run:
                results["earnings_inserted"] = ch.insert_stock_earnings(earnings)
            else:
                results["earnings_inserted"] = len(earnings)
            
        # Spacing delay between endpoints
        time.sleep(random.uniform(0.3, 1.0))
        
        # 3. Insider
        insider = fetch_insider_trades([(symbol, ticker)])
        if insider:
            if not dry_run:
                results["insider_inserted"] = ch.insert_stock_insider(insider)
            else:
                results["insider_inserted"] = len(insider)
            
        # Spacing delay between endpoints
        time.sleep(random.uniform(0.3, 1.0))
        
        # 4. Valuation
        valuation = fetch_valuation([(symbol, ticker)])
        if valuation:
            if not dry_run:
                results["valuation_inserted"] = ch.insert_stock_valuation(valuation)
            else:
                results["valuation_inserted"] = len(valuation)
            
    except Exception as exc:
        results["error"] = str(exc)
        logger.error("Failed parallel import for %s: %s", symbol, exc)
    finally:
        ch.close()
        
    return results

def run_parallel_stock_import(
    symbols: list[tuple[str, str]],
    category: str,
    lookback_days: int = 365,
    full_reimport: bool = False,
    workers: int = 5,
    clickhouse_config: dict = None,
    dry_run: bool = False,
) -> dict:
    """
    Run parallel import for a list of stocks.
    """
    if clickhouse_config is None:
        from config.settings import settings
        clickhouse_config = {
            "host": settings.clickhouse_host,
            "port": settings.clickhouse_port,
            "database": settings.clickhouse_database,
            "username": settings.clickhouse_user,
            "password": settings.clickhouse_password,
        }
        
    results_list = []
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(
                import_single_stock, sym, ticker, category, lookback_days, full_reimport, clickhouse_config, dry_run
            ): sym
            for sym, ticker in symbols
        }
        for future in as_completed(futures):
            sym = futures[future]
            try:
                res = future.result()
                results_list.append(res)
            except Exception as e:
                logger.error("Exception for parallel stock %s: %s", sym, e)
                
    return {
        "processed": len(results_list),
        "prices": sum(r["prices_inserted"] for r in results_list),
        "earnings": sum(r["earnings_inserted"] for r in results_list),
        "insider": sum(r["insider_inserted"] for r in results_list),
        "valuation": sum(r["valuation_inserted"] for r in results_list),
        "failures": len([r for r in results_list if r["error"] is not None]),
    }
