import logging
import time
from datetime import date, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from src.data_importer.clickhouse import ClickHouseImporter
from src.data_importer.fetchers.yfinance_fetcher import fetch_ohlcv
from src.data_importer.fetchers.earnings_fetcher import fetch_earnings
from src.data_importer.fetchers.insider_fetcher import fetch_insider_trades
from src.data_importer.fetchers.valuation_fetcher import fetch_valuation

logger = logging.getLogger(__name__)

# Per-domain inter-request spacing (seconds).  Phase 3 will replace these
# with proper token-bucket rate limiters (aiolimiter).
_INITIAL_SLEEP = 0.3   # stagger thread start
_BETWEEN_CALLS = 0.4   # spacing between sequential API calls per symbol


def import_single_stock(
    symbol: str,
    ticker: str,
    category: str,
    lookback_days: int,
    full_reimport: bool,
    clickhouse_config: dict,
    dry_run: bool = False,
    data_source: str = "shoonya",
) -> dict:
    """
    Import price and other relevant data (earnings, insider, valuation) for a single stock.
    Borrows a connection from the shared CHPool instead of opening a raw TCP handshake
    per thread.
    """
    from src.db.pool import get_pool

    logger.info("Starting parallel import for %s (%s) | %s (dry_run=%s)", symbol, ticker, category, dry_run)

    pool = get_pool()

    results = {
        "symbol": symbol,
        "prices_inserted": 0,
        "earnings_inserted": 0,
        "insider_inserted": 0,
        "valuation_inserted": 0,
        "error": None
    }

    try:
        # Stagger thread start to avoid thundering-herd on the upstream APIs
        time.sleep(_INITIAL_SLEEP)

        with pool.acquire() as client:
            ch = ClickHouseImporter(
                host=clickhouse_config.get("host", "localhost"),
                port=clickhouse_config.get("port", 8123),
                database=clickhouse_config.get("database", "market_data"),
                username=clickhouse_config.get("username", "default"),
                password=clickhouse_config.get("password", ""),
                client=client,  # pool-managed — no TCP handshake, no close() needed
            )

            today = date.today()
            from_date = today - timedelta(days=lookback_days)
            if not full_reimport and not dry_run:
                wm = ch.get_watermark(data_source, symbol, dataset="prices")
                if wm:
                    from_date = wm - timedelta(days=3)  # 3 days overlap

            # 1. Prices
            if category in ("stocks", "etfs"):
                if data_source == "nse":
                    from src.data_importer.fetchers.adapters import NSElibFetcher
                    prices = NSElibFetcher(category, [(symbol, ticker)]).fetch(from_date, today)
                elif data_source == "yfinance":
                    prices = fetch_ohlcv([(symbol, ticker)], category, from_date, today)
                else:
                    from src.data_importer.fetchers.adapters import ShoonyaFetcher
                    prices = ShoonyaFetcher(category, [(symbol, ticker)]).fetch(from_date, today)
            else:
                prices = fetch_ohlcv([(symbol, ticker)], category, from_date, today)
            if prices:
                if not dry_run:
                    inserted_prices = ch.insert_prices(prices)
                    results["prices_inserted"] = inserted_prices
                    max_date = max(r["trade_date"] for r in prices)
                    ch.set_watermark(data_source, symbol, max_date, dataset="prices")
                else:
                    results["prices_inserted"] = len(prices)

            time.sleep(_BETWEEN_CALLS)

            # 2. Earnings
            earnings = fetch_earnings([(symbol, ticker)])
            if earnings:
                if not dry_run:
                    results["earnings_inserted"] = ch.insert_stock_earnings(earnings)
                    ch.set_watermark("yfinance", symbol, date.today(), dataset="earnings")
                else:
                    results["earnings_inserted"] = len(earnings)

            time.sleep(_BETWEEN_CALLS)

            # 3. Insider
            insider = fetch_insider_trades([(symbol, ticker)])
            if insider:
                if not dry_run:
                    results["insider_inserted"] = ch.insert_stock_insider(insider)
                    ch.set_watermark("yfinance", symbol, date.today(), dataset="insider")
                else:
                    results["insider_inserted"] = len(insider)

            time.sleep(_BETWEEN_CALLS)

            # 4. Valuation
            valuation = fetch_valuation([(symbol, ticker)])
            if valuation:
                if not dry_run:
                    results["valuation_inserted"] = ch.insert_stock_valuation(valuation)
                    ch.set_watermark("yfinance", symbol, date.today(), dataset="valuation")
                else:
                    results["valuation_inserted"] = len(valuation)

    except Exception as exc:
        results["error"] = str(exc)
        logger.error("Failed parallel import for %s: %s", symbol, exc)

    return results

def run_parallel_stock_import(
    symbols: list[tuple[str, str]],
    category: str,
    lookback_days: int = 365,
    full_reimport: bool = False,
    workers: int = 5,
    clickhouse_config: dict = None,
    dry_run: bool = False,
    data_source: str = "shoonya",
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
                import_single_stock, sym, ticker, category, lookback_days,
                full_reimport, clickhouse_config, dry_run, data_source
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
