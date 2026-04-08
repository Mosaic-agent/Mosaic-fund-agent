"""
src/importer/clickhouse.py
──────────────────────────
ClickHouse client wrapper for the historical data importer.

Tables (all in `market_data` database):
  daily_prices      — OHLCV for stocks, ETFs, commodities, indices
  mf_nav            — Daily NAV for mutual funds / ETFs from MFAPI.in
  import_watermarks — Last successfully imported date per (source, symbol)

Design:
  • ReplacingMergeTree — idempotent inserts; re-importing the same date
    is safe and simply replaces the existing row.
  • Watermark-based delta sync — only fetch data after last watermark date,
    with a configurable overlap window (default 3 days) to catch late-arriving
    corrections.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from typing import Any

import clickhouse_connect

logger = logging.getLogger(__name__)

# ── DDL ───────────────────────────────────────────────────────────────────────

_DDL_DATABASE = "CREATE DATABASE IF NOT EXISTS market_data"

_DDL_DAILY_PRICES = """
CREATE TABLE IF NOT EXISTS market_data.daily_prices (
    symbol       String,
    category     String,       -- stocks | etfs | commodities | indices
    trade_date   Date,
    open         Float64,
    high         Float64,
    low          Float64,
    close        Float64,
    volume       Float64,
    imported_at  DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(imported_at)
PARTITION BY toYYYYMM(trade_date)
ORDER BY (symbol, trade_date)
"""

_DDL_MF_NAV = """
CREATE TABLE IF NOT EXISTS market_data.mf_nav (
    symbol       String,
    scheme_code  String,
    nav_date     Date,
    nav          Float64,
    imported_at  DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(imported_at)
PARTITION BY toYYYYMM(nav_date)
ORDER BY (symbol, nav_date)
"""

_DDL_WATERMARKS = """
CREATE TABLE IF NOT EXISTS market_data.import_watermarks (
    source       String,       -- yfinance | mfapi
    symbol       String,
    last_date    Date,
    updated_at   DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (source, symbol)
"""

_DDL_COT_GOLD = """
CREATE TABLE IF NOT EXISTS market_data.cot_gold (
    report_date     Date,
    mm_long         Int64,
    mm_short        Int64,
    mm_spread       Int64,
    mm_net          Int64,
    comm_long       Int64,
    comm_short      Int64,
    comm_net        Int64,
    open_interest   Int64,
    source          String DEFAULT 'cftc_disaggregated',
    _ver            DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(_ver)
ORDER BY (report_date)
"""

_DDL_CB_GOLD_RESERVES = """
CREATE TABLE IF NOT EXISTS market_data.cb_gold_reserves (
    ref_period      Date,
    country_code    String,
    country_name    String,
    reserves_tonnes Float64,
    source          String DEFAULT 'imf_ifs',
    _ver            DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(_ver)
ORDER BY (ref_period, country_code)
"""

_DDL_ETF_AUM = """
CREATE TABLE IF NOT EXISTS market_data.etf_aum (
    trade_date      Date,
    symbol          String,
    aum_usd         Float64,
    price           Float64,
    implied_tonnes  Float64,
    source          String DEFAULT 'yfinance',
    _ver            DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(_ver)
ORDER BY (trade_date, symbol)
"""

_DDL_FX_RATES = """
CREATE TABLE IF NOT EXISTS market_data.fx_rates (
    trade_date   Date,
    symbol       String,       -- e.g. USDINR, USDCNY, USDAED, USDSAR, USDKWD
    open         Float64,
    high         Float64,
    low          Float64,
    close        Float64,
    source       String DEFAULT 'yfinance',
    imported_at  DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(imported_at)
PARTITION BY toYYYYMM(trade_date)
ORDER BY (symbol, trade_date)
"""

_DDL_INAV_SNAPSHOTS = """
CREATE TABLE IF NOT EXISTS market_data.inav_snapshots (
    symbol                String,
    snapshot_at           DateTime,    -- UTC timestamp of the fetch
    inav                  Float64,     -- Indicative NAV from NSE (₹)
    market_price          Float64,     -- Last traded price from NSE (₹)
    premium_discount_pct  Float64,     -- (market_price - inav) / inav * 100
    source                String       -- NSE | Yahoo
)
ENGINE = ReplacingMergeTree(snapshot_at)
PARTITION BY toYYYYMM(snapshot_at)
ORDER BY (symbol, snapshot_at)
"""

_DDL_ML_PREDICTIONS = """
CREATE TABLE IF NOT EXISTS market_data.ml_predictions (
    as_of                Date,
    horizon_days         UInt8,
    expected_return_pct  Float64,
    confidence_low       Float64,
    confidence_high      Float64,
    regime_signal        String,
    cv_r2_mean           Float64,
    n_training_rows      UInt32,
    goldbees_close       Float64,
    created_at           DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(created_at)
ORDER BY (as_of, horizon_days)
"""

_DDL_MF_HOLDINGS = """
CREATE TABLE IF NOT EXISTS market_data.mf_holdings (
    scheme_code    String,
    fund_name      String,
    as_of_month    Date,
    isin           String,
    security_name  String,
    asset_type     String,       -- equity | gold | bond | cash | other
    market_value_cr Float64,
    pct_of_nav     Float64,
    imported_at    DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(imported_at)
PARTITION BY toYYYYMM(as_of_month)
ORDER BY (scheme_code, as_of_month, isin)
"""

_DDL_FII_DII_FLOWS = """
CREATE TABLE IF NOT EXISTS market_data.fii_dii_flows (
    trade_date         Date,
    fii_gross_buy_cr   Float64,   -- FII gross purchases (Rs Crore, cash segment)
    fii_gross_sell_cr  Float64,   -- FII gross sales (Rs Crore, cash segment)
    fii_net_cr         Float64,   -- FII net flow = buy -- sell (Rs Crore)
    dii_gross_buy_cr   Float64,   -- DII gross purchases (Rs Crore, cash segment)
    dii_gross_sell_cr  Float64,   -- DII gross sales (Rs Crore, cash segment)
    dii_net_cr         Float64,   -- DII net flow = buy -- sell (Rs Crore)
    imported_at        DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(imported_at)
ORDER BY (trade_date)
"""

_DDL_FII_DII_MONTHLY = """
CREATE TABLE IF NOT EXISTS market_data.fii_dii_monthly (
    month_date         Date,        -- First day of month (YYYY-MM-01)
    fii_buy_cr         Float64,     -- FII gross purchases that month (Rs Crore)
    fii_sell_cr        Float64,     -- FII gross sales that month (Rs Crore)
    fii_net_cr         Float64,     -- FII net = buy - sell (Rs Crore)
    dii_buy_cr         Float64,     -- DII gross purchases that month (Rs Crore)
    dii_sell_cr        Float64,     -- DII gross sales that month (Rs Crore)
    dii_net_cr         Float64,     -- DII net = buy - sell (Rs Crore)
    nifty_close        Float64,     -- Nifty 50 month-end close
    nifty_change_pct   Float64,     -- Nifty % change that month
    imported_at        DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(imported_at)
ORDER BY (month_date)
"""

_DDL_FII_DII_FNO_DAILY = """
CREATE TABLE IF NOT EXISTS market_data.fii_dii_fno_daily (
    trade_date                    Date,
    -- Index futures (quantity-wise net OI change)
    fii_fut_net_oi                Float64,  -- FII index futures net OI change
    fii_fut_outstanding_oi        Float64,  -- FII index futures outstanding OI
    fii_fut_nifty_net_oi          Float64,  -- FII Nifty futures net OI change
    fii_fut_banknifty_net_oi      Float64,  -- FII BankNifty futures net OI change
    dii_fut_net_oi                Float64,
    dii_fut_outstanding_oi        Float64,
    pro_fut_net_oi                Float64,
    pro_fut_outstanding_oi        Float64,
    client_fut_net_oi             Float64,
    client_fut_outstanding_oi     Float64,
    -- Stock futures
    fii_fut_stock_net_oi          Float64,
    dii_fut_stock_net_oi          Float64,
    pro_fut_stock_net_oi          Float64,
    client_fut_stock_net_oi       Float64,
    -- Options (overall net OI)
    fii_opt_overall_net_oi        Float64,  -- FII overall options net OI
    fii_opt_overall_net_oi_change Float64,  -- FII options OI change (day)
    fii_opt_call_net_oi           Float64,
    fii_opt_put_net_oi            Float64,
    dii_opt_overall_net_oi        Float64,
    dii_opt_overall_net_oi_change Float64,
    pro_opt_overall_net_oi        Float64,
    pro_opt_overall_net_oi_change Float64,
    client_opt_overall_net_oi     Float64,
    client_opt_overall_net_oi_change Float64,
    -- Index context
    nifty_close                   Float64,
    banknifty_close               Float64,
    nifty_change_pct              Float64,
    banknifty_change_pct          Float64,
    imported_at                   DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(imported_at)
ORDER BY (trade_date)
"""


class ClickHouseImporter:
    """
    Thin wrapper around clickhouse_connect.Client for bulk data imports.

    Parameters
    ----------
    host, port, database, username, password : from Settings
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 8123,
        database: str = "market_data",
        username: str = "default",
        password: str = "",
    ) -> None:
        self._client = clickhouse_connect.get_client(
            host=host,
            port=port,
            username=username,
            password=password,
            connect_timeout=15,
        )
        self._database = database

    # ── Schema ────────────────────────────────────────────────────────────────

    def ensure_schema(self) -> None:
        """Create database and tables if they don't already exist."""
        for ddl in (
            _DDL_DATABASE, _DDL_DAILY_PRICES, _DDL_MF_NAV, _DDL_WATERMARKS,
            _DDL_INAV_SNAPSHOTS, _DDL_COT_GOLD, _DDL_CB_GOLD_RESERVES, _DDL_ETF_AUM,
            _DDL_FX_RATES, _DDL_ML_PREDICTIONS, _DDL_MF_HOLDINGS, _DDL_FII_DII_FLOWS,
            _DDL_FII_DII_MONTHLY, _DDL_FII_DII_FNO_DAILY,
        ):
            self._client.command(ddl)
        logger.debug("ClickHouse schema verified.")

    # ── Watermarks ────────────────────────────────────────────────────────────

    def get_watermark(self, source: str, symbol: str) -> date | None:
        """
        Return the last successfully imported date for (source, symbol),
        or None if this symbol has never been imported.
        """
        result = self._client.query(
            "SELECT last_date FROM market_data.import_watermarks FINAL "
            "WHERE source = {source:String} AND symbol = {symbol:String} "
            "LIMIT 1",
            parameters={"source": source, "symbol": symbol},
        )
        rows = result.result_rows
        if rows:
            return rows[0][0]  # clickhouse_connect returns date objects
        return None

    def set_watermark(self, source: str, symbol: str, last_date: date) -> None:
        """Upsert the watermark for (source, symbol)."""
        self._client.insert(
            "market_data.import_watermarks",
            [[source, symbol, last_date]],
            column_names=["source", "symbol", "last_date"],
        )

    # ── Bulk insert: daily_prices ────────────────────────────────────────────

    def insert_prices(
        self,
        rows: list[dict[str, Any]],
        *,
        dry_run: bool = False,
    ) -> int:
        """
        Bulk-insert OHLCV rows into daily_prices.

        Each row dict must have keys:
            symbol, category, trade_date (date), open, high, low, close, volume

        Returns the number of rows inserted (or that would have been inserted).
        """
        if not rows:
            return 0
        if dry_run:
            logger.info("[dry-run] Would insert %d price rows.", len(rows))
            return len(rows)

        data = [
            [
                r["symbol"],
                r["category"],
                r["trade_date"],
                r["open"],
                r["high"],
                r["low"],
                r["close"],
                r["volume"],
            ]
            for r in rows
        ]
        self._client.insert(
            "market_data.daily_prices",
            data,
            column_names=["symbol", "category", "trade_date", "open", "high", "low", "close", "volume"],
            settings={"max_partitions_per_insert_block": 300},
        )
        return len(rows)

    # ── Bulk insert: mf_nav ───────────────────────────────────────────────────

    def insert_nav(
        self,
        rows: list[dict[str, Any]],
        *,
        dry_run: bool = False,
    ) -> int:
        """
        Bulk-insert NAV rows into mf_nav.

        Each row dict must have keys:
            symbol, scheme_code, nav_date (date), nav

        Returns the number of rows inserted (or that would have been inserted).
        """
        if not rows:
            return 0
        if dry_run:
            logger.info("[dry-run] Would insert %d NAV rows.", len(rows))
            return len(rows)

        data = [
            [r["symbol"], r["scheme_code"], r["nav_date"], r["nav"]]
            for r in rows
        ]
        self._client.insert(
            "market_data.mf_nav",
            data,
            column_names=["symbol", "scheme_code", "nav_date", "nav"],
            settings={"max_partitions_per_insert_block": 300},
        )
        return len(rows)

    # ── Bulk insert: mf_holdings ──────────────────────────────────────────────

    def insert_mf_holdings(
        self,
        rows: list[dict[str, Any]],
        *,
        dry_run: bool = False,
    ) -> int:
        """
        Bulk-insert MF portfolio holdings into mf_holdings.

        Each row dict must have keys:
            scheme_code, fund_name, as_of_month (date), isin, security_name,
            asset_type, market_value_cr, pct_of_nav

        Returns the number of rows inserted (or that would have been inserted).
        """
        if not rows:
            return 0
        if dry_run:
            logger.info("[dry-run] Would insert %d holdings rows.", len(rows))
            return len(rows)

        data = [
            [
                r["scheme_code"],
                r["fund_name"],
                r["as_of_month"],
                r["isin"],
                r["security_name"],
                r["asset_type"],
                r["market_value_cr"],
                r["pct_of_nav"],
            ]
            for r in rows
        ]
        self._client.insert(
            "market_data.mf_holdings",
            data,
            column_names=[
                "scheme_code", "fund_name", "as_of_month", "isin",
                "security_name", "asset_type", "market_value_cr", "pct_of_nav",
            ],
        )
        return len(rows)

    # ── Bulk insert: inav_snapshots ───────────────────────────────────────────

    def insert_inav_snapshots(
        self,
        rows: list[dict[str, Any]],
        *,
        dry_run: bool = False,
    ) -> int:
        """
        Insert iNAV snapshot rows into inav_snapshots.

        Each row dict must have keys:
            symbol, snapshot_at (datetime), inav, market_price,
            premium_discount_pct, source

        Returns the number of rows inserted (or that would be inserted).
        """
        if not rows:
            return 0
        if dry_run:
            logger.info("[dry-run] Would insert %d iNAV snapshot rows.", len(rows))
            return len(rows)

        data = [
            [
                r["symbol"],
                r["snapshot_at"],
                r["inav"],
                r["market_price"],
                r["premium_discount_pct"],
                r["source"],
            ]
            for r in rows
        ]
        self._client.insert(
            "market_data.inav_snapshots",
            data,
            column_names=["symbol", "snapshot_at", "inav", "market_price",
                          "premium_discount_pct", "source"],
        )
        return len(rows)

    # ── Bulk insert: cot_gold ────────────────────────────────────────────────

    def insert_cot_gold(
        self,
        rows: list[dict[str, Any]],
        *,
        dry_run: bool = False,
    ) -> int:
        """Insert CFTC COT rows into cot_gold. Returns row count."""
        if not rows:
            return 0
        if dry_run:
            return len(rows)
        self._client.insert(
            "market_data.cot_gold",
            [[r["report_date"], r["mm_long"], r["mm_short"], r["mm_spread"],
              r["mm_net"], r["comm_long"], r["comm_short"], r["comm_net"],
              r["open_interest"], r["source"]]
             for r in rows],
            column_names=["report_date", "mm_long", "mm_short", "mm_spread",
                          "mm_net", "comm_long", "comm_short", "comm_net",
                          "open_interest", "source"],
            settings={"max_partitions_per_insert_block": 300},
        )
        return len(rows)

    # ── Bulk insert: cb_gold_reserves ────────────────────────────────────────

    def insert_cb_reserves(
        self,
        rows: list[dict[str, Any]],
        *,
        dry_run: bool = False,
    ) -> int:
        """Insert IMF central bank gold reserve rows. Returns row count."""
        if not rows:
            return 0
        if dry_run:
            return len(rows)
        self._client.insert(
            "market_data.cb_gold_reserves",
            [[r["ref_period"], r["country_code"], r["country_name"],
              r["reserves_tonnes"], r["source"]]
             for r in rows],
            column_names=["ref_period", "country_code", "country_name",
                          "reserves_tonnes", "source"],
            settings={"max_partitions_per_insert_block": 300},
        )
        return len(rows)

    # ── Bulk insert: etf_aum ─────────────────────────────────────────────────

    def insert_etf_aum(
        self,
        rows: list[dict[str, Any]],
        *,
        dry_run: bool = False,
    ) -> int:
        """Insert ETF AUM snapshot rows. Returns row count."""
        if not rows:
            return 0
        if dry_run:
            return len(rows)
        self._client.insert(
            "market_data.etf_aum",
            [[r["trade_date"], r["symbol"], r["aum_usd"],
              r["price"], r["implied_tonnes"], r["source"]]
             for r in rows],
            column_names=["trade_date", "symbol", "aum_usd",
                          "price", "implied_tonnes", "source"],
            settings={"max_partitions_per_insert_block": 300},
        )
        return len(rows)

    # ── Bulk insert: fx_rates ─────────────────────────────────────────────────

    def insert_fx_rates(
        self,
        rows: list[dict[str, Any]],
        *,
        dry_run: bool = False,
    ) -> int:
        """Insert daily FX rate rows into fx_rates. Returns row count."""
        if not rows:
            return 0
        if dry_run:
            return len(rows)
        self._client.insert(
            "market_data.fx_rates",
            [[r["trade_date"], r["symbol"], r["open"], r["high"],
              r["low"], r["close"], r["source"]]
             for r in rows],
            column_names=["trade_date", "symbol", "open", "high", "low", "close", "source"],
            settings={"max_partitions_per_insert_block": 300},
        )
        return len(rows)

    # ── Bulk insert: fii_dii_flows ────────────────────────────────────────────

    def insert_fii_dii_flows(
        self,
        rows: list[dict[str, Any]],
        *,
        dry_run: bool = False,
    ) -> int:
        """
        Bulk-insert FII/DII institutional flow rows into fii_dii_flows.

        Each row dict must have keys:
            trade_date (date), fii_gross_buy_cr, fii_gross_sell_cr, fii_net_cr,
            dii_gross_buy_cr, dii_gross_sell_cr, dii_net_cr

        Returns the number of rows inserted (or that would have been inserted).
        """
        if not rows:
            return 0
        if dry_run:
            logger.info("[dry-run] Would insert %d FII/DII flow rows.", len(rows))
            return len(rows)
        self._client.insert(
            "market_data.fii_dii_flows",
            [
                [
                    r["trade_date"],
                    r["fii_gross_buy_cr"],
                    r["fii_gross_sell_cr"],
                    r["fii_net_cr"],
                    r["dii_gross_buy_cr"],
                    r["dii_gross_sell_cr"],
                    r["dii_net_cr"],
                ]
                for r in rows
            ],
            column_names=[
                "trade_date",
                "fii_gross_buy_cr",
                "fii_gross_sell_cr",
                "fii_net_cr",
                "dii_gross_buy_cr",
                "dii_gross_sell_cr",
                "dii_net_cr",
            ],
            settings={"max_partitions_per_insert_block": 300},
        )
        return len(rows)

    def insert_fii_dii_monthly(
        self,
        rows: list[dict],
        *,
        dry_run: bool = False,
    ) -> int:
        """
        Bulk-insert monthly FII/DII cash-market aggregate rows.

        Each row dict must have keys:
            month_date (date), fii_buy_cr, fii_sell_cr, fii_net_cr,
            dii_buy_cr, dii_sell_cr, dii_net_cr, nifty_close, nifty_change_pct
        """
        if not rows:
            return 0
        if dry_run:
            logger.info("[dry-run] Would insert %d FII/DII monthly rows.", len(rows))
            return len(rows)
        self._client.insert(
            "market_data.fii_dii_monthly",
            [
                [
                    r["month_date"], r["fii_buy_cr"], r["fii_sell_cr"], r["fii_net_cr"],
                    r["dii_buy_cr"], r["dii_sell_cr"], r["dii_net_cr"],
                    r["nifty_close"], r["nifty_change_pct"],
                ]
                for r in rows
            ],
            column_names=[
                "month_date", "fii_buy_cr", "fii_sell_cr", "fii_net_cr",
                "dii_buy_cr", "dii_sell_cr", "dii_net_cr",
                "nifty_close", "nifty_change_pct",
            ],
        )
        return len(rows)

    def insert_fii_dii_fno_daily(
        self,
        rows: list[dict],
        *,
        dry_run: bool = False,
    ) -> int:
        """
        Bulk-insert daily F&O participant OI rows.

        Each row must have the keys matching fii_dii_fno_daily columns
        (see _DDL_FII_DII_FNO_DAILY for full list).
        """
        if not rows:
            return 0
        if dry_run:
            logger.info("[dry-run] Would insert %d FII/DII F&O daily rows.", len(rows))
            return len(rows)
        cols = [
            "trade_date",
            "fii_fut_net_oi", "fii_fut_outstanding_oi",
            "fii_fut_nifty_net_oi", "fii_fut_banknifty_net_oi",
            "dii_fut_net_oi", "dii_fut_outstanding_oi",
            "pro_fut_net_oi", "pro_fut_outstanding_oi",
            "client_fut_net_oi", "client_fut_outstanding_oi",
            "fii_fut_stock_net_oi", "dii_fut_stock_net_oi",
            "pro_fut_stock_net_oi", "client_fut_stock_net_oi",
            "fii_opt_overall_net_oi", "fii_opt_overall_net_oi_change",
            "fii_opt_call_net_oi", "fii_opt_put_net_oi",
            "dii_opt_overall_net_oi", "dii_opt_overall_net_oi_change",
            "pro_opt_overall_net_oi", "pro_opt_overall_net_oi_change",
            "client_opt_overall_net_oi", "client_opt_overall_net_oi_change",
            "nifty_close", "banknifty_close",
            "nifty_change_pct", "banknifty_change_pct",
        ]
        self._client.insert(
            "market_data.fii_dii_fno_daily",
            [[r.get(c, 0.0) for c in cols] for r in rows],
            column_names=cols,
            settings={"max_partitions_per_insert_block": 300},
        )
        return len(rows)

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "ClickHouseImporter":
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()
