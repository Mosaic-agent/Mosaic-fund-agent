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
            _DDL_FX_RATES,
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
        )
        return len(rows)

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "ClickHouseImporter":
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()
