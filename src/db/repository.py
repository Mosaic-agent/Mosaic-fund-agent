"""
src/db/repository.py
────────────────────
MarketDataRepository — single access point for all ClickHouse reads.

Centralises:
  • Consistent FINAL usage on ReplacingMergeTree tables
  • Typed return shapes (tuples, dicts, DataFrames)
  • One place to add caching, retries, or schema changes

Usage
─────
    from src.db.repository import MarketDataRepository
    from src.db.pool import get_pool

    repo = MarketDataRepository(get_pool())
    fii, dii = repo.fii_dii_5d()
    pred      = repo.latest_ml_prediction()
    ohlcv_df  = repo.goldbees_ohlcv()
"""
from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Any

import pandas as pd

log = logging.getLogger(__name__)


# ── Result type for run_fetcher ───────────────────────────────────────────────

from dataclasses import dataclass, field


@dataclass
class FetchResult:
    """Outcome of a single MarketDataRepository.run_fetcher() call."""
    fetcher:   Any
    n:         int
    from_date: "date"
    to_date:   "date"
    dry_run:   bool = False
    skipped:   bool = False

    @property
    def source(self) -> str:
        return getattr(self.fetcher, "source_name", "?")

    @property
    def label(self) -> str:
        return getattr(self.fetcher, "description", self.source)

    def __str__(self) -> str:
        tag = " (dry-run)" if self.dry_run else (" (skipped)" if self.skipped else "")
        return f"{self.label}: {self.n} rows  {self.from_date}→{self.to_date}{tag}"


def _date_today() -> "date":
    from datetime import date as _d
    return _d.today()


class MarketDataRepository:
    """
    Typed read interface over the market_data ClickHouse database.

    Pass an CHPool instance (from src.db.pool.get_pool()). The repository
    borrows a client per call — it does not hold a long-lived connection.
    """

    def __init__(self, pool) -> None:
        self._pool = pool

    def _q(self, sql: str, parameters: dict | None = None):
        """Execute a query and return raw result rows."""
        with self._pool.get_client() as c:
            return c.query(sql, parameters=parameters or {}).result_rows

    def _qdf(self, sql: str) -> pd.DataFrame:
        """Execute a query and return a DataFrame."""
        return self._pool.query_df(sql)

    # ── FII / DII ─────────────────────────────────────────────────────────────

    def fii_dii_5d(self) -> tuple[float, float]:
        """5-day rolling FII and DII net flows in ₹ Crore."""
        rows = self._q(
            "SELECT sum(fii_net_cr), sum(dii_net_cr) "
            "FROM market_data.fii_dii_flows FINAL "
            "WHERE trade_date >= today() - 5"
        )
        if rows and rows[0][0] is not None:
            return float(rows[0][0] or 0), float(rows[0][1] or 0)
        return 0.0, 0.0

    # ── News sentiment ────────────────────────────────────────────────────────

    def news_sentiment_rows(self, days: int = 7) -> list[tuple[str, str]]:
        """
        Recent news rows: (etfs_impacted, sentiment).
        sentiment values: 'POSITIVE' | 'NEGATIVE' | 'NEUTRAL'
        """
        rows = self._q(
            f"SELECT etfs_impacted, sentiment "
            f"FROM market_data.news_articles "
            f"WHERE fetched_at >= now() - INTERVAL {days} DAY"
        )
        return [(str(r[0]), str(r[1])) for r in rows]

    # ── ML predictions ───────────────────────────────────────────────────────

    def latest_ml_prediction(self) -> dict[str, Any] | None:
        """Latest GOLDBEES ML prediction row, or None if table is empty."""
        rows = self._q(
            "SELECT expected_return_pct, prob_up, regime_signal, cv_auc_mean, as_of "
            "FROM market_data.ml_predictions FINAL "
            "ORDER BY as_of DESC LIMIT 1"
        )
        if not rows:
            return None
        r = rows[0]
        return {
            "expected_return_pct": float(r[0]),
            "prob_up":             float(r[1]),
            "regime_signal":       str(r[2]),
            "cv_auc_mean":         float(r[3]),
            "as_of":               r[4],
        }

    # ── Price data ───────────────────────────────────────────────────────────

    def ohlcv(self, symbol: str, category: str) -> pd.DataFrame:
        """
        Full OHLCV history for a symbol, deduplicated via argMax on imported_at.
        Returns columns: trade_date, open, high, low, close, volume.
        """
        return self._qdf(
            f"SELECT trade_date, "
            f"argMax(open, imported_at) AS open, "
            f"argMax(high, imported_at) AS high, "
            f"argMax(low, imported_at) AS low, "
            f"argMax(close, imported_at) AS close, "
            f"argMax(volume, imported_at) AS volume "
            f"FROM market_data.daily_prices "
            f"WHERE symbol = '{symbol}' AND category = '{category}' "
            f"GROUP BY trade_date ORDER BY trade_date ASC"
        )

    def latest_close(self, symbols: list[str], category: str) -> dict[str, float]:
        """Latest closing price for each symbol in the list."""
        sym_in = ", ".join(f"'{s}'" for s in symbols)
        rows = self._q(
            f"SELECT symbol, argMax(close, trade_date) "
            f"FROM market_data.daily_prices FINAL "
            f"WHERE symbol IN ({sym_in}) AND category = '{category}' "
            f"GROUP BY symbol"
        )
        return {str(r[0]): float(r[1]) for r in rows}

    # ── iNAV snapshots ────────────────────────────────────────────────────────

    def inav_latest_and_history(
        self,
        symbols: list[str],
        lookback_days: int = 30,
    ) -> tuple[dict[str, float], dict[str, list[float]]]:
        """
        Two-query batch for premium/discount analytics.

        Returns
        -------
        latest_map  : {symbol: latest_premium_pct}
        hist_map    : {symbol: [hourly_premium_pct, ...]}  within lookback window
        """
        from collections import defaultdict

        sym_in = ", ".join(f"'{s}'" for s in symbols)
        cutoff = (date.today() - timedelta(days=lookback_days)).isoformat()

        latest_rows = self._q(
            f"SELECT symbol, argMax(premium_discount_pct, snapshot_at) "
            f"FROM market_data.inav_snapshots "
            f"WHERE symbol IN ({sym_in}) GROUP BY symbol"
        )
        latest_map = {r[0]: float(r[1]) for r in latest_rows if r[1] is not None}

        hist_rows = self._q(
            f"SELECT symbol, toStartOfHour(snapshot_at), "
            f"argMax(premium_discount_pct, snapshot_at) "
            f"FROM market_data.inav_snapshots "
            f"WHERE symbol IN ({sym_in}) "
            f"  AND snapshot_at >= toDateTime('{cutoff} 00:00:00') "
            f"GROUP BY symbol, toStartOfHour(snapshot_at) "
            f"ORDER BY symbol, toStartOfHour(snapshot_at) ASC"
        )
        hist_map: dict[str, list[float]] = defaultdict(list)
        for r in hist_rows:
            hist_map[r[0]].append(float(r[2]))

        return latest_map, dict(hist_map)

    # ── Watermarks ───────────────────────────────────────────────────────────

    def get_watermark(self, source: str, symbol: str) -> "date | None":
        """Last successfully imported date for (source, symbol)."""
        from src.importer.clickhouse import ClickHouseImporter
        ch = ClickHouseImporter(**self._ch_kwargs())
        try:
            return ch.get_watermark(source, symbol)
        finally:
            ch.close()

    def set_watermark(self, source: str, symbol: str, last_date: "date") -> None:
        from src.importer.clickhouse import ClickHouseImporter
        ch = ClickHouseImporter(**self._ch_kwargs())
        try:
            ch.set_watermark(source, symbol, last_date)
        finally:
            ch.close()

    # ── Generic fetcher runner ────────────────────────────────────────────────

    def run_fetcher(
        self,
        fetcher: "Fetcher",
        *,
        dry_run: bool = False,
        full: bool = False,
        lookback_days: int = 3650,
    ) -> "FetchResult":
        """
        Execute the full fetch → validate → insert → watermark cycle.

        Parameters
        ----------
        fetcher       : a Fetcher adapter instance
        dry_run       : fetch but do not write to ClickHouse
        full          : ignore watermarks; re-fetch full lookback window
        lookback_days : history depth on first import or full re-import

        Returns a FetchResult with row count and date range.
        """
        from datetime import timedelta
        from src.importer.clickhouse import ClickHouseImporter

        today = _date_today()
        ch = ClickHouseImporter(**self._ch_kwargs())
        try:
            ch.ensure_schema()

            # Determine start date
            if full:
                from_date = today - timedelta(days=lookback_days)
            else:
                wm = ch.get_watermark(fetcher.source_name, fetcher.symbol_key)
                if wm is None:
                    from_date = today - timedelta(days=lookback_days)
                else:
                    from_date = wm - timedelta(days=fetcher.overlap_days)

            rows = fetcher.fetch(from_date, today)
            if not rows:
                return FetchResult(fetcher=fetcher, n=0, from_date=from_date,
                                   to_date=today, skipped=True)

            rows = fetcher.validate(rows)

            if dry_run:
                return FetchResult(fetcher=fetcher, n=len(rows), from_date=from_date,
                                   to_date=fetcher.max_date(rows), dry_run=True)

            n = fetcher.insert(rows, ch)
            max_dt = fetcher.max_date(rows)
            ch.set_watermark(fetcher.source_name, fetcher.symbol_key, max_dt)

            result = FetchResult(fetcher=fetcher, n=n, from_date=from_date, to_date=max_dt)

            # Publish post-import event — observers react asynchronously
            self._publish_imported(fetcher, result)

            return result

        finally:
            ch.close()

    def _publish_imported(self, fetcher, result: "FetchResult") -> None:
        """Fire DataImportedEvent on the global EventBus after a successful insert."""
        try:
            from src.events.bus import get_event_bus, DataImportedEvent
            event = DataImportedEvent(
                source     = fetcher.source_name,
                category   = getattr(fetcher, "category", fetcher.symbol_key.lower()),
                symbol_key = fetcher.symbol_key,
                n_rows     = result.n,
                from_date  = result.from_date,
                to_date    = result.to_date,
            )
            get_event_bus().publish(event)
        except Exception as exc:
            log.warning("EventBus publish failed (non-fatal): %s", exc)

    def _ch_kwargs(self) -> dict:
        """Extract ClickHouse connection params from the pool config."""
        try:
            client = self._pool._pool[0] if hasattr(self._pool, "_pool") else None
            if client and hasattr(client, "_url"):
                import re
                m = re.match(r"http://([^:]+):(\d+)", str(client._url))
                if m:
                    return {"host": m.group(1), "port": int(m.group(2))}
        except Exception:
            pass
        return {}  # ClickHouseImporter defaults to localhost:8123

    # ── Signal composite ─────────────────────────────────────────────────────

    def latest_signal_composite(self, symbols: list[str]) -> dict[str, dict]:
        """Latest composite signal row per symbol."""
        sym_in = ", ".join(f"'{s}'" for s in symbols)
        rows = self._q(
            f"SELECT etf_symbol, composite_score, action, anomaly_flag "
            f"FROM market_data.signal_composite FINAL "
            f"WHERE etf_symbol IN ({sym_in}) "
            f"ORDER BY etf_symbol, as_of DESC LIMIT {len(symbols)} BY etf_symbol"
        )
        return {
            r[0]: {"composite_score": float(r[1]), "action": str(r[2]), "anomaly_flag": str(r[3])}
            for r in rows
        }
