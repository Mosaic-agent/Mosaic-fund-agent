"""
src/deepdive/clickhouse.py
───────────────────────────
ClickHouse persistence layer for the company deep-dive pipeline.

Tables (all in `market_data` database, all ReplacingMergeTree — idempotent):

  deepdive_filings      — one row per filing downloaded (10-K, 10-Q, 8-K, DEF14A)
  deepdive_financials   — annual P&L / CF / BS rows from XBRL
  deepdive_segments     — product-family segment revenue rows from MD&A
  deepdive_valuation    — market snapshot (P/E, EV multiples, peer medians)
  deepdive_headcount    — headcount per fiscal period
  deepdive_exec_comp    — NEO compensation rows from DEF14A / ExecCompApi
  deepdive_jobs         — Workday job count per function × location

Watermark pattern (same as existing importer):
  `is_imported(ticker, source, period)` — check deepdive_watermarks
  `set_watermark(ticker, source, period)` — upsert after successful insert

Every row carries filing reference columns:
  filing_accession_no, filing_url, filing_form_type, filing_date

Design decisions
────────────────
  * ReplacingMergeTree(imported_at) → re-running the same ticker+period is safe
  * ORDER BY always starts with (ticker, ...) so per-ticker queries are fast
  * All monetary values in USD millions (Float64) to match models.py
  * Boolean presence check before each phase fetch avoids redundant API calls
"""

from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any

log = logging.getLogger(__name__)

# ── DDL ───────────────────────────────────────────────────────────────────────

_DDL_FILINGS = """
CREATE TABLE IF NOT EXISTS market_data.deepdive_filings (
    ticker              String,
    report_date         Date,        -- run date YYYY-MM-DD
    form_type           String,      -- 10-K | 10-Q | 8-K | DEF 14A
    filed_date          String,
    filing_url          String,
    accession_no        String,
    period_of_report    String,
    primary_doc         String,
    company_name        String,
    cik                 String,
    sic                 String,
    exchange            String,
    imported_at         DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(imported_at)
PARTITION BY toYYYYMM(report_date)
ORDER BY (ticker, report_date, form_type, filed_date)
"""

_DDL_FINANCIALS = """
CREATE TABLE IF NOT EXISTS market_data.deepdive_financials (
    ticker                  String,
    report_date             Date,        -- run date
    fiscal_year             String,      -- e.g. FY2025
    revenue_usd_m           Float64,
    gross_profit_usd_m      Float64,
    operating_income_usd_m  Float64,
    net_income_usd_m        Float64,
    free_cash_flow_usd_m    Float64,
    rd_expense_usd_m        Float64,
    capex_usd_m             Float64,
    gross_margin_pct        Float64,
    operating_margin_pct    Float64,
    -- filing reference
    filing_accession_no     String,
    filing_url              String,
    filing_form_type        String DEFAULT '10-K',
    filing_date             String,
    imported_at             DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(imported_at)
ORDER BY (ticker, report_date, fiscal_year)
"""

_DDL_SEGMENTS = """
CREATE TABLE IF NOT EXISTS market_data.deepdive_segments (
    ticker              String,
    report_date         Date,
    fiscal_year         String,
    segment_name        String,
    revenue_usd_m       Float64,
    yoy_growth_pct      Float64,
    -- filing reference
    filing_accession_no String,
    filing_url          String,
    filing_form_type    String DEFAULT '10-K',
    filing_date         String,
    imported_at         DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(imported_at)
ORDER BY (ticker, report_date, fiscal_year, segment_name)
"""

_DDL_VALUATION = """
CREATE TABLE IF NOT EXISTS market_data.deepdive_valuation (
    ticker                  String,
    report_date             Date,
    as_of_date              String,
    market_cap_usd_b        Float64,
    pe_trailing             Float64,
    pe_forward              Float64,
    ev_revenue              Float64,
    ev_ebitda               Float64,
    fcf_yield_pct           Float64,
    peer_pe_median          Float64,
    peer_ev_ebitda_median   Float64,
    peer_ev_revenue_median  Float64,
    source                  String DEFAULT 'yfinance',
    imported_at             DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(imported_at)
ORDER BY (ticker, report_date)
"""

_DDL_HEADCOUNT = """
CREATE TABLE IF NOT EXISTS market_data.deepdive_headcount (
    ticker              String,
    report_date         Date,
    fiscal_period       String,      -- e.g. FY2025
    total_headcount     Int64,
    yoy_change_pct      Float64,
    notes               String,
    -- filing reference
    filing_accession_no String,
    filing_url          String,
    filing_form_type    String DEFAULT '10-K',
    filing_date         String,
    imported_at         DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(imported_at)
ORDER BY (ticker, report_date, fiscal_period)
"""

_DDL_EXEC_COMP = """
CREATE TABLE IF NOT EXISTS market_data.deepdive_exec_comp (
    ticker              String,
    report_date         Date,
    fiscal_year         String,
    exec_name           String,
    position            String,
    salary_usd          Float64,
    stock_awards_usd    Float64,
    option_awards_usd   Float64,
    other_comp_usd      Float64,
    total_usd           Float64,
    stock_pct           Float64,      -- stock_awards / total * 100
    -- filing reference
    filing_accession_no String,       -- from DEF14A
    filing_url          String,
    filing_form_type    String DEFAULT 'DEF 14A',
    filing_date         String,
    imported_at         DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(imported_at)
ORDER BY (ticker, report_date, fiscal_year, exec_name)
"""

_DDL_JOBS = """
CREATE TABLE IF NOT EXISTS market_data.deepdive_jobs (
    ticker          String,
    report_date     Date,
    function_bucket String,      -- Engineering | Sales | Product | ...
    location        String,      -- city/country
    job_count       Int64,
    source          String DEFAULT 'workday',
    imported_at     DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(imported_at)
ORDER BY (ticker, report_date, function_bucket, location)
"""

_DDL_PRICES = """
CREATE TABLE IF NOT EXISTS market_data.deepdive_prices (
    ticker          String,
    trade_date      Date,
    open            Float64,
    high            Float64,
    low             Float64,
    close           Float64,
    volume          Int64,
    imported_at     DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(imported_at)
ORDER BY (ticker, trade_date)
"""

_DDL_REPORTS = """
CREATE TABLE IF NOT EXISTS market_data.deepdive_reports (
    ticker          String,
    report_date     Date,
    section_key     String,   -- 'core_business' | 'financials' | ... | '__full__' for the full report
    section_heading String,
    content_md      String,   -- full markdown text of this section
    imported_at     DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(imported_at)
ORDER BY (ticker, report_date, section_key)
"""

_DDL_WATERMARKS = """
CREATE TABLE IF NOT EXISTS market_data.deepdive_watermarks (
    ticker      String,
    source      String,      -- financials | segments | valuation | headcount | exec_comp | jobs | filings
    period      String,      -- fiscal_year or report_date
    imported_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(imported_at)
ORDER BY (ticker, source, period)
"""

_ALL_DDL = [
    _DDL_FILINGS,
    _DDL_FINANCIALS,
    _DDL_SEGMENTS,
    _DDL_VALUATION,
    _DDL_HEADCOUNT,
    _DDL_EXEC_COMP,
    _DDL_JOBS,
    _DDL_PRICES,
    _DDL_REPORTS,
    _DDL_WATERMARKS,
]


# ── Client wrapper ────────────────────────────────────────────────────────────

class DeepDiveStore:
    """
    ClickHouse persistence for the deep-dive pipeline.

    Instantiate once per run and pass around; lazy-creates all tables on first use.
    Falls back gracefully on connection errors — a CH failure never crashes the pipeline.
    """

    def __init__(self) -> None:
        self._client: Any = None
        self._ready = False
        self._connect()

    def _connect(self) -> None:
        try:
            import clickhouse_connect  # noqa: PLC0415
            from config.settings import settings  # noqa: PLC0415
            self._client = clickhouse_connect.get_client(
                host=settings.clickhouse_host,
                port=settings.clickhouse_port,
                username=settings.clickhouse_user,
                password=settings.clickhouse_password,
                connect_timeout=10,
            )
            for ddl in _ALL_DDL:
                self._client.command(ddl)
            self._ready = True
            log.debug("deepdive CH: schema verified")
        except Exception as exc:
            log.warning("deepdive CH: unavailable (%s) — data will not be persisted", exc)
            self._ready = False

    # ── Watermark helpers ──────────────────────────────────────────────────────

    def is_imported(self, ticker: str, source: str, period: str) -> bool:
        """Return True if this (ticker, source, period) was previously imported."""
        if not self._ready:
            return False
        try:
            r = self._client.query(
                "SELECT count() FROM market_data.deepdive_watermarks "
                "WHERE ticker = {t:String} AND source = {s:String} AND period = {p:String}",
                parameters={"t": ticker, "s": source, "p": period},
            )
            return int(r.result_rows[0][0]) > 0
        except Exception as exc:
            log.warning("deepdive CH: watermark check failed: %s", exc)
            return False

    def _set_watermark(self, ticker: str, source: str, period: str) -> None:
        if not self._ready:
            return
        try:
            self._client.insert(
                "market_data.deepdive_watermarks",
                [[ticker, source, period]],
                column_names=["ticker", "source", "period"],
            )
        except Exception as exc:
            log.warning("deepdive CH: set_watermark failed: %s", exc)

    # ── Insert: filings ────────────────────────────────────────────────────────

    def insert_filings(
        self,
        ticker: str,
        report_date: str,
        filings: list[dict],
        company_meta: dict,
    ) -> int:
        """
        Insert filing index rows into deepdive_filings.

        Each filing dict: {form_type, filed_date, filing_url, accession_no,
                           period_of_report, primary_doc}
        """
        if not self._ready or not filings:
            return 0
        rd = _parse_date(report_date)
        rows = [
            [
                ticker, rd,
                f.get("form_type", ""),
                f.get("filed_date", ""),
                f.get("filing_url", ""),
                f.get("accession_no", ""),
                f.get("period_of_report", ""),
                f.get("primary_doc", ""),
                company_meta.get("name", ""),
                company_meta.get("cik", ""),
                company_meta.get("sic", ""),
                company_meta.get("exchange", ""),
            ]
            for f in filings
        ]
        try:
            self._client.insert(
                "market_data.deepdive_filings",
                rows,
                column_names=[
                    "ticker", "report_date", "form_type", "filed_date",
                    "filing_url", "accession_no", "period_of_report", "primary_doc",
                    "company_name", "cik", "sic", "exchange",
                ],
            )
            self._set_watermark(ticker, "filings", report_date)
            log.info("deepdive CH: inserted %d filings for %s", len(rows), ticker)
            return len(rows)
        except Exception as exc:
            log.warning("deepdive CH: insert_filings failed: %s", exc)
            return 0

    # ── Insert: financials ─────────────────────────────────────────────────────

    def insert_financials(
        self,
        ticker: str,
        report_date: str,
        financials: list[Any],     # list[AnnualFinancials]
        filing_ref: dict,          # {accession_no, filing_url, filed_date}
    ) -> int:
        if not self._ready or not financials:
            return 0
        rd = _parse_date(report_date)
        rows = []
        for f in financials:
            rows.append([
                ticker, rd,
                _str(f, "fiscal_year"),
                _flt(f, "revenue_usd_m"),
                _flt(f, "gross_profit_usd_m"),
                _flt(f, "operating_income_usd_m"),
                _flt(f, "net_income_usd_m"),
                _flt(f, "free_cash_flow_usd_m"),
                _flt(f, "rd_expense_usd_m"),
                _flt(f, "capex_usd_m"),
                _flt(f, "gross_margin_pct"),
                _flt(f, "operating_margin_pct"),
                filing_ref.get("accession_no", ""),
                filing_ref.get("filing_url", ""),
                filing_ref.get("form_type", "10-K"),
                filing_ref.get("filed_date", ""),
            ])
        try:
            self._client.insert(
                "market_data.deepdive_financials",
                rows,
                column_names=[
                    "ticker", "report_date", "fiscal_year",
                    "revenue_usd_m", "gross_profit_usd_m", "operating_income_usd_m",
                    "net_income_usd_m", "free_cash_flow_usd_m", "rd_expense_usd_m",
                    "capex_usd_m", "gross_margin_pct", "operating_margin_pct",
                    "filing_accession_no", "filing_url", "filing_form_type", "filing_date",
                ],
            )
            self._set_watermark(ticker, "financials", report_date)
            log.info("deepdive CH: inserted %d financials rows for %s", len(rows), ticker)
            return len(rows)
        except Exception as exc:
            log.warning("deepdive CH: insert_financials failed: %s", exc)
            return 0

    # ── Insert: segments ───────────────────────────────────────────────────────

    def insert_segments(
        self,
        ticker: str,
        report_date: str,
        fiscal_year: str,
        segments: list[Any],      # list[SegmentRevenue]
        filing_ref: dict,
    ) -> int:
        if not self._ready or not segments:
            return 0
        rd = _parse_date(report_date)
        rows = [
            [
                ticker, rd, fiscal_year,
                _str(s, "name"),
                _flt(s, "revenue_usd_m"),
                _flt(s, "yoy_growth_pct"),
                filing_ref.get("accession_no", ""),
                filing_ref.get("filing_url", ""),
                filing_ref.get("form_type", "10-K"),
                filing_ref.get("filed_date", ""),
            ]
            for s in segments
        ]
        try:
            self._client.insert(
                "market_data.deepdive_segments",
                rows,
                column_names=[
                    "ticker", "report_date", "fiscal_year", "segment_name",
                    "revenue_usd_m", "yoy_growth_pct",
                    "filing_accession_no", "filing_url", "filing_form_type", "filing_date",
                ],
            )
            self._set_watermark(ticker, "segments", report_date)
            log.info("deepdive CH: inserted %d segment rows for %s", len(rows), ticker)
            return len(rows)
        except Exception as exc:
            log.warning("deepdive CH: insert_segments failed: %s", exc)
            return 0

    # ── Insert: valuation ──────────────────────────────────────────────────────

    def insert_valuation(
        self,
        ticker: str,
        report_date: str,
        valuation: Any,           # ValuationSnapshot
    ) -> int:
        if not self._ready or valuation is None:
            return 0
        rd = _parse_date(report_date)
        try:
            self._client.insert(
                "market_data.deepdive_valuation",
                [[
                    ticker, rd,
                    _str(valuation, "as_of_date"),
                    _flt(valuation, "market_cap_usd_b"),
                    _flt(valuation, "pe_trailing"),
                    _flt(valuation, "pe_forward"),
                    _flt(valuation, "ev_revenue"),
                    _flt(valuation, "ev_ebitda"),
                    _flt(valuation, "fcf_yield_pct"),
                    _flt(valuation, "peer_pe_median"),
                    _flt(valuation, "peer_ev_ebitda_median"),
                    _flt(valuation, "peer_ev_revenue_median"),
                ]],
                column_names=[
                    "ticker", "report_date", "as_of_date",
                    "market_cap_usd_b", "pe_trailing", "pe_forward",
                    "ev_revenue", "ev_ebitda", "fcf_yield_pct",
                    "peer_pe_median", "peer_ev_ebitda_median", "peer_ev_revenue_median",
                ],
            )
            self._set_watermark(ticker, "valuation", report_date)
            log.info("deepdive CH: inserted valuation for %s on %s", ticker, report_date)
            return 1
        except Exception as exc:
            log.warning("deepdive CH: insert_valuation failed: %s", exc)
            return 0

    # ── Insert: headcount ──────────────────────────────────────────────────────

    def insert_headcount(
        self,
        ticker: str,
        report_date: str,
        headcount: list[Any],     # list[HeadcountData]
        filing_ref: dict,
    ) -> int:
        if not self._ready or not headcount:
            return 0
        rd = _parse_date(report_date)
        rows = [
            [
                ticker, rd,
                _str(h, "period"),
                int(_flt(h, "total_headcount") or 0),
                _flt(h, "yoy_change_pct"),
                _str(h, "notes"),
                filing_ref.get("accession_no", ""),
                filing_ref.get("filing_url", ""),
                filing_ref.get("form_type", "10-K"),
                filing_ref.get("filed_date", ""),
            ]
            for h in headcount
        ]
        try:
            self._client.insert(
                "market_data.deepdive_headcount",
                rows,
                column_names=[
                    "ticker", "report_date", "fiscal_period",
                    "total_headcount", "yoy_change_pct", "notes",
                    "filing_accession_no", "filing_url", "filing_form_type", "filing_date",
                ],
            )
            self._set_watermark(ticker, "headcount", report_date)
            log.info("deepdive CH: inserted %d headcount rows for %s", len(rows), ticker)
            return len(rows)
        except Exception as exc:
            log.warning("deepdive CH: insert_headcount failed: %s", exc)
            return 0

    # ── Insert: exec comp ──────────────────────────────────────────────────────

    def insert_exec_comp(
        self,
        ticker: str,
        report_date: str,
        summaries: list[dict],    # from summarise_exec_comp()
        filing_ref: dict,         # from DEF14A filing
    ) -> int:
        if not self._ready or not summaries:
            return 0
        rd = _parse_date(report_date)
        rows = [
            [
                ticker, rd,
                str(s.get("year") or ""),
                s.get("name", ""),
                s.get("position", ""),
                float(s.get("salary_usd", 0) or 0),
                float(s.get("stock_awards_usd", 0) or 0),
                float(s.get("option_awards_usd", 0) or 0),
                float(s.get("other_comp_usd", 0) or 0),
                float(s.get("total_usd_m", 0) or 0) * 1_000_000,  # store in USD not $M
                float(s.get("stock_pct", 0) or 0),
                filing_ref.get("accession_no", ""),
                filing_ref.get("filing_url", ""),
                filing_ref.get("form_type", "DEF 14A"),
                filing_ref.get("filed_date", ""),
            ]
            for s in summaries
        ]
        try:
            self._client.insert(
                "market_data.deepdive_exec_comp",
                rows,
                column_names=[
                    "ticker", "report_date", "fiscal_year",
                    "exec_name", "position",
                    "salary_usd", "stock_awards_usd", "option_awards_usd",
                    "other_comp_usd", "total_usd", "stock_pct",
                    "filing_accession_no", "filing_url", "filing_form_type", "filing_date",
                ],
            )
            self._set_watermark(ticker, "exec_comp", report_date)
            log.info("deepdive CH: inserted %d exec_comp rows for %s", len(rows), ticker)
            return len(rows)
        except Exception as exc:
            log.warning("deepdive CH: insert_exec_comp failed: %s", exc)
            return 0

    # ── Insert: jobs ───────────────────────────────────────────────────────────

    def insert_jobs(
        self,
        ticker: str,
        report_date: str,
        jobs: dict,               # {function: {location: count}}
    ) -> int:
        if not self._ready or not jobs:
            return 0
        rd = _parse_date(report_date)
        rows = [
            [ticker, rd, func, loc, int(count)]
            for func, locs in jobs.items()
            for loc, count in locs.items()
        ]
        if not rows:
            return 0
        try:
            self._client.insert(
                "market_data.deepdive_jobs",
                rows,
                column_names=["ticker", "report_date", "function_bucket", "location", "job_count"],
            )
            self._set_watermark(ticker, "jobs", report_date)
            log.info("deepdive CH: inserted %d job rows for %s", len(rows), ticker)
            return len(rows)
        except Exception as exc:
            log.warning("deepdive CH: insert_jobs failed: %s", exc)
            return 0

    # ── Read-back: load dataset from ClickHouse ────────────────────────────────

    def load_headcount(self, ticker: str, report_date: str) -> list[dict]:
        if not self._ready:
            return []
        try:
            r = self._client.query(
                "SELECT fiscal_period, total_headcount, yoy_change_pct, notes, "
                "filing_accession_no, filing_url, filing_form_type, filing_date "
                "FROM market_data.deepdive_headcount FINAL "
                "WHERE ticker = {t:String} AND report_date = {d:Date} "
                "ORDER BY report_date",
                parameters={"t": ticker, "d": report_date},
            )
            cols = ["period", "total_headcount", "yoy_change_pct", "notes",
                    "filing_accession_no", "filing_url", "filing_form_type", "filing_date"]
            return [dict(zip(cols, row)) for row in r.result_rows]
        except Exception as exc:
            log.warning("deepdive CH: load_headcount failed: %s", exc)
            return []

    def load_exec_comp(self, ticker: str, report_date: str) -> list[dict]:
        if not self._ready:
            return []
        try:
            r = self._client.query(
                "SELECT fiscal_year, exec_name, position, "
                "salary_usd, stock_awards_usd, option_awards_usd, other_comp_usd, "
                "total_usd, stock_pct "
                "FROM market_data.deepdive_exec_comp FINAL "
                "WHERE ticker = {t:String} AND report_date = {d:Date} "
                "ORDER BY total_usd DESC",
                parameters={"t": ticker, "d": report_date},
            )
            cols = ["year", "name", "position",
                    "salary_usd", "stock_awards_usd", "option_awards_usd", "other_comp_usd",
                    "total_usd_m", "stock_pct"]
            rows = []
            for row in r.result_rows:
                d = dict(zip(cols, row))
                d["total_usd_m"] = round(float(d["total_usd_m"]) / 1_000_000, 2)
                rows.append(d)
            return rows
        except Exception as exc:
            log.warning("deepdive CH: load_exec_comp failed: %s", exc)
            return []

    # ── Insert / load: price history ──────────────────────────────────────────

    def insert_prices(self, ticker: str, rows: list[dict]) -> int:
        """
        Persist 2-year OHLCV price history rows for a ticker.

        Args:
            ticker: e.g. "ADSK"
            rows:   list of {date, open, high, low, close, volume}

        Returns:
            Number of rows inserted (0 on failure or CH unavailable).
        """
        if not self._ready or not rows:
            return 0
        data = [
            [
                ticker,
                _parse_date(r["date"]),
                float(r.get("open", 0) or 0),
                float(r.get("high", 0) or 0),
                float(r.get("low", 0) or 0),
                float(r.get("close", 0) or 0),
                int(r.get("volume", 0) or 0),
            ]
            for r in rows
        ]
        try:
            self._client.insert(
                "market_data.deepdive_prices",
                data,
                column_names=["ticker", "trade_date", "open", "high", "low", "close", "volume"],
            )
            # watermark: use the latest date in the dataset as the period
            last_date = max(r["date"] for r in rows)
            self._set_watermark(ticker, "prices", last_date)
            log.info("deepdive CH: inserted %d price rows for %s (latest: %s)", len(data), ticker, last_date)
            return len(data)
        except Exception as exc:
            log.warning("deepdive CH: insert_prices failed: %s", exc)
            return 0

    def load_prices(self, ticker: str, years: int = 2) -> list[dict]:
        """
        Load price history from ClickHouse for charting.

        Returns list of {date, open, high, low, close, volume} dicts sorted by date.
        """
        if not self._ready:
            return []
        try:
            cutoff = f"today() - INTERVAL {years} YEAR"
            r = self._client.query(
                f"SELECT toString(trade_date), open, high, low, close, volume "
                f"FROM market_data.deepdive_prices FINAL "
                f"WHERE ticker = {{t:String}} AND trade_date >= {cutoff} "
                f"ORDER BY trade_date",
                parameters={"t": ticker},
            )
            cols = ["date", "open", "high", "low", "close", "volume"]
            return [dict(zip(cols, row)) for row in r.result_rows]
        except Exception as exc:
            log.warning("deepdive CH: load_prices failed: %s", exc)
            return []

    def prices_up_to_date(self, ticker: str) -> bool:
        """Return True if price history was imported within the last 24 hours."""
        if not self._ready:
            return False
        try:
            r = self._client.query(
                "SELECT max(imported_at) FROM market_data.deepdive_prices "
                "WHERE ticker = {t:String}",
                parameters={"t": ticker},
            )
            if not r.result_rows or r.result_rows[0][0] is None:
                return False
            from datetime import datetime, timezone  # noqa: PLC0415
            last = r.result_rows[0][0]
            if not isinstance(last, datetime):
                return False
            age_hours = (datetime.now(timezone.utc) - last.replace(tzinfo=timezone.utc)).total_seconds() / 3600
            return age_hours < 24
        except Exception:
            return False

    # ── Insert / load: report sections ────────────────────────────────────────

    def insert_report(
        self,
        ticker: str,
        report_date: str,
        sections: dict[str, tuple[str, str]],   # key → (heading, markdown_text)
        full_report_md: str,
        sources_md: str,
    ) -> int:
        """
        Persist all section markdown + the full assembled report to ClickHouse.

        Args:
            ticker:        e.g. "ADSK"
            report_date:   YYYY-MM-DD run date
            sections:      {section_key: (heading, markdown_text)}
            full_report_md: full report.md content
            sources_md:    sources.md content

        Returns:
            number of rows inserted (sections + 2 special rows)
        """
        if not self._ready:
            return 0
        rd = _parse_date(report_date)
        rows = []
        for key, (heading, content) in sections.items():
            rows.append([ticker, rd, key, heading, content])
        # Store the full assembled report + sources as special rows
        rows.append([ticker, rd, "__full__", "Full Report", full_report_md])
        rows.append([ticker, rd, "__sources__", "Sources", sources_md])
        try:
            self._client.insert(
                "market_data.deepdive_reports",
                rows,
                column_names=["ticker", "report_date", "section_key", "section_heading", "content_md"],
            )
            self._set_watermark(ticker, "report", report_date)
            log.info("deepdive CH: inserted %d report rows for %s", len(rows), ticker)
            return len(rows)
        except Exception as exc:
            log.warning("deepdive CH: insert_report failed: %s", exc)
            return 0

    def load_report(self, ticker: str, report_date: str) -> dict[str, str]:
        """
        Load all report sections for a ticker+date from ClickHouse.

        Returns:
            dict mapping section_key → content_md
            Keys include '__full__' (full report) and '__sources__' (sources provenance).
            Empty dict if nothing found or CH unavailable.
        """
        if not self._ready:
            return {}
        try:
            r = self._client.query(
                "SELECT section_key, content_md "
                "FROM market_data.deepdive_reports FINAL "
                "WHERE ticker = {t:String} AND report_date = {d:Date}",
                parameters={"t": ticker, "d": report_date},
            )
            return {row[0]: row[1] for row in r.result_rows}
        except Exception as exc:
            log.warning("deepdive CH: load_report failed: %s", exc)
            return {}

    def load_financials(self, ticker: str, report_date: str) -> list[dict]:
        """Load financials for ticker from CH (used instead of XBRL re-fetch)."""
        if not self._ready:
            return []
        try:
            r = self._client.query(
                "SELECT fiscal_year, revenue_usd_m, gross_profit_usd_m, "
                "operating_income_usd_m, net_income_usd_m, free_cash_flow_usd_m, "
                "rd_expense_usd_m, capex_usd_m, gross_margin_pct, operating_margin_pct, "
                "filing_accession_no, filing_url, filing_form_type, filing_date "
                "FROM market_data.deepdive_financials FINAL "
                "WHERE ticker = {t:String} AND report_date = {d:Date} "
                "ORDER BY fiscal_year",
                parameters={"t": ticker, "d": report_date},
            )
            cols = [
                "fiscal_year", "revenue_usd_m", "gross_profit_usd_m",
                "operating_income_usd_m", "net_income_usd_m", "free_cash_flow_usd_m",
                "rd_expense_usd_m", "capex_usd_m", "gross_margin_pct", "operating_margin_pct",
                "filing_accession_no", "filing_url", "filing_form_type", "filing_date",
            ]
            return [dict(zip(cols, row)) for row in r.result_rows]
        except Exception as exc:
            log.warning("deepdive CH: load_financials failed: %s", exc)
            return []

    def load_segments(self, ticker: str, report_date: str) -> list[dict]:
        if not self._ready:
            return []
        try:
            r = self._client.query(
                "SELECT segment_name, revenue_usd_m, yoy_growth_pct, "
                "filing_accession_no, filing_url, filing_form_type, filing_date "
                "FROM market_data.deepdive_segments FINAL "
                "WHERE ticker = {t:String} AND report_date = {d:Date} "
                "ORDER BY revenue_usd_m DESC",
                parameters={"t": ticker, "d": report_date},
            )
            cols = ["name", "revenue_usd_m", "yoy_growth_pct",
                    "filing_accession_no", "filing_url", "filing_form_type", "filing_date"]
            return [dict(zip(cols, row)) for row in r.result_rows]
        except Exception as exc:
            log.warning("deepdive CH: load_segments failed: %s", exc)
            return []

    def load_valuation(self, ticker: str, report_date: str) -> dict | None:
        if not self._ready:
            return None
        try:
            r = self._client.query(
                "SELECT as_of_date, market_cap_usd_b, pe_trailing, pe_forward, "
                "ev_revenue, ev_ebitda, fcf_yield_pct, "
                "peer_pe_median, peer_ev_ebitda_median, peer_ev_revenue_median "
                "FROM market_data.deepdive_valuation FINAL "
                "WHERE ticker = {t:String} AND report_date = {d:Date}",
                parameters={"t": ticker, "d": report_date},
            )
            if not r.result_rows:
                return None
            cols = ["as_of_date", "market_cap_usd_b", "pe_trailing", "pe_forward",
                    "ev_revenue", "ev_ebitda", "fcf_yield_pct",
                    "peer_pe_median", "peer_ev_ebitda_median", "peer_ev_revenue_median"]
            return dict(zip(cols, r.result_rows[0]))
        except Exception as exc:
            log.warning("deepdive CH: load_valuation failed: %s", exc)
            return None

    def load_jobs(self, ticker: str, report_date: str) -> dict:
        """Return jobs as {function: {location: count}} dict."""
        if not self._ready:
            return {}
        try:
            r = self._client.query(
                "SELECT function_bucket, location, job_count "
                "FROM market_data.deepdive_jobs FINAL "
                "WHERE ticker = {t:String} AND report_date = {d:Date}",
                parameters={"t": ticker, "d": report_date},
            )
            out: dict = {}
            for func, loc, count in r.result_rows:
                out.setdefault(func, {})[loc] = int(count)
            return out
        except Exception as exc:
            log.warning("deepdive CH: load_jobs failed: %s", exc)
            return {}


# ── Private helpers ────────────────────────────────────────────────────────────

def _parse_date(d: str) -> date:
    """Parse YYYY-MM-DD string to date; fall back to today."""
    try:
        return datetime.strptime(d, "%Y-%m-%d").date()
    except Exception:
        return date.today()


def _flt(obj: Any, attr: str) -> float:
    """Get float attribute from object or dict; return 0.0 on missing/None."""
    val = getattr(obj, attr, None) if not isinstance(obj, dict) else obj.get(attr)
    try:
        return float(val) if val is not None else 0.0
    except (TypeError, ValueError):
        return 0.0


def _str(obj: Any, attr: str) -> str:
    val = getattr(obj, attr, None) if not isinstance(obj, dict) else obj.get(attr)
    return str(val) if val is not None else ""
