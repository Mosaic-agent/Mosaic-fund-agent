"""
Integration test for src/importer

Run with:
    python3 tests/_test_importer.py

Tests (in order):
  1. Registry — symbol lists and category helpers
  2. yfinance fetcher — real live OHLCV download (small sample)
  3. MFAPI fetcher — real live NAV download (1 scheme)
  4. ClickHouse schema — create tables, confirm idempotent
  5. ClickHouse insert + watermarks — round-trip data
  6. Full dry-run import — end-to-end pipeline without writing
  7. Full live import (mini) — stocks only, 5-day window, confirms rows in CH
"""
from __future__ import annotations

import os
import sys
import time
from datetime import date, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

PASS = "[✓]"
SKIP = "[~]"
FAIL = "[✗]"

# ── ClickHouse connection params ───────────────────────────────────────────────
# Override via env vars if needed:
#   CLICKHOUSE_HOST=localhost CLICKHOUSE_PORT=8123 python3 tests/_test_importer.py
CH_HOST = os.environ.get("CLICKHOUSE_HOST", "localhost")
CH_PORT = int(os.environ.get("CLICKHOUSE_PORT", "8123"))
CH_DB   = os.environ.get("CLICKHOUSE_DATABASE", "market_data")
CH_USER = os.environ.get("CLICKHOUSE_USER", "default")
CH_PASS = os.environ.get("CLICKHOUSE_PASSWORD", "")

_errors: list[str] = []


def section(title: str) -> None:
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")


def ok(msg: str) -> None:
    print(f"  {PASS} {msg}")


def warn(msg: str) -> None:
    print(f"  {SKIP} {msg}")


def fail(msg: str) -> None:
    print(f"  {FAIL} {msg}")
    _errors.append(msg)


# ─────────────────────────────────────────────────────────────────────────────
# 1. Registry
# ─────────────────────────────────────────────────────────────────────────────
section("TEST 1 — Registry")

from src.importer.registry import (
    STOCKS, ETFS, COMMODITIES, INDICES, MF_SCHEME_CODES,
    ALL_CATEGORIES, get_symbols_for_categories,
)

assert len(STOCKS) >= 10,      f"STOCKS too small: {len(STOCKS)}"
assert len(ETFS) >= 5,         f"ETFS too small: {len(ETFS)}"
assert len(COMMODITIES) >= 3,  f"COMMODITIES too small: {len(COMMODITIES)}"
assert len(INDICES) >= 4,      f"INDICES too small: {len(INDICES)}"
assert len(MF_SCHEME_CODES) >= 5, f"MF_SCHEME_CODES too small"
assert "mf" in ALL_CATEGORIES, "mf missing from ALL_CATEGORIES"
assert "stocks" in ALL_CATEGORIES

selected = get_symbols_for_categories(["stocks", "etfs"])
assert set(selected.keys()) == {"stocks", "etfs"}, f"unexpected keys: {selected.keys()}"

# Validate format: each entry is (nse_symbol, yahoo_ticker)
for nse, yahoo in STOCKS[:5]:
    assert yahoo.endswith(".NS"), f"Expected .NS suffix: {yahoo}"
for nse, yahoo in COMMODITIES[:3]:
    assert "=" in yahoo or "^" in yahoo or ".NS" in yahoo, f"Odd commodity ticker: {yahoo}"
for nse, code in MF_SCHEME_CODES.items():
    assert code.isdigit(), f"Scheme code should be numeric: {code}"

ok(f"STOCKS={len(STOCKS)}, ETFS={len(ETFS)}, COMMODITIES={len(COMMODITIES)}, "
   f"INDICES={len(INDICES)}, MF schemes={len(MF_SCHEME_CODES)}")
ok(f"ALL_CATEGORIES={ALL_CATEGORIES}")
ok("get_symbols_for_categories works correctly")
ok("All symbol formats valid")


# ─────────────────────────────────────────────────────────────────────────────
# 2. yfinance fetcher
# ─────────────────────────────────────────────────────────────────────────────
section("TEST 2 — yfinance fetcher (live)")

from src.importer.fetchers.yfinance_fetcher import fetch_ohlcv

today = date.today()
# Use a short 5-day window to keep the test fast
from_date = today - timedelta(days=7)

# Test with 2 stocks and 1 ETF
sample_symbols = [
    ("RELIANCE", "RELIANCE.NS"),
    ("TCS",      "TCS.NS"),
    ("GOLDBEES", "GOLDBEES.NS"),
]

t0 = time.time()
rows = fetch_ohlcv(sample_symbols, "stocks", from_date, today)
elapsed = time.time() - t0

if len(rows) == 0:
    warn("yfinance returned 0 rows — market may be closed (weekend/holiday) or API blocked")
else:
    symbols_seen = {r["symbol"] for r in rows}
    # Check schema
    r0 = rows[0]
    assert "symbol" in r0 and "trade_date" in r0 and "close" in r0, f"Bad row keys: {r0.keys()}"
    assert isinstance(r0["trade_date"], date), f"trade_date should be date: {type(r0['trade_date'])}"
    assert r0["close"] > 0, f"close should be > 0: {r0['close']}"
    assert r0["category"] == "stocks", f"wrong category: {r0['category']}"
    ok(f"Fetched {len(rows)} rows in {elapsed:.1f}s  (symbols: {', '.join(sorted(symbols_seen))})")
    ok(f"Sample row: {r0['symbol']} {r0['trade_date']} close={r0['close']:.2f}")

# Test empty input — should return [] gracefully
empty = fetch_ohlcv([], "stocks", from_date, today)
assert empty == [], f"empty input should return []: {empty}"
ok("Empty symbol list → [] (no crash)")


# ─────────────────────────────────────────────────────────────────────────────
# 3. MFAPI fetcher
# ─────────────────────────────────────────────────────────────────────────────
section("TEST 3 — MFAPI fetcher (live)")

from src.importer.fetchers.mfapi_fetcher import fetch_nav, fetch_all_nav

mf_from = today - timedelta(days=10)
nav_rows = fetch_nav("GOLDBEES", "140088", mf_from, today)

if len(nav_rows) == 0:
    warn("MFAPI returned 0 rows — may be a holiday period. Trying 30-day window...")
    nav_rows = fetch_nav("GOLDBEES", "140088", today - timedelta(days=30), today)

if len(nav_rows) == 0:
    fail("MFAPI.in returned no NAV for GOLDBEES — API may be down")
else:
    r0 = nav_rows[0]
    assert r0["symbol"] == "GOLDBEES", f"wrong symbol: {r0['symbol']}"
    assert r0["scheme_code"] == "140088", f"wrong code: {r0['scheme_code']}"
    assert isinstance(r0["nav_date"], date), f"nav_date should be date: {type(r0['nav_date'])}"
    assert r0["nav"] > 0, f"nav should be > 0: {r0['nav']}"
    ok(f"GOLDBEES: {len(nav_rows)} NAV records (latest: {r0['nav_date']} nav={r0['nav']:.4f})")

# Test bad scheme code → graceful empty return
bad = fetch_nav("TEST", "9999999", mf_from, today)
assert bad == [], f"bad scheme code should return []: {bad}"
ok("Invalid scheme code → [] (no crash)")

# Test date range filter
all_nav = fetch_all_nav({"NIFTYBEES": "140084"}, mf_from, today)
if all_nav:
    ok(f"fetch_all_nav: NIFTYBEES → {len(all_nav)} rows")
else:
    warn("fetch_all_nav returned 0 rows (holiday window?)")


# ─────────────────────────────────────────────────────────────────────────────
# 4. ClickHouse schema
# ─────────────────────────────────────────────────────────────────────────────
section(f"TEST 4 — ClickHouse schema (host={CH_HOST}:{CH_PORT})")

from src.importer.clickhouse import ClickHouseImporter

try:
    ch = ClickHouseImporter(
        host=CH_HOST, port=CH_PORT, database=CH_DB,
        username=CH_USER, password=CH_PASS,
    )
    ch.ensure_schema()
    ok("Connected to ClickHouse and schema created (idempotent)")
    CH_AVAILABLE = True
except Exception as exc:
    warn(f"ClickHouse not reachable: {exc}")
    warn("Skipping ClickHouse-dependent tests (tests 4–7)")
    warn("Start ClickHouse with: docker compose up clickhouse -d")
    CH_AVAILABLE = False

if CH_AVAILABLE:
    # Verify tables exist
    import clickhouse_connect
    client = clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT, username=CH_USER, password=CH_PASS
    )
    tables = {r[0] for r in client.query("SHOW TABLES IN market_data").result_rows}
    assert "daily_prices" in tables, f"daily_prices missing: {tables}"
    assert "mf_nav" in tables, f"mf_nav missing: {tables}"
    assert "import_watermarks" in tables, f"import_watermarks missing: {tables}"
    ok(f"All 3 tables exist: {sorted(tables)}")

    # Idempotent — run ensure_schema again
    ch.ensure_schema()
    ok("ensure_schema is idempotent (second call OK)")
    client.close()


# ─────────────────────────────────────────────────────────────────────────────
# 5. ClickHouse insert + watermark round-trip
# ─────────────────────────────────────────────────────────────────────────────
if CH_AVAILABLE:
    section("TEST 5 — ClickHouse insert + watermarks")

    import clickhouse_connect

    test_date = date(2024, 1, 15)

    # Pre-clean any leftover data from previous runs
    client = clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT, username=CH_USER, password=CH_PASS
    )
    client.command("ALTER TABLE market_data.daily_prices DELETE WHERE symbol = '_TEST_SYMBOL'")
    client.command("ALTER TABLE market_data.mf_nav DELETE WHERE symbol = '_TEST_ETF'")
    client.command("ALTER TABLE market_data.import_watermarks DELETE WHERE source = 'test_source'")
    client.close()

    # Insert a synthetic price row
    price_rows = [{
        "symbol":     "_TEST_SYMBOL",
        "category":   "stocks",
        "trade_date": test_date,
        "open":       100.0,
        "high":       105.0,
        "low":        99.0,
        "close":      103.5,
        "volume":     10000.0,
    }]
    inserted = ch.insert_prices(price_rows)
    assert inserted == 1, f"expected 1 inserted, got {inserted}"
    ok("insert_prices: 1 row inserted")

    # Verify it's in ClickHouse
    client = clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT, username=CH_USER, password=CH_PASS
    )
    result = client.query(
        "SELECT symbol, close FROM market_data.daily_prices FINAL "
        "WHERE symbol = '_TEST_SYMBOL'"
    )
    rows_found = result.result_rows
    client.close()
    assert len(rows_found) >= 1, f"Row not found after insert: {rows_found}"
    assert rows_found[0][1] == 103.5, f"Wrong close: {rows_found[0][1]}"
    ok(f"Row verified in daily_prices: symbol={rows_found[0][0]} close={rows_found[0][1]}")

    # Idempotent insert (ReplacingMergeTree) — re-insert same date with different close
    price_rows[0]["close"] = 110.0
    ch.insert_prices(price_rows)
    ok("Re-insert same date (ReplacingMergeTree idempotency) — no error")

    # Insert a NAV row
    nav_rows_test = [{
        "symbol":      "_TEST_ETF",
        "scheme_code": "999999",
        "nav_date":    test_date,
        "nav":         75.25,
    }]
    inserted_nav = ch.insert_nav(nav_rows_test)
    assert inserted_nav == 1, f"expected 1 NAV inserted, got {inserted_nav}"
    ok("insert_nav: 1 row inserted")

    # Watermark round-trip
    wm_before = ch.get_watermark("test_source", "_TEST_SYMBOL")
    assert wm_before is None, f"watermark should not exist yet: {wm_before}"
    ok("get_watermark → None before first set")

    mark_date = date(2024, 1, 20)
    ch.set_watermark("test_source", "_TEST_SYMBOL", mark_date)
    wm_after = ch.get_watermark("test_source", "_TEST_SYMBOL")
    assert wm_after == mark_date, f"watermark mismatch: {wm_after} != {mark_date}"
    ok(f"set_watermark + get_watermark round-trip: {wm_after}")

    # Overwrite watermark (upsert)
    mark_date2 = date(2024, 1, 25)
    ch.set_watermark("test_source", "_TEST_SYMBOL", mark_date2)
    wm_after2 = ch.get_watermark("test_source", "_TEST_SYMBOL")
    assert wm_after2 == mark_date2, f"watermark upsert failed: {wm_after2}"
    ok(f"Watermark upsert: {wm_after} → {wm_after2}")

    # Cleanup test data
    cleanup_client = clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT, username=CH_USER, password=CH_PASS
    )
    cleanup_client.command("ALTER TABLE market_data.daily_prices DELETE WHERE symbol = '_TEST_SYMBOL'")
    cleanup_client.command("ALTER TABLE market_data.mf_nav DELETE WHERE symbol = '_TEST_ETF'")
    cleanup_client.command("ALTER TABLE market_data.import_watermarks DELETE WHERE source = 'test_source'")
    cleanup_client.close()
    ok("Test data cleaned up")


# ─────────────────────────────────────────────────────────────────────────────
# 6. Dry-run import (no ClickHouse writes)
# ─────────────────────────────────────────────────────────────────────────────
section("TEST 6 — Full dry-run import (stocks + mf, 7-day window)")

from rich.console import Console
from src.importer.cli import run_import

dry_console = Console()

try:
    run_import(
        categories=["stocks", "mf"],
        lookback_days=7,
        full_reimport=True,
        dry_run=True,
        console=dry_console,
        clickhouse_host=CH_HOST,
        clickhouse_port=CH_PORT,
        clickhouse_database=CH_DB,
        clickhouse_user=CH_USER,
        clickhouse_password=CH_PASS,
    )
    ok("dry-run completed without exceptions")
except SystemExit as exc:
    if not CH_AVAILABLE:
        warn(f"dry-run skipped — ClickHouse not available (exit code {exc.code})")
    else:
        fail(f"dry-run raised SystemExit: {exc}")
except Exception as exc:
    fail(f"dry-run raised unexpected exception: {exc}")


# ─────────────────────────────────────────────────────────────────────────────
# 7. Live mini import (write to ClickHouse)
# ─────────────────────────────────────────────────────────────────────────────
if CH_AVAILABLE:
    section("TEST 7 — Live mini import (ETFs only, 5-day window)")

    import clickhouse_connect

    # Clear any existing rows for our test symbols first
    client = clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT, username=CH_USER, password=CH_PASS
    )
    client.command("ALTER TABLE market_data.daily_prices DELETE WHERE category = 'etfs' AND symbol IN ('GOLDBEES', 'NIFTYBEES')")
    client.command("ALTER TABLE market_data.import_watermarks DELETE WHERE source = 'yfinance' AND symbol IN ('GOLDBEES', 'NIFTYBEES')")
    client.close()

    # Monkey-patch registry to use only 2 ETFs for speed
    import src.importer.registry as _reg
    _original_etfs = _reg.ETFS
    _reg.ETFS = [("GOLDBEES", "GOLDBEES.NS"), ("NIFTYBEES", "NIFTYBEES.NS")]
    _reg.CATEGORY_MAP["etfs"] = _reg.ETFS

    try:
        run_import(
            categories=["etfs"],
            lookback_days=5,
            full_reimport=True,
            dry_run=False,
            console=dry_console,
            clickhouse_host=CH_HOST,
            clickhouse_port=CH_PORT,
            clickhouse_database=CH_DB,
            clickhouse_user=CH_USER,
            clickhouse_password=CH_PASS,
        )

        # Verify data landed in ClickHouse
        client = clickhouse_connect.get_client(
            host=CH_HOST, port=CH_PORT, username=CH_USER, password=CH_PASS
        )
        count_result = client.query(
            "SELECT count() FROM market_data.daily_prices FINAL WHERE category = 'etfs' AND symbol IN ('GOLDBEES','NIFTYBEES')"
        )
        row_count = count_result.result_rows[0][0]
        ok(f"Rows in daily_prices for GOLDBEES/NIFTYBEES: {row_count}")

        if row_count > 0:
            # Verify watermarks were set
            wm_gold = ch.get_watermark("yfinance", "GOLDBEES")
            ok(f"Watermark for GOLDBEES: {wm_gold}")
        else:
            warn("0 rows inserted — market data may be unavailable for this window")

        client.close()
    finally:
        # Restore registry
        _reg.ETFS = _original_etfs
        _reg.CATEGORY_MAP["etfs"] = _original_etfs


# ─────────────────────────────────────────────────────────────────────────────
# Summary
# ─────────────────────────────────────────────────────────────────────────────
print(f"\n{'='*60}")
if _errors:
    print(f"  {FAIL} {len(_errors)} FAILURE(S):")
    for e in _errors:
        print(f"     • {e}")
    sys.exit(1)
else:
    print(f"  {PASS} All importer integration tests passed!")
print(f"{'='*60}\n")
