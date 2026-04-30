"""
Unit tests for deepdive caching optimisations.

Run with: .venv/bin/python tests/test_deepdive_cache.py

Tests:
  1. _cache_dir — path is per-ticker only (no run_date component)
  2. DeepDiveStore.is_jobs_imported_this_month — monthly watermark logic
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from pathlib import Path
from unittest.mock import MagicMock, patch
from datetime import date


# ── Test 1: _cache_dir is shared across run dates ──────────────────────────────

def test_cache_dir_is_per_ticker_only():
    print("\n" + "=" * 60)
    print("TEST 1: _cache_dir — shared across run dates")
    print("=" * 60)

    from src.deepdive.runner import _cache_dir

    path_today    = _cache_dir("ADSK", "2026-04-30")
    path_tomorrow = _cache_dir("ADSK", "2026-05-01")
    path_other    = _cache_dir("MSFT", "2026-04-30")

    # Same ticker → same cache dir regardless of run_date
    assert path_today == path_tomorrow, (
        f"Cache path must not vary by run_date.\n  got: {path_today}\n  vs : {path_tomorrow}"
    )
    print(f"  ✓ same ticker, different dates → same path: {path_today}")

    # Different tickers → different dirs
    assert path_today != path_other, (
        "Different tickers must have different cache dirs"
    )
    print(f"  ✓ different tickers → different paths")

    # run_date must NOT appear anywhere in the path
    assert "2026-04-30" not in str(path_today), (
        f"run_date must not appear in cache path: {path_today}"
    )
    print(f"  ✓ run_date not present in path")

    # Path ends with the ticker name
    assert path_today.name == "ADSK", (
        f"Expected path to end with ticker, got: {path_today.name}"
    )
    print(f"  ✓ path ends with ticker name")


# ── Test 2: is_jobs_imported_this_month — returns True when CH has a record ───

def test_jobs_imported_this_month_true():
    print("\n" + "=" * 60)
    print("TEST 2: is_jobs_imported_this_month — found in CH")
    print("=" * 60)

    from src.deepdive.clickhouse import DeepDiveStore

    store = DeepDiveStore.__new__(DeepDiveStore)
    store._ready = True

    mock_result = MagicMock()
    mock_result.result_rows = [[1]]
    store._client = MagicMock()
    store._client.query.return_value = mock_result

    result = store.is_jobs_imported_this_month("ADSK")

    assert result is True, f"Expected True when CH returns count=1, got {result}"
    print("  ✓ returns True when ClickHouse count = 1")

    # Verify the query used startsWith and 'jobs' source
    call_args = store._client.query.call_args
    query_str = call_args[0][0]
    assert "startsWith" in query_str, "Query must use startsWith for month prefix"
    assert "jobs" in query_str, "Query must filter source = 'jobs'"
    print("  ✓ query contains startsWith and 'jobs' filter")

    # Verify the month prefix matches today
    params = call_args[1]["parameters"]
    expected_month = date.today().strftime("%Y-%m")
    assert params["m"] == expected_month, (
        f"Month prefix mismatch: expected {expected_month}, got {params['m']}"
    )
    print(f"  ✓ month prefix is current month: {expected_month}")


# ── Test 3: is_jobs_imported_this_month — returns False when no record ─────────

def test_jobs_imported_this_month_false():
    print("\n" + "=" * 60)
    print("TEST 3: is_jobs_imported_this_month — not found in CH")
    print("=" * 60)

    from src.deepdive.clickhouse import DeepDiveStore

    store = DeepDiveStore.__new__(DeepDiveStore)
    store._ready = True

    mock_result = MagicMock()
    mock_result.result_rows = [[0]]
    store._client = MagicMock()
    store._client.query.return_value = mock_result

    result = store.is_jobs_imported_this_month("ADSK")

    assert result is False, f"Expected False when CH returns count=0, got {result}"
    print("  ✓ returns False when ClickHouse count = 0")


# ── Test 4: is_jobs_imported_this_month — short-circuits when CH unavailable ───

def test_jobs_not_ready():
    print("\n" + "=" * 60)
    print("TEST 4: is_jobs_imported_this_month — CH not available")
    print("=" * 60)

    from src.deepdive.clickhouse import DeepDiveStore

    store = DeepDiveStore.__new__(DeepDiveStore)
    store._ready = False
    store._client = MagicMock()

    result = store.is_jobs_imported_this_month("ADSK")

    assert result is False, f"Expected False when _ready=False, got {result}"
    store._client.query.assert_not_called()
    print("  ✓ returns False without querying CH when not ready")


# ── Test 5: is_jobs_imported_this_month — returns False on query exception ─────

def test_jobs_ch_exception():
    print("\n" + "=" * 60)
    print("TEST 5: is_jobs_imported_this_month — CH query raises exception")
    print("=" * 60)

    from src.deepdive.clickhouse import DeepDiveStore

    store = DeepDiveStore.__new__(DeepDiveStore)
    store._ready = True
    store._client = MagicMock()
    store._client.query.side_effect = RuntimeError("connection refused")

    result = store.is_jobs_imported_this_month("ADSK")

    assert result is False, f"Expected False on exception, got {result}"
    print("  ✓ returns False (does not raise) when CH query throws")


# ── Runner ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    test_cache_dir_is_per_ticker_only()
    test_jobs_imported_this_month_true()
    test_jobs_imported_this_month_false()
    test_jobs_not_ready()
    test_jobs_ch_exception()
    print("\n" + "=" * 60)
    print("All deepdive cache tests passed ✓")
    print("=" * 60)
