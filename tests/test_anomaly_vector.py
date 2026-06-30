"""
tests/test_anomaly_vector.py
────────────────────────────
Unit tests for the Qdrant anomaly vector integration.
All tests mock Qdrant — no live server required.
"""

from __future__ import annotations

import threading
import time
from datetime import date, timedelta
from unittest.mock import MagicMock, patch, call

import numpy as np
import pandas as pd
import pytest


# ── helpers ───────────────────────────────────────────────────────────────────

def _make_flagged_df(n: int = 3) -> pd.DataFrame:
    base = date(2023, 6, 1)
    rows = []
    for i in range(n):
        rows.append({
            "trade_date":    base + timedelta(days=i),
            "open":          100.0 + i,
            "high":          105.0 + i,
            "low":           98.0 + i,
            "close":         102.0 + i,
            "volume":        1_000_000.0,
            "daily_return":  2.5 - i * 0.5,
            "log_return":    0.025,
            "z_robust":      3.5 + i * 0.1,
            "z_resid":       2.8,
            "z_resid_abs":   2.8,
            "garch_vol":     18.5,
            "if_confidence": 0.85,
            "if_label":      -1,
            "final_z":       4.2 + i * 0.1,
            "final_z_abs":   4.2 + i * 0.1,
            "regime":        "⚡ Flash Crash / Black Swan (EXIT)",
            "is_anomaly":    True,
            "is_changepoint":False,
            "cp_confirmed":  False,
        })
    return pd.DataFrame(rows)


def _make_synthetic_ohlcv(n: int = 80) -> pd.DataFrame:
    dates = [date(2022, 1, 1) + timedelta(days=i) for i in range(n)]
    np.random.seed(42)
    close = np.cumprod(1 + np.random.randn(n) * 0.02) * 100
    df = pd.DataFrame({
        "trade_date": dates,
        "open":       close * 0.99,
        "high":       close * 1.01,
        "low":        close * 0.98,
        "close":      close,
        "volume":     np.abs(np.random.randn(n)) * 1e6 + 1e5,
    })
    # inject a large spike to guarantee at least one anomaly
    df.loc[40, "close"] = df.loc[40, "close"] * 1.10
    df.loc[40, "high"]  = df.loc[40, "high"]  * 1.10
    return df


# ── Test 1: store_anomalies upserts ──────────────────────────────────────────

def test_store_anomalies_upserts_to_qdrant():
    """store_anomalies calls QdrantClient.upsert with correct collection + points."""
    import src.db.anomaly_vector as av

    mock_client = MagicMock()
    mock_client.get_collections.return_value.collections = []

    df_flagged = _make_flagged_df(2)

    with patch.object(av, "_get_client", return_value=mock_client), \
         patch.object(av, "_collection_ready", False), \
         patch("src.db.anomaly_vector.embed_batch" if False else "src.ml.correlation.news_rag.embed_batch",
               return_value=[[0.1] * 768, [0.2] * 768], create=True):

        # Patch embed at the module level av uses
        with patch("src.db.anomaly_vector._embed", return_value=[[0.1] * 768, [0.2] * 768]):
            # Reset collection flag so ensure_collection runs
            av._collection_ready = False
            av._client = mock_client

            av._do_store(df_flagged, "GOLDBEES", "etfs")

    mock_client.upsert.assert_called_once()
    call_kwargs = mock_client.upsert.call_args
    collection_name = call_kwargs[1].get("collection_name") or call_kwargs[0][0]
    assert collection_name == "market_anomalies"

    points = call_kwargs[1].get("points") or call_kwargs[0][1]
    assert len(points) == 2

    payload = points[0].payload
    assert "symbol" in payload
    assert payload["symbol"] == "GOLDBEES"
    assert "regime" in payload
    assert "trade_date" in payload
    assert "final_z" in payload
    assert "data_type" in payload
    assert payload["data_type"] == "anomaly"


# ── Test 2: no-op on empty df ─────────────────────────────────────────────────

def test_store_anomalies_no_op_when_empty():
    """store_anomalies does NOT call upsert when df_flagged is empty."""
    import src.db.anomaly_vector as av

    mock_client = MagicMock()
    av._client = mock_client
    av._collection_ready = True

    av._do_store(pd.DataFrame(), "GOLDBEES", "etfs")

    mock_client.upsert.assert_not_called()


# ── Test 3: retrieve returns results ─────────────────────────────────────────

def test_retrieve_similar_anomalies_returns_results():
    """retrieve_similar_anomalies maps Qdrant hits to expected dict shape."""
    import src.db.anomaly_vector as av

    hit1 = MagicMock()
    hit1.score = 0.91
    hit1.payload = {
        "symbol": "GOLDBEES", "category": "etfs",
        "trade_date": "2023-03-15", "regime": "⚡ Flash Crash / Black Swan (EXIT)",
        "final_z": -4.1, "garch_vol": 17.2, "daily_return": -2.5,
        "text": "GOLDBEES (etfs) 2023-03-15: ⚡ Flash Crash ...",
    }
    hit2 = MagicMock()
    hit2.score = 0.82
    hit2.payload = {
        "symbol": "NIFTYBEES", "category": "etfs",
        "trade_date": "2022-10-05", "regime": "⚡ Flash Crash / Black Swan (EXIT)",
        "final_z": -3.8, "garch_vol": 15.0, "daily_return": -1.9,
        "text": "NIFTYBEES (etfs) 2022-10-05: ⚡ Flash Crash ...",
    }

    mock_result = MagicMock()
    mock_result.points = [hit1, hit2]

    mock_client = MagicMock()
    mock_client.get_collections.return_value.collections = [MagicMock(name="market_anomalies")]
    mock_client.query_points.return_value = mock_result

    with patch.object(av, "_get_client", return_value=mock_client), \
         patch.object(av, "_collection_ready", True), \
         patch.object(av, "_embed", return_value=[[0.5] * 768]):

        av._collection_ready = True
        av._client = mock_client

        results = av.retrieve_similar_anomalies(
            symbol="GOLDBEES",
            regime="⚡ Flash Crash / Black Swan (EXIT)",
            trade_date=date(2024, 1, 15),
            k=5,
        )

    # Two searches (before/after exclusion window) each return 2 hits → merged + sorted top-5
    assert len(results) >= 1
    first = results[0]
    assert first["symbol"] in ("GOLDBEES", "NIFTYBEES")
    assert "trade_date" in first
    assert "regime" in first
    assert "final_z" in first
    assert "similarity" in first
    assert "text" in first


# ── Test 4: graceful when Qdrant is down ─────────────────────────────────────

def test_retrieve_similar_anomalies_graceful_when_qdrant_down():
    """retrieve_similar_anomalies returns [] without raising when client is None."""
    import src.db.anomaly_vector as av

    with patch.object(av, "_get_client", return_value=None):
        results = av.retrieve_similar_anomalies(
            symbol="RELIANCE",
            regime="🔥 Volatile Breakout",
            trade_date=date(2024, 1, 1),
        )

    assert results == []


# ── Test 5: run_composite_anomaly calls store_anomalies ──────────────────────

def test_run_composite_anomaly_triggers_store():
    """Passing symbol to run_composite_anomaly causes store_anomalies to be called."""
    from src.ml.anomaly import run_composite_anomaly

    df_test = _make_synthetic_ohlcv(80)
    store_calls = []

    def fake_store(df_flagged, symbol, category):
        store_calls.append((df_flagged.copy(), symbol, category))

    with patch("src.db.anomaly_vector.store_anomalies", side_effect=fake_store):
        df_res, df_flagged, loglik = run_composite_anomaly(
            df_test,
            z_threshold=1.5,
            contamination=0.1,
            symbol="TESTSTOCK",
            category="stocks",
        )

    assert len(store_calls) == 1, f"Expected 1 call, got {len(store_calls)}"
    called_df, called_sym, called_cat = store_calls[0]
    assert called_sym == "TESTSTOCK"
    assert called_cat == "stocks"
    assert not called_df.empty, "store_anomalies should receive non-empty df_flagged"


# ── Test 6: find_similar_anomaly_events tool — no results ────────────────────

def test_find_similar_anomaly_events_tool_no_results():
    """Tool returns a message mentioning 'populate' when Qdrant has no matches."""
    from src.tools.market.equity import find_similar_anomaly_events

    with patch("src.ml.anomaly.retrieve_similar_anomalies", return_value=[]):
        result = find_similar_anomaly_events.func(
            symbol="GOLDBEES",
            regime="⚡ Flash Crash",
            trade_date="2024-01-15",
        )

    assert "populate" in result.lower() or "empty" in result.lower(), (
        f"Expected 'populate'/'empty' hint, got: {result[:200]}"
    )


# ── Test 7: find_similar_anomaly_events tool — with results ──────────────────

def test_find_similar_anomaly_events_tool_with_results():
    """Tool returns a Markdown table with one row per similar event."""
    from src.tools.market.equity import find_similar_anomaly_events

    fake_hits = [
        {
            "symbol": "GOLDBEES", "category": "etfs",
            "trade_date": "2022-06-17", "regime": "⚡ Flash Crash / Black Swan (EXIT)",
            "final_z": -4.3, "garch_vol": 19.1, "daily_return": -3.1,
            "similarity": 0.94,
            "text": "GOLDBEES (etfs) 2022-06-17: ⚡ Flash Crash ...",
        },
        {
            "symbol": "NIFTYBEES", "category": "etfs",
            "trade_date": "2021-03-08", "regime": "⚡ Flash Crash / Black Swan (EXIT)",
            "final_z": -3.9, "garch_vol": 16.5, "daily_return": -2.4,
            "similarity": 0.88,
            "text": "NIFTYBEES (etfs) 2021-03-08: ⚡ Flash Crash ...",
        },
    ]

    with patch("src.ml.anomaly.retrieve_similar_anomalies", return_value=fake_hits):
        result = find_similar_anomaly_events.func(
            symbol="GOLDBEES",
            regime="⚡ Flash Crash / Black Swan (EXIT)",
            trade_date="2024-01-15",
            k=5,
        )

    # Should be a Markdown table
    assert "| Date |" in result or "|" in result
    assert "GOLDBEES" in result
    assert "NIFTYBEES" in result
    # Both rows present
    assert "2022-06-17" in result
    assert "2021-03-08" in result
