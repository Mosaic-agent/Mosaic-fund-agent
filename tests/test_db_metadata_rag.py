"""
tests/test_db_metadata_rag.py
──────────────────────────────
Unit tests for ClickHouse metadata RAG in Qdrant.
"""

import pytest
from src.db.db_metadata_rag import retrieve_db_metadata

def test_retrieve_db_metadata_tables():
    res = retrieve_db_metadata("daily_prices", k=1, type_filter="table_schema")
    assert len(res) >= 1
    assert res[0]["name"] == "market_data.daily_prices"
    assert "symbol" in res[0]["content"]

def test_retrieve_db_metadata_templates():
    res = retrieve_db_metadata("latest price etf GOLDBEES", k=1, type_filter="sql_template")
    assert len(res) >= 1
    assert "daily_prices" in res[0]["content"] or "close" in res[0]["content"]
