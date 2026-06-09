from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pandas as pd

from src.importer.source_preference import (
    get_saved_data_source,
    normalize_data_source,
    resolve_data_source,
    save_data_source,
)


def test_normalize_data_source_accepts_menu_choices_and_aliases():
    assert normalize_data_source("1") == "shoonya"
    assert normalize_data_source("NSE") == "nse"
    assert normalize_data_source("Yahoo") == "yfinance"


@patch("src.db.pool.query_df")
@patch("src.db.pool.execute")
def test_get_saved_data_source_returns_fresh_db_value(mock_execute, mock_query):
    mock_query.return_value = pd.DataFrame([{"value": "nse"}])

    assert get_saved_data_source() == "nse"
    mock_execute.assert_called_once()
    assert "INTERVAL 24 HOUR" in mock_query.call_args.args[0]


@patch("src.db.pool.query_df")
@patch("src.db.pool.execute")
def test_get_saved_data_source_returns_empty_after_ttl(mock_execute, mock_query):
    mock_query.return_value = pd.DataFrame(columns=["value"])

    assert get_saved_data_source() == ""


@patch("src.db.pool.acquire")
@patch("src.db.pool.execute")
def test_save_data_source_refreshes_preference(mock_execute, mock_acquire):
    client = MagicMock()

    @contextmanager
    def acquired():
        yield client

    mock_acquire.side_effect = acquired

    assert save_data_source("3") is True
    client.insert.assert_called_once_with(
        "market_data.agent_preferences",
        [["market_import_data_source", "yfinance"]],
        column_names=["preference_key", "value"],
    )


@patch("src.importer.source_preference.save_data_source")
def test_explicit_source_refreshes_ttl(mock_save):
    assert resolve_data_source("nse") == ("nse", False)
    mock_save.assert_called_once_with("nse")
