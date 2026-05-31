"""
tests/test_drift_monitor.py
────────────────────────────
Unit tests for GOLDBEES ML prediction model drift monitor.
"""

import unittest
from unittest.mock import MagicMock, patch
import pandas as pd
from datetime import datetime, date
import numpy as np

# Adjust sys.path or import directly
from src.ml.drift_monitor import update_realized_returns, run_drift_monitor


class TestDriftMonitor(unittest.TestCase):

    def setUp(self):
        # Sample predictions data
        self.mock_predictions = pd.DataFrame([
            {
                "as_of": date(2026, 5, 1),
                "horizon_days": 5,
                "expected_return_pct": 0.5,
                "confidence_low": 0.1,
                "confidence_high": 0.9,
                "regime_signal": "BUY",
                "cv_r2_mean": 0.05,
                "n_training_rows": 500,
                "goldbees_close": 50.0,
                "prob_up": 0.6,
                "cv_auc_mean": 0.55,
                "realized_return_pct": 0.0,  # default / not yet populated
            },
            {
                "as_of": date(2026, 5, 2),
                "horizon_days": 5,
                "expected_return_pct": -0.2,
                "confidence_low": -0.6,
                "confidence_high": 0.2,
                "regime_signal": "HOLD",
                "cv_r2_mean": 0.05,
                "n_training_rows": 500,
                "goldbees_close": 50.5,
                "prob_up": 0.45,
                "cv_auc_mean": 0.55,
                "realized_return_pct": 0.0,
            }
        ])

        # Sample GOLDBEES price history (trading days indices: 0 to 7)
        # Dates: May 1, 2, 3, 4, 5, 6, 7, 8 (all consecutive trading days in mock)
        self.mock_prices = pd.DataFrame([
            {"trade_date": date(2026, 5, 1), "close": 50.0},  # Index 0
            {"trade_date": date(2026, 5, 2), "close": 50.5},  # Index 1
            {"trade_date": date(2026, 5, 3), "close": 51.0},  # Index 2
            {"trade_date": date(2026, 5, 4), "close": 50.8},  # Index 3
            {"trade_date": date(2026, 5, 5), "close": 51.2},  # Index 4
            {"trade_date": date(2026, 5, 6), "close": 51.5},  # Index 5
            {"trade_date": date(2026, 5, 7), "close": 50.2},  # Index 6
            {"trade_date": date(2026, 5, 8), "close": 52.0},  # Index 7
        ])

    @patch("src.ml.drift_monitor.execute")
    @patch("src.ml.drift_monitor.query_df")
    @patch("src.ml.drift_monitor.get_pool")
    def test_update_realized_returns(self, mock_get_pool, mock_query_df, mock_execute):
        # Configure mocks to return predictions and prices
        def query_side_effect(sql, *args, **kwargs):
            if "ml_predictions" in sql:
                return self.mock_predictions
            elif "daily_prices" in sql:
                return self.mock_prices
            return pd.DataFrame()

        mock_query_df.side_effect = query_side_effect

        # Mock ClickHouse client and pool
        mock_client = MagicMock()
        mock_pool = MagicMock()
        mock_pool.get_client.return_value = mock_client
        mock_get_pool.return_value = mock_pool

        # Run update
        update_realized_returns()

        # Verify migrate was called
        mock_execute.assert_called_once_with(
            "ALTER TABLE market_data.ml_predictions ADD COLUMN IF NOT EXISTS realized_return_pct Float64 DEFAULT 0.0"
        )

        # Verify insert was called with the updated returns
        mock_client.insert.assert_called_once()
        args, kwargs = mock_client.insert.call_args
        
        # Get arguments
        table_name = args[0]
        data_inserted = args[1]
        column_names = kwargs.get("column_names")

        self.assertEqual(table_name, "market_data.ml_predictions")
        self.assertIn("realized_return_pct", column_names)

        # Verify calculations:
        # Date 2026-05-01 (Idx 0) + 5 trading days = 2026-05-07 (Idx 5, close=51.5)
        # Expected log return: ln(51.5 / 50.0) * 100 = 2.9559%
        ret1 = round(float(np.log(51.5 / 50.0) * 100), 4)
        
        # Date 2026-05-02 (Idx 1) + 5 trading days = 2026-05-08 (Idx 6, close=50.2) -- Wait, Index 1 + 5 = Index 6, close=50.2
        # Expected log return: ln(50.2 / 50.5) * 100 = -0.5958%
        ret2 = round(float(np.log(50.2 / 50.5) * 100), 4)

        # Verify the computed values in data_inserted
        as_of_idx = column_names.index("as_of")
        ret_idx = column_names.index("realized_return_pct")

        val1 = next(row[ret_idx] for row in data_inserted if row[as_of_idx] == date(2026, 5, 1))
        val2 = next(row[ret_idx] for row in data_inserted if row[as_of_idx] == date(2026, 5, 2))

        self.assertAlmostEqual(val1, ret1, places=3)
        self.assertAlmostEqual(val2, ret2, places=3)

    @patch("src.ml.drift_monitor.roc_auc_score")
    @patch("src.ml.drift_monitor.retrain_model")
    @patch("src.ml.drift_monitor.query_df")
    @patch("src.ml.drift_monitor.update_realized_returns")
    def test_run_drift_monitor_optimal(self, mock_update, mock_query_df, mock_retrain, mock_auc):
        # Set up predictions that all have positive outcomes matching prediction signals (100% Hit Ratio)
        mock_matured_predictions = pd.DataFrame([
            {
                "as_of": date(2026, 5, 1),
                "goldbees_close": 50.0,
                "expected_return_pct": 0.5,     # predict UP
                "realized_return_pct": 2.9,     # actual UP
                "prob_up": 0.7,
            },
            {
                "as_of": date(2026, 5, 2),
                "goldbees_close": 50.5,
                "expected_return_pct": -0.2,    # predict DOWN
                "realized_return_pct": -0.5,    # actual DOWN
                "prob_up": 0.3,
            },
            {
                "as_of": date(2026, 5, 3),
                "goldbees_close": 50.8,
                "expected_return_pct": 0.3,     # predict UP
                "realized_return_pct": 1.2,     # actual UP
                "prob_up": 0.8,
            },
            {
                "as_of": date(2026, 5, 4),
                "goldbees_close": 51.0,
                "expected_return_pct": 0.4,     # predict UP
                "realized_return_pct": 0.8,     # actual UP
                "prob_up": 0.65,
            },
            {
                "as_of": date(2026, 5, 5),
                "goldbees_close": 51.2,
                "expected_return_pct": -0.1,    # predict DOWN
                "realized_return_pct": -1.2,    # actual DOWN
                "prob_up": 0.25,
            }
        ])
        mock_query_df.return_value = mock_matured_predictions
        mock_auc.return_value = 1.0  # perfect classification

        run_drift_monitor(lookback_days=5, auto_retrain=True)

        # Retrain should NOT be triggered since hit ratio = 100% (>50%) and AUC = 1.0 (>0.50)
        mock_retrain.assert_not_called()

    @patch("src.ml.drift_monitor.roc_auc_score")
    @patch("src.ml.drift_monitor.retrain_model")
    @patch("src.ml.drift_monitor.query_df")
    @patch("src.ml.drift_monitor.update_realized_returns")
    def test_run_drift_monitor_drift_alert(self, mock_update, mock_query_df, mock_retrain, mock_auc):
        # Set up predictions where predictions are completely wrong (0% Hit Ratio)
        mock_matured_predictions = pd.DataFrame([
            {
                "as_of": date(2026, 5, 1),
                "goldbees_close": 50.0,
                "expected_return_pct": 0.5,     # predict UP
                "realized_return_pct": -2.9,    # actual DOWN
                "prob_up": 0.7,
            },
            {
                "as_of": date(2026, 5, 2),
                "goldbees_close": 50.5,
                "expected_return_pct": -0.2,    # predict DOWN
                "realized_return_pct": 0.5,     # actual UP
                "prob_up": 0.3,
            },
            {
                "as_of": date(2026, 5, 3),
                "goldbees_close": 50.8,
                "expected_return_pct": 0.3,     # predict UP
                "realized_return_pct": -1.2,    # actual DOWN
                "prob_up": 0.8,
            },
            {
                "as_of": date(2026, 5, 4),
                "goldbees_close": 51.0,
                "expected_return_pct": 0.4,     # predict UP
                "realized_return_pct": -0.8,    # actual DOWN
                "prob_up": 0.65,
            },
            {
                "as_of": date(2026, 5, 5),
                "goldbees_close": 51.2,
                "expected_return_pct": -0.1,    # predict DOWN
                "realized_return_pct": 1.2,     # actual UP
                "prob_up": 0.25,
            }
        ])
        mock_query_df.return_value = mock_matured_predictions
        mock_auc.return_value = 0.0  # inverse classification / skill < 0.50

        run_drift_monitor(lookback_days=5, auto_retrain=True)

        # Retrain SHOULD be triggered since hit ratio = 0% (<50%) and AUC = 0.0 (<0.50)
        mock_retrain.assert_called_once()
