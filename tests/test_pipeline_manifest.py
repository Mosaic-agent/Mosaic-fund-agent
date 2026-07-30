"""
tests/test_pipeline_manifest.py
───────────────────────────────
Unit tests for the ManifestTracker pipeline DAG module.
"""

from __future__ import annotations

import unittest
from unittest.mock import MagicMock
from src.pipeline.manifest import (
    ManifestTracker,
    StageDefinition,
    StageStatus,
    ML_PREDICTIONS,
    SIGNAL_COMPOSITE,
    WEIGHT_CHECKPOINTS,
    ALL_STAGES,
)


class TestPipelineManifest(unittest.TestCase):

    def setUp(self):
        self.mock_pool = MagicMock()
        self.mock_client = MagicMock()
        self.mock_pool.acquire.return_value.__enter__.return_value = self.mock_client
        self.tracker = ManifestTracker(pool=self.mock_pool)

        self.test_stage = StageDefinition(
            name="test_stage",
            symbol="TEST_SYM",
            upstream_queries={"table_a": "SELECT max(date) FROM a"},
            code_files=[],
        )

    def test_predefined_stages_exist(self):
        self.assertEqual(len(ALL_STAGES), 3)
        self.assertIn(ML_PREDICTIONS, ALL_STAGES)
        self.assertIn(SIGNAL_COMPOSITE, ALL_STAGES)
        self.assertIn(WEIGHT_CHECKPOINTS, ALL_STAGES)

    def test_fingerprint_determinism(self):
        stage = self.test_stage
        details = {"table_a": "2026-07-28"}
        fp1 = ManifestTracker._fingerprint(stage, details, "v1.0")
        fp2 = ManifestTracker._fingerprint(stage, details, "v1.0")
        fp3 = ManifestTracker._fingerprint(stage, details, "v1.1")

        self.assertEqual(fp1, fp2)
        self.assertNotEqual(fp1, fp3)

    def test_check_never_run(self):
        # Query 1: upstream date -> '2026-07-28'
        # Query 2: get_stored -> None
        query_result_1 = MagicMock()
        query_result_1.result_rows = [["2026-07-28"]]

        query_result_2 = MagicMock()
        query_result_2.result_rows = []

        self.mock_client.query.side_effect = [query_result_1, query_result_2]

        status, details = self.tracker.check(self.test_stage)
        self.assertEqual(status, StageStatus.NEVER_RUN)
        self.assertEqual(details, {"table_a": "2026-07-28"})

    def test_check_fresh(self):
        details = {"table_a": "2026-07-28"}
        code_ver = self.tracker._code_version(self.test_stage)
        fp = ManifestTracker._fingerprint(self.test_stage, details, code_ver)

        query_result_1 = MagicMock()
        query_result_1.result_rows = [["2026-07-28"]]

        # Stored manifest row matches
        query_result_2 = MagicMock()
        query_result_2.result_rows = [
            ["test_stage", "TEST_SYM", fp, code_ver, "2026-07-28 10:00:00", '{"table_a":"2026-07-28"}', 100, "success"]
        ]
        query_result_2.column_names = ["stage", "symbol", "input_fingerprint", "code_version", "computed_at", "input_details", "duration_ms", "status"]

        self.mock_client.query.side_effect = [query_result_1, query_result_2]

        status, _ = self.tracker.check(self.test_stage)
        self.assertEqual(status, StageStatus.FRESH)

    def test_check_stale_data(self):
        code_ver = self.tracker._code_version(self.test_stage)
        old_fp = ManifestTracker._fingerprint(self.test_stage, {"table_a": "2026-07-20"}, code_ver)

        query_result_1 = MagicMock()
        query_result_1.result_rows = [["2026-07-28"]]

        query_result_2 = MagicMock()
        query_result_2.result_rows = [
            ["test_stage", "TEST_SYM", old_fp, code_ver, "2026-07-20 10:00:00", '{"table_a":"2026-07-20"}', 100, "success"]
        ]
        query_result_2.column_names = ["stage", "symbol", "input_fingerprint", "code_version", "computed_at", "input_details", "duration_ms", "status"]

        self.mock_client.query.side_effect = [query_result_1, query_result_2]

        status, details = self.tracker.check(self.test_stage)
        self.assertEqual(status, StageStatus.STALE_DATA)
        self.assertEqual(details, {"table_a": "2026-07-28"})

    def test_check_stale_code(self):
        old_code_ver = "v0.9.0"
        old_fp = ManifestTracker._fingerprint(self.test_stage, {"table_a": "2026-07-28"}, old_code_ver)

        query_result_1 = MagicMock()
        query_result_1.result_rows = [["2026-07-28"]]

        query_result_2 = MagicMock()
        query_result_2.result_rows = [
            ["test_stage", "TEST_SYM", old_fp, old_code_ver, "2026-07-28 10:00:00", '{"table_a":"2026-07-28"}', 100, "success"]
        ]
        query_result_2.column_names = ["stage", "symbol", "input_fingerprint", "code_version", "computed_at", "input_details", "duration_ms", "status"]

        self.mock_client.query.side_effect = [query_result_1, query_result_2]

        status, _ = self.tracker.check(self.test_stage)
        self.assertEqual(status, StageStatus.STALE_CODE)


if __name__ == "__main__":
    unittest.main()
