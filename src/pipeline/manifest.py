"""
src/pipeline/manifest.py
─────────────────────────
Manifest-tracked pipeline DAG.

Tracks upstream input freshness and code version for each pipeline stage.
Decides whether a stage needs recomputation based on fingerprint comparison.
"""
from __future__ import annotations

import hashlib
import json
import logging
import pathlib
import subprocess
import time
from dataclasses import dataclass, field
from datetime import date, datetime
from enum import Enum
from typing import Any

log = logging.getLogger(__name__)

PROJECT_ROOT = pathlib.Path(__file__).parents[2]


class StageStatus(Enum):
    FRESH = "fresh"           # inputs + code unchanged → skip
    STALE_DATA = "stale_data" # upstream data changed → recompute
    STALE_CODE = "stale_code" # code version changed → recompute
    NEVER_RUN = "never_run"   # no manifest exists → compute


@dataclass
class StageDefinition:
    """Declares a pipeline stage and its upstream dependencies."""
    name: str                           # e.g. "ml_predictions"
    symbol: str                         # e.g. "GOLDBEES" or "ALL"
    upstream_queries: dict[str, str]    # {label: SQL returning max date/timestamp}
    code_files: list[str] = field(default_factory=list)  # relative paths to hash for code_version


# ── Pre-defined stage definitions ─────────────────────────────────────────

ML_PREDICTIONS = StageDefinition(
    name="ml_predictions",
    symbol="GOLDBEES",
    upstream_queries={
        "daily_prices_GOLDBEES": "SELECT max(trade_date) FROM market_data.daily_prices FINAL WHERE symbol='GOLDBEES'",
        "daily_prices_GOLD": "SELECT max(trade_date) FROM market_data.daily_prices FINAL WHERE symbol='GOLD'",
        "cot_gold": "SELECT max(report_date) FROM market_data.cot_gold FINAL",
        "fx_rates_USDINR": "SELECT max(trade_date) FROM market_data.fx_rates FINAL WHERE symbol='USDINR'",
        "fii_dii": "SELECT max(trade_date) FROM market_data.fii_dii_flows FINAL",
        "mf_nav_GOLDBEES": "SELECT max(nav_date) FROM market_data.mf_nav FINAL WHERE symbol='GOLDBEES'",
        "etf_aum": "SELECT max(trade_date) FROM market_data.etf_aum FINAL",
    },
    code_files=["src/ml/trend_predictor.py"],
)

SIGNAL_COMPOSITE = StageDefinition(
    name="signal_composite",
    symbol="ALL",
    upstream_queries={
        "ml_predictions": "SELECT max(as_of) FROM market_data.ml_predictions FINAL",
        "daily_prices_etfs": "SELECT max(trade_date) FROM market_data.daily_prices FINAL WHERE category='etfs'",
        "fii_dii": "SELECT max(trade_date) FROM market_data.fii_dii_flows FINAL",
        "news_articles": "SELECT max(published_at) FROM market_data.news_articles FINAL",
    },
    code_files=["src/agents/signal_aggregator.py", "src/agents/signal_sources.py"],
)

WEIGHT_CHECKPOINTS = StageDefinition(
    name="weight_checkpoints",
    symbol="GOLDBEES",
    upstream_queries={
        "ml_predictions": "SELECT max(as_of) FROM market_data.ml_predictions FINAL",
        "daily_prices_GOLDBEES": "SELECT max(trade_date) FROM market_data.daily_prices FINAL WHERE symbol='GOLDBEES'",
        "signal_composite": "SELECT max(as_of) FROM market_data.signal_composite FINAL WHERE etf_symbol='GOLDBEES'",
    },
    code_files=["src/tools/risk_governor.py", "src/tools/adaptive_kelly.py"],
)

ALL_STAGES = [ML_PREDICTIONS, SIGNAL_COMPOSITE, WEIGHT_CHECKPOINTS]


class ManifestTracker:
    """
    Compares current upstream state against stored manifest to decide
    whether a pipeline stage needs recomputation.
    """

    def __init__(self, pool=None):
        if pool is None:
            from src.db.pool import get_pool
            self._pool = get_pool()
        else:
            self._pool = pool

    def check(self, stage: StageDefinition) -> tuple[StageStatus, dict[str, str]]:
        """
        Returns (status, current_details) where:
          - status: FRESH / STALE_DATA / STALE_CODE / NEVER_RUN
          - current_details: {"table_label": "max_date", ...} for the current state
        """
        current_details = self._collect_upstream_dates(stage)
        code_ver = self._code_version(stage)
        fingerprint = self._fingerprint(stage, current_details, code_ver)

        stored = self._get_stored(stage)
        if stored is None:
            return StageStatus.NEVER_RUN, current_details

        if stored["input_fingerprint"] == fingerprint:
            return StageStatus.FRESH, current_details

        if stored["code_version"] != code_ver:
            return StageStatus.STALE_CODE, current_details

        return StageStatus.STALE_DATA, current_details

    def record(
        self,
        stage: StageDefinition,
        details: dict[str, str],
        duration_ms: int = 0,
        status: str = "success",
    ) -> None:
        """Write manifest row after stage computation."""
        code_ver = self._code_version(stage)
        fp = self._fingerprint(stage, details, code_ver)

        row = {
            "stage": stage.name,
            "symbol": stage.symbol,
            "input_fingerprint": fp,
            "code_version": code_ver,
            "computed_at": datetime.now(),
            "input_details": details,
            "duration_ms": duration_ms,
            "status": status,
        }

        try:
            with self._pool.acquire() as client:
                from src.importer.clickhouse import ClickHouseImporter
                ch = ClickHouseImporter(client=client)
                ch.insert_pipeline_manifest(row)
            log.info("ManifestTracker: recorded manifest for %s [%s] (status=%s, duration=%dms)",
                     stage.name, stage.symbol, status, duration_ms)
        except Exception as exc:
            log.warning("ManifestTracker failed to record manifest for %s: %s", stage.name, exc)

    def staleness_report(self) -> list[dict[str, Any]]:
        """Check all stages and return a summary list for CLI/report display."""
        results = []
        for stage in ALL_STAGES:
            status, details = self.check(stage)
            stored = self._get_stored(stage)
            computed_at = stored.get("computed_at") if stored else None
            results.append({
                "stage": stage.name,
                "symbol": stage.symbol,
                "status": status.value,
                "computed_at": computed_at,
                "details": details,
            })
        return results

    # ── Private Helpers ───────────────────────────────────────────────────

    def _collect_upstream_dates(self, stage: StageDefinition) -> dict[str, str]:
        """Run each upstream query and collect max dates."""
        dates: dict[str, str] = {}
        with self._pool.acquire() as client:
            for label, sql in stage.upstream_queries.items():
                try:
                    result = client.query(sql)
                    val = result.result_rows[0][0] if result.result_rows else None
                    if val is not None:
                        dates[label] = str(val)
                    else:
                        dates[label] = "NULL"
                except Exception as exc:
                    log.warning("ManifestTracker query failed for %s -> %s: %s", stage.name, label, exc)
                    dates[label] = "ERROR"
        return dates

    def _code_version(self, stage: StageDefinition) -> str:
        """Hash the content of code_files for this stage. Fallback to git hash."""
        if not stage.code_files:
            return self._git_hash()

        h = hashlib.sha256()
        file_found = False
        for fpath in sorted(stage.code_files):
            p = PROJECT_ROOT / fpath if not pathlib.Path(fpath).is_absolute() else pathlib.Path(fpath)
            if p.exists():
                try:
                    h.update(p.read_bytes())
                    file_found = True
                except Exception:
                    pass
        if file_found:
            return h.hexdigest()[:12]
        return self._git_hash()

    @staticmethod
    def _git_hash() -> str:
        try:
            return subprocess.check_output(
                ["git", "rev-parse", "--short", "HEAD"],
                cwd=str(PROJECT_ROOT),
                stderr=subprocess.DEVNULL,
            ).decode().strip()
        except Exception:
            return "unknown"

    @staticmethod
    def _fingerprint(stage: StageDefinition, details: dict[str, str], code_ver: str) -> str:
        raw = f"{stage.name}|{stage.symbol}|"
        raw += "|".join(f"{k}:{v}" for k, v in sorted(details.items()))
        raw += f"|code:{code_ver}"
        return hashlib.sha256(raw.encode()).hexdigest()

    def _get_stored(self, stage: StageDefinition) -> dict | None:
        try:
            with self._pool.acquire() as client:
                from src.importer.clickhouse import ClickHouseImporter
                ch = ClickHouseImporter(client=client)
                ch.ensure_schema()
                return ch.get_pipeline_manifest(stage.name, stage.symbol)
        except Exception as exc:
            log.warning("ManifestTracker failed to fetch stored manifest for %s: %s", stage.name, exc)
            return None
