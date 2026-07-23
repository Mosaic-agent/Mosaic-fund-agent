"""Deterministic context compression for StateGraph workflow fetch results.

This module deliberately never asks an LLM to summarize data: workflow inputs
must retain source numbers verbatim.  It also provides a per-run fetch cache so
callers can avoid repeating an expensive fetch in one workflow execution.
"""
from __future__ import annotations

import contextvars
import json
import re
from contextlib import contextmanager
from dataclasses import dataclass, field
from threading import RLock
from typing import Any, Callable, Iterator


DEFAULT_MAX_CHARS = 12_000


@dataclass(frozen=True)
class DatasetRef:
    """Prompt-ready fetch output plus the metadata needed to audit compaction."""

    key: str
    content: str
    source_type: str
    original_chars: int
    compacted_chars: int
    rows_deduplicated: bool
    truncated: bool


@dataclass
class ContextRun:
    """Ephemeral, thread-safe state for a single workflow fetch phase."""

    cache: dict[str, Any] = field(default_factory=dict)
    artifacts: list[DatasetRef] = field(default_factory=list)
    lock: RLock = field(default_factory=RLock, repr=False)


_active_run: contextvars.ContextVar[ContextRun | None] = contextvars.ContextVar(
    "_workflow_context_run", default=None
)


def truncate_text(text: str, max_chars: int = DEFAULT_MAX_CHARS) -> str:
    """Head-truncate text with an explicit marker while preserving Unicode."""
    if max_chars < 0:
        raise ValueError("max_chars must be non-negative")
    if len(text) <= max_chars:
        return text
    trimmed = len(text) - max_chars
    return f"{text[:max_chars]}\n…[{trimmed} chars trimmed — use narrower queries to fit context]"


def summarize_dict(raw: dict[str, Any]) -> str:
    """Serialize a structured tool result without deriving or dropping fields."""
    return json.dumps(raw, ensure_ascii=False, indent=2, sort_keys=True, default=str)


_TABLE_SEPARATOR = re.compile(r"^\s*\|?\s*:?-{3,}:?\s*(?:\|\s*:?-{3,}:?\s*)+\|?\s*$")


def dedup_rows(text: str) -> str:
    """Remove duplicate body rows from contiguous Markdown tables.

    Non-table prose and table headers/separators are left unchanged.  This is a
    lossless reduction for repeated result rows, rather than a semantic summary.
    """
    lines = text.splitlines(keepends=True)
    result: list[str] = []
    index = 0
    while index < len(lines):
        if index + 1 < len(lines) and "|" in lines[index] and _TABLE_SEPARATOR.match(lines[index + 1].rstrip("\r\n")):
            result.extend((lines[index], lines[index + 1]))
            index += 2
            seen: set[str] = set()
            while index < len(lines) and "|" in lines[index] and lines[index].strip():
                row = lines[index]
                normalized = row.rstrip("\r\n").strip()
                if normalized not in seen:
                    result.append(row)
                    seen.add(normalized)
                index += 1
            continue
        result.append(lines[index])
        index += 1
    return "".join(result)


class ContextManager:
    """Shared context layer, separate from workflow execution/orchestration.

    Like a harness shared layer, this owns context policy and run-local artifacts;
    callers retain ownership of tool execution and graph state.
    """

    def __init__(self, max_chars: int = DEFAULT_MAX_CHARS) -> None:
        self.max_chars = max_chars

    def _build_ref(self, key: str, raw: str | dict[str, Any]) -> DatasetRef:
        source_type = "dict" if isinstance(raw, dict) else "text"
        original = summarize_dict(raw) if isinstance(raw, dict) else str(raw)
        deduplicated = dedup_rows(original) if source_type == "text" else original
        content = truncate_text(deduplicated, self.max_chars)
        return DatasetRef(
            key=key,
            content=content,
            source_type=source_type,
            original_chars=len(original),
            compacted_chars=len(content),
            rows_deduplicated=deduplicated != original,
            truncated=len(deduplicated) > self.max_chars,
        )

    def compress(self, key: str, raw: str | dict[str, Any]) -> str:
        return self.to_dataset_ref(key, raw).content

    def to_dataset_ref(self, key: str, raw: str | dict[str, Any]) -> DatasetRef:
        """Like `compress()` but returns the full DatasetRef, not just its content."""
        ref = self._build_ref(key, raw)
        run = _active_run.get()
        if run is not None:
            with run.lock:
                run.artifacts.append(ref)
        return ref

    @contextmanager
    def run_scope(self) -> Iterator[ContextRun]:
        """Start a run-local cache and artifact ledger for one fetch fan-out."""
        run = ContextRun()
        token = _active_run.set(run)
        try:
            yield run
        finally:
            _active_run.reset(token)

    def fetch_once(self, cache_key: str, fetcher: Callable[[], Any]) -> Any:
        """Deduplicate completed fetches within the active workflow run."""
        run = _active_run.get()
        if run is None:
            return fetcher()
        with run.lock:
            if cache_key in run.cache:
                return run.cache[cache_key]
        result = fetcher()
        with run.lock:
            # A duplicate can only arise when separate callers share a key; keep
            # the first completed result rather than altering downstream context.
            return run.cache.setdefault(cache_key, result)

    @property
    def artifacts(self) -> tuple[DatasetRef, ...]:
        """Artifacts for the current run, or an empty tuple outside a run."""
        run = _active_run.get()
        if run is None:
            return ()
        with run.lock:
            return tuple(run.artifacts)

    @staticmethod
    def summarize_dict(raw: dict[str, Any]) -> str:
        return summarize_dict(raw)

    @staticmethod
    def dedup_rows(text: str) -> str:
        return dedup_rows(text)
