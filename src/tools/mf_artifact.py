"""
src/tools/mf_artifact.py
─────────────────────────
Artifact-store wrapper for heavy MF subprocess tool outputs.

Full subprocess stdout is persisted to output/.cache/ (via src/utils/cache.py)
before being condensed, so the LLM only ever sees a token-bounded summary while
the complete data stays retrievable on disk under the returned artifact key.

Gated by settings.mf_optimize_mode — callers should skip condense_text() and
return the raw tool output unchanged when that flag is False.
"""
from __future__ import annotations

import hashlib
import json
import logging

from src.utils.cache import cache_get, cache_set

logger = logging.getLogger(__name__)

# Artifacts are point-in-time snapshots retrieved by exact key, not a
# freshness-sensitive cache — TTL is long just to keep output/.cache/ from
# growing forever, not to force re-fetches.
_ARTIFACT_TTL_SECONDS = 30 * 86400
_CONDENSE_THRESHOLD_CHARS = 4000
_HEAD_LINES = 40
_TAIL_LINES = 20


def _artifact_key(tool_name: str, params: dict) -> str:
    params_str = json.dumps(params, sort_keys=True, default=str)
    digest = hashlib.sha1(params_str.encode()).hexdigest()[:10]
    return f"mf_artifact_{tool_name}_{digest}"


def write_mf_artifact(tool_name: str, params: dict, full_result: str) -> str:
    """Persist the full tool output to disk and return its artifact key."""
    key = _artifact_key(tool_name, params)
    cache_set(key, full_result)
    return key


def read_mf_artifact(key: str) -> str | None:
    """Retrieve a previously persisted full tool output by artifact key."""
    return cache_get(key, ttl_seconds=_ARTIFACT_TTL_SECONDS)


def condense_text(tool_name: str, params: dict, full_output: str) -> str:
    """
    Persist ``full_output`` as an artifact, then return a token-bounded version:
    unmodified if already small, otherwise head+tail with the artifact key
    noted for retrieval of the full detail.
    """
    key = write_mf_artifact(tool_name, params, full_output)
    if len(full_output) <= _CONDENSE_THRESHOLD_CHARS:
        return full_output

    lines = full_output.splitlines()
    if len(lines) <= _HEAD_LINES + _TAIL_LINES:
        return full_output

    omitted = len(lines) - _HEAD_LINES - _TAIL_LINES
    condensed = (
        lines[:_HEAD_LINES]
        + [
            f"\n… [{omitted} lines omitted — {len(full_output)} chars total, "
            f"full output saved as artifact `{key}`] …\n"
        ]
        + lines[-_TAIL_LINES:]
    )
    return "\n".join(condensed)
