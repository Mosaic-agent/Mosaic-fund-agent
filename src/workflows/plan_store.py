"""
src/workflows/plan_store.py
───────────────────────────
Saves and retrieves agent plans for future reference.

Each plan is stored as a JSON file under output/plans/<date>_<intent>_<hash>.json
and indexed in a lightweight SQLite catalogue (output/plans/index.db).

Similarity search uses token-overlap (Jaccard) — no embedding model required.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import sqlite3
from datetime import date
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_PLANS_DIR = Path(os.getenv("OUTPUT_DIR", "output")) / "plans"
_INDEX_DB = _PLANS_DIR / "index.db"

_STOP = frozenset({
    "what", "how", "the", "is", "for", "in", "on", "of", "a", "an", "and",
    "to", "do", "run", "get", "show", "me", "my", "please", "can", "you",
    "with", "or", "by", "its", "this", "that", "which", "are", "was", "did",
    "about", "from", "at", "it", "be", "has", "have", "had",
})


def _tokens(s: str) -> set[str]:
    """Tokenise a string into lowercase words, removing stop-words."""
    return {w for w in re.findall(r"\b\w+\b", s.lower()) if len(w) > 1 and w not in _STOP}


def _ensure_store() -> sqlite3.Connection:
    """Return (and lazily create) the plan SQLite index."""
    _PLANS_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(_INDEX_DB), check_same_thread=False)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS plan_index (
            id            TEXT PRIMARY KEY,
            intent        TEXT NOT NULL,
            question      TEXT NOT NULL,
            steps_json    TEXT NOT NULL,
            created_at    TEXT NOT NULL,
            file_path     TEXT NOT NULL,
            metadata_json TEXT DEFAULT '{}'
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_intent ON plan_index (intent)")
    conn.commit()
    return conn


# ── Public API ────────────────────────────────────────────────────────────────

def save_plan(
    intent: str,
    question: str,
    steps: list[str],
    metadata: dict[str, Any] | None = None,
) -> str:
    """
    Persist a plan to disk and index it for future similarity lookup.

    Parameters
    ----------
    intent   : sub-agent intent (signal / macro / news / mf / research / india_equity)
    question : original user question
    steps    : ordered list of step descriptions
    metadata : optional extra context (symbol, revision, parent_plan_id, …)

    Returns
    -------
    str: plan_id (12-char sha256 prefix, unique per intent+question+date)
    """
    plan_id = hashlib.sha256(
        f"{intent}:{question}:{date.today()}".encode()
    ).hexdigest()[:12]

    payload: dict[str, Any] = {
        "plan_id":    plan_id,
        "intent":     intent,
        "question":   question,
        "steps":      steps,
        "created_at": date.today().isoformat(),
        "metadata":   metadata or {},
    }
    try:
        _PLANS_DIR.mkdir(parents=True, exist_ok=True)
        file_path = _PLANS_DIR / f"{date.today()}_{intent}_{plan_id}.json"
        file_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False))

        conn = _ensure_store()
        conn.execute(
            "INSERT OR REPLACE INTO plan_index VALUES (?,?,?,?,?,?,?)",
            (
                plan_id, intent, question,
                json.dumps(steps),
                date.today().isoformat(),
                str(file_path),
                json.dumps(metadata or {}),
            ),
        )
        conn.commit()
        logger.info(
            "plan_store: saved plan %s (%d steps) → %s",
            plan_id, len(steps), file_path.name,
        )
    except Exception as exc:
        logger.warning("plan_store: save failed: %s", exc)

    return plan_id


def find_similar_plans(
    question: str,
    intent: str | None = None,
    top_k: int = 3,
    min_score: float = 0.15,
) -> list[dict[str, Any]]:
    """
    Return up to top_k past plans whose question overlaps with the new question.

    Uses token-overlap Jaccard similarity (no embedding model needed).

    Parameters
    ----------
    question  : new user question to match against
    intent    : if provided, restrict search to this intent
    top_k     : maximum number of results to return
    min_score : minimum Jaccard similarity threshold (0–1)
    """
    try:
        conn = _ensure_store()
        sql = "SELECT id, intent, question, steps_json, created_at FROM plan_index"
        params: tuple = ()
        if intent:
            sql += " WHERE intent=?"
            params = (intent,)
        rows = conn.execute(sql, params).fetchall()
    except Exception as exc:
        logger.debug("plan_store: find_similar_plans query failed: %s", exc)
        return []

    q_tok = _tokens(question)
    scored: list[dict[str, Any]] = []
    for plan_id, pi, pq, ps, ca in rows:
        r_tok = _tokens(pq)
        union = q_tok | r_tok
        if not union:
            continue
        score = len(q_tok & r_tok) / len(union)
        if score >= min_score:
            scored.append({
                "plan_id":    plan_id,
                "intent":     pi,
                "question":   pq,
                "steps":      json.loads(ps),
                "created_at": ca,
                "similarity": round(score, 3),
            })

    return sorted(scored, key=lambda x: x["similarity"], reverse=True)[:top_k]


def load_plan(plan_id: str) -> dict[str, Any] | None:
    """
    Load a saved plan by its ID.

    Returns None if the plan cannot be found on disk.
    """
    try:
        conn = _ensure_store()
        row = conn.execute(
            "SELECT file_path FROM plan_index WHERE id=?", (plan_id,)
        ).fetchone()
        if row and Path(row[0]).exists():
            return json.loads(Path(row[0]).read_text())
    except Exception as exc:
        logger.debug("plan_store: load_plan(%s) failed: %s", plan_id, exc)
    return None
