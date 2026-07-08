"""
src/utils/sqlite_history.py
────────────────────────────
SQLite-backed prompt_toolkit History for the chat REPL, so ↑/↓ recall
survives across `./mosaic.sh` invocations (container `--rm` wipes anything
outside the `mosaic-output` named volume, where this db lives).
"""
from __future__ import annotations

import sqlite3
from typing import Iterable

from prompt_toolkit.history import History


class SQLiteHistory(History):
    """prompt_toolkit History backend that stores entries in a SQLite table."""

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS chat_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    entry TEXT NOT NULL,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            conn.commit()
        finally:
            conn.close()
        super().__init__()

    def load_history_strings(self) -> Iterable[str]:
        conn = sqlite3.connect(self.db_path)
        try:
            rows = conn.execute(
                "SELECT entry FROM chat_history ORDER BY id DESC"
            ).fetchall()
        finally:
            conn.close()
        return (row[0] for row in rows)

    def store_string(self, string: str) -> None:
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute("INSERT INTO chat_history (entry) VALUES (?)", (string,))
            conn.commit()
        finally:
            conn.close()
