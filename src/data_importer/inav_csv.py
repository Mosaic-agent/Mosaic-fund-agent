"""
data_importer.inav_csv
──────────────────────
Flat-file CSV store for iNAV snapshots.

Layout: market_data/inav/{SYMBOL}/{YYYY}/{MM}/{DD}.csv

Schema: snapshot_at,inav,market_price,premium_discount_pct,source
"""

from __future__ import annotations

import csv
import os
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Iterable, Mapping

import pandas as pd


CSV_COLUMNS = ["snapshot_at", "inav", "market_price", "premium_discount_pct", "source"]


def _resolve_base_dir(base_dir: str | os.PathLike | None) -> Path:
    if base_dir is not None:
        return Path(base_dir)
    env = os.environ.get("MOSAIC_MARKET_DATA_DIR")
    if env:
        return Path(env) / "inav"
    # Default: <repo-root>/market_data/inav
    here = Path(__file__).resolve()
    for parent in here.parents:
        if (parent / "market_data").is_dir():
            return parent / "market_data" / "inav"
    return Path.cwd() / "market_data" / "inav"


def inav_csv_path(
    symbol: str,
    snapshot_at: datetime | date,
    base_dir: str | os.PathLike | None = None,
) -> Path:
    """Return the target CSV path for a given symbol + date."""
    d = snapshot_at.date() if isinstance(snapshot_at, datetime) else snapshot_at
    root = _resolve_base_dir(base_dir)
    return root / symbol.upper() / f"{d.year:04d}" / f"{d.month:02d}" / f"{d.day:02d}.csv"


def _coerce_snapshot_at(value) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day)
    return datetime.fromisoformat(str(value))


def write_inav_rows(
    rows: Iterable[Mapping],
    base_dir: str | os.PathLike | None = None,
) -> dict[Path, int]:
    """
    Append iNAV rows to per-day CSV files. Creates dirs and header when needed.

    Returns a dict {path: rows_written} for the caller to report progress.
    """
    grouped: dict[Path, list[dict]] = defaultdict(list)
    for row in rows:
        symbol = row["symbol"]
        snap = _coerce_snapshot_at(row["snapshot_at"])
        path = inav_csv_path(symbol, snap, base_dir)
        grouped[path].append({
            "snapshot_at": snap.isoformat(timespec="seconds"),
            "inav": row["inav"],
            "market_price": row["market_price"],
            "premium_discount_pct": row.get("premium_discount_pct"),
            "source": row.get("source", ""),
        })

    written: dict[Path, int] = {}
    for path, day_rows in grouped.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        needs_header = not path.exists()
        with path.open("a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
            if needs_header:
                writer.writeheader()
            writer.writerows(day_rows)
        written[path] = len(day_rows)
    return written


def read_inav(
    symbol: str,
    start: str | date | datetime,
    end: str | date | datetime | None = None,
    base_dir: str | os.PathLike | None = None,
) -> pd.DataFrame:
    """
    Read iNAV rows for `symbol` between `start` and `end` (inclusive).

    Returns an empty DataFrame with correct columns if no files exist.
    """
    start_d = pd.to_datetime(start).date()
    end_d = pd.to_datetime(end).date() if end is not None else date.today()

    root = _resolve_base_dir(base_dir) / symbol.upper()
    if not root.is_dir():
        return pd.DataFrame(columns=CSV_COLUMNS)

    frames: list[pd.DataFrame] = []
    for csv_path in sorted(root.glob("*/*/*.csv")):
        try:
            y, m, d = int(csv_path.parts[-3]), int(csv_path.parts[-2]), int(csv_path.stem)
            file_date = date(y, m, d)
        except (ValueError, IndexError):
            continue
        if start_d <= file_date <= end_d:
            frames.append(pd.read_csv(csv_path))

    if not frames:
        return pd.DataFrame(columns=CSV_COLUMNS)

    df = pd.concat(frames, ignore_index=True)
    df["snapshot_at"] = pd.to_datetime(df["snapshot_at"])
    return df.sort_values("snapshot_at").reset_index(drop=True)
