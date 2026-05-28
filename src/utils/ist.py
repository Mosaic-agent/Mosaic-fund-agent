"""
src/utils/ist.py
────────────────
IST (Indian Standard Time, UTC+5:30) helpers for display-facing timestamps.

Data is stored in ClickHouse as naive UTC datetimes.  All user-visible
date/time strings should pass through this module so the display layer
is consistently in IST.
"""

from __future__ import annotations

from datetime import datetime, timezone, timedelta, date
from typing import Union

_IST = timezone(timedelta(hours=5, minutes=30))
_IST_OFFSET_SECONDS = 19800   # 5h30m in seconds


def now_ist() -> datetime:
    """Return the current datetime in IST (aware)."""
    return datetime.now(_IST)


def utc_to_ist(dt: datetime) -> datetime:
    """
    Convert a datetime to IST.

    Accepts:
    - naive UTC datetime  (assumed to be UTC, as stored by ClickHouse)
    - aware UTC datetime  (any tzinfo — converted via UTC)
    Returns an aware IST datetime.
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(_IST)


def fmt_ist(dt: datetime | None, fmt: str = "%Y-%m-%d %H:%M:%S IST") -> str:
    """
    Format a datetime as an IST string.

    Args:
        dt:  naive UTC or aware datetime; None → empty string
        fmt: strftime format (default "YYYY-MM-DD HH:MM:SS IST")
    """
    if dt is None:
        return ""
    return utc_to_ist(dt).strftime(fmt)


def fmt_date_ist(d: date | datetime | None) -> str:
    """Format a date or datetime as a plain IST date string (YYYY-MM-DD)."""
    if d is None:
        return ""
    if isinstance(d, datetime):
        return utc_to_ist(d).strftime("%Y-%m-%d")
    return d.isoformat()


def ist_date_today() -> date:
    """Return today's date in IST."""
    return now_ist().date()
