"""
src/db/pool.py
──────────────
Thread-safe ClickHouse connection pool.

Why a pool?
-----------
clickhouse_connect clients are NOT thread-safe — concurrent calls on the same
client corrupt responses.  The previous workaround was to create a brand-new
client per function/thread, which is expensive (TCP handshake + auth on every
call).

This pool pre-creates N clients and lends them out via a context manager.
Each thread gets an exclusive client for the duration of its operation, then
returns it.  Under low concurrency the same connections are reused
indefinitely; under burst load the pool grows up to `max_size`.

Architecture
------------
    _idle  : queue.Queue[clickhouse_connect.Client]   — available clients
    _all   : list[clickhouse_connect.Client]          — every client ever made

    acquire() context manager:
        1. try _idle.get(timeout=checkout_timeout)
        2. if empty and len(_all) < max_size → create a new client
        3. yield client to caller
        4. on exit: verify client is healthy, put back to _idle
                    (if unhealthy: close, create fresh replacement)

Pool is a module-level singleton (lazy-initialised on first acquire()).
All public helpers delegate to the singleton.

Configuration (via env / config/settings.py)
---------------------------------------------
    CLICKHOUSE_HOST          default: localhost
    CLICKHOUSE_PORT          default: 8123
    CLICKHOUSE_DATABASE      default: market_data
    CLICKHOUSE_USER          default: default
    CLICKHOUSE_PASSWORD      default: ""
    CLICKHOUSE_POOL_MIN      default: 2   (warm connections kept alive)
    CLICKHOUSE_POOL_MAX      default: 10  (hard cap)
    CLICKHOUSE_POOL_TIMEOUT  default: 10s (seconds to wait for a free slot)
"""

from __future__ import annotations

import logging
import queue
import threading
from contextlib import contextmanager
from typing import Generator

import clickhouse_connect
import pandas as pd

log = logging.getLogger(__name__)


# ── Pool class ────────────────────────────────────────────────────────────────

class CHPool:
    """
    Thread-safe connection pool for clickhouse_connect clients.

    Parameters
    ----------
    host, port, database, username, password
        Connection parameters — default to config/settings.py values.
    min_size : int
        Connections to pre-create at startup (warm pool).
    max_size : int
        Hard cap on total live connections.
    checkout_timeout : float
        Seconds to block waiting for a free connection before raising.
    connect_timeout : float
        Seconds for each individual TCP connect attempt.
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 8123,
        database: str = "market_data",
        username: str = "default",
        password: str = "",
        min_size: int = 2,
        max_size: int = 10,
        checkout_timeout: float = 10.0,
        connect_timeout: float = 10.0,
    ) -> None:
        self._host = host
        self._port = port
        self._database = database
        self._username = username
        self._password = password
        self._min_size = min_size
        self._max_size = max_size
        self._checkout_timeout = checkout_timeout
        self._connect_timeout = connect_timeout

        self._idle: queue.Queue = queue.Queue()
        self._all: list = []
        self._lock = threading.RLock()

        # Pre-warm min_size connections if ClickHouse host is reachable
        if self._is_port_open(host, port):
            for _ in range(min_size):
                try:
                    self._idle.put(self._new_client())
                except Exception as exc:
                    log.warning("CHPool: pre-warm failed (%s) — pool will grow lazily", exc)
                    break
        else:
            log.debug("CHPool: %s:%s not reachable — pre-warm skipped, pool will grow lazily", host, port)

        log.debug(
            "CHPool ready — %d/%d connections warm (%s:%s/%s)",
            self._idle.qsize(), max_size, host, port, database,
        )

    @staticmethod
    def _is_port_open(host: str, port: int, timeout: float = 0.2) -> bool:
        """Check if TCP port is accepting connections before pre-warming."""
        import socket
        try:
            with socket.create_connection((host, port), timeout=timeout):
                return True
        except (OSError, TimeoutError):
            return False

    # ── Internal ──────────────────────────────────────────────────────────────

    def _new_client(self):
        """Create a fresh authenticated client."""
        client = clickhouse_connect.get_client(
            host=self._host,
            port=self._port,
            database=self._database,
            username=self._username,
            password=self._password,
            connect_timeout=self._connect_timeout,
            compress="lz4",  # ~3× wire reduction for OHLCV bulk reads/writes
        )
        with self._lock:
            self._all.append(client)
        log.debug("CHPool: new client created (total=%d)", len(self._all))
        return client

    def _is_healthy(self, client) -> bool:
        """Ping the client; return False if the connection is dead."""
        try:
            client.ping()
            return True
        except Exception:
            return False

    def _recycle(self, client) -> None:
        """Return a client to the idle queue, replacing it if unhealthy."""
        if self._is_healthy(client):
            self._idle.put(client)
        else:
            log.warning("CHPool: stale connection detected — replacing")
            with self._lock:
                try:
                    self._all.remove(client)
                except ValueError:
                    pass
            try:
                client.close()
            except Exception:
                pass
            try:
                self._idle.put(self._new_client())
            except Exception as exc:
                log.error("CHPool: could not replace stale connection: %s", exc)

    # ── Public API ────────────────────────────────────────────────────────────

    @contextmanager
    def acquire(self) -> Generator:
        """
        Context manager that checks out an exclusive client from the pool.

        Usage::

            with pool.acquire() as client:
                df = client.query_df("SELECT ...")
        """
        client = None
        try:
            # Try the idle queue first
            try:
                client = self._idle.get(timeout=self._checkout_timeout)
            except queue.Empty:
                # Idle queue exhausted — grow the pool if below max
                with self._lock:
                    if len(self._all) < self._max_size:
                        client = self._new_client()
                    else:
                        # Pool saturated — wait a bit longer for a return
                        pass
                if client is None:
                    # Block indefinitely (caller already waited checkout_timeout)
                    client = self._idle.get(timeout=self._checkout_timeout)

            yield client

        finally:
            if client is not None:
                self._recycle(client)

    def query_df(self, sql: str, parameters: dict | None = None) -> pd.DataFrame:
        """Execute a SELECT and return a DataFrame, using a pooled client."""
        with self.acquire() as client:
            return client.query_df(sql, parameters=parameters or {})

    def execute(self, sql: str, parameters: dict | None = None) -> None:
        """Execute a non-SELECT statement (DDL, INSERT, etc.)."""
        with self.acquire() as client:
            client.command(sql, parameters=parameters or {})

    def get_client(self):
        """
        Return a client *not* managed by the pool.

        Use only in scripts or code paths that need long-running exclusive
        access.  Caller is responsible for calling ``client.close()``.
        """
        return self._new_client()

    def close_all(self) -> None:
        """Drain the idle queue and close every known connection."""
        with self._lock:
            clients = list(self._all)
        for c in clients:
            try:
                c.close()
            except Exception:
                pass
        log.info("CHPool: all %d connections closed", len(clients))

    @property
    def size(self) -> int:
        """Total number of clients created (idle + in-use)."""
        return len(self._all)

    @property
    def idle(self) -> int:
        """Number of clients currently available in the idle queue."""
        return self._idle.qsize()

    def __repr__(self) -> str:
        return (
            f"CHPool(host={self._host}:{self._port}/{self._database} "
            f"idle={self.idle}/{self.size} max={self._max_size})"
        )


# ── Singleton ─────────────────────────────────────────────────────────────────

_pool: CHPool | None = None
_pool_lock = threading.Lock()


def get_pool() -> CHPool:
    """
    Return the module-level singleton pool, creating it on first call.

    Connection parameters are read from ``config.settings`` so a single
    ``CLICKHOUSE_HOST`` env-var change propagates everywhere.
    """
    global _pool
    if _pool is not None:
        return _pool

    with _pool_lock:
        if _pool is not None:          # double-checked locking
            return _pool

        from config.settings import settings  # local import — avoids circular deps

        min_size = getattr(settings, "clickhouse_pool_min", 2)
        max_size = getattr(settings, "clickhouse_pool_max", 10)
        timeout  = getattr(settings, "clickhouse_pool_timeout", 10.0)

        _pool = CHPool(
            host=settings.clickhouse_host,
            port=settings.clickhouse_port,
            database=settings.clickhouse_database,
            username=settings.clickhouse_user,
            password=settings.clickhouse_password,
            min_size=min_size,
            max_size=max_size,
            checkout_timeout=timeout,
        )
        log.info("CHPool singleton initialised: %r", _pool)
        return _pool


# ── Module-level convenience helpers ─────────────────────────────────────────

def query_df(sql: str, parameters: dict | None = None) -> pd.DataFrame:
    """Query helper — delegates to the singleton pool."""
    return get_pool().query_df(sql, parameters)


def execute(sql: str, parameters: dict | None = None) -> None:
    """Execute helper — delegates to the singleton pool."""
    get_pool().execute(sql, parameters)


@contextmanager
def acquire() -> Generator:
    """Acquire a pooled client — delegates to the singleton pool."""
    with get_pool().acquire() as client:
        yield client


def get_client():
    """
    Return an unmanaged client from the pool singleton.

    The connection parameters are read from ``config.settings`` (env vars) — no
    explicit host/port/user/password needed anywhere in calling code.

    The returned client is NOT pool-managed: the caller is responsible for
    calling ``client.close()`` when done.  For automatic lifecycle use
    ``acquire()`` instead.

    Usage::

        from src.db.pool import get_client

        client = get_client()
        try:
            client.insert_df("market_data.daily_prices", df)
        finally:
            client.close()
    """
    return get_pool().get_client()
