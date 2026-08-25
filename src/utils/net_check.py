"""
src/utils/net_check.py
────────────────────────
Cheap TCP reachability probe.

Qdrant client init (prefer_grpc=True) doesn't reliably honour its own
``timeout`` kwarg when the target host is simply down — gRPC's channel-level
connect/backoff can stall ~30s per call regardless, which is fine in
production (Qdrant is always up) but turns any CI/dev environment without a
running Qdrant into a multi-minute stall across the many lazy client-init
sites in this codebase. A plain socket probe fails in milliseconds and lets
callers skip straight to their existing "Qdrant unavailable" fallback path.
"""
from __future__ import annotations

import socket


def is_port_open(host: str, port: int, timeout: float = 0.2) -> bool:
    """Return True if a TCP connection to (host, port) succeeds within `timeout`s."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False
