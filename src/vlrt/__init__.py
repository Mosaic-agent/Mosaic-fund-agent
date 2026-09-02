"""VLRT v3 — volatility-targeted allocator with a bounded valuation tilt.

See docs in the module docstrings and the plan that produced this package:
returns at monthly frequency are close to unpredictable with the available data,
volatility is not — so the framework sizes risk rather than timing returns.
"""

from src.vlrt.data import VLRTData, load_all
from src.vlrt.pillars import PILLAR_WEIGHTS, build_pillars, expanding_rank

__all__ = ["VLRTData", "load_all", "build_pillars", "expanding_rank", "PILLAR_WEIGHTS"]
