"""
src/ml/correlation/strategies.py
─────────────────────────────────
Pluggable correlation strategies: maps detected price anomalies to external
signals (macro events, FX shocks, news) that impacted the price.

Architecture
────────────
The pipeline is anomaly-first:
  1. Anomaly detection (GARCH + Isolation Forest + PELT) runs independently.
  2. Strategies attribute each flagged anomaly to external events.

Each strategy splits its work into two distinct phases:

  1. ``_detect_signals()``  — **SIGNAL** phase.
     Pure detection. Iterates events/anomalies and produces raw ``_Signal``
     records carrying the features needed to score the match. No scoring,
     no thresholds, no penalties, no explanation strings.

  2. ``_score_signal()``    — **EXECUTION** phase.
     Takes one ``_Signal`` and produces a fully-scored ``CorrelationFinding``
     (or returns ``None`` when the signal fails a quality gate such as
     minimum return / lag decay / direction penalty).

The base class ``CorrelationStrategy.analyze()`` is a concrete orchestrator
that calls both phases in order, emits timestamped INFO logs at each
boundary (with elapsed milliseconds), and returns the surviving findings.

Strategies:
  - PreEventLeakStrategy         — detects insider leaks before corporate actions
  - PostMacroShockStrategy       — detects reactions after macro events
"""

from __future__ import annotations

import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import date
from typing import Any, List, Optional

import numpy as np
import pandas as pd

from .models import CandidateEvent, CorrelationFinding, EventType

log = logging.getLogger(__name__)


# ── Timestamped logging setup ─────────────────────────────────────────────────
#
# The CLI configures root logging via _setup_logging() in src/main.py with an
# asctime formatter. When the correlation engine runs from contexts where root
# logging is NOT configured (Streamlit, MCP server, pytest, ad-hoc scripts),
# we attach our own handler so every strategy log line still carries a
# timestamp. The helper is idempotent — safe to call repeatedly.

_TS_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
_TS_DATEFMT = "%Y-%m-%d %H:%M:%S"


def _ensure_timestamp_handler() -> None:
    """Attach a stream handler with asctime if neither root nor this module
    already has one. Prevents duplicate handlers on repeat calls."""
    if log.handlers:
        return
    root = logging.getLogger()
    root_has_ts = any(
        getattr(h.formatter, "_fmt", None) and "%(asctime)" in (h.formatter._fmt or "")
        for h in root.handlers
    )
    if root.handlers and root_has_ts:
        return  # root will format with timestamps — propagation does the work
    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter(_TS_FORMAT, datefmt=_TS_DATEFMT))
    log.addHandler(h)
    if log.level == logging.NOTSET:
        log.setLevel(logging.INFO)
    log.propagate = False


_ensure_timestamp_handler()


# ── Internal: signal phase output ─────────────────────────────────────────────


@dataclass
class _Signal:
    """Raw detection record produced by ``_detect_signals()``.

    Carries the features needed for ``_score_signal()`` to compute a final
    ``CorrelationFinding``. Strategy-specific fields live inside ``metrics``
    so the dataclass stays stable across strategies.
    """
    anomaly_date: date
    event: CandidateEvent
    metrics: dict = field(default_factory=dict)


# ── Base class ────────────────────────────────────────────────────────────────


class CorrelationStrategy(ABC):
    """Abstract interface for correlation mapping strategies.

    Subclasses implement ``_detect_signals`` (signal phase) and
    ``_score_signal`` (execution phase). The orchestrator ``analyze()`` is
    concrete and handles ordering, timing, and per-signal error containment.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        ...

    # ── Phase 1: SIGNAL ──
    @abstractmethod
    def _detect_signals(
        self,
        df_ohlcv: pd.DataFrame,
        df_anomaly: pd.DataFrame,
        df_benchmark: Optional[pd.DataFrame],
        events: List[CandidateEvent],
    ) -> List[_Signal]:
        """Produce raw signal candidates — no scoring or thresholding."""

    # ── Phase 2: EXECUTION ──
    @abstractmethod
    def _score_signal(
        self,
        signal: _Signal,
        df_ohlcv: pd.DataFrame,
        df_anomaly: pd.DataFrame,
        df_benchmark: Optional[pd.DataFrame],
    ) -> Optional[CorrelationFinding]:
        """Score one signal. Return ``None`` to drop it from the findings."""

    # ── Concrete orchestrator ──
    def analyze(
        self,
        df_ohlcv: pd.DataFrame,
        df_anomaly: pd.DataFrame,
        df_benchmark: Optional[pd.DataFrame],
        events: List[CandidateEvent],
    ) -> List[CorrelationFinding]:
        """SIGNAL → EXECUTION pipeline with timed, timestamped logging."""
        t0 = time.perf_counter()
        try:
            signals = self._detect_signals(df_ohlcv, df_anomaly, df_benchmark, events)
        except Exception as exc:  # noqa: BLE001 — strategy is plug-in
            log.error("[%s] SIGNAL phase failed: %s", self.name, exc, exc_info=True)
            return []
        t1 = time.perf_counter()
        log.info(
            "[%s] SIGNAL    events=%d → signals=%d (%.1f ms)",
            self.name, len(events), len(signals), (t1 - t0) * 1000.0,
        )

        findings: List[CorrelationFinding] = []
        for sig in signals:
            try:
                f = self._score_signal(sig, df_ohlcv, df_anomaly, df_benchmark)
            except Exception as exc:  # noqa: BLE001 — keep other signals
                log.warning(
                    "[%s] EXECUTION failed for signal on %s: %s",
                    self.name, sig.anomaly_date, exc,
                )
                continue
            if f is not None:
                findings.append(f)
        t2 = time.perf_counter()
        log.info(
            "[%s] EXECUTION signals=%d → findings=%d (%.1f ms, total %.1f ms)",
            self.name, len(signals), len(findings),
            (t2 - t1) * 1000.0, (t2 - t0) * 1000.0,
        )
        return findings


# ── Confidence band helper ────────────────────────────────────────────────────


def _confidence_for(score: float) -> str:
    if score >= 70.0:
        return "HIGH"
    if score >= 40.0:
        return "MODERATE"
    return "LOW"


# ── Pre-Event Leak Strategy (DEPRECATED) ─────────────────────────────────────
# Not registered by default. Kept for backward-compatibility imports.
# Rationale: the pipeline now focuses on anomaly detection + attribution to
# external signals impacting price (macro/FX). Speculative insider-leak scoring
# produced noise without actionable alpha.


class PreEventLeakStrategy(CorrelationStrategy):
    """**DEPRECATED** — no longer registered by default.

    Detects potential insider leaks or front-running prior to corporate
    actions. Scans the window [T - W, T - 1] before an ex-date.

    .. deprecated:: 2026-06-20
        Use PostMacroShockStrategy instead. This class remains importable for
        existing callers that register it explicitly.
    """

    def __init__(self, window_days: int = 5, min_score: float = 20.0) -> None:
        self.window_days = window_days
        self.min_score = min_score

    @property
    def name(self) -> str:
        return "Pre-Event Leak Detector"

    # ── Phase 1: SIGNAL ──────────────────────────────────────────────────────
    def _detect_signals(
        self,
        df_ohlcv: pd.DataFrame,
        df_anomaly: pd.DataFrame,
        df_benchmark: Optional[pd.DataFrame],
        events: List[CandidateEvent],
    ) -> List[_Signal]:
        signals: List[_Signal] = []

        company_events = [
            e for e in events
            if e.event_type in (EventType.COMPANY_FILING, EventType.NEWS_ANNOUNCEMENT)
        ]
        if not company_events or df_ohlcv.empty:
            return signals

        df_ohlcv = df_ohlcv.sort_values("trade_date").reset_index(drop=True)
        trade_dates = pd.to_datetime(df_ohlcv["trade_date"]).dt.date.tolist()

        for ev in company_events:
            ev_date = ev.trade_date
            if ev_date not in trade_dates:
                continue

            ev_idx = trade_dates.index(ev_date)
            start_idx = max(0, ev_idx - self.window_days)
            end_idx = max(0, ev_idx - 1)
            if start_idx >= end_idx:
                continue

            sub_ohlcv = df_ohlcv.iloc[start_idx:end_idx + 1]
            sub_anomaly = df_anomaly.iloc[start_idx:end_idx + 1]

            # Raw price return in pre-event window
            price_start = float(df_ohlcv.iloc[max(0, start_idx - 1)]["close"])
            price_end = float(df_ohlcv.iloc[end_idx]["close"])
            raw_return = (price_end / price_start) - 1.0

            # Cumulative Abnormal Return (CAR)
            bench_return = 0.0
            if df_benchmark is not None and not df_benchmark.empty:
                df_bench_sorted = df_benchmark.sort_values("trade_date").reset_index(drop=True)
                b_dates = pd.to_datetime(df_bench_sorted["trade_date"]).dt.date.tolist()
                try:
                    bench_start_date = df_ohlcv.iloc[max(0, start_idx - 1)]["trade_date"]
                    bench_end_date = df_ohlcv.iloc[end_idx]["trade_date"]
                    b_start_idx = b_dates.index(pd.to_datetime(bench_start_date).date())
                    b_end_idx = b_dates.index(pd.to_datetime(bench_end_date).date())
                    b_price_start = float(df_bench_sorted.iloc[b_start_idx]["close"])
                    b_price_end = float(df_bench_sorted.iloc[b_end_idx]["close"])
                    bench_return = (b_price_end / b_price_start) - 1.0 if b_price_start > 0 else 0.0
                except (ValueError, IndexError):
                    pass
            car = raw_return - bench_return

            # Abnormal Volume Ratio (AVR)
            pre_start_idx = max(0, start_idx - 20)
            hist_vol_df = df_ohlcv.iloc[pre_start_idx:start_idx]
            hist_vol_median = hist_vol_df["volume"].median() if not hist_vol_df.empty else 1.0
            if hist_vol_median <= 0:
                hist_vol_median = 1.0
            pre_vol_median = sub_ohlcv["volume"].median()
            avr = pre_vol_median / hist_vol_median

            # Volatility expansion (GARCH)
            hist_vol_garch = df_anomaly.iloc[pre_start_idx:start_idx]["garch_vol"].median() \
                if "garch_vol" in df_anomaly.columns else 1.0
            pre_vol_garch = sub_anomaly["garch_vol"].median() \
                if "garch_vol" in sub_anomaly.columns else 1.0
            vol_ratio = pre_vol_garch / hist_vol_garch \
                if (hist_vol_garch and not pd.isna(hist_vol_garch)) else 1.0

            # Count of flagged anomalies in pre-event window
            anomaly_count = 0
            if "is_anomaly" in sub_anomaly.columns:
                anomaly_count = int(sub_anomaly["is_anomaly"].sum())

            # Pick representative anomaly date for the finding
            best_anom_idx = start_idx
            if anomaly_count > 0:
                anomaly_indices = sub_anomaly[sub_anomaly["is_anomaly"] == True].index.tolist()  # noqa: E712
                if anomaly_indices:
                    best_anom_idx = anomaly_indices[-1]
            else:
                returns = sub_ohlcv["close"].pct_change().abs().values
                max_dev = np.argmax(returns) if len(returns) > 0 else 0
                best_anom_idx = start_idx + max_dev

            anomaly_date = pd.to_datetime(df_ohlcv.iloc[best_anom_idx]["trade_date"]).date()
            lead_days = int((anomaly_date - pd.to_datetime(ev_date).date()).days)

            action_type = str(ev.metadata.get("action_type", "")).lower() if ev.metadata else ""
            is_public_value_neutral = action_type in {"bonus", "split", "face_value_split"}

            signals.append(_Signal(
                anomaly_date=anomaly_date,
                event=ev,
                metrics={
                    "avr": avr,
                    "car": car,
                    "vol_ratio": vol_ratio,
                    "anomaly_count": anomaly_count,
                    "lead_days": lead_days,
                    "is_public_value_neutral": is_public_value_neutral,
                },
            ))

        return signals

    # ── Phase 2: EXECUTION ───────────────────────────────────────────────────
    def _score_signal(
        self,
        signal: _Signal,
        df_ohlcv: pd.DataFrame,
        df_anomaly: pd.DataFrame,
        df_benchmark: Optional[pd.DataFrame],
    ) -> Optional[CorrelationFinding]:
        m = signal.metrics
        avr = m["avr"]
        car = m["car"]
        vol_ratio = m["vol_ratio"]
        anomaly_count = m["anomaly_count"]
        lead_days = m["lead_days"]
        is_public_value_neutral = m["is_public_value_neutral"]

        volume_pts = min(30.0, max(0.0, (avr - 1.0) * 20.0))
        car_pts    = min(30.0, max(0.0, abs(car) * 600.0))
        vol_pts    = min(20.0, max(0.0, (vol_ratio - 1.0) * 40.0))
        anom_pts   = 20.0 if anomaly_count > 0 else 0.0
        score = volume_pts + car_pts + vol_pts + anom_pts

        if score < self.min_score:
            return None

        # Bonus/split/face-value-split are publicly announced weeks before ex-date;
        # pre-event positioning is routine arbitrage, not information leakage.
        if is_public_value_neutral:
            score *= 0.5
            explanation = (
                f"Pre-corporate-action positioning on {signal.anomaly_date} ahead of "
                f"'{signal.event.label}' ({abs(lead_days)} days before ex-date). "
                f"Detected abnormal volume ratio of {avr:.2f}x, cumulative abnormal "
                f"return of {car*100:+.2f}%, and {anomaly_count} flagged anomaly day(s) "
                f"in the pre-event window. "
                f"Note: bonus/split actions are typically publicly announced weeks "
                f"before the ex-date; early positioning is routine corporate-action "
                f"arbitrage — not information leakage."
            )
        else:
            explanation = (
                f"Anomaly on {signal.anomaly_date} occurred {abs(lead_days)} days before "
                f"the corporate action '{signal.event.label}'. Detected abnormal volume "
                f"ratio of {avr:.2f}x, cumulative abnormal return of {car*100:+.2f}%, "
                f"and {anomaly_count} flagged anomaly day(s) in the pre-event window."
            )

        return CorrelationFinding(
            anomaly_date=signal.anomaly_date,
            event=signal.event,
            strategy_name=self.name,
            correlation_score=score,
            lead_lag_days=lead_days,
            confidence=_confidence_for(score),
            explanation=explanation,
            abnormal_return=car,
        )


# ── Post-Macro Shock Strategy ─────────────────────────────────────────────────


class PostMacroShockStrategy(CorrelationStrategy):
    """Detects market price/volatility reaction immediately following a macro
    event. Scans the window [T, T + W] after a macro trigger date.
    """

    def __init__(self, window_days: int = 3, min_return_pct: float = 1.5) -> None:
        self.window_days = window_days
        self.min_return_pct = min_return_pct

    @property
    def name(self) -> str:
        return "Post-Macro Shock Trigger"

    # ── Phase 1: SIGNAL ──────────────────────────────────────────────────────
    def _detect_signals(
        self,
        df_ohlcv: pd.DataFrame,
        df_anomaly: pd.DataFrame,
        df_benchmark: Optional[pd.DataFrame],
        events: List[CandidateEvent],
    ) -> List[_Signal]:
        signals: List[_Signal] = []
        if not events or df_ohlcv.empty:
            return signals

        df_ohlcv = df_ohlcv.sort_values("trade_date").reset_index(drop=True)
        trade_dates = pd.to_datetime(df_ohlcv["trade_date"]).dt.date.tolist()

        for ev in events:
            ev_date = ev.trade_date
            matching_dates = [d for d in trade_dates if d >= ev_date]
            if not matching_dates:
                continue

            trigger_date = matching_dates[0]
            trig_idx = trade_dates.index(trigger_date)
            start_idx = trig_idx
            end_idx = min(len(df_ohlcv) - 1, trig_idx + self.window_days)

            sub_anomaly = df_anomaly.iloc[start_idx:end_idx + 1]

            # Locate the maximum-move date inside the post-event window.
            anom_indices: List[int] = []
            if "is_anomaly" in sub_anomaly.columns:
                anom_indices = sub_anomaly[sub_anomaly["is_anomaly"] == True].index.tolist()  # noqa: E712

            shock_idx = start_idx
            max_daily_ret = -1.0
            iter_indices = anom_indices if anom_indices else list(range(start_idx, end_idx + 1))
            for idx in iter_indices:
                close_prev = float(df_ohlcv.iloc[idx - 1]["close"]) if idx > 0 else float(df_ohlcv.iloc[idx]["open"])
                close_curr = float(df_ohlcv.iloc[idx]["close"])
                daily_ret = (close_curr / close_prev) - 1.0 if close_prev > 0 else 0.0
                if abs(daily_ret) > max_daily_ret:
                    max_daily_ret = abs(daily_ret)
                    shock_idx = idx

            shock_date = pd.to_datetime(df_ohlcv.iloc[shock_idx]["trade_date"]).date()

            close_prev = float(df_ohlcv.iloc[shock_idx - 1]["close"]) if shock_idx > 0 else float(df_ohlcv.iloc[shock_idx]["open"])
            close_curr = float(df_ohlcv.iloc[shock_idx]["close"])
            shock_return = (close_curr / close_prev) - 1.0 if close_prev > 0 else 0.0

            # Benchmark return on the shock date
            bench_return = 0.0
            if df_benchmark is not None and not df_benchmark.empty:
                shock_date_val = df_ohlcv.iloc[shock_idx]["trade_date"]
                df_bench_match = df_benchmark[df_benchmark["trade_date"] == shock_date_val]
                if not df_bench_match.empty:
                    bench_idx_list = df_benchmark.index[df_benchmark["trade_date"] == shock_date_val].tolist()
                    if bench_idx_list:
                        b_idx = bench_idx_list[0]
                        b_close_curr = float(df_benchmark.iloc[b_idx]["close"])
                        b_close_prev = float(df_benchmark.iloc[b_idx - 1]["close"]) if b_idx > 0 else b_close_curr
                        bench_return = (b_close_curr / b_close_prev) - 1.0 if b_close_prev > 0 else 0.0

            abnormal_return = shock_return - bench_return
            is_anomaly_day = bool(sub_anomaly.loc[shock_idx, "is_anomaly"]) \
                if ("is_anomaly" in sub_anomaly.columns and shock_idx in sub_anomaly.index) \
                else False

            signals.append(_Signal(
                anomaly_date=shock_date,
                event=ev,
                metrics={
                    "shock_return": shock_return,
                    "abnormal_return": abnormal_return,
                    "is_anomaly_day": is_anomaly_day,
                    "lag_days": int((shock_date - ev_date).days),
                },
            ))

        return signals

    # ── Phase 2: EXECUTION ───────────────────────────────────────────────────
    def _score_signal(
        self,
        signal: _Signal,
        df_ohlcv: pd.DataFrame,
        df_anomaly: pd.DataFrame,
        df_benchmark: Optional[pd.DataFrame],
    ) -> Optional[CorrelationFinding]:
        m = signal.metrics
        shock_return    = m["shock_return"]
        abnormal_return = m["abnormal_return"]
        is_anomaly_day  = m["is_anomaly_day"]
        lag_days        = m["lag_days"]

        # Score on the *idiosyncratic* (market-adjusted) move, not the raw
        # return.  A 5% drop where NIFTY was also down 5% is a market event,
        # not a stock-specific reaction to the macro trigger.  Falls back to
        # raw return when no benchmark was provided (abnormal_return is None).
        signal_return = abnormal_return if abnormal_return is not None else shock_return
        move_val = abs(signal_return) * 100.0
        # Filter: must have meaningful idiosyncratic move OR be a flagged anomaly day
        if not (move_val >= self.min_return_pct or is_anomaly_day):
            return None

        score = min(100.0, move_val * 25.0)
        if is_anomaly_day:
            score = min(100.0, score + 20.0)

        # Lag-weighted decay — peak weight at lag=+1 (textbook next-day
        # reaction to a public event). Same-day matches are statistically
        # easier to hit by coincidence (any anomaly day will likely have
        # *some* macro headline on the same calendar date), so they receive
        # a small relative penalty vs. +1.
        # Curve: weight = exp(-|lag - 1| / 3.0)
        #   lag = -2 → 0.37   lag = -1 → 0.51   lag =  0 → 0.72
        #   lag = +1 → 1.00   lag = +2 → 0.72   lag = +3 → 0.51
        lag_weight = np.exp(-abs(lag_days - 1) / 3.0)
        score = score * lag_weight

        ev = signal.event
        if score < 15.0:
            return None

        explanation = (
            f"Anomaly day/shock on {signal.anomaly_date} mapped {lag_days} days after "
            f"macro event '{ev.label}'. Asset exhibited maximum absolute return "
            f"deviation of {shock_return*100:+.2f}% with post-event anomaly flag: "
            f"{is_anomaly_day}."
        )
        if (
            abs(abnormal_return) >= 0.02
            and ev.event_type in (EventType.MACRO_RATE_DECISION, EventType.MACRO_GEOPOLITICAL)
        ):
            explanation += (
                " ⚠️ Note: this is a market-adjusted residual return. A large abnormal "
                "return alongside a broad macro event suggests idiosyncratic "
                "amplification or model mis-specification — verify company-specific "
                "catalysts before attributing to the macro trigger."
            )

        return CorrelationFinding(
            anomaly_date=signal.anomaly_date,
            event=ev,
            strategy_name=self.name,
            correlation_score=score,
            lead_lag_days=lag_days,
            confidence=_confidence_for(score),
            explanation=explanation,
            abnormal_return=abnormal_return,
        )
