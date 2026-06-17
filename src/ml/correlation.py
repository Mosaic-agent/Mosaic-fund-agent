"""
src/ml/correlation.py
──────────────────────
Extensible Correlation Service to map price/volume anomalies to company-specific
corporate actions/filings and global macro events using pluggable strategies.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import date, datetime
from enum import Enum
from typing import Any, List, Optional

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)


class EventType(str, Enum):
    COMPANY_FILING = "company_filing"
    NEWS_ANNOUNCEMENT = "news_announcement"
    MACRO_RATE_DECISION = "macro_rate_decision"
    MACRO_COMMODITY_SHOCK = "macro_commodity_shock"
    MACRO_GEOPOLITICAL = "macro_geopolitical"


@dataclass
class CandidateEvent:
    """A qualitative corporate filing or external macro event."""
    trade_date: date
    event_type: EventType
    label: str
    description: str
    metadata: dict = field(default_factory=dict)


@dataclass
class CorrelationFinding:
    """The mapping of an anomaly day to a candidate event trigger."""
    anomaly_date: date
    event: CandidateEvent
    strategy_name: str
    correlation_score: float  # 0 to 100
    lead_lag_days: int        # Negative = anomaly is BEFORE event (leak), Positive = AFTER (reaction)
    confidence: str           # "HIGH" | "MODERATE" | "LOW"
    explanation: str
    abnormal_return: Optional[float] = None


class CorrelationStrategy(ABC):
    """Abstract interface for correlation mapping strategies."""

    @property
    @abstractmethod
    def name(self) -> str:
        pass

    @abstractmethod
    def analyze(
        self,
        df_ohlcv: pd.DataFrame,
        df_anomaly: pd.DataFrame,  # df with 'is_anomaly', 'garch_vol'
        df_benchmark: Optional[pd.DataFrame],
        events: List[CandidateEvent],
    ) -> List[CorrelationFinding]:
        """
        Scans events and daily prices to find correlated anomalies.
        """
        pass


# ── Pre-Event Leak Strategy ───────────────────────────────────────────────────

class PreEventLeakStrategy(CorrelationStrategy):
    """
    Detects potential insider leaks or front-running prior to corporate actions.
    Scans the window [T - W, T - 1] before an ex-date.
    """

    def __init__(self, window_days: int = 5, min_score: float = 20.0) -> None:
        self.window_days = window_days
        self.min_score = min_score

    @property
    def name(self) -> str:
        return "Pre-Event Leak Detector"

    def analyze(
        self,
        df_ohlcv: pd.DataFrame,
        df_anomaly: pd.DataFrame,
        df_benchmark: Optional[pd.DataFrame],
        events: List[CandidateEvent],
    ) -> List[CorrelationFinding]:
        findings: List[CorrelationFinding] = []

        # Filter events for company filings/actions and news announcements
        company_events = [e for e in events if e.event_type in (EventType.COMPANY_FILING, EventType.NEWS_ANNOUNCEMENT)]
        if not company_events or df_ohlcv.empty:
            return findings

        # Ensure sorted trade dates
        df_ohlcv = df_ohlcv.sort_values("trade_date").reset_index(drop=True)
        trade_dates = pd.to_datetime(df_ohlcv["trade_date"]).dt.date.tolist()

        for ev in company_events:
            ev_date = ev.trade_date
            if ev_date not in trade_dates:
                continue

            ev_idx = trade_dates.index(ev_date)
            # Pre-event window indices
            start_idx = max(0, ev_idx - self.window_days)
            end_idx = max(0, ev_idx - 1)

            if start_idx >= end_idx:
                continue

            sub_ohlcv = df_ohlcv.iloc[start_idx:end_idx + 1]
            sub_anomaly = df_anomaly.iloc[start_idx:end_idx + 1]

            # Calculate price return in pre-event window
            price_start = float(df_ohlcv.iloc[max(0, start_idx - 1)]["close"])
            price_end = float(df_ohlcv.iloc[end_idx]["close"])
            raw_return = (price_end / price_start) - 1.0

            # Cumulative Abnormal Return (CAR)
            bench_return = 0.0
            if df_benchmark is not None and not df_benchmark.empty:
                df_bench_sorted = df_benchmark.sort_values("trade_date").reset_index(drop=True)
                b_dates = pd.to_datetime(df_bench_sorted["trade_date"]).dt.date.tolist()
                try:
                    # Find matching dates in benchmark
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

            # Abnormal Volume Ratio (AVR) relative to preceding 20 days
            pre_start_idx = max(0, start_idx - 20)
            hist_vol_df = df_ohlcv.iloc[pre_start_idx:start_idx]
            hist_vol_median = hist_vol_df["volume"].median() if not hist_vol_df.empty else 1.0
            if hist_vol_median <= 0:
                hist_vol_median = 1.0

            pre_vol_median = sub_ohlcv["volume"].median()
            avr = pre_vol_median / hist_vol_median

            # Volatility expansion (GARCH)
            hist_vol_garch = df_anomaly.iloc[pre_start_idx:start_idx]["garch_vol"].median() if "garch_vol" in df_anomaly.columns else 1.0
            pre_vol_garch = sub_anomaly["garch_vol"].median() if "garch_vol" in sub_anomaly.columns else 1.0
            vol_ratio = pre_vol_garch / hist_vol_garch if (hist_vol_garch and not pd.isna(hist_vol_garch)) else 1.0

            # Count of flagged anomalies in pre-event window
            anomaly_count = 0
            if "is_anomaly" in sub_anomaly.columns:
                anomaly_count = int(sub_anomaly["is_anomaly"].sum())

            # ── Score calculation (0-100) ──
            # 1. Volume spike contribution (max 30 pts)
            volume_pts = min(30.0, max(0.0, (avr - 1.0) * 20.0))
            # 2. CAR run-up contribution (max 30 pts)
            car_pts = min(30.0, max(0.0, abs(car) * 600.0))
            # 3. GARCH vol expansion contribution (max 20 pts)
            vol_pts = min(20.0, max(0.0, (vol_ratio - 1.0) * 40.0))
            # 4. Anomaly presence (max 20 pts)
            anom_pts = 20.0 if anomaly_count > 0 else 0.0

            score = volume_pts + car_pts + vol_pts + anom_pts

            if score >= self.min_score:
                # Find the most anomalous day in the window to label as the anomaly date
                best_anom_idx = start_idx
                if anomaly_count > 0:
                    anomaly_indices = sub_anomaly[sub_anomaly["is_anomaly"] == True].index.tolist()
                    if anomaly_indices:
                        best_anom_idx = anomaly_indices[-1]
                else:
                    # Fallback: largest return deviation day
                    returns = sub_ohlcv["close"].pct_change().abs().values
                    max_dev = np.argmax(returns) if len(returns) > 0 else 0
                    best_anom_idx = start_idx + max_dev

                anomaly_date = pd.to_datetime(df_ohlcv.iloc[best_anom_idx]["trade_date"]).date()
                lead_days = int((anomaly_date - pd.to_datetime(ev_date).date()).days)

                # Bonus/split/face-value-split are publicly announced weeks before ex-date;
                # pre-event positioning is routine arbitrage, not information leakage.
                action_type = str(ev.metadata.get("action_type", "")).lower() if ev.metadata else ""
                is_public_value_neutral = action_type in {"bonus", "split", "face_value_split"}
                if is_public_value_neutral:
                    score *= 0.5

                confidence = "LOW"
                if score >= 70.0:
                    confidence = "HIGH"
                elif score >= 40.0:
                    confidence = "MODERATE"

                if is_public_value_neutral:
                    explanation = (
                        f"Pre-corporate-action positioning on {anomaly_date} ahead of '{ev.label}' "
                        f"({abs(lead_days)} days before ex-date). "
                        f"Detected abnormal volume ratio of {avr:.2f}x, cumulative abnormal return of {car*100:+.2f}%, "
                        f"and {anomaly_count} flagged anomaly day(s) in the pre-event window. "
                        f"Note: bonus/split actions are typically publicly announced weeks before the ex-date; "
                        f"early positioning is routine corporate-action arbitrage — not information leakage."
                    )
                else:
                    explanation = (
                        f"Anomaly on {anomaly_date} occurred {abs(lead_days)} days before the corporate action '{ev.label}'. "
                        f"Detected abnormal volume ratio of {avr:.2f}x, cumulative abnormal return of {car*100:+.2f}%, "
                        f"and {anomaly_count} flagged anomaly day(s) in the pre-event window."
                    )

                findings.append(
                    CorrelationFinding(
                        anomaly_date=anomaly_date,
                        event=ev,
                        strategy_name=self.name,
                        correlation_score=score,
                        lead_lag_days=lead_days,
                        confidence=confidence,
                        explanation=explanation,
                        abnormal_return=car,
                    )
                )

        return findings


# ── Post-Macro Shock Strategy ─────────────────────────────────────────────────

class PostMacroShockStrategy(CorrelationStrategy):
    """
    Detects market price/volatility reaction immediately following a macro event.
    Scans the window [T, T + W] after a macro trigger date.
    """

    def __init__(self, window_days: int = 3, min_return_pct: float = 1.5) -> None:
        self.window_days = window_days
        self.min_return_pct = min_return_pct

    @property
    def name(self) -> str:
        return "Post-Macro Shock Trigger"

    def analyze(
        self,
        df_ohlcv: pd.DataFrame,
        df_anomaly: pd.DataFrame,
        df_benchmark: Optional[pd.DataFrame],
        events: List[CandidateEvent],
    ) -> List[CorrelationFinding]:
        findings: List[CorrelationFinding] = []

        candidate_events = events
        if not candidate_events or df_ohlcv.empty:
            return findings

        df_ohlcv = df_ohlcv.sort_values("trade_date").reset_index(drop=True)
        trade_dates = pd.to_datetime(df_ohlcv["trade_date"]).dt.date.tolist()

        for ev in candidate_events:
            ev_date = ev.trade_date
            # Find the closest trading date on or after the macro event
            matching_dates = [d for d in trade_dates if d >= ev_date]
            if not matching_dates:
                continue
            
            trigger_date = matching_dates[0]
            trig_idx = trade_dates.index(trigger_date)

            start_idx = trig_idx
            end_idx = min(len(df_ohlcv) - 1, trig_idx + self.window_days)

            sub_ohlcv = df_ohlcv.iloc[start_idx:end_idx + 1]
            sub_anomaly = df_anomaly.iloc[start_idx:end_idx + 1]

            # 1. Identify shock date in the window [T, T + W]
            anom_indices = []
            if "is_anomaly" in sub_anomaly.columns:
                anom_indices = sub_anomaly[sub_anomaly["is_anomaly"] == True].index.tolist()

            shock_idx = start_idx
            if anom_indices:
                # Prioritize actual anomaly days: pick the one with largest absolute daily return
                max_daily_ret = -1.0
                for idx in anom_indices:
                    close_prev = float(df_ohlcv.iloc[idx - 1]["close"]) if idx > 0 else float(df_ohlcv.iloc[idx]["open"])
                    close_curr = float(df_ohlcv.iloc[idx]["close"])
                    daily_ret = (close_curr / close_prev) - 1.0 if close_prev > 0 else 0.0
                    if abs(daily_ret) > max_daily_ret:
                        max_daily_ret = abs(daily_ret)
                        shock_idx = idx
            else:
                # Fallback: day with largest absolute daily return in the window
                max_daily_ret = -1.0
                for idx in range(start_idx, end_idx + 1):
                    close_prev = float(df_ohlcv.iloc[idx - 1]["close"]) if idx > 0 else float(df_ohlcv.iloc[idx]["open"])
                    close_curr = float(df_ohlcv.iloc[idx]["close"])
                    daily_ret = (close_curr / close_prev) - 1.0 if close_prev > 0 else 0.0
                    if abs(daily_ret) > max_daily_ret:
                        max_daily_ret = abs(daily_ret)
                        shock_idx = idx

            shock_date = pd.to_datetime(df_ohlcv.iloc[shock_idx]["trade_date"]).date()
            
            # Observed daily return on the shock date
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

            is_anomaly_day = bool(sub_anomaly.loc[shock_idx, "is_anomaly"]) if ("is_anomaly" in sub_anomaly.columns and shock_idx in sub_anomaly.index) else False

            # Calculate score based on price movement size & anomaly status
            move_val = abs(shock_return) * 100.0
            score = min(100.0, move_val * 25.0)  # 4% move = 100 points
            if is_anomaly_day:
                score = min(100.0, score + 20.0)

            # We trigger a correlation if the asset moved significantly or had an anomaly
            if move_val >= self.min_return_pct or is_anomaly_day:
                lag_days = int((shock_date - ev_date).days)

                # Apply lag weight decay
                lag_weight = np.exp(-abs(lag_days) / 2.0)
                score = score * lag_weight

                # For FX shock events, apply direction-consistency check before the
                # threshold filter: same-direction co-movement contradicts a negative-beta
                # relationship and likely reflects a spurious date-proximity match.
                direction_mismatch = False
                if ev.event_type == EventType.MACRO_COMMODITY_SHOCK and ev.metadata:
                    fx_pct = float(ev.metadata.get("fx_pct_change", 0.0))
                    if fx_pct != 0.0 and shock_return != 0.0 and (fx_pct * shock_return) > 0:
                        score *= 0.3
                        direction_mismatch = True

                # If score falls below a minimum threshold after decay, skip it
                if score < 15.0:
                    continue

                confidence = "LOW"
                if score >= 70.0:
                    confidence = "HIGH"
                elif score >= 40.0:
                    confidence = "MODERATE"

                explanation = (
                    f"Anomaly day/shock on {shock_date} mapped {lag_days} days after macro event '{ev.label}'. "
                    f"Asset exhibited maximum absolute return deviation of {shock_return*100:+.2f}% "
                    f"with post-event anomaly flag: {is_anomaly_day}."
                )
                if direction_mismatch:
                    explanation += (
                        " ⚠️ Direction mismatch: stock and FX moved in the same direction on this date, "
                        "contradicting a negative-beta relationship — this match is likely spurious."
                    )
                if (
                    abs(abnormal_return) >= 0.02
                    and ev.event_type in (EventType.MACRO_RATE_DECISION, EventType.MACRO_GEOPOLITICAL)
                ):
                    explanation += (
                        " ⚠️ Note: this is a market-adjusted residual return. A large abnormal return "
                        "alongside a broad macro event suggests idiosyncratic amplification or model "
                        "mis-specification — verify company-specific catalysts before attributing to the macro trigger."
                    )

                findings.append(
                    CorrelationFinding(
                        anomaly_date=shock_date,
                        event=ev,
                        strategy_name=self.name,
                        correlation_score=score,
                        lead_lag_days=lag_days,
                        confidence=confidence,
                        explanation=explanation,
                        abnormal_return=abnormal_return,
                    )
                )

        return findings


# ── Cross-Asset Co-Movement Strategy ──────────────────────────────────────────

class CrossAssetCoMovementStrategy(CorrelationStrategy):
    """
    Correlates stock/ETF anomalies with extreme macro currency or commodity daily shocks.
    Maps co-movements within a window [T - 1, T + 1].
    """

    @property
    def name(self) -> str:
        return "Cross-Asset Co-Movement"

    def analyze(
        self,
        df_ohlcv: pd.DataFrame,
        df_anomaly: pd.DataFrame,
        df_benchmark: Optional[pd.DataFrame],
        events: List[CandidateEvent],
    ) -> List[CorrelationFinding]:
        findings: List[CorrelationFinding] = []

        shocks = [e for e in events if e.event_type == EventType.MACRO_COMMODITY_SHOCK]
        if not shocks or df_ohlcv.empty:
            return findings

        df_ohlcv = df_ohlcv.sort_values("trade_date").reset_index(drop=True)
        trade_dates = pd.to_datetime(df_ohlcv["trade_date"]).dt.date.tolist()

        for ev in shocks:
            ev_date = ev.trade_date
            # Check if there is an anomaly day within 1 trading day of the shock
            for idx, row in df_anomaly.iterrows():
                if not row.get("is_anomaly", False):
                    continue
                
                anom_date = pd.to_datetime(row["trade_date"]).date()
                days_diff = (anom_date - ev_date).days
                if abs(days_diff) <= 1:
                    score = 75.0 if days_diff == 0 else 50.0
                    confidence = "HIGH" if days_diff == 0 else "MODERATE"

                    explanation = (
                        f"Price anomaly on {anom_date} correlated with extreme asset shock '{ev.label}' "
                        f"on {ev_date} (lead/lag offset: {days_diff} days)."
                    )

                    # Direction-consistency check: for a negative-beta stock, FX and stock
                    # should move in opposite directions. Same-direction co-movement contradicts
                    # the beta relationship and likely reflects a spurious date-proximity match.
                    fx_pct = float(ev.metadata.get("fx_pct_change", 0.0)) if ev.metadata else 0.0

                    prev_idx = max(0, idx - 1)
                    prev_close = float(df_ohlcv.iloc[prev_idx]["close"])
                    curr_close = float(df_ohlcv.iloc[idx]["close"])
                    daily_ret = (curr_close / prev_close) - 1.0 if prev_close > 0 else 0.0

                    # Benchmark return on the shock date
                    bench_return = 0.0
                    if df_benchmark is not None and not df_benchmark.empty:
                        anom_date_val = df_ohlcv.iloc[idx]["trade_date"]
                        df_bench_match = df_benchmark[df_benchmark["trade_date"] == anom_date_val]
                        if not df_bench_match.empty:
                            b_idx_list = df_benchmark.index[df_benchmark["trade_date"] == anom_date_val].tolist()
                            if b_idx_list:
                                b_idx = b_idx_list[0]
                                b_close_curr = float(df_benchmark.iloc[b_idx]["close"])
                                b_close_prev = float(df_benchmark.iloc[b_idx - 1]["close"]) if b_idx > 0 else b_close_curr
                                bench_return = (b_close_curr / b_close_prev) - 1.0 if b_close_prev > 0 else 0.0

                    abnormal_return = daily_ret - bench_return

                    # Penalise same-direction co-movement. For a negative-beta stock
                    # (FX up → stock down), both moving up or both moving down on the
                    # same date is directionally inconsistent and likely a spurious match.
                    if fx_pct != 0.0 and daily_ret != 0.0 and (fx_pct * daily_ret) > 0:
                        score *= 0.3
                        explanation += (
                            " ⚠️ Direction mismatch: stock and FX moved in the same direction on "
                            "this date, contradicting a negative-beta relationship — this match is "
                            "likely spurious."
                        )

                    findings.append(
                        CorrelationFinding(
                            anomaly_date=anom_date,
                            event=ev,
                            strategy_name=self.name,
                            correlation_score=score,
                            lead_lag_days=days_diff,
                            confidence=confidence,
                            explanation=explanation,
                            abnormal_return=abnormal_return,
                        )
                    )

        return findings


# ── Correlation Service Orchestrator ──────────────────────────────────────────

class CorrelationService:
    """Orchestrates candidate event loading and pluggable correlation strategies."""

    def __init__(self) -> None:
        self._strategies: List[CorrelationStrategy] = []
        # Register default strategies
        self.register_strategy(PreEventLeakStrategy())
        self.register_strategy(PostMacroShockStrategy())
        self.register_strategy(CrossAssetCoMovementStrategy())

    def register_strategy(self, strategy: CorrelationStrategy) -> None:
        self._strategies.append(strategy)

    def find_correlations(
        self,
        symbol: str,
        df_ohlcv: pd.DataFrame,
        df_benchmark: Optional[pd.DataFrame] = None,
        lookback_days: int = 365,
    ) -> List[CorrelationFinding]:
        """
        Executes all registered strategies against price data and candidate events.
        """
        findings: List[CorrelationFinding] = []
        if df_ohlcv.empty or len(df_ohlcv) < 5:
            return findings

        # Run composite anomaly pipeline on the stock to retrieve GARCH & anomaly flags
        from src.ml.anomaly import run_composite_anomaly
        # Ensure dates are datetime
        df_ohlcv = df_ohlcv.copy()
        df_ohlcv["trade_date"] = pd.to_datetime(df_ohlcv["trade_date"])

        # Fetch corporate actions for corporate actions candidate events
        df_corp = self._load_corp_actions(symbol)
        df_anomaly_res, _, _ = run_composite_anomaly(df_ohlcv, df_corp_actions=df_corp)

        # Build candidate event registry
        events = self._build_candidate_events(symbol, df_corp)

        # Ingestion of dynamic company-specific news
        news_events = self._fetch_symbol_news(symbol, lookback_days)
        events.extend(news_events)

        # Execute strategies
        for strat in self._strategies:
            try:
                strat_findings = strat.analyze(df_ohlcv, df_anomaly_res, df_benchmark, events)
                findings.extend(strat_findings)
            except Exception as exc:
                log.error("Correlation Strategy %s failed: %s", strat.name, exc, exc_info=True)

        # Apply news quality and source hierarchy weights
        adjusted_findings: List[CorrelationFinding] = []
        for f in findings:
            # 1. Source Hierarchy Weight
            h_weight = 1.0
            et = f.event.event_type
            if et in (EventType.COMPANY_FILING, EventType.NEWS_ANNOUNCEMENT):
                text = (f.event.label + " " + f.event.description).lower()
                # Use clean word matching to avoid substring issues like matching "it" in "profits"
                clean_text = "".join(c if c.isalnum() or c.isspace() else " " for c in text)
                words = set(clean_text.split())
                sector_keywords = {"sector", "industry", "auto", "it", "banking", "pharma", "metal", "oil", "commodity"}
                if words & sector_keywords:
                    h_weight = 0.8
                else:
                    h_weight = 1.0
            elif et in (EventType.MACRO_RATE_DECISION, EventType.MACRO_GEOPOLITICAL, EventType.MACRO_COMMODITY_SHOCK):
                h_weight = 0.5

            # 2. News Quality Weight (NEWS_ANNOUNCEMENT only)
            nq_weight = 1.0
            if et == EventType.NEWS_ANNOUNCEMENT:
                text = (f.event.label + " " + f.event.description).lower()
                source = str(f.event.metadata.get("source", "")).lower()
                url = str(f.event.metadata.get("url", "")).lower()
                
                # Check speculative/opinion news first or blocklist publishers (simplywall.st, blogs)
                if any(k in text or k in source or k in url for k in ["simplywall.st", "simplywall", "blog", "opinion article"]):
                    nq_weight = 0.0
                elif any(k in text for k in ["could get bumped", "should weakness", "opinion", "why simply", "foolish", "target by 20", "target by 2", "motherson sumi share price target"]):
                    nq_weight = 0.10
                elif any(k in text for k in ["earnings", "quarterly results", "financial performance", "q1", "q2", "q3", "q4", "revenue", "net profit", "ebitda", "profit after tax"]):
                    nq_weight = 1.0
                elif any(k in text for k in ["guidance", "outlook", "forecast", "projection"]):
                    nq_weight = 0.90
                elif any(k in text for k in ["bonus", "split", "demerger", "rights issue"]):
                    nq_weight = 0.85
                elif "dividend" in text:
                    nq_weight = 0.70
                elif any(k in text for k in ["upgrade", "downgrade", "recommendation", "broker", "rating", "target price"]):
                    nq_weight = 0.60
                else:
                    nq_weight = 0.20

            # Compute final adjusted score
            f.correlation_score = f.correlation_score * nq_weight * h_weight

            # Filter out findings with adjusted score < 15.0
            if f.correlation_score < 15.0:
                continue

            # Update confidence based on adjusted score
            if f.correlation_score >= 70.0:
                f.confidence = "HIGH"
            elif f.correlation_score >= 40.0:
                f.confidence = "MODERATE"
            else:
                f.confidence = "LOW"

            adjusted_findings.append(f)

        findings = adjusted_findings

        # Group by anomaly date to deduplicate / cluster
        by_date: dict[Any, List[CorrelationFinding]] = {}
        for f in findings:
            by_date.setdefault(f.anomaly_date, []).append(f)

        deduped_findings: List[CorrelationFinding] = []
        for anom_date, date_findings in by_date.items():
            if len(date_findings) == 1:
                deduped_findings.append(date_findings[0])
            else:
                # Sort by score descending, then by strategy_name (to be deterministic)
                date_findings = sorted(date_findings, key=lambda x: (-x.correlation_score, x.strategy_name))
                primary = date_findings[0]
                secondary_trigs = date_findings[1:]

                # Merge secondary triggers into explanation
                extra_explanations = []
                for sec in secondary_trigs:
                    offset_str = f"{sec.lead_lag_days:+}d"
                    extra_explanations.append(
                        f"{sec.event.label} ({offset_str} offset, score: {sec.correlation_score:.1f} via {sec.strategy_name})"
                    )
                
                # Append to primary's explanation
                new_explanation = primary.explanation + "\n   *Secondary Triggers:* " + "; ".join(extra_explanations)
                
                # Create a new CorrelationFinding with the merged explanation
                merged_finding = CorrelationFinding(
                    anomaly_date=primary.anomaly_date,
                    event=primary.event,
                    strategy_name=primary.strategy_name,
                    correlation_score=primary.correlation_score,
                    lead_lag_days=primary.lead_lag_days,
                    confidence=primary.confidence,
                    explanation=new_explanation,
                )
                deduped_findings.append(merged_finding)

        # Sort by anomaly date
        findings = sorted(deduped_findings, key=lambda f: (f.anomaly_date, -f.correlation_score))
        return findings

    def _load_corp_actions(self, symbol: str) -> Optional[pd.DataFrame]:
        try:
            from src.db.pool import query_df
            _ca = query_df(
                "SELECT ex_date, action_type, ratio, purpose "
                "FROM market_data.corporate_actions FINAL "
                "WHERE symbol = {sym:String}",
                parameters={"sym": symbol.upper()},
            )
            if not _ca.empty:
                _ca["ex_date"] = pd.to_datetime(_ca["ex_date"])
                return _ca
        except Exception as e:
            log.warning("Failed to load corporate actions for %s: %s", symbol, e)
        return None

    def _build_candidate_events(self, symbol: str, df_corp: Optional[pd.DataFrame]) -> List[CandidateEvent]:
        events: List[CandidateEvent] = []

        # 1. Company Filings
        if df_corp is not None and not df_corp.empty:
            for _, row in df_corp.iterrows():
                ex_date = pd.to_datetime(row["ex_date"]).date()
                action_type = str(row["action_type"])
                ratio = str(row["ratio"])
                purpose = str(row["purpose"])

                events.append(
                    CandidateEvent(
                        trade_date=ex_date,
                        event_type=EventType.COMPANY_FILING,
                        label=f"{action_type.upper()} ({ratio})" if ratio else action_type.upper(),
                        description=purpose,
                        metadata={"action_type": action_type, "ratio": ratio},
                    )
                )

        # 2. Add major interest rate decisions / macro policy events in 2025-2026
        macro_milestones = [
            # Fed decisions
            (date(2025, 9, 18), EventType.MACRO_RATE_DECISION, "US Fed Rate Cut (-50 bps)", "Fed pivot kicks off policy easing cycle"),
            (date(2025, 11, 7), EventType.MACRO_RATE_DECISION, "US Fed Rate Cut (-25 bps)", "Fed rate cut following election results"),
            (date(2025, 12, 18), EventType.MACRO_RATE_DECISION, "US Fed Rate Cut (-25 bps)", "Final Fed easing of 2025"),
            (date(2026, 3, 19), EventType.MACRO_RATE_DECISION, "US Fed Meeting Pause", "Fed holds rates steady amid sticky inflation"),
            # RBI policy decisions
            (date(2025, 10, 9), EventType.MACRO_RATE_DECISION, "RBI Policy Pause", "RBI holds repo rate at 6.50%"),
            (date(2025, 12, 5), EventType.MACRO_RATE_DECISION, "RBI Repo Rate Cut (-25 bps)", "RBI starts monetary easing cycle"),
            (date(2026, 2, 6), EventType.MACRO_RATE_DECISION, "RBI Policy Pause", "RBI pauses rate cuts to monitor food inflation"),
            # Geopolitical major shocks
            (date(2025, 10, 1), EventType.MACRO_GEOPOLITICAL, "Middle East Geopolitical Escalation", "Spike in energy and global risk off sentiment"),
            (date(2026, 1, 12), EventType.MACRO_GEOPOLITICAL, "Global Trade War Tariff Tariffs", "Geopolitical tensions trigger worldwide supply shock"),
        ]

        for dt, ev_type, label, desc in macro_milestones:
            events.append(CandidateEvent(trade_date=dt, event_type=ev_type, label=label, description=desc))

        # 3. Dynamic USDINR extreme rate shocks (from fx_rates table)
        try:
            from src.db.pool import query_df
            df_fx = query_df(
                "SELECT trade_date, toFloat64(close) AS close "
                "FROM market_data.fx_rates FINAL WHERE symbol = 'USDINR' "
                "ORDER BY trade_date ASC"
            )
            if not df_fx.empty:
                df_fx["trade_date"] = pd.to_datetime(df_fx["trade_date"])
                df_fx["pct_change"] = df_fx["close"].pct_change()
                # Find days with > 0.75% currency movement (large shock for USDINR)
                extreme_fx = df_fx[df_fx["pct_change"].abs() >= 0.0075]
                for _, row in extreme_fx.iterrows():
                    fx_date = pd.to_datetime(row["trade_date"]).date()
                    pct = float(row["pct_change"])
                    direction = "Depreciation" if pct > 0 else "Appreciation"
                    events.append(
                        CandidateEvent(
                            trade_date=fx_date,
                            event_type=EventType.MACRO_COMMODITY_SHOCK,
                            label=f"USDINR {direction} ({pct*100:+.2f}%)",
                            description=f"Significant daily currency volatility shock in INR exchange rates.",
                            metadata={"fx_pct_change": float(pct)},
                        )
                    )
        except Exception as e:
            log.warning("Could not dynamically build USDINR macro events: %s", e)

        # 4. Dynamic DXY extreme rate shocks (from daily_prices table)
        try:
            from src.db.pool import query_df
            df_dxy = query_df(
                "SELECT trade_date, toFloat64(close) AS close "
                "FROM market_data.daily_prices FINAL WHERE symbol = 'DX-Y.NYB' "
                "ORDER BY trade_date ASC"
            )
            if not df_dxy.empty:
                df_dxy["trade_date"] = pd.to_datetime(df_dxy["trade_date"])
                df_dxy["pct_change"] = df_dxy["close"].pct_change()
                # Find days with > 0.50% global dollar index movement (large shock for DXY)
                extreme_dxy = df_dxy[df_dxy["pct_change"].abs() >= 0.0050]
                for _, row in extreme_dxy.iterrows():
                    dxy_date = pd.to_datetime(row["trade_date"]).date()
                    pct = float(row["pct_change"])
                    direction = "Rise" if pct > 0 else "Fall"
                    events.append(
                        CandidateEvent(
                            trade_date=dxy_date,
                            event_type=EventType.MACRO_COMMODITY_SHOCK,
                            label=f"DXY {direction} ({pct*100:+.2f}%)",
                            description=f"Significant daily currency volatility shock in US Dollar Index (DXY).",
                            metadata={"fx_pct_change": float(pct)},
                        )
                    )
        except Exception as e:
            log.warning("Could not dynamically build DXY macro events: %s", e)

        return events

    def _fetch_symbol_news(self, symbol: str, lookback_days: int) -> List[CandidateEvent]:
        """
        Fetches up to 20 Google News RSS articles for the stock over the lookback window.
        """
        try:
            from gnews import GNews
            from config.settings import settings
            from dateutil import parser as date_parser
            import pytz
            from src.utils.symbol_mapper import get_company_name

            # Create a GNews client
            client = GNews(
                language="en",
                country="IN",
                max_results=20,
                period=f"{lookback_days}d",
            )
            company_name = get_company_name(symbol)
            query = f"{company_name} NSE stock" if company_name else f"{symbol} NSE stock"
            articles = client.get_news(query)
            if not articles:
                articles = client.get_news(symbol)

            events = []
            tz = pytz.timezone(settings.market_timezone or "Asia/Kolkata")
            for a in articles:
                pub_date_str = a.get("published date", "")
                if not pub_date_str:
                    continue
                try:
                    pub_date = date_parser.parse(pub_date_str)
                    if pub_date.tzinfo is not None:
                        pub_date = pub_date.astimezone(tz)
                    pub_dt = pub_date.date()
                except Exception:
                    continue

                title = a.get("title") or ""
                desc = a.get("description") or ""
                publisher = a.get("publisher", {})
                source = publisher.get("title", "") if isinstance(publisher, dict) else str(publisher)

                events.append(
                    CandidateEvent(
                        trade_date=pub_dt,
                        event_type=EventType.NEWS_ANNOUNCEMENT,
                        label=title,
                        description=desc,
                        metadata={"source": source, "url": a.get("url") or ""},
                    )
                )
            return events
        except Exception as e:
            log.warning("Failed to fetch symbol news for %s: %s", symbol, e)
            return []
