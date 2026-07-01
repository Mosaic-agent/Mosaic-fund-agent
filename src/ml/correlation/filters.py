"""
src/ml/correlation/filters.py
──────────────────────────────
Post-processing pipeline for CorrelationFindings:
  1. Quality & source hierarchy weighting
  2. Precedent weighting from past attributed anomalies (market_anomalies Qdrant)
  3. Date-based deduplication with secondary trigger merging
  4. Episode clustering (consecutive anomalies → single representative)

Each stage is a standalone function composable into a pipeline via
`FindingsPipeline.run()`.
"""

from __future__ import annotations

import logging
from typing import Any, List

import pandas as pd

from .models import CorrelationFinding, EventType

log = logging.getLogger(__name__)

# Macro events whose semantic relevance to the symbol falls below this
# threshold are dropped entirely rather than down-weighted. The previous
# floor of 0.30 let irrelevant macro themes (e.g. "India Gold Import Duty"
# → a specialty-films stock) leak into findings with MODERATE confidence.
_MIN_MACRO_RELEVANCE = 0.42

# Sentinel value: when h_weight equals this, the relevance is a *fallback*
# (e.g. Ollama unreachable) rather than a measured similarity. Fallbacks
# bypass the cutoff so non-embedding environments keep working.
_RELEVANCE_FALLBACK = 0.5


# ── Individual Filter Stages ──────────────────────────────────────────────────


def apply_quality_weights(
    findings: List[CorrelationFinding],
    min_score: float = 15.0,
    symbol: str = "",
) -> List[CorrelationFinding]:
    """Apply source hierarchy and news quality weights to adjust scores.

    - Company filings/news with sector keywords get h_weight=0.8
    - Macro events get semantic relevance weight (cosine similarity between
      event description and symbol context), ranging [0.3, 1.0]
    - NEWS_ANNOUNCEMENT gets semantic quality weight via RAG exemplar scoring
    - Hard blocklist zeroes out known-bad publishers
    """
    adjusted: List[CorrelationFinding] = []

    for f in findings:
        # 1. Source Hierarchy Weight
        h_weight = 1.0
        et = f.event.event_type
        if et in (EventType.COMPANY_FILING, EventType.NEWS_ANNOUNCEMENT):
            text = (f.event.label + " " + f.event.description).lower()
            clean_text = "".join(c if c.isalnum() or c.isspace() else " " for c in text)
            words = set(clean_text.split())
            sector_keywords = {"sector", "industry", "auto", "it", "banking", "pharma", "metal", "oil", "commodity"}
            if words & sector_keywords:
                h_weight = 0.8
        elif et in (EventType.MACRO_RATE_DECISION, EventType.MACRO_GEOPOLITICAL, EventType.MACRO_COMMODITY_SHOCK):
            # Check for commodity-specific keyword mismatch (e.g. Gold event on Reliance)
            if symbol:
                try:
                    from src.utils.symbol_mapper import get_company_name
                    company = get_company_name(symbol)
                except Exception:
                    company = ""
                
                event_text_lower = f"{f.event.label} {f.event.description}".lower()
                symbol_context_lower = f"{symbol} {company}".lower()
                commodity_keywords = {"gold", "silver", "bullion", "platinum", "palladium"}
                has_commodity_kw = any(cw in event_text_lower for cw in commodity_keywords)
                symbol_has_commodity = any(cw in symbol_context_lower for cw in commodity_keywords)
                
                if has_commodity_kw and not symbol_has_commodity:
                    log.debug(
                        "Dropping commodity-specific macro event '%s' for non-commodity symbol %s",
                        f.event.label, symbol
                    )
                    continue

            # Semantic relevance: how directly does this event relate to the symbol?
            # "Gold import duty" → GOLDBEES → high similarity → h_weight ~0.85
            # "Fed rate cut"     → GOLDBEES → low similarity  → h_weight ~0.40
            if symbol:
                try:
                    from .news_rag import score_event_relevance
                    event_text = f"{f.event.label} {f.event.description}"
                    h_weight = score_event_relevance(event_text, symbol)
                except Exception:
                    h_weight = _RELEVANCE_FALLBACK
            else:
                h_weight = _RELEVANCE_FALLBACK

            # Drop events whose measured relevance is below the cutoff.
            # The fallback value (Ollama unreachable) is exempt so non-
            # embedding environments don't silently filter everything.
            if (
                h_weight < _MIN_MACRO_RELEVANCE
                and abs(h_weight - _RELEVANCE_FALLBACK) > 1e-6
            ):
                log.debug(
                    "Dropping low-relevance macro event '%s' for %s (h=%.3f < %.2f)",
                    f.event.label, symbol, h_weight, _MIN_MACRO_RELEVANCE,
                )
                continue

        # 2. News Quality Weight (NEWS_ANNOUNCEMENT only)
        nq_weight = 1.0
        if et == EventType.NEWS_ANNOUNCEMENT:
            # Check for commodity-specific keyword mismatch (e.g. Gold news on Reliance)
            if symbol:
                try:
                    from src.utils.symbol_mapper import get_company_name
                    company = get_company_name(symbol)
                except Exception:
                    company = ""
                
                event_text_lower = f"{f.event.label} {f.event.description}".lower()
                symbol_context_lower = f"{symbol} {company}".lower()
                commodity_keywords = {"gold", "silver", "bullion", "platinum", "palladium"}
                has_commodity_kw = any(cw in event_text_lower for cw in commodity_keywords)
                symbol_has_commodity = any(cw in symbol_context_lower for cw in commodity_keywords)
                
                if has_commodity_kw and not symbol_has_commodity:
                    log.debug(
                        "Dropping commodity-specific news '%s' for non-commodity symbol %s",
                        f.event.label, symbol
                    )
                    continue

            text = (f.event.label + " " + f.event.description).lower()
            source = str(f.event.metadata.get("source", "")).lower()
            url = str(f.event.metadata.get("url", "")).lower()

            # Hard blocklist
            if any(k in text or k in source or k in url for k in ["simplywall.st", "simplywall", "blog", "opinion article"]):
                nq_weight = 0.0
            elif any(k in text for k in ["could get bumped", "should weakness", "opinion", "why simply", "foolish", "target by 20", "target by 2"]):
                nq_weight = 0.10
            else:
                # Semantic scoring via exemplar embeddings
                try:
                    from .news_rag import score_news_quality
                    nq_weight = score_news_quality(f.event.label)
                except Exception:
                    nq_weight = 0.20

        # Compute final adjusted score
        f.correlation_score = f.correlation_score * nq_weight * h_weight

        if f.correlation_score < min_score:
            continue

        # Update confidence band
        if f.correlation_score >= 70.0:
            f.confidence = "HIGH"
        elif f.correlation_score >= 40.0:
            f.confidence = "MODERATE"
        else:
            f.confidence = "LOW"

        adjusted.append(f)

    return adjusted


# Bounded score adjustment from precedent — a soft corroboration signal, not
# a hard override. Capped low so a handful of noisy/sparse precedents can't
# swing a finding across a confidence band on their own.
_PRECEDENT_ADJ_PER_VOTE = 5.0
_PRECEDENT_ADJ_CAP = 10.0
_PRECEDENT_K = 5


def apply_precedent_weight(
    findings: List[CorrelationFinding],
    symbol: str = "",
    df_anomaly: Any = None,
    **_: Any,
) -> List[CorrelationFinding]:
    """Adjust each finding's score using precedent from past attributed anomalies.

    For each finding, retrieves up to _PRECEDENT_K statistically similar past
    anomalies (via the market_anomalies Qdrant collection) and checks what they
    were attributed to:
      - similar anomalies attributed to the SAME event type  → corroborates
      - similar anomalies that went UNEXPLAINED                → contradicts
      - similar anomalies never checked (no attribution yet)  → ignored (cold start)

    No-ops entirely until enough history has accumulated — early findings are
    scored exactly as before; the corroboration signal appears as attributed
    history builds up in market_anomalies.
    """
    if not findings or df_anomaly is None or df_anomaly.empty or "regime" not in df_anomaly.columns:
        return findings
    if not symbol:
        return findings

    try:
        from src.db.anomaly_vector import retrieve_similar_anomalies
    except Exception:
        return findings

    regime_by_date = dict(zip(
        pd.to_datetime(df_anomaly["trade_date"]).dt.date,
        df_anomaly["regime"],
    ))

    adjusted: List[CorrelationFinding] = []
    for f in findings:
        regime = regime_by_date.get(f.anomaly_date, "")
        if not regime:
            adjusted.append(f)
            continue

        try:
            precedents = retrieve_similar_anomalies(
                symbol=symbol, regime=regime, trade_date=f.anomaly_date, k=_PRECEDENT_K,
            )
        except Exception as exc:
            log.debug("Precedent lookup failed for %s/%s: %s", symbol, f.anomaly_date, exc)
            precedents = []

        checked = [p for p in precedents if p.get("attributed_confidence")]
        if not checked:
            # Cold start — no attributed history to compare against yet.
            adjusted.append(f)
            continue

        corroborating = sum(
            1 for p in checked
            if p.get("attributed_event_type") == f.event.event_type.value
            and p.get("attributed_confidence") in ("HIGH", "MODERATE")
        )
        contradicting = sum(1 for p in checked if p.get("attributed_confidence") == "UNEXPLAINED")

        net_votes = corroborating - contradicting
        if net_votes != 0:
            adj = max(-_PRECEDENT_ADJ_CAP, min(_PRECEDENT_ADJ_CAP, net_votes * _PRECEDENT_ADJ_PER_VOTE))
            f.correlation_score = max(0.0, min(100.0, f.correlation_score + adj))
            if f.correlation_score >= 70.0:
                f.confidence = "HIGH"
            elif f.correlation_score >= 40.0:
                f.confidence = "MODERATE"
            else:
                f.confidence = "LOW"
            if net_votes > 0:
                f.explanation += (
                    f" Precedent: {corroborating}/{len(checked)} similar past anomalies "
                    f"were also attributed to '{f.event.event_type.value}' ({adj:+.0f})."
                )
            else:
                f.explanation += (
                    f" ⚠️ Precedent: {contradicting}/{len(checked)} similar past anomalies "
                    f"went unexplained — this match may be coincidental ({adj:+.0f})."
                )

        adjusted.append(f)

    return adjusted


def deduplicate_by_date(findings: List[CorrelationFinding]) -> List[CorrelationFinding]:
    """Group findings by anomaly date, keeping best score + merging secondaries into explanation."""
    by_date: dict[Any, List[CorrelationFinding]] = {}
    for f in findings:
        by_date.setdefault(f.anomaly_date, []).append(f)

    deduped: List[CorrelationFinding] = []
    for anom_date, date_findings in by_date.items():
        if len(date_findings) == 1:
            deduped.append(date_findings[0])
        else:
            date_findings = sorted(date_findings, key=lambda x: (-x.correlation_score, x.strategy_name))
            primary = date_findings[0]
            secondary_trigs = date_findings[1:]

            extra_explanations = []
            for sec in secondary_trigs:
                offset_str = f"{sec.lead_lag_days:+}d"
                extra_explanations.append(
                    f"{sec.event.label} ({offset_str} offset, score: {sec.correlation_score:.1f} via {sec.strategy_name})"
                )

            new_explanation = primary.explanation + "\n   *Secondary Triggers:* " + "; ".join(extra_explanations)

            merged_finding = CorrelationFinding(
                anomaly_date=primary.anomaly_date,
                event=primary.event,
                strategy_name=primary.strategy_name,
                correlation_score=primary.correlation_score,
                lead_lag_days=primary.lead_lag_days,
                confidence=primary.confidence,
                explanation=new_explanation,
            )
            deduped.append(merged_finding)

    return sorted(deduped, key=lambda f: (f.anomaly_date, -f.correlation_score))


def cluster_episodes(
    findings: List[CorrelationFinding],
    max_gap_days: int = 5,
) -> List[CorrelationFinding]:
    """Group consecutive anomaly dates within `max_gap_days` into single episodes."""
    if not findings:
        return findings

    episodes: List[List[CorrelationFinding]] = []
    current_episode: List[CorrelationFinding] = [findings[0]]
    for f in findings[1:]:
        prev_date = current_episode[-1].anomaly_date
        if (f.anomaly_date - prev_date).days <= max_gap_days:
            current_episode.append(f)
        else:
            episodes.append(current_episode)
            current_episode = [f]
    episodes.append(current_episode)

    clustered: List[CorrelationFinding] = []
    for ep in episodes:
        if len(ep) == 1:
            clustered.append(ep[0])
        else:
            ep_sorted = sorted(ep, key=lambda x: -x.correlation_score)
            primary = ep_sorted[0]
            other_dates = [
                f"{f.anomaly_date} ({f.event.label}, score: {f.correlation_score:.1f})"
                for f in ep_sorted[1:]
            ]
            episode_note = (
                f"\n   *Episode cluster ({len(ep)} days):* "
                + "; ".join(other_dates)
            )
            merged = CorrelationFinding(
                anomaly_date=primary.anomaly_date,
                event=primary.event,
                strategy_name=primary.strategy_name,
                correlation_score=primary.correlation_score,
                lead_lag_days=primary.lead_lag_days,
                confidence=primary.confidence,
                explanation=primary.explanation + episode_note,
                abnormal_return=primary.abnormal_return,
            )
            clustered.append(merged)

    return clustered


# ── Pipeline Orchestrator ─────────────────────────────────────────────────────


class FindingsPipeline:
    """Chains filter stages into an ordered pipeline.

    Default pipeline: quality_weights → deduplicate → cluster_episodes.
    Stages can be customized by passing a list of callables to __init__.
    Keyword arguments passed to run() are forwarded to each stage (stages
    that don't accept them will simply ignore them via **kwargs).
    """

    def __init__(self, stages=None):
        if stages is None:
            stages = [
                apply_quality_weights,
                apply_precedent_weight,
                deduplicate_by_date,
                cluster_episodes,
            ]
        self._stages = stages

    def run(self, findings: List[CorrelationFinding], **kwargs) -> List[CorrelationFinding]:
        """Execute all pipeline stages in order, forwarding kwargs."""
        import inspect

        for stage in self._stages:
            sig = inspect.signature(stage)
            # Only pass kwargs that the stage accepts
            accepted = {k: v for k, v in kwargs.items() if k in sig.parameters}
            findings = stage(findings, **accepted)
        return findings
