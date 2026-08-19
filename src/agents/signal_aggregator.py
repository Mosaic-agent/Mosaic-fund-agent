"""
Signal Aggregator Agent — combines macro, news, valuation, flows, ML,
and anomaly signals into a unified per-ETF composite score.

Usage:
    from src.agents.signal_aggregator import run_signal_aggregation
    result = run_signal_aggregation(save=True, verbose=True)
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import date, datetime

log = logging.getLogger(__name__)

# SIGNAL_ETFS is now owned by signal_sources to avoid circular imports.
from src.agents.signal_sources import SIGNAL_ETFS  # noqa: E402

# ── Weights for each pillar (Baseline) ────────────────────────────────────────

WEIGHTS = {
    "macro":     0.20,
    "sentiment": 0.15,
    "valuation": 0.25,
    "flow":      0.10,
    "ml":        0.20,
    "anomaly":   0.10,
}

# ── Dynamic Regime-Adaptive Weights (static lookup, hand-tuned) ──────────────
# In dislocation/breakout regimes, static mean-reverting valuation and short-term
# ML models decay, while Macro and Anomaly/Precedent signals carry higher SNR.
# In persistent trend regimes, ML directional momentum and institutional Flows lead.
# NOTE: these weight sets are fixed constants, not Bayesian-updated — there is no
# prior/posterior or calibration procedure here, just a per-regime lookup table.
REGIME_WEIGHTS: dict[str, dict[str, float]] = {
    "Volatile Breakout": {
        "macro": 0.28, "anomaly": 0.18, "sentiment": 0.14, "valuation": 0.16, "ml": 0.14, "flow": 0.10,
    },
    "Panic": {
        "macro": 0.30, "anomaly": 0.20, "sentiment": 0.15, "valuation": 0.15, "ml": 0.10, "flow": 0.10,
    },
    "Flash Crash (Contrarian BUY)": {
        "macro": 0.25, "anomaly": 0.25, "valuation": 0.20, "sentiment": 0.10, "ml": 0.10, "flow": 0.10,
    },
    "🔀 Regime Shift (Change Point)": {
        "macro": 0.25, "anomaly": 0.20, "sentiment": 0.15, "valuation": 0.15, "ml": 0.15, "flow": 0.10,
    },
    "Strong Trend (HODL)": {
        "ml": 0.25, "macro": 0.20, "flow": 0.15, "sentiment": 0.15, "valuation": 0.15, "anomaly": 0.10,
    },
    "Normal": {
        "valuation": 0.25, "ml": 0.20, "macro": 0.20, "sentiment": 0.15, "flow": 0.10, "anomaly": 0.10,
    },
}


def _get_regime_weights(flag: str) -> dict[str, float]:
    """Look up the fixed pillar weights for the active market regime label."""
    if flag in REGIME_WEIGHTS:
        return REGIME_WEIGHTS[flag]
    flag_lower = flag.lower()
    for key, weights_dict in REGIME_WEIGHTS.items():
        if key.lower() in flag_lower:
            return weights_dict
    return WEIGHTS


# ── Anomaly regime → numeric score ───────────────────────────────────────────
# Regime labels from the GARCH+IF+PELT anomaly pipeline are mapped to 0-100
# scores that participate in the weighted composite.  "Strong Trend (HODL)"
# maps to 70 (moderately bullish) rather than neutral 50 — the *Risk Governor*
# handles sizing (1.0× multiplier), but the composite should acknowledge that
# a trending regime is mildly positive for the held asset.
ANOMALY_REGIME_SCORES: dict[str, float] = {
    "Strong Trend (HODL)":           70.0,
    "Normal":                        50.0,
    "Volatile Breakout":             40.0,
    "🔀 Regime Shift (Change Point)": 35.0,
    "Flash Crash (Contrarian BUY)":  25.0,
    "Blow-off Top":                  30.0,
    "Panic":                         20.0,
    "🏢 Price Driven by Company Event": 50.0,   # mechanical — neutral
}


@dataclass
class ETFSignal:
    """Composite signal for a single ETF."""
    etf: str
    macro_score: float = 50.0
    sentiment_score: float = 50.0
    valuation_score: float = 50.0
    flow_score: float = 50.0
    ml_score: float = 50.0
    anomaly_flag: str = "Normal"
    anomaly_score: float = 50.0       # numeric regime score (0–100)
    composite_score: float = 50.0
    action: str = "HOLD"
    rationale: str = ""               # human-readable pillar breakdown


@dataclass
class SignalReport:
    """Output of the signal aggregator."""
    as_of: date
    signals: list[ETFSignal] = field(default_factory=list)
    regime: str = "NEUTRAL"


# ── Composite scoring ─────────────────────────────────────────────────────────

def _anomaly_to_score(flag: str) -> float:
    """Convert an anomaly regime label to a 0–100 numeric score."""
    # Exact match first, then substring fallback
    if flag in ANOMALY_REGIME_SCORES:
        return ANOMALY_REGIME_SCORES[flag]
    flag_lower = flag.lower()
    for key, score in ANOMALY_REGIME_SCORES.items():
        if key.lower() in flag_lower:
            return score
    return 50.0  # unknown → neutral


def _build_breakdown(scores: dict[str, float], active_weights: dict[str, float] | None = None) -> str:
    """
    Build a human-readable, auditable per-pillar breakdown.

    Each line shows the pillar, its effective weight, its raw 0–100 score,
    and its *weighted contribution* (score × weight).  The sum of contributions
    equals the composite score.
    """
    w_map = active_weights if active_weights is not None else WEIGHTS
    parts = []
    for label, key in [
        ("Macro", "macro"), ("Sent.", "sentiment"), ("Val.", "valuation"),
        ("Flow", "flow"), ("ML", "ml"), ("Anom.", "anomaly"),
    ]:
        raw = scores[key]
        w = w_map[key]
        parts.append(f"{label}={raw:.0f} ×{w:.2f}={raw * w:+.1f}")

    # Split into two lines of 3 for readability
    return " | ".join(parts[:3]) + "\n" + " | ".join(parts[3:])


def _compute_composite(
    macro: dict, sentiment: dict, valuation: dict,
    flow: dict, ml: dict, anomaly: dict,
) -> list[ETFSignal]:
    """Compute weighted composite score and action for each ETF.

    All six pillars (including anomaly) are weighted dynamically based
    on the underlying regime. Weights sum to 1.00.
    """
    signals = []
    for etf in SIGNAL_ETFS:
        m       = macro.get(etf, 50)
        s       = sentiment.get(etf, 50)
        v       = valuation.get(etf, 50)
        f       = flow.get(etf, 50)
        ml_s    = ml.get(etf, 50)
        a_flag  = anomaly.get(etf, "Normal")
        a_score = _anomaly_to_score(a_flag)

        # Dynamic regime weights
        eff_weights = _get_regime_weights(a_flag)

        # Fully weighted composite — all 6 pillars, weights sum to 1.00
        composite = (
            m       * eff_weights["macro"]
            + s     * eff_weights["sentiment"]
            + v     * eff_weights["valuation"]
            + f     * eff_weights["flow"]
            + ml_s  * eff_weights["ml"]
            + a_score * eff_weights["anomaly"]
        )

        composite = round(composite, 1)

        # Action thresholds
        if composite >= 75:
            action = "BUY"
        elif composite >= 60:
            action = "ACCUMULATE"
        elif composite >= 40:
            action = "HOLD"
        elif composite >= 25:
            action = "TRIM"
        else:
            action = "AVOID"

        # Build auditable breakdown string
        pillar_scores = {
            "macro": m, "sentiment": s, "valuation": v,
            "flow": f, "ml": ml_s, "anomaly": a_score,
        }
        breakdown = _build_breakdown(pillar_scores, active_weights=eff_weights)

        signals.append(ETFSignal(
            etf=etf,
            macro_score=m,
            sentiment_score=s,
            valuation_score=v,
            flow_score=f,
            ml_score=ml_s,
            anomaly_flag=a_flag,
            anomaly_score=a_score,
            composite_score=composite,
            action=action,
            rationale=breakdown,
        ))

    signals.sort(key=lambda s: s.composite_score, reverse=True)
    return signals


# ── Main entry point ──────────────────────────────────────────────────────────

def run_signal_aggregation(
    save: bool = False,
    verbose: bool = False,
    force: bool = False,
) -> SignalReport:
    """
    Run all signal collectors, compute composite, and optionally save to DB.

    Returns a SignalReport with per-ETF composite scores.
    """
    import time
    start_time_sec = time.time()
    manifest_details = {}
    try:
        from src.pipeline.manifest import ManifestTracker, SIGNAL_COMPOSITE, StageStatus
        tracker = ManifestTracker()
        status, manifest_details = tracker.check(SIGNAL_COMPOSITE)
        if status == StageStatus.FRESH and not force:
            log.info("ManifestTracker: signal_composite FRESH — skipping recomputation")
            from src.db.pool import get_pool
            from src.db.repository import MarketDataRepository
            repo = MarketDataRepository(get_pool())
            latest_rows = repo.latest_signal_composite(SIGNAL_ETFS)
            if latest_rows:
                signals = [
                    ETFSignal(
                        etf=r["etf_symbol"],
                        macro_score=r.get("macro_score", 50.0),
                        sentiment_score=r.get("sentiment_score", 50.0),
                        valuation_score=r.get("valuation_score", 50.0),
                        flow_score=r.get("flow_score", 50.0),
                        ml_score=r.get("ml_score", 50.0),
                        anomaly_flag=r.get("anomaly_flag", "Normal"),
                        composite_score=float(r["composite_score"]),
                        action=r["action"],
                        rationale=r.get("rationale", ""),
                    )
                    for r in latest_rows
                ]
                return SignalReport(as_of=date.today(), signals=signals, regime="MIXED")
    except Exception as _m_err:
        log.warning("Manifest check failed for signal_composite: %s", _m_err)

    log.info("Starting signal aggregation for %d ETFs...", len(SIGNAL_ETFS))

    from src.db.pool import get_pool
    from src.db.repository import MarketDataRepository
    from src.agents.signal_sources import (
        MacroSignalSource, SentimentSignalSource, ValuationSignalSource,
        FlowSignalSource, MLSignalSource, GARCHAnomalySource,
    )

    repo = MarketDataRepository(get_pool())

    # Registered signal sources — add/remove here to change what's scored.
    # Order doesn't matter; they run in parallel.
    score_sources = [
        MacroSignalSource(),
        SentimentSignalSource(),
        ValuationSignalSource(),
        FlowSignalSource(),
        MLSignalSource(),
    ]
    anomaly_source = GARCHAnomalySource()

    # Run all sources in parallel — each is fully independent
    with ThreadPoolExecutor(max_workers=len(score_sources) + 1) as pool:
        score_futures  = {pool.submit(s.collect, repo): s.name for s in score_sources}
        anomaly_future = pool.submit(anomaly_source.collect, repo)

        raw_scores: dict[str, dict[str, float]] = {}
        for future in as_completed(score_futures):
            raw_scores[score_futures[future]] = future.result()

        anomaly = anomaly_future.result()

    macro     = raw_scores["macro"]
    sentiment = raw_scores["sentiment"]
    valuation = raw_scores["valuation"]
    flow      = raw_scores["flow"]
    ml        = raw_scores["ml"]

    # Compute composite
    signals = _compute_composite(macro, sentiment, valuation, flow, ml, anomaly)

    today = date.today()
    report = SignalReport(as_of=today, signals=signals)

    # Determine overall regime from top/bottom signals
    top_actions = [s.action for s in signals[:5]]
    if top_actions.count("BUY") >= 3:
        report.regime = "RISK_ON"
    elif top_actions.count("AVOID") + top_actions.count("TRIM") >= 3:
        report.regime = "RISK_OFF"
    else:
        report.regime = "MIXED"

    # Save to DB
    if save:
        try:
            from src.importer.clickhouse import ClickHouseImporter
            ch = ClickHouseImporter()
            ch.ensure_schema()
            rows = [
                {
                    "as_of": today,
                    "etf_symbol": s.etf,
                    "macro_score": s.macro_score,
                    "sentiment_score": s.sentiment_score,
                    "valuation_score": s.valuation_score,
                    "flow_score": s.flow_score,
                    "ml_score": s.ml_score,
                    "anomaly_flag": s.anomaly_flag,
                    "composite_score": s.composite_score,
                    "action": s.action,
                    "rationale": s.rationale,
                }
                for s in signals
            ]
            n = ch.insert_signal_composite(rows)
            ch.close()
            log.info("Saved %d signal composite rows to ClickHouse", n)

            from src.pipeline.manifest import ManifestTracker, SIGNAL_COMPOSITE
            tracker = ManifestTracker()
            duration_ms = int((time.time() - start_time_sec) * 1000)
            tracker.record(SIGNAL_COMPOSITE, manifest_details, duration_ms=duration_ms)
        except Exception as e:
            log.warning("Failed to save signal composite / record manifest: %s", e)

    log.info("Signal aggregation complete: regime=%s, %d ETFs scored", report.regime, len(signals))
    return report


def print_signal_report(report: SignalReport) -> None:
    """Print the signal report to terminal using Rich."""
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel

    console = Console()

    # Header
    regime_color = {"RISK_ON": "green", "RISK_OFF": "red", "MIXED": "yellow"}.get(report.regime, "white")
    console.print(Panel(
        f"[bold]Signal Aggregator[/bold]\n"
        f"[dim]As of {report.as_of} · {len(report.signals)} ETFs · "
        f"Regime: [{regime_color}]{report.regime}[/{regime_color}][/dim]",
        border_style="cyan",
    ))

    # Table
    table = Table(title="ETF Signal Composite", show_lines=True)
    table.add_column("ETF", style="bold", width=14)
    table.add_column("Macro", justify="right", width=7)
    table.add_column("Sent.", justify="right", width=7)
    table.add_column("Val.", justify="right", width=7)
    table.add_column("Flow", justify="right", width=7)
    table.add_column("ML", justify="right", width=7)
    table.add_column("Anom.", justify="right", width=7)
    table.add_column("Regime", width=16)
    table.add_column("Score", justify="right", style="bold", width=7)
    table.add_column("Action", width=12)

    ACTION_STYLE = {
        "BUY": "[bold green]BUY[/bold green]",
        "ACCUMULATE": "[green]ACCUMULATE[/green]",
        "HOLD": "[yellow]HOLD[/yellow]",
        "TRIM": "[red]TRIM[/red]",
        "AVOID": "[bold red]AVOID[/bold red]",
    }

    def _score_color(v: float) -> str:
        if v >= 70: return f"[green]{v:.0f}[/green]"
        if v >= 55: return f"[bright_green]{v:.0f}[/bright_green]"
        if v >= 45: return f"[yellow]{v:.0f}[/yellow]"
        if v >= 30: return f"[red]{v:.0f}[/red]"
        return f"[bold red]{v:.0f}[/bold red]"

    for s in report.signals:
        table.add_row(
            s.etf,
            _score_color(s.macro_score),
            _score_color(s.sentiment_score),
            _score_color(s.valuation_score),
            _score_color(s.flow_score),
            _score_color(s.ml_score),
            _score_color(s.anomaly_score),
            s.anomaly_flag,
            _score_color(s.composite_score),
            ACTION_STYLE.get(s.action, s.action),
        )

    console.print(table)

    # ── Score Breakdown panel (verbose detail for top 5 ETFs) ─────────────
    if report.signals:
        breakdown_lines = []
        for s in report.signals[:5]:
            breakdown_lines.append(
                f"[bold]{s.etf}[/bold] = {s.composite_score:.0f}\n"
                f"  {s.rationale}"
            )
        console.print(Panel(
            "\n\n".join(breakdown_lines),
            title="Score Breakdown (Top 5)",
            border_style="dim cyan",
        ))

    # Top picks
    buys = [s for s in report.signals if s.action in ("BUY", "ACCUMULATE")]
    if buys:
        console.print(Panel(
            "\n".join(f"  [green]▲[/green] {s.etf}: {s.composite_score:.0f}/100 → {s.action}" for s in buys[:5]),
            title="Top Picks", border_style="green",
        ))

    avoids = [s for s in report.signals if s.action in ("TRIM", "AVOID")]
    if avoids:
        console.print(Panel(
            "\n".join(f"  [red]▼[/red] {s.etf}: {s.composite_score:.0f}/100 → {s.action}" for s in avoids[:5]),
            title="Avoid / Trim", border_style="red",
        ))
