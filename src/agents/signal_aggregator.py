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

# ── Weights for each pillar ───────────────────────────────────────────────────

WEIGHTS = {
    "macro":     0.25,
    "sentiment": 0.15,
    "valuation": 0.15,
    "flow":      0.15,
    "ml":        0.15,
    "anomaly":   0.05,
}
# Remaining 0.10 is distributed to flow (FII/DII component)


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
    composite_score: float = 50.0
    action: str = "HOLD"
    rationale: str = ""


@dataclass
class SignalReport:
    """Output of the signal aggregator."""
    as_of: date
    signals: list[ETFSignal] = field(default_factory=list)
    regime: str = "NEUTRAL"


# ── Composite scoring ─────────────────────────────────────────────────────────

def _compute_composite(
    macro: dict, sentiment: dict, valuation: dict,
    flow: dict, ml: dict, anomaly: dict,
) -> list[ETFSignal]:
    """Compute weighted composite score and action for each ETF."""
    signals = []
    for etf in SIGNAL_ETFS:
        m = macro.get(etf, 50)
        s = sentiment.get(etf, 50)
        v = valuation.get(etf, 50)
        f = flow.get(etf, 50)
        ml_s = ml.get(etf, 50)
        a_flag = anomaly.get(etf, "Normal")

        # Weighted composite
        composite = (
            m * WEIGHTS["macro"]
            + s * WEIGHTS["sentiment"]
            + v * WEIGHTS["valuation"]
            + f * (WEIGHTS["flow"] + 0.10)  # flow gets extra 10% from the remaining
            + ml_s * WEIGHTS["ml"]
        )

        # Anomaly override: boost contrarian if Flash Crash
        if "Flash Crash" in a_flag and composite < 40:
            composite = min(composite + 15, 60)
        # Blow-off top: dampen bullish signal
        elif "Blow-off" in a_flag and composite > 60:
            composite = max(composite - 10, 55)
        # Note: "Strong Trend (HODL)" is intentionally handled neutrally (no score modification)
        # to prevent momentum-chasing (buying high). Position-sizing adjustments (Risk Governor)
        # are used instead to maintain full exposure (1.00x multiplier) without deploying new cash.

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

        signals.append(ETFSignal(
            etf=etf,
            macro_score=m,
            sentiment_score=s,
            valuation_score=v,
            flow_score=f,
            ml_score=ml_s,
            anomaly_flag=a_flag,
            composite_score=composite,
            action=action,
        ))

    signals.sort(key=lambda s: s.composite_score, reverse=True)
    return signals


# ── Main entry point ──────────────────────────────────────────────────────────

def run_signal_aggregation(
    save: bool = False,
    verbose: bool = False,
) -> SignalReport:
    """
    Run all signal collectors, compute composite, and optionally save to DB.

    Returns a SignalReport with per-ETF composite scores.
    """
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
        except Exception as e:
            log.warning("Failed to save signal composite: %s", e)

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
    table.add_column("Anomaly", width=12)
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
            s.anomaly_flag,
            _score_color(s.composite_score),
            ACTION_STYLE.get(s.action, s.action),
        )

    console.print(table)

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
