"""Signal sub-agent: ETF signals, GOLDBEES ML pipeline, risk governor."""
from __future__ import annotations

import logging
from typing import Any

from .base import _SubAgent

logger = logging.getLogger(__name__)

class SignalSubAgent(_SubAgent):
    """
    ETF signal pipeline: composite scores, GOLDBEES ML, Kelly weights, risk governor.
    """

    SYSTEM_PROMPT = (
        "You are a quantitative signal analyst for Indian ETF markets (NSE). "
        "Use `run_daily_signal_composite` to compute unified 0-100 composite scores "
        "for all tracked ETFs across macro, news, NAV Z-score, FII/DII, and ML pillars. "
        "Use `run_goldbees_pipeline` for the GOLDBEES ML prediction: report "
        "prob_up, expected_return_pct, regime_signal, and weights.blended_50 verbatim. "
        "Use `run_risk_governor_analysis` for GARCH volatility-targeted position sizing. "
        "Use `run_etf_news_sentiment` for ETF category news sentiment. "
        "Use `explain_price_anomalies` to scan price history for return outliers (magnitude >= 2%) and query news on those dates to find their causes. Whenever you call this tool to explain anomalies, you MUST also call `plot_price_chart` in parallel to visually display the price trend.\n"
        "Use `search_anomaly_events(symbol)` for equity/stock anomaly investigation — it suppresses corporate actions and runs parallel Google News searches per flagged date.\n"
        "Use `find_similar_anomaly_events(symbol, regime)` to retrieve historical anomaly events similar to a current regime from Qdrant `market_anomalies` — answer 'what historical crashes looked like this?' or 'find past GOLDBEES flash crashes'.\n"
        "PDF EXPORT: Only call `publish_consolidated_pdf(report_markdown=<full_output>)` when the user explicitly asks to save, export, or publish as PDF.\n"
        "Use `get_shoonya_quotes` or `get_shoonya_live_tick` when the user asks for live prices or ticks via Shoonya. "
        "CRITICAL: Never invent composite scores or labels like ACCUMULATE/STRONG BUY. "
        "Use regime_signal and blended_50 exactly as the pipeline outputs them. "
        "Format all signal tables in clean Markdown.\n\n"
        "## iNAV / Premium freshness\n"
        "iNAV data is automatically kept current. During market hours (IST 09:15–15:30) "
        "the system fetches a live NSE snapshot if the DB copy is older than 10 minutes. "
        "Tool output includes an `inav_source` field: 'db' = cached, 'nse_api_live' = just "
        "fetched. Always report which source was used and the snapshot timestamp.\n\n"
        "## Charts\n"
        "Call chart tools when the user asks to visualise signals or weights:\n"
        "- `plot_signal_scores()` — overall composite scores for all ETFs\n"
        "- `plot_signal_breakdown('SYM1,SYM2')` — weighted pillar breakdown (macro/sentiment/valuation/flow/ML)\n"
        "- `plot_weight_recommendations('blended_50')` — recommended position weights\n"
        "- `plot_garch_volatility_chart(symbol)` — GARCH vol trend vs vol-target line\n"
        "- `plot_price_chart(symbol)` — price trend for a specific ETF\n"
        "- `plot_macd_chart(symbol, days)` — MACD(12,26,9) with EMA overlay and histogram"
    )

    def _get_tools(self) -> list:
        from src.tools.skills_tools import (
            run_daily_signal_composite,
            run_goldbees_pipeline,
            run_etf_news_sentiment,
            run_risk_governor_analysis,
            run_premium_alerts,
            get_live_inav,
            query_clickhouse_db,
            explain_price_anomalies,
        )
        from src.tools.chart_tools import (
            plot_price_chart, plot_signal_scores, plot_multi_price_chart,
            plot_signal_breakdown, plot_weight_recommendations,
            plot_garch_volatility_chart, plot_macd_chart,
        )
        from src.tools.shoonya_tools import get_shoonya_quotes, get_shoonya_live_tick
        from src.tools.market.equity import search_anomaly_events, find_similar_anomaly_events
        from src.tools.market.correlation_tools import find_anomaly_correlations
        from src.tools.report_publisher import publish_research_pdf, publish_consolidated_pdf
        return [
            run_daily_signal_composite,
            run_goldbees_pipeline,
            run_etf_news_sentiment,
            run_risk_governor_analysis,
            run_premium_alerts,
            get_live_inav,
            query_clickhouse_db,
            explain_price_anomalies,
            search_anomaly_events,
            find_similar_anomaly_events,
            find_anomaly_correlations,
            plot_price_chart,
            plot_signal_scores,
            plot_signal_breakdown,
            plot_weight_recommendations,
            plot_garch_volatility_chart,
            plot_multi_price_chart,
            plot_macd_chart,
            get_shoonya_quotes,
            get_shoonya_live_tick,
            publish_research_pdf,
            publish_consolidated_pdf,
        ]

    def _fallback(self, question: str) -> str:
        """
        Programmatic fallback for local models that cannot emit tool-call JSON.

        Routes by keyword detection:
          anomaly/spike/drop/crash  → explain_price_anomalies + plot_price_chart
          signal/pipeline/goldbees  → run_goldbees_pipeline
          composite/scores/etf      → run_daily_signal_composite
        """
        import re as _re
        q = question.lower()

        # ── Anomaly explanation path ──────────────────────────────────────────
        if any(kw in q for kw in ("anomal", "spike", "crash", "drop", "outlier", "shock")):
            # Extract symbol — default GOLDBEES for gold ETF queries
            symbol = "GOLDBEES"
            m = _re.search(r"\b([A-Z]{4,12}(?:BEES|ETF|GOLD|SILVER)?)\b", question.upper())
            if m and m.group(1) not in ("OVER", "LAST", "DAYS", "SHOW", "FIND", "EXPLAIN", "ANALYSE", "ANALYZE"):
                symbol = m.group(1)

            # Extract time window — supports "30 days", "3 months", "1 year"
            days = 30
            dm = _re.search(r"(\d+)\s*(year|month|week|day|d\b)s?", q)
            if dm:
                n, unit = int(dm.group(1)), dm.group(2)
                if unit.startswith("year"):
                    days = n * 365
                elif unit.startswith("month"):
                    days = n * 30
                elif unit.startswith("week"):
                    days = n * 7
                else:
                    days = n
                days = min(days, 730)  # cap at 2 years

            logger.info("SignalSubAgent._fallback: anomaly path — %s %d days", symbol, days)

            from src.tools.market.gold import explain_price_anomalies
            from src.tools.chart_tools import plot_price_chart

            price_chart = plot_price_chart.invoke({"symbol": symbol, "days": days})
            anomaly_report = explain_price_anomalies.invoke({"symbol": symbol, "days": days})

            parts = [f"## {symbol} — Price Chart ({days}d)\n```text\n{price_chart}\n```\n", anomaly_report]

            # Optional LLM synthesis
            if self._llm is not None:
                try:
                    from langchain_core.messages import SystemMessage, HumanMessage
                    synthesis = self._llm.invoke([
                        SystemMessage(content=(
                            "You are a quant analyst. The tool output below contains a price anomaly report "
                            "with GARCH regime labels, Final Z scores, news correlation, and ML forward context. "
                            "Summarise the key anomalies, their regimes, and what the signal/model implied. "
                            "Do NOT invent any numbers — only narrate what is in the report."
                        )),
                        HumanMessage(content=anomaly_report[:4000]),
                    ])
                    parts.append("\n---\n### Summary\n" + synthesis.content)
                except Exception as _e:
                    logger.warning("SignalSubAgent._fallback LLM synthesis failed: %s", _e)

            return "\n\n".join(parts)

        # ── GOLDBEES pipeline path ────────────────────────────────────────────
        if any(kw in q for kw in ("signal", "pipeline", "goldbees", "recommendation", "buy", "sell", "weight")):
            logger.info("SignalSubAgent._fallback: goldbees pipeline path")
            from src.tools.skills_tools import run_goldbees_pipeline
            return run_goldbees_pipeline.invoke({})

        # ── Composite scores path ─────────────────────────────────────────────
        if any(kw in q for kw in ("composite", "score", "etf", "signal composite")):
            logger.info("SignalSubAgent._fallback: composite scores path")
            from src.tools.skills_tools import run_daily_signal_composite
            return run_daily_signal_composite.invoke({"save": False})

        return (
            "Your configured LLM does not support tool-calling for this query. "
            "Try: 'explain GOLDBEES anomalies', 'run goldbees pipeline', or 'composite ETF scores'. "
            "For full capability, set LLM_PROVIDER=openai or LLM_PROVIDER=anthropic in .env."
        )
