"""
src/tools/smallcap_pattern_tool.py
───────────────────────────────────
LangChain @tool wrapper for Small Cap Pattern Analyzer.

Allows MosaicFundAgent and subagents to invoke Small Cap quantitative analysis directly.
"""

import logging
from langchain_core.tools import tool
from src.scripts.portfolio.smallcap_pattern_analyzer import SmallcapPatternAnalyzer

logger = logging.getLogger(__name__)


@tool
def analyze_smallcap_patterns(
    amc: str = "all",
    lookback_months: int = 24,
) -> str:
    """
    Run Multi-AMC Small Cap pattern and institutional shift analyzer.

    Returns EOD ETF price trends, AMFI mutual fund category inflows,
    Small Cap portfolio holdings across AMCs (Nippon, DSP, HDFC, Quant, ICICI, Kotak, Bajaj, or ALL),
    MoM net additions & trims, persistence-ranked multi-AMC cross-conviction
    (names held by multiple AMCs continuously vs. a recent single-month pile-in),
    and composite quant signals.

    Parameters:
      amc: Target AMC group ('all', 'dsp', 'nippon', 'hdfc', 'quant', 'icici', 'kotak', 'bajaj')
      lookback_months: Months of holdings history to score cross-conviction persistence over (default 24)
    """
    try:
        analyzer = SmallcapPatternAnalyzer()
        report = analyzer.analyze(amc=amc, lookback_months=lookback_months)
        return analyzer.render_ascii_dashboard(report)
    except Exception as exc:
        logger.error("Error running analyze_smallcap_patterns: %s", exc)
        return f"Error analyzing Small Cap patterns for AMC '{amc}': {exc}"
