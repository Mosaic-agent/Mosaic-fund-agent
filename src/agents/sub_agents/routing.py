"""Regex routing — maps a question string to a sub-agent name."""
from __future__ import annotations

import logging
import re

logger = logging.getLogger(__name__)

_DEEPDIVE_RE = re.compile(
    r"deep.?dive|10.?k filing|10.?q filing|sec filing|edgar|xbrl|annual report"
    r"|adsk|autodesk|aapl|apple|msft|microsoft|nvda|nvidia|amzn|amazon|googl|meta|tsla",
    re.I,
)
_SIGNAL_RE = re.compile(
    r"\bsignal\b"
    r"|kelly\s+weight|composite\s+score|inav\s+premium|inav\s+discount"
    r"|etf\s+recommendation|buy\s+signal|sell\s+signal|risk\s+governor|blended\s+weight"
    r"|\bgarch\b|volatility\s+chart|vol\s+chart|ml\s+prediction|regime\s+signal"
    r"|goldbees\s+(?:pipeline|ml|prediction|recommendation)"
    r"|run\s+goldbees|run\s+pipeline"
    r"|today.?s\s+(?:gold|etf|composite)\s+signal"
    r"|plot\s+(?:the\s+)?(?:price|chart|data|returns|volatility|garch)"
    r"|price\s+chart|returns\s+chart|garch\s+chart|volatility\s+trend",
    re.I,
)
_MACRO_RE = re.compile(
    r"\bcomex|macro theme|macro scan|\bfii\b|\bdii\b|fii flow|dii flow|institutional flow"
    r"|gold price|silver price|copper price|crude oil|fed rate|rbi rate"
    # Commodity ticker/symbol notations (XAUUSD, GC=F, ...) — an LLM or user
    # may reference gold/silver/copper by ticker rather than the spelled-out word.
    r"|\bxau(?:usd)?\b|\bxag(?:usd)?\b|\bxpt(?:usd)?\b|\bxpd(?:usd)?\b"
    r"|\bgc=f\b|\bsi=f\b|\bhg=f\b|\bpl=f\b|\bpa=f\b"
    r"|usd.?inr|cot report|geopolit|war risk|tariff|trade war"
    r"|\b(?:iran|russia|ukraine|taiwan|israel|gaza|pakistan|opec|china)\b"
    r"|financial\s+news|global\s+(?:news|market)|macro\s+news"
    r"|\bsanctions?\b|\b(?:war|conflict|crisis)\b",
    re.I,
)
_NEWS_RE = re.compile(
    r"\bnews\s+(?:on|for|about|of)\s+\w"
    r"|\blatest\s+news\b"
    r"|\bmarket\s+(?:news|headlines)\b"
    r"|\betf\s+news\b"
    r"|\bearnings\s+news\b"
    r"|\bbreaking\s+news\b"
    r"|\bnews\s+today\b"
    r"|\bheadlines\b"
    r"|\bwhat.?s\s+happening\s+(?:with|in)\b"
    r"|\bnews\s+sentiment\b"
    r"|\bsaved\s+news\b",
    re.I,
)
_INTL_ETF_RE = re.compile(
    r"\b(?:international|intl|global)\s+etf"
    r"|\b(?:mafang|hngsngbees|mon100|masptop50|mahktech|monq50)\b"
    r"|\bhang\s+seng(?:\s+etf)?|nasdaq\s+etf|s&p\s+500\s+etf|china\s+tech\s+etf"
    r"|\bscarcity\s+premium"
    r"|intl\s+etf\s+(?:chart|pattern|regime|season|premium|performance|correlation|drawdown|lgbm|ml)"
    r"|\boverseas\s+etf|foreign\s+etf"
    r"|international\s+etf\s+(?:regime|season|drawdown|correlation|performance|premium|lgbm)"
    r"|plot_intl_etf_(?:performance|premium)"
    r"|get_intl_etf_(?:performance|premium|data)",
    re.I,
)
_MF_RE = re.compile(
    r"\b(?:mutual\s+fund|mf\s+holdings?|fund\s+holdings?|holding\s+pattern)"
    r"|\b(?:dsp|nippon|bajaj|quant|icici)\s+(?:multi[\s-]?asset|equity|small\s+cap|mid\s+cap|flexi|focused|value|tax\s+saver|elss|healthcare|business\s+cycle)"
    r"|\bmulti[\s-]?asset(?:\s+fund)?\b"
    # AMC-specific import/refresh triggers → mf agent has the importers
    r"|\b(?:import|refresh|sync|update)\s+(?:all\s+)?(?:dsp|nippon|icici(?:\s+pru(?:dential)?)?)\s*(?:holdings?|data|funds?|portfolio)?\b"
    r"|\b(?:import|refresh|sync|update)\s+all\s+(?:multi[\s-]?asset\s+)?(?:fund\s+)?holdings?\b"
    r"|\bnav\s+return|monthly\s+return\s+of\s+(?:the\s+)?fund"
    r"|\bfund\s+(?:return|nav|cagr|performance|allocation|exposure)"
    r"|\bwhich\s+(?:funds?|amcs?)\s+(?:hold|own)"
    r"|\bcross[\s-]?fund\s+(?:consensus|overlap|ownership)"
    r"|\bsmart\s+money\s+(?:overlap|consensus|signal)"
    r"|\bfund\s+manager(?:'?s)?"
    r"|\bscheme\s+code\s*\d{4,7}",
    re.I,
)
_IMPORT_RE = re.compile(
    r"\b(?:import|refresh|sync|update)\s+(?:[a-zA-Z-]+\s+){0,3}(?:nav|price|prices|data|etfs?|stocks?|mf|fii|dii|cot|fx_rates|fx|inav|holdings?|flows?)\b"
    r"|\bimport\s+--(?:category|full)\b"
    r"|\bbackfill\b"
    r"|\brun\s+(?:the\s+)?(?:importer|import\s+pipeline)\b"
    # Symbol-level imports: "import GOLDBEES", "import gold bees", "refresh NIFTYBEES"
    # Negative lookahead excludes common English words that indicate a macro/news
    # query ("import duty on gold", "import tariff", "what is the import tax") so
    # those still reach the macro/news agents via the normal routing chain.
    r"|^(?:import|refresh|sync)\s+"
    r"(?!(?:duty|duties|tax|tariff|tariffs|ban|rule|rules|law|policy|regulation"
    r"|the|a|an|my|all|latest|today|new|more|some|those|these|this|that"
    # AMC names → fall through to _MF_RE in _regex_route_intent
    r"|dsp|nippon|icici|bajaj|quant)\b)"
    r"[a-zA-Z][a-zA-Z0-9\s,\-_]{1,50}$",
    re.I,
)
_DB_RE = re.compile(
    r"\bquery\s+(?:the\s+)?(?:database|db|clickhouse)\b"
    r"|\bsql\s+query\b"
    r"|\bclickhouse\b"
    r"|\bshow\s+(?:me\s+)?(?:all\s+)?tables\b"
    r"|\bdescribe\s+(?:table|the\s+table)\b"
    r"|\blist\s+(?:db\s+|database\s+)?tables\b"
    r"|\bhow\s+many\s+(?:rows|records)\b"
    r"|\blast\s+(?:import|watermark|sync)\b"
    r"|\bwatermarks?\b"
    r"|\bshow\s+(?:me\s+)?(?:the\s+)?schema\b"
    r"|\braw\s+(?:db\s+|database\s+)?data\b"
    r"|\bdb\s+query\b"
    r"|\bselect\b.{1,80}\bfrom\b",
    re.I,
)
_CODE_RE = re.compile(
    r"write\s+(?:a\s+)?(?:python\s+)?(?:script|code|function|tool|fetcher)"
    r"|create\s+(?:a\s+)?(?:new\s+)?(?:script|code|snippet|tool|fetcher)"
    r"|(?:add|create)\s+(?:a\s+)?(?:new\s+)?(?:signal\s+source|fetcher|adapter|tool)"
    r"|execute\s+(?:python|this\s+code|this\s+snippet)"
    r"|run\s+(?:python\s+)?(?:code|snippet)"
    r"|debug\s+(?:this|the)\s+(?:code|script|error|bug)"
    r"|fix\s+(?:the\s+)?(?:code|script|bug|error)"
    r"|custom\s+(?:query|script|analysis|sql)"
    r"|backtest\s+(?:this|the|a)"
    r"|(?:analyse?|analyze)\s+(?:\w+\s+)?(?:data|table|results)\s+(?:with\s+(?:code|python)|using\s+(?:code|python))?"
    r"|python\s+snippet|ad.?hoc\s+(?:query|analysis)"
    r"|show\s+me\s+the\s+code|list\s+(?:all\s+)?scripts",
    re.I,
)
_GENERAL_RESEARCH_RE = re.compile(
    r"(?:"
    r"find\s+(?:info|information|data)\s+(?:about|on|for)\s+"
    r"|(?:get|show|give)\s+(?:me\s+)?(?:info|data|details)\s+(?:about|on|for)\s+"
    r"|tell\s+me\s+about\s+"
    r"|(?:research|analyze|analyse)\s+(?!market|portfolio|holdings|etf)"
    r"|look\s+up\s+"
    r"|overview\s+of\s+"
    r"|details?\s+(?:about|on|for)\s+"
    r")(.+)",
    re.I,
)
_CLOUD_NEEDED_RE = re.compile(
    r"deep.?dive|10.?k\b|10.?q\b|sec filing|annual report|edgar"
    r"|portfolio analysis|full report|compare.*holdings"
    r"|explain.*over.*year|full.*analysis|comprehensive"
    r"|autonomous\s+research|deep\s+research|investigate\b|full\s+thesis",
    re.I,
)
_RESEARCH_RE = re.compile(
    r"\b(?:autonomous|deep|comprehensive|full|complete|thorough)\s+(?:research|analysis|study|report|investigation)\b"
    r"|\bresearch\s+(?:agent|mode)\b"
    r"|\binvestigate\b"
    r"|\bfull\s+thesis\b"
    r"|\bcross[- ](?:asset|domain|sector)\s+(?:research|analysis|correlation)\b"
    r"|\b(?:macro|mf)\s+(?:pattern|correlation|trend)\s+(?:for|on|in)\b"
    r"|\bpredict\s+(?:price|return|movement)\s+(?:for|of)\s+\w"
    r"|\bml\s+(?:price|return)\s+(?:prediction|forecast)\b"
    r"|\bwhy\s+(?:is|are)\s+\w+\s+(?:moving|falling|rising|crashing|rallying|volatile)\b"
    r"|\bmf\s+holding\s+(?:pattern|trend|analysis)\b"
    r"|\bholding\s+pattern\s+(?:for|of|in)\b",
    re.I,
)


# ── Ordered routing tables ──────────────────────────────────────────────────────
# Priority is explicit by list position. Add a new intent = one insert at the
# correct index. The plot-branch sub-table (_VIZ_ROUTE_TABLE) is checked only
# when the query contains visualisation keywords.

_PRE_PLOT_TABLE: list[tuple] = [
    (_DEEPDIVE_RE, "deepdive"),
    (_IMPORT_RE,   "main"),
    (_DB_RE,       "database"),
    (_CODE_RE,     "code"),
]

_VIZ_ROUTE_TABLE: list[tuple] = [
    (_INTL_ETF_RE, "intl_etf"),
    (_MACRO_RE,    "macro"),
    (_MF_RE,       "mf"),
]

_POST_PLOT_TABLE: list[tuple] = [
    (_SIGNAL_RE,   "signal"),
    (_INTL_ETF_RE, "intl_etf"),
    (_MF_RE,       "mf"),
    (_RESEARCH_RE, "research"),
    (_MACRO_RE,    "macro"),
    (_NEWS_RE,     "news"),
]

_VIZ_KEYWORDS: frozenset[str] = frozenset(("plot", "chart", "visualise", "visualize", "show"))


def _needs_cloud(question: str) -> bool:
    """Return True when the query needs a large-context or heavy-reasoning model."""
    return bool(_CLOUD_NEEDED_RE.search(question))


def _fast_path_intent(question: str) -> str | None:
    """
    Tiny fast-path router for the 3 truly unambiguous cases where calling an
    LLM would be wasteful. Returns None if the LLM router should decide.
    """
    if _IMPORT_RE.search(question):
        return "main"
    if _DB_RE.search(question):
        return "database"
    bare = question.strip().rstrip("?.")
    if 0 < len(bare.split()) <= 2:
        try:
            from src.tools.company_resolver import _local_indian_lookup
            from src.agents.signal_sources import SIGNAL_ETFS
            sym = _local_indian_lookup(bare)
            if sym:
                return "signal" if sym in SIGNAL_ETFS else "india_equity"
        except Exception:
            pass
    return None


def route_intent(question: str) -> str:
    """
    Determine which sub-agent should handle this question.

    Strategy: 3-case fast-path regex → LLM router (cached Haiku/gpt-4o-mini)
    → minimal-regex fallback only if LLM router unavailable.
    """
    hit = _fast_path_intent(question)
    if hit is not None:
        return hit
    try:
        from src.agents.intent_router import route_intent_llm
        return route_intent_llm(question)
    except Exception as exc:
        logger.debug("route_intent: LLM router unavailable (%s) — using regex fallback", exc)
        return _regex_route_intent(question)


def _regex_route_intent(question: str) -> str:
    """
    Legacy regex-only router. Used as the absolute fallback when no LLM
    router is configured and called by intent_router._regex_fallback.
    """
    # Pre-plot checks (import/db/code must fire before visualisation branch)
    for pattern, intent in _PRE_PLOT_TABLE:
        if pattern.search(question):
            return intent

    # Visualisation branch — "plot X" / "chart Y" / "show Z"
    if any(k in question.lower() for k in _VIZ_KEYWORDS):
        for pattern, intent in _VIZ_ROUTE_TABLE:
            if pattern.search(question):
                return intent
        # Best-effort: strip action words and check if the remainder is an Indian stock
        try:
            from src.tools.company_resolver import _local_indian_lookup
            from src.agents.signal_sources import SIGNAL_ETFS
            _words = re.sub(
                r"\b(?:plot|chart|visualise|visualize|show|display|give|me|the|a|an"
                r"|trend|year|month|day|week|daily|weekly|monthly|price|prices"
                r"|macd|rsi|bollinger|ema|sma|moving|average|volume|technical|indicator"
                r"|1|2|3|5|10|52|30|60|90|180|252|365)\b|'s\b",
                "", question, flags=re.I,
            ).strip()
            if _words:
                sym = _local_indian_lookup(_words)
                if sym and sym not in SIGNAL_ETFS:
                    return "india_equity"
        except Exception:
            pass
        return "signal"

    # Post-plot checks
    for pattern, intent in _POST_PLOT_TABLE:
        if pattern.search(question):
            return intent

    # Resolution-based fallbacks (runtime lookups — can't be in a static table)
    m = _GENERAL_RESEARCH_RE.search(question)
    if m:
        subject = m.group(1).strip().rstrip("?.")
        try:
            from src.tools.company_resolver import _local_indian_lookup, resolve_company_info
            if _local_indian_lookup(subject):
                return "india_equity"
            if len(subject.split()) <= 5:
                info = resolve_company_info(subject)
                if info["source"] != "fallback":
                    return "india_equity" if info["market"] == "India" else "deepdive"
        except Exception:
            pass
    bare = question.strip().rstrip("?.")
    if len(bare.split()) <= 4:
        try:
            from src.tools.company_resolver import _local_indian_lookup
            if _local_indian_lookup(bare):
                return "india_equity"
        except Exception:
            pass
    return "main"
