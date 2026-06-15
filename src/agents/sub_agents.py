"""
src/agents/sub_agents.py
────────────────────────
Specialised LangChain sub-agents for focused task domains.

Each sub-agent wraps a ``create_react_agent`` with a curated tool subset and
a domain-specific system prompt.  The main MosaicFundAgent auto-routes to the
appropriate sub-agent based on keyword intent detection.

Sub-agents
----------
DeepDiveSubAgent          — US stock SEC filings, XBRL financials, peer valuation
SignalSubAgent            — ETF signals, GOLDBEES ML pipeline, risk governor
MacroSubAgent             — COMEX, FII/DII flows, macro theme scanner
CodeSubAgent              — Python code execution, script writing, ClickHouse ad-hoc queries

Usage (internal)
----------------
    from src.agents.sub_agents import route_intent, get_subagent

    intent = route_intent(question)          # 'deepdive' | 'signal' | 'macro' | 'code' | 'main'
    if intent != 'main':
        answer = get_subagent(intent).run(question)
"""
from __future__ import annotations

import logging
import re
from typing import Any

logger = logging.getLogger(__name__)


def _make_context_trimmer(context_window: int):
    """
    Returns a ``pre_model_hook`` for ``create_react_agent`` that keeps each
    LLM call within *context_window* tokens (approximated as chars / 4).

    Strategy (applied before every model call):
      1. Hard-truncate each ToolMessage to ≤ 20 % of context (biggest single
         source of overflow — SQL results, news dumps, chart ASCII).
      2. If the total message chars still exceed 60 % of context, evict the
         oldest AI+Tool round-trip (the pair of AIMessage-with-tool_calls +
         its ToolMessages) repeatedly until it fits.
      3. Return the trimmed list as ``llm_input_messages`` so the actual
         LangGraph state (used for the fallback synthesis path) is untouched.

    Only attached when running a local model; cloud models skip this.
    """
    max_input_chars = int(context_window * 0.60 * 4)   # 60 % of ctx for input
    max_tool_chars  = int(context_window * 0.20 * 4)   # 20 % per tool output

    def _hook(state: dict) -> dict:
        from langchain_core.messages import ToolMessage, AIMessage

        msgs = list(state.get("llm_input_messages") or state.get("messages") or [])

        # Step 1 — truncate oversized ToolMessage content
        result = []
        for m in msgs:
            if isinstance(m, ToolMessage):
                content = str(m.content)
                if len(content) > max_tool_chars:
                    trimmed_n = len(content) - max_tool_chars
                    content = (
                        content[:max_tool_chars]
                        + f"\n…[{trimmed_n} chars trimmed — use narrower queries to fit local context]"
                    )
                    m = m.model_copy(update={"content": content})
            result.append(m)

        # Step 2 — evict oldest AI+Tool round-trips until total fits
        def _total(ms):
            return sum(len(str(m.content)) for m in ms)

        while _total(result) > max_input_chars and len(result) > 2:
            evicted = False
            for i in range(1, len(result)):
                m = result[i]
                if isinstance(m, AIMessage) and getattr(m, "tool_calls", None):
                    j = i + 1
                    while j < len(result) and isinstance(result[j], ToolMessage):
                        j += 1
                    result = result[:i] + result[j:]
                    evicted = True
                    break
            if not evicted:
                break

        return {"llm_input_messages": result}

    return _hook


def _print_thinking_blocks(content: Any, label: str = "🧠 Analyst Reasoning") -> None:
    """
    Extract Anthropic extended-thinking blocks from a message content and
    print them to the console in a distinctive cyan panel.

    Called after the synthesis LLM responds so the user can see the
    cross-check reasoning before reading the final report.

    content: AIMessage.content (list of dicts, or plain str)
    """
    if not isinstance(content, list):
        return
    thinking_parts = [
        blk.get("thinking", "")
        for blk in content
        if isinstance(blk, dict) and blk.get("type") == "thinking" and blk.get("thinking")
    ]
    if not thinking_parts:
        return
    try:
        from rich.console import Console
        from rich.panel import Panel
        from rich.markdown import Markdown
        _c = Console()
        thinking_text = "\n\n---\n\n".join(thinking_parts)
        _c.print(Panel(
            Markdown(thinking_text),
            title=f"[bold cyan]{label}[/bold cyan]",
            border_style="cyan",
            expand=False,
        ))
    except Exception:
        # Non-critical — log as debug if Rich unavailable
        logger.debug("thinking: %s", "\n".join(thinking_parts[:200]))


# ── Common indicator typo corrections ──────────────────────────────────────────

_INDICATOR_TYPOS: dict[str, str] = {
    "mcad": "MACD",
    "mcda": "MACD",
    "risi": "RSI",
    "bolinger": "Bollinger",
    "bolliger": "Bollinger",
    "boilinger": "Bollinger",
}

_INDICATOR_TYPO_RE = re.compile(
    r"\b(" + "|".join(re.escape(k) for k in _INDICATOR_TYPOS) + r")\b",
    re.IGNORECASE,
)


def _fix_indicator_typos(question: str) -> str:
    """Correct common misspellings of technical indicator names."""
    def _repl(m: re.Match) -> str:
        return _INDICATOR_TYPOS[m.group(0).lower()]
    return _INDICATOR_TYPO_RE.sub(_repl, question)


# ── Shared rule: injected into every agent system prompt ───────────────────────
NO_LLM_CALC_RULE = (
    "\n\nNUMERIC COMPUTATION RULE (mandatory — never violate): "
    "NEVER compute, estimate, or derive any number (returns, ratios, averages, "
    "percentages, scores, sums, differences, CAGR, PE, Kelly fractions, etc.) "
    "inside your response. ALL numeric work MUST be performed by a tool call "
    "(Python, SQL, or a dedicated function). You may ONLY narrate or format "
    "numbers that were returned verbatim by a tool. If no tool has produced a "
    "number, state that the data is unavailable — do NOT approximate."
)

# ── Intent routing ─────────────────────────────────────────────────────────────

_DEEPDIVE_RE = re.compile(
    r"deep.?dive|10.?k filing|10.?q filing|sec filing|edgar|xbrl|annual report"
    r"|adsk|autodesk|aapl|apple|msft|microsoft|nvda|nvidia|amzn|amazon|googl|meta|tsla",
    re.I,
)
_SIGNAL_RE = re.compile(
    # Explicit signal-intent keywords only — bare ETF names (goldbees, gold bees, etc.)
    # are intentionally excluded here so they resolve via the equity path (stock info,
    # price, news) unless the user adds an explicit signal keyword.
    r"\bsignal\b"
    r"|kelly\s+weight|composite\s+score|inav\s+premium|inav\s+discount"
    r"|etf\s+recommendation|buy\s+signal|sell\s+signal|risk\s+governor|blended\s+weight"
    r"|\bgarch\b|volatility\s+chart|vol\s+chart|ml\s+prediction|regime\s+signal"
    # Pipeline / ML explicit triggers (require the word pipeline, ml, or prediction)
    r"|goldbees\s+(?:pipeline|ml|prediction|recommendation)"
    r"|run\s+goldbees|run\s+pipeline"
    r"|today.?s\s+(?:gold|etf|composite)\s+signal"
    # Plot/chart triggers to direct them to the specialized chart tools in the signal/research agent
    r"|plot\s+(?:the\s+)?(?:price|chart|data|returns|volatility|garch)"
    r"|price\s+chart|returns\s+chart|garch\s+chart|volatility\s+trend",
    re.I,
)
_MACRO_RE = re.compile(
    r"\bcomex|macro theme|macro scan|\bfii\b|\bdii\b|fii flow|dii flow|institutional flow"
    r"|gold price|silver price|copper price|crude oil|fed rate|rbi rate"
    r"|usd.?inr|cot report|geopolit|war risk|tariff|trade war"
    # Geopolitical countries / blocs whose events move Indian markets
    r"|\b(?:iran|russia|ukraine|taiwan|israel|gaza|pakistan|opec|china)\b"
    # Broad macro/geopolitical news intent
    r"|financial\s+news|global\s+(?:news|market)|macro\s+news"
    r"|\bsanctions?\b|\b(?:war|conflict|crisis)\b",
    re.I,
)
_NEWS_RE = re.compile(
    # Explicit news-intent phrases — macro agent is checked first so geopolitical
    # country names (iran, russia, etc.) never reach here.
    r"\bnews\s+(?:on|for|about|of)\s+\w"   # "news on X", "news for X"
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
# Data-import intent — must fire before instrument-name regexes so that
# "import nav of HNGSNGBEES" routes to main (run_data_engineering_importer)
# rather than being hijacked by the intl_etf instrument match.
_IMPORT_RE = re.compile(
    r"\b(?:import|refresh|sync)\s+(?:nav|price|prices|data|etfs?|stocks?|mf|fii|dii|cot|fx|inav|holdings?|flows?)\b"
    r"|\bimport\s+--(?:category|full)\b"   # CLI form: import --category etfs / import --full
    r"|\bupdate\s+(?:nav|price|prices|data|etfs?|stocks?|mf|fii|dii|inav)\b"
    r"|\bbackfill\b"
    r"|\brun\s+(?:the\s+)?(?:importer|import\s+pipeline)\b",
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
# Broad research-intent phrases — used to detect "find info about X" style queries
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

# Queries that benefit from large-context cloud LLM (deep analysis, long reports)
_CLOUD_NEEDED_RE = re.compile(
    r"deep.?dive|10.?k\b|10.?q\b|sec filing|annual report|edgar"
    r"|portfolio analysis|full report|compare.*holdings"
    r"|explain.*over.*year|full.*analysis|comprehensive"
    r"|autonomous\s+research|deep\s+research|investigate\b|full\s+thesis",
    re.I,
)

# Multi-domain autonomous research — combines fundamentals, ML, macro, news, MF holdings
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


def _needs_cloud(question: str) -> bool:
    """Return True when the query needs a large-context or heavy-reasoning model."""
    return bool(_CLOUD_NEEDED_RE.search(question))


def _fast_path_intent(question: str) -> str | None:
    """
    Tiny fast-path router for the 3 truly unambiguous cases where calling an
    LLM would be wasteful. Returns None if the LLM router should decide.

    Cases handled here:
      - `import|refresh|sync` data ops  → main
      - Explicit SQL / database ops     → database
      - Bare ticker (≤2 words, in local lookup) → india_equity
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

    Returns
    -------
    'deepdive' | 'research' | 'india_equity' | 'signal' | 'macro'
    | 'intl_etf' | 'news' | 'code' | 'database' | 'main'
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
    router is configured (no OPENAI/ANTHROPIC/GOOGLE key) and called by
    intent_router._regex_fallback.
    """
    if _DEEPDIVE_RE.search(question):
        return "deepdive"
    if _IMPORT_RE.search(question):
        return "main"
    if _DB_RE.search(question):
        return "database"
    if _CODE_RE.search(question):
        return "code"
    if any(k in question.lower() for k in ("plot", "chart", "visualise", "visualize", "show")):
        if _INTL_ETF_RE.search(question):
            return "intl_etf"
        if _MACRO_RE.search(question):
            return "macro"
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
    if _SIGNAL_RE.search(question):
        return "signal"
    if _INTL_ETF_RE.search(question):
        return "intl_etf"
    if _RESEARCH_RE.search(question):
        return "research"
    if _MACRO_RE.search(question):
        return "macro"
    if _NEWS_RE.search(question):
        return "news"
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


def _get_message_text(content: Any) -> str:
    """Extract string content from LangChain message content, which could be a list of blocks."""
    if isinstance(content, list):
        texts = []
        for block in content:
            if isinstance(block, dict):
                if block.get("type") == "text":
                    texts.append(block.get("text", ""))
            elif isinstance(block, str):
                texts.append(block)
        return "\n".join(texts)
    return str(content) if content else ""


# ── Base sub-agent ─────────────────────────────────────────────────────────────

class _SubAgent:
    """
    Lazy-initialised sub-agent base.

    The LangGraph ReAct agent (and the LLM) are built on the first call to
    ``run()`` to avoid unnecessary startup cost when the sub-agent is never
    invoked in a session.
    """

    #: Override in subclass
    SYSTEM_PROMPT: str = "You are a helpful assistant."
    #: Override in subclass — property or class-level list
    TOOLS: list = []
    #: Max LangGraph steps. Simple agents (news, signal) need ~8; equity/research
    #: need more due to parallel tool batches + optional import steps.
    #: None = LangGraph default (25). Override per subclass as needed.
    RECURSION_LIMIT: int | None = 20

    def __init__(self) -> None:
        self._agent: Any = None
        self._llm: Any = None
        import os
        self._built_caveman_level: str | None = os.environ.get("CAVEMAN_LEVEL")

    def _build(self, llm_override: Any = None) -> None:
        """Lazily build the LangGraph ReAct agent.

        Parameters
        ----------
        llm_override:
            When provided (e.g. a cloud LLM), use it instead of the default
            local model.  This is set by ``get_subagent_for(question)`` when
            ``_needs_cloud(question)`` is True.

        Gemma 4 supports native function calling, but a ReAct loop with 5+ tool
        calls accumulates ~2500 tokens of history, leaving no room in a 4k context
        for the model to write a real answer.  For small context windows we skip
        the agent entirely and use the programmatic _fallback() path: Python
        collects all data, then a single LLM synthesis call produces the narrative.

        Threshold: 12 000 tokens gives enough headroom for the sub-agent system
        prompt + all tool payloads + final synthesis.
        """
        from src.agents.mosaic_fund_agent import MosaicFundAgent
        tmp = object.__new__(MosaicFundAgent)
        tmp._checkpointer = None

        if llm_override is not None:
            self._llm = llm_override
        else:
            self._llm = tmp._build_llm()

        from config.settings import settings
        if self._llm is None or (llm_override is None and settings.llm_context_window < 12000):
            # Local model disabled or context too small for a ReAct loop — try cloud LLM instead.
            cloud_llm = tmp._build_cloud_llm()
            if cloud_llm is not None:
                if self._llm is None:
                    logger.info("%s: local LLM disabled — upgrading to cloud LLM", self.__class__.__name__)
                else:
                    logger.info(
                        "%s: local context_window=%d < 12000 — upgrading to cloud LLM",
                        self.__class__.__name__, settings.llm_context_window,
                    )
                self._llm = cloud_llm
            else:
                if self._llm is None:
                    logger.warning("%s: local LLM disabled and no cloud LLM configured — falling back", self.__class__.__name__)
                else:
                    logger.info(
                        "%s: local context_window=%d < 12000 and no cloud LLM configured — falling back",
                        self.__class__.__name__, settings.llm_context_window,
                    )
                return  # leave self._agent = None → _confirm_fallback() path

        try:
            from langgraph.prebuilt import create_react_agent, ToolNode
            if self._llm is None:
                logger.warning("%s: LLM unavailable — sub-agent disabled", self.__class__.__name__)
                return
            tools = self._get_tools()
            # ToolNode runs all tool calls returned in a single AIMessage concurrently
            # via its internal ThreadPoolExecutor — no extra configuration required.
            tool_node = ToolNode(tools)
            from src.utils.caveman import get_caveman_prompt

            # Attach context trimmer for local models to prevent token-overflow mid-run.
            # Cloud models have large enough windows to not need this.
            pre_hook = None
            if llm_override is None and settings.is_local_model:
                pre_hook = _make_context_trimmer(settings.llm_context_window)
                logger.info(
                    "%s: context trimmer attached (window=%d tokens)",
                    self.__class__.__name__, settings.llm_context_window,
                )

            self._agent = create_react_agent(
                model=self._llm,
                tools=tool_node,
                prompt=self.SYSTEM_PROMPT + get_caveman_prompt() + NO_LLM_CALC_RULE,
                pre_model_hook=pre_hook,
            )
            logger.info("%s: agent built with parallel ToolNode (%d tools)", self.__class__.__name__, len(tools))
        except Exception as exc:
            logger.error("%s: build failed: %s", self.__class__.__name__, exc)

    def _get_tools(self) -> list:
        """Return the tool list.  Subclasses can override for lazy imports."""
        return self.TOOLS

    def run(self, question: str, llm_override: Any = None, callbacks: list | None = None) -> str:
        """Invoke the sub-agent and return its text response.

        Parameters
        ----------
        llm_override:
            Cloud LLM to use instead of the default local model.
        callbacks:
            LangChain callbacks list (e.g. [RichConsoleCallbackHandler()]) for
            verbose tool-call tracing.  Passed directly to agent.invoke().
        """
        import os
        current_caveman = os.environ.get("CAVEMAN_LEVEL")
        if self._agent is None or current_caveman != getattr(self, "_built_caveman_level", None):
            self._build(llm_override=llm_override)
            self._built_caveman_level = current_caveman
        if self._agent is None:
            return self._confirm_fallback(question)

        from src.tools.chart_tools import get_active_charts
        get_active_charts().clear()

        from langchain_core.messages import HumanMessage, ToolMessage, AIMessage
        from config.settings import settings
        is_local = (bool(settings.llm_base_url) and not settings.llm_local_disabled)
        limit = 8 if is_local else self.RECURSION_LIMIT
        config: dict = (
            {"recursion_limit": limit}
            if limit is not None
            else {}
        )
        if callbacks:
            config["callbacks"] = callbacks

        # Stream instead of invoke so we accumulate partial state at every step.
        # If the recursion limit fires mid-run, we still have all tool outputs
        # collected so far and can synthesise from them.
        msgs: list = []
        _recursion_hit = False
        try:
            for state in self._agent.stream(
                {"messages": [HumanMessage(content=question)]},
                config=config,
                stream_mode="values",
            ):
                if isinstance(state, dict):
                    msgs = state.get("messages", msgs)
        except Exception as exc:
            err = str(exc).lower()
            if "recursion" in err:
                _recursion_hit = True
                logger.warning(
                    "%s: recursion limit hit — synthesising from %d partial messages",
                    self.__class__.__name__, len(msgs),
                )
            elif any(k in err for k in ("tool", "400", "invalid_request", "function", "tool_calls", "not support")):
                logger.info(
                    "%s: LLM tool-calling failed (%s), using programmatic fallback",
                    self.__class__.__name__, type(exc).__name__,
                )
                return self._confirm_fallback(question)
            else:
                logger.error("%s.run() failed: %s", self.__class__.__name__, exc)
                return f"Research incomplete: {exc}"

        try:
            # Collect tool outputs; skip pure-plumbing symbol-resolution calls.
            _SKIP_KEYS = ('"symbol"', '"nse_symbol"', '"yf_symbol"')
            tool_sections = []
            for m in msgs:
                if not isinstance(m, ToolMessage):
                    continue
                content = str(m.content).strip()
                if not content:
                    continue
                if content.startswith("{") and any(k in content for k in _SKIP_KEYS):
                    continue
                tool_sections.append(content)

            if tool_sections:
                # Prefer a final LLM synthesis if it already exists.
                last_ai = next(
                    (m for m in reversed(msgs) if isinstance(m, AIMessage) and _get_message_text(m.content).strip()),
                    None,
                )
                if last_ai and not _recursion_hit:
                    ai_text = _get_message_text(last_ai.content)

                    # Print any extended-thinking blocks from the final message
                    _print_thinking_blocks(last_ai.content)

                    from src.tools.chart_tools import get_active_charts
                    chart_by_type = get_active_charts().copy()

                    # Strip any box-drawing / chart characters the LLM may have
                    # reproduced despite instructions.  These corrupt Rich panels.
                    import re as _re
                    _CHART_LINE_RE = _re.compile(
                        r"^.*[┤┼┌┐┘└├┬┴─]{3,}.*$|"   # box-drawing heavy lines
                        r"^.*[████▓▓▒▒░░]{4,}.*$|"    # bar chart fill blocks
                        r"^.*▞▞.*▗▌.*$",               # plotext braille scatter
                        _re.MULTILINE,
                    )
                    ai_text = _CHART_LINE_RE.sub("", ai_text)
                    # Clean up empty ``` blocks left behind
                    ai_text = _re.sub(r"```\s*```", "", ai_text)
                    # Collapse runs of 3+ blank lines
                    ai_text = _re.sub(r"\n{3,}", "\n\n", ai_text)

                    # Replace placeholders for all charts in chart_by_type
                    for tname in list(chart_by_type.keys()):
                        placeholders = [f"[CHART:{tname}]"]
                        if tname.startswith("plot_") and tname.endswith("_chart"):
                            short_name = tname[5:-6]  # e.g., "plot_macd_chart" -> "macd"
                            placeholders.append(f"[CHART:{short_name}]")
                        
                        for placeholder in placeholders:
                            if placeholder in ai_text:
                                ai_text = ai_text.replace(placeholder, chart_by_type.pop(tname))
                                break

                    # Fallbacks for specific standard sections if not explicitly replaced
                    if "price" in chart_by_type:
                        snap = _re.search(r"(#+\s*(?:\(?\d\)?\s*)?Company\s+Snapshot.*?)(?=\n\s*#|\Z)", ai_text, _re.I | _re.DOTALL)
                        if snap:
                            ai_text = ai_text[:snap.end()] + "\n\n" + chart_by_type.pop("price") + "\n" + ai_text[snap.end():]
                        else:
                            ai_text += "\n\n" + chart_by_type.pop("price")

                    if "shareholding" in chart_by_type:
                        own = _re.search(r"(#+\s*(?:\(?\d\)?\s*)?Institutional\s+Ownership.*?)(?=\n\s*[╭|])", ai_text, _re.I | _re.DOTALL)
                        if own:
                            ai_text = ai_text[:own.end()] + "\n\n" + chart_by_type.pop("shareholding") + "\n" + ai_text[own.end():]
                        else:
                            ai_text += "\n\n" + chart_by_type.pop("shareholding")

                    # Append any remaining charts (FII/DII, etc.) that weren't placed
                    for tname, chart_str in chart_by_type.items():
                        title = tname.replace("plot_", "").replace("_", " ").title()
                        ai_text += f"\n\n### {title}\n\n{chart_str}"

                    logger.info(
                        "%s: returning LLM synthesis (%d chars)",
                        self.__class__.__name__, len(ai_text),
                    )
                    return ai_text

                # Recursion limit hit (or no final AI message) — synthesise now.
                if self._llm:
                    try:
                        from langchain_core.messages import SystemMessage

                        # Use extended thinking for the synthesis call when the LLM
                        # is Anthropic Claude — gives a deeper reasoning pass over
                        # all collected tool data before writing the research note.
                        synth_llm = self._llm
                        try:
                            if hasattr(synth_llm, "model") and "claude" in str(getattr(synth_llm, "model", "")).lower():
                                synth_llm = synth_llm.bind(thinking={"type": "enabled", "budget_tokens": 8000})
                                logger.info("%s: extended thinking enabled for synthesis", self.__class__.__name__)
                        except Exception:
                            pass  # non-critical — fall through to normal LLM

                        combined = "\n\n---\n\n".join(tool_sections[:10])
                        from src.utils.caveman import get_caveman_prompt
                        sys_prompt = self.SYSTEM_PROMPT + get_caveman_prompt() + NO_LLM_CALC_RULE + "\n\n" + (
                            "PARTIAL DATA SYNTHESIS RULES (apply strictly):\n"
                            "- Write ONLY the sections for which you have actual tool output data.\n"
                            "- OMIT any section entirely if no tool data was collected for it.\n"
                            "- NEVER write '(Data pending)', 'N/A', or placeholder text.\n"
                            "- Do not mention step limits, recursion, or missing data.\n"
                            "- Be concise and factual — only report what the tools returned."
                        )
                        synth = synth_llm.invoke([
                            SystemMessage(content=sys_prompt),
                            HumanMessage(content=f"Question: {question}\n\nData collected:\n{combined}"),
                        ])
                        # Print extended-thinking blocks if present
                        _print_thinking_blocks(synth.content, label="🧠 Analyst Reasoning (extended thinking)")
                        logger.info(
                            "%s: partial synthesis (%d tool outputs → %d chars)",
                            self.__class__.__name__, len(tool_sections), len(str(synth.content)),
                        )
                        return _get_message_text(synth.content) if isinstance(synth.content, list) else str(synth.content)
                    except Exception as synth_exc:
                        logger.warning("%s: synthesis call failed: %s", self.__class__.__name__, synth_exc)

                # Last resort — concatenate raw tool outputs.
                logger.info("%s: merged %d tool outputs programmatically", self.__class__.__name__, len(tool_sections))
                return "\n\n---\n\n".join(tool_sections)

            # No tool calls — return the last AI message directly.
            return _get_message_text(msgs[-1].content) if msgs else "No response from sub-agent."
        except Exception as exc:
            logger.error("%s: message processing failed: %s", self.__class__.__name__, exc)
            return f"Research incomplete: {exc}"

    def _confirm_fallback(self, question: str) -> str:
        """Prompt the user before switching to the programmatic data-gathering path."""
        import sys
        prompt = "\n[mosaic] LLM tool-calling unavailable — use programmatic data gathering instead? [Y/n] "
        try:
            # Open /dev/tty directly so the prompt works even when Rich's Live display
            # is active and has captured stdout/stdin.
            with open("/dev/tty", "r+") as tty:
                tty.write(prompt)
                tty.flush()
                ans = tty.readline().strip().lower()
        except OSError:
            # Non-interactive environment (piped, tests) — default to yes.
            sys.stdout.write(prompt + "\n")
            sys.stdout.flush()
            ans = ""
        if ans in ("", "y", "yes"):
            return self._fallback(question)
        return "Aborted. To enable full capability, configure a tool-calling LLM in your .env."

    def _fallback(self, question: str) -> str:
        """Programmatic fallback for when the LLM cannot call tools.  Override in subclasses."""
        return (
            "Your configured LLM does not support tool-calling.  "
            "Set `LLM_PROVIDER=openai` or `LLM_PROVIDER=anthropic` in your .env for full capability.  "
            "Alternatively use `./mosaic.sh chat` for the interactive REPL."
        )


# ── Programmatic Indian equity data gatherer (no LLM tool-calling required) ──────────

def _gather_indian_equity_data(symbol: str, exchange: str, company_name: str, llm: Any = None) -> str:
    """
    Gather comprehensive Indian equity data via direct Python function calls.

    This is the tool-calling-free fallback path for models like gemma4 that
    do not support function/tool use.  Calls each data-source function directly
    and assembles a formatted Markdown research note.
    """
    from datetime import date as _date
    parts: list[str] = [
        f"# {company_name} ({symbol})\n"
        f"*Exchange: {exchange} \u2022 Research date: {_date.today()}*\n"
    ]

    # 1. Yahoo Finance overview + price momentum
    try:
        from src.tools.yahoo_finance import fetch_yahoo_data, fetch_price_history
        yf   = fetch_yahoo_data(symbol, exchange)
        hist = fetch_price_history(symbol, exchange, "1y")
        mc   = f"₹{yf.market_cap / 1e7:,.0f} Cr" if yf.market_cap else "N/A"

        yoy_change_str = "—"
        if len(hist) >= 2:
            latest = hist[-1]["close"]
            prev1y = hist[0]["close"]
            r1y = round((latest - prev1y) / prev1y * 100, 2) if prev1y else 0
            yoy_change_str = f"{r1y:+.2f}%"

        parts.append(
            f"## Company Snapshot\n"
            f"| Metric | Value |\n|---|---|\n"
            f"| Sector | {yf.sector or 'N/A'} |\n"
            f"| Industry | {yf.industry or 'N/A'} |\n"
            f"| Market Cap | {mc} (YoY Change: {yoy_change_str}) |\n"
            f"| P/E (Trailing) | {round(yf.pe_ratio, 1) if yf.pe_ratio else 'N/A'} |\n"
            f"| P/B | {round(yf.pb_ratio, 1) if yf.pb_ratio else 'N/A'} |\n"
            f"| Current Price | ₹{yf.current_price:,.2f} (YoY Change: {yoy_change_str}) |\n"
            f"| 52-Week High | ₹{yf.fifty_two_week_high:,.2f} |\n"
            f"| 52-Week Low | ₹{yf.fifty_two_week_low:,.2f} |\n"
        )
        if yf.description:
            parts.append(f"**Business:** {yf.description[:500]}…")
        if len(hist) >= 2:
            latest  = hist[-1]["close"]
            idx_30d = max(0, len(hist) - 22)
            idx_90d = max(0, len(hist) - 66)
            prev30  = hist[idx_30d]["close"]
            prev90  = hist[idx_90d]["close"]
            prev1y  = hist[0]["close"]
            r30 = round((latest - prev30) / prev30 * 100, 2) if prev30 else 0
            r90 = round((latest - prev90) / prev90 * 100, 2) if prev90 else 0
            r1y = round((latest - prev1y) / prev1y * 100, 2) if prev1y else 0
            sig = "BULLISH" if r30 > 5 else "BEARISH" if r30 < -5 else "NEUTRAL"
            parts.append(f"**Price Momentum:** 30d {r30:+.2f}% │ 90d {r90:+.2f}% │ 1y (YoY) {r1y:+.2f}% │ Signal: **{sig}**")
            try:
                from src.tools.chart_tools import plot_price_chart
                chart_str = plot_price_chart(symbol, days=365)
                if chart_str and "No price data found" not in chart_str and "Error" not in chart_str:
                    parts.append(f"### 1-Year Price Chart\n{chart_str}")
            except Exception as chart_exc:
                logger.warning("Failed to add price chart to programmatic output: %s", chart_exc)
    except Exception as exc:
        parts.append(f"## Company Snapshot\n*Yahoo Finance unavailable: {exc}*")

    # 2. Quarterly results
    try:
        from src.tools.earnings_scraper import fetch_from_screener, fetch_from_yahoo_financials
        q = fetch_from_screener(symbol) or fetch_from_yahoo_financials(symbol, exchange)
        if q:
            parts.append(
                f"## Latest Quarterly Results ({q.period})\n"
                f"| Metric | Value |\n|---|---|\n"
                f"| Revenue | ₹{q.revenue_cr:,.0f} Cr |\n"
                f"| Net Profit | ₹{q.net_profit_cr:,.0f} Cr |\n"
                f"| EPS | ₹{q.eps:.2f} |\n"
                f"| Revenue Growth YoY | {q.revenue_yoy_pct:+.1f}% |\n"
                f"| Profit Growth YoY | {q.profit_yoy_pct:+.1f}% |\n"
                f"| EPS Growth YoY | {q.eps_yoy_pct:+.1f}% |\n"
            )
        else:
            parts.append("## Quarterly Results\n*Not available via Screener.in for this symbol.*")
    except Exception as exc:
        parts.append(f"## Quarterly Results\n*Unavailable: {exc}*")

    # 3. Cash flow
    try:
        from src.tools.indian_equity_tools import get_stock_cashflow
        cf_result = get_stock_cashflow.invoke({"input_str": f"{symbol}:{exchange}"})
        if isinstance(cf_result, dict) and cf_result.get("annual_cashflows"):
            rows = cf_result["annual_cashflows"]
            lines = ["## Annual Cash Flows\n| FY End | FCF (\u20b9M) | Op CF (\u20b9M) | Capex (\u20b9M) |", "|---|---|---|---|"]
            for r in rows:
                lines.append(
                    f"| {r['fiscal_year_end']} "
                    f"| {r.get('free_cash_flow_usd_m') or 'N/A'} "
                    f"| {r.get('operating_cash_flow_usd_m') or 'N/A'} "
                    f"| {r.get('capex_usd_m') or 'N/A'} |"
                )
            parts.append("\n".join(lines))
        else:
            parts.append("## Cash Flow\n*No cash flow data available.*")
    except Exception as exc:
        parts.append(f"## Cash Flow\n*Unavailable: {exc}*")

    # 4. DSP mutual fund holdings
    try:
        from src.tools.indian_equity_tools import get_mf_holdings_for_stock
        mf_result = get_mf_holdings_for_stock.invoke({"company_name_or_symbol": company_name})
        parts.append(
            f"## DSP Mutual Fund Holdings\n{mf_result}"
            if mf_result and "Error" not in mf_result
            else f"## DSP Mutual Fund Holdings\n*{mf_result or 'No data'}*"
        )
    except Exception as exc:
        parts.append(f"## DSP Fund Holdings\n*Unavailable: {exc}*")

    # 5. Recent news
    try:
        from src.tools.news_search import fetch_news_for_symbol
        news = fetch_news_for_symbol(symbol, company_name)
        if news:
            lines = ["## Recent News\n| Headline | Source | Sentiment |", "|---|---|---|"]
            for n in news[:8]:
                lines.append(f"| {n.title[:70]} | {n.source} | {n.sentiment.value} |")
            parts.append("\n".join(lines))
        else:
            parts.append("## Recent News\n*No recent news found.*")
    except Exception as exc:
        parts.append(f"## News\n*Unavailable: {exc}*")

    raw_data = "\n\n".join(parts)

    # LLM synthesis — only attempted when the model has enough context headroom.
    # For small-context models (e.g. gemma4 at 4k) we return the raw tables directly;
    # the data is already complete and actionable without an extra LLM call.
    from config.settings import settings
    if llm is not None and settings.llm_context_window >= 12000:
        try:
            from langchain_core.messages import HumanMessage, SystemMessage
            budget = settings.llm_prompt_budget  # chars
            data_budget = max(500, budget - 300)
            truncated = raw_data[:data_budget]
            if len(raw_data) > data_budget:
                truncated += "\n\n*[data truncated to fit context window]*"

            synthesis_prompt = (
                f"You are a senior Indian equity analyst. Below is live market data for "
                f"{company_name} ({symbol}, {exchange}). "
                f"In 3-5 concise paragraphs synthesise: (1) business quality, "
                f"(2) valuation vs sector, (3) cash flow trend, "
                f"(4) institutional sentiment, (5) a clear BUY/HOLD/SELL/WATCH verdict with one-line rationale. "
                f"Never invent numbers — use only the data provided.\n\n"
                f"--- DATA ---\n{truncated}"
            )
            from src.utils.caveman import get_caveman_prompt
            res = llm.invoke([
                SystemMessage(content="You are a concise Indian equity research analyst." + get_caveman_prompt()),
                HumanMessage(content=synthesis_prompt),
            ])
            synthesis = str(res.content).strip()
            parts.append(f"## Analyst Synthesis\n{synthesis}")
            logger.info("_gather_indian_equity_data: LLM synthesis complete (%d chars)", len(synthesis))
        except Exception as exc:
            logger.warning("_gather_indian_equity_data: LLM synthesis failed: %s", exc)
        return "\n\n".join(parts)

    return raw_data


# ── DeepDive sub-agent ─────────────────────────────────────────────────────────

class DeepDiveSubAgent(_SubAgent):
    """
    US equity research: SEC 10-K/10-Q filings, XBRL financials, peer valuation.

    Uses the full deepdive pipeline tool (run_deepdive_analysis) plus
    ClickHouse readers for previously stored structured data.

    Also carries ``resolve_company`` so the LLM can disambiguate company names
    before fetching SEC data.  If the company resolves to Indian (NSE/BSE) the
    agent will say so and advise using IndianEquityResearchSubAgent instead.
    """

    SYSTEM_PROMPT = (
        "You are a US equity research analyst specialising in SEC filing analysis. "
        "You have access to EDGAR data, XBRL financials, and Yahoo Finance market data. "
        "FIRST: Call `resolve_company` on any input ticker/name to confirm the symbol "
        "and verify the market is 'US'. Ticker symbols can change or be newly listed; "
        "always check if the output contains an 'error' field before proceeding. "
        "If `resolve_company` returns market='India', "
        "immediately reply: \"This stock is listed in India (NSE/BSE). "
        "Please use the Indian equity research path.\".  "
        "For US tickers: use `run_deepdive_analysis` to fetch SEC filings. "
        "Use `query_clickhouse_db` to read deepdive_* tables in ClickHouse "
        "(always add FINAL). "
        "Use `get_yahoo_finance_data` for live price and valuation multiples. "
        "Present all data as Markdown tables. Never invent numbers."
    )

    def _get_tools(self) -> list:
        from src.tools.yahoo_finance import YAHOO_TOOLS
        from src.tools.earnings_scraper import EARNINGS_TOOLS
        from src.tools.skills_tools import run_deepdive_analysis, query_clickhouse_db
        from src.tools.company_resolver import resolve_company
        return [resolve_company] + YAHOO_TOOLS + EARNINGS_TOOLS + [
            run_deepdive_analysis, query_clickhouse_db,
        ]


# ── Indian Equity Research sub-agent ──────────────────────────────────────────

class IndianEquityResearchSubAgent(_SubAgent):
    """
    Comprehensive research for any Indian stock (NSE/BSE).

    Covers: company overview · price momentum · quarterly earnings ·
    MF fund holdings · annual cash flow · recent news · FII/DII flows.

    Can accept a company name ("adani enterprise"), a partial name, or a
    direct NSE symbol — ``resolve_company`` is always called first.
    """

    # resolve(1) + optional check/import(2) + 8 parallel tools(1) + synthesis(1) = ~8-15 steps
    RECURSION_LIMIT = 50

    SYSTEM_PROMPT = (
        "You are a senior Indian equity analyst covering NSE/BSE listed stocks. "
        "Research happens in exactly TWO rounds to maximise parallel execution:\n\n"
        "ROUND 1 — Resolve (single call):\n"
        "  Call `resolve_company(query)` to get `symbol` (e.g. ADANIENT), `exchange`, "
        "and `company_name`. Wait for the result before proceeding. "
        "Note that company symbols can change, demerge, or be newly listed; always check "
        "if the output contains an 'error' field before proceeding to Round 2.\n\n"
        "ROUND 2 — Parallel data fetch (emit ALL in ONE response — never call them one by one):\n"
        "  • `get_yahoo_finance_data(\"SYMBOL:EXCHANGE\")` — price, P/E, P/B, 52-week range, market cap\n"
        "  • `get_price_momentum(\"SYMBOL:EXCHANGE\")` — 30d/90d returns, momentum signal\n"
        "  • `get_quarterly_results(\"SYMBOL:EXCHANGE\")` — revenue, net profit, EPS, YoY growth\n"
        "  • `get_stock_cashflow(\"SYMBOL:EXCHANGE\")` — 3yr FCF, operating CF, capex\n"
        "  • `plot_shareholding_bar(symbol)` — fetches AND charts Promoter/FII/DII/Public % (do NOT also call get_shareholding_pattern separately)\n"
        "  • `get_mf_holdings_for_stock(company_name)` — DSP fund cross-ownership\n"
        "  • `get_db_price_summary(symbol)` — 30/60/90/365-day price trends from ClickHouse (auto-imports if missing)\n"
        "  • `get_stock_news(company_name)` AND `get_newsapi_stock_news(symbol)` — news & sentiment\n"
        "  • `plot_price_chart(symbol, 365)` — ALWAYS call this to fetch a 1-year price chart\n"
        "  • `search_anomaly_events(symbol, 365)` — ALWAYS call this to scan for 1-year price anomalies and fetch news context explaining the underlying reasons for those shocks\n"
        "  • `find_anomaly_correlations(symbol, 365)` — ALWAYS call this to map anomaly dates to FX shocks, macro events, and corporate filings; saves correlation timeline and lead-lag grid charts to disk for inclusion in the PDF\n"
        "  • `plot_macd_chart(symbol, days)` — MACD(12,26,9) chart with signal line + histogram (use when user asks for MACD)\n\n"
        "TECHNICAL INDICATOR RECOGNITION:\n"
        "When the query contains MACD, RSI, Bollinger, EMA, SMA, or similar indicator names, "
        "the user wants a chart/analysis of that indicator — NOT a second stock. "
        "For example, 'ADVENZYMES MACD' means 'show MACD chart for ADVENZYMES', not two stocks. "
        "Call `plot_macd_chart(symbol, 180)` for MACD requests.\n\n"
        "CRITICAL: All parallel tools must appear in one AIMessage response as parallel tool calls. "
        "Calling them one at a time wastes steps and will hit the recursion limit.\n\n"
        "SYNTHESIS: After all results arrive, reason through the data before writing:\n\n"
        "REASONING STEP (do this silently before writing the report):\n"
        "  1. Cross-check revenue/profit growth vs price momentum — do they corroborate each other?\n"
        "  2. Assess FCF quality: is operating CF genuinely growing, or is capex masking weak earnings?\n"
        "  3. Evaluate promoter + FII/DII QoQ deltas — are institutions accumulating or distributing?\n"
        "  4. Gauge valuation: P/E relative to profit growth → compute a qualitative PEG assessment.\n"
        "  5. Assess competitive moat — is this a niche leader or a commodity player?\n"
        "  6. Identify the single most important risk that could invalidate the investment thesis.\n"
        "  7. Arrive at a conviction-weighted BUY/HOLD/SELL/WATCH rating with clear rationale.\n\n"
        "Then write the structured Markdown research note:\n"
        "(1) Company Snapshot — table of key metrics, then write `[CHART:price]` on its own line where the price chart should appear  "
        "(1b) Price Anomalies & Shock Events — summarise the dates, price shocks, and underlying news/macro causes retrieved from search_anomaly_events (explain the anomalies/red dots on the chart)  "
        "(1c) Event Correlation Analysis — include the full output of find_anomaly_correlations verbatim (attribution table, mapped anomalies timeline, FX validation block, and attribution summary). "
        "Write `[CHART:correlation_timeline]` then `[CHART:lead_lag_grid]` on their own lines immediately after the attribution table so the charts appear inline.  "
        "(2) Financials table  (3) Valuation vs sector  "
        "(4) Cash Flow quality  "
        "(5) Institutional Ownership — write `[CHART:shareholding]` on its own line where the shareholding bar should appear, then the "
        "Promoter/FII/DII/Public % table with QoQ delta arrows (↑↓) from plot_shareholding_bar output. "
        "Also include DSP MF cross-ownership from get_mf_holdings_for_stock.  "
        "(6) News Sentiment  "
        "(7) Key Risks (ranked by severity, with the thesis-killer risk called out explicitly)  "
        "(8) Analyst Reasoning — 3-5 sentences explaining the cross-checks from the reasoning step above  "
        "(9) Recommendation (BUY/HOLD/SELL/WATCH + conviction level LOW/MEDIUM/HIGH + one-line rationale)\n\n"
        "CHART RULES (CRITICAL — violating these causes duplicate charts):\n"
        "- NEVER reproduce, copy, or re-type any chart/graph output from plot_* tools.\n"
        "- NEVER include box-drawing characters (┤ ┼ ─ └ ┐ ┘ ┌ ├ ████ ▓▓ ░░) in your text.\n"
        "- Write placeholder tags on their own lines where charts should appear inline:\n"
        "  `[CHART:price]` — in section (1) after the Company Snapshot table\n"
        "  `[CHART:shareholding]` — in section (5) after the Institutional Ownership header\n"
        "  `[CHART:correlation_timeline]` — in section (1c) after the attribution table\n"
        "  `[CHART:lead_lag_grid]` — in section (1c) immediately after correlation_timeline\n"
        "- The publisher replaces these with actual inline chart images.\n"
        "- Charts from plot_* tools are rendered separately — your job is ONLY the narrative text.\n\n"
        "RULES: All monetary values in ₹. Never invent figures.\n\n"
        "DATA AVAILABILITY: If a ClickHouse query returns 0 rows, or plot_price_chart "
        "returns 'No price data found', call `check_and_refresh_symbol_data(symbol)` "
        "to auto-import the data, then retry the query or chart tool.\n\n"
        "EXPORT: Only export when the user explicitly asks. Formats available:\n"
        "  PDF (default):  `publish_consolidated_pdf(report_markdown=<full_note>, format='pdf')`\n"
        "  Markdown file:  `publish_consolidated_pdf(report_markdown=<full_note>, format='md')`\n"
        "  Self-contained HTML: `publish_consolidated_pdf(report_markdown=<full_note>, format='html')`"
    )

    def _get_tools(self) -> list:
        from src.tools.company_resolver import resolve_company
        from src.tools.yahoo_finance import YAHOO_TOOLS
        from src.tools.earnings_scraper import get_quarterly_results  # get_shareholding_pattern excluded — plot_shareholding_bar calls it internally
        from src.tools.news_search import get_stock_news
        from src.tools.newsapi_search import get_newsapi_stock_news
        from src.tools.skills_tools import query_clickhouse_db, import_symbol_data
        from src.tools.indian_equity_tools import get_mf_holdings_for_stock, get_stock_cashflow, get_db_price_summary
        from src.tools.chart_tools import plot_price_chart, plot_shareholding_bar, plot_macd_chart
        from src.tools.market.equity import search_anomaly_events
        from src.tools.market.correlation_tools import find_anomaly_correlations
        from src.tools.agent_tools import check_and_refresh_symbol_data
        from src.tools.report_publisher import publish_research_pdf, publish_consolidated_pdf
        return (
            [resolve_company]
            + YAHOO_TOOLS
            + [get_quarterly_results]
            + [get_stock_news, get_newsapi_stock_news, query_clickhouse_db,
               import_symbol_data, check_and_refresh_symbol_data,
               plot_price_chart, plot_shareholding_bar, plot_macd_chart,
               get_mf_holdings_for_stock, get_stock_cashflow, get_db_price_summary,
               search_anomaly_events, find_anomaly_correlations,
               publish_research_pdf, publish_consolidated_pdf]
        )

    def _fallback(self, question: str) -> str:
        """Programmatic research path — works without LLM tool-calling."""
        import re as _re
        # The question may be pre-formatted: "Research COMPANY (SYMBOL) listed on EXCHANGE."
        m = _re.search(r"Research (.+?) \((\S+?)\) listed on (\S+)", question)
        if m:
            company_name = m.group(1)
            symbol       = m.group(2)
            exchange     = m.group(3).rstrip(".")
        else:
            # Strip action verbs and resolve the remainder
            subject = _re.sub(
                r"^(?:find\s+(?:info|information|data)\s+(?:about|on|for)|tell\s+me\s+about"
                r"|research|analyze|look\s+up|info\s+(?:about|on))\s+",
                "", question, flags=_re.I,
            ).strip().rstrip("?.")
            from src.tools.company_resolver import resolve_company_info
            info         = resolve_company_info(subject or question)
            symbol       = info["symbol"]
            exchange     = info["exchange"]
            company_name = info["company_name"]
        logger.info(
            "IndianEquityResearchSubAgent: programmatic research for %s (%s)",
            symbol, exchange,
        )
        return _gather_indian_equity_data(symbol, exchange, company_name, self._llm)


# ── Signal sub-agent ───────────────────────────────────────────────────────────

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
        from src.tools.market.equity import search_anomaly_events
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


# ── Macro sub-agent ────────────────────────────────────────────────────────────

class MacroSubAgent(_SubAgent):
    """
    Macro analysis: COMEX commodity signals, FII/DII flows, macro theme scanner.
    """

    SYSTEM_PROMPT = (
        "You are a macro analyst covering Indian and global commodity markets. "
        "You handle both quantitative macro signals AND news on geopolitical topics.\n\n"
        "## Macro signals\n"
        "Use `run_macro_scanner` to scan live macro/geopolitical events and map "
        "their directional impact to ETFs. "
        "Use `run_comex_analysis` for COMEX gold/silver/copper pre-market price signals. "
        "Use `run_whale_tracker` to track weight shifts and institutional moves in core macro themes (Gold, Silver, Nuclear, Energy, Infra) across multi-asset funds. "
        "Use `get_dxy_context` to get the current US Dollar Index (DXY) level, 5-day and "
        "20-day change, trend direction, and macro interpretation for gold and INR. "
        "Call `get_dxy_context` whenever the user asks about the dollar, DXY, USD strength, "
        "or its impact on gold / USDINR. "
        "Use `query_clickhouse_db` to read `market_data.fii_dii_flows FINAL` and "
        "`market_data.cot_gold FINAL` for institutional positioning data. "
        "Net article flow index interpretation: ≥+16 = strong bullish | +8 to +15 = moderate bullish "
        "| ≤−16 = strong bearish.\n\n"
        "## Index stats (valuation & breadth)\n"
        "Use `run_market_indicators` to fetch the index valuation (weighted P/E, P/B), market breadth (% of stocks above 50/200 DMA, Advances/Declines), and macro stress indicators (rupee stress DXY deviation, gold ETF SPDR GLD tonnes flow, sector rotation rank). "
        "When asked about general market health, daily overview, or index valuations, ALWAYS run `run_market_indicators` and integrate this quantitative context with `run_macro_scanner` output.\n\n"
        "## Geopolitical / country news\n"
        "When the query is about a country or geopolitical event (Iran, Russia, crude oil, "
        "sanctions, war, etc.), call `search_financial_news` with a focused query such as "
        "'Iran oil sanctions Indian market impact' to fetch live news articles. "
        "Then call `run_macro_scanner` to get the ETF net article flows. "
        "Present both: news table first, then ETF net article flows.\n\n"
        "## Charts\n"
        "If the user asks for a chart, visualisation, or trend:\n"
        "- FII/DII flow trend → `plot_fii_dii_chart(days)`\n"
        "- Gold/silver/commodity price trend → `plot_price_chart(symbol, days)`\n"
        "- Multi-asset fund holdings, allocations, or institutional shifts → `run_whale_tracker` (automatically appends ASCII trend charts)\n"
        "Always call the appropriate chart tool to render the visual when requested.\n\n"
        "CRITICAL: Only cite prices and flows from live tool output — never from "
        "training-time knowledge. Gold, FII, USDINR change daily."
    )

    def _get_tools(self) -> list:
        from src.tools.skills_tools import (
            run_macro_scanner,
            run_comex_analysis,
            query_clickhouse_db,
            run_whale_tracker,
            run_market_indicators,
        )
        from src.tools.market_context import get_dxy_context
        from src.tools.news_search import search_financial_news, get_db_news
        from src.tools.chart_tools import plot_fii_dii_chart, plot_price_chart
        return [
            run_macro_scanner,
            run_comex_analysis,
            query_clickhouse_db,
            run_whale_tracker,
            run_market_indicators,
            get_dxy_context,
            search_financial_news,
            get_db_news,
            plot_fii_dii_chart,
            plot_price_chart,
        ]


# ── International ETF sub-agent ───────────────────────────────────────────────

class IntlETFSubAgent(_SubAgent):
    """
    International ETF Pattern Analysis agent.

    Symbols: MAFANG · HNGSNGBEES · MON100 · MASPTOP50 · MAHKTECH · MONQ50

    7 analytical lenses
    -------------------
    Performance  — 3-year return, volatility, Sharpe ratio
    Premium      — scarcity premium/discount (RBI overseas cap creates arbitrage)
    Regimes      — KMeans Bull/Sideways/Bear detection
    Correlation  — return correlations + USDINR sensitivity
    Seasonality  — best/worst months per ETF
    LightGBM     — feature importance for 5-day return prediction
    Drawdowns    — major episodes > 10% from peak
    """

    SYSTEM_PROMPT = """\
You are the Mosaic International ETF Analyst covering NSE-listed overseas ETFs.

## Universe (6 ETFs)
| Symbol      | AMC    | Underlying Index          | Geography        |
|-------------|--------|---------------------------|------------------|
| MAFANG      | Mirae  | NYSE FANG+ Index          | US / China Tech  |
| HNGSNGBEES  | Nippon | Hang Seng Index           | Hong Kong        |
| MON100      | Motilal| Nasdaq 100 Index          | US Large-Cap Tech|
| MASPTOP50   | Mirae  | S&P 500 Top 50            | US Large-Cap     |
| MAHKTECH    | Mirae  | Hang Seng Tech Index      | HK Tech          |
| MONQ50      | Motilal| Nasdaq 50 Index           | US Mid-Cap Tech  |

## Tool Selection Guide
Match the user's intent to the right tool — call the chart immediately after the data tool:

| Intent                              | Data tool                        | Chart tool                  |
|-------------------------------------|----------------------------------|-----------------------------|
| Performance / 3-year returns        | `get_intl_etf_performance()`     | `plot_intl_etf_performance()`|
| Scarcity premium / discount         | `get_intl_etf_premium(symbol)`   | `plot_intl_etf_premium(symbol)`|
| Bull/Sideways/Bear regime           | `get_intl_etf_regimes()`         | (narrate regimes in text)   |
| Best / worst months (seasonality)   | `get_intl_etf_seasonality()`     | (narrate in table)          |
| Return correlations + USDINR        | `get_intl_etf_correlation()`     | (narrate in table)          |
| Major drawdown episodes             | `get_intl_etf_drawdowns()`       | (narrate in table)          |
| ML feature importance (LightGBM)    | `get_intl_etf_lgbm()`            | (narrate feature ranks)     |
| Simple price trend                  | (use price from performance)     | `plot_price_chart(symbol)`  |

For a full picture, combine: performance → premium → regime → correlation.

## Scarcity Premium — Key Mechanism
SEBI/RBI cap India's overseas fund exposure at USD 7 billion industry-wide. When the
limit is fully utilised, AMCs cannot create new ETF units → ETF market price detaches
from NAV and trades at a PREMIUM. When RBI relaxes headroom, the premium compresses.
Interpretation:
- Premium > +5%  → expensive; avoid fresh entry, demand exceeds supply
- Premium 0–5%   → normal; unit-creation friction priced in
- Discount < 0%  → rare buying window; overseas cap has headroom, creation is open
Always check the premium trend alongside the regime before recommending.

## USDINR Sensitivity
These ETFs have a built-in USDINR (or HKDINR) currency overlay — a weakening INR
inflates NAV even when the underlying index is flat. Use `get_intl_etf_correlation()`
to show how much of each ETF's return is FX-driven vs index-driven.

## Import Queries
This agent is read-only. If the user asks to import, refresh, or update NAV/price data,
tell them to use: `python src/main.py import --category etfs`
or type: "import etfs" in the chat (routes to the main agent).

## iNAV Freshness
Premium data is automatically kept current:
- **During market hours (IST 09:15–15:30)**: if the DB snapshot is older than 10 minutes
  the tool fetches live iNAV from the NSE API and stores the result. The `inav_source`
  field in tool output will show `"nse_api_live"` when this happens.
- **Outside market hours**: last stored snapshot (up to 4 days old) is used.
When reporting a premium, always mention the snapshot timestamp so the user knows
whether they are seeing live or cached data.

## Rules
- Never invent numbers — use only tool output.
- Always call the chart tool after the data tool when visualisation is useful.
- For a single ETF query: pull that ETF's premium and regime before concluding.
- For comparison queries: get_intl_etf_performance first, then drill into premium/regime.
- All six ETFs are Indian rupee-denominated despite tracking foreign indices.
"""

    def _get_tools(self) -> list:
        from src.tools.intl_etf_tools import INTL_ETF_TOOLS
        from src.tools.chart_tools import plot_intl_etf_performance, plot_intl_etf_premium, plot_price_chart
        return INTL_ETF_TOOLS + [plot_intl_etf_performance, plot_intl_etf_premium, plot_price_chart]


# ── News sub-agent ────────────────────────────────────────────────────────────

class NewsSubAgent(_SubAgent):
    """
    Financial news aggregation and sentiment agent.

    Sources
    -------
    • Google News (GNews)   — free, no quota, good for Indian market news
    • NewsAPI.org           — richer metadata, 100 req/day free tier
    • ClickHouse            — saved ETF news from previous `etf-news` runs
    • ETF news sentinel     — run_etf_news_sentiment for category-level news

    Workflow
    --------
    1. Company query → resolve symbol → get_stock_news + get_newsapi_stock_news (parallel)
    2. General query → search_financial_news (free-text GNews)
    3. Historical/saved → get_db_news (ClickHouse news_articles table)
    4. ETF category → run_etf_news_sentiment
    Synthesise: Markdown table + 2-3 sentence sentiment summary.
    """

    SYSTEM_PROMPT = (
        "You are the Mosaic News Agent — an Indian financial news aggregator.\n\n"
        "## Workflow\n"
        "**Company/ETF news** (e.g. 'news on HDFC', 'news for gold bees'):\n"
        "  1. Call `resolve_company` to get the NSE symbol.\n"
        "  2. Call `get_stock_news` AND `get_newsapi_stock_news` in parallel using \"SYMBOL|Company Name\".\n"
        "  3. Merge results, deduplicate by title, sort by date.\n\n"
        "**Broad queries** ('market news today', 'etf news', 'earnings news'):\n"
        "  1. Call `search_financial_news(query)` with a focused search string.\n\n"
        "**Saved ETF news** ('saved news', 'news sentiment for gold'):\n"
        "  1. Call `get_db_news(category='gold', sentiment='')` to query ClickHouse.\n\n"
        "**ETF category scan** ('latest etf news', 'etf news sentiment'):\n"
        "  1. Call `run_etf_news_sentiment` for a full multi-category scan.\n\n"
        "**Price anomaly explanation** ('explain anomalies for GOLDBEES', 'why did the price spike/drop'):\n"
        "  1. ETFs/gold: Call `explain_price_anomalies(symbol)` + `plot_price_chart(symbol)` in parallel.\n"
        "  2. Stocks: Call `search_anomaly_events(symbol)` + `plot_price_chart(symbol)` in parallel.\n\n"
        "**PDF export** (only when user says 'save as PDF', 'publish report', 'export PDF'):\n"
        "  1. Call `publish_consolidated_pdf(report_markdown=<full_output>)`. "
        "Auto-detects symbols and charts. Report the saved file path.\n\n"
        "## Output format\n"
        "Always present results as a Markdown table:\n"
        "| Title | Source | Date | Sentiment |\n\n"
        "After the table, write 2-3 sentences summarising:\n"
        "- Dominant sentiment (bullish / bearish / mixed)\n"
        "- Key themes or events driving the news\n"
        "- Any actionable observation (e.g. 'FII selling pressure visible in 3 of 5 articles')\n\n"
        "## Rules\n"
        "- Never invent headlines — only report what the tools return.\n"
        "- If both GNews and NewsAPI return results, merge and deduplicate by title similarity.\n"
        "- Truncate long titles to 80 characters in the table."
    )

    def _get_tools(self) -> list:
        from src.tools.company_resolver import resolve_company
        from src.tools.news_search import get_stock_news, search_financial_news, get_db_news
        from src.tools.newsapi_search import get_newsapi_stock_news
        from src.tools.skills_tools import run_etf_news_sentiment, explain_price_anomalies
        from src.tools.chart_tools import plot_price_chart
        from src.tools.market.equity import search_anomaly_events
        from src.tools.report_publisher import publish_research_pdf, publish_consolidated_pdf
        return [
            resolve_company,
            get_stock_news,
            get_newsapi_stock_news,
            search_financial_news,
            get_db_news,
            run_etf_news_sentiment,
            explain_price_anomalies,
            search_anomaly_events,
            plot_price_chart,
            publish_research_pdf,
            publish_consolidated_pdf,
        ]


# ── Database sub-agent ────────────────────────────────────────────────────────

class DatabaseSubAgent(_SubAgent):
    """
    Natural-language → SQL agent for the market_data ClickHouse database.

    Workflow
    --------
    1. list_db_tables()          — discover available tables
    2. describe_db_table(name)   — confirm column names before querying
    3. execute_db_query(sql)     — run SELECT and return markdown results
    4. sample_db_table(name)     — inspect raw data shape
    5. get_db_watermarks()       — check data freshness

    Always adds FINAL to every ReplacingMergeTree table.
    Never modifies data — read-only queries only.
    """

    SYSTEM_PROMPT = (
        "You are the Mosaic Database Agent — an expert in querying the market_data "
        "ClickHouse database for the Mosaic fund platform.\n\n"

        "## Workflow\n"
        "1. If you don't know the table structure, call `describe_db_table` first.\n"
        "2. Write a precise SQL query and call `execute_db_query`.\n"
        "3. Present results as Markdown tables. Explain key findings in 2-3 sentences.\n"
        "4. For freshness checks, call `get_db_watermarks`.\n\n"

        "## ClickHouse rules (CRITICAL)\n"
        "- Always add `FINAL` to every table — tables use ReplacingMergeTree:\n"
        "    SELECT ... FROM market_data.mf_holdings FINAL WHERE ...\n"
        "- Date literals: `toDate('2026-01-15')` not '2026-01-15'\n"
        "- Last N days: `trade_date >= today() - 30`\n"
        "- String comparison is case-sensitive: use exact values\n"
        "- Only SELECT/SHOW/DESCRIBE/EXPLAIN/WITH — no INSERT/UPDATE/DELETE\n\n"

        "## Key tables and columns\n"
        "| table | key columns |\n|---|---|\n"
        "| daily_prices | symbol, category('etfs'/'stocks'), trade_date, close |\n"
        "| mf_holdings | scheme_code, fund_name, as_of_month, security_name, pct_of_nav, market_value_cr |\n"
        "| mf_nav | scheme_code, nav_date, nav |\n"
        "| fii_dii_flows | trade_date, fii_net_cr, dii_net_cr |\n"
        "| fii_dii_fno_daily | trade_date, fii_fut_net_oi, fii_opt_call_net_oi, fii_opt_put_net_oi |\n"
        "| signal_composite | as_of, etf_symbol, composite_score, action |\n"
        "| ml_predictions | as_of, expected_return_pct, regime_signal, cv_r2_mean |\n"
        "| weight_checkpoints | as_of, symbol, method, recommended_weight, garch_vol_pct |\n"
        "| inav_snapshots | symbol, snapshot_at, inav, market_price, premium_discount_pct |\n"
        "| cot_gold | report_date, mm_long, mm_short, mm_net, open_interest |\n"
        "| fx_rates | trade_date, symbol('USDINR=X' etc.), close |\n"
        "| macro_indicators | ref_year, country_code, indicator_code, value |\n"
        "| news_articles | fetched_at, category, sentiment, impact_tier, title |\n"
        "| import_watermarks | source, symbol, last_date |\n"
        "| deepdive_financials | ticker, report_date, revenue_usd_m, net_income_usd_m, free_cash_flow_usd_m |\n"
        "| deepdive_valuation | ticker, report_date, pe_trailing, ev_ebitda, fcf_yield_pct |\n\n"

        "## Common patterns\n"
        "```sql\n"
        "-- Latest GOLDBEES price\n"
        "SELECT trade_date, close FROM market_data.daily_prices FINAL\n"
        "WHERE symbol='GOLDBEES' AND category='etfs' ORDER BY trade_date DESC LIMIT 5\n\n"
        "-- DSP fund holdings for a stock\n"
        "SELECT fund_name, as_of_month, pct_of_nav, market_value_cr\n"
        "FROM market_data.mf_holdings FINAL\n"
        "WHERE security_name ILIKE '%Reliance%' ORDER BY as_of_month DESC LIMIT 10\n\n"
        "-- FII net flows last 10 days\n"
        "SELECT trade_date, fii_net_cr, dii_net_cr\n"
        "FROM market_data.fii_dii_flows FINAL\n"
        "ORDER BY trade_date DESC LIMIT 10\n"
        "```\n\n"
        "Never invent numbers — always run the query and report the output.\n\n"
        "## Charts\n"
        "After returning query results, offer to visualise the data:\n"
        "- Time-series price data → `plot_price_chart(symbol, days)`\n"
        "- Multi-symbol comparison → `plot_multi_price_chart('SYM1,SYM2', days)`\n"
        "- FII/DII flows → `plot_fii_dii_chart(days)`\n"
        "- Signal scores → `plot_signal_scores()`\n"
        "- MF NAV trend → `plot_nav_chart(scheme_code, days)`\n"
        "Always call the chart tool when the user asks to 'plot', 'chart', 'show trend', "
        "or 'visualise' data."
    )

    def _get_tools(self) -> list:
        from src.tools.db_tools import DB_TOOLS
        from src.tools.chart_tools import CHART_TOOLS
        return DB_TOOLS + CHART_TOOLS


# ── Code sub-agent ────────────────────────────────────────────────────────────

class CodeSubAgent(_SubAgent):
    """
    Python code execution and project scripting agent.

    Capabilities
    ------------
    - Execute ad-hoc Python snippets against live ClickHouse data
    - Write new analysis scripts to src/scripts/
    - Read existing project files for context
    - Search the codebase for patterns / symbols
    - Run any existing script by path

    The agent is restricted from writing outside src/scripts/ and output/ to
    prevent accidental modification of core agent/importer code.
    """

    SYSTEM_PROMPT = (
        "You are the Mosaic Code Agent — a Python expert for Indian equity/commodity "
        "quantitative analysis.  You can write, execute, and debug Python code against "
        "the live Mosaic platform and its ClickHouse database.\n\n"
        "## Workflow\n"
        "1. To answer a data question: use `execute_python_snippet` — write pandas/numpy "
        "code that queries ClickHouse via `query_df(sql)` and prints results.\n"
        "2. To create a reusable script: use `write_project_file` (target: src/scripts/<domain>/<name>.py) "
        "then `run_existing_script` to validate it.\n"
        "3. To understand existing code: use `read_project_file` or `search_project_code`.\n"
        "4. To run an existing script: use `run_existing_script`.\n\n"
        "## ClickHouse rules\n"
        "- Always add FINAL to ReplacingMergeTree tables:\n"
        "  `SELECT ... FROM market_data.mf_holdings FINAL WHERE ...`\n"
        "- Available tables: daily_prices, mf_holdings, mf_nav, fii_dii_flows, "
        "signal_composite, ml_predictions, macro_indicators, fx_rates, inav_snapshots.\n"
        "- `query_df(sql)` returns a pandas DataFrame; use `.to_markdown(index=False)` to print.\n\n"
        "## Project conventions\n"
        "- New signal sources go in src/agents/signal_sources.py — subclass SignalSource ABC.\n"
        "- New fetcher adapters go in src/importer/fetchers/adapters.py — subclass Fetcher ABC.\n"
        "- New standalone scripts go in src/scripts/<domain>/.\n"
        "- Never modify src/agents/mosaic_fund_agent.py or src/importer/clickhouse.py directly.\n\n"
        "## Charts and Visualisation\n"
        "- Use predefined chart functions (like `plot_price_chart`, `plot_fii_dii_chart`, etc.) when available.\n"
        "- If a specific chart function does not exist or does not cover the required data, write Python code at run time to fetch the data from ClickHouse and build the chart using `plotext` (or fallback) and execute it using `execute_python_snippet` to output the chart trend.\n\n"
        "## Output rules\n"
        "- Never compute numbers in your text — always execute code and report printed output.\n"
        "- Format all data as Markdown tables.\n"
        "- If code fails, read the STDERR, diagnose the root cause, fix, and re-execute."
    )

    def _build(self, llm_override: Any = None) -> None:
        """Use the code-specific LLM (CODE_LLM_PROVIDER) when configured."""
        if llm_override is None:
            try:
                from src.agents.mosaic_fund_agent import MosaicFundAgent
                tmp = object.__new__(MosaicFundAgent)
                tmp._checkpointer = None
                code_llm = tmp._build_code_llm()
                if code_llm is not None:
                    from config.settings import settings
                    logger.info(
                        "CodeSubAgent: using dedicated LLM  provider=%s  model=%s",
                        settings.code_llm_provider, settings.code_llm_model,
                    )
                    llm_override = code_llm
            except Exception as exc:
                logger.warning("CodeSubAgent: could not build code LLM: %s", exc)
        super()._build(llm_override=llm_override)

    def _get_tools(self) -> list:
        from src.tools.code_tools import CODE_TOOLS
        from src.tools.skills_tools import query_clickhouse_db
        from src.tools.chart_tools import CHART_TOOLS
        return CODE_TOOLS + [query_clickhouse_db] + CHART_TOOLS


# ── Autonomous Research Agent ─────────────────────────────────────────────────

class AutonomousResearchAgent(_SubAgent):
    """
    Self-directed, multi-domain research agent.

    Combines: fundamental data, ML / GARCH volatility, macro intelligence,
    news with agent-chosen date windows, MF holding pattern analysis,
    institutional flows, ClickHouse queries, and custom Python execution.
    """

    # 10-layer framework + optional delegation calls + synthesis
    RECURSION_LIMIT = 50

    SYSTEM_PROMPT = """\
You are the Mosaic Autonomous Research Agent — a self-directed, multi-domain analyst
for Indian equity and commodity markets.

You have access to every capability: fundamentals, ML price prediction, GARCH volatility,
macro/geopolitical intelligence, news retrieval with flexible date windows, mutual-fund
holding pattern analysis, institutional flows, ClickHouse SQL, and custom Python execution.

## Research Framework
Work through these layers in order, skipping only what is genuinely irrelevant:

0. **Data availability** — For any symbol the user explicitly names for price analysis,
   call `check_and_refresh_symbol_data(symbol)` ONCE before running momentum, correlation,
   or GARCH tools. Parse the result prefix and act accordingly:
   - `FRESH` / `REFRESHED` / `UNCHANGED` → proceed normally
   - `IMPORT_FAILED` → proceed and note data staleness in the report
   - `UNKNOWN_SYMBOL` → skip the import; use `get_yahoo_finance_data` for price data
   Do NOT call for every ETF in a broad scan — only for the 1–3 primary symbols the
   user explicitly named.
   - Import tools reuse the user's saved data source for 24 hours. If a tool returns `DATA_SOURCE_REQUIRED`, ask the user to choose: 1. Shoonya, 2. NSE, or 3. yfinance, then retry with `data_source`. Never choose for the user.
   - If the user names a SPECIFIC symbol (e.g. 'import ADVENZYMES'), call `import_symbol_data(symbol, data_source=...)` instead of `run_data_engineering_importer`. When the user specifies a particular year (e.g. '2019'), date, or month range, parse the dates and pass them as `start_date` (format YYYY-MM-DD) and `end_date` (format YYYY-MM-DD) parameters to `import_symbol_data` and `plot_price_chart` (e.g. for year 2019, `start_date='2019-01-01'` and `end_date='2019-12-31'`).
   - Only call `run_data_engineering_importer(category='stocks', data_source=...)` when the user asks to import ALL stocks generically without naming a specific one.

1. **Entity resolution** — Call `resolve_company(query)` to get the NSE/BSE ticker, exchange, and full name. Note that company symbols can change, demerge, or be newly listed; always rely on `resolve_company` rather than hardcoding symbols, and check if its output contains an "error" field before running further tools.
2. **Price & Momentum** — `get_yahoo_finance_data` (P/E, 52w range, market cap);
   `get_price_momentum` (30d/90d returns, momentum signal); `plot_price_chart`
3. **Fundamentals** — `get_quarterly_results` (revenue, EPS, YoY growth);
   `get_stock_cashflow` (FCF, capex, operating CF)
4. **Institutional footprint** — `get_mf_holdings_for_stock` (DSP fund cross-ownership,
   trend across months); `get_fii_dii_summary` (net FII/DII flows); `plot_fii_dii_chart`
5. **Macro & sector context** — `run_macro_scanner` (active themes, ETF impact);
   `run_daily_signal_composite` for ETF sector positioning
6. **News intelligence** — YOU decide the timeframe based on query intent:
   - Recent results/event: `get_stock_news` or `get_newsapi_stock_news` → last 7–14 days
   - Sector/structural trend: `search_financial_news(query, max_results=10)` with a 90-day context
   - Historical investigation: `search_financial_news("COMPANY 2023")` for year-level patterns
   - Saved articles: `get_db_news(category, sentiment)` for tagged ETF/sector articles
   - **Price anomaly investigation** — `search_anomaly_events(symbol, days=90)`:
     detects the SAME red-dot anomaly dates shown on the price chart (GARCH + IF + PELT),
     suppresses corporate action ex-dates automatically, then runs parallel Google News
     searches per flagged date. Call whenever the user asks "what caused the spike/crash/
     anomaly on the chart". Always call `plot_price_chart(symbol)` in parallel.
   - **Corporate actions** — `get_corporate_actions(symbol)`: fetches NSE corporate actions
     (splits, bonuses, demergers, rights, dividends), stores them in ClickHouse, and returns
     a history table. Call when the user asks about stock splits, bonus issues, demergers,
     or when a chart shows an extreme return (>20%) that may be mechanical.
   - For a thorough multi-source sweep: `delegate_to_news_agent(question)`
7. **Volatility & signals** — `run_risk_governor_analysis` (GARCH vol, regime, position sizing);
   `plot_garch_volatility_chart`

7b. **Expert delegation** — When a research layer requires a specialised pipeline that
   produces materially better output than calling tools directly, delegate:
   - `delegate_to_signal_agent(q)` — GOLDBEES ML pipeline (prob_up, expected_return_pct,
     regime_signal, blended_50), composite ETF scores, Kelly weights, risk governor.
     Use when the user explicitly asks for today's ETF signal or GOLDBEES recommendation.
   - `delegate_to_macro_agent(q)` — COMEX pre-market commodities, full FII/DII flow
     analysis, COT positioning, geopolitical themes mapped to ETF impact scores.
     Use when macro context needs more than `run_macro_scanner` alone.
   - `delegate_to_intl_etf_agent(q)` — scarcity premium/discount, KMeans regime,
     monthly seasonality, drawdown episodes, LightGBM feature importance for the 6
     intl ETFs (MAFANG, HNGSNGBEES, MON100, MASPTOP50, MAHKTECH, MONQ50).
     Use whenever the research involves these ETFs beyond simple price data.
   - `delegate_to_news_agent(q)` — GNews + NewsAPI + ClickHouse news with sentiment
     for a specific company or ETF — for a thorough multi-source sweep.
   - `delegate_to_india_equity_agent(q)` — full 8-section stock research note
     (Yahoo + Screener + MF holdings + news + FII). Use when this agent is doing
     multi-asset work and needs a complete equity sub-report on one name.

   Delegation rules:
   - Pass the complete question with all context — the sub-agent starts fresh.
   - Do NOT delegate if you already called the underlying tools directly for the same
     question (avoid duplicate work).
   - Delegation is always optional — use it when the sub-agent's specialised toolset
     will produce a better result than what you can do with your own tools.

8. **Correlation & custom ML** — use `execute_python_snippet` to:
   - Compute rolling pairwise correlations:
     `df = query_df("SELECT trade_date, symbol, close FROM market_data.daily_prices FINAL ...")`
     then `df.pivot(...).pct_change().rolling(60).corr()`
   - Run LightGBM or custom GARCH on price series pulled from ClickHouse
   - Find instruments co-moving with the target: SQL JOIN + pandas correlation
   - `get_intl_etf_correlation` for intl ETF / USDINR sensitivity
9. **Visualise** — pair each data layer with a chart where it adds clarity:
   - **Always** call `plot_price_chart(symbol, days=365)` AND `plot_macd_chart(symbol, days=180)` for every deep dive — price trend + MACD(12,26,9) momentum are mandatory outputs.
   - Also call `plot_garch_volatility_chart(symbol)` when GARCH vol data is available.
   - Use `plot_fii_dii_chart`, `plot_fund_holdings_chart`, `plot_multi_price_chart` where relevant.
   - If a specific chart function does not exist or does not cover the required data, write Python code at run time to fetch the data from ClickHouse and build the chart using `plotext` (or fallback) and execute it using `execute_python_snippet` to output the chart trend.
10. **Synthesise** — write a structured Markdown research report
11. **Publish (on demand only)** — call `publish_consolidated_pdf(report_markdown=<full_report>)` ONLY when the user explicitly asks to save, export, or publish as PDF. Do NOT call this automatically after every research run.

## ClickHouse rules (critical)
- Always add `FINAL` after table name: `SELECT ... FROM market_data.daily_prices FINAL`
- MF holdings columns: `pct_of_nav`, `security_name` (NEVER `weight_pct` or `name`)
- Available tables: `daily_prices`, `mf_holdings`, `mf_nav`, `fii_dii_flows`,
  `signal_composite`, `ml_predictions`, `macro_indicators`, `fx_rates`,
  `inav_snapshots`, `news_articles`, `import_watermarks`
- In `execute_python_snippet`: `query_df(sql)` → pandas DataFrame; use
  `.to_markdown(index=False)` to display

## Arithmetic rule
Never compute any number in your response text. All returns, ratios, scores, and
aggregations must be computed by Python or SQL, then narrated.

## Output format
```
### Research: <Company / Topic>
#### 1. Snapshot
#### 2. Fundamentals
#### 3. Institutional Footprint
#### 4. Macro & Sector Context
#### 5. News Intelligence
#### 6. Quant Signals & Volatility
#### 7. Correlations
#### 8. Thesis & Risks
```
"""

    def _get_tools(self) -> list:
        from src.tools.company_resolver import resolve_company
        from src.tools.yahoo_finance import YAHOO_TOOLS
        from src.tools.earnings_scraper import get_quarterly_results
        from src.tools.indian_equity_tools import INDIAN_EQUITY_TOOLS
        from src.tools.skills_tools import (
            query_clickhouse_db,
            run_macro_scanner,
            run_daily_signal_composite,
            run_risk_governor_analysis,
        )
        from src.tools.market_context import get_dxy_context
        from src.tools.news_search import search_financial_news, get_stock_news, get_db_news
        from src.tools.newsapi_search import get_newsapi_stock_news
        from src.tools.intl_etf_tools import get_intl_etf_correlation, get_intl_etf_performance
        from src.tools.code_tools import execute_python_snippet, install_python_dependency
        from src.tools.chart_tools import (
            plot_price_chart,
            plot_multi_price_chart,
            plot_fii_dii_chart,
            plot_fund_holdings_chart,
            plot_garch_volatility_chart,
            plot_macd_chart,
        )
        from src.tools.agent_tools import AGENT_TOOLS
        from src.tools.report_publisher import publish_research_pdf, publish_consolidated_pdf
        return [
            resolve_company,
            *YAHOO_TOOLS,
            get_quarterly_results,
            *INDIAN_EQUITY_TOOLS,
            query_clickhouse_db,
            execute_python_snippet,
            install_python_dependency,
            run_macro_scanner,
            run_daily_signal_composite,
            run_risk_governor_analysis,
            get_dxy_context,
            search_financial_news,
            get_stock_news,
            get_newsapi_stock_news,
            get_db_news,
            get_intl_etf_correlation,
            get_intl_etf_performance,
            plot_price_chart,
            plot_multi_price_chart,
            plot_fii_dii_chart,
            plot_fund_holdings_chart,
            plot_garch_volatility_chart,
            plot_macd_chart,
            publish_research_pdf,
            publish_consolidated_pdf,
            *AGENT_TOOLS,  # check_and_refresh_symbol_data + 5 delegation tools
        ]


# ── Registry ──────────────────────────────────────────────────────────────────

_registry: dict[str, _SubAgent] = {}


def get_subagent(name: str) -> _SubAgent:
    """Return (lazily creating) a sub-agent by name."""
    if name not in _registry:
        cls_map: dict[str, type[_SubAgent]] = {
            "deepdive":     DeepDiveSubAgent,
            "research":     AutonomousResearchAgent,
            "india_equity": IndianEquityResearchSubAgent,
            "signal":       SignalSubAgent,
            "macro":        MacroSubAgent,
            "intl_etf":     IntlETFSubAgent,
            "news":         NewsSubAgent,
            "code":         CodeSubAgent,
            "database":     DatabaseSubAgent,
        }
        cls = cls_map.get(name)
        if cls is None:
            raise ValueError(f"Unknown sub-agent: {name!r}  (valid: {list(cls_map)})")
        _registry[name] = cls()
    return _registry[name]


def run_subagent_for(intent: str, question: str, callbacks: list | None = None) -> str:
    """Run a named sub-agent, automatically routing to cloud LLM when needed.

    Parameters
    ----------
    callbacks:
        Pass [RichConsoleCallbackHandler()] to see live tool-call output.
        TracingCallbackHandler is always appended for observability.
    """
    # Fix common indicator typos before the sub-agent LLM sees the query
    question = _fix_indicator_typos(question)
    import os
    from src.agents.tracer import TracingCallbackHandler, log_trace
    from src.agents.budget import BudgetCallbackHandler
    import time

    cloud_llm = None
    if _needs_cloud(question):
        try:
            from src.agents.mosaic_fund_agent import MosaicFundAgent
            tmp = object.__new__(MosaicFundAgent)
            tmp._checkpointer = None
            cloud_llm = tmp._build_cloud_llm()
            if cloud_llm is not None:
                logger.info("run_subagent_for: using cloud LLM for %r", question[:60])
        except Exception as exc:
            logger.warning("run_subagent_for: could not build cloud LLM: %s", exc)

    # Build callback list with tracing and budget always enabled
    tracer = TracingCallbackHandler(agent=intent)
    budget = BudgetCallbackHandler()
    if callbacks is None:
        callbacks = []
        if os.getenv("VERBOSE") == "1":
            from src.agents.mosaic_fund_agent import RichConsoleCallbackHandler
            callbacks.append(RichConsoleCallbackHandler())
    callbacks.extend([tracer, budget])

    # Log the routing decision itself
    log_trace(
        agent="router",
        run_id=tracer.run_id,
        tool_name="route_intent",
        args_json=f'{{"question": "{question[:200]}", "intent": "{intent}"}}',
        status="ok",
    )

    start = time.monotonic()
    result = get_subagent(intent).run(question, llm_override=cloud_llm, callbacks=callbacks)
    elapsed_ms = int((time.monotonic() - start) * 1000)

    # Log completion
    log_trace(
        agent=intent,
        run_id=tracer.run_id,
        tool_name="_complete",
        latency_ms=elapsed_ms,
        result_json=result[:500] if result else "",
        status="ok",
    )

    return result
