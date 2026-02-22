"""
src/agents/news_sentiment_agent.py
────────────────────────────────────
Deep Agents-powered news sentiment analysis agent.

Built with create_deep_agent (github.com/langchain-ai/deepagents) — a
batteries-included LangGraph agent harness with built-in planning, filesystem
access, and sub-agent delegation.

This agent:
  1. Fetches news from NewsAPI.org  (premium Indian financial publications)
  2. Fetches news from Google News  (via GNews RSS — no API key required)
  3. Deduplicates articles across both sources
  4. Scores per-article sentiment with keyword heuristics
  5. Computes aggregate metrics: score (-1→+1), counts, top headlines
  6. Returns a structured NewsSentimentReport

Usage (CLI):
    python src/main.py news RELIANCE
    python src/main.py news RELIANCE --company "Reliance Industries"

Usage (API):
    from src.agents.news_sentiment_agent import NewsSentimentAgent
    agent = NewsSentimentAgent()
    report = agent.run("RELIANCE", "Reliance Industries")
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any

from langchain_core.tools import tool

from config.settings import settings
from src.models.portfolio import Sentiment
from src.tools.news_search import _infer_sentiment, fetch_news_for_symbol, get_stock_news
from src.tools.newsapi_search import fetch_newsapi_articles, get_newsapi_stock_news

logger = logging.getLogger(__name__)

# ── System prompt ──────────────────────────────────────────────────────────────

NEWS_SENTIMENT_SYSTEM_PROMPT = """\
You are a financial news sentiment analyst for Indian equity markets (NSE/BSE).

CRITICAL INSTRUCTION: Call collate_news_sentiment EXACTLY ONCE.
Immediately after receiving the result, return the full JSON as your final answer.
Do NOT call any tool a second time. Do NOT loop.

WORKFLOW (one pass only):
  Step 1: Call collate_news_sentiment with "SYMBOL|Company Name" input.
          This fetches both NewsAPI and GNews, deduplicates, and scores sentiment.
  Step 2: Immediately return the full JSON from that single call as your final answer.

Do not call get_newsapi_stock_news or get_stock_news — collate_news_sentiment
already does both. STOP after Step 2.
"""


# ── Core Collation Tool ────────────────────────────────────────────────────────

@tool
def collate_news_sentiment(input_str: str) -> dict[str, Any]:
    """
    Fetch news from BOTH NewsAPI.org and Google News, deduplicate articles,
    and compute a comprehensive multi-source sentiment report.

    This is the primary tool for complete news sentiment analysis.

    Input format: "SYMBOL" or "SYMBOL|Company Full Name"
    Examples:
      "RELIANCE"                    → fetches and colates RELIANCE news
      "RELIANCE|Reliance Industries" → uses company name for richer queries

    Returns:
      symbol, total_articles, newsapi_count, gnews_count,
      deduplicated_count, overall_sentiment (POSITIVE/NEUTRAL/NEGATIVE),
      sentiment_score (-1.0 to +1.0), positive/negative/neutral counts,
      sentiment_breakdown (percentages), top_positive_headlines,
      top_negative_headlines, source_breakdown, articles (full list).
    """
    parts = input_str.strip().split("|")
    symbol = parts[0].strip().upper()
    company_name = parts[1].strip() if len(parts) > 1 else ""

    # ── Fetch from both sources ───────────────────────────────────────────────
    newsapi_items = fetch_newsapi_articles(symbol, company_name)
    gnews_items = fetch_news_for_symbol(symbol, company_name)

    # Convert to plain dicts and tag source
    def _to_dict(item: Any, source_tag: str) -> dict:
        return {
            "title": item.title,
            "source": item.source,
            "published_at": item.published_at,
            "url": item.url,
            "description": item.description,
            "sentiment": item.sentiment.value,
            "_source_tag": source_tag,
        }

    newsapi_dicts = [_to_dict(i, "NewsAPI") for i in newsapi_items]
    gnews_dicts = [_to_dict(i, "GNews") for i in gnews_items]

    # ── Deduplicate by normalised title ───────────────────────────────────────
    def _norm(title: str) -> str:
        return "".join(c for c in title.lower() if c.isalnum() or c.isspace()).strip()

    seen: set[str] = set()
    all_articles: list[dict] = []

    for art in newsapi_dicts:
        nt = _norm(art.get("title", ""))
        if nt and nt not in seen:
            seen.add(nt)
            all_articles.append(art)

    for art in gnews_dicts:
        nt = _norm(art.get("title", ""))
        if nt and nt not in seen:
            seen.add(nt)
            all_articles.append(art)

    if not all_articles:
        return {
            "symbol": symbol,
            "total_articles": 0,
            "newsapi_count": 0,
            "gnews_count": 0,
            "deduplicated_count": 0,
            "overall_sentiment": "NEUTRAL",
            "sentiment_score": 0.0,
            "positive_count": 0,
            "negative_count": 0,
            "neutral_count": 0,
            "sentiment_breakdown": {"positive_pct": 0, "negative_pct": 0, "neutral_pct": 0},
            "top_positive_headlines": [],
            "top_negative_headlines": [],
            "source_breakdown": {"newsapi": 0, "gnews": 0},
            "articles": [],
            "note": "No articles found from either source.",
        }

    # ── Re-score sentiment for each deduped article ───────────────────────────
    pos, neg, neu = 0, 0, 0
    pos_headlines: list[str] = []
    neg_headlines: list[str] = []

    for art in all_articles:
        title = art.get("title", "")
        description = art.get("description", "") or ""
        # Re-run the heuristic on full text; overwrite pre-computed value
        scored = _infer_sentiment(f"{title} {description}").value
        art["sentiment"] = scored
        if scored == Sentiment.POSITIVE.value:
            pos += 1
            pos_headlines.append(title)
        elif scored == Sentiment.NEGATIVE.value:
            neg += 1
            neg_headlines.append(title)
        else:
            neu += 1

    total = len(all_articles)
    newsapi_final = sum(1 for a in all_articles if a.get("_source_tag") == "NewsAPI")
    gnews_final = sum(1 for a in all_articles if a.get("_source_tag") == "GNews")

    # Sentiment score: +1.0 = all positive, 0 = balanced, -1.0 = all negative
    sentiment_score = round((pos - neg) / total, 3) if total else 0.0

    if pos > neg:
        overall = "POSITIVE"
    elif neg > pos:
        overall = "NEGATIVE"
    else:
        overall = "NEUTRAL"

    return {
        "symbol": symbol,
        "total_articles": total,
        "newsapi_count": newsapi_final,
        "gnews_count": gnews_final,
        "deduplicated_count": total,
        "overall_sentiment": overall,
        "sentiment_score": sentiment_score,
        "positive_count": pos,
        "negative_count": neg,
        "neutral_count": neu,
        "sentiment_breakdown": {
            "positive_pct": round(pos / total * 100, 1),
            "negative_pct": round(neg / total * 100, 1),
            "neutral_pct": round(neu / total * 100, 1),
        },
        "top_positive_headlines": pos_headlines[:3],
        "top_negative_headlines": neg_headlines[:3],
        "source_breakdown": {"newsapi": newsapi_final, "gnews": gnews_final},
        "articles": all_articles,
    }


# All tools this agent registers
SENTIMENT_TOOLS = [collate_news_sentiment, get_newsapi_stock_news, get_stock_news]


# ── Agent class ────────────────────────────────────────────────────────────────

class NewsSentimentAgent:
    """
    News sentiment analysis agent.

    Local model  → _run_direct() immediately (no LangGraph, no loop risk).
    Cloud model  → LangGraph deep-agent with recursion_limit + 1-call instruction.
    """

    def __init__(self) -> None:
        self._is_local = settings.is_local_model
        if self._is_local:
            logger.info(
                "NewsSentimentAgent — LOCAL DIRECT mode | model: %s @ %s "
                "(LangGraph skipped to prevent tool-call loop)",
                settings.llm_model,
                settings.llm_base_url,
            )
            self._llm = None
            self._agent = None
        else:
            logger.info(
                "NewsSentimentAgent — CLOUD REACT mode | provider: %s | model: %s",
                settings.llm_provider,
                settings.llm_model,
            )
            self._llm = self._build_llm()
            self._agent = self._build_deep_agent()

    # ── LLM builder ───────────────────────────────────────────────────────────

    def _build_llm(self) -> Any:
        """
        Build LLM with same priority as PortfolioAgent:
          1. Local OpenAI-compatible server (LLM_BASE_URL)
          2. Anthropic cloud
          3. OpenAI cloud
        """
        if settings.llm_base_url:
            from langchain_openai import ChatOpenAI
            logger.info(
                "NewsSentimentAgent — local LLM: %s @ %s",
                settings.llm_model,
                settings.llm_base_url,
            )
            return ChatOpenAI(
                model=settings.llm_model,
                base_url=settings.llm_base_url,
                api_key=settings.openai_api_key or "local",
                temperature=0.1,
                max_tokens=settings.llm_token_budget,
            )

        if settings.llm_provider.lower() == "anthropic":
            from langchain_anthropic import ChatAnthropic
            return ChatAnthropic(
                model=settings.llm_model,
                api_key=settings.anthropic_api_key,
                temperature=0.1,
                max_tokens=settings.llm_token_budget,
            )

        from langchain_openai import ChatOpenAI
        return ChatOpenAI(
            model=settings.llm_model,
            api_key=settings.openai_api_key,
            temperature=0.1,
            max_tokens=settings.llm_token_budget,
        )

    # ── Agent builder ─────────────────────────────────────────────────────────

    def _build_deep_agent(self) -> Any:
        """
        Build the Deep Agent using create_deep_agent from deepagents.
        Falls back to None if deepagents is not installed.
        """
        try:
            from deepagents import create_deep_agent  # type: ignore[import]
        except ImportError:
            logger.warning(
                "deepagents not installed — NewsSentimentAgent will use direct mode. "
                "Install with: pip install deepagents"
            )
            return None

        try:
            return create_deep_agent(
                model=self._llm,
                tools=SENTIMENT_TOOLS,
                system_prompt=NEWS_SENTIMENT_SYSTEM_PROMPT,
            )
        except Exception as exc:
            logger.warning("create_deep_agent failed (%s) — using direct mode.", exc)
            return None

    # ── Public API ─────────────────────────────────────────────────────────────

    def run(self, symbol: str, company_name: str = "") -> dict[str, Any]:
        """
        Run multi-source news sentiment analysis for a stock symbol.

        When the deep agent is available it will:
          - Plan the analysis using deepagents' write_todos tool
          - Call collate_news_sentiment (or the individual source tools)
          - Return the structured JSON report

        Falls back to _run_direct() when the deep agent is unavailable or fails.

        Args:
            symbol:       NSE/BSE trading symbol, e.g. "RELIANCE"
            company_name: Optional full company name for richer queries.

        Returns:
            Structured sentiment report dict from collate_news_sentiment.
        """
        if self._agent is None:
            logger.info("Deep agent unavailable — running direct collation.")
            return self._run_direct(symbol, company_name)

        # ── LOCAL: bypass LangGraph entirely ─────────────────────────────────────
        if self._is_local:
            logger.info(
                "NewsSentimentAgent.run() — local model, running direct "
                "(no LLM tokens consumed for news collation)"
            )
            return self._run_direct(symbol, company_name)

        query = f"{symbol}|{company_name}" if company_name else symbol
        user_message = (
            f"Analyze news sentiment for {symbol}"
            + (f" ({company_name})" if company_name else "")
            + ". Call collate_news_sentiment once with input "
            f'"{query}" and immediately return the full JSON result. '
            "Do not call any tool more than once."
        )
        logger.debug(
            "NewsSentimentAgent.run() starting — symbol=%s query=%s", symbol, query
        )

        try:
            result = self._agent.invoke(
                {"messages": [{"role": "user", "content": user_message}]},
                config={"recursion_limit": 6},   # hard cap on LangGraph steps
            )
            msgs = result.get("messages", [])
            logger.debug(
                "NewsSentimentAgent agent returned %d messages.", len(msgs)
            )
            last_content = msgs[-1].content if msgs else ""

            # Extract JSON block from the agent's final message
            json_match = re.search(r"\{.*\}", last_content, re.DOTALL)
            if json_match:
                try:
                    parsed = json.loads(json_match.group())
                    if "symbol" in parsed and "total_articles" in parsed:
                        logger.debug(
                            "NewsSentimentAgent returned valid report "
                            "(articles=%d, score=%.3f).",
                            parsed.get("total_articles", 0),
                            parsed.get("sentiment_score", 0.0),
                        )
                        return parsed
                except (json.JSONDecodeError, ValueError):
                    pass

            logger.info(
                "NewsSentimentAgent response did not contain parseable JSON — "
                "falling back to direct collation."
            )
        except Exception as exc:
            logger.warning(
                "NewsSentimentAgent invocation failed (%s) — using direct mode.", exc
            )

        # Fallback: run collation tool directly without the LLM agent
        return self._run_direct(symbol, company_name)

    def _run_direct(self, symbol: str, company_name: str = "") -> dict[str, Any]:
        """
        Bypass the LLM agent and invoke the collate_news_sentiment tool directly.
        Guaranteed to return a result regardless of LLM availability.
        """
        query = f"{symbol}|{company_name}" if company_name else symbol
        return collate_news_sentiment.invoke(query)
