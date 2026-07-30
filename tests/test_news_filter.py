"""
tests/test_news_filter.py
──────────────────────────
Unit tests for src/tools/news_filter.py

Six scenarios:
  1. Symbol match  — keep on-symbol article, drop unrelated article.
  2. Commodity match — keep gold article for GOLDBEES, drop IT noise.
  3. Sentence extraction — only matching sentence survives description trim.
  4. spaCy-absent fallback — filter still works via regex when _NLP_AVAILABLE=False.
  5. No-starvation safety — all-drop returns originals with starvation_fallback=True.
  6. Mistral/Ollama Stage C rescue — articles failed regex but answered YES by LLM
     are rescued; articles answered NO by LLM are dropped (mocked HTTP).
"""

from __future__ import annotations

import pytest

from src.models.portfolio import NewsItem, Sentiment


# ── Fixtures ──────────────────────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def disable_stage_c_llm_filter(monkeypatch):
    """Disable Stage C LLM fail-open filter by default for deterministic unit tests."""
    from config.settings import settings

    monkeypatch.setattr(settings, "news_filter_llm_enabled", False, raising=False)


# ── Helper ────────────────────────────────────────────────────────────────────


def _item(title: str, description: str = "") -> NewsItem:
    """Construct a minimal NewsItem for testing."""
    return NewsItem(title=title, description=description, sentiment=Sentiment.NEUTRAL)


# ── Test 1: symbol match ──────────────────────────────────────────────────────


def test_symbol_match_keeps_and_drops():
    """Article containing the stock symbol is kept; unrelated article is dropped."""
    from src.tools.news_filter import build_match_vocab, filter_articles

    vocab = build_match_vocab("RELIANCE", "Reliance Industries")
    articles = [
        _item(
            "RELIANCE Industries Q3 results beat estimates",
            "Net profit surges for Reliance on strong retail growth.",
        ),
        _item(
            "HDFC Bank raises deposit rates",
            "Banking sector sees renewed interest as deposit rates climb.",
        ),
    ]
    result = filter_articles(articles, vocab, symbol="RELIANCE")

    assert len(result.kept) == 1, "Only the RELIANCE article should survive the gate"
    assert "RELIANCE" in result.kept[0].title or "Reliance" in result.kept[0].title
    assert result.dropped_count == 1
    assert result.tokens_out < result.tokens_in


# ── Test 2: commodity match (ETF category keywords) ───────────────────────────


def test_commodity_match_keeps_gold_article():
    """
    Gold-related article should pass the GOLDBEES gate via ETF-category keywords
    (gold, xau, bullion, central bank, etc.) even without the ticker in the title.
    """
    from src.tools.news_filter import build_match_vocab, filter_articles

    vocab = build_match_vocab("GOLDBEES")
    articles = [
        _item(
            "Fed rate cut sends gold to record high",
            "Gold prices surged after the Federal Reserve signalled a pause.",
        ),
        _item(
            "Nifty IT index outlook positive for Q4",
            "TCS and Infosys results are expected next week.",
        ),
    ]
    result = filter_articles(articles, vocab, symbol="GOLDBEES")

    kept_titles = [a.title.lower() for a in result.kept]
    assert any("gold" in t for t in kept_titles), "Gold article must be kept for GOLDBEES"


# ── Test 3: sentence extraction ───────────────────────────────────────────────


def test_sentence_extraction_removes_irrelevant_sentences():
    """
    Only the sentence mentioning the stock should survive the description trim.
    """
    from src.tools.news_filter import build_match_vocab, extract_relevant_sentences

    vocab = build_match_vocab("INFY", "Infosys")
    description = (
        "Global markets ended mixed on Thursday. "
        "Infosys reported a 12% rise in net profit for Q3 FY26. "
        "Analysts remain cautious on broader consumer spending."
    )
    trimmed = extract_relevant_sentences(description, vocab)

    assert "Infosys" in trimmed or "INFY" in trimmed.upper(), "Infosys sentence must be kept"
    assert "Global markets" not in trimmed, "Off-topic sentence must be removed"


# ── Test 4: spaCy-absent fallback ─────────────────────────────────────────────


def test_regex_fallback_when_spacy_unavailable(monkeypatch):
    """
    When spaCy is not available (_NLP_AVAILABLE=False), the regex path must
    still filter correctly — no exceptions, valid FilterResult returned.
    """
    import src.tools.news_filter as nf

    monkeypatch.setattr(nf, "_NLP_AVAILABLE", False)
    monkeypatch.setattr(nf, "_nlp", None)

    from src.tools.news_filter import build_match_vocab, filter_articles

    vocab = build_match_vocab("WIPRO", "Wipro")
    articles = [
        _item("Wipro wins $200m deal from US bank", "Wipro expanded its US client base significantly."),
        _item("OPEC cuts oil production by 1 mb/d", "Crude prices rose on the supply cut announcement."),
    ]
    result = filter_articles(articles, vocab, symbol="WIPRO")

    assert not result.starvation_fallback, "Wipro article should pass the gate"
    assert len(result.kept) >= 1
    assert any("Wipro" in a.title for a in result.kept)


# ── Test 5: no-starvation safety ─────────────────────────────────────────────


def test_no_starvation_returns_originals():
    """
    When no article matches the vocabulary, all originals are returned with
    starvation_fallback=True so the LLM is never silently starved.
    """
    from src.tools.news_filter import build_match_vocab, filter_articles

    # Deliberately obscure symbol that will never appear in any article text
    vocab = build_match_vocab("ZZZNOMATCH9999")
    articles = [
        _item("General market commentary today", "Markets were volatile across sectors."),
        _item("Another generic story", "Analysts debated monetary policy outlook."),
    ]
    result = filter_articles(articles, vocab, symbol="ZZZNOMATCH9999")

    assert result.starvation_fallback, "Starvation fallback must trigger"
    assert len(result.kept) == len(articles), "All originals must be returned"


# ── Test 6: Mistral/Ollama Stage C — rescue + validate ───────────────────────


def test_mistral_stage_c_rescue_and_validate(monkeypatch):
    """
    Stage C (Mistral/Ollama):
    • An article that FAILED the regex gate but is answered YES by Ollama is rescued.
    • An article that PASSED the regex gate but is answered NO by Ollama is dropped.

    Ollama HTTP calls are mocked — no live Ollama needed.
    """
    import json
    import src.tools.news_filter as nf
    from src.tools.news_filter import build_match_vocab, filter_articles

    # --- Mock Ollama responses ---------------------------------------------------
    # We'll track call order: first call → YES (rescue), second call → NO (drop)
    call_count = {"n": 0}

    def mock_llm_is_relevant(title, description, symbol, context, base_url, model, timeout):
        call_count["n"] += 1
        # Rescue call (for the failed article): answer YES
        if "Fed pauses" in title:
            return True
        # Validate call (for the regex-passed article): answer NO
        if "General mention" in title:
            return False
        return True  # default keep

    monkeypatch.setattr(nf, "_llm_is_relevant", mock_llm_is_relevant)

    # Patch settings to enable Stage C
    import config.settings as cfg_mod

    class FakeSettings:
        news_filter_llm_enabled = True
        news_filter_llm_base_url = "http://localhost:11434/v1"
        news_filter_llm_model = "mistral:7b-instruct"
        news_filter_llm_timeout = 8

    monkeypatch.setattr(nf, "settings", FakeSettings(), raising=False)
    # Patch the settings import inside filter_articles
    import sys
    fake_settings_module = type(sys)("config.settings")
    fake_settings_module.settings = FakeSettings()
    monkeypatch.setitem(sys.modules, "config.settings", fake_settings_module)

    vocab = build_match_vocab("GOLDBEES")

    articles = [
        # This article FAILS the regex gate (no gold keyword) but LLM says YES → rescued
        _item(
            "Fed pauses rate hikes as inflation moderates",
            "The Federal Reserve held rates steady, real yields fell sharply.",
        ),
        # This article PASSES the regex gate (mentions gold) but LLM says NO → dropped
        _item(
            "General mention of gold in a broader commodities roundup",
            "Analysts reviewed oil, gold, and copper prices last week.",
        ),
    ]

    result = filter_articles(articles, vocab, symbol="GOLDBEES")

    kept_titles = [
        (a.get("title") if isinstance(a, dict) else a.title)
        for a in result.kept
    ]
    assert any("Fed pauses" in t for t in kept_titles), "LLM-rescued article must be in kept"
    assert not any("General mention" in t for t in kept_titles), "LLM-dropped false positive must be absent"

