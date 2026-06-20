"""
src/tools/news_filter.py
────────────────────────
Pre-LLM news relevance filter.

Two-stage pipeline applied upstream inside fetch_news_for_symbol():

  Stage A — Article gate  : drop articles where neither title nor description
                            contains any term from the symbol's match vocabulary.
  Stage B — Sentence trim : for kept articles, replace description with only the
                            sentences that contain a vocabulary match.

Entity engine:
  Primary  — spaCy en_core_web_sm NER + sentencizer (entity-aware, ~30 MB model).
  Fallback — compiled regex + naive sentence split (no extra deps required).

No-starvation guarantee: if Stage A would drop every article, the full original
list is returned with a WARNING so LLM calls are never silently starved.

Vocabulary is built by combining:
  • Ticker variants (SYMBOL, SYMBOL.NS, SYMBOL.BO, lowercase forms)
  • Company name tokens (non-stopword tokens ≥ 3 chars)
  • ETF-category keywords from ETF_NEWS_TOPICS (reused, no duplication)
  • Macro-theme keywords from MACRO_THEMES for symbols in their impact_map

Install spaCy model once:
  python -m spacy download en_core_web_sm
"""

from __future__ import annotations

import copy
import logging
import re
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

# ── spaCy lazy singleton ──────────────────────────────────────────────────────
# Probing happens at first call; thereafter _NLP_AVAILABLE is set.
# Thread-safe in CPython (bool assignment is atomic; worst case is a double-probe).

_nlp = None
_NLP_AVAILABLE: bool | None = None  # None = not yet probed


def _get_nlp():
    """Load spaCy en_core_web_sm once; return None and set fallback flag if unavailable."""
    global _nlp, _NLP_AVAILABLE
    if _NLP_AVAILABLE is not None:
        return _nlp  # already probed — return cached result (possibly None)
    try:
        import spacy  # noqa: F401

        _nlp = spacy.load(
            "en_core_web_sm",
            # Keep only NER (entity extraction) + tok2vec (required by NER).
            # Disabling parser/tagger/lemmatizer cuts load time and memory ~60%.
            disable=["parser", "tagger", "lemmatizer"],
        )
        # Sentencizer splits text into sentences; add it only if nothing already does.
        if "sentencizer" not in _nlp.pipe_names and "senter" not in _nlp.pipe_names:
            _nlp.add_pipe("sentencizer")

        _NLP_AVAILABLE = True
        logger.debug("news_filter: spaCy en_core_web_sm loaded OK")
    except (OSError, ImportError) as exc:
        logger.info(
            "news_filter: spaCy unavailable (%s) — using regex/naive-split fallback",
            exc,
        )
        _NLP_AVAILABLE = False
    return _nlp


# ── Vocabulary builder ────────────────────────────────────────────────────────

_COMPANY_STOPWORDS: frozenset[str] = frozenset(
    {
        "ltd", "limited", "pvt", "private", "inc", "corp", "corporation",
        "company", "co", "the", "of", "and", "india", "indian", "group",
        "holdings", "enterprises", "industries", "technologies", "solutions",
    }
)


@dataclass
class MatchVocab:
    """All terms that constitute a relevance-match for one symbol lookup."""

    # Compiled case-insensitive regex for fast scanning
    pattern: re.Pattern
    # Raw term set (for logging / debugging)
    terms: set[str] = field(default_factory=set)


def _etf_category_keywords(symbol: str) -> set[str]:
    """Return ETF-category keywords for *symbol* by scanning ETF_NEWS_TOPICS."""
    try:
        from src.tools.etf_news_scanner import ETF_NEWS_TOPICS  # type: ignore[import]

        symbol_upper = symbol.upper()
        for topic in ETF_NEWS_TOPICS:
            if symbol_upper in [e.upper() for e in topic.get("etfs", [])]:
                kws: set[str] = set()
                # Yahoo ticker aliases (e.g. "GC=F", "SI=F") — included verbatim
                kws.update(topic.get("yf_symbols", []))
                # Extract meaningful tokens from search queries (≥ 5 chars)
                for query in topic.get("queries", []):
                    for tok in query.lower().split():
                        tok_clean = re.sub(r"[^a-z0-9]", "", tok)
                        if len(tok_clean) >= 5 and tok_clean not in _COMPANY_STOPWORDS:
                            kws.add(tok_clean)
                return kws
    except Exception as exc:  # noqa: BLE001
        logger.debug("news_filter: ETF_NEWS_TOPICS lookup failed: %s", exc)
    return set()


def _macro_theme_keywords(symbol: str) -> set[str]:
    """Return macro-theme keywords for symbols that appear in any theme's impact_map."""
    try:
        from src.tools.macro_event_scanner import MACRO_THEMES  # type: ignore[import]

        symbol_upper = symbol.upper()
        kws: set[str] = set()
        for theme in MACRO_THEMES:
            if symbol_upper in theme.get("impact_map", {}):
                kws.update(theme.get("keywords", set()))
        return kws
    except Exception as exc:  # noqa: BLE001
        logger.debug("news_filter: MACRO_THEMES lookup failed: %s", exc)
    return set()


def build_match_vocab(symbol: str, company_name: str = "") -> MatchVocab:
    """
    Build a MatchVocab for *symbol* / *company_name* by combining:

    1. Ticker variants: SYMBOL, SYMBOL.NS, SYMBOL.BO, lowercase, strip-stripped.
    2. Company name tokens (non-stopword, ≥ 3 chars) split on whitespace / hyphens.
    3. ETF-category keywords if symbol appears in ETF_NEWS_TOPICS[*].etfs.
    4. Macro-theme keywords if symbol appears in any MACRO_THEMES[*].impact_map.
    """
    terms: set[str] = set()

    # 1. Ticker variants
    sym_upper = symbol.upper().strip()
    sym_lower = sym_upper.lower()
    terms.update(
        {
            sym_upper,
            sym_lower,
            f"{sym_upper}.NS",
            f"{sym_upper}.BO",
            sym_upper.replace("-", ""),
        }
    )

    # 2. Company name tokens
    if company_name:
        for tok in re.split(r"[\s\-/]+", company_name):
            tok_clean = re.sub(r"[^a-zA-Z0-9]", "", tok).lower()
            if len(tok_clean) >= 3 and tok_clean not in _COMPANY_STOPWORDS:
                terms.add(tok_clean)

    # 3. ETF-category keywords
    terms.update(_etf_category_keywords(symbol))

    # 4. Macro-theme keywords
    terms.update(_macro_theme_keywords(symbol))

    # Compile: sort longest-first so alternation works correctly for substrings
    sorted_terms = sorted((t for t in terms if t), key=len, reverse=True)
    pattern_str = "|".join(re.escape(t) for t in sorted_terms)
    try:
        pattern = re.compile(pattern_str, re.IGNORECASE)
    except re.error:
        # Degenerate fallback — only the raw symbol
        pattern = re.compile(re.escape(sym_upper), re.IGNORECASE)

    logger.debug(
        "news_filter: vocab for %s — %d terms, sample=%s",
        symbol,
        len(terms),
        list(terms)[:8],
    )
    return MatchVocab(pattern=pattern, terms=terms)


# ── Core filter functions ─────────────────────────────────────────────────────


def is_relevant_text(text: str, vocab: MatchVocab) -> bool:
    """Return True if *text* contains at least one vocabulary term."""
    if not text:
        return False
    return bool(vocab.pattern.search(text))


def _split_sentences_regex(text: str) -> list[str]:
    """Naive sentence splitter (fallback when spaCy is unavailable)."""
    parts = re.split(r"(?<=[.!?])\s+", text.strip())
    return [p.strip() for p in parts if p.strip()]


def _split_sentences_spacy(text: str, nlp) -> list[str]:
    """Use spaCy sentencizer to split text into sentences."""
    doc = nlp(text)
    return [sent.text.strip() for sent in doc.sents if sent.text.strip()]


def extract_relevant_sentences(description: str, vocab: MatchVocab) -> str:
    """
    Return only the sentences from *description* that contain a vocabulary match.

    Falls back to the full description if no individual sentence matches (avoids
    over-trimming very short descriptions where the match spans a phrase boundary).
    """
    if not description:
        return description

    nlp = _get_nlp()
    sentences = (
        _split_sentences_spacy(description, nlp)
        if nlp is not None
        else _split_sentences_regex(description)
    )

    matched = [s for s in sentences if is_relevant_text(s, vocab)]
    if not matched:
        # The article-gate passed (title or full description matched), but no
        # individual sentence matched.  Keep the full description rather than
        # returning an empty string.
        return description

    return " ".join(matched)


# ── Telemetry helper ──────────────────────────────────────────────────────────


def _token_proxy(text: str) -> int:
    """Approximate token count: word-count × 1.3 (accounts for BPE sub-words)."""
    return int(len(text.split()) * 1.3)


# ── Main filter entrypoint ────────────────────────────────────────────────────


@dataclass
class FilterResult:
    """Result of a filter_articles() call."""

    kept: list  # Kept article objects (NewsItem or dict), descriptions trimmed
    dropped_count: int
    tokens_in: int
    tokens_out: int
    starvation_fallback: bool = False  # True when no-starvation guard triggered


# ── Mistral / Ollama semantic relevance scorer (Stage C) ─────────────────────
#
# Calls mistral:7b-instruct (or any Ollama model) via the OpenAI-compatible
# chat-completions endpoint.  Returns a list of bool — True means "keep".
#
# Enabled when settings.news_filter_llm_enabled is True.
# Runs in parallel (ThreadPoolExecutor) to minimise latency across a batch.
#
# Design:  Stage C is a RESCUE + VALIDATION pass.
#   • Rescue : articles that FAILED Stage A regex (may be semantically relevant)
#   • Validate: articles that PASSED Stage A regex (may be false positives)
# Both sets are re-assessed and merged before Stage B sentence trim.

_LLM_SYSTEM_PROMPT = (
    "You are a financial news relevance classifier. "
    "Given a symbol and an article, decide whether the article is financially "
    "relevant to that symbol or its sector/commodity. "
    "Respond with exactly one word: YES or NO."
)

_LLM_USER_TEMPLATE = """Symbol: {symbol}
Context: {context}

Article title: {title}
Article description: {description}

Is this article financially relevant to {symbol}? Answer YES or NO."""


def _build_llm_context(symbol: str, vocab: MatchVocab) -> str:
    """Human-readable context string summarising what the symbol represents."""
    sample_terms = sorted(
        (t for t in vocab.terms if len(t) >= 4 and not t.endswith(".NS") and not t.endswith(".BO")),
        key=len,
        reverse=True,
    )[:10]
    return ", ".join(sample_terms) if sample_terms else symbol


def _llm_is_relevant(
    title: str,
    description: str,
    symbol: str,
    context: str,
    base_url: str,
    model: str,
    timeout: int,
) -> bool:
    """
    Call the Ollama OpenAI-compatible endpoint and parse YES/NO.

    Returns True (keep) when the model answers YES or when the call fails
    (fail-open so we don't silently drop articles on Ollama errors).
    """
    import json as _json
    import urllib.request as _urlreq

    text_body = _LLM_USER_TEMPLATE.format(
        symbol=symbol,
        context=context,
        title=(title or "")[:300],
        description=(description or "")[:500],
    )
    payload = _json.dumps(
        {
            "model": model,
            "messages": [
                {"role": "system", "content": _LLM_SYSTEM_PROMPT},
                {"role": "user", "content": text_body},
            ],
            "temperature": 0,
            "max_tokens": 5,
            "stream": False,
        }
    ).encode()

    url = base_url.rstrip("/") + "/chat/completions"
    req = _urlreq.Request(
        url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with _urlreq.urlopen(req, timeout=timeout) as resp:
            data = _json.loads(resp.read())
        answer = (
            data.get("choices", [{}])[0]
            .get("message", {})
            .get("content", "")
            .strip()
            .upper()
        )
        return not answer.startswith("NO")  # YES / anything else = keep
    except Exception as exc:  # noqa: BLE001
        logger.debug("news_filter LLM call failed (%s) — keeping article by default", exc)
        return True  # fail-open


def _batch_llm_score(
    articles: list,
    symbol: str,
    vocab: MatchVocab,
    base_url: str,
    model: str,
    timeout: int,
) -> list[bool]:
    """
    Score *articles* in parallel using ThreadPoolExecutor.
    Returns a list[bool] of the same length — True = keep.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed  # noqa: PLC0415

    context = _build_llm_context(symbol, vocab)
    results: list[bool] = [True] * len(articles)

    def _score(idx_article):
        idx, article = idx_article
        title = (article.get("title") if isinstance(article, dict) else getattr(article, "title", "")) or ""
        desc = (article.get("description") if isinstance(article, dict) else getattr(article, "description", "")) or ""
        return idx, _llm_is_relevant(title, desc, symbol, context, base_url, model, timeout)

    with ThreadPoolExecutor(max_workers=min(len(articles), 6)) as pool:
        futures = {pool.submit(_score, (i, a)): i for i, a in enumerate(articles)}
        for fut in as_completed(futures):
            try:
                idx, keep = fut.result()
                results[idx] = keep
            except Exception as exc:  # noqa: BLE001
                logger.debug("news_filter: LLM future error: %s", exc)

    return results


def _article_text(article) -> str:
    """Combine title + description from a NewsItem (Pydantic) or raw dict."""
    if isinstance(article, dict):
        return (article.get("title") or "") + " " + (article.get("description") or "")
    return (getattr(article, "title", None) or "") + " " + (
        getattr(article, "description", None) or ""
    )


def _trim_description(article, trimmed_desc: str):
    """Return a copy of *article* with description replaced by *trimmed_desc*."""
    if isinstance(article, dict):
        result = dict(article)
        result["description"] = trimmed_desc
        return result
    # Pydantic v2
    try:
        return article.model_copy(update={"description": trimmed_desc})
    except AttributeError:
        pass
    # Pydantic v1
    try:
        return article.copy(update={"description": trimmed_desc})
    except AttributeError:
        pass
    # Plain dataclass / named tuple — shallow copy + attribute set
    dup = copy.copy(article)
    dup.description = trimmed_desc
    return dup


def filter_articles(
    articles: list,
    vocab: MatchVocab,
    symbol: str = "",
) -> FilterResult:
    """
    Apply the three-stage pre-LLM relevance filter to *articles*.

    Stage A — Regex/spaCy article gate:
        Drop articles where neither title nor description contains any term from
        *vocab*.  This eliminates wholly off-topic articles cheaply.

    Stage C — Mistral/Ollama semantic correlation (optional, off by default):
        When settings.news_filter_llm_enabled is True, runs two passes in parallel:
          • Rescue : articles that FAILED Stage A are re-scored — catches semantically
                     relevant articles that lack explicit keywords (e.g. "Fed pauses
                     rate hikes" for GOLDBEES because gold correlates with real yields).
          • Validate: articles that PASSED Stage A are re-scored — drops false positives
                     (e.g. "Nifty 50 outlook" that mentioned gold in passing).
        Both passes merge and replace the Stage A output.

    Stage B — Sentence trim:
        For each kept article, replace description with only the sentences that
        contain a vocabulary match, reducing irrelevant context passed to the LLM.

    No-starvation:
        If after all stages no article survives, return the originals unchanged
        (with a WARNING log) so downstream LLM calls are never silently starved.

    Returns a FilterResult with the kept (and trimmed) articles plus telemetry.
    """
    from config.settings import settings  # noqa: PLC0415 — avoid circular at module load

    if not articles:
        return FilterResult(kept=[], dropped_count=0, tokens_in=0, tokens_out=0)

    # Measure input tokens
    tokens_in = sum(_token_proxy(_article_text(a)) for a in articles)

    # --- Stage A: regex / spaCy article gate ------------------------------------
    regex_passed = [a for a in articles if is_relevant_text(_article_text(a), vocab)]
    regex_failed = [a for a in articles if not is_relevant_text(_article_text(a), vocab)]

    # --- Stage C: Mistral / Ollama semantic correlation pass --------------------
    if settings.news_filter_llm_enabled and articles:
        try:
            llm_url = settings.news_filter_llm_base_url
            llm_model = settings.news_filter_llm_model
            llm_timeout = settings.news_filter_llm_timeout

            # Rescue: score articles that the regex gate dropped
            if regex_failed:
                rescue_scores = _batch_llm_score(
                    regex_failed, symbol, vocab, llm_url, llm_model, llm_timeout
                )
                rescued = [a for a, keep in zip(regex_failed, rescue_scores) if keep]
                if rescued:
                    logger.info(
                        "news_filter LLM rescue: symbol=%s rescued %d/%d regex-dropped articles",
                        symbol or "?", len(rescued), len(regex_failed),
                    )
            else:
                rescued = []

            # Validate: re-score articles that the regex gate kept (remove false positives)
            if regex_passed:
                validate_scores = _batch_llm_score(
                    regex_passed, symbol, vocab, llm_url, llm_model, llm_timeout
                )
                validated = [a for a, keep in zip(regex_passed, validate_scores) if keep]
                fp_dropped = len(regex_passed) - len(validated)
                if fp_dropped:
                    logger.info(
                        "news_filter LLM validate: symbol=%s dropped %d false-positive articles",
                        symbol or "?", fp_dropped,
                    )
            else:
                validated = []

            kept_a = validated + rescued
            dropped = len(articles) - len(kept_a)

        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "news_filter: Stage C (LLM) failed (%s) — falling back to regex gate output",
                exc,
            )
            kept_a = regex_passed
            dropped = len(regex_failed)
    else:
        kept_a = regex_passed
        dropped = len(regex_failed)

    # No-starvation guard
    starvation_fallback = False
    if not kept_a:
        logger.warning(
            "news_filter: all %d articles dropped for symbol=%s — "
            "returning originals (vocab may be too strict or symbol unrecognised)",
            len(articles),
            symbol or "?",
        )
        kept_a = list(articles)
        starvation_fallback = True

    # --- Stage B: sentence trim --------------------------------------------------
    kept_b: list = []
    for article in kept_a:
        if isinstance(article, dict):
            raw_desc = article.get("description") or ""
        else:
            raw_desc = getattr(article, "description", None) or ""

        trimmed_desc = extract_relevant_sentences(raw_desc, vocab) if raw_desc else raw_desc
        kept_b.append(_trim_description(article, trimmed_desc))

    # Measure output tokens
    tokens_out = sum(_token_proxy(_article_text(a)) for a in kept_b)
    reduction = int((1 - tokens_out / max(tokens_in, 1)) * 100)

    logger.info(
        "news_filter: symbol=%s kept=%d/%d tokens_in=%d tokens_out=%d reduction=%d%%",
        symbol or "?",
        len(kept_b),
        len(articles),
        tokens_in,
        tokens_out,
        reduction,
    )

    return FilterResult(
        kept=kept_b,
        dropped_count=dropped,
        tokens_in=tokens_in,
        tokens_out=tokens_out,
        starvation_fallback=starvation_fallback,
    )
