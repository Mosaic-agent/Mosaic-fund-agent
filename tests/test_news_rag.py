"""
tests/test_news_rag.py
──────────────────────
Unit tests for the RAG-based news quality scoring and retrieval module.
"""
import json
from datetime import date
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pytest


# ── Scoring tests ─────────────────────────────────────────────────────────────


class TestScoreNewsQuality:
    """Test the exemplar-based semantic quality scorer."""

    def test_earnings_headline_scores_high(self):
        from src.ml.news_rag import score_news_quality
        score = score_news_quality("Q3 earnings beat estimates by 20%")
        assert score >= 0.80, f"Earnings headline should score ≥0.80, got {score:.3f}"

    def test_gold_crash_headline_scores_medium_high(self):
        from src.ml.news_rag import score_news_quality
        score = score_news_quality("Gold ETFs crash 15% as precious metals tumble")
        assert score >= 0.60, f"Gold crash headline should score ≥0.60, got {score:.3f}"

    def test_clickbait_scores_low(self):
        from src.ml.news_rag import score_news_quality
        score = score_news_quality("5 stocks that could give 20% returns in 3 months")
        assert score <= 0.50, f"Clickbait should score ≤0.50, got {score:.3f}"

    def test_blog_scores_very_low(self):
        from src.ml.news_rag import score_news_quality
        score = score_news_quality("random blog post about penny stock predictions")
        assert score <= 0.15, f"Blog/penny stock should score ≤0.15, got {score:.3f}"

    def test_empty_string_scores_zero(self):
        from src.ml.news_rag import score_news_quality
        assert score_news_quality("") == 0.0
        assert score_news_quality("   ") == 0.0

    def test_score_in_valid_range(self):
        from src.ml.news_rag import score_news_quality
        headlines = [
            "Company reports strong quarterly results",
            "Market outlook remains uncertain",
            "New product launch announcement",
        ]
        for h in headlines:
            s = score_news_quality(h)
            assert 0.0 <= s <= 1.0, f"Score {s} out of range for: {h}"

    def test_material_news_beats_generic(self):
        from src.ml.news_rag import score_news_quality
        material = score_news_quality("RBI cuts repo rate by 25 basis points")
        generic = score_news_quality("markets closed mixed on low volumes")
        assert material > generic, (
            f"Material news ({material:.3f}) should score higher than generic ({generic:.3f})"
        )


# ── Embedding tests ───────────────────────────────────────────────────────────


class TestEmbedText:
    """Test the embedding primitive."""

    def test_returns_correct_dimension(self):
        from src.ml.news_rag import embed_text
        vec = embed_text("test sentence")
        assert len(vec) == 768

    def test_empty_string_returns_zeros(self):
        from src.ml.news_rag import embed_text
        vec = embed_text("")
        assert all(v == 0.0 for v in vec)

    def test_similar_texts_have_high_cosine(self):
        from src.ml.news_rag import embed_text
        v1 = np.array(embed_text("gold prices surge on safe haven demand"))
        v2 = np.array(embed_text("gold rallies as investors seek safety"))
        sim = np.dot(v1, v2) / (np.linalg.norm(v1) * np.linalg.norm(v2))
        assert sim > 0.7, f"Similar texts should have cosine > 0.7, got {sim:.3f}"

    def test_dissimilar_texts_have_lower_cosine(self):
        from src.ml.news_rag import embed_text
        v1 = np.array(embed_text("gold prices surge on safe haven demand"))
        v2 = np.array(embed_text("python programming tutorial for beginners"))
        sim = np.dot(v1, v2) / (np.linalg.norm(v1) * np.linalg.norm(v2))
        assert sim < 0.5, f"Dissimilar texts should have cosine < 0.5, got {sim:.3f}"


# ── Retrieval tests ───────────────────────────────────────────────────────────


class TestRetrieveArticles:
    """Test semantic retrieval from ClickHouse."""

    def test_retrieval_returns_list(self):
        from src.ml.news_rag import retrieve_articles
        results = retrieve_articles("gold ETF price", around_date=date.today(), days=30, k=5)
        assert isinstance(results, list)

    def test_results_sorted_by_similarity(self):
        from src.ml.news_rag import retrieve_articles
        results = retrieve_articles("gold price India", around_date=date.today(), days=30, k=10)
        if len(results) >= 2:
            sims = [r["similarity"] for r in results]
            assert sims == sorted(sims, reverse=True), "Results should be sorted by descending similarity"

    def test_results_have_required_keys(self):
        from src.ml.news_rag import retrieve_articles
        results = retrieve_articles("gold ETF", around_date=date.today(), days=30, k=3)
        required_keys = {"title", "source", "url", "published_at", "category", "sentiment", "similarity"}
        for r in results:
            assert required_keys.issubset(r.keys()), f"Missing keys: {required_keys - r.keys()}"

    def test_empty_query_returns_empty(self):
        from src.ml.news_rag import retrieve_articles
        results = retrieve_articles("", around_date=date.today(), days=7, k=5)
        assert results == []


# ── Exemplars file tests ──────────────────────────────────────────────────────


class TestExemplarsFile:
    """Validate the exemplars JSON file structure."""

    def test_exemplars_file_exists(self):
        path = Path(__file__).parent.parent / "data" / "news_quality_exemplars.json"
        assert path.exists(), f"Exemplars file not found at {path}"

    def test_exemplars_structure(self):
        path = Path(__file__).parent.parent / "data" / "news_quality_exemplars.json"
        data = json.loads(path.read_text())
        assert isinstance(data, list)
        assert len(data) >= 40, f"Need at least 40 exemplars, got {len(data)}"
        for item in data:
            assert "text" in item, "Each exemplar must have 'text'"
            assert "weight" in item, "Each exemplar must have 'weight'"
            assert 0.0 <= item["weight"] <= 1.0, f"Weight {item['weight']} out of range"

    def test_exemplars_cover_full_spectrum(self):
        path = Path(__file__).parent.parent / "data" / "news_quality_exemplars.json"
        data = json.loads(path.read_text())
        weights = [e["weight"] for e in data]
        assert max(weights) >= 0.9, "Should have high-quality exemplars (≥0.9)"
        assert min(weights) <= 0.1, "Should have low-quality exemplars (≤0.1)"
        mid_count = sum(1 for w in weights if 0.4 <= w <= 0.7)
        assert mid_count >= 5, f"Need at least 5 mid-range exemplars, got {mid_count}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
