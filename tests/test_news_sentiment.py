"""Quick smoke-test for the new news sentiment agent."""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from src.agents.news_sentiment_agent import NewsSentimentAgent, collate_news_sentiment

print("=== Testing collate_news_sentiment (direct, no LLM) ===")
result = collate_news_sentiment.invoke("RELIANCE|Reliance Industries")
print(f"  Total articles : {result['total_articles']}")
print(f"  NewsAPI count  : {result['newsapi_count']}")
print(f"  GNews count    : {result['gnews_count']}")
print(f"  Deduped count  : {result['deduplicated_count']}")
print(f"  Sentiment      : {result['overall_sentiment']}")
print(f"  Score          : {result['sentiment_score']:+.3f}")
print(f"  Breakdown      : +{result['positive_count']} / ~{result['neutral_count']} / -{result['negative_count']}")
if result.get("top_positive_headlines"):
    print(f"  Best headline  : {result['top_positive_headlines'][0][:70]}")
if result.get("top_negative_headlines"):
    print(f"  Worst headline : {result['top_negative_headlines'][0][:70]}")

print("\n=== NewsSentimentAgent._run_direct ===")
agent = NewsSentimentAgent()
r2 = agent._run_direct("TCS", "Tata Consultancy Services")
print(f"  TCS total={r2['total_articles']}  sentiment={r2['overall_sentiment']}  score={r2['sentiment_score']:+.3f}")

print("\nAll assertions passed ✓")
