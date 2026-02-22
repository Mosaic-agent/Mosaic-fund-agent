"""Quick test: verify COMEX and NewsAPI cache round-trips."""
import sys, os, time
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from src.utils.cache import cache_get, cache_set, cache_age_seconds, cache_clear

# ── Unit tests ─────────────────────────────────────────────────────────────────
print("=== cache round-trip ===")
cache_clear("test_key")
assert cache_get("test_key") is None, "miss expected"
cache_set("test_key", {"hello": "world", "num": 42})
result = cache_get("test_key", ttl_seconds=60)
assert result == {"hello": "world", "num": 42}, f"unexpected: {result}"
age = cache_age_seconds("test_key")
assert age is not None and age < 2, f"bad age: {age}"
print(f"  ✓ write → read roundtrip  (age {age:.1f}s)")

# expiry test
cache_set("test_expiry", {"x": 1})
time.sleep(0.1)
assert cache_get("test_expiry", ttl_seconds=0) is None, "should be expired with ttl=0"
print("  ✓ TTL=0 treated as always expired")

cache_clear("test_key")
cache_clear("test_expiry")
print("  ✓ cache_clear works")

# ── Integration: NewsAPI cache ─────────────────────────────────────────────────
print("\n=== NewsAPI cache integration ===")
from src.tools.newsapi_search import fetch_newsapi_articles
from src.utils.cache import cache_clear

cache_clear("newsapi_RELIANCE_7d")
print("  First call (live API)...")
t0 = time.time()
items1 = fetch_newsapi_articles("RELIANCE", "Reliance Industries")
t1 = time.time() - t0
print(f"  ✓ fetched {len(items1)} articles in {t1:.1f}s")

print("  Second call (should be cache HIT)...")
t0 = time.time()
items2 = fetch_newsapi_articles("RELIANCE", "Reliance Industries")
t2 = time.time() - t0
print(f"  ✓ returned {len(items2)} articles in {t2:.2f}s  (speedup {t1/max(t2,0.01):.0f}x)")
assert len(items1) == len(items2), "cache returned different count"
assert t2 < 0.5, f"cache hit took too long: {t2:.2f}s"
print(f"  ✓ cache hit confirmed ({t2:.3f}s vs {t1:.1f}s live)")

print("\nAll cache tests passed ✓")
