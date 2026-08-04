"""
src/importer/fetchers/expert_tweets.py
──────────────────────────────────────
Fetches tweets from macro experts using public RSS proxies (Nitter/RSS.app)
and prepares them for insertion into ClickHouse.
"""

import logging
import feedparser
from datetime import datetime
from typing import List, Dict, Any

log = logging.getLogger(__name__)

EXPERT_FEEDS = {
    'Ritesh Jain': [
        'https://nitter.poast.org/riteshmjn/rss',
        'https://xcancel.com/riteshmjn/rss',
        'https://nitter.no-logs.com/riteshmjn/rss'
    ],
}

def fetch_expert_tweets() -> List[Dict[str, Any]]:
    """
    Scrape expert RSS feeds and return list of news_articles rows.
    """
    rows = []
    
    for expert_name, feed_urls in EXPERT_FEEDS.items():
        feed_found = False
        for url in feed_urls:
            log.info(f"Attempting to fetch tweets for {expert_name} from {url}")
            try:
                feed = feedparser.parse(url)
                
                if feed.bozo or not feed.entries:
                    log.warning(f"Feed issue for {expert_name} at {url}: {feed.get('bozo_exception', 'No entries')}")
                    continue

                for entry in feed.entries:
                    content = entry.get('title', '')
                    tweet_url = entry.get('link', '')
                    pub_date_str = entry.get('published', '')
                    
                    rows.append({
                        "fetched_at":    datetime.now(),
                        "published_at":  pub_date_str,
                        "source_type":   "expert_tweet",
                        "category":      expert_name,
                        "etfs_impacted": "", 
                        "sentiment":     "NEUTRAL",
                        "impact_tier":   "MEDIUM",
                        "title":         content,
                        "source":        "X (Twitter)",
                        "url":           tweet_url,
                    })
                
                feed_found = True
                log.info(f"Successfully fetched {len(feed.entries)} tweets for {expert_name} from {url}")
                break # Stop after first successful feed
                
            except Exception as e:
                log.error(f"Failed to fetch tweets for {expert_name} from {url}: {e}")
        
        if not feed_found:
            log.error(f"All RSS proxies failed for {expert_name}")

    return rows
