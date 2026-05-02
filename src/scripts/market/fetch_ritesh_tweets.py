"""
scripts/fetch_ritesh_tweets.py
───────────────────────────────
Fallback scraper for Ritesh Jain's tweets when RSS proxies are down.
Uses direct web scraping of the profile if possible or structured search results.
"""

import sys
import os
from datetime import datetime
import requests
from bs4 import BeautifulSoup

# Add project root to sys.path
sys.path.append(os.getcwd())

from config.settings import settings
import clickhouse_connect

def fetch_and_insert():
    # Use a direct viewer that doesn't require login if possible
    # In 2026, many 'viewers' exist. Let's try a direct fetch from a stable viewer.
    url = "https://xcancel.com/riteshmjn"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    
    print(f"Attempting fallback scrape from {url}...")
    try:
        resp = requests.get(url, headers=headers, timeout=15)
        if resp.status_code != 200:
            print(f"Failed to fetch: {resp.status_code}")
            return

        soup = BeautifulSoup(resp.text, 'lxml')
        tweets = soup.find_all('div', class_='tweet-content')
        
        if not tweets:
            print("No tweets found in HTML structure.")
            return

        rows = []
        for t in tweets[:10]: # Latest 10
            content = t.get_text(strip=True)
            rows.append({
                "fetched_at":    datetime.now(),
                "published_at":  datetime.now().strftime('%Y-%m-%d %H:%M'), # Approximate
                "source_type":   "expert_tweet",
                "category":      "Ritesh Jain",
                "etfs_impacted": "",
                "sentiment":     "NEUTRAL",
                "impact_tier":   "MEDIUM",
                "title":         content[:250], # Truncate for title
                "source":        "X (Direct Scrape)",
                "url":           "https://x.com/riteshmjn",
            })

        if rows:
            client = clickhouse_connect.get_client(
                host=settings.clickhouse_host,
                port=settings.clickhouse_port,
                username=settings.clickhouse_user,
                password=settings.clickhouse_password,
            )
            
            # Map to list of lists for insert
            data = [
                [r['fetched_at'], r['published_at'], r['source_type'], r['category'],
                 r['etfs_impacted'], r['sentiment'], r['impact_tier'], r['title'],
                 r['source'], r['url']]
                for r in rows
            ]
            
            client.insert(
                'market_data.news_articles',
                data,
                column_names=[
                    'fetched_at', 'published_at', 'source_type', 'category',
                    'etfs_impacted', 'sentiment', 'impact_tier', 'title', 'source', 'url'
                ]
            )
            print(f"Successfully inserted {len(rows)} tweets into ClickHouse.")
            client.close()
            
    except Exception as e:
        print(f"Scrape error: {e}")

if __name__ == "__main__":
    fetch_and_insert()
