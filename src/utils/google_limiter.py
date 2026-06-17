"""
src/utils/google_limiter.py
───────────────────────────
Shared rate limiter for Google Gemini API to prevent 429 RESOURCE_EXHAUSTED.
Free tier limit is 15 RPM (Requests Per Minute) and 1M TPM (Tokens Per Minute).
"""

import logging
from langchain_core.rate_limiters import InMemoryRateLimiter

logger = logging.getLogger(__name__)

# Shared rate limiter for Google Gemini instances.
# 0.2 requests per second = 1 request every 5 seconds (max 12 RPM).
# Safely under the 15 RPM free tier limit.
gemini_rate_limiter = InMemoryRateLimiter(
    requests_per_second=0.2,
    max_bucket_size=1,
)

logger.info("Initialized shared Google Gemini rate limiter (1 request every 5 seconds)")
