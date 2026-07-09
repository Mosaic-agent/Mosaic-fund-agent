import logging
from typing import Optional
from config.settings import settings

def setup_logging(log_level: Optional[str] = None) -> None:
    """Standardizes logging configuration and suppresses noisy third-party loggers."""
    level_str = log_level or settings.log_level or "INFO"
    level = getattr(logging, level_str.upper(), logging.INFO)

    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Centralized list of noisy third-party loggers to suppress
    noisy_loggers = [
        "httpx",
        "urllib3",
        "yfinance",
        "clickhouse_driver",
        "qdrant_client",
        "openai",
        "anthropic",
        "chromadb",
    ]
    for logger_name in noisy_loggers:
        logging.getLogger(logger_name).setLevel(logging.WARNING)
