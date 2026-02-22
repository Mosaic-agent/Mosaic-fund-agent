"""
config/settings.py
──────────────────
Central configuration for Portfolio Insight.

All fields are loaded from the .env file (or environment variables).
Fields marked # [SENSITIVE] must NEVER be hard-coded or committed to source control.
Fields marked # [NON-SENSITIVE] are safe defaults that can be changed without risk.

Usage:
    from config.settings import settings
    print(settings.llm_model)
"""

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application-wide settings loaded from .env / environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ── LLM Provider ─────────────────────────────────────────────────────────

    # [SENSITIVE] OpenAI API key – https://platform.openai.com/api-keys
    openai_api_key: str = Field(default="", description="OpenAI API key")

    # [SENSITIVE] Anthropic API key – https://console.anthropic.com/
    anthropic_api_key: str = Field(default="", description="Anthropic API key")

    # [NON-SENSITIVE] Which LLM provider to use: "openai" or "anthropic"
    llm_provider: str = Field(default="openai", description="LLM provider")

    # [NON-SENSITIVE] Model name to use (gpt-4o-mini, claude-3-haiku, deepseek-r1:7b, etc.)
    llm_model: str = Field(default="gpt-4o-mini", description="LLM model name")

    # [NON-SENSITIVE] Custom base URL for OpenAI-compatible local inference servers.
    # Ollama:    http://localhost:11434/v1
    # LM Studio: http://localhost:1234/v1
    # Leave blank to use the official OpenAI/Anthropic cloud endpoints.
    llm_base_url: str = Field(default="", description="Custom OpenAI-compatible base URL (local models)")

    # ── Zerodha Kite MCP ─────────────────────────────────────────────────────

    # [NON-SENSITIVE] Hosted Kite MCP endpoint – no auth needed for hosted version
    kite_mcp_url: str = Field(
        default="https://mcp.kite.trade/mcp",
        description="Zerodha Kite MCP server URL",
    )

    # [SENSITIVE] Self-hosted Kite API key – leave blank for hosted mcp.kite.trade
    kite_api_key: str = Field(default="", description="Kite Connect API key (self-hosted only)")

    # [SENSITIVE] Self-hosted Kite API secret – leave blank for hosted mcp.kite.trade
    kite_api_secret: str = Field(default="", description="Kite Connect API secret (self-hosted only)")

    # [NON-SENSITIVE] MCP request timeout in seconds
    kite_mcp_timeout: int = Field(default=30, description="Kite MCP connection timeout (s)")
    # ── NewsAPI ───────────────────────────────────────────────────────────────────

    # [SENSITIVE] NewsAPI.org API key – https://newsapi.org/register
    # Used by NewsSentimentAgent to pull premium Indian financial news.
    newsapi_key: str = Field(default="", description="NewsAPI.org API key")
    # ── Gold / COMEX API ──────────────────────────────────────────────────────

    # [SENSITIVE] gold-api.com API key – https://gold-api.com/
    # Provides live COMEX prices: XAU (Gold), XAG (Silver), XPT (Platinum),
    # XPD (Palladium), HG (Copper). Used for pre-market signal detection.
    gold_api_key: str = Field(default="", description="gold-api.com API key")

    # [NON-SENSITIVE] Max news articles to fetch per stock symbol
    news_articles_per_stock: int = Field(default=5, description="Articles per stock")

    # [NON-SENSITIVE] How many days back to search for news (free tier max: 30)
    news_lookback_days: int = Field(default=7, description="News lookback window in days")

    # ── Application ───────────────────────────────────────────────────────────

    # [NON-SENSITIVE] Output directory for generated JSON/HTML reports
    output_dir: str = Field(default="./output", description="Report output directory")

    # [NON-SENSITIVE] Python log level: DEBUG | INFO | WARNING | ERROR
    log_level: str = Field(default="INFO", description="Logging level")

    # [NON-SENSITIVE] Max holdings to process per run (0 = process all holdings)
    max_holdings_per_run: int = Field(default=0, description="Holdings cap per run (0=unlimited)")

    # [NON-SENSITIVE] Seconds to wait between web-scraping requests (be polite)
    scrape_delay_seconds: float = Field(default=2.0, description="Delay between scrape requests")

    # ── Indian Market Constants ───────────────────────────────────────────────

    # [NON-SENSITIVE] Yahoo Finance suffix for NSE-listed stocks
    nse_suffix: str = Field(default=".NS", description="Yahoo Finance NSE ticker suffix")

    # [NON-SENSITIVE] Yahoo Finance suffix for BSE-listed stocks
    bse_suffix: str = Field(default=".BO", description="Yahoo Finance BSE ticker suffix")

    # [NON-SENSITIVE] Indian market timezone
    market_timezone: str = Field(default="Asia/Kolkata", description="Market timezone")

    # [NON-SENSITIVE] NSE regular session open time (IST, 24h HH:MM)
    market_open: str = Field(default="09:15", description="NSE market open time IST")

    # [NON-SENSITIVE] NSE regular session close time (IST, 24h HH:MM)
    market_close: str = Field(default="15:30", description="NSE market close time IST")

    def validate_sensitive_fields(self) -> list[str]:
        """
        Returns a list of warnings for any SENSITIVE fields that are missing.
        Call this at startup to surface mis-configuration early.
        """
        warnings: list[str] = []

        # Skip API key checks when using a local model via a custom base URL
        using_local = bool(self.llm_base_url)

        if not using_local:
            if not self.openai_api_key and not self.anthropic_api_key:
                warnings.append(
                    "[SENSITIVE] Neither OPENAI_API_KEY nor ANTHROPIC_API_KEY is set. "
                    "Set at least one in your .env file."
                )

            if self.llm_provider == "openai" and not self.openai_api_key:
                warnings.append(
                    "[SENSITIVE] LLM_PROVIDER=openai but OPENAI_API_KEY is not set."
                )

            if self.llm_provider == "anthropic" and not self.anthropic_api_key:
                warnings.append(
                    "[SENSITIVE] LLM_PROVIDER=anthropic but ANTHROPIC_API_KEY is not set."
                )

        if not self.newsapi_key:
            warnings.append(
                "[SENSITIVE] NEWSAPI_KEY is not set. NewsAPI news enrichment will be skipped. "
                "Get a free key at https://newsapi.org/register"
            )

        if not self.gold_api_key:
            warnings.append(
                "[SENSITIVE] GOLD_API_KEY is not set. COMEX pre-market signals will be skipped. "
                "Get a free key at https://gold-api.com/"
            )

        return warnings


# Singleton instance – import this throughout the app
settings = Settings()
