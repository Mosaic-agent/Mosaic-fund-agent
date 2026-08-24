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

    # [SENSITIVE] OpenRouter API key – https://openrouter.ai/keys
    openrouter_api_key: str = Field(default="", description="OpenRouter API key")

    # [SENSITIVE] NVIDIA NIM API key – https://integrate.api.nvidia.com/
    nvidia_api_key: str = Field(default="", description="NVIDIA NIM API key (nvapi-...)")

    # [NON-SENSITIVE] Which LLM provider to use: "openai", "anthropic", or "openrouter"
    llm_provider: str = Field(default="anthropic", description="LLM provider")

    # [NON-SENSITIVE] Model name to use (claude-sonnet-5, gpt-4o-mini, deepseek-r1:7b, etc.)
    llm_model: str = Field(default="claude-sonnet-5", description="LLM model name")

    # [NON-SENSITIVE] Custom base URL for OpenAI-compatible local inference servers.
    # Ollama:    http://localhost:11434/v1
    # LM Studio: http://localhost:1234/v1
    # Leave blank to use the official OpenAI/Anthropic cloud endpoints.
    llm_base_url: str = Field(default="", description="Custom OpenAI-compatible base URL (local models)")

    # [NON-SENSITIVE] Ollama server base URL (read from OLLAMA_HOST in .env)
    ollama_host: str = Field(default="http://localhost:11434", description="Ollama server base URL")

    # [NON-SENSITIVE] Ollama fallback base URL if container DNS resolution fails
    ollama_fallback_host: str = Field(default="http://localhost:11434", description="Fallback Ollama base URL")

    # [NON-SENSITIVE] Set to true to skip the local LLM entirely and use only the cloud LLM.
    # Useful when you want to route all traffic through the cloud model (e.g. Claude Sonnet).
    llm_local_disabled: bool = Field(default=False, description="Disable local LLM; use cloud LLM for all requests")

    # [NON-SENSITIVE] Route MF (mutual fund) tool calls through the artifact+condense
    # wrapper (src/tools/mf_artifact.py) and the declarative fund-holdings/whale-consensus
    # playbooks instead of raw subprocess stdout + the full MFSubAgent ReAct loop.
    # Set to False to fall back to the legacy raw-output path for debugging/comparison.
    mf_optimize_mode: bool = Field(
        default=True,
        description="Route MF tool calls through artifact+condense wrapper and declarative playbooks",
    )

    # [NON-SENSITIVE] Enable native thinking/reasoning tokens for supported Ollama models
    # (qwen3, deepseek-r1). Passes think=true in the request body. Has no effect on cloud models.
    llm_think: bool = Field(default=False, description="Enable Ollama native thinking mode (qwen3, deepseek-r1)")

    # [NON-SENSITIVE] Default temperature parameter for LLM generations
    llm_temperature: float = Field(default=0.0, description="Default LLM temperature")

    # ── Timeouts ─────────────────────────────────────────────────────────────────
    # Local LLMs (Ollama) need generous timeouts — GARCH + news + tool chains
    # can take 3-5 minutes. Cloud LLMs should respond in seconds.
    # llm_request_timeout: per-HTTP-call timeout sent to ChatOpenAI/ChatAnthropic.
    # agent_timeout: total wall-clock limit for the entire agent run (all tool calls).
    llm_request_timeout: int = Field(
        default=600,
        description="Per-LLM-call HTTP timeout (s). Local: 600. Cloud: 60.",
    )
    cloud_llm_request_timeout: int = Field(
        default=60,
        description="Per-LLM-call timeout for cloud providers (OpenAI, Anthropic, Google).",
    )
    agent_timeout: int = Field(
        default=600,
        description="Total agent-run wall-clock limit (s). Covers all tool calls in the ReAct loop.",
    )

    # ── Code Agent LLM — dedicated model for CodeSubAgent ───────────────────────
    # Leave CODE_LLM_PROVIDER blank to share the main LLM with the code agent.
    # When set, CodeSubAgent uses this model regardless of the main LLM.
    #
    # Recommended: a large-context coding model such as:
    #   anthropic  claude-sonnet-4-5         (200 k ctx, strong at Python)
    #   google     gemini-2.0-flash          (1 M ctx, fast + cheap)
    #   openai     gpt-4o                    (128 k ctx)
    #   openai     gpt-4.1-mini              (1 M ctx, cost-efficient)
    #
    # [NON-SENSITIVE] Provider: "openai" | "anthropic" | "google" | "" (disabled)
    code_llm_provider: str = Field(default="", description="Code agent LLM provider")
    # [NON-SENSITIVE] Model name for the code agent
    code_llm_model: str = Field(default="", description="Code agent LLM model name")
    # [NON-SENSITIVE] Optional custom base URL (Ollama / LM Studio) for the code agent
    code_llm_base_url: str = Field(default="", description="Code agent custom base URL")
    # [NON-SENSITIVE] Context window for the code agent's LLM (0 = auto-detect / inherit)
    code_llm_context_window_configured: int = Field(
        default=0,
        alias="code_llm_context_window",
        description="Code agent context window (0 = inherit)",
    )

    # [SENSITIVE] Google / Gemini API key — https://aistudio.google.com/app/apikey
    google_api_key: str = Field(default="", description="Google Gemini API key")

    # ── Secondary (cloud) LLM — for long-context / reasoning-heavy queries ────
    # Leave llm_cloud_provider blank to disable cloud routing entirely.
    # When set, queries matching _CLOUD_NEEDED_RE automatically use this model.
    #
    # Examples:
    #   LLM_CLOUD_PROVIDER=anthropic  LLM_CLOUD_MODEL=claude-3-5-haiku-20241022
    #   LLM_CLOUD_PROVIDER=openai     LLM_CLOUD_MODEL=gpt-4o-mini
    #
    # [NON-SENSITIVE] Cloud provider: "openai" | "anthropic" | "" (disabled)
    llm_cloud_provider: str = Field(default="", description="Cloud LLM provider (openai|anthropic); blank = disabled")
    # [NON-SENSITIVE] Cloud model name
    llm_cloud_model: str = Field(default="claude-3-5-haiku-20241022", description="Cloud LLM model name")
    # [NON-SENSITIVE] Context window of the cloud model (tokens) (0 = auto-detect)
    llm_cloud_context_window_configured: int = Field(
        default=0,
        alias="llm_cloud_context_window",
        description="Cloud model context window (0 = auto-detect)",
    )
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

    # [NON-SENSITIVE] Anomaly pipeline: when set, a large GARCH standardised
    # residual (|z_resid| > this) also flags a day, independent of the robust
    # z-score. GARCH residual is near-orthogonal to z_robust, so this catches
    # volatility-surprise shocks a modest raw z misses (and makes the Flash
    # Crash regime reachable). None = disabled (original behaviour). Recommended
    # enable value: 3.5. Production tool callers read this; raw run_composite_anomaly
    # defaults to None so unit tests stay deterministic.
    anomaly_garch_z_threshold: float | None = Field(
        default=None, description="GARCH residual flag threshold (None=off; 3.5 recommended)"
    )

    # [NON-SENSITIVE] Max news articles to fetch per stock symbol
    news_articles_per_stock: int = Field(default=5, description="Articles per stock")

    # [NON-SENSITIVE] How many days back to search for news (free tier max: 30)
    news_lookback_days: int = Field(default=7, description="News lookback window in days")

    # [NON-SENSITIVE] How long to cache COMEX gold-api.com responses (seconds).
    # gold-api.com free tier has a strict daily request quota — caching avoids
    # burning requests on repeated analysis runs within the same hour.
    # Set to 0 to disable caching (always fetch live).
    comex_cache_ttl_seconds: int = Field(
        default=3600,
        description="COMEX API response cache TTL in seconds (default 1 hour)",
    )

    # [NON-SENSITIVE] How long to cache NewsAPI.org responses (seconds).
    # Free tier allows only 100 requests/day — caching prevents throttling.
    newsapi_cache_ttl_seconds: int = Field(
        default=3600,
        description="NewsAPI response cache TTL in seconds (default 1 hour)",
    )

    # ── LLM response cache ────────────────────────────────────────────────────
    # Caches identical prompt→response pairs in output/.cache/llm_cache.db so
    # repeated lookups (same company, same question within the TTL window) are
    # served from disk instead of hitting the Anthropic/OpenAI API.
    # [NON-SENSITIVE] Set to false to disable entirely.
    llm_cache_enabled: bool = Field(
        default=True,
        description="Enable SQLite LLM response cache (saves API cost on repeated queries)",
    )
    # [NON-SENSITIVE] How many hours to keep a cached response.
    # 24 h is a good default — market data refreshes daily.
    llm_cache_ttl_hours: int = Field(
        default=24,
        description="LLM cache TTL in hours (0 = no expiry)",
    )

    # [NON-SENSITIVE] Configured input context window in tokens.
    # Set to 0 to auto-detect based on model.
    llm_context_window_configured: int = Field(
        default=0,
        alias="llm_context_window",
        description="Model input context window in tokens (0 = auto-detect)",
    )

    def _resolve_context_window(self, model_name: str, provider: str, base_url: str) -> int:
        """Dynamically resolve the context window size based on model type and provider."""
        model_lower = model_name.lower() if model_name else ""
        prov_lower = provider.lower() if provider else ""
        
        # 1. Local models via base URL (Ollama, LM Studio)
        if base_url and "openrouter" not in base_url and "nvidia" not in base_url:
            if any(k in model_lower for k in ["gemma", "gemma4", "gemma-3", "gemma-2", "mosaic-gemma"]):
                return 32768
            if "3.1" in model_lower or "3.2" in model_lower or "3.3" in model_lower:
                return 32768
            return 16384
            
        # 2. OpenRouter (can run any model)
        if prov_lower == "openrouter" or "openrouter.ai" in base_url:
            if "openrouter/auto" in model_lower or "auto" == model_lower:
                return 128000
            if "gemini-2.5" in model_lower or "gemini-2.0" in model_lower or "gemini-1.5" in model_lower:
                return 1000000
            if "claude-3" in model_lower:
                return 200000
            if "gpt-4o" in model_lower or "gpt-4-turbo" in model_lower:
                return 128000
            if "llama-3.1" in model_lower or "llama-3.2" in model_lower or "llama-3.3" in model_lower:
                return 128000
            if "deepseek-r1" in model_lower or "deepseek-v3" in model_lower:
                return 64000
            if "gemini" in model_lower:
                return 1000000
            if "claude" in model_lower:
                return 200000
            if "gpt-4" in model_lower:
                return 128000
            if "llama-3" in model_lower:
                return 8192
            return 16384

        # 3. Direct Google / Gemini
        if prov_lower == "google" or "gemini" in model_lower:
            return 1000000

        # 4. Direct Anthropic
        if prov_lower == "anthropic" or "claude" in model_lower:
            return 1000000

        # 5. Direct OpenAI
        if prov_lower == "openai":
            if "gpt-4o" in model_lower or "gpt-4-turbo" in model_lower:
                return 128000
            if "gpt-4" in model_lower:
                return 8192
            if "gpt-3.5" in model_lower:
                return 16385
            return 128000

        # 6. NVIDIA NIM
        if base_url and "nvidia" in base_url.lower():
            if "deepseek" in model_lower or "llama-3.1" in model_lower or "llama-3.3" in model_lower:
                return 16384
            return 16384

        return 16384

    @property
    def llm_context_window(self) -> int:
        """Dynamic model input context window in tokens."""
        if self.llm_context_window_configured > 0:
            return self.llm_context_window_configured
        return self._resolve_context_window(
            model_name=self.llm_model,
            provider=self.llm_provider,
            base_url=self.llm_base_url,
        )

    @property
    def code_llm_context_window(self) -> int:
        """Dynamic code agent context window."""
        if self.code_llm_context_window_configured > 0:
            return self.code_llm_context_window_configured
        model = self.code_llm_model or self.llm_model
        provider = self.code_llm_provider or self.llm_provider
        base_url = self.code_llm_base_url or self.llm_base_url
        return self._resolve_context_window(
            model_name=model,
            provider=provider,
            base_url=base_url,
        )

    @property
    def llm_cloud_context_window(self) -> int:
        """Dynamic cloud model context window."""
        if self.llm_cloud_context_window_configured > 0:
            return self.llm_cloud_context_window_configured
        return self._resolve_context_window(
            model_name=self.llm_cloud_model,
            provider=self.llm_cloud_provider,
            base_url="",
        )

    @property
    def llm_token_budget(self) -> int:
        """
        Max *output* tokens to request from the model.
        Caps output tokens safely so cloud provider limits (e.g. Anthropic 64k/128k, OpenAI 16k)
        are never exceeded, while flooring at 1024 for local models.
        """
        prov = self.llm_provider.lower()
        model = self.llm_model.lower()
        if "anthropic" in prov or "claude" in model:
            max_allowed = 64000
        elif "openai" in prov or "gpt" in model:
            max_allowed = 16384
        elif "google" in prov or "gemini" in model:
            max_allowed = 65536
        else:
            max_allowed = 8192

        return max(1024, min(max_allowed, self.llm_context_window // 4))

    @property
    def llm_prompt_budget(self) -> int:
        """
        Approx max characters allowed in a single prompt.
        = 75 % of context window × 4 chars/token (conservative BPE estimate).
        Use this to hard-truncate free-text fields before injecting into prompts.
        """
        return int(self.llm_context_window * 0.75 * 4)

    @property
    def is_local_model(self) -> bool:
        """True when using a local OpenAI-compatible server (LM Studio, Ollama, etc.)."""
        return bool(self.llm_base_url)

    # ── Application ───────────────────────────────────────────────────────────

    # [NON-SENSITIVE] Output directory for generated JSON/HTML reports
    output_dir: str = Field(default="./output", description="Report output directory")

    # [NON-SENSITIVE] Python log level: DEBUG | INFO | WARNING | ERROR
    log_level: str = Field(default="INFO", description="Logging level")

    # [NON-SENSITIVE] Max holdings to process per run (0 = process all holdings)
    max_holdings_per_run: int = Field(default=0, description="Holdings cap per run (0=unlimited)")

    # [NON-SENSITIVE] Seconds to wait between web-scraping requests (be polite)
    scrape_delay_seconds: float = Field(default=2.0, description="Delay between scrape requests")

    # ── Company Deep-Dive ─────────────────────────────────────────────────────

    # [SENSITIVE] sec-api.io API key — required for QueryApi, DownloadApi, XbrlApi,
    # ExtractorApi, ExecCompApi, MappingApi. Free tier at https://sec-api.io/signup
    sec_api_key: str = Field(default="", description="sec-api.io API key")

    # [NON-SENSITIVE] Path to the gemini CLI binary (https://github.com/google-gemini/gemini-cli).
    # Defaults to 'gemini' (on PATH). Override if installed to a custom location.
    gemini_cli_path: str = Field(default="gemini", description="Path to gemini CLI binary")

    # ── Shoonya (Finvasia) brokerage API — alternative NSE data source ──────
    # Used as a reliable alternative to Yahoo Finance for NSE OHLCV data.
    # Leave blank to disable; Yahoo Finance is used as fallback when not set.
    # Obtain credentials at https://shoonya.com/

    # [SENSITIVE] Shoonya login user ID
    shoonya_user_id: str = Field(default="", description="Shoonya user ID")

    # [SENSITIVE] Shoonya login password
    shoonya_password: str = Field(default="", description="Shoonya password")

    # [SENSITIVE] Shoonya API secret (generated from Prism)
    shoonya_api_secret: str = Field(default="", description="Shoonya API secret")

    # ── ClickHouse (historical data importer) ────────────────────────────────

    # [NON-SENSITIVE] ClickHouse server host
    clickhouse_host: str = Field(default="localhost", description="ClickHouse host")

    # [NON-SENSITIVE] ClickHouse HTTP port (default 8123)
    clickhouse_port: int = Field(default=8123, description="ClickHouse HTTP port")

    # [NON-SENSITIVE] ClickHouse database name
    clickhouse_database: str = Field(default="market_data", description="ClickHouse database")

    # [NON-SENSITIVE] ClickHouse username
    clickhouse_user: str = Field(default="default", description="ClickHouse username")

    # [SENSITIVE] ClickHouse password (leave blank for default no-auth setup)
    clickhouse_password: str = Field(default="", description="ClickHouse password")

    # [NON-SENSITIVE] Connection pool — min warm connections kept alive
    clickhouse_pool_min: int = Field(default=5, description="CH pool min size")

    # [NON-SENSITIVE] Connection pool — hard cap on total live connections
    clickhouse_pool_max: int = Field(default=30, description="CH pool max size")

    # [NON-SENSITIVE] Seconds to block waiting for a free pool slot
    clickhouse_pool_timeout: float = Field(default=30.0, description="CH pool checkout timeout")

    # ── Qdrant (vector database) ─────────────────────────────────────────────

    # [NON-SENSITIVE] Qdrant server host
    qdrant_host: str = Field(default="localhost", description="Qdrant host")

    # [NON-SENSITIVE] Qdrant port (default 6333)
    qdrant_port: int = Field(default=6333, description="Qdrant port")

    # [NON-SENSITIVE] Qdrant gRPC port (default 6334) — used when prefer_grpc=True
    qdrant_grpc_port: int = Field(default=6334, description="Qdrant gRPC port")

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

    # ── Live Monitor / Alerting ───────────────────────────────────────────────
    # Standalone multi-symbol live anomaly + news-correlation monitor
    # (src/agents/live_monitor.py). Watches Shoonya ticks during market hours,
    # scores 5-minute bars for price/volume anomalies, and pushes Slack alerts.

    # [SENSITIVE] Slack Incoming Webhook URL for live alert delivery.
    # Create one at https://api.slack.com/messaging/webhooks — leave blank to
    # disable Slack delivery (alerts are still logged to ClickHouse).
    slack_webhook_url: str = Field(default="", description="Slack Incoming Webhook URL for live alert delivery")

    # [SENSITIVE] CallMeBot WhatsApp API credentials for live alert delivery.
    # Setup (one-time, ~1 min):
    #   1. Add +34 644 597 079 to WhatsApp contacts as "CallMeBot".
    #   2. Send it the message: "I allow callmebot to send me messages"
    #   3. You'll receive your API key via WhatsApp within seconds.
    # Then set both env vars below. Leave blank to disable WhatsApp delivery.
    callmebot_whatsapp_phone: str = Field(default="", description="Your WhatsApp phone number with country code, e.g. 919876543210 (no +)")
    callmebot_whatsapp_apikey: str = Field(default="", description="CallMeBot API key received via WhatsApp")

    # [NON-SENSITIVE] Robust z-score threshold for live 5-min bar anomaly detection.
    # Same formula/scale as the EOD anomaly pipeline's z_robust, but NOT yet
    # validated on intraday bars — expect a paper-testing tuning period.
    live_monitor_zscore_threshold: float = Field(default=3.0, description="Robust z-score threshold for live bar anomaly detection")

    # [NON-SENSITIVE] Live bar aggregation interval in seconds (default 5 min).
    live_monitor_bar_seconds: int = Field(default=300, description="Live bar aggregation interval in seconds")

    # [NON-SENSITIVE] Rolling bar buffer size fed into the robust z-score (30 bars = 2.5h at 5-min bars).
    live_monitor_buffer_size: int = Field(default=30, description="Rolling bar buffer size for z-score scoring")

    # [NON-SENSITIVE] Max seconds to wait for concurrent news correlation before sending the alert.
    live_monitor_news_timeout_seconds: float = Field(default=5.0, description="Max wait for live news-correlation race before sending the alert")

    # [NON-SENSITIVE] Path to the ad-hoc watchlist config file (YAML list of symbols).
    live_monitor_watchlist_config: str = Field(default="config/live_watchlist.yaml", description="Path to ad-hoc watchlist config file")

    # [NON-SENSITIVE] Cross-symbol confirmation gate: a price_break alert on any
    # non-VIX symbol is only forwarded if INDIA VIX also moved at least this
    # much (|z_return|) in the same bar. Deliberately a lower bar than
    # live_monitor_zscore_threshold — this asks "did VIX move meaningfully",
    # not "did VIX itself trip its own full anomaly threshold". Does not gate
    # volume_spike alerts, and fails OPEN (never suppresses) whenever VIX has
    # no scored baseline yet or its data is stale by more than one bar.
    live_monitor_vix_confirmation_zscore: float = Field(
        default=2.0, description="Min |VIX z-return| in the same bar to confirm a price_break alert on another symbol"
    )

    # [NON-SENSITIVE] Polling interval (seconds) used by PollingFallbackManager when
    # Shoonya websocket is unavailable. NSE quote is tried first, Yahoo snapshot second.
    # Keep ≥60s — NSE throttles aggressive scrapers; 60s gives ~15-min-delayed data
    # which is fine for the anomaly baseline but not for tight intraday timing.
    live_monitor_poll_interval_seconds: int = Field(
        default=60, description="Poll interval (s) for NSE/Yahoo fallback when Shoonya websocket is unavailable"
    )

    # ── News Filter LLM Settings ──────────────────────────────────────────────
    # [NON-SENSITIVE] Enable semantic news filter using a local/cloud LLM
    news_filter_llm_enabled: bool = Field(default=False, description="Enable semantic news filter via LLM")

    # [NON-SENSITIVE] Custom base URL for the news filter LLM (e.g. Ollama)
    news_filter_llm_base_url: str = Field(default="http://localhost:11434/v1", description="News filter LLM base URL")

    # [NON-SENSITIVE] Model name for the news filter LLM
    news_filter_llm_model: str = Field(default="mistral:7b-instruct", description="News filter LLM model name")

    # [NON-SENSITIVE] Timeout in seconds for the news filter LLM requests
    news_filter_llm_timeout: int = Field(default=10, description="News filter LLM timeout (s)")

    def __init__(self, **values):
        super().__init__(**values)
        if "$$" in self.shoonya_password:
            self.shoonya_password = self.shoonya_password.replace("$$", "$")

        # If running locally (not inside a Docker container), 'host.docker.internal' will fail to resolve.
        # Rewrite it to '127.0.0.1' so CLI/local tools work seamlessly on the host machine.
        import socket
        import os
        
        is_docker = os.path.exists('/.dockerenv') or os.path.exists('/run/secrets/kubernetes.io') or os.environ.get('AM_I_IN_A_DOCKER_CONTAINER')
        
        if is_docker:
            # Inside Docker, localhost / 127.0.0.1 points to the container. Map to the host machine instead.
            for host in ("localhost", "127.0.0.1"):
                if self.llm_base_url and host in self.llm_base_url:
                    self.llm_base_url = self.llm_base_url.replace(host, "host.docker.internal")
                if self.code_llm_base_url and host in self.code_llm_base_url:
                    self.code_llm_base_url = self.code_llm_base_url.replace(host, "host.docker.internal")
        else:
            # Outside Docker, rewrite host.docker.internal to localhost if it's unresolvable.
            if self.llm_base_url and "host.docker.internal" in self.llm_base_url:
                try:
                    socket.gethostbyname("host.docker.internal")
                except socket.gaierror:
                    self.llm_base_url = self.llm_base_url.replace("host.docker.internal", "127.0.0.1")
            if self.code_llm_base_url and "host.docker.internal" in self.code_llm_base_url:
                try:
                    socket.gethostbyname("host.docker.internal")
                except socket.gaierror:
                    self.code_llm_base_url = self.code_llm_base_url.replace("host.docker.internal", "127.0.0.1")

    def validate_sensitive_fields(self) -> list[str]:
        """
        Returns a list of warnings for any SENSITIVE fields that are missing.
        Call this at startup to surface mis-configuration early.
        """
        warnings: list[str] = []

        # Skip API key checks when using a local model via a custom base URL
        using_local = bool(self.llm_base_url)

        if not using_local:
            if not self.openai_api_key and not self.anthropic_api_key and not self.openrouter_api_key:
                warnings.append(
                    "[SENSITIVE] Neither OPENAI_API_KEY, ANTHROPIC_API_KEY, nor OPENROUTER_API_KEY is set. "
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

            if self.llm_provider == "openrouter" and not self.openrouter_api_key:
                warnings.append(
                    "[SENSITIVE] LLM_PROVIDER=openrouter but OPENROUTER_API_KEY is not set."
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

        if not self.sec_api_key:
            warnings.append(
                "[SENSITIVE] SEC_API_KEY is not set. All sec-api.io calls (EDGAR filings, XBRL, "
                "section extraction, exec comp) will fail. "
                "Get a free key at https://sec-api.io/signup"
            )

        return warnings


# Singleton instance – import this throughout the app
settings = Settings()


# ── Monkey patch LLM classes for central temperature and Claude 5 Sonnet settings ──
# Setting temperature, top_p, or top_k to non-default values on claude-sonnet-5
# returns HTTP 400. We dynamically strip these parameters when building the client.
# For other models, we enforce settings.llm_temperature (unless it is a resolver call).
try:
    import langchain_anthropic
    _orig_anthropic_init = langchain_anthropic.chat_models.ChatAnthropic.__init__
    
    def _patched_anthropic_init(self, *args, **kwargs):
        model = kwargs.get("model") or (args[0] if args else None)
        is_resolver = kwargs.get("max_tokens") == 20
        
        # Enforce central temperature (unless resolver)
        if not is_resolver and "temperature" in kwargs:
            kwargs["temperature"] = settings.llm_temperature
            
        # Claude 5 Sonnet constraints
        if model and "sonnet-5" in str(model).lower():
            kwargs.pop("temperature", None)
            kwargs.pop("top_p", None)
            kwargs.pop("top_k", None)

        # Enforce max output tokens ceiling for Anthropic
        if "max_tokens" in kwargs and kwargs["max_tokens"] and kwargs["max_tokens"] > 64000:
            kwargs["max_tokens"] = 64000
            
        # Claude Thinking configuration control
        if not settings.llm_think:
            kwargs.pop("thinking", None)
            kwargs["thinking"] = None
            kwargs.pop("effort", None)
            kwargs["effort"] = None
            if "extra_body" in kwargs and isinstance(kwargs["extra_body"], dict):
                kwargs["extra_body"].pop("thinking", None)
                kwargs["extra_body"].pop("thinking_budget", None)
                kwargs["extra_body"].pop("thinking_effort", None)
            if "model_kwargs" in kwargs and isinstance(kwargs["model_kwargs"], dict):
                kwargs["model_kwargs"].pop("thinking", None)
                kwargs["model_kwargs"].pop("thinking_budget", None)
                kwargs["model_kwargs"].pop("thinking_effort", None)
        else:
            # If thinking is enabled and model supports it, configure it
            is_thinking_supported = model and any(x in str(model) for x in ["3-7", "3.7"])
            if is_thinking_supported:
                if kwargs.get("thinking") is None:
                    kwargs["thinking"] = {"type": "enabled", "budget_tokens": 2048}
                # Anthropic API requires temperature=1.0 when thinking is enabled
                kwargs["temperature"] = 1.0
                kwargs.pop("top_p", None)
                kwargs.pop("top_k", None)
            
        _orig_anthropic_init(self, *args, **kwargs)
        
    langchain_anthropic.chat_models.ChatAnthropic.__init__ = _patched_anthropic_init

    # ── Tool message cleanup monkey patch ─────────────────────────────────────
    # Ensures every tool_use in AIMessage has a matching ToolMessage immediately after.
    # Prevents HTTP 400 bad request errors due to orphaned tool calls in history.
    def _clean_messages_for_anthropic(messages):
        try:
            from langchain_core.messages import ToolMessage, AIMessage
        except ImportError:
            ToolMessage, AIMessage = None, None

        new_messages = []
        i = 0
        while i < len(messages):
            msg = messages[i]
            new_messages.append(msg)
            
            tool_calls = getattr(msg, "tool_calls", None)
            
            is_ai_msg = False
            if (AIMessage is not None and isinstance(msg, AIMessage)) or msg.__class__.__name__ == "AIMessage":
                is_ai_msg = True
            elif getattr(msg, "type", "") == "ai" or getattr(msg, "role", "") == "assistant":
                is_ai_msg = True

            if is_ai_msg and tool_calls:
                expected_ids = [tc["id"] for tc in tool_calls if tc.get("id")]
                if expected_ids:
                    found_tool_msgs = []
                    j = i + 1
                    while j < len(messages):
                        m = messages[j]
                        is_tool_msg = False
                        if (ToolMessage is not None and isinstance(m, ToolMessage)) or m.__class__.__name__ == "ToolMessage":
                            is_tool_msg = True
                        elif getattr(m, "type", "") == "tool" or getattr(m, "role", "") == "tool" or getattr(m, "tool_call_id", None) is not None:
                            is_tool_msg = True
                            
                        if is_tool_msg:
                            found_tool_msgs.append(m)
                            j += 1
                        else:
                            break
                    
                    found_ids = {m.tool_call_id for m in found_tool_msgs if getattr(m, "tool_call_id", None)}
                    missing_ids = [tid for tid in expected_ids if tid not in found_ids]
                    
                    if missing_ids:
                        for missing_id in missing_ids:
                            # Construct ToolMessage dynamically
                            try:
                                if ToolMessage is not None:
                                    dummy_msg = ToolMessage(
                                        content="Tool execution was interrupted or failed to return a result.",
                                        tool_call_id=missing_id,
                                        status="error"
                                    )
                                else:
                                    raise ImportError
                            except Exception:
                                from langchain_core.messages import ChatMessage
                                dummy_msg = ChatMessage(
                                    content="Tool execution was interrupted or failed to return a result.",
                                    role="tool",
                                    additional_kwargs={"tool_call_id": missing_id}
                                )
                                setattr(dummy_msg, "tool_call_id", missing_id)
                            found_tool_msgs.append(dummy_msg)
                    
                    new_messages.extend(found_tool_msgs)
                    i = j
                    continue
            i += 1
        return new_messages

    _orig_generate = langchain_anthropic.chat_models.ChatAnthropic._generate
    _orig_agenerate = langchain_anthropic.chat_models.ChatAnthropic._agenerate
    
    def _patched_generate(self, messages, stop=None, run_manager=None, **kwargs):
        cleaned = _clean_messages_for_anthropic(messages)
        return _orig_generate(self, cleaned, stop, run_manager, **kwargs)
        
    async def _patched_agenerate(self, messages, stop=None, run_manager=None, **kwargs):
        cleaned = _clean_messages_for_anthropic(messages)
        return await _orig_agenerate(self, cleaned, stop, run_manager, **kwargs)
        
    langchain_anthropic.chat_models.ChatAnthropic._generate = _patched_generate
    langchain_anthropic.chat_models.ChatAnthropic._agenerate = _patched_agenerate

except ImportError:
    pass

try:
    import langchain_openai
    _orig_openai_init = langchain_openai.chat_models.ChatOpenAI.__init__
    
    def _patched_openai_init(self, *args, **kwargs):
        is_resolver = kwargs.get("max_tokens") == 20
        if not is_resolver and "temperature" in kwargs:
            kwargs["temperature"] = settings.llm_temperature
        _orig_openai_init(self, *args, **kwargs)
        
    langchain_openai.chat_models.ChatOpenAI.__init__ = _patched_openai_init
except ImportError:
    pass

try:
    import langchain_google_genai
    _orig_google_init = langchain_google_genai.chat_models.ChatGoogleGenerativeAI.__init__
    
    def _patched_google_init(self, *args, **kwargs):
        is_resolver = kwargs.get("max_output_tokens") == 20
        if not is_resolver and "temperature" in kwargs:
            kwargs["temperature"] = settings.llm_temperature
        _orig_google_init(self, *args, **kwargs)
        
    langchain_google_genai.chat_models.ChatGoogleGenerativeAI.__init__ = _patched_google_init
except ImportError:
    pass
