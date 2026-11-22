import os
from dataclasses import dataclass

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

@dataclass(frozen=True)
class Settings:
    trading_mode: str = "paper"  # Live execution intentionally disabled in this release.
    paper_bankroll_usd: float = float(os.getenv("PAPER_BANKROLL_USD", "1000.0"))
    kelly_fraction: float = float(os.getenv("KELLY_FRACTION", "0.25"))
    max_risk_per_trade_usd: float = float(os.getenv("MAX_RISK_PER_TRADE_USD", "25.0"))
    max_daily_loss_usd: float = float(os.getenv("MAX_DAILY_LOSS_USD", "100.0"))
    min_net_edge_cents: float = float(os.getenv("MIN_NET_EDGE_CENTS", "0.03"))
    # In-memory is the default by request. Set PREDICTOR_IN_MEMORY_STORAGE=0
    # to opt back into the configured SQLite/PostgreSQL backend.
    in_memory_storage: bool = os.getenv("PREDICTOR_IN_MEMORY_STORAGE", "1").strip().lower() in {"1", "true", "yes", "on"}
    db_path: str = os.getenv(
        "PREDICTOR_DB_PATH",
        ":memory:" if os.getenv("PREDICTOR_IN_MEMORY_STORAGE", "1").strip().lower() in {"1", "true", "yes", "on"} else "satsignal_btc15m.db",
    )
    database_url: str | None = (
        None if os.getenv("PREDICTOR_IN_MEMORY_STORAGE", "1").strip().lower() in {"1", "true", "yes", "on"}
        else (os.getenv("DATABASE_URL") or None)
    )
    kalshi_public_base_url: str = os.getenv("KALSHI_PUBLIC_BASE_URL", "https://api.elections.kalshi.com/trade-api/v2")
    kalshi_demo_base_url: str = os.getenv("KALSHI_DEMO_BASE_URL", "https://demo-api.kalshi.co/trade-api/v2")
    kalshi_prod_base_url: str = os.getenv("KALSHI_PROD_BASE_URL", "https://external-api.kalshi.com/trade-api/v2")
    kalshi_key_id: str | None = os.getenv("KALSHI_KEY_ID") or None
    kalshi_private_key_path: str | None = os.getenv("KALSHI_PRIVATE_KEY_PATH") or None
    binance_ws_url: str = "wss://stream.binance.com:9443/stream?streams=btcusdt@aggTrade/btcusdt@depth20@100ms"
    coinbase_ws_url: str = "wss://ws-feed.exchange.coinbase.com"
    early_window_seconds: int = 300
    final_lockout_seconds: int = 65
    stale_feed_threshold_sec: float = 5.0
    checkpoint_interval_sec: int = 180
    host: str = os.getenv("HOST", "0.0.0.0")
    port: int = int(os.getenv("PORT", "8000"))

settings = Settings()

# Production can fail closed unless Hostless confirms a writable persistent mount.
# Leave disabled for local development and CI; enable it in the Hostless app only
# after a durable volume is actually attached at PREDICTOR_STORAGE_MOUNT.
from app.storage_guard import enforce_persistent_storage

storage_mount_path = os.getenv("PREDICTOR_STORAGE_MOUNT", "/data")
persistent_storage_required = os.getenv(
    "PREDICTOR_REQUIRE_PERSISTENT_STORAGE", "0"
).strip().lower() in {"1", "true", "yes", "on"}
storage_mount_status = (
    {
        "status": "MANAGED_DATABASE_CONFIGURED",
        "persistent_mount_confirmed": None,
        "managed_database_configured": True,
        "storage_type": "POSTGRESQL",
        "database_url_configured": True,
        "mount_path": None,
        "database_path": None,
    }
    if settings.database_url
    else enforce_persistent_storage(
        settings.db_path,
        storage_mount_path,
        required=persistent_storage_required,
    )
)

