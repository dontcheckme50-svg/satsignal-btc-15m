import os
from dataclasses import dataclass

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


@dataclass(frozen=True)
class Settings:
    trading_mode: str = "paper"  # Live execution is intentionally disabled.
    paper_bankroll_usd: float = float(os.getenv("PAPER_BANKROLL_USD", "1000.0"))
    kelly_fraction: float = float(os.getenv("KELLY_FRACTION", "0.25"))
    max_risk_per_trade_usd: float = float(os.getenv("MAX_RISK_PER_TRADE_USD", "25.0"))
    max_daily_loss_usd: float = float(os.getenv("MAX_DAILY_LOSS_USD", "100.0"))
    min_net_edge_cents: float = float(os.getenv("MIN_NET_EDGE_CENTS", "0.03"))

    # Supabase PostgreSQL is the only supported persistence backend.
    database_url: str | None = os.getenv("DATABASE_URL") or None

    kalshi_public_base_url: str = os.getenv("KALSHI_PUBLIC_BASE_URL", "https://api.elections.kalshi.com/trade-api/v2")
    kalshi_demo_base_url: str = os.getenv("KALSHI_DEMO_BASE_URL", "https://demo-api.kalshi.co/trade-api/v2")
    kalshi_prod_base_url: str = os.getenv("KALSHI_PROD_BASE_URL", "https://external-api.kalshi.com/trade-api/v2")
    kalshi_key_id: str | None = os.getenv("KALSHI_KEY_ID") or None
    kalshi_private_key: str | None = os.getenv("KALSHI_PRIVATE_KEY") or None
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
if not settings.database_url:
    raise RuntimeError(
        "DATABASE_URL is required. Configure the Supabase PostgreSQL connection string; "
        "local files, SQLite, and in-memory storage are not supported."
    )
if not settings.database_url.startswith(("postgres://", "postgresql://")):
    raise RuntimeError("DATABASE_URL must be a PostgreSQL/Supabase connection string.")

storage_mount_status = {
    "status": "MANAGED_DATABASE_CONFIGURED",
    "managed_database_configured": True,
    "storage_type": "POSTGRESQL",
    "provider": "SUPABASE",
    "database_url_configured": True,
    "mount_path": None,
    "database_path": None,
}
persistent_storage_required = True
