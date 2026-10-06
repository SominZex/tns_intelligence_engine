from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):

    # =========================================================
    # DATABRICKS
    # =========================================================

    databricks_host: str
    databricks_client_id: str
    databricks_client_secret: str

    genie_space_id: str

    genie_poll_interval_seconds: float = 1.0

    # genie_client enforces a floor of 1800s for Agent runs, because
    # forecasting/analysis runs can take many minutes.
    genie_timeout_seconds: int = 600

    # How many times an interrupted Agent SSE stream (for example
    # httpx.ReadError) is retried on a fresh connection before the
    # request is reported as failed. 0 disables retries.
    genie_stream_retries: int = 2

    # Enable TCP keep-alive on the Agent stream connection so that idle
    # periods during long agent computation do not drop the connection.
    # Set to false if your OS/network rejects the socket options.
    genie_tcp_keepalive: bool = True

    # Currency symbol used when the agent writes "$" in front of an amount.
    # Your data is in Indian Rupees. Set to an empty string to disable the
    # rewrite. (The durable fix is a Genie-space instruction; see notes.)
    genie_currency_symbol: str = "₹"

    # Debug aid: when set to a folder path, every Agent run writes the raw
    # SSE response and the Conversation-API message projection there as
    # JSON, so you can compare the app with the Databricks UI exactly.
    genie_debug_dump_dir: str = ""

    # =========================================================
    # APPLICATION AUTHENTICATION
    # =========================================================

    app_username: str

    # Argon2id password hash.
    # Never store the plaintext application password here.
    app_password_hash: str

    # Lifetime of the browser authentication session.
    app_auth_session_ttl_hours: int = 12

    # =========================================================
    # CHAT SESSION
    # =========================================================

    # Lifetime of an individual Genie chat session.
    app_chat_session_ttl_hours: int = 24

    # =========================================================
    # POSTGRESQL
    # =========================================================

    postgres_host: str = "127.0.0.1"
    postgres_port: int = 5432
    postgres_database: str
    postgres_user: str
    postgres_password: str

    postgres_schema: str = "genie_app"

    postgres_min_connections: int = 2
    postgres_max_connections: int = 10

    postgres_connect_timeout: int = 10

    # =========================================================
    # COOKIE
    # =========================================================

    # False for local HTTP development.
    # Must be True when deployed behind HTTPS.
    app_cookie_secure: bool = False

    # Lax is appropriate for the current same-site application.
    app_cookie_samesite: str = "lax"

    # =========================================================
    # LEGACY SQLITE SETTING
    # =========================================================

    # Kept temporarily so existing configuration does not break.
    # SQLite is no longer used as the primary application store.
    sqlite_timeout_seconds: int = 30

    # =========================================================
    # PYDANTIC SETTINGS
    # =========================================================

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )


settings = Settings()