import asyncio
import base64
import time
from dataclasses import dataclass
from typing import Optional

import httpx
import streamlit as st


class Settings:
    def __init__(self):
        self.databricks_host = str(st.secrets["DATABRICKS_HOST"]).rstrip("/")
        self.databricks_client_id = str(st.secrets["DATABRICKS_CLIENT_ID"])
        self.databricks_client_secret = str(st.secrets["DATABRICKS_CLIENT_SECRET"])
        self.genie_space_id = str(st.secrets["GENIE_SPACE_ID"])
        self.genie_poll_interval_seconds = float(st.secrets.get("GENIE_POLL_INTERVAL_SECONDS", 1.0))
        self.genie_timeout_seconds = int(st.secrets.get("GENIE_TIMEOUT_SECONDS", 1800))
        self.genie_stream_retries = int(st.secrets.get("GENIE_STREAM_RETRIES", 2))
        self.genie_tcp_keepalive = str(st.secrets.get("GENIE_TCP_KEEPALIVE", "true")).lower() in {"1", "true", "yes", "on"}
        self.postgres_host = str(st.secrets.get("POSTGRES_HOST", "127.0.0.1"))
        self.postgres_port = int(st.secrets.get("POSTGRES_PORT", 5432))
        self.postgres_database = str(st.secrets["POSTGRES_DATABASE"])
        self.postgres_user = str(st.secrets["POSTGRES_USER"])
        self.postgres_password = str(st.secrets["POSTGRES_PASSWORD"])
        self.postgres_schema = str(st.secrets.get("POSTGRES_SCHEMA", "genie_app"))
        self.app_username = str(st.secrets["APP_USERNAME"])
        self.app_password = str(st.secrets["APP_PASSWORD"])


@st.cache_resource
def get_settings():
    return Settings()


settings = get_settings()


class DatabricksAuthError(Exception):
    pass


class DatabricksAuth:
    def __init__(self):
        self._access_token: Optional[str] = None
        self._expires_at = 0.0
        self._client: Optional[httpx.AsyncClient] = None

    async def _get_client(self):
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(connect=30.0, read=60.0, write=30.0, pool=10.0),
                limits=httpx.Limits(max_connections=20, max_keepalive_connections=10, keepalive_expiry=30.0),
            )
        return self._client

    async def get_access_token(self, force_refresh: bool = False) -> str:
        if not force_refresh and self._access_token and time.time() < self._expires_at - 60:
            return self._access_token
        credentials = f"{settings.databricks_client_id}:{settings.databricks_client_secret}"
        encoded = base64.b64encode(credentials.encode("ascii")).decode("ascii")
        response = await (await self._get_client()).post(
            f"{settings.databricks_host}/oidc/v1/token",
            headers={"Authorization": f"Basic {encoded}", "Content-Type": "application/x-www-form-urlencoded"},
            data={"grant_type": "client_credentials", "scope": "genie"},
        )
        if response.status_code >= 400:
            raise DatabricksAuthError(f"Databricks OAuth failed: HTTP {response.status_code}: {response.text[:1000]}")
        payload = response.json()
        token = payload.get("access_token")
        if not token:
            raise DatabricksAuthError("Databricks OAuth response did not contain access_token.")
        self._access_token = token
        self._expires_at = time.time() + int(payload.get("expires_in", 3600))
        return token

    async def invalidate_token(self, token: str | None = None):
        self._access_token = None
        self._expires_at = 0.0

    async def close(self):
        if self._client and not self._client.is_closed:
            await self._client.aclose()
        self._client = None


@st.cache_resource
def get_databricks_auth():
    return DatabricksAuth()


databricks_auth = get_databricks_auth()
