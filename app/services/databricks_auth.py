import asyncio
import base64
import time
from typing import Optional

import httpx

from app.config import settings


class DatabricksAuthError(Exception):
    pass


class DatabricksAuth:
    def __init__(self) -> None:
        self._access_token: Optional[str] = None
        self._expires_at: float = 0

        # Prevent multiple concurrent OAuth refreshes.
        self._refresh_lock = asyncio.Lock()

        self._client: Optional[httpx.AsyncClient] = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(
                    connect=10.0,
                    read=30.0,
                    write=30.0,
                    pool=10.0,
                ),
                limits=httpx.Limits(
                    max_connections=20,
                    max_keepalive_connections=10,
                    keepalive_expiry=30.0,
                ),
            )

        return self._client

    async def get_access_token(
        self,
        force_refresh: bool = False,
    ) -> str:

        # Fast path: existing token is still valid
        if (
            not force_refresh
            and self._access_token
            and time.time() < self._expires_at - 60
        ):
            return self._access_token

        # Only one coroutine should refresh the token.
        async with self._refresh_lock:

            # we were waiting for the lock
            if (
                not force_refresh
                and self._access_token
                and time.time() < self._expires_at - 60
            ):
                return self._access_token

            credentials = (
                f"{settings.databricks_client_id}:"
                f"{settings.databricks_client_secret}"
            )

            encoded_credentials = base64.b64encode(
                credentials.encode("ascii")
            ).decode("ascii")

            headers = {
                "Authorization": (
                    f"Basic {encoded_credentials}"
                ),
                "Content-Type": (
                    "application/x-www-form-urlencoded"
                ),
            }

            data = {
                "grant_type": "client_credentials",
                "scope": "genie",
            }

            token_url = (
                f"{settings.databricks_host}"
                "/oidc/v1/token"
            )

            client = await self._get_client()

            last_error = None

            for attempt in range(3):

                try:
                    response = await client.post(
                        token_url,
                        headers=headers,
                        data=data,
                    )

                except httpx.RequestError as exc:
                    last_error = exc

                    if attempt == 2:
                        raise DatabricksAuthError(
                            "Unable to connect to "
                            "Databricks OAuth endpoint."
                        ) from exc

                    await asyncio.sleep(
                        0.5 * (2 ** attempt)
                    )
                    continue

                # transient server/rate-limit failures.
                if response.status_code in {
                    429,
                    500,
                    502,
                    503,
                    504,
                }:
                    last_error = response.text

                    if attempt == 2:
                        raise DatabricksAuthError(
                            "Databricks OAuth failed after "
                            "multiple attempts: "
                            f"{response.status_code}"
                        )

                    retry_after = (
                        response.headers.get(
                            "Retry-After"
                        )
                    )

                    try:
                        delay = float(retry_after)
                    except (
                        TypeError,
                        ValueError,
                    ):
                        delay = 0.5 * (2 ** attempt)

                    await asyncio.sleep(
                        min(delay, 10.0)
                    )
                    continue

                if response.status_code != 200:
                    raise DatabricksAuthError(
                        "Databricks OAuth failed: "
                        f"{response.status_code}"
                    )

                try:
                    payload = response.json()
                except ValueError as exc:
                    raise DatabricksAuthError(
                        "Databricks OAuth returned "
                        "invalid JSON."
                    ) from exc

                access_token = payload.get(
                    "access_token"
                )

                expires_in = payload.get(
                    "expires_in",
                    3600,
                )

                if not access_token:
                    raise DatabricksAuthError(
                        "Databricks OAuth response did "
                        "not contain an access_token."
                    )

                try:
                    expires_in = float(expires_in)
                except (
                    TypeError,
                    ValueError,
                ):
                    expires_in = 3600.0

                # to create an immediately expired token.
                expires_in = max(
                    expires_in,
                    60.0,
                )

                self._access_token = (
                    str(access_token)
                )

                self._expires_at = (
                    time.time()
                    + expires_in
                )

                return self._access_token

            raise DatabricksAuthError(
                "Databricks OAuth authentication failed."
            )

    async def invalidate_token(
        self,
        token: Optional[str] = None,
    ) -> None:

        async with self._refresh_lock:
            # invalidating a newer token.
            if (
                token is None
                or token == self._access_token
            ):
                self._access_token = None
                self._expires_at = 0

    async def close(self) -> None:

        if self._client is not None:
            await self._client.aclose()
            self._client = None

        self._access_token = None
        self._expires_at = 0


databricks_auth = DatabricksAuth()