import hashlib
import secrets
import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional

from pwdlib import PasswordHash

from app.config import settings
from app.services.database import db


class AuthError(Exception):
    """Authentication-related application error."""


class AuthService:

    def __init__(self) -> None:
        # PasswordHash.recommended() currently provides a secure
        # password hashing configuration, including Argon2id when
        # the required backend is available.
        self.password_hash = PasswordHash.recommended()

    async def initialize(self) -> None:
        """
        Create the authentication database objects if they do not exist.
        """

        await db.execute(
            f"""
            CREATE SCHEMA IF NOT EXISTS
            "{settings.postgres_schema}"
            """
        )

        await db.execute(
            f"""
            CREATE TABLE IF NOT EXISTS
            "{settings.postgres_schema}".users (
                username TEXT PRIMARY KEY,
                password_hash TEXT NOT NULL,
                is_active BOOLEAN NOT NULL DEFAULT TRUE,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )

        # Preserve the existing single-user configuration by migrating
        # the configured application account into the users table.
        await db.execute(
            f"""
            INSERT INTO "{settings.postgres_schema}".users
                (username, password_hash)
            VALUES (%s, %s)
            ON CONFLICT (username) DO NOTHING
            """,
            (settings.app_username, settings.app_password_hash),
        )

        await db.execute(
            f"""
            CREATE TABLE IF NOT EXISTS
            "{settings.postgres_schema}".auth_sessions (
                session_id TEXT PRIMARY KEY,
                token_hash TEXT NOT NULL UNIQUE,
                username TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL,
                expires_at TIMESTAMPTZ NOT NULL
            )
            """
        )

        await db.execute(
            f"""
            CREATE INDEX IF NOT EXISTS
            idx_auth_sessions_expires_at
            ON "{settings.postgres_schema}".auth_sessions
            (expires_at)
            """
        )

        await db.execute(
            f"""
            CREATE INDEX IF NOT EXISTS
            idx_auth_sessions_username
            ON "{settings.postgres_schema}".auth_sessions
            (username)
            """
        )

    @staticmethod
    def _hash_token(token: str) -> str:
        """
        Hash the browser session token before storing it in PostgreSQL.

        The plaintext token is only given to the browser through the
        HttpOnly cookie. PostgreSQL stores only its SHA-256 digest.
        """

        return hashlib.sha256(
            token.encode("utf-8")
        ).hexdigest()

    async def authenticate(
        self,
        username: str,
        password: str,
    ) -> bool:
        """Authenticate against the PostgreSQL users table."""

        row = await db.fetch_one(
            f"""
            SELECT password_hash, is_active
            FROM "{settings.postgres_schema}".users
            WHERE username = %s
            """,
            (username,),
        )

        if row is None or not row["is_active"]:
            return False

        try:
            return self.password_hash.verify(
                row["password_hash"],
                password,
            )
        except Exception:
            return False

    async def create_session(
        self,
        username: str,
    ) -> str:
        """
        Create a new authenticated browser session.

        Only the SHA-256 hash of the session token is persisted.
        """

        session_id = str(uuid.uuid4())

        token = secrets.token_urlsafe(48)

        token_hash = self._hash_token(token)

        now = datetime.now(timezone.utc)

        expires_at = (
            now
            + timedelta(
                hours=settings.app_auth_session_ttl_hours
            )
        )

        await db.execute(
            f"""
            INSERT INTO
            "{settings.postgres_schema}".auth_sessions
            (
                session_id,
                token_hash,
                username,
                created_at,
                expires_at
            )
            VALUES (%s, %s, %s, %s, %s)
            """,
            (
                session_id,
                token_hash,
                username,
                now,
                expires_at,
            ),
        )

        return token

    async def validate_session(
        self,
        token: str,
    ) -> Optional[str]:
        """
        Validate an authentication session token.

        Returns:
            Username associated with the valid session,
            or None when the session is invalid/expired.
        """

        if not token:
            return None

        token_hash = self._hash_token(token)

        row = await db.fetch_one(
            f"""
            SELECT
                username,
                expires_at
            FROM
                "{settings.postgres_schema}".auth_sessions
            WHERE
                token_hash = %s
            """,
            (token_hash,),
        )

        if row is None:
            return None

        expires_at = row["expires_at"]

        if datetime.now(timezone.utc) >= expires_at:
            await self.delete_session(token)
            return None

        return row["username"]

    async def delete_session(
        self,
        token: str,
    ) -> None:
        """
        Delete an authenticated browser session.
        """

        if not token:
            return

        token_hash = self._hash_token(token)

        await db.execute(
            f"""
            DELETE FROM
                "{settings.postgres_schema}".auth_sessions
            WHERE
                token_hash = %s
            """,
            (token_hash,),
        )

    async def cleanup_expired_sessions(
        self,
    ) -> None:
        """
        Remove expired authentication sessions.

        This can later be called periodically by a background
        cleanup task or scheduled maintenance job.
        """

        await db.execute(
            f"""
            DELETE FROM
                "{settings.postgres_schema}".auth_sessions
            WHERE
                expires_at < NOW()
            """
        )


auth_service = AuthService()
