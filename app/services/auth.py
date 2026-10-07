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
        # PasswordHash.recommended() provides a secure password hashing
        # configuration, including Argon2id when the required backend
        # is available.
        self.password_hash = PasswordHash.recommended()

    async def initialize(self) -> None:
        """Create authentication database objects if they do not exist."""

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
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )

        # Preserve the existing configured application account.
        await db.execute(
            f"""
            INSERT INTO "{settings.postgres_schema}".users
                (username, password_hash)
            VALUES (%s, %s)
            ON CONFLICT (username) DO NOTHING
            """,
            (
                settings.app_username,
                settings.app_password_hash,
            ),
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
        """Hash the browser session token before storing it."""
        return hashlib.sha256(
            token.encode("utf-8")
        ).hexdigest()

    async def authenticate(
        self,
        username: str,
        password: str,
    ) -> bool:
        """Authenticate a user against the PostgreSQL users table."""

        if not username or not password:
            return False

        row = await db.fetch_one(
            f"""
            SELECT
                password_hash,
                is_active
            FROM
                "{settings.postgres_schema}".users
            WHERE
                username = %s
            LIMIT 1
            """,
            (username,),
        )

        if row is None or not row["is_active"]:
            return False

        try:
            # pwdlib.verify() expects plaintext password first,
            # followed by the stored password hash.
            return self.password_hash.verify(
                password,
                row["password_hash"],
            )
        except Exception:
            return False

    async def create_user(
        self,
        username: str,
        password: str,
    ) -> bool:
        """
        Create a new active user.

        Returns True when created and False when the username already exists.
        """

        username = username.strip()

        if not username or not password:
            raise ValueError("Username and password are required.")

        password_hash = self.password_hash.hash(password)

        try:
            await db.execute(
                f"""
                INSERT INTO "{settings.postgres_schema}".users
                    (
                        username,
                        password_hash,
                        is_active
                    )
                VALUES (%s, %s, TRUE)
                """,
                (
                    username,
                    password_hash,
                ),
            )
            return True

        except Exception as exc:
            # PostgreSQL duplicate-key errors are intentionally translated
            # into a clean False result for the admin API.
            if "duplicate key" in str(exc).lower() or "unique" in str(exc).lower():
                return False
            raise

    async def list_users(self) -> list[dict]:
        """Return users for the administrator interface."""

        rows = await db.fetch_all(
            f"""
            SELECT
                username,
                is_active,
                created_at,
                updated_at
            FROM
                "{settings.postgres_schema}".users
            ORDER BY
                username ASC
            """
        )

        return [
            {
                "username": row["username"],
                "is_active": row["is_active"],
                "created_at": row["created_at"].isoformat(),
                "updated_at": row["updated_at"].isoformat(),
            }
            for row in rows
        ]

    async def set_user_active(
        self,
        username: str,
        is_active: bool,
    ) -> bool:
        """Activate or deactivate a user."""

        username = username.strip()

        if username == settings.app_username and not is_active:
            raise ValueError("The administrator account cannot be deactivated.")

        result = await db.execute(
            f"""
            UPDATE
                "{settings.postgres_schema}".users
            SET
                is_active = %s,
                updated_at = NOW()
            WHERE
                username = %s
            """,
            (
                is_active,
                username,
            ),
        )

        # The database service returns the underlying cursor result in the
        # current application, so existence is checked explicitly as well.
        row = await db.fetch_one(
            f"""
            SELECT username
            FROM "{settings.postgres_schema}".users
            WHERE username = %s
            """,
            (username,),
        )

        return row is not None

    async def reset_password(
        self,
        username: str,
        new_password: str,
    ) -> bool:
        """Set a new password for an existing user."""

        username = username.strip()

        if not new_password:
            raise ValueError("Password is required.")

        password_hash = self.password_hash.hash(new_password)

        await db.execute(
            f"""
            UPDATE
                "{settings.postgres_schema}".users
            SET
                password_hash = %s,
                updated_at = NOW(),
                is_active = TRUE
            WHERE
                username = %s
            """,
            (
                password_hash,
                username,
            ),
        )

        row = await db.fetch_one(
            f"""
            SELECT username
            FROM "{settings.postgres_schema}".users
            WHERE username = %s
            """,
            (username,),
        )

        return row is not None

    async def create_session(
        self,
        username: str,
    ) -> str:
        """Create a new authenticated browser session."""

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
        """Validate a browser session and return its username."""

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
        """Delete an authenticated browser session."""

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
        """Remove expired authentication sessions."""

        await db.execute(
            f"""
            DELETE FROM
                "{settings.postgres_schema}".auth_sessions
            WHERE
                expires_at < NOW()
            """
        )


auth_service = AuthService()
