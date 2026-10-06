import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional

from psycopg.types.json import Jsonb

from app.config import settings
from app.services.database import db


class SessionStoreError(Exception):
    pass


class SessionStore:

    async def initialize(self) -> None:

        schema = settings.postgres_schema

        await db.execute(
            f"""
            CREATE SCHEMA IF NOT EXISTS
            "{schema}"
            """
        )

        # =====================================================
        # CHAT SESSIONS
        # =====================================================

        await db.execute(
            f"""
            CREATE TABLE IF NOT EXISTS
            "{schema}".chat_sessions (

                session_id TEXT PRIMARY KEY,

                genie_conversation_id TEXT NOT NULL,

                username TEXT NOT NULL,

                title TEXT NOT NULL,

                created_at TIMESTAMPTZ NOT NULL,

                last_activity_at TIMESTAMPTZ NOT NULL,

                expires_at TIMESTAMPTZ NOT NULL
            )
            """
        )

        # =====================================================
        # CHAT MESSAGES
        # =====================================================

        await db.execute(
            f"""
            CREATE TABLE IF NOT EXISTS
            "{schema}".chat_messages (

                message_id TEXT PRIMARY KEY,

                session_id TEXT NOT NULL,

                role TEXT NOT NULL,

                content TEXT NOT NULL,

                metadata JSONB NOT NULL DEFAULT '{{}}'::jsonb,

                created_at TIMESTAMPTZ NOT NULL,

                CONSTRAINT fk_chat_session
                    FOREIGN KEY (session_id)
                    REFERENCES "{schema}".chat_sessions(session_id)
                    ON DELETE CASCADE,

                CONSTRAINT valid_chat_role
                    CHECK (
                        role IN ('user', 'assistant')
                    )
            )
            """
        )

        # Existing installations need the new rich-response column too.
        await db.execute(
            f"""
            ALTER TABLE "{schema}".chat_messages
            ADD COLUMN IF NOT EXISTS
                metadata JSONB NOT NULL DEFAULT '{{}}'::jsonb
            """
        )

        # =====================================================
        # INDEXES
        # =====================================================

        await db.execute(
            f"""
            CREATE INDEX IF NOT EXISTS
            idx_chat_sessions_username
            ON "{schema}".chat_sessions
            (username)
            """
        )

        await db.execute(
            f"""
            CREATE INDEX IF NOT EXISTS
            idx_chat_sessions_activity
            ON "{schema}".chat_sessions
            (username, last_activity_at DESC)
            """
        )

        await db.execute(
            f"""
            CREATE INDEX IF NOT EXISTS
            idx_chat_sessions_expires
            ON "{schema}".chat_sessions
            (expires_at)
            """
        )

        await db.execute(
            f"""
            CREATE INDEX IF NOT EXISTS
            idx_chat_messages_session
            ON "{schema}".chat_messages
            (session_id, created_at ASC)
            """
        )

    async def create_session(
        self,
        genie_conversation_id: str,
        username: str,
        title: str,
    ) -> str:

        session_id = str(uuid.uuid4())

        now = datetime.now(timezone.utc)

        expires_at = (
            now
            + timedelta(
                hours=settings.app_chat_session_ttl_hours
            )
        )

        await db.execute(
            f"""
            INSERT INTO
            "{settings.postgres_schema}".chat_sessions
            (
                session_id,
                genie_conversation_id,
                username,
                title,
                created_at,
                last_activity_at,
                expires_at
            )
            VALUES (
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s
            )
            """,
            (
                session_id,
                genie_conversation_id,
                username,
                title,
                now,
                now,
                expires_at,
            ),
        )

        return session_id

    async def get_genie_conversation_id(
        self,
        session_id: str,
        username: str,
    ) -> Optional[str]:

        row = await db.fetch_one(
            f"""
            SELECT
                genie_conversation_id
            FROM
                "{settings.postgres_schema}".chat_sessions
            WHERE
                session_id = %s
                AND username = %s
                AND expires_at > NOW()
            """,
            (
                session_id,
                username,
            ),
        )

        if row is None:
            return None

        return row["genie_conversation_id"]

    async def set_genie_conversation_id(
        self,
        session_id: str,
        username: str,
        genie_conversation_id: str,
    ) -> bool:

        await db.execute(
            f"""
            UPDATE "{settings.postgres_schema}".chat_sessions
            SET
                genie_conversation_id = %s,
                last_activity_at = NOW()
            WHERE
                session_id = %s
                AND username = %s
                AND expires_at > NOW()
            """,
            (
                genie_conversation_id,
                session_id,
                username,
            ),
        )

        return True

    async def find_session_by_genie_conversation(
        self,
        genie_conversation_id: str,
        username: str,
    ) -> Optional[str]:

        row = await db.fetch_one(
            f"""
            SELECT session_id
            FROM "{settings.postgres_schema}".chat_sessions
            WHERE genie_conversation_id = %s
              AND username = %s
              AND expires_at > NOW()
            LIMIT 1
            """,
            (genie_conversation_id, username),
        )

        return row["session_id"] if row else None

    async def list_sessions(
        self,
        username: str,
    ) -> list[dict]:

        await self.cleanup_expired_sessions()

        rows = await db.fetch_all(
            f"""
            SELECT
                session_id,
                title,
                created_at,
                last_activity_at
            FROM
                "{settings.postgres_schema}".chat_sessions
            WHERE
                username = %s
                AND expires_at > NOW()
            ORDER BY
                last_activity_at DESC
            """,
            (username,),
        )

        return rows

    async def get_chat_history(
        self,
        session_id: str,
        username: str,
    ) -> Optional[dict]:

        session = await db.fetch_one(
            f"""
            SELECT
                session_id,
                title,
                created_at,
                last_activity_at
            FROM
                "{settings.postgres_schema}".chat_sessions
            WHERE
                session_id = %s
                AND username = %s
                AND expires_at > NOW()
            """,
            (
                session_id,
                username,
            ),
        )

        if session is None:
            return None

        messages = await db.fetch_all(
            f"""
            SELECT
                role,
                content,
                metadata,
                created_at
            FROM
                "{settings.postgres_schema}".chat_messages
            WHERE
                session_id = %s
            ORDER BY
                created_at ASC
            """,
            (session_id,),
        )

        return {
            "session_id": session["session_id"],
            "title": session["title"],
            "created_at": session["created_at"].isoformat(),
            "last_activity_at": session[
                "last_activity_at"
            ].isoformat(),
            "messages": [
                {
                    "role": message["role"],
                    "content": message["content"],
                    "presentation": message.get("metadata") or {},
                    "created_at": message[
                        "created_at"
                    ].isoformat(),
                }
                for message in messages
            ],
        }

    async def add_message(
        self,
        session_id: str,
        username: str,
        role: str,
        content: str,
        metadata: Optional[dict] = None,
    ) -> str:

        session = await db.fetch_one(
            f"""
            SELECT
                session_id
            FROM
                "{settings.postgres_schema}".chat_sessions
            WHERE
                session_id = %s
                AND username = %s
                AND expires_at > NOW()
            """,
            (
                session_id,
                username,
            ),
        )

        if session is None:
            raise SessionStoreError(
                "Chat session not found."
            )

        message_id = str(uuid.uuid4())

        now = datetime.now(timezone.utc)

        await db.execute(
            f"""
            INSERT INTO
            "{settings.postgres_schema}".chat_messages
            (
                message_id,
                session_id,
                role,
                content,
                metadata,
                created_at
            )
            VALUES (
                %s,
                %s,
                %s,
                %s,
                %s,
                %s
            )
            """,
            (
                message_id,
                session_id,
                role,
                content,
                Jsonb(metadata or {}),
                now,
            ),
        )

        await self.update_activity(
            session_id,
            username,
        )

        return message_id

    async def update_activity(
        self,
        session_id: str,
        username: str,
    ) -> None:

        now = datetime.now(timezone.utc)

        expires_at = (
            now
            + timedelta(
                hours=settings.app_chat_session_ttl_hours
            )
        )

        await db.execute(
            f"""
            UPDATE
                "{settings.postgres_schema}".chat_sessions
            SET
                last_activity_at = %s,
                expires_at = %s
            WHERE
                session_id = %s
                AND username = %s
            """,
            (
                now,
                expires_at,
                session_id,
                username,
            ),
        )

    async def delete_session(
        self,
        session_id: str,
        username: str,
    ) -> None:

        await db.execute(
            f"""
            DELETE FROM
                "{settings.postgres_schema}".chat_sessions
            WHERE
                session_id = %s
                AND username = %s
            """,
            (
                session_id,
                username,
            ),
        )

    async def cleanup_expired_sessions(self) -> None:

        await db.execute(
            f"""
            DELETE FROM
                "{settings.postgres_schema}".chat_sessions
            WHERE
                expires_at < NOW()
            """)

        await db.execute(
            f"""
            DELETE FROM
                "{settings.postgres_schema}".auth_sessions
            WHERE
                expires_at < NOW()
            """)


session_store = SessionStore()