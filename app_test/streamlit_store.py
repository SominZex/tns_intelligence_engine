import json
import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from streamlit_runtime import settings


class Store:
    def __init__(self):
        self.conninfo = (
            f"host={settings.postgres_host} port={settings.postgres_port} "
            f"dbname={settings.postgres_database} user={settings.postgres_user} "
            f"password={settings.postgres_password} connect_timeout=10"
        )

    def connect(self):
        return psycopg.connect(self.conninfo, row_factory=dict_row)

    def initialize(self):
        schema = settings.postgres_schema
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
                cur.execute(f'''CREATE TABLE IF NOT EXISTS "{schema}".chat_sessions (
                    session_id TEXT PRIMARY KEY,
                    genie_conversation_id TEXT NOT NULL,
                    username TEXT NOT NULL,
                    title TEXT NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL,
                    last_activity_at TIMESTAMPTZ NOT NULL,
                    expires_at TIMESTAMPTZ NOT NULL
                )''')
                cur.execute(f'''CREATE TABLE IF NOT EXISTS "{schema}".chat_messages (
                    message_id TEXT PRIMARY KEY,
                    session_id TEXT NOT NULL REFERENCES "{schema}".chat_sessions(session_id) ON DELETE CASCADE,
                    role TEXT NOT NULL CHECK (role IN ('user','assistant')),
                    content TEXT NOT NULL,
                    metadata JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL
                )''')
                cur.execute(f'''ALTER TABLE "{schema}".chat_messages
                    ADD COLUMN IF NOT EXISTS metadata JSONB NOT NULL DEFAULT '{{}}'::jsonb''')
                cur.execute(f'''CREATE INDEX IF NOT EXISTS idx_chat_sessions_username
                    ON "{schema}".chat_sessions(username, last_activity_at DESC)''')
                cur.execute(f'''CREATE INDEX IF NOT EXISTS idx_chat_messages_session
                    ON "{schema}".chat_messages(session_id, created_at)''')
            conn.commit()

    def create_session(self, genie_conversation_id, username, title):
        now = datetime.now(timezone.utc)
        sid = str(uuid.uuid4())
        expires = now + timedelta(hours=24)
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(f'''INSERT INTO "{settings.postgres_schema}".chat_sessions
                    (session_id, genie_conversation_id, username, title, created_at, last_activity_at, expires_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s)''', (sid, genie_conversation_id, username, title, now, now, expires))
            conn.commit()
        return sid

    def get_conversation_id(self, session_id, username):
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(f'''SELECT genie_conversation_id FROM "{settings.postgres_schema}".chat_sessions
                    WHERE session_id=%s AND username=%s AND expires_at > now()''', (session_id, username))
                row = cur.fetchone()
                return row["genie_conversation_id"] if row else None

    def set_conversation_id(self, session_id, username, conversation_id):
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(f'''UPDATE "{settings.postgres_schema}".chat_sessions
                    SET genie_conversation_id=%s, last_activity_at=now(), expires_at=now()+interval '24 hours'
                    WHERE session_id=%s AND username=%s''', (conversation_id, session_id, username))
            conn.commit()

    def list_sessions(self, username):
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(f'''SELECT session_id,title,created_at,last_activity_at
                    FROM "{settings.postgres_schema}".chat_sessions
                    WHERE username=%s AND expires_at > now()
                    ORDER BY last_activity_at DESC''', (username,))
                return cur.fetchall()

    def get_history(self, session_id, username):
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(f'''SELECT session_id,title,created_at,last_activity_at
                    FROM "{settings.postgres_schema}".chat_sessions
                    WHERE session_id=%s AND username=%s AND expires_at > now()''', (session_id, username))
                session = cur.fetchone()
                if not session:
                    return None
                cur.execute(f'''SELECT role,content,metadata,created_at
                    FROM "{settings.postgres_schema}".chat_messages
                    WHERE session_id=%s ORDER BY created_at''', (session_id,))
                messages = cur.fetchall()
                return {**session, "messages": messages}

    def add_message(self, session_id, username, role, content, metadata=None):
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(f'''SELECT 1 FROM "{settings.postgres_schema}".chat_sessions
                    WHERE session_id=%s AND username=%s''', (session_id, username))
                if not cur.fetchone():
                    raise ValueError("Chat session not found.")
                cur.execute(f'''INSERT INTO "{settings.postgres_schema}".chat_messages
                    (message_id,session_id,role,content,metadata,created_at)
                    VALUES (%s,%s,%s,%s,%s,%s)''',
                    (str(uuid.uuid4()), session_id, role, content, Jsonb(metadata or {}), datetime.now(timezone.utc)))
                cur.execute(f'''UPDATE "{settings.postgres_schema}".chat_sessions
                    SET last_activity_at=now(), expires_at=now()+interval '24 hours'
                    WHERE session_id=%s''', (session_id,))
            conn.commit()

    def delete_session(self, session_id, username):
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(f'''DELETE FROM "{settings.postgres_schema}".chat_sessions
                    WHERE session_id=%s AND username=%s''', (session_id, username))
            conn.commit()

    def find_by_conversation(self, conversation_id, username):
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(f'''SELECT session_id FROM "{settings.postgres_schema}".chat_sessions
                    WHERE genie_conversation_id=%s AND username=%s AND expires_at > now()''', (conversation_id, username))
                return cur.fetchone()


store = Store()
