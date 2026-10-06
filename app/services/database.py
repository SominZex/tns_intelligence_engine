from typing import Any, Optional

from psycopg import AsyncConnection
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from app.config import settings


class Database:

    def __init__(self) -> None:

        self.pool: Optional[AsyncConnectionPool] = None

    async def connect(self) -> None:

        if self.pool is not None:
            return

        connection_string = (
            f"host={settings.postgres_host} "
            f"port={settings.postgres_port} "
            f"dbname={settings.postgres_database} "
            f"user={settings.postgres_user} "
            f"password={settings.postgres_password} "
            f"connect_timeout={settings.postgres_connect_timeout}"
        )

        self.pool = AsyncConnectionPool(
            conninfo=connection_string,
            min_size=settings.postgres_min_connections,
            max_size=settings.postgres_max_connections,
            open=False,
        )

        await self.pool.open()

        await self.pool.wait()

    async def close(self) -> None:

        if self.pool is not None:

            await self.pool.close()

            self.pool = None

    async def execute(
        self,
        query: str,
        params: tuple[Any, ...] = (),
    ) -> None:

        if self.pool is None:
            raise RuntimeError(
                "Database pool is not initialized."
            )

        async with self.pool.connection() as connection:

            async with connection.cursor() as cursor:

                await cursor.execute(
                    query,
                    params,
                )

    async def fetch_one(
        self,
        query: str,
        params: tuple[Any, ...] = (),
    ) -> Optional[dict]:

        if self.pool is None:
            raise RuntimeError(
                "Database pool is not initialized."
            )

        async with self.pool.connection() as connection:

            async with connection.cursor(
                row_factory=dict_row
            ) as cursor:

                await cursor.execute(
                    query,
                    params,
                )

                row = await cursor.fetchone()

                return row

    async def fetch_all(
        self,
        query: str,
        params: tuple[Any, ...] = (),
    ) -> list[dict]:

        if self.pool is None:
            raise RuntimeError(
                "Database pool is not initialized."
            )

        async with self.pool.connection() as connection:

            async with connection.cursor(
                row_factory=dict_row
            ) as cursor:

                await cursor.execute(
                    query,
                    params,
                )

                rows = await cursor.fetchall()

                return list(rows)


db = Database()