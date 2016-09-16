"""
An engine using the ``asyncpg`` backend.
"""
import asyncpg

from katagawa.engine.base import BaseEngine


class _FakeContextManager:
    def __init__(self, connection):
        self.connection = connection

    async def __aenter__(self):
        return self.connection

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return


class AsyncpgEngine(BaseEngine):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # The current connection.
        self.connection = None  # type: asyncpg.connection.Connection

        # The current connection pool.
        # self.connection will be None if this exists.
        self.pool = None  # type: asyncpg.pool.Pool

    def get_connection(self) -> asyncpg.connection.Connection:
        """
        Returns an asyncpg connection.
        """
        if self.pool is None:
            r = self.connection
            r = _FakeContextManager(r)
        else:
            # Acquire a new connection from the pool.
            r = self.pool.acquire()

        return r

    async def fetch(self, sql: str, rows: int = 1, params: dict = None):
        async with self.get_connection() as conn:
            # TODO: Do parameters
            assert isinstance(conn, asyncpg.connection.Connection)
            # Open up a new transaction.
            async with conn.transaction():
                # Create a cursor.
                cursor = conn.cursor(sql, prefetch=rows or 50)
                items = []
                async for row in cursor:
                    items.append(row)

                return items

    async def fetchall(self, sql: str, params: dict = None):
        pass

    async def _connect(self):
        """
        Connects using a pool if specified, else using a single connection.
        """
        if self.use_connection_pool:
            # Create the connection pool.
            self.pool = await asyncpg.create_pool(host=self.host, port=self.port, user=self.username,
                                                  password=self.password, database=self.database,
                                                  loop=self.loop)
        else:
            # Connect using a single connection.
            self.connection = await asyncpg.connect(host=self.host, port=self.port, user=self.username,
                                                    password=self.password, database=self.database,
                                                    loop=self.loop)
