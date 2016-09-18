"""
An engine using the ``asyncpg`` backend.
"""
import typing

import asyncpg
import logging

from katagawa.engine.base import BaseEngine
from katagawa.engine.transaction import Transaction


class _FakeContextManager:
    def __init__(self, connection):
        self.connection = connection

    async def __aenter__(self):
        return self.connection

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return


def get_param_query(sql: str, params: dict) -> typing.Tuple[str, tuple]:
    """
    Gets a parametrized query.

    Returns the reparsed SQL, and a tuple of items to be passed as parameters.
    """
    if not params or len(params) < 1:
        return sql, ()
    # Dump the params into key -> value pairs.
    kv = [(k, v) for (k, v) in params.items()]

    # Define the list of items that are used to return.
    items = []

    fmt_dict = {}

    # Iterate over the key-values, adding each key to the fmt_dict with the number index.
    for n, (k, v) in enumerate(kv):
        n += 1
        # Add to the format dict.
        fmt_dict[k] = "${}".format(n)
        items.append(v)

    sql_statement = sql.format(**fmt_dict)

    return sql_statement, tuple(items)


class AsyncpgTransaction(Transaction):
    """
    A transaction class specific to the asyncpg driver.
    """

    async def execute(self, sql: str, params: dict):
        """
        Executes SQL inside the connection.
        """
        if not self.started:
            raise RuntimeError("Cannot execute SQL inside non-started transaction")

        # Create tuples from the params.
        sql, args = get_param_query(sql, params)
        # Execute the query.
        result = await self.connection.fetch(sql, *args)

        return result

    def __init__(self, engine: 'AsyncpgEngine', isolation="read_committed", read_only=False, deferrable=False):
        super().__init__(engine, read_only=read_only)

        self.connection = engine.get_connection()

        # Define the asyncpg transaction object.
        self.internal_transaction = self.connection.transaction(isolation=isolation, readonly=read_only,
                                                                deferrable=deferrable)

    async def _acquire(self):
        # Start the transaction.
        await self.internal_transaction.start()
        return self

    async def _release(self):
        # Rollback if we're read-only, otherwise commit.
        if self.read_only:
            await self.internal_transaction.rollback()
        else:
            await self.internal_transaction.commit()

        return self


class AsyncpgEngine(BaseEngine):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # The current connection.
        self.connection = None  # type: asyncpg.connection.Connection

        # The current connection pool.
        # self.connection will be None if this exists.
        self.pool = None  # type: asyncpg.pool.Pool

        self.logger = logging.getLogger("Katagawa.engine.asyncpg")

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

    def create_transaction(self, read_only=False, **kwargs) -> AsyncpgTransaction:
        trans = AsyncpgTransaction(self, kwargs.get("isolation", "read_only"), read_only,
                                   kwargs.get("deferrable", False))

        return trans

    def emit_param(self, name: str):
        return "{%s}" % name

    async def fetch(self, sql: str, rows: int = 1, params: dict = None):
        async with self.get_connection() as conn:
            assert isinstance(conn, asyncpg.connection.Connection)
            # Parse the sql and the params.
            new_sql, p_tup = get_param_query(sql, params)
            self.logger.debug("Fetching `{}`".format(new_sql))
            self.logger.debug("{}".format(p_tup))
            # Open up a new transaction.
            async with conn.transaction():
                # Create a cursor.
                cursor = await conn.cursor(new_sql, *p_tup)
                items = await cursor.fetch(rows)

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
