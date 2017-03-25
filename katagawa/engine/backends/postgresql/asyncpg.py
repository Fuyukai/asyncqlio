"""
An engine using the ``asyncpg`` backend.
"""
import typing

import asyncpg
import logging

from asyncpg.cursor import Cursor

from katagawa.engine.base import BaseEngine, ResultSet
from katagawa.engine.transaction import Transaction


class _FakeContextManager:
    def __init__(self, connection):
        self.connection = connection

    async def __aenter__(self):
        return self.connection

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return False


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


class AsyncpgResultSet(ResultSet):
    """
    A result set class specific to the asyncpg driver.
    """

    def __init__(self, cursor: Cursor):
        #: The cursor used for this request.
        self.cursor = cursor

    async def get_next(self, *, count: int = None):
        """
        Gets the next <count> rows from this query.
        
        :param count: The number of rows to fetch. If this is None, the next row is fetched.
        """
        if count is None:
            row = await self.cursor.fetchrow()
        else:
            row = await self.cursor.fetch(count)

        if row is None:
            raise StopAsyncIteration

        return row


class AsyncpgTransaction(Transaction):
    """
    A transaction class specific to the asyncpg driver.
    """

    def __init__(self, engine: 'AsyncpgEngine', isolation="read_committed", read_only=False, deferrable=False):
        super().__init__(engine, read_only=read_only)

        #: The connection we've retrieved.
        self.connection = None  # type: asyncpg.connection.Connection

        #: The internal asyncpg transaction object.
        self.internal_transaction = None  # type: asyncpg.connection.transaction.Transaction

        self.isolation = isolation
        self.read_only = read_only
        self.deferrable = deferrable

    @classmethod
    async def create(cls, engine: 'AsyncpgEngine',
                     isolation="read_committed", read_only=False, deferrable=False) -> 'AsyncpgTransaction':
        """
        Creates the transaction.
        
        :param engine: The :class:`~.AsyncpgEngine` to use as this engine. 
        """
        obb = cls(engine, isolation, read_only, deferrable)
        await obb._get_transaction()

        return obb

    async def _get_transaction(self):
        self.connection = await self.engine.get_connection()

        self.internal_transaction = self.connection.transaction(isolation=self.isolation, readonly=self.read_only,
                                                                deferrable=self.deferrable)

    async def execute(self, sql: str, params: dict):
        """
        Executes SQL inside the connection.
        """
        if not self.started:
            raise RuntimeError("Cannot execute SQL inside non-started transaction")

        # Create tuples from the params.
        sql, args = get_param_query(sql, params)
        # Execute the query.
        result = await self.connection.execute(sql, *args)

        return result

    async def _acquire(self):
        # Start the transaction.
        await self.internal_transaction.start()
        return self

    async def _release(self, errored: bool = False):
        # Rollback if we encountered an error.
        if errored:
            await self.rollback()
        else:
            await self.commit()

        return self

    async def fetch(self, sql: str, params: dict = None) -> AsyncpgResultSet:
        if not self.started:
            raise RuntimeError("Cannot execute SQL inside non-started transaction")

        # Create tuples from the params.
        sql, args = get_param_query(sql, params)

        cursor = await self.connection.cursor(sql, *args)
        return AsyncpgResultSet(cursor=cursor)

    def commit(self):
        return self.internal_transaction.commit()

    def rollback(self):
        return self.internal_transaction.rollback()


class AsyncpgEngine(BaseEngine):
    """
    An engine type that uses ``asyncpg`` as the SQL backend.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # The current connection.
        self.connection = None  # type: asyncpg.connection.Connection

        # The current connection pool.
        # self.connection will be None if this exists.
        self.pool = None  # type: asyncpg.pool.Pool

        self.logger = logging.getLogger("Katagawa.engine.asyncpg")

    async def _connect(self):
        """
        Connects using a pool if specified, else using a single connection.
        """
        if self.use_connection_pool:
            # Create the connection pool.
            self.pool = await asyncpg.create_pool(host=self.host, port=self.port, user=self.username,
                                                  password=self.password, database=self.database,
                                                  loop=self.loop, min_size=self.pool_min_size,
                                                  max_size=self.pool_max_size)
        else:
            # Connect using a single connection.
            self.connection = await asyncpg.connect(host=self.host, port=self.port, user=self.username,
                                                    password=self.password, database=self.database,
                                                    loop=self.loop)

    async def get_connection(self) -> asyncpg.connection.Connection:
        """
        Returns an asyncpg connection.
        """
        if self.use_connection_pool is False:
            if self.connection is None:
                await self.connect()
            r = self.connection
            r = _FakeContextManager(r)
        else:
            if self.pool is None:
                await self.connect()
            # Acquire a new connection from the pool.
            r = await self.pool.acquire()

        return r

    async def create_transaction(self, read_only=False, **kwargs) -> AsyncpgTransaction:
        trans = await AsyncpgTransaction.create(self, kwargs.get("isolation", "read_committed"), read_only,
                                                kwargs.get("deferrable", False))

        return trans

    def emit_param(self, name: str):
        return "{%s}" % name

    async def fetch(self, sql: str, rows: int = 1, params: dict = None):
        """
        Fetches <rows> rows from the database, using the specified query.
        
        .. warning::
        
            It is not recommended to use this method - use :meth:`.AsyncpgTransaction.execute` instead!
        
        :param sql: The SQL query to execute. 
        :param rows: The number of rows to return.
        :param params: The parameters to return.
        :return: 
        """
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
