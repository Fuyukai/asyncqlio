"""
A backend using the stdlib sqlite3 driver.
"""
import asyncio
import sqlite3
import typing

from asyncio_extras import threadpool

from asyncqlio.backends.base import BaseConnector, BaseResultSet, BaseTransaction, DictRow


class _SqlitePool:
    """
    A connection pool for sqlite3 connections.
    """

    def __init__(self, max_size: int = 12, *, loop=None, **kwargs):
        """
        :param max_size: The maximum size of the pool.
        """
        self.queue = asyncio.Queue(maxsize=max_size, loop=loop)

        self.connection_args = kwargs

    def _new_connection(self) -> sqlite3.Connection:
        # check_same_thread is needed because we're connecting from inside a threadpool.
        conn = sqlite3.connect(**self.connection_args, check_same_thread=False)
        # this allows dict-like access
        conn.row_factory = sqlite3.Row
        return conn

    async def connect(self, *args, **kwargs):
        """
        Connects this pool.
        """
        if "timeout" not in kwargs:
            kwargs["timeout"] = 3600

        async with threadpool():
            for x in range(0, self.queue.maxsize):
                conn = self._new_connection()
                self.queue.put_nowait(conn)

        return self

    async def acquire(self) -> sqlite3.Connection:
        """
        Acquires a connection from the pool.
        """
        return await self.queue.get()

    async def release(self, conn: sqlite3.Connection):
        """
        Releases a connection back to the pool of available connections.
        """
        async with threadpool():
            # rollback anything stale left in the DB
            if conn.in_transaction:
                conn.close()
                conn = self._new_connection()

        self.queue.put_nowait(conn)

    async def close(self):
        """
        Closes the pool.
        """
        while True:
            try:
                conn = self.queue.get_nowait()
            except asyncio.QueueEmpty:
                return

            conn.close()


class Sqlite3Connector(BaseConnector):
    """
    A connector powered by sqlite3.
    """

    def __init__(self, parsed, *, loop: asyncio.AbstractEventLoop = None,
                 max_size: int = 12):
        super().__init__(parsed, loop=loop)

        self.max_size = max_size
        self.pool = None  # type: _SqlitePool

    async def connect(self) -> 'BaseConnector':
        """
        Creates the new pool of sqlite3 connections.
        """
        self.pool = _SqlitePool(max_size=self.max_size, database=self.db, **self.params)
        await self.pool.connect()
        return self

    async def close(self):
        """
        Closes this connector.
        """
        await self.pool.close()

    def get_transaction(self) -> 'BaseTransaction':
        return Sqlite3Transaction(self)

    def emit_param(self, name: str) -> str:
        return ":{}".format(name)

    async def get_db_server_info(self):
        raise NotImplementedError


class Sqlite3Transaction(BaseTransaction):
    """
    Represents a sqlite3 transaction.
    """

    def __init__(self, connector: 'Sqlite3Connector'):
        super().__init__(connector)

        #: The connection for this transaction.
        self.connection = None  # type: sqlite3.Connection

        self._lock = asyncio.Lock(loop=self.connector.loop)

    async def begin(self):
        """
        Begins the current transaction.
        """
        self.connection = await self.connector.pool.acquire()

    async def execute(self, sql: str, params: typing.Union[typing.Mapping, typing.Iterable] = None):
        """
        Executes SQL in the current transaction.
        """
        # lock to ensure nothing else is using the connection at once

        async with self._lock:
            async with threadpool():
                res = self.connection.execute(sql, params)

        return res

    async def commit(self):
        """
        Commits the current transaction.
        """
        async with self._lock:
            async with threadpool():
                self.connection.commit()

    async def rollback(self, checkpoint: str = None):
        """
        Rolls back the current transaction.
        """
        if checkpoint is not None:
            await self.execute("ROLLBACK TRANSACTION TO SAVEPOINT %s;", (checkpoint,))
            return

        async with self._lock:
            async with threadpool():
                self.connection.rollback()

    async def create_savepoint(self, name: str):
        """
        Creates a savepoint for this transaction.
        """
        await self.execute("CREATE SAVEPOINT %s;", (name,))

    async def release_savepoint(self, name: str):
        """
        Releases a savepoint in this transaction.
        """
        await self.execute("RELEASE SAVEPOINT %s;")

    async def cursor(self, sql: str, params: typing.Union[typing.Mapping, typing.Iterable] = None) \
            -> 'Sqlite3ResultSet':
        """
        Gets a cursor for the specified SQL.
        """
        async with self._lock:
            async with threadpool():
                cur = self.connection.cursor()
                cur.execute(sql, params)

        return Sqlite3ResultSet(cur)

    async def close(self):
        """
        Closes the current transaction.
        """
        await self.connector.pool.release(self.connection)
        self.connection = None


class Sqlite3ResultSet(BaseResultSet):
    """
    A result set for a sqlite3 database.
    """

    def __init__(self, cursor: sqlite3.Cursor):
        self.cursor = cursor

        self._keys = None

    @property
    def keys(self) -> typing.Iterable[str]:
        return self._keys

    async def close(self):
        async with threadpool():
            self.cursor.close()

    async def fetch_many(self, n: int) -> typing.List[typing.Mapping[str, typing.Any]]:
        """
        Fetches many rows.
        """
        async with threadpool():
            rows = self.cursor.fetchmany(size=n)

        return [DictRow(r) for r in rows if r is not None]

    async def fetch_row(self) -> typing.Mapping[str, typing.Any]:
        """
        Fetches one row.
        """
        async with threadpool():
            row = self.cursor.fetchone()

        return DictRow(row) if row is not None else None


CONNECTOR_TYPE = Sqlite3Connector
