"""
The :ref:`asyncpg` connector for PostgreSQL databases.
"""
import typing
import logging

import asyncpg
from asyncpg import Record
from asyncpg.cursor import Cursor
from asyncpg.transaction import Transaction
from dsnparse import ParseResult

from katagawa.backends.base import BaseConnector, BaseTransaction, BaseResultSet

logger = logging.getLogger(__name__)


def get_param_query(sql: str, params: dict) -> typing.Tuple[str, tuple]:
    """
    Re-does a SQL query so that it uses asyncpg's special query format.
    
    :param sql: The SQL statement to use.
    :param params: The dict of parameters to use.
    :return: A two-item tuple of (new_query, arguments)
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

    # Finally, format the SQL with the required params.
    sql_statement = sql.format(**fmt_dict)

    return sql_statement, tuple(items)


class AsyncpgResultSet(BaseResultSet):
    def __init__(self, cur: Cursor):
        self.cur = cur

        self._keys = None

    async def fetch_many(self, n: int):
        res = await self.cur.fetch(n)
        if res and self._keys is None:
            self._keys = res[0].keys()

        return res

    @property
    def keys(self) -> typing.Iterable[str]:
        if self._keys is None:
            raise RuntimeError("Cursor is not started - no keys have been fetched")
        return self._keys

    async def fetch_row(self):
        row = await self.cur.fetchrow()  # type: Record
        if self._keys is None:
            self._keys = row.keys()

        return row


class AsyncpgTransaction(BaseTransaction):
    """
    A transaction that uses the `asyncpg <https://github.com/MagicStack/asyncpg`_ library.
    """

    def __init__(self, conn: 'AsyncpgConnector'):
        super().__init__(conn)

        #: The acquired connection from the connection pool.
        self.acquired_connection = None  # type: asyncpg.connection.Connection

        #: The asyncpg internal transaction.
        self.transaction = None  # type: Transaction

    async def begin(self, **transaction_options):
        """
        Begins the transaction.
        """
        self.acquired_connection = \
            await self.connector.pool.acquire()  # type: asyncpg.connection.Connection
        self.transaction = self.acquired_connection.transaction(**transaction_options)
        await self.transaction.start()

        return self

    async def commit(self):
        """
        Commits the transaction.
        """
        await self.transaction.commit()

    async def rollback(self, checkpoint: str = None):
        if checkpoint is not None:
            # execute the ROLLBACK TO
            await self.acquired_connection.execute("ROLLBACK TO {}".format(checkpoint))
        else:
            await self.transaction.rollback()

    async def close(self):
        await self.connector.pool.release(self.acquired_connection)

    async def execute(self, sql: str, params: typing.Mapping[str, typing.Any] = None):
        """
        Executes SQL inside the transaction.
        
        :param sql: The SQL to execute. 
        :param params: The parameters to excuse with.
        """
        # re-paramatarize the query
        query, params = get_param_query(sql, params)
        results = await self.acquired_connection.execute(query, *params)
        return results

    async def cursor(self, sql: str, params: typing.Mapping[str, typing.Any] = None) \
            -> AsyncpgResultSet:
        """
        Executes a SQL statement and returns a cursor to iterate over the rows of the result.
        """
        query, params = get_param_query(sql, params)
        cur = await self.acquired_connection.cursor(query, *params)
        result = AsyncpgResultSet(cur)

        return result


class AsyncpgConnector(BaseConnector):
    """
    A connector that uses the `asyncpg <https://github.com/MagicStack/asyncpg>`_ library.
    """

    def __init__(self, parsed: ParseResult):
        super().__init__(parsed)

        #: The :class:`asyncpg.pool.Pool` connection pool.
        self.pool = None  # type: asyncpg.pool.Pool

    async def close(self):
        await self.pool.close()

    def emit_param(self, name: str) -> str:
        # note: asyncpg doesn't support DBAPI params
        # so we have to do a "fun" re-parsing pass
        # which has the potential to KILL performance
        # inst
        return "{{{name}}}}".format(name=name)

    async def connect(self) -> 'BaseConnector':
        # create our connection pool
        logger.debug("Creating new asyncpg pool...")
        self.pool = await asyncpg.create_pool(self.dsn)
        logger.debug("Created pool.")
        return self

    def get_transaction(self) -> 'AsyncpgTransaction':
        return AsyncpgTransaction(self)

    async def get_db_server_info(self):
        return None


# define the asyncpg connector as the connector type to make an instance of
CONNECTOR_TYPE = AsyncpgConnector
