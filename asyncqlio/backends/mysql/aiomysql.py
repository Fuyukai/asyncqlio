"""
The :ref:`aiomysql` connector for MySQL/MariaDB databases.
"""
import logging
import typing

import aiomysql
import pymysql

from asyncqlio.backends.base import BaseConnector, BaseResultSet, BaseTransaction, DictRow

logger = logging.getLogger(__name__)

# hijack aiomysql a bit
aiomysql.DictCursor.dict_type = DictRow


class AiomysqlResultSet(BaseResultSet):
    """
    Represents a result set returned by the MySQL database.
    """

    def __init__(self, cursor: aiomysql.DictCursor):
        self.cursor = cursor

        self._keys = None

    @property
    def keys(self):
        return self._keys

    async def close(self):
        return await self.cursor.close()

    async def fetch_row(self) -> typing.Dict[typing.Any, typing.Any]:
        """
        Fetches the next row in this result set.
        """
        row = await self.cursor.fetchone()
        if self._keys is None and row is not None:
            self._keys = row.keys()

        return row

    async def fetch_many(self, n: int):
        """
        Fetches the next N rows.
        """
        return await self.cursor.fetchmany(size=n)

    async def fetch_all(self):
        """
        Fetches ALL the rows.
        """
        return await self.cursor.fetchall()


class AiomysqlTransaction(BaseTransaction):
    """
    Represents a transaction for aiomysql.
    """

    def __init__(self, connector: 'AiomysqlConnector'):
        super().__init__(connector)

        #: The current acquired connection for this transaction.
        self.connection = None  # type: aiomysql.Connection

    async def close(self):
        """
        Closes the current connection.
        """
        # release it back to the pool so we don't eat all the connections
        self.connector.pool.release(self.connection)

    async def begin(self):
        """
        Begins the current transaction.
        """
        self.connection = await self.connector.pool.acquire()  # type: aiomysql.Connection
        await self.connection.begin()
        return self

    async def execute(self, sql: str, params: typing.Union[typing.Mapping, typing.Iterable] = None):
        """
        Executes some SQL in the current transaction.
        """
        # parse DictCursor in order to get a dict-like cursor back
        # this will use the custom DictRow class passed from before
        cursor = await self.connection.cursor(cursor=aiomysql.DictCursor)
        # the doc lies btw
        # we can pass a dict in instead of a list/tuple
        # i don't fucking trust this at all though.
        res = await cursor.execute(sql, params)
        await cursor.close()
        return res

    async def cursor(self, sql: str, params: typing.Union[typing.Mapping, typing.Iterable] = None) \
            -> 'AiomysqlResultSet':
        """
        Returns a :class:`.AiomysqlResultSet` for the specified SQL.
        """
        cursor = await self.connection.cursor(cursor=aiomysql.DictCursor)
        await cursor.execute(sql, params)
        return AiomysqlResultSet(cursor)

    async def rollback(self, checkpoint: str = None):
        """
        Rolls back the current transaction.

        :param checkpoint: Ignored.
        """
        await self.connection.rollback()

    async def commit(self):
        """
        Commits the current transaction.
        """
        await self.connection.commit()


class AiomysqlConnector(BaseConnector):
    """
    A connector that uses the `aiomysql <https://github.com/aio-libs/aiomysql>`_ library.
    """

    def __init__(self, dsn, *, loop=None):
        super().__init__(dsn, loop=loop)

        #: The current connection pool for this connector.
        self.pool = None  # type: aiomysql.Pool

    async def connect(self) -> 'AiomysqlConnector':
        """
        Connects this connector.
        """
        # aiomysql doesnt support a nice dsn
        port = self.port or 3306
        logger.info("Connecting to MySQL on mysql://{}:{}/{}".format(self.host, port, self.db))
        self.pool = await aiomysql.create_pool(host=self.host, user=self.username,
                                               password=self.password, port=port,
                                               db=self.db, **self.params)
        return self

    async def close(self, forcefully: bool = False):
        """
        Closes this connector.
        """
        if forcefully:
            await self.pool.terminate()
        else:
            await self.pool.close()
            await self.pool.wait_closed()

    def get_transaction(self) -> BaseTransaction:
        """
        Gets a new transaction object.
        """
        return AiomysqlTransaction(self)

    def emit_param(self, name: str) -> str:
        if pymysql.paramstyle == "pyformat":
            return "%({})s".format(name)
        elif pymysql.paramstyle == "named":
            return ":{}".format(name)
        else:
            raise ValueError("Cannot work with paramstyle {}".format(pymysql.paramstyle))

    def get_db_server_info(self):
        pass


CONNECTOR_TYPE = AiomysqlConnector
