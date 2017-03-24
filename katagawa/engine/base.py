"""
The base class for any Katagawa back-end engine.
"""

import abc
import asyncio

import dsnparse

from katagawa.engine import transaction as tr
from katagawa.exceptions import OperationalException


class BaseEngine(object):
    """
    An engine is a way of connecting Katagawa to an actual database server.

    It implements the connection logic and the actual sending and fetching of results from the server.

    .. code:: python

        engine = AsyncpgEngine(url="postgresql://user:password@127.0.0.1:5432/database")
        await engine.connect()
        results = await engine.fetch('''SELECT a, b, c FROM sometable''', rows=1)
        other_results = await engine.execute('''INSERT INTO sometable (a, b, c) VALUES (100), (200), (300)''')
        await engine.close()
    """

    def __init__(self, dsn: str = None, *, host: str = None, port: int = None,
                 username: str = None, password: str = None, database: str = None,
                 use_connection_pool: bool = True, pool_min_size: int = 2, pool_max_size: int = 8,
                 loop=None):
        """
        Creates a new database engine.

        Either a DSN or each individual part of it should be specified.

        :param dsn: The DSN to use to create the new engine.
        :param host: The hostname to connect to.
        :param port: The port to connect to.
        :param username: The username to use to log in as.
        :param password: The password to log in with.
        :param database: The specific database to use.

        :param use_connection_pool: Should a connection pool be used to connect to the database?
            Note that variances between the connectors means that this is not always guaranteed to be used.

        :param pool_min_size: The minimum size of the connection pool, if applicable.
        :param pool_max_size: The maximum size of the connection pool, if applicable.
        """
        if dsn:
            parsed = dsnparse.parse(dsn)
            self.host = parsed.host
            self.port = parsed.port
            self.username = parsed.username
            self.password = parsed.password

            try:
                self.database = parsed.paths[0]
            except IndexError as e:
                raise KeyError("Database was not specified in the DSN") from e

        else:
            self.host = host
            self.port = port
            self.username = username
            self.password = password

            self.database = database

        self.use_connection_pool = use_connection_pool
        self.pool_max_size = pool_max_size
        self.pool_min_size = pool_min_size

        self.loop = loop or asyncio.get_event_loop()

    def __repr__(self):
        return "<{.__name__} host='{}' port='{}' database='{}'>".format(type(self), self.host, self.port, self.database)

    @abc.abstractmethod
    async def emit_param(self, name: str):
        """
        Emits a parameter for the current engine.

        This will return the param you need to add to the SQL string for the DBAPI to complete successfully,
        for example asyncpg will emit ``{name}``.

        :param name: The name of the parameter.
        :return: What the name would be.
        """

    @abc.abstractmethod
    async def _connect(self):
        """
        This is the actual connect logic.

        It is automatically killed when the ``wait_for`` in ``connect`` is ended.
        """

    async def connect(self, *, timeout=30):
        """
        Connects the engine to the database specified.
        :param timeout: How long to wait before we terminate the connection?
        """
        # Create a new future which is wait_for'd
        try:
            coro = asyncio.wait_for(asyncio.ensure_future(self._connect()), timeout=timeout, loop=self.loop)
            return await coro
        except ConnectionError as e:
            exc = OperationalException("Could not connect to server: {}\n\t"
                                       "Is the server running on host \"{}\" "
                                       "and accepting connections on port {}?".format(e.strerror,
                                                                                      self.host,
                                                                                      self.port))
            raise exc

    @abc.abstractmethod
    async def create_transaction(self, read_only=False, **kwargs) -> 'tr.Transaction':
        """
        Creates a new transaction and returns it.
        """

    @abc.abstractmethod
    async def get_connection(self):
        """
        Gets a connection.

        This should either return a new connection from the pool, or the existing connection.
        """

    @abc.abstractmethod
    async def fetch(self, sql: str, rows: int = 1, params: dict = None):
        """
        Fetches data from the database using the underlying connection.

        :param sql: The SQL statement to execute on the connection.
        :param rows: The number of rows to fetch in the result.
            If this value is 0, it will attempt to return all.

        :param params: A dictionary of parameters to add to the SQL query.

        :return: An iterator that allows you iterate over the results returned from the query.
        """

    @abc.abstractmethod
    async def fetchall(self, sql: str, params: dict = None):
        """
        Fetches data from the database using the underlying connection.

        This returns an iterator that can be used to iterate over the rows of the database that are returned.

        :param sql: The SQL statement to execute on the connection.
        :param params: A dictionary of parameters to insert into the query.
        :return:
        """
