"""
The main Database object. This is the "database interface" to the actual DB server.
"""
import asyncio
import importlib
import itertools
import logging
from typing import Tuple, Type, Union
from urllib.parse import ParseResult, urlparse

from asyncqlio.backends.base import BaseConnector, BaseDialect, BaseTransaction
from asyncqlio.orm import session as md_session
from asyncqlio.orm.ddl import ddlsession as md_ddlsession
from asyncqlio.orm.schema import table as md_table

# sentinels
NO_CONNECTOR = object()

logger = logging.getLogger("asyncqlio")


class DatabaseInterface(object):
    """
    The "database interface" to your database. This provides the actual connection to the DB server,
    including things such as querying, inserting, updating, et cetera.

    Creating a new database object is simple:

    .. code-block:: python3

        # pass the DSN in the constructor
        dsn = "postgresql://postgres:B07_L1v3s_M4tt3r_T00@127.0.0.1/mydb"
        my_database = DatabaseInterface(dsn)
        # then connect
        await my_database.connect()

    """
    param_counter = itertools.count()

    def __init__(self, dsn: str, *, loop: asyncio.AbstractEventLoop = None,
                 connector: Type[BaseConnector] = None):
        """
        :param dsn:
            The `Data Source Name <http://whatis.techtarget.com/definition/data-source-name-DSN>_`
            to connect to the database on.
        """
        self._dsn = dsn
        self.loop = loop or asyncio.get_event_loop()

        parsed_dsn = urlparse(self._dsn)  # type: ParseResult
        # db type must always exist
        # the connector doesn't have to exist, however
        # if so we use a sentinel value
        schemes = parsed_dsn.scheme.split("+")
        db_type = schemes[0]
        try:
            db_connector = schemes[1]
        except IndexError:
            db_connector = NO_CONNECTOR

        import_path = "asyncqlio.backends.{}".format(db_type)
        package = importlib.import_module(import_path)

        #: The current Dialect instance.
        self.dialect = getattr(package, "{}Dialect".format(db_type.title()))()  # type: BaseDialect

        if connector is None:
            if db_connector is not NO_CONNECTOR:
                mod_path = ".".join([import_path, db_connector])
            else:
                mod_path = ".".join([import_path, package.DEFAULT_CONNECTOR])

            logger.debug("Loading connector {}".format(mod_path))
            connector_mod = importlib.import_module(mod_path)
            connector = connector_mod.CONNECTOR_TYPE

        self._connector_type = connector
        self._parsed_dsn = parsed_dsn

        #: The current connector instance.
        self.connector = None  # type: BaseConnector

    async def __aenter__(self):
        if not self.connected:
            await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
        return False

    @property
    def connected(self):
        """
        Checks if this DB is connected.
        """
        return self.connector is not None

    def bind_tables(self, md: 'md_table.TableMetadata'):
        """
        Binds tables to this DB instance.
        """
        if isinstance(md, md_table.TableMeta):
            md = md.metadata
        # first set a bind on the metadata
        md._bind = self
        # then setup tables
        md.setup_tables()
        return md

    async def connect(self, **kwargs) -> BaseConnector:
        """
        Connects the interface to the database server.

        .. note::
            For SQLite3 connections, this will just open the database for reading.

        :return: The :class:`~.BaseConnector` established.
        """
        self.connector = self._connector_type(self._parsed_dsn, loop=self.loop)
        try:
            await self.connector.connect(**kwargs)
        except Exception:
            # delete self.connector and re-raise in the event that it fucks up
            self.connector = None
            raise

        return self.connector

    def emit_param(self, name: str = None) -> Union[Tuple[str, str], str]:
        """
        Emits a param in the format that the DB driver specifies.

        :param name: The name to use. If this is None, a name will automatically be used, \
            and no name param will be returned.
        :return: The emitted param, and the name of the param emitted.
        """
        if name is not None:
            return self.connector.emit_param(name)

        name = "param_{}".format(next(self.param_counter))
        return self.connector.emit_param(name), name

    def get_transaction(self, **kwargs) -> BaseTransaction:
        """
        Gets a low-level :class:`.BaseTransaction`.

        .. code-block:: python3

            async with db.get_transaction() as transaction:
                results = await transaction.cursor("SELECT 1;")
        """
        return self.connector.get_transaction(**kwargs)

    def get_session(self, **kwargs) -> 'md_session.Session':
        """
        Gets a new :class:`.Session` bound to this instance.
        """
        return md_session.Session(self, **kwargs)

    def get_ddl_session(self, **kwargs) -> 'md_ddlsession.DDLSession':
        """
        Gets a new :class:`.DDLSession` bound to this instance.
        """
        return md_ddlsession.DDLSession(self, **kwargs)

    async def close(self):
        """
        Closes the current database interface.
        """
        if self.connector is not None:
            await self.connector.close()

    # db server stuff
    async def get_db_server_version(self) -> str:
        """
        Gets the version of the DB server.
        """
        return await self.connector.get_db_server_version()
