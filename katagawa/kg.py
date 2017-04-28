"""
The main Katagawa object. This is the "database interface" to the actual DB server.
"""
import importlib
import logging

import dsnparse

from katagawa.backends.base import BaseConnector, BaseTransaction
from katagawa.orm import session as md_session

# sentinels
NO_CONNECTOR = object()

logger = logging.getLogger("katagawa")


class Katagawa(object):
    """
    The "database interface" to your database. This provides the actual connection to the DB server,
    including things such as querying, inserting, updating, et cetera.
    
    Creating a new database object is simple:
    
    .. code-block:: python
        # pass the DSN in the constructor
        dsn = "postgresql://postgres:B07_L1v3s_M4tt3r_T00@127.0.0.1/joku"
        my_database = Katagawa(dsn)
        # or provide it in the `.connect()` call
        await my_database.connect(dsn)
        
    
    """

    def __init__(self, dsn: str = None):
        """
        :param dsn: 
            The `Data Source Name <http://whatis.techtarget.com/definition/data-source-name-DSN>_`
            to connect to the database on.
        """
        self._dsn = dsn

        #: The current connector instance.
        self.connector = None  # type: BaseConnector

    @property
    def connected(self):
        """
        Checks if this DB is connected. 
        """
        return self.connector is not None

    async def connect(self, dsn: str = None, **kwargs) -> BaseConnector:
        """
        Connects the interface to the database server.
        
        .. note::
            For SQLite3 connections, this will just open the database for reading.
        
        :param dsn: The Data Source Name to connect to, if it was not specified in the constructor.
        :return: The :class:`~.BaseConnector` established.
        """
        if dsn is not None:
            self._dsn = dsn

        parsed_dsn = dsnparse.parse(self._dsn)
        # db type must always exist
        # the connector doesn't have to exist, however
        # if so we use a sentinel value
        db_type = parsed_dsn.schemes[0]
        try:
            db_connector = parsed_dsn.schemes[1]
        except IndexError:
            db_connector = NO_CONNECTOR

        import_path = "katagawa.backends.{}".format(db_type)
        package = importlib.import_module(import_path)
        if db_connector is not NO_CONNECTOR:
            mod_path = import_path + ".{}".format(db_connector)
        else:
            mod_path = import_path + ".{}".format(package.DEFAULT_CONNECTOR)

        logger.debug("Loading connector {}".format(mod_path))

        connector_mod = importlib.import_module(mod_path)
        connector_ins = connector_mod.CONNECTOR_TYPE(parsed_dsn)  # type: BaseConnector
        self.connector = connector_ins
        try:
            await self.connector.connect(**kwargs)
        except Exception:
            # delete self.connector and re-raise in the event that it fucks up
            self.connector = None
            raise

        return self.connector

    def emit_param(self, name: str) -> str:
        """
        Emits a param in the format that the DB driver specifies.
        
        :param name: The name of the parameter to emit. 
        :return: A str representing the emitted param.
        """
        return self.connector.emit_param(name)

    def get_transaction(self) -> BaseTransaction:
        """
        Gets a low-level :class:`.BaseTransaction`.
         
        .. code-block:: python
            async with db.get_transaction() as transaction:
                results = await transaction.cursor("SELECT 1;")
        """
        return self.connector.get_transaction()

    def get_session(self) -> 'md_session.Session':
        """
        Gets a new :class:`.Session` bound to this Katagawa instance.
        """
        return md_session.Session(self)

    async def close(self):
        """
        Closes the current database interface.
        """
        await self.connector.close()

    async def get_db_server_info(self):
        """
        Gets DB server info.
        
        .. warning::
            This is **not** supported on SQLite3 connections. 
        """
        # todo: make this do something
