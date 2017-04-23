import logging
import typing

import enum

from katagawa import kg as md_kg
from katagawa.backends.base import BaseTransaction

logger = logging.getLogger(__name__)


class SessionState(enum.Enum):
    NOT_READY = 0
    READY = 1
    CLOSED = 2


class Session(object):
    """
    Sessions act as a temporary window into the database. They are responsible for creating queries,
    inserting and updating rows, etc.
    
    Sessions are bound to a :class:`.Katagawa` instance which they use to get a transaction and 
    execute queries in.
    
    .. code-block:: python
        # get a session from our db interface
        sess = db.get_session()
    """

    def __init__(self, bind: 'md_kg.Katagawa'):
        """
        :param bind: The :class:`.Katagawa` instance we are bound to. 
        """
        self.bind = bind

        #: The current state for the session.
        self._state = SessionState.NOT_READY

        #: The current :class:`.BaseTransaction` this Session is associated with.
        #: The transaction is used for making queries and inserts, etc.
        self.transaction = None  # type: BaseTransaction

        #: The current list of "new" objects.
        #: These are table rows that are ready to be inserted.
        self.new = []

        #: The current list of "dirty" objects.
        #: These are table rows that are ready to be updated.
        self.dirty = []

        #: The current list of "deleted" objects.
        #: These are table rows that are ready to be deleted.
        self.deleted = []

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        try:
            if exc_type is None:
                await self.commit()
            else:
                await self.rollback()
        finally:
            await self.close()

        return False

    async def start(self) -> 'Session':
        """
        Starts the session, acquiring a transaction connection which will be used to modify the DB.
        This **must** be called before using the session.  
        
        .. code-block:: python
            sess = db.get_session()
            await sess.start()
        
        .. note::
            When using ``async with``, this is automatically called.
        """
        if self._state is not SessionState.NOT_READY:
            raise RuntimeError("Session must not be ready or closed")

        self.transaction = self.bind.get_transaction()
        await self.transaction.begin()

        self._state = SessionState.READY
        return self

    async def commit(self):
        """
        Commits the current session, running inserts/updates/deletes.
         
        This will **not** close the session; it can be re-used after a commit.
        """
        # TODO: Write INSERT, UPDATE and DELETE statements here
        await self.transaction.commit()
        return self

    async def rollback(self, checkpoint: str = None):
        """
        Rolls the current session back.  
        This is useful if an error occurs inside your code.
        
        :param checkpoint: The checkpoint to roll back to, if applicable. 
        """
        await self.transaction.rollback(checkpoint=checkpoint)
        return self

    async def close(self):
        """
        Closes the current session.
        
        .. warning::
            This will **NOT COMMIT ANY DATA**. Old data will die.
        """
        await self.transaction.close()
        self._state = SessionState.CLOSED
        del self.transaction

    async def execute(self, sql: str, params: typing.Union[typing.Container, typing.Iterable]):
        """
        Executes SQL inside the current session.
        
        This is part of the **low-level API.**
        
        :param sql: The SQL to execute.
        :param params: The parameters to use inside the query.
        :return: A :class:`.BaseResultSet` representing the results.
        """