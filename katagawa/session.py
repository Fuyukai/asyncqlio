import logging

from katagawa import engine as md_engine
from katagawa.engine.base import ResultSet
from katagawa.engine.transaction import Transaction
from katagawa.orm.table import Table
from katagawa.querying.query import BaseQuery

logger = logging.getLogger("Katagawa.session")


class Session(object):
    """
    A session represents a database transaction in progress.

    A new session should be created every time a unique function wants to make a query or insert 
    data into the database.
    """

    def __init__(self, engine: 'md_engine.BaseEngine', **kwargs):
        """
        Creates a new session instance.
        :param engine: The engine to bind to.
        :param kwargs: Any keyword arguments used by the session.
        """
        #: The :class:`~.BaseEngine` that is connected to this session.
        self.engine = engine

        #: The :class:`~.Transaction` that is connected to this session.
        self.transaction = None  # type: Transaction

        #: The query class to use to create new query items.
        self.query_class = kwargs.pop("query_cls", BaseQuery)

        # Added, dirty, and deleted items.
        self.added = []
        self.dirty = []
        self.deleted = []

    def query(self, tbl: Table, **kwargs) -> BaseQuery:
        """
        Produces a new Query object, bound to this session.
        
        :param tbl: The :class:`~.Table` to query.
        :return: A new :class:`.BaseQuery` that can be used to query the database with.
        """
        query = self.query_class(session=self, table=tbl, **kwargs)
        return query

    # magic methods
    async def __aenter__(self):
        await self.begin()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            await self.rollback(errored = True)
            return False

        else:
            await self.commit()
            # never suppress
            return False

    async def begin(self, **transaction_kwargs) -> 'Session':
        """
        Starts the session.
        
        This will open a new transaction to execute this query inside of.
        :return: This :class:`.Session`.
        """
        self.transaction = await self.engine.create_transaction(**transaction_kwargs)
        await self.transaction.acquire()

        return self

    async def commit(self):
        """
        Commits this session, writing the results of the actions to the database.
        
        Once a session is committed, the session must be re-opened with :meth:`.Session.begin`.
        """
        # todo: added, dirty, deleted
        # commit the transaction
        await self.transaction.commit()
        # release the transaction away from us
        await self.transaction.release(errored=False)

    async def rollback(self, errored: bool = False):
        """
        Rolls back this session, undoing any changes.
        """
        await self.transaction.rollback()
        await self.transaction.release(errored=errored)

    async def execute(self, query: BaseQuery) -> ResultSet:
        """
        Executes a query and runs it.

        This takes in a :class:`.BaseQuery` object and executes the actual query.

        :param query: The query object.
        :return: A ResultSet object.
        """
        # create the transaction for the sess
        if self.transaction is None:
            await self.begin()

        # BEGIN
        final_query, params = query.get_token()
        final_sql = final_query.generate_sql()
        logger.debug("Running query `{}` with params `{}`".format(final_sql, params))
        results = await self.transaction.fetch(final_sql, params)

        # no implicit ROLLBACK

        return results
