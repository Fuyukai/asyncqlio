import logging

from katagawa.engine import BaseEngine
from katagawa.engine.transaction import Transaction
from katagawa.querying.query import BaseQuery

logger = logging.getLogger("Katagawa.session")


class Session(object):
    """
    A session represents a database transaction in progress.

    A new session should be created every time a unique function wants to make a query or insert data into the database.
    """

    def __init__(self, engine: BaseEngine, **kwargs):
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

    @property
    def query(self) -> BaseQuery:
        """
        Alias of :meth:`.Session.get_query`.
        """
        return self.get_query()

    def get_query(self, **kwargs) -> BaseQuery:
        """
        Produces a new Query object, bound to this session.
        
        :return: A new :class:`.BaseQuery` that can be used to query the database with.
        """
        query = self.query_class(session=self, **kwargs)
        return query

    # magic methods

    async def begin(self, **transaction_kwargs) -> 'Session':
        """
        Starts the session.
        
        This will open a new transaction to execute this query inside of.
        :return: This :class:`.Session`.
        """
        self.transaction = await self.engine.create_transaction(**transaction_kwargs)

        return self

    async def commit(self):
        """
        Commits this session, writing the results of the actions to the database.
        
        Once a session is committed, the session must be re-opened with :meth:`.Session.begin`.
        """
        # todo: added, dirty, deleted
        await self.transaction.commit()

    async def execute(self, query: BaseQuery):
        """
        Executes a query and runs it.

        This takes in a :class:`.BaseQuery` object and executes the actual query.

        :param query: The query object.
        :return: A ResultSet object.
        """
        # create the transaction for the sess
        if self.transaction is None:
            await self.begin()

        await self.transaction.acquire()

        # BEGIN
        final_query, params = query.get_token()
        final_sql = final_query.generate_sql()
        logger.debug("Running query `{}` with params `{}`".format(final_sql, params))
        results = await self.transaction.fetch(final_sql, params)

        # no implicit ROLLBACK

        return results
