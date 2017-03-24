import logging

from katagawa.engine import BaseEngine
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
        self.engine = engine

        # Define the query class.
        self.query_class = kwargs.pop("query_cls", BaseQuery)

        # Added, dirty, and deleted items.
        self.added = []
        self.dirty = []
        self.deleted = []

    def query(self, **kwargs) -> BaseQuery:
        """
        Produces a new Query object, bound to this session.
        :return: A new :class:`.BaseQuery` that can be used to query the database with.
        """
        query = self.query_class(session=self, **kwargs)
        return query

    async def execute(self, query: BaseQuery):
        """
        Executes a query and runs it.

        This takes in a :class:`.BaseQuery` object and executes the actual query.

        :param query: The query object.
        :return: A ResultSet object.
        """
        # create the transaction for the sess
        transaction = await self.engine.create_transaction()

        # BEGIN
        async with transaction:
            final_query, params = query.get_token()
            final_sql = final_query.generate_sql()
            logger.debug("Running query `{}` with params `{}`".format(final_sql, params))
            results = await transaction.execute(final_sql, params)

        # COMMIT/ROLLBACK

        return results
