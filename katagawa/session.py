from katagawa.engine import BaseEngine
from katagawa.query import BaseQuery


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
        :return: A new :class:`BaseQuery` that can be used to query the database with.
        """
        query = self.query_class(session=self, **kwargs)
        return query

    async def execute(self, query):
        """
        Executes a query and runs it.

        This takes in a :class:`katagawa.structures.query.BaseQuery` object and executes the actual query.

        :param query: The query object.
        :return: A ResultSet object.
        """
        # Open a new transaction.
        transaction = self.engine.create_transaction(read_only=True)

        # Handle the query.
        results = await query.run_query(transaction)

        return results
