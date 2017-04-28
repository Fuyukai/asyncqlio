"""
Classes for query objects.
"""
import collections
import itertools
import typing
import warnings

from katagawa.backends.base import BaseResultSet
from katagawa.orm import session as md_session
from katagawa.orm import schema as md_schema
from katagawa.orm.operators import BaseOperator
from katagawa.orm.schema import TableRow


class _ResultGenerator(collections.AsyncIterator):
    """
    A helper class that will generate new results from a query when iterated over.
    """

    def __init__(self, q: 'SelectQuery'):
        """
        :param q: The :class:`.SelectQuery` to use. 
        """
        self.query = q

        self._results = None  # type: BaseResultSet

    async def __anext__(self):
        # ensure we have a BaseResultSet
        if self._results is None:
            self._results = await self.query._execute()  # type: BaseResultSet

        row = await self._results.fetch_row()
        if row is None:
            raise StopAsyncIteration

        mapped = self.query.map_columns(row)
        return row

    async def flatten(self) -> 'typing.List[md_schema.TableRow]':
        """
        Flattens this query into a single list.
        """
        l = []
        async for result in self:
            l.append(self.query.map_columns(result))

        return l


class SelectQuery(object):
    """
    Represents a SELECT query, which fetches data from the database.
    
    This is not normally created by user code directly, but rather as a result of a 
    :meth:`.Session.select` call.
    
    .. code-block:: python
        sess = db.get_session()
        async with sess:
            query = sess.select(User)  # query is instance of SelectQuery
            
    However, it is possible to create this class manually:
    
    .. code-block:: python
        query = SelectQuery(db.get_session()
        query.set_table(User)
        query.add_condition(User.id == 2)
        user = await query.first()
        
    """

    def __init__(self, session: 'md_session.Session'):
        """
        :param session: The session to bind to this query.
        """
        self.session = session

        #: The table being queried.
        self.table = None

        #: A list of conditions to fulfil.
        self.conditions = []

        # TODO: Order by, limit, etc

    def generate_sql(self) -> typing.Tuple[str, dict]:
        """
        Generates the SQL for this query. 
        """
        counter = itertools.count()

        # calculate the column names
        columns = [r'"{}"."{}" AS {}'.format(column.table.__tablename__,
                                             column.name, column.alias_name(quoted=True))
                   for column in self.table.columns]

        # format the basic select
        fmt = "SELECT {} FROM {} ".format(','.join(columns), self.table.__quoted_name__)
        # format conditions
        params = {}
        c_sql = []
        for condition in self.conditions:
            # pass the condition offset
            condition_sql, name, val = condition.generate_sql(self.session.bind.emit_param, counter)
            if val is not None:
                # special-case
                # this means it's a coalescing token
                if isinstance(val, dict) and name is None:
                    params.update(val)
                else:
                    params[name] = val

            c_sql.append(condition_sql)

        # append the fmt with the conditions
        # these are assumed to be And if there are multiple!
        if c_sql:
            fmt += "WHERE {}".format(" AND ".join(c_sql))

        return fmt, params

    # "fetch" methods
    async def _execute(self) -> BaseResultSet:
        """
        Executes this query in the session bound.
        :return: A :class:`.BaseResultSet` representing the results of this query.
        """
        sql, params = self.generate_sql()
        results = await self.session.cursor(sql, params)
        return results

    async def first(self) -> 'TableRow':
        """
        Gets the first result that matches from this query.
        
        :return: A :class:`.TableRow` representing the first item, or None if no item matched.
        """
        result_set = await self._execute()
        row = await result_set.fetch_row()

        # only map if the row isn't none
        if row is not None:
            return self.map_columns(row)

    async def all(self) -> '_ResultGenerator':
        """
        Gets all results that match from this query.
        
        :return: A :class:`._ResultGenerator` that can be iterated over.
        """
        # note: why is all a coroutine?
        # it looks more consistent, especially with .first().
        return _ResultGenerator(self)

    # ORM methods
    def map_columns(self, results: typing.Mapping[str, typing.Any]) -> 'md_schema.TableRow':
        """
        Maps columns in a result row to a :class:`.TableRow` object.
        
        :param results: A single row of results from the query cursor.
        :return: A new :class:`.TableRow` that represents the row returned.
        """
        # try and map columns to our Table
        mapping = {column.alias_name(self.table, quoted=False): column
                   for column in self.table.iter_columns()}
        row_expando = {}

        for colname in results.keys():
            # TODO: Joins
            if colname in mapping:
                column = mapping[colname]
                row_expando[column.name] = results[colname]

        # create a new TableRow
        row = self.table(**row_expando)  # type: md_schema.TableRow
        # update the existed
        row._TableRow__existed = True
        row._session = self.session

        return row

    # Helper methods for natural builder-style queries
    def where(self, *conditions: BaseOperator):

        """
        Adds a WHERE clause to the query. This is a shortcut for :meth:`.add_condition`.
        
        .. code-block:: python
            sess.select(User).where(User.id == 1)
        
        :param conditions: The conditions to use for this WHERE clause.
        :return: This query.
        """
        for condition in conditions:
            self.add_condition(condition)

        return self

    # "manual" methods
    def set_table(self, tbl) -> 'SelectQuery':
        """
        Sets the table to query on.
        
        :param tbl: The :class:`.Table` object to set. 
        :return: This query.
        """
        self.table = tbl
        return self

    def add_condition(self, condition: BaseOperator) -> 'SelectQuery':
        """
        Adds a condition to the query/
        
        :param condition: The :class:`.BaseOperator` to add.
        :return: This query.
        """
        self.conditions.append(condition)
        return self
