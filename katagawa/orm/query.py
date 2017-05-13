"""
Classes for query objects.
"""
import collections
import itertools
import typing
import warnings

import functools

import time

from katagawa.backends.base import BaseResultSet
from katagawa.orm import session as md_session
from katagawa.orm import inspection
from katagawa.orm.operators import BaseOperator
from katagawa.orm.schema import row as md_row


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
        self.last_primary_key = None

    def check_new(self, record):
        """
        Checks if this record has a new primary key.
        """
        vals = tuple(record[column.alias_name(quoted=False)]
                     for column in self.query.table.primary_key.columns)
        if vals == self.last_primary_key:
            return False

        else:
            if self.last_primary_key is None:
                self.last_primary_key = vals
                return True

            return True

    async def __anext__(self):
        # ensure we have a BaseResultSet
        if self._results is None:
            self._results = await self.query._execute()  # type: BaseResultSet

        row = await self._results.fetch_row()
        if row is None:
            raise StopAsyncIteration

        return self.query.map_columns(row)

    async def flatten(self) -> 'typing.List[md_row.TableRow]':
        """
        Flattens this query into a single list.
        """
        l = []
        async for result in self:
            l.append(result)

        return l


class SelectQuery(object):
    """
    Represents a SELECT query, which fetches data from the database.
    
    This is not normally created by user code directly, but rather as a result of a 
    :meth:`.Session.select` call.
    
    .. code-block:: python
        sess = db.get_session()
        async with sess:
            query = sess.select.from_(User)  # query is instance of SelectQuery
            # alternatively, but not recommended
            query = sess.select(User)
            
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

        #: The limit on the number of rows returned from this query.
        self.row_limit = None

        #: The offset to start fetching rows from.
        self.row_offset = None

    def __call__(self, table):
        return self.from_(table)

    def get_required_join_paths(self):
        """
        Gets the required join paths for this query.
        """
        foreign_tables = []
        joins = []
        for relationship in self.table.iter_relationships():
            # ignore join relationships
            if relationship.load_type != "joined":
                continue

            foreign_table = relationship.foreign_table
            foreign_tables.append(foreign_table)
            fmt = "JOIN {} ".format(foreign_table.__quoted_name__)
            column1, column2 = relationship.join_columns
            fmt += 'ON {} = {}'.format(column1.quoted_fullname, column2.quoted_fullname)
            joins.append(fmt)

        return foreign_tables, joins

    def generate_sql(self) -> typing.Tuple[str, dict]:
        """
        Generates the SQL for this query. 
        """
        counter = itertools.count()

        # calculate the column names
        foreign_tables, joins = self.get_required_join_paths()
        selected_columns = self.table.iter_columns()
        column_names = [r'"{}"."{}" AS {}'.format(column.table.__tablename__,
                                                  column.name, column.alias_name(quoted=True))
                        for column in
                        itertools.chain(self.table.iter_columns(),
                                        *[tbl.iter_columns() for tbl in foreign_tables])]

        # BEGIN THE GENERATION
        fmt = "SELECT {} FROM {} ".format(", ".join(column_names), self.table.__quoted_name__)
        # cleanup after ourselves for a bit
        del selected_columns

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

        # append joins
        fmt += " ".join(joins)

        # append the fmt with the conditions
        # these are assumed to be And if there are multiple!
        if c_sql:
            fmt += " WHERE {}".format(" AND ".join(c_sql))

        if self.row_limit is not None:
            fmt += " LIMIT {}".format(self.row_limit)

        if self.row_offset is not None:
            fmt += " OFFSET {}".format(self.row_offset)

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

    async def first(self) -> 'md_row.TableRow':
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
    def map_columns(self, results: typing.Mapping[str, typing.Any]) -> 'md_row.TableRow':
        """
        Maps columns in a result row to a :class:`.TableRow` object.
        
        :param results: A single row of results from the query cursor.
        :return: A new :class:`.TableRow` that represents the row returned.
        """
        # try and map columns to our Table
        mapping = {column.alias_name(self.table, quoted=False): column
                   for column in self.table.iter_columns()}
        row_expando = {}
        relation_data = {}

        for colname in results.keys():
            if colname in mapping:
                column = mapping[colname]
                row_expando[column.name] = results[colname]
            else:
                relation_data[colname] = results[colname]

        # create a new TableRow
        try:
            row = self.table(**row_expando)  # type: md_row.TableRow
        except TypeError:
            # probably the unexpected argument error
            raise TypeError("Failed to initialize a new row object. Does your `__init__` allow"
                            "all columns to be passed as values?")

        # update the existed
        row._TableRow__existed = True
        row._session = self.session

        ## ensure relationships are cascaded
        # row._update_relationships(relation_data)

        return row

    def map_many(self, *rows: typing.Mapping[str, typing.Any]):
        """
        Maps many records to one row.
        
        This will group the records by the primary key of the main query table, then add additional
        columns as appropriate.
        """
        # this assumes that the rows come in grouped by PK on the left
        # also fuck right joins
        # get the first row and construct the first table row using map_one
        first_row = rows[0]
        tbl_row = self.map_columns(first_row)

        # loop over every "extra" rows
        # and update the relationship data in the table
        for runon_row in rows[1:]:
            # TODO: Write this
            # tbl_row._update_relationships(runon_row)
            pass

        return tbl_row

    # Helper methods for natural builder-style queries
    def from_(self, tbl) -> 'SelectQuery':
        """
        Sets the table this query is selecting from.
        
        :param tbl: The :class:`.Table` object to select. 
        :return: This query.
        """
        self.set_table(tbl)
        return self

    def where(self, *conditions: BaseOperator) -> 'SelectQuery':
        """
        Adds a WHERE clause to the query. This is a shortcut for :meth:`.add_condition`.
        
        .. code-block:: python
            sess.select.from_(User).where(User.id == 1)
        
        :param conditions: The conditions to use for this WHERE clause.
        :return: This query.
        """
        for condition in conditions:
            self.add_condition(condition)

        return self

    def limit(self, row_limit: int) -> 'SelectQuery':
        """
        Sets a limit of the number of rows that can be returned from this query.
        
        :param row_limit: The maximum number of rows to return. 
        :return: This query.
        """
        self.row_limit = row_limit
        return self

    def offset(self, offset: int) -> 'SelectQuery':
        """
        Sets the offset of rows to start returning results from/
        
        :param offset: The row offset. 
        :return: This query.
        """
        self.row_offset = offset
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


class InsertQuery(object):
    """
    Represents an INSERT query.
    """

    def __init__(self, sess: 'md_session.Session'):
        """
        :param sess: The :class:`.Session` this object is bound to. 
        """
        #: The :class:`.Session` associated with this query.
        self.session = sess

        #: A list of rows to generate the insert statements for.
        self.rows = []

    def __await__(self):
        return self.run()

    async def run(self) -> 'typing.List[md_row.TableRow]':
        """
        Runs this query.
        
        :return: A list of inserted :class:`.md_row.TableRow`.
        """
        return await self.session._do_insert_query(self)

    def rows(self, *rows: 'md_row.TableRow') -> 'InsertQuery':
        """
        Adds a set of rows to the query.
        
        :param rows: The rows to insert. 
        :return: This query.
        """
        for row in rows:
            self.add_row(row)

        return self

    def add_row(self, row: 'md_row.TableRow') -> 'InsertQuery':
        """
        Adds a row to this query, allowing it to be executed later.
        
        :param row: The :class:`.TableRow` to use for this query.
        :return: This query.
        """
        self.rows.append(row)
        return self

    def generate_sql(self) -> typing.List[typing.Tuple[str, tuple]]:
        """
        Generates the SQL statements for this insert query.
        
        This will return a list of two-item tuples to execute: 
            - The SQL query+params to emit to actually insert the row
        """
        queries = []
        counter = itertools.count()

        def emit():
            return "param_{}".format(next(counter))

        for row in self.rows:
            query, params = row._get_insert_sql(emit, self.session)
            queries.append((query, params))

        return queries
