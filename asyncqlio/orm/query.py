"""
Classes for query objects.
"""
import abc
import collections
import itertools
import typing

from asyncqlio.backends.base import BaseResultSet
from asyncqlio.meta import AsyncABC
from asyncqlio.orm import inspection as md_inspection, operators as md_operators, \
    session as md_session
from asyncqlio.orm.schema import column as md_column, relationship as md_relationship, \
    table as md_table
from asyncqlio.sentinels import NO_VALUE


class BaseQuery(AsyncABC):
    """
    A base query object.
    """

    def __init__(self, sess: 'md_session.Session'):
        """
        :param sess: The :class:`.Session` associated with this query.
        """
        self.session = sess

    @abc.abstractmethod
    def generate_sql(self) -> typing.Tuple[str, typing.Mapping[str, typing.Any]]:
        """
        Generates the SQL for this query.
        :return: A two item tuple, the SQL to use and a mapping of params to pass.
        """

    @abc.abstractmethod
    async def run(self):
        """
        Runs this query.
        """


class ResultGenerator(collections.AsyncIterator):
    """
    A helper class that will generate new results from a query when iterated over.
    """

    def __init__(self, q: 'SelectQuery'):
        """
        :param q: The :class:`.SelectQuery` to use. 
        """
        self.query = q
        self._results = None  # type: BaseResultSet

        self._result_deque = collections.deque()

    async def _fill(self):
        # peek from the first item
        try:
            first = self._result_deque[0]
        except IndexError:
            # no row was stored from last time
            last_pkey = None
            rows_filled = 0
        else:
            # there was a row from last time, so we need to check for any run-off rows for that
            last_pkey = tuple(first[col.alias_name(quoted=False)]
                              for col in self.query.table.primary_key.columns)
            # also, since there was technically one row before, we start at 1 here
            rows_filled = 1

        while True:
            # fetch a new row, make sure its not none
            row = await self._results.fetch_row()
            if row is None:
                break

            # add it to the results
            self._result_deque.append(row)
            # load the primary key via getting every column through alias name
            pkey = tuple(row[col.alias_name(quoted=False)] for col in
                         self.query.table.primary_key.columns)

            # if there was no results before, we just set the primary key and continue
            if last_pkey is None:
                last_pkey = pkey
                rows_filled += 1
                continue
            else:
                # otherwise, check if the primary key matches
                if pkey == last_pkey:
                    # it does, so it's a run-on row that has been joined
                    rows_filled += 1
                    continue
                else:
                    # it does not, so it's a new row
                    break

        # return the rows filled to ensure
        return rows_filled

    async def __anext__(self):
        # ensure we have a BaseResultSet
        if self._results is None:
            self._results = await self.query.session.cursor(*self.query.generate_sql())

        # get the number of rows filled off of the end
        filled = await self._fill()

        if filled == 0:
            raise StopAsyncIteration

        rows = [self._result_deque.popleft() for x in range(0, filled)]
        if len(rows) == 1:
            return self.query.map_columns(rows[0])

        return self.query.map_many(*rows)

    async def next(self):
        try:
            return await self.__anext__()
        except StopAsyncIteration:
            return None

    async def flatten(self) -> 'typing.List[md_table.Table]':
        """
        Flattens this query into a single list.
        """
        l = []
        async for result in self:
            l.append(result)

        return l


class SelectQuery(BaseQuery):
    """
    Represents a SELECT query, which fetches data from the database.
    
    This is not normally created by user code directly, but rather as a result of a 
    :meth:`.Session.select` call.
    
    .. code-block:: python3

        sess = db.get_session()
        async with sess:
            query = sess.select.from_(User)  # query is instance of SelectQuery
            # alternatively, but not recommended
            query = sess.select(User)
            
    However, it is possible to create this class manually:
    
    .. code-block:: python3

        query = SelectQuery(db.get_session()
        query.set_table(User)
        query.add_condition(User.id == 2)
        user = await query.first()
        
    """

    def __init__(self, session: 'md_session.Session'):
        super().__init__(session)

        #: The table being queried.
        self.table = None

        #: A list of conditions to fulfil.
        self.conditions = []

        #: The limit on the number of rows returned from this query.
        self.row_limit = None

        #: The offset to start fetching rows from.
        self.row_offset = None

        #: The column to order by.
        self.orderer = None

    def __call__(self, table):
        return self.from_(table)

    # used so you can async iterate over a query directly
    def __aiter__(self):
        return ResultGenerator(q=self)

    def _get_joins_for_table(self, parent: 'md_relationship.Relationship',
                             table: 'md_table.Table', seen: list = None):
        """
        Gets the foreign joins for a table.
        """
        if seen is None:
            seen = [table]

        foreign_tables = []
        joins = []
        for relationship in table.iter_relationships():
            # ignore non-join relationships
            if relationship.load_type != "joined":
                continue

            if relationship.foreign_table in seen:
                continue

            foreign_table = relationship.foreign_table
            foreign_tables.append(foreign_table)

            joins.append(relationship._get_join_query(parent))

        return foreign_tables, joins

    def _recursive_get_table_joins(self, parent: 'md_relationship.Relationship',
                                   table: 'md_table.Table', seen: list = None):
        """
        Recursively loads the joins for a table.

        :param parent: The parent relationship this table is being loaded from, or None if it was \
            loaded directly.
        :param table: The table to get joins for.
        :param seen: A list of tables that have already been seen and should not be re-joined.
        """
        # this scans the tree of relationships
        # and determines how to join them properly
        if seen is None:
            seen = [table]
        elif table in seen:
            return [], []

        foreign_tables, joins = self._get_joins_for_table(parent, table, seen=seen)
        for relationship in table.iter_relationships():
            if relationship.load_type != "joined":
                continue

            if relationship.foreign_table in seen:
                continue

            # get the table joins for the foreign table
            f, j = self._recursive_get_table_joins(relationship,
                                                   relationship.foreign_table, seen=seen)
            seen.append(relationship.foreign_table)
            foreign_tables.extend(f), joins.extend(j)

        return foreign_tables, joins

    def get_required_join_paths(self):
        """
        Gets the required join paths for this query.
        """
        # we can just pass None since it's the first in the chain
        return self._recursive_get_table_joins(None, self.table, seen=None)

    def generate_sql(self) -> typing.Tuple[str, dict]:
        """
        Generates the SQL for this query. 
        """
        counter = itertools.count()

        # calculate the column names
        foreign_tables, joins = self.get_required_join_paths()
        selected_columns = self.table.iter_columns()
        column_names = []

        for table in itertools.chain([self.table], foreign_tables):
            for column in table.iter_columns():
                a = column.alias_name(table=table, quoted=True)
                column_names.append(r'{} AS {}'.format(column.quoted_fullname_with_table(table), a))

        # BEGIN THE GENERATION
        fmt = "SELECT {} FROM {} ".format(", ".join(column_names), self.table.__quoted_name__)
        # cleanup after ourselves for a bit
        del selected_columns

        # format conditions
        params = {}
        c_sql = []
        for condition in self.conditions:
            response = condition.generate_sql(self.session.bind.emit_param, counter)
            params.update(response.parameters)
            c_sql.append(response.sql)

        # append joins
        fmt += " ".join(joins)

        # append the fmt with the conditions
        # these are assumed to be And if there are multiple!
        if c_sql:
            fmt += " WHERE {}".format(" AND ".join(c_sql))

        if self.orderer is not None:
            res = self.orderer.generate_sql(self.session.bind.emit_param, counter)
            fmt += " ORDER BY {}".format(res.sql)
        if self.row_limit is not None:
            fmt += " LIMIT {}".format(self.row_limit)

        if self.row_offset is not None:
            fmt += " OFFSET {}".format(self.row_offset)

        return fmt, params

    # "fetch" methods
    async def first(self) -> 'md_table.Table':
        """
        Gets the first result that matches from this query.
        
        :return: A :class:`.Table` instance representing the first item, or None if no item matched.
        """
        gen = await self.session.run_select_query(self)
        row = await gen.next()

        if row is not None:
            return row

    async def all(self) -> 'ResultGenerator':
        """
        Gets all results that match from this query.
        
        :return: A :class:`.ResultGenerator` that can be iterated over.
        """
        return await self.session.run_select_query(self)

    async def run(self):
        return await self.all()

    # ORM methods
    def map_columns(self, results: typing.Mapping[str, typing.Any]) -> 'md_table.Table':
        """
        Maps columns in a result row to a :class:`.Table` instance object.
        
        :param results: A single row of results from the query cursor.
        :return: A new :class:`.Table` instance that represents the row returned.
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

        # create a new Table
        row = self.table._internal_from_row(row_expando, existed=True)

        # update previous values
        for column in self.table.iter_columns():
            val = row.get_column_value(column, return_default=False)
            if val is not NO_VALUE:
                row._previous_values[column] = val

        # update the existed
        md_inspection._set_mangled(row, "existed", True)
        # give the row a session
        row._session = self.session

        # ensure relationships are cascaded
        row._update_relationships(relation_data)

        return row

    def map_many(self, *rows: typing.Mapping[str, typing.Any]):
        """
        Maps many records to one row.
        
        This will group the records by the primary key of the main query table, then add additional
        columns as appropriate.
        """
        # this assumes that the rows come in grouped by PK on the left
        # also fuck right joins
        # get the first row and construct the first table row using map_columns
        # this will also map any extra relationship data there
        first_row = rows[0]
        tbl_row = self.map_columns(first_row)

        # loop over every "extra" rows
        # and update the relationship data in the table
        for runon_row in rows[1:]:
            tbl_row._update_relationships(runon_row)
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

    def where(self, *conditions: 'md_operators.BaseOperator') -> 'SelectQuery':
        """
        Adds a WHERE clause to the query. This is a shortcut for :meth:`.SelectQuery.add_condition`.
        
        .. code-block:: python3

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

    def order_by(self, *col: 'typing.Union[md_column.Column, md_operators.Sorter]',
                 sort_order: str = "asc"):
        """
        Sets the order by clause for this query.
        
        The argument provided can either be a :class:`.Column`, or a :class:`.Sorter` which is 
        provided by :meth:`.Column.asc` / :meth:`.Column.desc`. By default, ``asc`` is used when
        passing a column. 
        """
        if not col:
            raise TypeError("Must provide at least one item to order with")

        if len(col) == 1 and isinstance(col[0], md_operators.Sorter):
            self.orderer = col[0]
        else:
            if sort_order == "asc":
                self.orderer = md_operators.AscSorter(*col)
            elif sort_order == "desc":
                self.orderer = md_operators.DescSorter(*col)
            else:
                raise TypeError("Unknown sort order {}".format(sort_order))

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

    def add_condition(self, condition: 'md_operators.BaseOperator') -> 'SelectQuery':
        """
        Adds a condition to the query/
        
        :param condition: The :class:`.BaseOperator` to add.
        :return: This query.
        """
        self.conditions.append(condition)
        return self


class InsertQuery(BaseQuery):
    """
    Represents an INSERT query.
    """

    def __init__(self, sess: 'md_session.Session'):
        super().__init__(sess)

        #: A list of rows to generate the insert statements for.
        self.rows_to_insert = []

    def __await__(self):
        return self.run().__await__()

    async def run(self) -> 'typing.List[md_table.Table]':
        """
        Runs this query.
        
        :return: A list of inserted :class:`.md_table.Table`.
        """
        return await self.session.run_insert_query(self)

    def rows(self, *rows: 'md_table.Table') -> 'InsertQuery':
        """
        Adds a set of rows to the query.
        
        :param rows: The rows to insert. 
        :return: This query.
        """
        for row in rows:
            self.add_row(row)

        return self

    def add_row(self, row: 'md_table.Table') -> 'InsertQuery':
        """
        Adds a row to this query, allowing it to be executed later.
        
        :param row: The :class:`.Table` instance to use for this query.
        :return: This query.
        """
        self.rows_to_insert.append(row)
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

        for row in self.rows_to_insert:
            query, params = row._get_insert_sql(emit, self.session)
            queries.append((query, params))

        return queries


class BulkQuery(BaseQuery, metaclass=abc.ABCMeta):
    """
    Represents a **bulk query**.

    This allows adding conditionals to the query.
    """

    def __init__(self, sess: 'md_session.Session'):
        super().__init__(sess)

        #: The table to update.
        self._table = None  # type: md_table.TableMeta

        #: The list of conditions to query by.
        self.conditions = []

    def __call__(self, *args, **kwargs):
        return self.table(*args, **kwargs)

    def __await__(self):
        return self.run().__await__()

    # Builder methods
    def table(self, table: 'typing.Type[md_table.Table]'):
        """
        Sets the table for this query.
        """
        self._table = table
        return self

    def where(self, *conditions: 'md_operators.ComparisonOp'):
        """
        Sets the conditions for this query.
        """
        self.conditions.extend(conditions)
        return self

    # Manual-style methods
    def set_table(self, table: 'typing.Type[md_table.Table]'):
        """
        Sets a table on this query.
        """
        self._table = table

    def add_condition(self, condition: 'md_operators.BaseOperator'):
        """
        Adds a condition to this query.
        """
        self.conditions.append(condition)


class BulkUpdateQuery(BulkQuery):
    """
    Represents a **bulk update query**. This updates many rows based on certain criteria.

    .. code-block:: python3

        query = BulkUpdateQuery(session)

        # style 1: manual
        query.set_table(User)
        query.add_condition(User.xp < 300)
        # add on a value
        query.set_update(User.xp + 100)
        # or set a value
        query.set_update(User.xp.set(300))
        await query.run()

        # style 2: builder
        await query.table(User).where(User.xp < 300).set(User.xp + 100).run()
        await query.table(User).where(User.xp < 300).set(User.xp, 300).run()

    """

    def __init__(self, sess: 'md_session.Session'):
        super().__init__(sess)
        #: The thing to set on the updated rows.
        self.setting = None

    def set(self, setter, value: typing.Any = None):
        """
        Sets a column in this query.
        """
        if value is not None:
            setter = md_operators.ValueSetter(setter, value)

        self.setting = setter
        return self

    def set_update(self, update):
        """
        Sets the update for this query.
        """
        self.setting = update

    def generate_sql(self):
        """
        Generates the SQL for this query.
        """
        # base query is update table
        query = 'UPDATE {} SET '.format(self._table.__quoted_name__)

        # define counter and params used in generating sql
        counter = itertools.count()
        params = {}

        # get the sql and params from the generate_sql call
        response = self.setting.generate_sql(self.session.bind.emit_param, counter)
        # update params
        params.update(response.parameters)
        query += response.sql

        # format conditions
        c_sql = []
        for condition in self.conditions:
            # pass the condition offset
            res = condition.generate_sql(self.session.bind.emit_param, counter)
            params.update(res.parameters)
            c_sql.append(res.sql)

        query += ' WHERE ' + ' AND '.join(c_sql)

        # all generated
        return query, params

    async def run(self):
        return await self.session.run_update_query(self)


class BulkDeleteQuery(BulkQuery):
    """
    Represents a **bulk delete query**. This deletes many rows based on criteria.

    .. code-block:: python3

        query = BulkDeleteQuery(session)

        # style 1: manual
        query.set_table(User)
        query.add_condition(User.xp < 300)
        await query.run()

        # style 2: builder
        await query.table(User).where(User.xp < 300).run()
        await query.table(User).where(User.xp < 300).run()
    """

    def generate_sql(self):
        query = "DELETE FROM {} ".format(self._table.__quoted_name__)

        # define counter and params used in generating sql
        counter = itertools.count()
        params = {}

        # format conditions
        c_sql = []
        for condition in self.conditions:
            # pass the condition offset
            res = condition.generate_sql(self.session.bind.emit_param, counter)
            params.update(res.parameters)
            c_sql.append(res.sql)

        query += ' WHERE ' + ' AND '.join(c_sql)
        return query, params

    async def run(self):
        return await self.session.run_delete_query(self)


class RowUpdateQuery(BaseQuery):
    """
    Represents a **row update query**. This is **NOT** a bulk update query - it is used for updating
    specific rows.
    """

    def __init__(self, sess: 'md_session.Session'):
        super().__init__(sess)

        #: The list of rows to update.
        self.rows_to_update = []

    def __await__(self):
        return self.run().__await__()

    async def run(self):
        """
        Executes this query.
        """
        return await self.session.run_update_query(self)

    def rows(self, *rows: 'md_table.Table') -> 'RowUpdateQuery':
        """
        Adds a set of rows to the query.

        :param rows: The rows to insert. 
        :return: This query.
        """
        for row in rows:
            self.add_row(row)

        return self

    def add_row(self, row: 'md_table.Table') -> 'RowUpdateQuery':
        """
        Adds a row to this query, allowing it to be executed later.

        :param row: The :class:`.Table` instance to use for this query.
        :return: This query.
        """
        self.rows_to_update.append(row)
        return self

    def generate_sql(self) -> typing.List[typing.Tuple[str, tuple]]:
        """
        Generates the SQL statements for this row update query.
        
        This will return a list of two-item tuples to execute: 
        
            - The SQL query+params to emit to actually insert the row
        """
        queries = []
        counter = itertools.count()

        def emit():
            return "param_{}".format(next(counter))

        for row in self.rows_to_update:
            queries.append(row._get_update_sql(emit, self.session))

        return queries


class RowDeleteQuery(BaseQuery):
    """
    Represents a row deletion query. This is **NOT** a bulk delete query - it is used for deleting
    specific rows.
    """

    def __init__(self, sess: 'md_session.Session'):
        super().__init__(sess)

        #: The list of rows to delete.
        self.rows_to_delete = []

    def rows(self, *rows: 'md_table.Table') -> 'RowDeleteQuery':
        """
        Adds a set of rows to the query.

        :param rows: The rows to insert. 
        :return: This query.
        """
        for row in rows:
            self.add_row(row)

        return self

    def add_row(self, row: 'md_table.Table'):
        """
        Adds a row to this query.
        
        :param row: The :class:`.Table` instance  
        :return: 
        """
        self.rows_to_delete.append(row)

    def generate_sql(self) -> typing.List[typing.Tuple[str, tuple]]:
        """
        Generates the SQL statements for this row delete query.

        This will return a list of two-item tuples to execute: 
        
            - The SQL query+params to emit to actually insert the row
        """
        queries = []
        counter = itertools.count()

        def emit():
            return "param_{}".format(next(counter))

        for row in self.rows_to_delete:
            queries.append(row._get_delete_sql(emit, self.session))

        return queries

    async def run(self):
        return await self.session.run_delete_query(self)
