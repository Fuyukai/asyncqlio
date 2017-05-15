import logging
import typing

import enum

import functools

import itertools

from katagawa import kg as md_kg
from katagawa.exc import OperationalError
from katagawa.orm import inspection as md_inspection
from katagawa.orm import query as md_query
from katagawa.backends.base import BaseTransaction
from katagawa.orm.schema import row as md_row
from katagawa.sentinels import NO_DEFAULT, NO_VALUE

logger = logging.getLogger(__name__)


class SessionState(enum.Enum):
    NOT_READY = 0
    READY = 1
    CLOSED = 2


# decorators
def enforce_open(func):
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        if self._state is not SessionState.READY:
            raise RuntimeError("Session is not ready or closed")
        else:
            return func(self, *args, **kwargs)

    return wrapper


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

    async def __aenter__(self) -> 'Session':
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> 'Session':
        try:
            if exc_type is None:
                await self.commit()
            else:
                await self.rollback()
        except Exception:
            # error in committing, etc
            await self.rollback()
            raise
        finally:
            await self.close()

        return False

    # Query builders
    @property
    def select(self) -> 'md_query.SelectQuery':
        """
        Creates a new SELECT query that can be built upon.
        
        :return: A new :class:`.SelectQuery`.
        """
        q = md_query.SelectQuery(self)
        return q

    @property
    def insert(self) -> 'md_query.InsertQuery':
        """
        Creates a new INSERT INTO query that can be built upon.
        
        :return: A new :class:`.InsertQuery`. 
        """
        return md_query.InsertQuery(self)

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

        logger.debug("Acquiring new transaction, and beginning")
        self.transaction = self.bind.get_transaction()
        await self.transaction.begin()

        self._state = SessionState.READY
        return self

    @enforce_open
    async def commit(self):
        """
        Commits the current session, running inserts/updates/deletes.
         
        This will **not** close the session; it can be re-used after a commit.
        """
        logger.debug("Committing transaction")
        await self.transaction.commit()
        return self

    @enforce_open
    async def rollback(self, checkpoint: str = None):
        """
        Rolls the current session back.  
        This is useful if an error occurs inside your code.
        
        :param checkpoint: The checkpoint to roll back to, if applicable. 
        """
        await self.transaction.rollback(checkpoint=checkpoint)
        return self

    @enforce_open
    async def close(self):
        """
        Closes the current session.
        
        .. warning::
            This will **NOT COMMIT ANY DATA**. Old data will die.
        """
        await self.transaction.close()
        self._state = SessionState.CLOSED
        del self.transaction

    @enforce_open
    async def fetch(self, sql: str, params=None):
        """
        Fetches a single row.
        """
        cur = await self.transaction.cursor(sql, params)
        next = await cur.fetch_row()
        await cur.close()
        return next

    @enforce_open
    async def execute(self, sql: str, params: typing.Union[typing.Mapping[str, typing.Any],
                                                           typing.Iterable[typing.Any]] = None):
        """
        Executes SQL inside the current session.
        
        This is part of the **low-level API.**
        
        :param sql: The SQL to execute.
        :param params: The parameters to use inside the query.
        """
        return await self.transaction.execute(sql, params)

    @enforce_open
    async def cursor(self, sql: str, params: typing.Union[typing.Mapping[str, typing.Any],
                                                          typing.Iterable[typing.Any]] = None):
        """
        Executes SQL inside the current session, and returns a new :class:`.BaseResultSet.`
        
        :param sql: The SQL to execute.
        :param params: The parameters to use inside the query.
        """
        return await self.transaction.cursor(sql, params)

    @enforce_open
    async def insert_now(self, row: 'md_row.TableRow') -> typing.Any:
        """
        Inserts a row NOW. 
        
        .. warning::
            This will only generate the INSERT statement for the row now. Only :meth:`.commit` will
            actually commit the row to storage.
            
            Also, tables with auto-incrementing fields will only have their first field filled in
            outside of Postgres databases.
        
        :param row: The :class:`.TableRow` to insert.
        :return: The row, with primary key included.
        """
        # this just creates a new query
        # and calls _do_insert_query
        # to actually run the query
        q = md_query.InsertQuery(self)
        q.add_row(row)
        result = await self.run_insert_query(q)
        try:
            return result[0]
        except IndexError:
            return None

    @enforce_open
    async def update_now(self, row: 'md_row.TableRow') -> 'md_row.TableRow':
        """
        Updates a row NOW. 

        .. warning::
            This will only generate the UPDATE statement for the row now. Only :meth:`.commit` will
            actually commit the row to storage.

        :param row: The :class:`.TableRow` to update.
        :return: The :class:`.TableRow` that was updated.
        """
        q = md_query.RowUpdateQuery(self)
        q.add_row(row)

        await self.run_update_query(q)
        return row

    async def _run_insert(self, row: 'md_row.TableRow', query: str, params):
        # this needs to be a cursor
        # since postgres uses RETURNING
        cur = await self.cursor(query, params)
        # some drivers don't execute until this is done
        # (asyncpg, apparently)
        # so always fetch a row now
        pkey_rows = await cur.fetch_row()

        if self.bind.dialect.has_returns:
            for colname, value in pkey_rows.items():
                try:
                    column = next(filter(lambda column: column.name == colname,
                                         row.table.iter_columns()))
                except StopIteration:
                    # wat
                    continue
                row.store_column_value(column, value, track_history=False)
                await cur.close()
        else:
            # TODO: Figure out how to implement this properly.
            await cur.close()

        return row

    async def run_insert_query(self, query: 'md_query.InsertQuery'):
        """
        Does an insert, based on a query.
        
        :param query: The :class:`.InsertQuery` to use.
        :return: The list of rows that were inserted.
        """
        queries = query.generate_sql()
        results = []

        for row, (sql, params) in zip(query.rows_to_insert, queries):
            results.append(await self._run_insert(row, sql, params))

        return results

    async def run_update_query(self, query: 'md_query.RowUpdateQuery'):
        """
        Executes an update query.
        
        :param query: The :class:`.RowUpdateQuery` or :class:`.BulkUpdateQuery` to execute. 
        """
        if isinstance(query, md_query.RowUpdateQuery):
            for sql, params in query.generate_sql():
                if sql is None and params is None:
                    continue
                await self.execute(sql, params)

        return query

    async def add(self, row: 'md_row.TableRow') -> 'md_row.TableRow':
        """
        Adds a row to the current transaction. This will emit SQL that will generate an INSERT or 
        UPDATE statement, and then update the primary key of this row.
        
        .. warning::
            This will only generate the INSERT statement for the row now. Only :meth:`.commit` will
            actually commit the row to storage.
    
        :param row: The :class:`.TableRow` object to add to the transaction.
        :return: The :class:`.TableRow` with primary key filled in, if applicable.
        """
        # it already existed in our session, so emit a UPDATE
        if row._TableRow__existed:
            return await self.update_now(row)
        # otherwise, emit an INSERT
        else:
            return await self.insert_now(row)

    async def merge(self, row: 'md_row.TableRow') -> 'md_row.TableRow':
        """
        Merges a row with a row that already exists in the database.
        
        This should be used for rows that have a primary key, but were not returned from 
        :meth:`.select`.
        
        :param row: The :class:`.TableRow` to merge. 
        :return: The :class:`.TableRow` once updated.
        """
        return await self.update_now(row)
