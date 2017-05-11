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

    def __init__(self, bind: 'md_kg.Katagawa', allow_notifications: bool = False):
        """
        :param bind: The :class:`.Katagawa` instance we are bound to. 
        """
        self.bind = bind

        #: If this session allows row notifications.
        self.allow_notifications = allow_notifications

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

    def notify_set(self, row: 'md_row.TableRow'):
        """
        Notifies that a row has been set. Typically called by ``ColumnType.on_set``.  
        This will put the row into the ``dirty`` list of rows, if it isn't in ``new`` or 
        ``deleted``.
        
        :param row: The :class:`.TableRow` to be notified of.
        """
        if not self.allow_notifications:
            return

        if row._TableRow__existed is True:
            if not [row in x for x in [self.new, self.dirty, self.deleted]]:
                self.dirty.append(row)

    # Query builders
    def select(self, table) -> 'md_query.SelectQuery':
        """
        Creates a new SELECT query that can be built upon.
        
        :param table: The :class:`.Table` to select. 
        :return: A new :class:`.SelectQuery`.
        """
        q = md_query.SelectQuery(self)
        q.set_table(table)
        return q

    def _generate_inserts(self, new: list = None) \
            -> typing.List[typing.Tuple[str, typing.Dict[str, typing.Any]]]:
        """
        Generates INSERT INTO queries for the current session.
        """
        queries = []
        counter = itertools.count()

        if new is None:
            new = self.new

        # group the rows
        for tbl, rows in itertools.groupby(new, lambda r: r.table):
            rows = list(rows)
            # insert into the quoted table
            base_query = "INSERT INTO {} ".format(tbl.__quoted_name__)
            # add the columns (quoted names)
            base_query += "({})".format(", ".join(column.quoted_name
                                                  for column in tbl.iter_columns()))
            # get the values
            base_query += " VALUES "

            # build the params dict and VALUES section
            # this is done by looping over the rows
            # looping over the columns of the row's table
            # checking for the new value, then emitting it
            # if no value is available, it will emit DEFAULT
            params = {}
            value_sets = []
            for row in rows:
                prms_so_far = []
                assert isinstance(row, md_row.TableRow)
                # row._validate()
                for column in tbl.iter_columns():
                    value = row.get_column_value(column, return_default=True)
                    if value is NO_VALUE or (value is None and column.default is NO_DEFAULT):
                        prms_so_far.append("DEFAULT")
                    else:
                        # emit a new param
                        number = next(counter)
                        name = "param_{}".format(number)
                        param_name = self.bind.emit_param(name)
                        # set the params to value
                        # then add the {param_name} to the VALUES
                        params[name] = value
                        prms_so_far.append(param_name)

                # join the params together
                value_sets.append("({})".format(", ".join(prms_so_far)))

            # join all of the value sets together
            base_query += ", ".join(value_sets)
            base_query += ";"
            queries.append((base_query, params, rows))

        return queries

    def _generate_updates(self) -> typing.List[typing.Tuple[str, typing.Dict[str, typing.Any]]]:
        """
        Generates UPDATE queries for all dirty rows in the current session.
        """
        queries = []
        counter = itertools.count()
        # helper functions
        next_param = lambda: "param_{}".format(next(counter))
        emit = self.bind.emit_param

        # don't group by - we can't do multi updates in one go (yet)
        for row in self.dirty:
            assert isinstance(row, md_row.TableRow)
            params = {}
            base_query = "UPDATE {} SET ".format(row.table.__quoted_name__)

            # extract the row history
            history = md_inspection.get_row_history(row)
            sets = []
            for col, d in history.items():
                # don't bother setting it
                if d["old"] is None:
                    continue

                # get the next param from the counter
                # then store the name and the value in the row
                p = next_param()
                params[p] = d["new"]
                sets.append("{} = {}".format(col.quoted_name, emit(p)))

            # join the set actions, and add it to the base query
            base_query += ", ".join(sets)

            wheres = []
            for col in row.table.primary_key.columns:
                # get the param name
                # then store it in the params counter
                # and build a new condition for the WHERE clause
                p = next_param()
                params[p] = history[col]["old"]
                wheres.append("{} = {}".format(col.quoted_name, emit(p)))
            base_query += " WHERE "
            base_query += " AND ".join(wheres)
            base_query += ";"

            queries.append((base_query, params, [row]))

        return queries

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

    @enforce_open
    async def commit(self):
        """
        Commits the current session, running inserts/updates/deletes.
         
        This will **not** close the session; it can be re-used after a commit.
        """
        # TODO: Checkpoints
        # TODO: More complex logic relating to dependants and relationships
        inserts = self._generate_inserts()
        for query, params, rows in inserts:
            await self.execute(query, params=params)
            for row in rows:
                self.new.remove(row)

        # TODO: Investigate update performance increase possibilities
        updates = self._generate_updates()
        for query, params, rows in updates:
            await self.execute(query, params=params)
            for row in rows:
                self.dirty.remove(row)

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
            
            Also, tables with auto-incrementing compound primary keys will not have all fields 
            filled properly.
        
        :param row: The :class:`.TableRow` to insert.
        :return: The row, with primary key included.
        """
        generated = self._generate_inserts(new=[row])[0]
        await self.execute(generated[0], generated[1])

        # check if the row already had a primary key
        pk = row.primary_key
        if not isinstance(pk, tuple):
            pk = (pk,)

        if any(pk):
            return row
        else:
            # fetch LASTVAL() or the equivalent
            lastval_func = self.bind.dialect.lastval_method
            fmt = "SELECT {}".format(lastval_func, lastval_func)
            try:
                pkey = await self.fetch(fmt)
            except OperationalError:
                raise OperationalError("Unable to determine the primary key of this row.")

            for col, val in itertools.zip_longest(row.table.primary_key.columns, pkey):
                row.store_column_value(col, val)

            return row

    def add(self, row: 'md_row.TableRow') -> 'Session':
        """
        Adds a row to this session, storing it for a later commit.
        
        :param row: The :class:`.TableRow` to add to this session.
        :return: This session.
        """
        if row._TableRow__existed is True:
            self.dirty.append(row)
        else:
            self.new.append(row)

        return self

    def merge(self, row: 'md_row.TableRow') -> 'Session':
        """
        Merges a row with this session.
        
        This should be used for rows with a primary key THAT ALREADY EXIST IN THE DATABASE.
        This will **NOT** insert new rows.
        """
        self.dirty.append(row)


    insert = add
