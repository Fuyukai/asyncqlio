import logging
import typing

import enum

import functools

import itertools

from katagawa import kg as md_kg
from katagawa.orm import query as md_query
from katagawa.backends.base import BaseTransaction
from katagawa.orm.schema import TableRow
from katagawa.orm.schema.row import NO_VALUE

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

    def _generate_inserts(self) -> typing.List[typing.Tuple[str, typing.Dict[str, typing.Any]]]:
        """
        Generates INSERT INTO queries for the current session.
        """
        queries = []
        counter = itertools.count()

        # group the rows
        for tbl, rows in itertools.groupby(self.new, lambda r: r._table):
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
                assert isinstance(row, TableRow)
                #row._validate()
                for column in tbl.iter_columns():
                    value = row.get_column_value(column, return_default=False)
                    if value is NO_VALUE:
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
        # TODO: Update generation
        # TODO: Checkpoints
        inserts = self._generate_inserts()
        for query, params, rows in inserts:
            await self.execute(query, params=params)
            for row in rows:
                self.new.remove(row)

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

    def add(self, row: TableRow) -> 'Session':
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

    insert = add
