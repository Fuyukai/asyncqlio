"""
Module containing the actual query
"""
import logging
import typing

from katagawa.engine.transaction import Transaction
from katagawa.exceptions import TableConflictException
from katagawa.orm.table import Table
from katagawa.sql.dialects.common import Select, Column, From, Where, Operator

logger = logging.getLogger("Katagawa.query")


class BaseQuery(object):
    """
    A BaseQuery object is used to query the database with SELECT statements or otherwise.

    It is produced from a :meth:`katagawa.sessions.session.Session.query` and is used to actually query the database.
    """
    def __init__(self, session, **kwargs):
        """
        Creates a new BaseQuery.

        :param session: The session to bind this query to.
        """
        self.session = session

        # Define a dict of tables to access in this query.
        self.tables = {}

        # Define a list of conditions to generate in the SELECT.
        self.conditions = []

    # internal workhouse methods
    async def run_query(self, transaction: Transaction):
        """
        Runs a query in a transaction.
        
        This will execute the query on the current engine, and return the results set.
        
        :param transaction: An instance of a type that implements :class:`~.engine.Transaction`.
            This will be used to actually run the query.
         
        :return: 
        """
        async with transaction:
            # todo: params
            final_query = self.get_tokens()
            final_sql = final_query.generate_sql()
            logger.debug("Running query: {}".format(final_sql))
            results = await transaction.execute(final_sql, {})

        return results

    # query methods

    def select(self, *tables: Table):
        """
        Selects some tables to query.

        :param tables: A list of DeclarativeMeta or aliased tables to query.
        :return: Ourself.
        """
        for table in tables:
            if not isinstance(table, Table):
                raise TypeError("Table must be instance of Table")

            self.tables[table.name] = table

        return self

    def where(self, *conditions):
        """
        Adds conditions to the query.

        :param conditions: A list of field operators to query.
        :return: Ourself.
        """
        for condition in conditions:
            self.conditions.append(condition)

        return self

    # sql methods
    def get_token(self) -> typing.Tuple[Select, dict]:
        """
        Gets the Select tokens for this query.
        """
        # get the fields
        fields = []
        for tbl_name, table in self.tables.items():
            for column in table.columns:
                fields.append(Column(
                    '"{}"."{}"'.format(table.name, column.name))
                )

        s = Select(
            subtokens=[
                # expand out the fields and tables as From queries
                *fields,
                *[From(name) for name in self.tables]
            ]
        )

        # update subfields with WHERE query
        where = Where()
        param_count = 0
        params = {}
        for op in self.conditions:
            o = op.get_token()  # type: Operator

            if isinstance(o.value, str):
                name = "param_{}".format(param_count)
                params[name] = o.value
                o.value = "{{{n}}}".format(n=name)
                param_count += 1

            where.subtokens.append(o)

        s.subtokens.append(where)

        return s, params

    # return methods

