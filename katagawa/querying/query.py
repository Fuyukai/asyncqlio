"""
Module containing the actual query
"""
import logging
import typing

from katagawa.engine.base import ResultSet
from katagawa.engine.transaction import Transaction
from katagawa.exceptions import TableConflictException
from katagawa.orm.column import _Operator, _CombinatorOperator
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
        params = {}
        if self.conditions:
            where = Where()
            param_count = 0
            for op in self.conditions:
                if isinstance(op, _Operator):
                    ops = [op]
                elif isinstance(op, _CombinatorOperator):
                    ops = op.operators

                final = op.get_token()

                for nop in ops:
                    o = nop.get_token()

                    # update parameterized queries
                    if isinstance(o.value, str):
                        name = "param_{}".format(param_count)
                        params[name] = o.value
                        o.value = "{{{n}}}".format(n=name)
                        param_count += 1

                where.subtokens.append(final)

            s.subtokens.append(where)

        return s, params

    # return methods

    async def all(self) -> ResultSet:
        """
        Returns a :class:`.ResultSet` iterator for the specified query.
        """
        r = await self.session.execute(self)

        return r
