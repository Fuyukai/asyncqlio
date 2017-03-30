"""
Module containing the actual query
"""
import logging
import typing

import async_generator

from katagawa.engine.base import ResultSet
from katagawa.engine.transaction import Transaction
from katagawa.exceptions import TableConflictException
from katagawa.orm.column import _Operator, _CombinatorOperator
from katagawa.orm.table import Table, TableRow
from katagawa import session as md_sess
from katagawa.sql.dialects.common import Select, Column, From, Where, Operator

logger = logging.getLogger("Katagawa.query")


class BaseQuery(object):
    """
    A BaseQuery object is used to query the database with SELECT statements or otherwise.

    It is produced from a :meth:`.Session.query` and is used to actually query the database.
    """
    def __init__(self, session: 'md_sess.Session', table: 'Table', **kwargs):
        """
        Creates a new BaseQuery.

        :param session: The session to bind this query to.
        """
        self.session = session  # type: md_sess.Session

        #: The table being queried.
        self.from_ = table

        #: A dict of tables being joined, or similar.
        self.tables = {}

        #: A list of conditions being used.
        self.conditions = []

        # internal alias mapping
        # this is used once the query returns to get the right column from the response
        self._alias_mapping = {}  # type: typing.Mapping[str, Column]

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

        # add the main table's fields
        for column in self.from_.columns:
            fields.append(
                Column('"{}"."{}"'.format(self.from_.name, column.name))
            )

        # add any joined tables fields
        for tbl_name, table in self.tables.items():
            for column in table.columns:
                fields.append(Column(
                    '"{}"."{}"'.format(table.name, column.name))
                )

        s = Select(
            subtokens=[
                # expand out the fields and tables as From queries
                *fields,
                From(self.from_.name)
            ]
        )

        # update subfields with WHERE query
        params = {}
        if self.conditions:
            where = Where()
            param_count = 0
            for op in self.conditions:
                # extract the condition subtokens from the operators
                if isinstance(op, _Operator):
                    ops = [op]
                elif isinstance(op, _CombinatorOperator):
                    ops = op.operators

                # unfuck the final tokens to prevent sql injection
                final = op.get_token()

                for nop in ops:
                    o = nop.get_token()

                    # update parameterized queries
                    if isinstance(o.value, str):
                        name = "param_{}".format(param_count)
                        params[name] = o.value
                        o.value = self.session.engine.emit_param(name)
                        param_count += 1

                where.subtokens.append(final)

            s.subtokens.append(where)

        return s, params

    # return methods
    def _convert_result(self, result: typing.Mapping[str, typing.Any]) -> TableRow:
        """
        Converts a result from the raw DB connection into a :class:`.TableRow`.
        
        :param result: The result to convert from. 
            This should be a dictionary-like object, with keys and values methods that can be used 
            to get the columns and values from the result.
        
        :return: A new :class:`.TableRow` representing the result.
        """
        row = TableRow(self.from_)
        for key in result.keys():
            if key in self._alias_mapping:
                col = self._alias_mapping[key]
            else:
                # try and find the column from the main table
                col = self.from_.column_mapping[key]

            # update the current values
            row._values[col.name] = col.type_.cast(result[key])

        return row

    async def all(self) -> typing.Generator[typing.Mapping, None, None]:
        """
        Gets
        """
        r = await self.session.execute(self)

        async for result in r:
            result = self._convert_result(result)
            await async_generator.yield_(result)

    async def first(self):
        """
        Returns the first result that matches.
        """
        rset = await self.session.execute(self)
        next = await rset.get_next()

        return next
