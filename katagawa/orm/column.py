import typing

from katagawa.sql import Token
from katagawa.sql.dialects.common import Eq, Gt, Lt, Ne, IsNotNull, IsNull, Column as sql_Column, \
    Operator, And, Or
from katagawa.sql.types import BaseType
from katagawa.orm import table

d = {
    0: Eq,
    1: Gt,
    2: Lt,
    3: Ne,
    4: IsNotNull,
    5: IsNull
}


class _CombinatorOperator(object):
    """
    Represents a combinator operator, such as `AND` or `OR`.
    """

    def __init__(self, type_: typing.Type[Token], *terms: '_Operator'):
        """
        :param token: The token type of this combinator.
        :param terms: The operator terms for this statement.
        """
        self._type = type_
        self.operators = list(terms)

    def get_token(self) -> Token:
        """
        :return: The :class:`~.And` token for this column. 
        """
        token = self._type(
            subtokens=[_operator.get_token() for _operator in self.operators]
        )
        return token

    def __and__(self, other):
        # used to chain ands
        if self._type != And:
            return NotImplemented

        self.operators.append(other)
        return self

    def __or__(self, other):
        if self._type != Or:
            return NotImplemented

        self.operators.append(other)
        return self


class _Operator(object):
    """
    Represents something returned from an equality comparison on some columns.
    """

    def __init__(self, operator: int, column: 'Column', other: str):
        """
        :param operator: The ID of the operator to use for this equality comparison. 
        :param column: The name of the column being compared.
        :param other: The value comparing against. Could be an escape.
        """
        self.operator = operator
        self.column = column
        self.other = other

        # prevent slow global lookup
        self._d = d

    def get_token(self) -> Operator:
        """
        Gets the :class:`~.tokens.Operator` that represents this operator. 
        """
        op = self._d[self.operator]
        ident = '"{}"."{}"'.format(self.column.table.name, self.column.name)
        col = sql_Column(identifier=ident)

        # create the operator
        return op(col, self.other)

    def __call__(self, *args, **kwargs):
        # Return the appropriate SQL operator token.
        return self.get_token()

    # magic methods
    def __and__(self, other):
        return _CombinatorOperator(And, self, other)

    def __or__(self, other):
        return _CombinatorOperator(Or, self, other)


class Column(object):
    """
    A column is a class that represents a column in a table in a database.

    A table is comprised of multiple columns.
    """

    def __init__(self,
                 name: str,
                 type_: BaseType,
                 *,
                 primary_key: bool = False,
                 autoincrement: bool = False):
        """
        :param name:
            The name of this column. Is used to create the column in the database.

        :param type_:
            The type of items this column accepts.
            This should be an instance of a class that subclasses BaseType.

        :param primary_key:
            Is this column a primary key?

        :param autoincrement:
            Should this column autoincrement?
        """
        self.name = name

        self.type_: BaseType = type_
        if not isinstance(self.type_, BaseType):
            # Try and instantiate it.
            if not issubclass(self.type_, BaseType):
                raise TypeError("type_ should be an instance or subclass of BaseType")
            else:
                self.type_ = type_()

        self.primary_key = primary_key
        self.autoincrement = autoincrement

        # The table this is registered to.
        self.table = None  # type: table.Table

    def register_table(self, tbl: table.Table):
        self.table = tbl

    # properties
    @property
    def sql_field(self) -> sql_Column:
        """
        :return: This column as a :class:`~.Field`. 
        """
        return sql_Column(identifier=self.name)

    # operator methods
    def __eq__(self, other):
        return _Operator(0, self, other)
