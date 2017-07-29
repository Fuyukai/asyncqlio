"""
Classes for operators returned from queries.
"""
import abc
import functools
import itertools
import typing

from asyncqlio.orm.schema import column as md_column


class OperatorResponse:
    """
    A storage class for the generated SQL from an operator.
    """
    __slots__ = ("sql", "parameters")

    def __init__(self, sql: str, parameters: dict):
        """
        :param sql: The generated SQL for this operator.
        :param parameters: A dict of parameters to use for this response.
        """
        self.sql = sql
        self.parameters = parameters
        if self.parameters is None:
            self.parameters = {}


def requires_bop(func) -> 'typing.Callable[[BaseOperator, BaseOperator], typing.Any]':
    """
    A decorator that marks a magic method as requiring another BaseOperator.

    :param func: The function to decorate.
    :return: A function that returns NotImplemented when the class required isn't specified.
    """

    @functools.wraps(func)
    def inner(self, other: 'BaseOperator'):
        if not isinstance(other, BaseOperator):
            return NotImplemented

        return func(self, other)

    return inner


class BaseOperator(abc.ABC):
    """
    The base operator class.
    """

    def get_param(self, emitter: typing.Callable[[str], str], counter: itertools.count) \
            -> typing.Tuple[str, str]:
        """
        Gets the next parameter.

        :param emitter: A function that emits a parameter name that can be formatted in a SQL query.
        :param counter: The counter for parameters.
        """
        name = "param_{}".format(next(counter))
        return emitter(name), name

    @abc.abstractmethod
    def generate_sql(self, emitter: typing.Callable[[str], str], counter: itertools.count) \
            -> OperatorResponse:
        """
        Generates the SQL for an operator.

        Parameters must be generated using the emitter callable.

        :param emitter: A callable that can be used to generate param placeholders in a query.
        :param counter: The current "parameter number".
        :return: A :class:`.OperatorResponse` representing the result.

        .. warning::
            The param name and the param can be empty if none is to be returned.
        """

    @requires_bop
    def __and__(self, other: 'BaseOperator'):
        if isinstance(self, And):
            self.operators.append(other)
            return self
        elif isinstance(other, And):
            other.operators.append(self)
            return other
        else:
            return And(self, other)

    @requires_bop
    def __or__(self, other: 'BaseOperator'):
        if isinstance(self, Or):
            self.operators.append(other)
            return self
        elif isinstance(other, Or):
            other.operators.append(self)
            return other
        else:
            return Or(self, other)

    # copies that signify bitwise operators too
    __rand__ = __and__
    __ror__ = __or__


class And(BaseOperator):
    """
    Represents an AND operator in a query.

    This will join multiple other :class:`.BaseOperator` objects together.
    """

    def __init__(self, *ops: 'BaseOperator'):
        self.operators = list(ops)

    def generate_sql(self, emitter: typing.Callable[[str], str], counter: itertools.count):
        final = []
        vals = {}
        for op in self.operators:
            response = op.generate_sql(emitter, counter)
            final.append(response.sql)
            vals.update(response.parameters)

        fmt = "({})".format(" AND ".join(final))
        res = OperatorResponse(fmt, vals)
        return res


class Or(BaseOperator):
    """
    Represents an OR operator in a query.

    This will join multiple other :class:`.BaseOperator` objects together.
    """

    def __init__(self, *ops: 'BaseOperator'):
        self.operators = list(ops)

    def generate_sql(self, emitter: typing.Callable[[str], str], counter: itertools.count):
        final = []
        vals = {}
        for op in self.operators:
            response = op.generate_sql(emitter, counter)
            final.append(response.sql)
            vals.update(response.parameters)

        fmt = "({})".format(" OR ".join(final))
        return OperatorResponse(fmt, vals)


class Sorter(BaseOperator, metaclass=abc.ABCMeta):
    """
    A generic sorter operator, for use in ORDER BY.
    """

    def __init__(self, *columns: 'md_column.Column'):
        self.cols = columns

    @property
    @abc.abstractmethod
    def sort_order(self):
        """
        The sort order for this row; ASC or DESC.
        """
        pass

    def generate_sql(self, emitter: typing.Callable[[str], str], counter: itertools.count):
        names = ", ".join(col.alias_name(quoted=True) for col in self.cols)
        sql = "{} {}".format(names, self.sort_order)

        return OperatorResponse(sql, {})


class AscSorter(Sorter):
    sort_order = "ASC"


class DescSorter(Sorter):
    sort_order = "DESC"


class ColumnValueMixin(object):
    """
    A mixin that specifies that an operator takes both a Column and a Value as arguments.

    .. code-block:: python3

        class MyOp(BaseOperator, ColumnValueMixin):
            ...

        # myop is constructed MyOp(col, value)
    """

    def __init__(self, column: 'md_column.Column', value: typing.Any):
        self.column = column
        self.value = value


class BasicSetter(BaseOperator, ColumnValueMixin, metaclass=abc.ABCMeta):
    """
    Represents a basic setting operation. Used for bulk queries.
    """

    @property
    @abc.abstractmethod
    def set_operator(self) -> str:
        """
        :return: The "setting" operator to use when generating the SQL.
        """
        pass

    def generate_sql(self, emitter: typing.Callable[[str], str], counter: itertools.count):
        param_name, name = self.get_param(emitter, counter)
        params = {name: self.value}

        sql = "{0} = {0} {1} {2}".format(self.column.quoted_name, self.set_operator, param_name)
        return OperatorResponse(sql, params)


class ValueSetter(BasicSetter):
    """
    Represents a value setter (``col = 1``).
    """
    set_operator = "="

    # override as the default setter impl doesn't work
    def generate_sql(self, emitter: typing.Callable[[str], str], counter: itertools.count):
        param_name, name = self.get_param(emitter, counter)
        params = {name: self.value}

        sql = "{0} = {1}".format(self.column.quoted_name, param_name)
        return OperatorResponse(sql, params)


class IncrementSetter(BasicSetter):
    """
    Represents an increment setter. (``col = col + 1``)
    """
    set_operator = "+"


class DecrementSetter(BasicSetter):
    """
    Represents a decrement setter.
    """
    set_operator = "-"


class In(BaseOperator, ColumnValueMixin):
    def generate_sql(self, emitter: typing.Callable[[str], str], counter: itertools.count):
        # generate a dict of params
        params = {}
        l = []
        for item in self.value:
            emitted, name = self.get_param(emitter, counter)
            params[name] = item
            l.append(emitted)

        sql = "{} IN ({})".format(self.column.quoted_fullname, ", ".join(l))
        return OperatorResponse(sql, params)


class ComparisonOp(ColumnValueMixin, BaseOperator):
    """
    A helper class that implements easy generation of comparison-based operators.

    To customize the operator provided, set the value of ``operator`` in the class body.
    """
    operator = None

    def generate_sql(self, emitter: typing.Callable[[str], str], counter: itertools.count):
        params = {}
        if isinstance(self.value, md_column.Column):
            sql = "{} {} {}".format(self.column.quoted_fullname, self.operator,
                                    self.value.quoted_fullname)
        else:
            param_name, name = self.get_param(emitter, counter)
            sql = "{} {} {}".format(self.column.quoted_fullname, self.operator, param_name)
            params[name] = self.value

        res = OperatorResponse(sql, params)
        return res


class Eq(ComparisonOp):
    """
    Represents an equality operator.
    """
    operator = "="


class NEq(ComparisonOp):
    """
    Represents a non-equality operator.
    """
    operator = "!="


class Lt(ComparisonOp):
    """
    Represents a less than operator.
    """
    operator = "<"


class Gt(ComparisonOp):
    """
    Represents a more than operator.
    """
    operator = ">"


class Lte(ComparisonOp):
    """
    Represents a less than or equals to operator.
    """
    operator = "<="


class Gte(ComparisonOp):
    """
    Represents a more than or equals to operator.
    """
    operator = ">="


class Like(ComparisonOp):
    """
    Represents a LIKE operator.
    """
    operator = "LIKE"


class ILike(ComparisonOp):
    """
    Represents an ILIKE operator.

    .. warning::
        This operator is not natively supported on all dialects. If used on a dialect that
        doesn't support it, it will fallback to a lowercase LIKE.
    """
    operator = "ILIKE"


class HackyILike(BaseOperator, ColumnValueMixin):
    """
    A "hacky" ILIKE operator for databases that do not support it.
    """

    def generate_sql(self, emitter: typing.Callable[[str], str], counter: itertools.count):
        # lower(column) like (pattern|column)
        # this will lower the column
        params = {}

        # special-case columns again
        if isinstance(self.value, md_column.Column):
            param_name = "LOWER({})".format(self.value.quoted_fullname)
        else:
            param_name, name = self.get_param(emitter, counter)
            params[name] = self.value

        sql = "LOWER({}) LIKE {}".format(self.column.quoted_fullname, param_name)
        res = OperatorResponse(sql, params)
        return res
