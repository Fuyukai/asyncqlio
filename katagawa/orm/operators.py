"""
Classes for operators returned from queries.
"""
import abc
import functools
import itertools
import typing

from katagawa.orm.schema import column as md_column


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

    def get_param(self, emitter, counter: itertools.count):
        name = "param_{}".format(next(counter))
        return emitter(name), name

    @abc.abstractmethod
    def generate_sql(self, emitter: typing.Callable[[str], str], counter: itertools.count) \
            -> typing.Tuple[str, str, typing.Any]:
        """
        Generates the SQL for an operator.
        
        Parameters must be generated using the emitter callable.
        
        :param emitter: A callable that can be used to generate param placeholders in a query.
        :param counter: The current "parameter number".
        :return: A str representing the SQL, a str representing the param name, \
            and an any representing the param.
            
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
            sql, name, val = op.generate_sql(emitter, counter)
            final.append(sql)
            if name is not None and val is not None:
                vals[name] = val

        fmt = "({})".format(" AND ".join(final))
        return fmt, None, vals


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
            sql, name, val = op.generate_sql(emitter, counter)
            final.append(sql)
            if name is not None and val is not None:
                vals[name] = val

        fmt = "({})".format(" OR ".join(final))
        return fmt, None, vals


class Sorter(BaseOperator):
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
        return "{} {}".format(', '.join(col.alias_name(quoted=True) for col in self.cols),
                              self.sort_order)


class AscSorter(Sorter):
    sort_order = "ASC"


class DescSorter(Sorter):
    sort_order = "DESC"


class ColumnValueMixin(object):
    """
    A mixin that specifies that an operator takes both a Column and a Value as arguments.
    
    .. code-block:: python
        class MyOp(BaseOperator, ColumnValueMixin):
            ...
            
        # myop is constructed MyOp(col, value)
    """

    def __init__(self, column: 'md_column.Column', value: typing.Any):
        self.column = column
        self.value = value


class In(BaseOperator, ColumnValueMixin):
    def generate_sql(self, emitter: typing.Callable[[str], str], counter: itertools.count):
        # generate a dict of params
        params = {}
        l = []
        for item in self.value:
            emitted, name = self.get_param(emitter, counter)
            params[name] = item
            l.append(emitted)

        return "{} IN ({})".format(self.column.quoted_fullname, ", ".join(l)), \
               None, params


class ComparisonOp(ColumnValueMixin, BaseOperator):
    """
    A helper class that implements easy generation of comparison-based operators.
    
    To customize the operator provided, set the value of ``operator`` in the class body.
    """
    operator = None

    def generate_sql(self, emitter: typing.Callable[[str], str], counter: itertools.count):
        if isinstance(self.value, md_column.Column):
            # special-case columns
            return "{} {} {}".format(self.column.quoted_fullname, self.operator,
                                     self.value.quoted_fullname), None, None

        param_name, name = self.get_param(emitter, counter)
        return "{} {} {}".format(self.column.quoted_fullname, self.operator,
                                 param_name), \
               name, self.value


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
        This operator is not available on all dialects.
    """
    operator = "ILIKE"
