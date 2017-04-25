"""
Classes for operators returned from queries.
"""
import abc
import functools
import typing

import itertools

from katagawa.orm import schema as md_schema


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
        # todo: return Or operator
        return None

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


class ColumnValueMixin(object):
    """
    A mixin that specifies that an operator takes both a Column and a Value as arguments.
    
    .. code-block:: python
        class MyOp(BaseOperator, ColumnValueMixin):
            ...
            
        # myop is constructed MyOp(col, value)
    """

    def __init__(self, column: 'md_schema.Column', value: typing.Any):
        self.column = column
        self.value = value


class Eq(ColumnValueMixin, BaseOperator):
    """
    Represents an equality operator.
    """

    def generate_sql(self, emitter: typing.Callable[[str], str], counter: itertools.count):
        param_name, name = self.get_param(emitter, counter)
        return "{} = {}".format(self.column.quoted_name, param_name), \
               name, self.value
