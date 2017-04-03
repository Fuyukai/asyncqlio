"""
Represents tables.
"""
import typing
from types import MappingProxyType

from cached_property import cached_property

from katagawa.exceptions import OperationalException
from katagawa.orm import column as cl


class Table(object):
    """
    A table corresponds to an ORM table.
    """

    def __init__(self,
                 name: str,
                 *columns: 'cl.Column'):
        """
        :param name:
            The table name in the database this table corresponds to.

        :param columns:
            The columns that this table contains.
        """
        #: The name of the table.
        self.name = name

        #: The list of columns.
        self._columns = columns
        if not all(isinstance(i, cl.Column) for i in self._columns):
            raise TypeError("Columns arguments must be instances of Column")

        # register a field mapping
        self._column_mapping = {}

        for column in self._columns:
            self._column_mapping[column.name] = column
            column.register_table(self)

    @cached_property
    def column_mapping(self) -> 'typing.Mapping[str, cl.Column]':
        """
        :return: A read-only mapping of column_name -> column. 
        """
        return MappingProxyType(self._column_mapping)

    @cached_property
    def columns(self) -> 'typing.ValuesView[cl.Column]':
        """
        :return: A list of columns for this table.
        """
        return self.column_mapping.values()

    @cached_property
    def primary_key(self):
        """
        :return: The :class:`~.Column` that represents the primary key for this table. 
        """
        try:
            return next(filter(lambda column: column.primary_key is True, self.columns))
        except StopIteration:
            raise OperationalException("Table `{}` has no primary key".format(self.name))

    # magic method overrides
    def __getattr__(self, item):
        try:
            return self._column_mapping[item]
        except KeyError:
            raise AttributeError(item) from None

    def __call__(self, *args, **kwargs) -> 'TableRow':
        row = TableRow(self)
        # update values
        for name, val in kwargs.items():
            if name not in self._column_mapping:
                raise ValueError("{} is not a column in this table".format(name))

            row.update_value(name, val)

        return row


class TableRow(object):
    """
    Represents a row in a table. This is created when a table is called, or from query results. 
    """

    def __init__(self, table: 'Table'):
        """
        :param table: The :class:`~.Table` instance that this row is associated with. 
        """

        #: The :class:`~.Table` that this row is associated with.
        self._table = table

        # internal mappings

        #: A dict of Column: Previous value.
        #: Used when updating a column, it will store the previous value to know an update happened.
        self._previous_values = {}

        #: A dict of Column: Current value.
        #: Used when updating a column, it will store the current values.
        #: When created, this is initialized to the current values of all columns.
        self._values = {}

    # workhouse methods
    def get_value(self, col_name: str) -> typing.Any:
        """
        Gets the current value of the column by name ``col_name``.
        
        :param col_name: The name of the column to get.
        :return: The value of the column.
        """
        col = self._table.column_mapping[col_name]
        val = self._values.get(col.name)  # col.default)
        return val

    def update_value(self, col_name: str, new_val: typing.Any):
        """
        Updates or inserts the value into a result.
        
        :param col_name: The name of the column to update. 
        :param new_val: The new value of the data to insert or update.
        """
        if col_name not in self._previous_values and col_name in self._values:
            self._previous_values[col_name] = self._values[col_name]

        self._values[col_name] = new_val

    def __repr__(self):
        fmt = " "
        for col in self._table.columns:
            val = self.get_value(col.name)
            fmt += "{}='{}'".format(col.name, val)

        return "<{.name}{}>".format(self._table, fmt)

    def __iter__(self):
        """
        Alias for ``iter(row._values)``. 
        """
        return iter(self._values)

    def __getattr__(self, item):
        # called when all else failed
        # this means we need to return the column value
        try:
            return self.get_value(item)
        except KeyError as e:
            raise AttributeError(item) from e

    def __setattr__(self, key: str, value: typing.Any):
        if key.startswith("_"):
            super().__setattr__(key, value)

        if key in self._table.column_mapping:
            self.update_value(key, new_val=value)
        else:
            super().__setattr__(key, value)
