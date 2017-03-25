"""
Represents tables.
"""
import typing
from types import MappingProxyType

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
        self._field_mapping = {}

        for column in self._columns:
            self._field_mapping[column.name] = column
            column.register_table(self)

    @property
    def column_mapping(self) -> 'typing.Mapping[str, cl.Column]':
        """
        :return: A read-only mapping of column_name -> column. 
        """
        return MappingProxyType(self._field_mapping)

    @property
    def columns(self) -> 'typing.ValuesView[cl.Column]':
        """
        :return: A list of columns for this table.
        """
        return self.column_mapping.values()

    # magic method overrides
    def __getattr__(self, item):
        try:
            return self._field_mapping[item]
        except KeyError:
            raise AttributeError(item) from None


class TableRow(object):
    """
    
    """
