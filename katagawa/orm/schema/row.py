import inspect
import typing

import types

from katagawa.exc import NoSuchColumnError
from katagawa.orm.schema import column as md_column
from katagawa.orm.schema import table as md_table
from katagawa.orm import session as md_session

NO_VALUE = object()


class TableRow(object):
    """
    Represents a single row in a table.  
    :class:`.Table` objects cannot be instantiated (not without hacking at the object level), so as
    such they return TableRow objects when called.
    
    TableRow objects are representative of a single row in said table - the column names are the 
    keys, and the value in that row are the items.
     
    .. code-block:: python
        class User(Table):
            id = Column(Integer, primary_key=True)
            
        user = User(id=1)  # user is actually a TableRow bound to the User table
    """

    def __init__(self, tbl: 'md_table.TableMeta'):
        """
        :param tbl: The table object to bind this row to.
        """
        #: The :class:`.Table` object to use for this table row.
        self.table = tbl

        #: If this row existed before.
        #: If this is True, this row was fetched from the DB previously.
        #: Otherwise, it is a fresh row.
        self.__existed = False

        #: The session this row is attached to.
        self._session = None  # type: md_session.Session

        #: A mapping of Column -> Previous values for this row.
        #: Used in update generation.
        self._previous_values = {}

        #: A mapping of Column -> Current value for this row.
        self._values = {}

    def __repr__(self):
        gen = ("{}={}".format(col.name, self.get_column_value(col)) for col in self.table.columns)
        return "<{} {}>".format(self.table.__name__, " ".join(gen))

    def __getattr__(self, item: str):
        obb = self._resolve_item(item)
        return obb

    def __setattr__(self, key, value):
        # ensure we're not doing stupid shit until we get _values
        try:
            object.__getattribute__(self, "_values")
        except AttributeError:
            return super().__setattr__(key, value)

        try:
            col = next(filter(lambda col: col.name == key, self.table.columns))
        except StopIteration:
            return super().__setattr__(key, value)

        # call on_set for the column
        return col.type.on_set(self, value)

    def _resolve_item(self, name: str):
        """
        Resolves an item on this TableRow.
        
        This will check:
        
            - Functions decorated with :func:`.row_attr`
            - Non-column :class:`.Table` members
            - Columns
        
        :param name: The name to resolve. 
        :return: The object returned, if applicable.
        """
        # try and getattr the name from the Table object
        try:
            item = getattr(self.table, name)
        except AttributeError:
            pass
        else:
            # proxy to the table
            # but don't proxy column accesses
            if not isinstance(item, md_column.Column):
                if hasattr(item, "__row_attr__"):
                    return item(self)

                if inspect.isfunction(item):
                    # bind it to ourselves, and return it
                    return types.MethodType(item, self)
                return item

        # failed to load item, so load a column value instead
        try:
            col = next(filter(lambda col: col.name == name, self.table.columns))
        except StopIteration:
            raise AttributeError("{} was not a function or attribute on the associated table, "
                                 "and was not a column".format(name)) from None

        return col.type.on_get(self)

    def get_column_value(self, column: 'md_column.Column', return_default: bool = True):
        """
        Gets the value from the specified column in this row.
        
        :param column: The column.
        :param return_default: If this should return the column default, or NO_VALUE.
        """
        if column.table != self.table:
            raise ValueError("Column table must match row table")

        try:
            return self._values[column]
        except KeyError:
            if return_default:
                default = column.default
                if default is md_column.NO_DEFAULT:
                    return NO_VALUE
            else:
                return NO_VALUE

    def store_column_value(self, column: 'md_column.Column', value: typing.Any):
        """
        Updates the value of a column in this row.
        
        This will also update the history of the value, if applicable.
        """
        if column not in self._previous_values:
            if column in self._values:
                self._previous_values[column] = self._values[column]

        self._values[column] = value

        return self

    def to_dict(self, *, include_attrs: bool = False) -> dict:
        """
        Converts this row to a dict, indexed by Column.
         
        :param include_attrs: Should this include row_attrs?
        """
        # todo: include row attrs
        d = {col: self.get_column_value(col) for col in self.table.columns}
        return d

    @property
    def primary_key(self) -> typing.Union[typing.Any, typing.Iterable[typing.Any]]:
        """
        Gets the primary key for this row.
          
        If this table only has one primary key column, this property will be a single value.  
        If this table has multiple columns in a primary key, this property will be a tuple. 
        """
        pk = self.table.primary_key  # type: md_table.PrimaryKey
        result = []

        for col in pk.columns:
            val = self.get_column_value(col)
            result.append(val)

        if len(result) == 1:
            return result[0]

        return tuple(result)
