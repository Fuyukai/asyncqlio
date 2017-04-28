import typing

from katagawa.orm.schema import Column


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

    def __init__(self, tbl):
        """
        :param tbl: The table object to bind this row to.
        """
        self._table = tbl

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

        # BECAUSE PYTHON
        self.__setattr__ = self._setattr__

    def __repr__(self):
        gen = ("{}={}".format(col.name, self._get_column_value(col)) for col in self._table.columns)
        return "<{} {}>".format(self._table.__name__, " ".join(gen))

    def __getattr__(self, item):
        col = next(filter(lambda col: col.name == item, self._table.columns), None)
        if col is None:
            raise NoSuchColumnError(item)

        return self._values[col]

    def _setattr__(self, key, value):
        col = next(filter(lambda col: col.name == key, self._table.columns), None)
        if col is None:
            return super().__setattr__(key, value)

        return self.update_column(col, value)

    def _get_column_value(self, column: 'Column'):
        """
        Gets the value from the specified column in this row.
        """
        if column.table != self._table:
            raise ValueError("Column table must match row table")

        try:
            return self._values[column]
        except KeyError:
            return column.default

    def update_column(self, column: 'Column', value: typing.Any):
        """
        Updates the value of a column in this row.
        """
        if column not in self._previous_values:
            if column in self._values:
                self._previous_values[column] = self._values[column]

        self._values[column] = value

        return self

    @property
    def primary_key(self) -> typing.Union[typing.Any, typing.Iterable[typing.Any]]:
        """
        Gets the primary key for this row.
          
        If this table only has one primary key column, this property will be a single value.  
        If this table has multiple columns in a primary key, this property will be a tuple. 
        """
        pk = self._table.primary_key  # type: PrimaryKey
        result = []

        for col in pk.columns:
            val = self._get_column_value(col)
            result.append(val)

        if len(result) == 1:
            return result[0]

        return tuple(result)
