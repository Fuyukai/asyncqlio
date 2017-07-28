import abc
import datetime
import typing

from asyncqlio.exc import DatabaseException
from asyncqlio.orm import operators as md_operators
from asyncqlio.orm.schema import column as md_column, table as md_table


class ColumnValidationError(DatabaseException):
    """
    Raised when a column fails validation.
    """


class ColumnType(abc.ABC):
    """
    Implements some underlying mechanisms for a :class:`.Column`.

    The only method that is required to be implemented on children is :meth:`.ColumnType.sql` -
    which is used in CREATE TABLE declarations, etc. :meth:`.ColumnType.on_set`,
    :meth:`.ColumnType.on_get` and so on are not required to be implemented - the defaults will
    work fine.

    The ColumnType is responsible for actually loading the data from the row's internal storage
    and to the user code.

    .. code-block:: python3

        # we hate fun
        def on_get(self, row, column):
            return "lol"

        ...

        # row is a random row object
        # load the `fun` column which has this weird type
        value = row.fun
        print(value)  # "lol", regardless of what was stored in the database.

    Accordingly, it is also responsible for storing the data into the row's internal storage.

    .. code-block:: python3

        def on_set(*args, **kwargs):
            return None

        row.not_fun = 1
        print(row.not_fun)  # None - no value was stored in the row

    To actually insert a value into the row's storage table, use :meth:`.ColumnType.store_value`.
    Correspondingly, loading a value from the row's storage table can be achieved with
    :meth:`.ColumnType.load_value`. These functions should be used, as they are guarenteed to work
    across all versions.

    Columns will proxy bad attribute accesses from the Column object to this type object - meaning
    types can implement custom operators, if applicable.

    .. code-block:: python3

        class User(Table):
            id = Column(MyWeirdType())

        ...

        # MyWeirdType implements `.contains`
        # the contains call is proxied to (MyWeirdType instance).contains("heck")
        q = await sess.select(User).where(User.id.contains("heck")).first()

    """
    __slots__ = ("column",)

    def __init__(self):
        #: The column this type object is associated with.
        self.column = None  # type: md_column.Column

    @abc.abstractmethod
    def sql(self) -> str:
        """
        :return: The str SQL name of this type.
        """

    def validate_set(self, row: 'md_table.Table',
                     value: typing.Any) -> bool:
        """
        Validates that the item being set is valid.
        This is called by the default ``on_set``.

        :param row: The row being set.
        :param value: The value to set.
        :return: A bool indicating if this is valid or not.
        """
        return True

    def store_value(self, row: 'md_table.Table',
                    value: typing.Any):
        """
        Stores a value in the row's storage table.

        This is for internal usage only.

        :param row: The row to store in.
        :param value: The value to store in the row.
        """
        row.store_column_value(self.column, value)

    def on_set(self, row: 'md_table.Table', value: typing.Any) -> typing.Any:
        """
        Called when a value is a set on this column.

        This is the default method - it will call :meth:`.ColumnType.validate_set` to validate the
        type before storing it. This is useful for simple column types.

        :param row: The row this value is being set on.
        :param value: The value being set.
        """
        if value is not None:
            valid = self.validate_set(row, value)
            if not valid:
                raise ColumnValidationError("Value {} failed to validate in type {}"
                                            .format(value, type(self).__name__))

        self.store_value(row, value)

    def on_get(self, row: 'md_table.Table') -> typing.Any:
        """
        Called when a value is retrieved from this column.

        :param row: The row that is being retrieved.
        :return: The value of the row's internal storage.
        """
        return row.get_column_value(self.column)

    @classmethod
    def create_default(cls) -> 'ColumnType':
        """
        Creates the default object for this table in the event that a type is passed to a column,
        instead of an instance.
        """
        return cls()

    # Some methods for base types
    def in_(self, *args) -> 'md_operators.In':
        """
        Returns an IN operator, checking if a value in this column is in a tuple of items.

        :param args: The items to check.
        """
        if len(args) <= 0:
            raise ValueError("Must provide at least one argument to in_")

        return md_operators.In(self.column, args)


class String(ColumnType):
    """
    Represents a VARCHAR() type.
    """

    def __init__(self, size: int = -1):
        super().__init__()
        #: The max size of this String.
        self.size = size

    def sql(self):
        # return max if theres no size
        # since we want to create an unbounded varchar
        if self.size >= 0:
            return "VARCHAR({})".format(self.size)
        else:
            return "VARCHAR"

    def validate_set(self, row, value: typing.Any):
        if self.size < 0:
            return True

        if len(value) > self.size:
            raise ColumnValidationError("Value {} is more than {} chars long".format(value,
                                                                                     self.size))

        return True

    def like(self, other: str) -> 'md_operators.Like':
        """
        Returns a LIKE operator, checking if this column is LIKE another string.

        :param other: The other string to check.
        """
        return md_operators.Like(self.column, other)

    def ilike(self, other: str) -> 'typing.Union[md_operators.ILike, md_operators.HackyILike]':
        """
        Returns an ILIKE operator, checking if this column is case-insensitive LIKE another string.

        .. warning::
            This is not supported in all DB backends.

        :param other: The other string to check.
        """
        if self.column.table._bind.dialect.has_ilike:
            return md_operators.ILike(self.column, other)
        else:
            return md_operators.HackyILike(self.column, other)


class Text(String):
    """
    Represents a TEXT type.
    TEXT type columns are very similar to String type objects, except that they have no size limit.

    .. note::
        This is preferable to the String type in some databases.

    .. warning::
        This is deprecated in MSSQL.
    """

    def __init__(self):
        # unlimited size
        super().__init__(size=-1)

    def sql(self):
        return "TEXT"


class Boolean(ColumnType):
    """
    Represents a BOOL type.
    """

    def sql(self):
        return "BOOLEAN"

    def validate_set(self, row: 'md_table.Table', value: typing.Any):
        return value in [True, False]


class Integer(ColumnType):
    """
    Represents an INTEGER type.

    .. warning::
        This represents a 32-bit integer (2**31-1 to -2**32)
    """

    def sql(self):
        return "INTEGER"

    def validate_set(self, row, value: typing.Any):
        """
        Checks if this int is in range for the type.
        """
        return -2147483648 < value < 2147483647

    def on_set(self, row, value: typing.Any):
        if not isinstance(value, int):
            raise ColumnValidationError("Value {} is not an int".format(value))

        return super().on_set(row, value)


class SmallInt(Integer):
    """
    Represents a SMALLINT type.
    """

    def sql(self):
        return "SMALLINT"

    def validate_set(self, row, value: typing.Any):
        return -32768 < value < 32767


class BigInt(Integer):
    """
    Represents a BIGINT type.
    """

    def sql(self):
        return "BIGINT"

    def validate_set(self, row, value):
        return -9223372036854775808 < value < 9223372036854775807


class Timestamp(ColumnType):
    """
    Represents a TIMESTAMP type.
    """

    def sql(self):
        return "TIMESTAMP"

    def validate_set(self, row, value):
        return isinstance(value, datetime.datetime)
