import abc
import typing


class ColumnType(abc.ABC):
    """
    Represents the type of a column.
    """
    @abc.abstractmethod
    def sql(self) -> str:
        """
        :return: The str SQL name of this type. 
        """

    @abc.abstractmethod
    def cast(self, value: typing.Any) -> str:
        """
        Casts a Python value to a SQL value in a string.
        
        :param value: The value to cast. 
        :return: A string representing the value. 
        """

    @abc.abstractmethod
    def reverse_cast(self, sql: str) -> typing.Any:
        """
        Casts the SQL value to a Python value.
        
        :param sql: The SQL value. 
        :return: The Python value created.
        """


class String(ColumnType):
    """
    Represents a VARCHAR() type.
    """

    def __init__(self, size: int = -1):
        #: The max size of this String.
        self.size = size

    def sql(self):
        # return max if theres no size
        # since we want to create an unbounded varchar
        if self.size >= 0:
            return "VARCHAR({})".format(self.size)
        else:
            return "VARCHAR(MAX)"

    def cast(self, value: typing.Any) -> str:
        return str(value)

    def reverse_cast(self, sql: str) -> str:
        return sql


class Text(ColumnType):
    """
    Represents a TEXT type.
    
    .. note::
        This is preferable to the String type in some databases.
        
    .. warning::
        This is deprecated in MSSQL.
    """

    def sql(self):
        return "TEXT"

    def cast(self, value: typing.Any):
        return str(value)

    def reverse_cast(self, sql: str):
        return sql


class Integer(ColumnType):
    """
    Represents an INTEGER type.
    
    .. warning::
        This represents a 32-bit integer (2**31-1 to -2**32)
    """
    def sql(self):
        return "INTEGER"

    def check_cap(self, value: int) -> bool:
        """
        Checks if this int is in range for the type.
        """
        return -2147483648 < value < 2147483647

    def cast(self, value: typing.Any) -> str:
        if not self.check_cap(value):
            raise ValueError("Value `{}` out of range for type `{.__class__.__name__}`")

        return str(value)

    def reverse_cast(self, sql: str):
        return int(sql)


class SmallInt(Integer):
    """
    Represents a SMALLINT type.
    """
    def sql(self):
        return "SMALLINT"

    def check_cap(self, value: int):
        return -32767 < value < 32767


class BigInt(Integer):
    """
    Represents a BIGINT type.
    """
    def sql(self):
        return "BIGINT"

    def check_cap(self, value: int):
        return -9223372036854775808 < value < 9223372036854775807
