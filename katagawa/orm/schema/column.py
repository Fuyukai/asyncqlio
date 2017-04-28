import logging
import sys
import typing

from cached_property import cached_property

from katagawa.orm import operators as md_operators
from katagawa.orm.schema.types import ColumnType

PY36 = sys.version_info[0:2] >= (3, 6)
logger = logging.getLogger(__name__)


class Column(object):
    """
    Represents a column in a table in a database.

    .. code-block:: python
        class MyTable(Table):
            id = Column(Integer, primary_key=True)
            
    The ``id`` column will mirror the ID of records in the table when fetching, etc. and can be set 
    on a record when storing in a table.
    
    .. code-block:: python
        sess = db.get_session()
        user = await sess.select(User).where(User.id == 2).first()
        
        print(user.id)  # 2

    """

    def __init__(self, type_: typing.Union[ColumnType, typing.Type[ColumnType]], *,
                 primary_key: bool = False,
                 nullable: bool = True,
                 default: typing.Any = None,
                 autoincrement: bool = False,
                 index: bool = True,
                 unique: bool = True):
        """
        :param type_:
            The :class:`.ColumnType` that represents the type of this column.
         
        :param primary_key: 
            Is this column the table's Primary Key (the unique identifier that identifies each row)?
            
        :param nullable: 
            Can this column be NULL?
            
        :param default:
            The client-side default for this column. If no value is provided when inserting, this 
            value will automatically be added to the insert query.
         
        :param autoincrement: 
            Should this column auto-increment? This will create a serial sequence.
        
        :param index: 
            Should this column be indexed?
        
        :param unique: 
            Is this column unique?
        """
        #: The name of the column.
        #: This can be manually set, or automatically set when set on a table.
        self.name = None  # type: str

        #: The :class:`.Table` instance this Column is associated with.
        self.table = None

        #: The :class:`.ColumnType` that represents the type of this column.
        self.type = type_

        #: The default for this column.
        self.default = default

        #: If this Column is a primary key.
        self.primary_key = primary_key

        #: If this Column is nullable.
        self.nullable = nullable

        #: If this Column is to autoincrement.
        self.autoincrement = autoincrement

        #: If this Column is indexed.
        self.indexed = index

        #: If this Column is unique.
        self.unique = unique

    def __hash__(self):
        return super().__hash__()

    def __set_name__(self, owner, name):
        """
        Called to update the table and the name of this Column.
        
        :param owner: The :class:`.Table` this Column is on. 
        :param name: The str name of this table.
        """
        logger.debug("Column created with name {} on {}".format(name, owner))
        self.name = name
        self.table = owner

    def __eq__(self, other: typing.Any) -> 'md_operators.Eq':
        return md_operators.Eq(self, other)

    def __ne__(self, other) -> 'md_operators.NEq':
        return md_operators.NEq(self, other)

    def __lt__(self, other) -> 'md_operators.Lt':
        return md_operators.Lt(self, other)

    def __gt__(self, other) -> 'md_operators.Gt':
        return md_operators.Gt(self, other)

    def __le__(self, other) -> 'md_operators.Lte':
        return md_operators.Lte(self, other)

    def __ge__(self, other) -> 'md_operators.Gte':
        return md_operators.Gte(self, other)

    def like(self, other: str) -> 'md_operators.Like':
        """
        Returns a LIKE operator, checking if this string is LIKE another string.
        
        :param other: The other string to check. 
        """
        return md_operators.Like(self, other)

    def ilike(self, other: str) -> 'md_operators.ILike':
        """
        Returns an ILIKE operator, checking if this string is case-insensitive LIKE another string.
        
        .. warning::
            This is not supported in all DB backends.
        
        :param other: The other string to check. 
        """
        # TODO: Check for ILIKE support.
        return md_operators.ILike(self, other)

    @cached_property
    def quoted_name(self) -> str:
        """
        Gets the quoted name for this column.
         
        This returns the column name in "column" format.
        """
        return r'"{}"'.format(self.name)

    @cached_property
    def quoted_fullname(self) -> str:
        """
        Gets the full quoted name for this column.
         
        This returns the column name in "table"."column" format.
        """
        return r'"{}"."{}"'.format(self.table.__tablename__, self.name)

    def alias_name(self, table=None, quoted: bool = False) -> str:
        """
        Gets the alias name for a column, given the table.
        
        This is in the format of `t_<table name>_<column_name>`.
         
        :param table: The :class:`.Table` to use to generate the alias name.
            This is useful for aliased tables.
        :param quoted: Should the name be quoted?
        :return: A str representing the alias name.
        """
        if table is None:
            table = self.table

        fmt = "t_{}_{}".format(table.__tablename__, self.name)
        if quoted:
            return '"{}"'.format(fmt)

        return fmt


