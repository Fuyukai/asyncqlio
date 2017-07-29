import functools
import inspect
import logging
import typing

from cached_property import cached_property

from asyncqlio.meta import proxy_to_getattr
from asyncqlio.orm import operators as md_operators
from asyncqlio.orm.schema import relationship as md_relationship, table as md_table, \
    types as md_types
from asyncqlio.sentinels import NO_DEFAULT

logger = logging.getLogger(__name__)


def _wrap(self, i):
    if not inspect.ismethod(i):
        return i

    # create a wrapper function
    # that hijacks any ColumnValueMixins
    def _wrapper(*args, **kwargs):
        result = i(*args, **kwargs)

        if not isinstance(result, md_operators.ColumnValueMixin):
            return result

        result.column = self
        return result

    return _wrapper


@proxy_to_getattr("__eq__", "__neq__", "__gt__", "__lt__", "__gte__", "__lte__")
class AliasedColumn(object):
    """
    Represents a column on an aliased table.
    """

    def __init__(self, alias_table: 'md_table.AliasedTable',
                 column: 'Column'):
        """
        :param alias_table: The alias table this column is a member of.
        :param column: The Column object this aliased column proxies.
        """
        self.alias_table = alias_table
        self.column = column

    @property
    def quoted_fullname(self):
        return r'"{}"."{}"'.format(self.alias_table.alias_name, self.column.name)

    # needed since we override `__eq__`
    def __hash__(self):
        return self.column.__hash__()

    # proxy to the column object
    def __getattr__(self, item):
        i = getattr(self.column, item)
        # check if it's a metho

        return _wrap(self, i)


@proxy_to_getattr("__contains__", "__getitem__", "__setitem__")
class Column(object):
    """
    Represents a column in a table in a database.

    .. code-block:: python3

        class MyTable(Table):
            id = Column(Integer, primary_key=True)

    The ``id`` column will mirror the ID of records in the table when fetching, etc. and can be set
    on a record when storing in a table.

    .. code-block:: python3

        sess = db.get_session()
        user = await sess.select(User).where(User.id == 2).first()

        print(user.id)  # 2

    """

    def __init__(self, type_: 'typing.Union[md_types.ColumnType, typing.Type[md_types.ColumnType]]',
                 *,
                 primary_key: bool = False,
                 nullable: bool = True,
                 default: typing.Any = NO_DEFAULT,
                 autoincrement: bool = False,
                 index: bool = True,
                 unique: bool = False,
                 foreign_key: 'md_relationship.ForeignKey' = None):
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

        :param foreign_key:
            The :class:`.ForeignKey` associated with this column.
        """
        #: The name of the column.
        #: This can be manually set, or automatically set when set on a table.
        self.name = None  # type: str

        #: The :class:`.Table` instance this Column is associated with.
        self.table = None

        #: The :class:`.ColumnType` that represents the type of this column.
        self.type = type_  # type: md_types.ColumnType
        if not isinstance(self.type, md_types.ColumnType):
            # assume we need to create the "default" type
            self.type = self.type.create_default()  # type: md_types.ColumnType
        # update our own object on the column
        self.type.column = self

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

        #: The foreign key associated with this column.
        self.foreign_key = foreign_key  # type: md_relationship.ForeignKey
        if self.foreign_key is not None:
            self.foreign_key.column = self

    def __repr__(self):
        return "<Column table={} name={} type={}>".format(self.table, self.name, self.type.sql())

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

    def __getattr__(self, item):
        # try and get it from the columntype
        try:
            i = getattr(self.type, item)
        except AttributeError:
            raise AttributeError("Column object '{}' has no attribute '{}'".format(self.name,
                                                                                   item)) from None

        # if it's a function, return a partial that uses this Column
        if inspect.isfunction(i):
            # can be called like Column.whatever(val) and it will pass Column in too
            return functools.partial(i, self)

        # otherwise just return the attribute
        return i

    def __eq__(self, other: typing.Any) -> 'typing.Union[md_operators.Eq, bool]':
        # why is this here?
        # sometimes, we need to check if two columns are equal
        # so this does `col1 == col2` etc
        # however, we override col.__eq__ to return an Eq operator.
        # python does a best guess and calls bool(col.__eq__(other)), which is True
        # because default __bool__ is truthy, this returns True
        # so it assumes they ARE equal
        # an example of this is checking if a column is in a primary key

        # if you need to compare two columns in a where() clause, use `Column.eq` etc.
        if isinstance(other, Column):
            return self.table == other.table and self.name == other.name

        return md_operators.Eq(self, other)

    def __ne__(self, other) -> 'typing.Union[md_operators.NEq, bool]':
        if isinstance(other, Column):
            return self.table != other.table or self.name != other.name

        return md_operators.NEq(self, other)

    def __lt__(self, other) -> 'md_operators.Lt':
        return md_operators.Lt(self, other)

    def __gt__(self, other) -> 'md_operators.Gt':
        return md_operators.Gt(self, other)

    def __le__(self, other) -> 'md_operators.Lte':
        return md_operators.Lte(self, other)

    def __ge__(self, other) -> 'md_operators.Gte':
        return md_operators.Gte(self, other)

    def eq(self, other) -> 'md_operators.Eq':
        """
        Checks if this column is equal to something else.

        .. note::

            This is the easy way to check if a column equals another column in a WHERE clause,
            because the default __eq__ behaviour returns a bool rather than an operator.
        """
        return md_operators.Eq(self, other)

    def ne(self, other) -> 'md_operators.NEq':
        """
        Checks if this column is not equal to something else.

        .. note::

            This is the easy way to check if a column doesn't equal another column in a WHERE
            clause, because the default __ne__ behaviour returns a bool rather than an operator.
        """
        return md_operators.NEq(self, other)

    def asc(self) -> 'md_operators.AscSorter':
        """
        Returns the ascending sorter operator for this column.
        """
        return md_operators.AscSorter(self)

    def desc(self) -> 'md_operators.DescSorter':
        """
        Returns the descending sorter operator for this column.
        """
        return md_operators.DescSorter(self)

    def set(self, value: typing.Any) -> 'md_operators.ValueSetter':
        """
        Sets this column in a bulk update.
        """
        return md_operators.ValueSetter(self, value)

    def incr(self, value: typing.Any) -> 'md_operators.IncrementSetter':
        """
        Increments this column in a bulk update.
        """
        return md_operators.IncrementSetter(self, value)

    def __add__(self, other):
        """
        Magic method for incr()
        """
        return self.incr(other)

    def decr(self, value: typing.Any) -> 'md_operators.DecrementSetter':
        """
        Decrements this column in a bulk update.
        """
        return md_operators.DecrementSetter(self, value)

    def __sub__(self, other):
        """
        Magic method for decr()
        """
        return self.decr(other)

    def quoted_fullname_with_table(self, table: 'md_table.TableMeta') -> str:
        """
        Gets the quoted fullname with a table.
        This is used for columns with alias tables.

        :param table: The :class:`.Table` or :class:`.AliasedTable` to use.
        :return:
        """
        return r'"{}"."{}"'.format(table.__tablename__, self.name)

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

    @property
    def foreign_column(self) -> 'Column':
        """
        :return: The foreign :class:`.Column` this is associated with, or None otherwise.
        """
        if self.foreign_key is None:
            return None

        return self.foreign_key.foreign_column

    def alias_name(self, table=None, quoted: bool = False) -> str:
        """
        Gets the alias name for a column, given the table.

        This is in the format of `t_<table name>_<column_name>`.

        :param table: The :class:`.Table` to use to generate the alias name. \
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
