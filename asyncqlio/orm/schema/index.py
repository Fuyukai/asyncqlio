"""
Represents
"""

import io
import logging
import typing

from cached_property import cached_property

from asyncqlio.orm.schema import column as md_column, table as md_table

logger = logging.getLogger(__name__)


class Index(object):
    """
    Represents an index in a table in a database.

    .. code-block:: python3

        class MyTable(Table):
            id = Column(Integer, primary_key=True)
            name = Column(Text)

            # make an index on the name column
            # by specifying it as the column
            name_index = Index(name)

    .. versionadded:: 0.2.0
    """
    def __init__(self, *columns: 'typing.Union[md_column.Column, str]',
                 unique: bool = False,
                 table: 'md_table.Table' = None):
        """
        :param columns: The :class:`.Column` objects that this index is on.
        :param unique: Is this index unique?
        :param table: The :class:`.Table` for this index. Can be None if the index is a member of \
            a table.
        """
        self.columns = columns
        self.unique = unique
        self.table = table

    def __repr__(self):
        return "<Index table={} columns={} name={}>".format(self.table_name, self.columns,
                                                            self.name)

    def __hash__(self):
        return super().__hash__()

    def __set_name__(self, owner, name):
        """
        Called to update the table and name of this Index.

        :param owner: The :class:`.Table` this Column is on.
        :param name: The str name of this table.
        """
        self.table = owner
        if name.startswith(self.table_name):
            self.name = name
        else:
            self.name = "{}_{}".format(self.table_name, name)
        logger.debug("Index created with name {} on {}".format(name, owner))

    def get_column_names(self) -> typing.Generator[str, None, None]:
        """
        :return: A generator that yields the names of the columns for this index.
        """
        for column in self.columns:
            if isinstance(column, str):
                yield column
            else:
                yield column.name

    @property
    def table_name(self) -> str:
        """
        The name of this index's table.
        """
        if isinstance(self.table, str):
            return self.table
        return self.table.__tablename__

    @cached_property
    def quoted_name(self) -> str:
        """
        Gets the quoted name for this Index.

        This returns the column name in "inde" format.
        """
        return r'"{}"'.format(self.name)

    @cached_property
    def quoted_fullname(self) -> str:
        """
        Gets the full quoted name for this index.

        This returns the column name in "table"."index" format.
        """
        return r'"{}"."{}"'.format(self.table_name, self.name)

    @classmethod
    def with_name(cls, name: str, *args, **kwargs) -> 'Index':
        """
        Creates this column with a name and, optionally, table name already set.
        """
        idx = cls(*args, **kwargs)
        idx.name = name
        return idx

    def get_ddl_sql(self) -> str:
        """
        Gets the DDL SQL for this index.
        """
        base = io.StringIO()
        base.write("CREATE ")
        if self.unique:
            base.write("UNIQUE ")
        base.write("INDEX ")
        base.write(self.name)
        base.write(" ON ")
        base.write(self.table_name)
        base.write(" (")
        base.write(', '.join(self.get_column_names()))
        base.write(")")

        return base.getvalue()

    def _get_column_refs(self) -> 'typing.Generator[str, None, None]':
        for column in self.columns:
            if isinstance(column, str):
                yield '"{}"'.format(column)
            else:
                yield column.name

    def generate_schema(self, fp) -> str:
        """
        Generates the library schema for this index.
        """
        schema = fp or io.StringIO()

        schema.write(self.name)
        schema.write(" = ")
        schema.write(type(self).__name__)
        schema.write("(")
        schema.write(", ".join(self._get_column_refs()))
        if self.unique:
            schema.write(", unique=True")
        schema.write(")")

        return schema.getvalue() if fp is None else ""
