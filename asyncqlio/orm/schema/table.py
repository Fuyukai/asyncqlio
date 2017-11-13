"""
Table objects.
"""

import collections
import io
import itertools
import logging
import sys
import typing
from collections import OrderedDict

from asyncqlio import db as md_db
from asyncqlio.exc import SchemaError
from asyncqlio.meta import typeproperty
from asyncqlio.orm import inspection as md_inspection, session as md_session
from asyncqlio.orm.schema import column as md_column, history as md_history, index as md_index, \
    relationship as md_relationship
from asyncqlio.orm.schema.decorators import enforce_bound
from asyncqlio.sentinels import NO_DEFAULT, NO_VALUE

PY36 = sys.version_info[0:2] >= (3, 6)
logger = logging.getLogger(__name__)


class TableMetadata(object):
    """
    The root class for table metadata.
    This stores a registry of tables, and is responsible for calculating relationships etc.

    .. code-block:: python3

        meta = TableMetadata()
        Table = table_base(metadata=meta)

    """

    def __init__(self):
        #: A registry of table name -> table object for this metadata.
        self.tables = {}

        #: The DB object bound to this metadata.
        self.bind = None  # type: md_db.DatabaseInterface

    def register_table(self, tbl: 'TableMeta', *,
                       autosetup_tables: bool = False) -> 'TableMeta':
        """
        Registers a new table object.

        :param tbl: The table to register.
        :param autosetup_tables: Should tables be setup again?
        """
        tbl.metadata = self
        self.tables[tbl.__tablename__] = tbl

        if autosetup_tables:
            self.setup_tables()

        return tbl

    def get_table(self, table_name: str) -> 'typing.Type[Table]':
        """
        Gets a table from the current metadata.

        :param table_name: The name of the table to get.
        :return: A :class:`.Table` object.
        """
        try:
            return self.tables[table_name]
        except KeyError:
            # we can load this from the name instead
            for table in self.tables.values():
                if table.__name__ == table_name:
                    return table
            else:
                return None

    def setup_tables(self):
        """
        Sets up the tables for usage in the ORM.
        """
        self.resolve_floating_relationships()
        self.resolve_aliases()
        self.resolve_backrefs()
        self.generate_primary_key_indexes()
        self.generate_unique_column_indexes()

    def resolve_aliases(self):
        """
        Resolves all alias tables on relationship objects.
        """
        for tbl in self.tables.copy().values():
            if isinstance(tbl, AliasedTable):
                continue

            for relationship in tbl.iter_relationships():
                if relationship._table_alias is None:
                    # auto-create the alias name
                    # using the relationship name
                    relationship._table_alias = "r_{}_{}".format(
                        relationship.owner_table.__name__, relationship._name.lower()
                    )

                if not isinstance(relationship._table_alias, AliasedTable):
                    relationship._table_alias = AliasedTable(relationship._table_alias,
                                                             relationship.foreign_table)
                self.tables[relationship._table_alias.alias_name] = relationship._table_alias

    def resolve_backrefs(self):
        """
        Resolves back-references.
        """
        for tbl in self.tables.values():
            # type: TableMeta
            if isinstance(tbl, AliasedTable):
                # don't try and setup aliased tables
                continue

            for relationship in tbl.iter_relationships():
                if relationship.back_reference is None:
                    continue

                table, name = relationship.back_reference.split(".")
                table = self.get_table(table)
                # create the new relationship object
                # this flips the two columns so that the join path is correct
                new_rel = md_relationship.Relationship(relationship.right_column,
                                                       relationship.left_column,
                                                       load="joined", use_iter=False)

                # call `__set_name__` so that it knows what table it's assigned to
                new_rel.__set_name__(table, name)

                table._relationships[name] = new_rel
                relationship.back_reference = new_rel

    def resolve_floating_relationships(self):
        """
        Resolves any "floating" relationships - i.e any relationship/foreign keys that don't
        directly reference a column object.
        """
        for tbl in self.tables.values():
            if isinstance(tbl, AliasedTable):
                # don't try and resolve relationships on aliases
                continue

            for column in tbl._columns.values():
                # if the fk is none we don't need to set it up
                if column.foreign_key is None:
                    continue

                foreignkey = column.foreign_key
                if foreignkey.foreign_column is None:
                    table, column = foreignkey._f_name.split(".")
                    table_obb = self.get_table(table)
                    if table_obb is None:
                        raise SchemaError("No such table '{}' exists in FK {}"
                                          .format(table, foreignkey))

                    col = table_obb.get_column(column)
                    if col is None:
                        raise SchemaError("No such column '{}' exists on table '{}'"
                                          "(from FK {})"
                                          .format(column, table, foreignkey))

                    foreignkey.foreign_column = col

            for relation in tbl._relationships.values():
                assert isinstance(relation, md_relationship.Relationship)

                resolving_columns = [col for col in [relation.left_column, relation.right_column]
                                     if isinstance(col, str)]

                for to_resolve in resolving_columns:
                    table, column = to_resolve.split(".")
                    table_obb = self.get_table(table)

                    if table_obb is None:
                        raise SchemaError("No such table '{}' exists"
                                          "(from relationship {})".format(table, relation))

                    col = table_obb.get_column(column)
                    if col is None:
                        raise SchemaError("No such column '{}' exists on table '{}'"
                                          .format(table, column))

                    if (to_resolve == relation.left_column) is True:
                        relation.left_column = col
                        logger.debug("Resolved {} to {}".format(to_resolve, col))
                    elif (to_resolve == relation.right_column) is True:
                        relation.right_column = col
                        logger.debug("Resolved {} to {}".format(to_resolve, col))
                    else:
                        raise SchemaError("Could not resolve column '{}' - it did not match the "
                                          "left or right column!")

    def generate_primary_key_indexes(self):
        """
        Generates an index for the primary key of each table, if the dialect
        creates one.

        .. versionadded:: 0.2.0
        """
        for name, table in self.tables.items():
            index_name = self.bind.dialect.get_primary_key_index_name(name)
            if not index_name:
                return

            table._indexes[index_name] = md_index.Index.with_name(
                index_name,
                *table._primary_key.columns,
                table=name,
            )

            table._primary_key.index_name = index_name

    def generate_unique_column_indexes(self):
        """
        Generates an index for columns marked as unique in each table, if the
        dialect creates them.

        .. versionadded:: 0.2.0
        """
        for name, table in self.tables.items():
            if isinstance(table, AliasedTable):
                continue
            for column in table.iter_columns():
                index_name = self.bind.dialect.get_unique_column_index_name(name, column.name)
                if not index_name:
                    return
                table._indexes[index_name] = md_index.Index.with_name(
                    index_name,
                    column,
                    table=name,
                )


class TableMeta(type):
    """
    The metaclass for a table object. This represents the "type" of a table class.
    """

    def __prepare__(*args, **kwargs):
        # this is required so that columns are ordered.
        return OrderedDict()

    def __new__(mcs, name: str, bases: tuple, class_body: dict,
                register: bool = True, *args, **kwargs):
        # usually a cloned class
        # so we just skip it directly
        if register is False:
            return type.__new__(mcs, name, bases, class_body)

        # what we do here is hijack some objects
        # this allows us to re-organize them in our own internal data structures
        # allowing us to do some better actions with them

        columns = OrderedDict()
        relationships = OrderedDict()
        indexes = OrderedDict()
        for col_name, value in class_body.copy().items():
            if isinstance(value, md_column.Column):
                columns[col_name] = value
                # nuke the column
                class_body.pop(col_name)
            elif isinstance(value, md_relationship.Relationship):
                relationships[col_name] = value
                class_body.pop(col_name)
            elif isinstance(value, md_index.Index):
                indexes[col_name] = value
                class_body.pop(col_name)

        class_body["_columns"] = columns
        class_body["_relationships"] = relationships
        class_body["_indexes"] = indexes

        try:
            class_body["__tablename__"] = kwargs["table_name"]
        except KeyError:
            class_body["__tablename__"] = name.lower()

        return type.__new__(mcs, name, bases, class_body)

    def __init__(self, tblname: str, tblbases: tuple, class_body: dict, register: bool = True,
                 *args, **kwargs):
        """
        Creates a new Table instance.

        :param register: Should this table be registered in the TableMetadata?
        :param table_name: The name for this table.
        """
        # create the new type object
        super().__init__(tblname, tblbases, class_body)

        if register is False:
            return
        elif not hasattr(self, "metadata"):
            raise TypeError("Table {} has been created but has no metadata - did you subclass Table"
                            " directly instead of a clone?".format(tblname))

        # emulate `__set_name__` on Python 3.5
        # also, set names on columns unconditionally
        it = itertools.chain(self._columns.items(), self._relationships.items(),
                             self._indexes.items())
        if not PY36:
            it = itertools.chain(class_body.items(), it)

        for name, value in it:
            if hasattr(value, "__set_name__"):
                value.__set_name__(self, name)

        # ================ #
        # TABLE ATTRIBUTES #
        # ================ #

        #: The :class:`.TableMetadata` for this table.
        self.metadata = self.metadata  # type: TableMetadata

        #: A dict of columns for this table.
        self._columns = self._columns  # type: typing.Dict[str, md_column.Column]

        #: A dict of relationships for this table.
        self._relationships = \
            self._relationships  # type: typing.Dict[str, md_relationship.Relationship]

        #: A dict of indexes for this table.
        self._indexes = self._indexes  # type: typing.Dict[str, md_index.Index]

        #: The primary key for this table.
        #: This should be a :class:`.PrimaryKey`.
        self._primary_key = self._calculate_primary_key()

        logger.debug("Registered new table {}".format(tblname))
        self.metadata.register_table(self)

    def __getattr__(self, item):
        if item.startswith("_"):
            raise AttributeError("'{}' object has no attribute {}".format(self.__name__, item))

        # lets hope we have a descendant as Table
        col = self.get_column(item)
        if col is not None:
            return col

        relationship = self.get_relationship(item)
        if relationship is not None:
            return relationship

        index = self.get_index(item)
        if index is not None:
            return index

        raise AttributeError("'{}' object has no attribute {}".format(self.__name__, item))

    def __repr__(self):
        try:
            return "<Table object='{}' name='{}'>".format(self.__name__, self.__tablename__)
        except AttributeError:
            return super().__repr__()

    def _calculate_primary_key(self) -> typing.Union['PrimaryKey', None]:
        """
        Calculates the current primary key for a table, given all the columns.

        If no columns are marked as a primary key, the key will not be generated.
        """
        pk_cols = []
        for col in self.iter_columns():
            if col.primary_key is True:
                pk_cols.append(col)

        if pk_cols:
            pk = PrimaryKey(*pk_cols)
            pk.table = self
            logger.debug("Calculated new primary key {}".format(pk))
            return pk

        return None

    @property
    def primary_key(self) -> 'PrimaryKey':
        """
        :getter: The :class:`.PrimaryKey` for this table.
        :setter: A new :class:`.PrimaryKey` for this table.

        .. note::

            A primary key will automatically be calculated from columns at define time, if any
            columns have ``primary_key`` set to True.
        """
        return self._primary_key

    @primary_key.setter
    def primary_key(self, key: 'PrimaryKey'):
        key.table = self
        self._primary_key = key

    def _internal_from_row(cls, values: dict, *,
                           existed: bool = False):
        obb = object.__new__(cls)  # type: Table
        # init but dont pass any values
        obb.__init__()
        setattr(obb, "_{}__existed".format(cls.__name__), existed)
        obb._init_row(**values)
        return obb


class Table(metaclass=TableMeta, register=False):
    """
    The "base" class for all tables. This class is not actually directly used; instead
    :meth:`.table_base` should be called to get a fresh clone.
    """

    def __init__(self, **kwargs):
        #: The actual table that this object is an instance of.
        self.table = type(self)  # type: TableMeta

        #: If this row existed before.
        #: If this is True, this row was fetched from the DB previously.
        #: Otherwise, it is a fresh row.
        self.__existed = False

        #: If this row is marked as "deleted".
        #: This means that the row cannot be updated.
        self.__deleted = False

        #: The session this row is attached to.
        self._session = None  # type: md_session.Session

        #: A mapping of Column -> ColumnChange object for this row.
        self._history = {}  # type: typing.Dict[md_column.Column, md_history.ColumnChange]

        #: A mapping of relationship -> rows for this row.
        self._relationship_mapping = collections.defaultdict(lambda: [])

        #: A mapping of Column -> Current value for this row.
        self._values = {}

        if kwargs:
            self._init_row(**kwargs)

    # Class properties
    @typeproperty
    @classmethod
    def columns(cls) -> 'typing.List[md_column.Column]':
        """
        :return: A list of :class:`.Column` this Table has.
        """
        return list(cls.iter_columns())

    @typeproperty
    @classmethod
    def __quoted_name__(cls) -> str:
        """
        :return: The quoted name of this table.
        """
        return '"{}"'.format(cls.__tablename__)

    # Class methods
    @classmethod
    @enforce_bound
    async def create(cls, *, if_not_exists: bool = True):
        """
        Creates a table with this schema in the database.
        """
        async with cls.metadata.bind.get_ddl_session() as sess:
            await sess.create_table(cls.__tablename__,
                                    *cls.iter_columns(),
                                    *cls.explicit_indexes(),
                                    if_not_exists=if_not_exists,
                                    )

    @classmethod
    @enforce_bound
    async def drop(cls, *, cascade: bool = False, if_exists: bool = True):
        """
        Drops this table, or a table with the same name, from the database.

        :param cascade: If this drop should cascade.
        :param if_exists: If we should only attempt to drop tables that exist.
        """
        async with cls.metadata.bind.get_ddl_session() as sess:
            await sess.drop_table(cls.__tablename__, if_exists=if_exists, cascade=cascade)

    @classmethod
    async def truncate(cls, *, cascade: bool = False):
        """
        Truncates this table.

        :param cascade: If this truncation should cascade to other tables.

        .. versionchanged:: 0.2.0

            Moved from :class:`.TableMeta` to :class:`.Table` as a classmethod.
        """
        async with cls.metadata.bind.get_session() as sess:
            return await sess.truncate(cls, cascade=cascade)

    @classmethod
    def iter_relationships(cls) -> 'typing.Generator[md_relationship.Relationship, None, None]':
        """
        :return: A generator that yields :class:`.Relationship` objects for this table.

        .. versionchanged:: 0.2.0

            Moved from :class:`.TableMeta` to :class:`.Table` as a classmethod.

        """
        for rel in cls._relationships.values():
            yield rel

    @classmethod
    def iter_columns(cls) -> 'typing.Generator[md_column.Column, None, None]':
        """
        :return: A generator that yields :class:`.Column` objects for this table.

        .. versionchanged:: 0.2.0

            Moved from :class:`.TableMeta` to :class:`.Table` as a classmethod.

        """
        for col in cls._columns.values():
            yield col

    @classmethod
    def iter_indexes(cls) -> 'typing.Generator[md_index.Index, None, None]':
        """
        :return: A generator that yields :class:`.Index` objects for this table.

        .. versionchanged:: 0.2.0

            Moved from :class:`.TableMeta` to :class:`.Table` as a classmethod.

        """
        for idx in cls._indexes.values():
            yield idx

    @classmethod
    @enforce_bound
    def explicit_indexes(cls) -> 'typing.Generator[md_index.Index, None, None]':
        """
        :return: A generator that yields :class:`.Index` objects for this table.

        Only manually added indexes are yielded from this generator; that is, it
        ignores primary key indexes, unique column indexes, relationship indexes, etc

        .. versionchanged:: 0.2.0

            Moved from :class:`.TableMeta` to :class:`.Table` as a classmethod.
        """
        table_name = cls.__tablename__
        for index in cls.iter_indexes():
            if index.table_name != table_name:
                continue
            if index.name == cls.metadata.bind.dialect.get_primary_key_index_name(table_name):
                continue
            unique_names = (cls.metadata.bind.dialect.get_unique_column_index_name(table_name,
                                                                                   col_name)
                            for col_name in index.get_column_names())
            if index.name in unique_names:
                continue
            yield index

    @classmethod
    def get_column(cls, column_name: str) -> 'typing.Union[md_column.Column, None]':
        """
        Gets a column by name.

        :param column_name: The column name to lookup.

            This can be one of the following:
                - The column's ``name``
                - The column's ``alias_name()`` for this table

        :return: The :class:`.Column` associated with that name, or None if no column was found.

        .. versionchanged:: 0.2.0

            Moved from :class:`.TableMeta` to :class:`.Table` as a classmethod.

        """
        try:
            return cls._columns[column_name]
        except KeyError:
            for column in cls._columns.values():
                alias = column.alias_name(table=cls)
                if alias == column_name:
                    return column

        return None

    @classmethod
    def get_relationship(cls, relationship_name) \
            -> 'typing.Union[md_relationship.Relationship, None]':
        """
        Gets a relationship by name.

        :param relationship_name: The name of the relationship to get.
        :return: The :class:`.Relationship` associated with that name, or None if it doesn't exist.

        .. versionchanged:: 0.2.0

            Moved from :class:`.TableMeta` to :class:`.Table` as a classmethod.

        """
        try:
            return cls._relationships[relationship_name]
        except KeyError:
            return None

    @classmethod
    def get_index(cls, index_name) -> 'typing.Union[md_index.Index, None]':
        """
        Gets an index by name.

        :param index_name: The name of the index to get.
        :return: The :class:`.Index` associated with that name, or None if it doesn't exist.

        .. versionchanged:: 0.2.0

            Moved from :class:`.TableMeta` to :class:`.Table` as a classmethod.

        """
        try:
            return cls._indexes[index_name]
        except KeyError:
            return None

    def _init_row(self, **values):
        """
        Initializes the rows for this table, setting the values of the object.

        :param values: The values to pass into this column.
        """
        for name, value in values.items():
            column = self.table.get_column(name)
            if column is None:
                raise TypeError("Unexpected row parameter: '{}'".format(name))

            self._values[column] = value

        return self

    def __repr__(self):
        gen = ("{}={}".format(col.name, self.get_column_value(col)) for col in self.table.columns)
        return "<{} {}>".format(self.table.__name__, " ".join(gen))

    def __eq__(self, other):
        if not isinstance(other, Table):
            return NotImplemented

        if other.table != self.table:
            raise ValueError("Rows to compare must be on the same table")

        return self.primary_key == other.primary_key

    def __le__(self, other):
        if not isinstance(other, Table):
            return NotImplemented

        if other.table != self.table:
            raise ValueError("Rows to compare must be on the same table")

        return self.primary_key <= other.primary_key

    def __setattr__(self, key, value):
        # ensure we're not doing stupid shit until we get _values
        try:
            object.__getattribute__(self, "_values")
        except AttributeError:
            return super().__setattr__(key, value)

        # micro optimization
        # if it's in our __dict__, it's probably not a column
        # so bypass the column check and set it directly
        if key in self.__dict__:
            return super().__setattr__(key, value)

        col = self.table.get_column(column_name=key)
        if col is None:
            return super().__setattr__(key, value)

        # call on_set for the column
        return col.type.on_set(self, value)

    @property
    def primary_key(self) -> typing.Union[typing.Any, typing.Iterable[typing.Any]]:
        """
        Gets the primary key for this row.

        If this table only has one primary key column, this property will be a single value.
        If this table has multiple columns in a primary key, this property will be a tuple.
        """
        pk = self.table.primary_key  # type: PrimaryKey
        result = []

        for col in pk.columns:
            val = self.get_column_value(col)
            result.append(val)

        if len(result) == 1:
            return result[0]

        return tuple(result)

    def __getattr__(self, item: str):
        obb = self._resolve_item(item)
        return obb

    __hash__ = object.__hash__

    # sql generation methods
    def _get_insert_sql(self, emitter: typing.Callable[[], typing.Tuple[str, str]],
                        session: 'md_session.Session'):
        """
        Gets the INSERT into statement SQL for this row.
        """
        if self._session is None:
            self._session = session

        q = io.StringIO()
        q.write("INSERT INTO {} ".format(self.__quoted_name__))
        params = {}
        column_names = []
        sql_params = []

        for column in self.table.iter_columns():
            value = self.get_column_value(column)
            if value is NO_VALUE or (value is None and column.default is NO_DEFAULT):
                # XXX: Only emit a column w/ DEFUALT if the DB supports it (i.e. not sqlite3).
                # In sqlite3, missing out that column is an implicit default anyway.
                # It's better to be explicit, but otherwise it syntax errors.
                if session.bind.dialect.has_default:
                    column_names.append(column.quoted_name)
                    sql_params.append("DEFAULT")
            else:
                # emit a new param
                param, name = emitter()
                # set the params to value
                # then add the {param_name} to the VALUES
                params[name] = value
                column_names.append(column.quoted_name)
                sql_params.append(param)

        q.write("({}) ".format(", ".join(column_names)))
        q.write("VALUES ")
        q.write("({}) ".format(", ".join(sql_params)))
        # check if we support RETURNS
        if session.bind.dialect.has_returns:
            columns_to_get = []
            # always return every column
            # this allows filling in of autoincrement + defaults
            for column in self.table.iter_columns():
                columns_to_get.append(column)

            to_return = ", ".join(column.quoted_name for column in columns_to_get)
            q.write(" RETURNING {}".format(to_return))

        q.write(";")
        return q.getvalue(), params

    def _get_update_sql(self, emitter: typing.Callable[[], typing.Tuple[str, str]],
                        session: 'md_session.Session'):
        """
        Gets the UPDATE statement SQL for this row.
        """
        if self._session is None:
            self._session = session

        params = {}
        base_query = io.StringIO()
        base_query.write("UPDATE {} SET ".format(self.__quoted_name__))

        for column in self.table.columns:
            # lookup the history object
            # if there is none, there's been no change
            try:
                change = self._history[column]
            except KeyError:
                continue

            response = change.get_update_sql(emitter)
            base_query.write(" ")
            base_query.write(response.sql)
            params.update(response.parameters)

        base_query.write(" WHERE (")
        where_clauses = 0

        for idx, column in enumerate(self.table.primary_key.columns):
            # don't use history object here, however
            try:
                value = self._values[column]
            except KeyError:
                # bad, usually
                continue
            else:
                where_clauses += 1

            name, param = emitter()
            params[param] = value
            base_query.write("{} = {}".format(column.quoted_fullname,
                                              name))

            if idx + 1 < len(self.table.primary_key.columns):
                base_query.write(" AND ")

        base_query.write(")")

        if where_clauses == 0:
            raise ValueError("No where clauses specified when generating update")

        return base_query.getvalue(), params

    def _get_upsert_sql(self, emitter: 'typing.Callable[[], str]', session: 'md_session.Session',
                        *,
                        update_columns: 'typing.List[md_column.Column]',
                        on_conflict_columns: 'typing.List[md_column.Column]',
                        on_conflict_update: bool):
        """
        Gets the UPSERT sql for this row.

        .. versionadded:: 0.2.0

        :param session: The :class:`.Session` whose dialect to use when creating the SQL.
        :param update_columns: The :class:`.Column` objects to update on conflict.
        :param on_conflict_columns: The :class:`.Column` objects on which there may be a conflict.
        :param on_conflict_update: Whether to update the table on conflict.
        """
        params = {}
        fmt_params = {}
        row_dict = {}

        fmt, needed_params = session.bind.dialect.get_upsert_sql(
            self.__quoted_name__,
            on_conflict_update=on_conflict_update,
        )

        for column in type(self).iter_columns():
            param = emitter()
            params[param] = self.get_column_value(column)
            row_dict[column] = session.bind.emit_param(param)

        col_names = ", ".join(col.quoted_name for col in row_dict.keys())

        for fmt_param in needed_params:
            if fmt_param == "where":
                fmt_params["where"] = " AND ".join("{}={}".format(col.quoted_fullname,
                                                   session.bind.emit_param(param))
                                                   for col in on_conflict_columns)
                params.update({emitter(): self.get_column_value(col)
                               for col in on_conflict_columns})

            elif fmt_param == "update":
                fmt_params["update"] = ", ".join("{}={}".format(col.quoted_name, param)
                                                 for col, param in row_dict.items()
                                                 if col in update_columns)

            elif fmt_param == "returning":
                fmt_params["returning"] = col_names

            elif fmt_param == "insert":
                fmt_params["insert"] = "({}) VALUES ({})".format(
                    col_names,
                    ", ".join(row_dict.values()),
                )

            elif fmt_param == "col":
                names = ", ".join(col.quoted_name for col in on_conflict_columns)
                fmt_params["col"] = names

            else:
                raise RuntimeError("Driver passed an invalid format specification.")

        sql = fmt.format(**fmt_params)

        return sql, params

    def _get_delete_sql(self, emitter: typing.Callable[[], typing.Tuple[str, str]],
                        session: 'md_session.Session') -> typing.Tuple[str, typing.Any]:
        """
        Gets the DELETE sql for this row.
        """
        if self._session is None:
            self._session = session

        query = io.StringIO()
        query.write("DELETE FROM {} ".format(self.__quoted_name__))
        # generate the where clauses
        wheres = []
        params = {}

        for col, value in zip(self.table.primary_key.columns,
                              md_inspection.get_pk(self, as_tuple=True)):
            param, name = emitter()
            params[name] = value
            wheres.append("{} = {}".format(col.quoted_fullname, param))

        query.write("WHERE ({}) ".format(" AND ".join(wheres)))
        return query.getvalue(), params

    # value loading methods
    def _resolve_item(self, name: str):
        """
        Resolves an item on this row.

        This will check:

            - Functions decorated with :func:`.row_attr`
            - Non-column :class:`.Table` members
            - Columns

        :param name: The name to resolve.
        :return: The object returned, if applicable.
        """
        # try and load a relationship loader object
        try:
            return self.get_relationship_instance(name)
        except ValueError:
            pass

        # failed to load relationship, too, so load a column value instead
        col = self.table.get_column(name)
        if col is None:
            raise AttributeError("{} was not a function or attribute on the associated table, "
                                 "and was not a column".format(name)) from None

        return col.type.on_get(self)

    def get_column_value(self, column: 'md_column.Column', return_default: bool = True):
        """
        Gets the value from the specified column in this row.

        .. warning::

            This method should not be used by user code; it is for types to interface with only.

        :param column: The column.
        :param return_default: If this should return the column default, or NO_VALUE.
        """
        if column.table != self.table:
            raise ValueError("Column table must match row table")

        try:
            return self._history[column].current_value
        except KeyError:
            try:
                return self._values[column]
            except KeyError:
                if return_default:
                    default = column.default
                    if default is NO_DEFAULT:
                        return None
                    else:
                        return default
                else:
                    return NO_VALUE

    def store_column_value(self, column: 'md_column.Column', value: typing.Any,
                           *, track_history: bool = True):
        """
        Updates the value of a column in this row.
        This will also update the history of the value, if applicable.

        .. warning::

            This method should not be used by user code; it is for types to interface with only.

        :param column: The column to store.
        :param value: The value to store in the column.
        :param track_history: Should history be tracked? Only false if creating a row from a data \
            source.
        """
        if self.__deleted:
            raise RuntimeError("This row is marked as deleted")

        if track_history:
            change = md_history.ValueChange(column)
            if column in self._history:
                change.handle_change_with_history(self._history[column], value)
            else:
                 change.handle_change(self._values[column], value)

            self._history[column] = change
        else:
            self._values[column] = value

        return self

    def get_relationship_instance(self, relation_name: str):
        """
        Gets a 'relationship instance'.

        :param relation_name: The name of the relationship to load.
        """
        try:
            relation = next(filter(
                lambda relationship: relationship._name == relation_name,
                self.table.iter_relationships()
            ))
        except StopIteration:
            raise ValueError("No such relationship '{}'".format(relation_name))

        rel = relation.get_instance(self, self._session)
        rel.set_rows(self._relationship_mapping[relation])
        rel._update_sub_relationships(self._relationship_mapping)
        return rel

    def _load_columns_using_table(self, table: 'TableMeta', record: dict, buckets: dict,
                                  seen: list):
        """
        Recursively organizes columns in a record into table buckets by scanning the
        relationships inside the table.

        :param table: The :class:`.TableMeta` to use to load the table.
        :param record: The dict-like record to read from.
        :param buckets: The dict of buckets to store tables in.
        :param seen: A list of relationships that have already been seen. This prevents infinite \
            loops.

            Outside of internal code, this should be passed in as an empty list.

        """
        for relationship in table.iter_relationships():
            if relationship in seen:
                continue

            seen.append(relationship)

            self._load_columns_using_relationship(relationship, record, buckets)
            self._load_columns_using_table(relationship.foreign_table, record, buckets, seen)

    def _load_columns_using_relationship(self, relationship, record: dict, buckets: dict):
        """
        Loads columns from a record dict using a relationship object.
        """
        if relationship not in buckets:
            buckets[relationship] = {}

        # iterate over every column in the record
        # checking to see if the column adds up
        for cname, value in record.copy().items():
            # this will load using cname too, thankfully
            # use the foreign column to load the columns
            # since this is the one we're joining on
            column = relationship.foreign_table.get_column(cname)
            if column is not None:
                # use the actual name
                # if we use the cname, it won't expand into the row correctly
                actual_name = column.name
                buckets[relationship][actual_name] = value
                # get rid of the record
                # so it doesn't come around in the next relationship check
                record.pop(cname)

    def _update_relationships(self, record: dict):
        """
        Updates relationship data for this row, storing any extra rows that are needed.

        :param record: The dict record of extra data to store.
        """
        if self.__deleted:
            raise RuntimeError("This row is marked as deleted")

        if self.table not in self._relationship_mapping:
            self._relationship_mapping[self.table] = [self]

        buckets = {}
        seen = []
        # this will load columns recursively
        self._load_columns_using_table(self.table, record, buckets, seen)

        # store the new relationship data
        for relationship, subdict in buckets.items():
            # Prevent null values from showing up
            if all(i is None for i in subdict.values()):
                continue

            row = relationship.foreign_table._internal_from_row(subdict, existed=True)
            # ensure the row doesn't already exist with the PK
            try:
                next(filter(lambda r: r.primary_key == row.primary_key,
                            self._relationship_mapping[relationship]))
            except StopIteration:
                # only append if the row didn't exist earlier
                # i.e that the filter raised StopIteration
                self._relationship_mapping[relationship].append(row)
            else:
                row._session = self._session

    def to_dict(self, *, include_attrs: bool = False) -> dict:
        """
        Converts this row to a dict, indexed by Column.

        :param include_attrs: Should this include row_attrs?
        """
        # todo: include row attrs
        d = {col: self.get_column_value(col) for col in self.table.columns}
        return d

    @classmethod
    def generate_schema(cls, fp=None) -> str:
        """
        Generates a Python class body that corresponds to the current DB schema.
        """
        schema = fp or io.StringIO()
        schema.write("class ")
        schema.write(cls.__name__)
        schema.write("(Table")
        if cls.__name__.lower() != cls.__tablename__:
            schema.write(', table_name="')
            schema.write(cls.__tablename__)
            schema.write('"')
        schema.write("):\n")
        for column in cls.iter_columns():
            schema.write("    ")
            schema.write(column.generate_schema(fp))
            schema.write("\n")
        for index in cls.explicit_indexes():
            schema.write("    ")
            schema.write(index.generate_schema(fp))
            schema.write("\n")
        for relationship in cls.iter_relationships():
            schema.write("    ")
            schema.write(relationship.generate_schema(fp))
            schema.write("\n")

        return schema.getvalue() if fp is None else ""


def table_base(name: str = "Table", meta: 'TableMetadata' = None):
    """
    Gets a new base object to use for OO-style tables.
    This object is the parent of all tables created in the object-oriented style; it provides some
    key configuration to the relationship calculator and the DB object itself.

    To use this object, you call this function to create the new object, and subclass it in your
    table classes:

    .. code-block:: python3

        Table = table_base()

        class User(Table):
            ...

    Binding the base object to the database object is essential for querying:

    .. code-block:: python3

        # ensure the table is bound to that database
        db.bind_tables(Table.metadata)

        # now we can do queries
        sess = db.get_session()
        user = await sess.select(User).where(User.id == 2).first()

    Each Table object is associated with a database interface, which it uses for special querying
    inside the object, such as :meth:`.Table.get`.

    .. code-block:: python3

        class User(Table):
            id = Column(Integer, primary_key=True)
            ...

        db.bind_tables(Table.metadata)
        # later on, in some worker code
        user = await User.get(1)

    :param name: The name of the new class to produce. By default, it is ``Table``.
    :param meta: The :class:`.TableMetadata` to use as metadata.
    :return: A new Table class that can be used for OO tables.
    """
    if meta is None:
        meta = TableMetadata()

    # This is the best way of cloning the Table object, instead of using `type()`.
    # It works on all Python versions, and is directly calling the metaclass.
    clone = TableMeta.__new__(TableMeta, name, (Table,), {"metadata": meta}, register=False)
    return clone


class AliasedTable(object):
    """
    Represents an "aliased table". This is a transparent proxy to a :class:`.TableMeta` table, and
    will create the right Table objects when called.

    .. code-block:: python3

        class User(Table):
            id = Column(Integer, primary_key=True, autoincrement=True)
            username = Column(String, nullable=False, unique=True)
            password = Column(String, nullable=False)

        NotUser = AliasedTable("not_user", User)

    """

    def __init__(self, alias_name: str, table: 'typing.Type[Table]'):
        """
        :param alias_name: The name of the alias for this table.
        :param table: The :class:`.TableMeta` used to alias this table.
        """
        self.alias_name = alias_name
        self.alias_table = table

    # proxy getattr
    def __getattr__(self, item):
        return getattr(self.alias_table, item)

    # proxy call to the alias table
    # so it makes new rows
    def __call__(self, *args, **kwargs):
        return self.alias_table(*args, **kwargs)

    def __repr__(self):
        return "<Alias {} for {}>".format(self.alias_name, self.alias_table)

    def get_column(self, column_name: str) -> 'md_column.Column':
        """
        Gets a column by name from the specified table.

        This will use the base :meth:`.TableMeta.get_column`, and then search for columns via
        their alias name using this table.
        """
        c = self.alias_table.get_column(column_name)
        if c is not None:
            return c

        for column in self.alias_table.iter_columns():
            if column.alias_name(self) == column_name:
                return column

        return None

    # override some attributes
    @property
    def __tablename__(self) -> str:
        return self.alias_name

    @property
    def __quoted_name__(self):
        return '"{}"'.format(self.alias_name)


class PrimaryKey(object):
    """
    Represents the primary key of a table.

    A primary key can be on any 1 to N columns in a table.

    .. code-block:: python3

        class Something(Table):
            first_id = Column(Integer)
            second_id = Column(Integer)

        pkey = PrimaryKey(Something.first_id, Something.second_id)
        Something.primary_key = pkey

    Alternatively, the primary key can be automatically calculated by passing ``primary_key=True``
    to columns in their constructor:

    .. code-block:: python3

        class Something(Table):
            id = Column(Integer, primary_key=True)

        print(Something.primary_key)

    """

    def __init__(self, *cols: 'md_column.Column'):
        #: A list of :class:`.Column` that this primary key encompasses.
        self.columns = list(cols)  # type: typing.List[md_column.Column]

        #: The table this primary key is bound to.
        self.table = None

        #: The index name of this primary key, if any
        self.index_name = None

    def __repr__(self):
        return "<PrimaryKey table='{}' columns='{}'>".format(self.table, self.columns)
