import collections
import io
import itertools
import logging
import sys
import typing
from collections import OrderedDict

from asyncqlio import db as md_db
from asyncqlio.exc import SchemaError
from asyncqlio.orm import inspection as md_inspection, session as md_session
from asyncqlio.orm.schema import column as md_column, relationship as md_relationship,\
    index as md_index
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
        """
        for name, table in self.tables.items():
            index_name = self._bind.dialect.get_primary_key_index_name(name)
            if not index_name:
                return
            table._indexes[index_name] = md_index.Index.with_name(
                index_name,
                *table._primary_key.columns,
                table_name=name,
            )
            table._primary_key.index_name = index_name

    def generate_unique_column_indexes(self):
        """
        Generates an index for columns marked as unique in each table, if the
        dialect creates them.
        """
        for name, table in self.tables.items():
            if isinstance(table, AliasedTable):
                continue
            for column in table.iter_columns():
                index_name = self._bind.dialect.get_unique_column_index_name(name, column.name)
                if not index_name:
                    return
                print(index_name)
                table._indexes[index_name] = md_index.Index.with_name(
                    index_name,
                    column,
                    table_name=name,
                )


class TableMeta(type):
    """
    The metaclass for a table object. This represents the "type" of a table class.
    """

    def __prepare__(*args, **kwargs):
        return OrderedDict()

    def __new__(mcs, name: str, bases: tuple, class_body: dict,
                register: bool = True, *args, **kwargs):
        if register is False:
            return type.__new__(mcs, name, bases, class_body)

        # hijack columns
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

        #: A dict of columns for this table.
        self._columns = self._columns  # type: typing.Dict[str, md_column.Column]

        #: A dict of relationships for this table.
        self._relationships = self._relationships  # type: typing.Dict[str, md_relationship.Relationship]

        #: A dict of indexes for this table.
        self._indexes = self._indexes  # type: typing.Dict[str, md_index.Index]

        #: The primary key for this table.
        #: This should be a :class:`.PrimaryKey`.
        self._primary_key = self._calculate_primary_key()

        logger.debug("Registered new table {}".format(tblname))
        self.metadata.register_table(self)

    async def truncate(cls, *, cascade: bool = False):
        """
        Truncates this table.

        :param cascade: If this truncation should cascade to other tables.
        """
        async with cls._bind.get_session() as sess:
            return await sess.truncate(cls, cascade=cascade)

    def __getattr__(self, item):
        if item.startswith("_"):
            raise AttributeError("'{}' object has no attribute {}".format(self.__name__, item))

        col = self.get_column(item)
        if col is None:
            try:
                return next(filter(lambda tup: tup[0] == item, self._relationships.items()))[1]
            except StopIteration:
                raise AttributeError(item) from None
        else:
            return col

    @property
    def _bind(self):
        return self.metadata._bind

    @property
    def __quoted_name__(self):
        return '"{}"'.format(self.__tablename__)

    @property
    def columns(self) -> 'typing.List[md_column.Column]':
        """
        :return: A list of :class:`.Column` this Table has.
        """
        return list(self.iter_columns())

    def __repr__(self):
        try:
            return "<Table object='{}' name='{}'>".format(self.__name__, self.__tablename__)
        except AttributeError:
            return super().__repr__()

    def iter_relationships(self) -> 'typing.Generator[md_relationship.Relationship, None, None]':
        """
        :return: A generator that yields :class:`.Relationship` objects for this table.
        """
        for rel in self._relationships.values():
            yield rel

    def iter_columns(self) -> 'typing.Generator[md_column.Column, None, None]':
        """
        :return: A generator that yields :class:`.Column` objects for this table.
        """
        for col in self._columns.values():
            yield col

    def iter_indexes(self) -> 'typing.Generator[md_index.Index, None, None]':
        """
        :return: A generator that yields :class:`.Index` objects for this table.
        """
        for idx in self._indexes.values():
            yield idx

    @enforce_bound
    def explicit_indexes(self) -> 'typing.Generator[md_index.Index, None, None]':
        """
        :return: A generator that yields :class:`.Index` objects for this table.

        Only manually added indexes are yielded from this generator; that is, it
        ignores primary key indexes, unique column indexes, relationship indexes, etc
        """
        unique_idx_name = self._bind.dialect.get_unique_column_index_name
        pkey_name = self._bind.dialect.get_primary_key_index_name
        for index in self.iter_indexes():
            if index.name == pkey_name(self.__tablename__):
                continue
            elif index.name == unique_idx_name(self.__tablename__, next(index.get_column_names())):
                continue
            elif index.table_name != self.__tablename__:
                continue
            yield index

    def get_column(self, column_name: str, *,
                   raise_si: bool = False) -> 'typing.Union[md_column.Column, None]':
        """
        Gets a column by name.

        :param column_name: The column name to lookup.

            This can be one of the following:
                - The column's ``name``
                - The column's ``alias_name()`` for this table

        :return: The :class:`.Column` associated with that name, or None if no column was found.
        """
        try:
            return self._columns[column_name]
        except KeyError:
            for column in self._columns.values():
                alias = column.alias_name(table=self)
                if alias == column_name:
                    return column

        return None

    def get_relationship(self,
                         relationship_name) -> 'typing.Union[md_relationship.Relationship, None]':
        """
        Gets a relationship by name.

        :param relationship_name: The name of the relationship to get.
        :return: The :class:`.Relationship` associated with that name, or None if it doesn't exist.
        """
        try:
            return self._relationships[relationship_name]
        except KeyError:
            return None

    def get_index(self, index_name) -> 'typing.Union[md_index.Index, None]':
        """
        Gets an index by name.

        :param index_name: The name of the index to get.
        :return: The :class:`.Index` associated with that name, or None if it doesn't exist.
        """
        try:
            return self._indexes[index_name]
        except KeyError:
            return None

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

    @enforce_bound
    async def create(self):
        """
        Creates a table with this schema in the database.
        """
        sql = io.StringIO()
        sql.write("CREATE TABLE ")
        sql.write(self.__tablename__)

        primary_key_columns = []
        column_fields = []
        relationship_fields = []

        for column in self.iter_columns():
            column_fields.append(column.get_ddl_sql())
            if column.primary_key is True:
                primary_key_columns.append(column)

        sql.write("(\n    ")
        sql.write(",\n    ".join(column_fields))

        if primary_key_columns:
            pkey_text = "PRIMARY KEY ({})".format(
                ", ".join("{.name}".format(x) for x in primary_key_columns)
            )
            sql.write(",\n    ")
            sql.write(pkey_text)
        sql.write("\n)")

        unique_idx_name = self._bind.dialect.get_unique_column_index_name
        pkey_name = self._bind.dialect.get_primary_key_index_name
        for index in self.explicit_indexes():
            sql.write(';\n')
            sql.write(index.get_ddl_sql())
        sql.write(";")

        async with self._bind.get_session() as session:
            await session.execute(sql.getvalue())

    @enforce_bound
    async def drop(self, cascade: bool = False):
        """
        Drops this table, or a table with the same name, from the database.

        :param cascade: If this drop should cascade.
        """
        async with self._bind.get_ddl_session() as sess:
            await sess.drop_table(self.__tablename__, cascade=cascade)

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
        self.table = type(self)

        #: If this row existed before.
        #: If this is True, this row was fetched from the DB previously.
        #: Otherwise, it is a fresh row.
        self.__existed = False

        #: If this row is marked as "deleted".
        #: This means that the row cannot be updated.
        self.__deleted = False

        #: The session this row is attached to.
        self._session = None  # type: md_session.Session

        #: A mapping of Column -> Previous values for this row.
        #: Used in update generation.
        self._previous_values = {}

        #: A mapping of relationship -> rows for this row.
        self._relationship_mapping = collections.defaultdict(lambda: [])

        #: A mapping of Column -> Current value for this row.
        self._values = {}

        if kwargs:
            self._init_row(**kwargs)

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
    def _get_insert_sql(self, emitter: typing.Callable[[], str], session: 'md_session.Session'):
        """
        Gets the INSERT into statement SQL for this row.
        """
        if self._session is None:
            self._session = session

        q = io.StringIO()
        q.write("INSERT INTO {} ".format(self.table.__quoted_name__))
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
                name = emitter()
                param_name = session.bind.emit_param(name)
                # set the params to value
                # then add the {param_name} to the VALUES
                params[name] = value
                column_names.append(column.quoted_name)
                sql_params.append(param_name)

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

    def _get_update_sql(self, emitter: typing.Callable[[], str], session: 'md_session.Session'):
        """
        Gets the UPDATE statement SQL for this row.
        """
        if self._session is None:
            self._session = session

        params = {}
        base_query = io.StringIO()
        base_query.write("UPDATE {} SET ".format(self.table.__quoted_name__))
        # the params to "set"
        sets = []

        # first, get our row history
        history = md_inspection.get_row_history(self)
        # ensure the row actually has some history
        # otherwise, ignore it
        if not history:
            return None, None

        for col, d in history.items():
            # minor optimization
            if d["old"] == d["new"]:
                continue

            # get the next param from the counter
            # then store the name and the value in the row
            p = emitter()
            params[p] = d["new"]
            sets.append("{} = {}".format(col.quoted_name, session.bind.emit_param(p)))

        # ensure there are actually fields to set
        if not sets:
            return None, None

        base_query.write(", ".join(sets))

        wheres = []
        for col in self.table.primary_key.columns:
            # get the param name
            # then store it in the params counter
            # and build a new condition for the WHERE clause
            p = emitter()
            old = history[col]["old"]
            if old is not NO_VALUE:
                params[p] = old
            else:
                params[p] = history[col]["new"]
            wheres.append("{} = {}".format(col.quoted_name, session.bind.emit_param(p)))

        base_query.write(" WHERE ({});".format(" AND ".join(wheres)))

        return base_query.getvalue(), params

    def _get_delete_sql(self, emitter: typing.Callable[[], str], session: 'md_session.Session') \
            -> typing.Tuple[str, typing.Any]:
        """
        Gets the DELETE sql for this row.
        """
        if self._session is None:
            self._session = session

        query = io.StringIO()
        query.write("DELETE FROM {} ".format(self.table.__quoted_name__))
        # generate the where clauses
        wheres = []
        params = {}

        for col, value in zip(self.table.primary_key.columns,
                              md_inspection.get_pk(self, as_tuple=True)):
            name = emitter()
            params[name] = value
            wheres.append("{} = {}".format(col.quoted_fullname, session.bind.emit_param(name)))

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

    def get_old_value(self, column: 'md_column.Column'):
        """
        Gets the old value from the specified column in this row.
        """
        if column.table != self.table:
            raise ValueError("Column table must match row table")

        try:
            return self._previous_values[column]
        except KeyError:
            return NO_VALUE

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
                if default is NO_DEFAULT:
                    return None
                else:
                    return default
            else:
                return NO_VALUE

    def store_column_value(self, column: 'md_column.Column', value: typing.Any,
                           track_history: bool = True):
        """
        Updates the value of a column in this row.

        This will also update the history of the value, if applicable.
        """
        if self.__deleted:
            raise RuntimeError("This row is marked as deleted")

        if column not in self._previous_values and track_history:
            if column in self._values:
                self._previous_values[column] = self._values[column]

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
        schema.write("(Table):\n")
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
        db.bind_tables(Table)

        # now we can do queries
        sess = db.get_session()
        user = await sess.select(User).where(User.id == 2).first()

    Each Table object is associated with a database interface, which it uses for special querying
    inside the object, such as :meth:`.Table.get`.

    .. code-block:: python3

        class User(Table):
            id = Column(Integer, primary_key=True)
            ...

        db.bind_tables(Table)
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
