import collections
import functools
import inspect
import types
import typing

import asyncqlio.sentinels
from asyncqlio.orm import inspection as md_inspection, session as md_session
from asyncqlio.orm.schema import column as md_column, relationship as md_relationship, \
    table as md_table
from asyncqlio.sentinels import NO_DEFAULT, NO_VALUE


@functools.total_ordering
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

    def __repr__(self):
        gen = ("{}={}".format(col.name, self.get_column_value(col)) for col in self.table.columns)
        return "<{} {}>".format(self.table.__name__, " ".join(gen))

    def __getattr__(self, item: str):
        obb = self._resolve_item(item)
        return obb

    __hash__ = object.__hash__

    # TODO: Allow tables to override these methods.
    def __eq__(self, other):
        if not isinstance(other, TableRow):
            return NotImplemented

        if other.table != self.table:
            raise ValueError("Rows to compare must be on the same table")

        return self.primary_key == other.primary_key

    def __le__(self, other):
        if not isinstance(other, TableRow):
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

    def _get_insert_sql(self, emitter: typing.Callable[[], str], session: 'md_session.Session'):
        """
        Gets the INSERT into statement SQL for this row.
        """
        if self._session is None:
            self._session = session

        q = "INSERT INTO {} ".format(self.table.__quoted_name__)
        params = {}
        column_names = []
        sql_params = []

        for column in self.table.iter_columns():
            column_names.append(column.quoted_name)
            value = self.get_column_value(column)
            if value is NO_VALUE or (value is None and column.default is NO_DEFAULT):
                sql_params.append("DEFAULT")
            else:
                # emit a new param
                name = emitter()
                param_name = session.bind.emit_param(name)
                # set the params to value
                # then add the {param_name} to the VALUES
                params[name] = value
                sql_params.append(param_name)

        q += "({}) ".format(", ".join(column_names))
        q += "VALUES "
        q += "({}) ".format(", ".join(sql_params))
        # check if we support RETURNS
        if session.bind.dialect.has_returns:
            columns_to_get = []
            # always return every column
            # this allows filling in of autoincrement + defaults
            for column in self.table.iter_columns():
                columns_to_get.append(column)

            to_return = ", ".join(column.quoted_name for column in columns_to_get)
            q += " RETURNING {}".format(to_return)

        q += ";"
        return q, params

    def _get_update_sql(self, emitter: typing.Callable[[], str], session: 'md_session.Session'):
        """
        Gets the UPDATE statement SQL for this row.
        """
        if self._session is None:
            self._session = session

        params = {}
        base_query = "UPDATE {} SET ".format(self.table.__quoted_name__)
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

        base_query += ", ".join(sets)

        wheres = []
        for col in self.table.primary_key.columns:
            # get the param name
            # then store it in the params counter
            # and build a new condition for the WHERE clause
            p = emitter()
            params[p] = history[col]["old"]
            wheres.append("{} = {}".format(col.quoted_name, session.bind.emit_param(p)))

        base_query += " WHERE ({});".format(" AND ".join(wheres))

        return base_query, params

    def _get_delete_sql(self, emitter: typing.Callable[[], str], session: 'md_session.Session') \
            -> typing.Tuple[str, typing.Any]:
        """
        Gets the DELETE sql for this row.
        """
        if self._session is None:
            self._session = session

        query = "DELETE FROM {} ".format(self.table.__quoted_name__)
        # generate the where clauses
        wheres = []
        params = {}

        for col, value in zip(self.table.primary_key.columns,
                              md_inspection.get_pk(self, as_tuple=True)):
            name = emitter()
            params[name] = value
            wheres.append("{} = {}".format(col.quoted_fullname, session.bind.emit_param(name)))

        query += "WHERE ({}) ".format(" AND ".join(wheres))
        return query, params

    def _update_relationships(self, record: dict):
        """
        Updates relationship data for this row, storing any extra rows that are needed.
        
        :param record: The dict record of extra data to store. 
        """
        if self.__deleted:
            raise RuntimeError("This row is marked as deleted")

        if not self.table in self._relationship_mapping:
            self._relationship_mapping[self.table] = [self]

        buckets = {}
        for relationship in self.table.iter_relationships():
            # type: md_relationship.Relationship
            table = relationship.foreign_table
            if table not in buckets:
                buckets[table] = {}

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
                    buckets[table][actual_name] = value
                    # get rid of the record
                    # so it doesn't come around in the next relationship check
                    record.pop(cname)

        # store the new relationship data
        for table, subdict in buckets.items():
            row = table(**subdict)
            # ensure the row doesn't already exist with the PK
            try:
                next(filter(lambda r: r.primary_key == row.primary_key,
                            self._relationship_mapping[table]))
            except StopIteration:
                # only append if the row didn't exist earlier
                # i.e that the filter raised StopIteration
                self._relationship_mapping[table].append(row)
            else:
                row._session = self._session

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
            # but don't proxy column accesses or relationships
            # also, if they're __hidden__, don't proxy
            if not isinstance(item, (md_column.Column, md_relationship.Relationship)) \
                    and not hasattr(item, "__hidden__"):
                if hasattr(item, "__row_attr__"):
                    return item(self)

                if inspect.isfunction(item):
                    # bind it to ourselves, and return it
                    return types.MethodType(item, self)
                return item

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
                if default is asyncqlio.sentinels.NO_DEFAULT:
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
                lambda relationship: relationship.name == relation_name,
                self.table.iter_relationships()
            ))
        except StopIteration:
            raise ValueError("No such relationship '{}'".format(relation_name))

        rel = relation.get_instance(self, self._session)
        rel.set_rows(self._relationship_mapping[relation.foreign_table])
        rel._update_sub_relationships(self._relationship_mapping)
        return rel

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
