"""
Relationship helpers.
"""
import typing

from cached_property import cached_property

from asyncqlio.orm import query as md_query
from asyncqlio.orm.schema import column as md_column
from asyncqlio.sentinels import NO_VALUE
from asyncqlio.utils import iter_to_aiter


class ForeignKey(object):
    """
    Represents a foreign key object in a column. This allows linking multiple tables together
    relationally.
    
    .. code-block:: python
        
        class Server(Table):
            id = Column(Integer, primary_key=True, autoincrement=True)
            name = Column(String)
            
            owner_id = Column(Integer, foreign_key=ForeignKey("User.id")
    """

    def __init__(self, foreign_column: 'typing.Union[md_column.Column, str]'):
        """
        :param foreign_column: Either a :class:`.Column` representing the foreign column, or a str \
            in the format ``<table object name>.<column name>``.
        """
        #: The :class:`.Column` object this FK references.
        self.foreign_column = None  # type: md_column.Column

        # used to resolve a column later if we can't resolve it now
        if isinstance(foreign_column, md_column.Column):
            self.foreign_column = foreign_column
            self._f_name = None
        else:
            # we will check this later
            self._f_name = foreign_column

        #: The :class:`.Column` object this FK is associated with.
        self.column = None  # type: md_column.Column


class Relationship(object):
    """
    Represents a relationship to another table object.

    This object provides an easy, object-oriented interface to a foreign key relationship between 
    two tables, the left table and the right table.   
    The left table is the "parent" table, and the right table is the "child" table; effectively
    creating a one to many/many to one relationship between the two tables.
    
    To create a relationship, there must be a column in the child table that represents the primary
    key of a parent table; this is the foreign key column, and will be used to load the other table.
    
    .. code-block:: python
    
        class User(Table):
            # id is the primary key of the parent table
            id = Column(Integer, auto_increment=True)
            name = Column(String)
            
            # this is the relationship joiner; it uses id as the left key, and user_id as the right
            # this will create a join between the two tables
            inventory = Relationship(left=id, right="InventoryItem.user_id")
            
        class InventoryItem(Table):
            id = Column(BigInteger, auto_increment=True)
            
            # user_id is the "foreign key" - it references the column User.id
            user_id = Column(Integer, foreign_key=ForeignKey(User.id)
            
    Once created, the new relationship object can be used to iterate over the child objects, using
    ``async for``:
    
    .. code-block:: python
    
        user = await sess.select.from_(User).where(User.id == 1).first()
        async for item in user.inventory:
            ...
    
    By default, the relationship will use a SELECT query to load the items; this can be changed to
    a joined query when loading any table rows, by changing the ``load`` param.  
    The possible values of this param are:
    
        - ``select`` - Emits a SELECT query to load child items.
        - ``joined`` - Emits a join query to load child items.
        
    For all possible options, see :ref:`Relationship Loading`.
    
    """

    def __init__(self,
                 left: 'typing.Union[md_column.Column, str]',
                 right: 'typing.Union[md_column.Column, str]', *,
                 load: str = "select", use_iter: bool = True):
        """
        :param left: The left-hand column (the Column on this table) in this relationship.
        
        :param right: The right-hand column (the Column on the foreign table) in this relationship.
        
        :param load: The way to load this relationship.
            The default is "select" - this means that a separate select statement will be issued
            to iterate over the rows of the relationship.
            
            For all possible options, see :ref:`Relationship Loading`.
            
        :param use_iter: Should this relationship use the iterable format?
            This controls if this relationship is created as one to many, or as a many to one/one to
            one relationship.
        """
        #: The left column for this relationship.
        self.left_column = left

        #: The right column for this relationship.
        self.right_column = right

        #: The load type for this relationship.
        self.load_type = load

        #: If this relationship uses the iterable format.
        self.use_iter = use_iter

        #: The owner table for this relationship.
        self.owner_table = None

        #: The name of this relationship.
        self.name = None

        if self.use_iter is False:
            self.load_type = "joined"

    def __set_name__(self, owner, name):
        self.owner_table = owner
        self.name = name

    def __repr__(self):
        try:
            o_name = "{}.{}".format(self.our_column.table.__tablename__,
                                    self.our_column.name)
        except AttributeError:
            o_name = "<unknown>"

        try:
            f_name = "{}.{}".format(self.foreign_column.table.__tablename__,
                                    self.foreign_column.name)
        except AttributeError:
            f_name = "<unknown>"
        return "<Relationship '{}' <-> '{}'>".format(o_name, f_name)

    # right-wing logic
    @cached_property
    def our_column(self) -> 'md_column.Column':
        """
        Gets the local column this relationship refers to.
        """
        if self.left_column == self.owner_table:
            return self.left_column

        return self.right_column

    @cached_property
    def foreign_column(self) -> 'md_column.Column':
        """
        Gets the foreign column this relationship refers to.
        """
        if self.left_column.table == self.owner_table:
            return self.right_column

        return self.left_column

    @cached_property
    def foreign_table(self):
        return self.foreign_column.table

    @property
    def join_columns(self) -> typing.Tuple['md_column.Column', 'md_column.Column']:
        """
        Gets the "join" columns of this relationship, i.e the columns that link the two columns.
        """
        return self.our_column, self.foreign_column

    def get_instance(self, row: 'md_row.TableRow', session):
        """
        Gets a new "relationship" instance.
        """
        if self.load_type == "select":
            return SelectLoadedRelationship(self, row, session or row._session)
        elif self.load_type == "joined":
            if self.use_iter is False:
                return JoinLoadedOTORelationship(self, row, session or row._session)
            else:
                return JoinLoadedOTMRelationship(self, row, session or row._session)
        else:
            raise NotImplementedError("Unknown load type {}".format(self.load_type))


# Specific relationship types produced for TableRow objects.
class BaseLoadedRelationship(object):
    """
    Provides some common methods for specific relationship type subclasses.
    """

    def __init__(self, rel: 'Relationship', row: 'md_row.TableRow', session):
        """
        :param rel: The :class:`.Relationship` that lies underneath this object. 
        :param row: The :class:`.TableRow` this is being loaded from.
        :param session: The :class:`.Session` this object is attached to.
        """
        self.relationship = rel
        self.row = row
        self.session = session or row._session

    def _it_stored_rows(self):
        raise NotImplementedError

    async def _add_row(self, row: 'md_row.TableRow'):
        """
        An overridable method called when a row is added.
        """

    async def add(self, row: 'md_row.TableRow'):
        """
        Adds a row to this relationship.

        .. warning::
            This will run an immediate insert/update of this row; if the parent row for this 
            relationship is not inserted it will run an immediate insert on the parent.

        :param row: The :class:`.TableRow` object to add to this relationship.
        """
        if not self.row._TableRow__existed:
            # we need to insert the row for it to be ready
            # so we do that now
            self.row = await self.session.insert_now(row)

        # get the data that we're updating the foreign column on
        our_column = self.relationship.our_column
        data = self.row.get_column_value(our_column)
        # set said data on our row in the FK field
        f_column = self.relationship.foreign_column
        row.store_column_value(f_column, data)
        # insert/update row
        row = await self.session.add(row)
        await self._add_row(row)

        return row

    async def _remove_row(self, row: 'md_row.TableRow'):
        """
        An overridable method called when a row is removed. 
        """

    async def remove(self, row: 'md_row.TableRow'):
        """
        Removes a row from this query.
         
        .. warning::
            This will run an immediate UPDATE of this row to remove the foreign key.
        
        :param row: The :class:`.TableRow` object to remove from this relationship. 
        """
        f_column = self.relationship.foreign_column

        if row.get_column_value(f_column, return_default=False) is NO_VALUE:
            raise ValueError("The row '{}' is not in this relationship".format(row))

        row.store_column_value(f_column, None)
        # generates an update
        row = await self.session.add(row)
        await self._remove_row(row)

        return row

    def set_rows(self, rows: 'typing.List[md_row.TableRow]'):
        """
        Sets the rows for this relationship, if applicable.
        
        By default, this does nothing.
        """
        pass

    def _update_sub_relationships(self, mapping):
        """
        Updates relationship mappings for this relationship automatically, if applicable.
        """
        # don't bother doing it on select loaded relationships
        if isinstance(self, SelectLoadedRelationship):
            return

        for row in self._it_stored_rows():
            # check for each relationship
            for relationship in row.table.iter_relationships():
                if relationship.foreign_table not in mapping:
                    continue

                # check if the columns match
                rows = mapping[relationship.foreign_table]
                for nrow in rows:
                    if row.get_column_value(relationship.our_column) \
                            == nrow.get_column_value(relationship.foreign_column):

                        row._relationship_mapping[relationship.foreign_table].append(nrow)

    def __iter__(self):
        raise TypeError("This cannot be iterated over normally")

    def __anext__(self):
        raise TypeError("This is not an async iterator")


class SelectLoadedRelationship(BaseLoadedRelationship):
    """
    A relationship object that uses a separate SELECT statement to load follow-on tables.
    """

    def _it_stored_rows(self):
        raise NotImplementedError

    @property
    def query(self) -> 'md_query.SelectQuery':
        """
        Gets the query for this relationship, allowing further customization.  
        For example, to change the order of the rows returned:
        
        .. code-block:: python3
        
            async for child in parent.children.query.order_by(Child.age):
                ...
        """
        columns = self.relationship.join_columns
        query = md_query.SelectQuery(self.row._session)
        query.set_table(self.relationship.foreign_table)
        # owner column == non owner column
        query.add_condition(columns[1] == self.row.get_column_value(columns[0]))
        return query

    async def first(self):
        rows = await self._load()
        return await rows.next()

    def __await__(self):
        return self._load().__await__()

    def __aiter__(self):
        return self._load()

    async def _load(self):
        """
        Loads the rows for this session.
        """
        return await self.query.all()


@iter_to_aiter
class JoinLoadedOTMRelationship(BaseLoadedRelationship):
    """
    Represents a join-loaded one to many relationship.
    """

    def __init__(self, rel: 'Relationship', row: 'md_row.TableRow', session):
        """
        :param rel: The :class:`.Relationship` that lies underneath this object. 
        :param row: The :class:`.TableRow` this is being loaded from.
        :param session: The :class:`.Session` this object is attached to.
        """
        super().__init__(rel, row, session)

        self._row_storage = []

    def _it_stored_rows(self):
        return self._row_storage

    def __repr__(self):
        return "<JoinLoadedOTMRelationship {}>".format(repr(self._row_storage))

    def __iter__(self):
        return iter(self._row_storage)

    def _add_row(self, row: 'md_row.TableRow'):
        self._row_storage.append(row)

    def _remove_row(self, row: 'md_row.TableRow'):
        self._row_storage.remove(row)

    def set_rows(self, rows: 'typing.List[md_row.TableRow]'):
        self._row_storage = rows


class JoinLoadedOTORelationship(BaseLoadedRelationship):
    """
    Represents a joined one<-to->one relationship.
    """

    def __init__(self, rel: 'Relationship', row: 'md_row.TableRow', session):
        """
        :param rel: The :class:`.Relationship` that lies underneath this object. 
        :param row: The :class:`.TableRow` this is being loaded from.
        :param session: The :class:`.Session` this object is attached to.
        """
        super().__init__(rel, row, session)

        self._rel_row = None

    def _it_stored_rows(self):
        return [self._rel_row]

    def __repr__(self):
        return "<JoinLoadedOTORelationship row='{}'>".format(self._rel_row)

    def set_rows(self, rows: 'typing.List[md_row.TableRow]'):
        try:
            self._rel_row = next(iter(rows))
        except StopIteration:
            return

    def add(self, row: 'md_row.TableRow'):
        raise NotImplementedError("This method does not work on one to one relationships")

    def remove(self, row: 'md_row.TableRow'):
        raise NotImplementedError("This method not work on one to one relationships")

    def __getattr__(self, item):
        if self._rel_row is None:
            raise AttributeError("Cannot load from empty row")
        else:
            return getattr(self._rel_row, item)

    async def set(self, row: 'md_row.TableRow'):
        """
        Sets the row for this one-to-one relationship.

        .. warning::
            This will run an immediate insert/update of this row; if the parent row for this
            relationship is not inserted it will run an immediate insert on the parent.

        :param row: The :class:`.TableRow` to set.
        """
        # copied from the add() code

        if not self.row._TableRow__existed:
            # we need to insert the row for it to be ready
            # so we do that now
            self.row = await self.session.insert_now(row)

        # get the data that we're updating the foreign column on
        our_column = self.relationship.our_column
        data = self.row.get_column_value(our_column)
        # set said data on our row in the FK field
        f_column = self.relationship.foreign_column
        row.store_column_value(f_column, data)
        # insert/update row
        row = await self.session.add(row)
        self._rel_row = row
        return row
