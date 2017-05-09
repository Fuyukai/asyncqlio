"""
Relationship helpers.
"""
import typing

from cached_property import cached_property

from katagawa.orm.schema import column as md_column
from katagawa.orm.schema import row as md_row
from katagawa.orm import query as md_query


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
    
    .. code-block:: python
    
        class User(Table):
            id = Column(Integer, primary_key=True, autoincrement=True)
            
            servers = Relationship(via="Server.owner_id", load="select")
        
        class Server(Table):
            id = Column(Integer, primary_key=True, autoincrement=True)
            name = Column(String)
            
            owner_id = Column(Integer, foreign_key=ForeignKey("User.id"))
            owner = Relationship(via=owner_id, load="join", use_list=False)
    """

    def __init__(self, via: 'typing.Union[md_column.Column, str]', *,
                 load: str = "select", use_iter: bool = True):
        """
        :param via: The column to load this relationship via.
        
            This can either be a :class:`.Column`, or a str in the format 
            ``<table object name>.<column name>``.
        
        :param load: The way to load this relationship.
            The default is "select" - this means that a separate select statement will be issued
            to iterate over the rows of the relationship.
            
            For all possible options, see :ref:`Relationship Loading`.
            
        :param use_iter: Should this relationship use the iterable format?
            This controls if this relationship is created as one to many, or as a many to one/one to
            one relationship.
        """
        self.owner_table = None
        self.name = None  # type: str

        #: The via column to use.
        self.via_column = None  # type: md_column.Column

        if isinstance(via, str):
            self._via_name = via
        else:
            self._via_name = None
            self.via_column = via  # type: md_column.Column

        #: The load type for this relationship.
        self.load_type = load

        #: If this relationship uses the iterable format.
        self.use_iter = use_iter

        if self.use_iter is False:
            self.load_type = "joined"

    def __set_name__(self, owner, name):
        self.owner_table = owner
        self.name = name

    # right-wing logic
    @cached_property
    def foreign_column(self) -> 'md_column.Column':
        """
        Gets the foreign column this relationship refers to.
        """
        via = self.via_column
        if via.table == self.owner_table:
            # uh.
            return via.foreign_key.foreign_column

        return self.via_column

    @cached_property
    def foreign_table(self):
        return self.foreign_column.table

    @property
    def join_columns(self) -> typing.Tuple['md_column.Column', 'md_column.Column']:
        """
        Gets the "join" columns of this relationship, i.e the columns that link the two columns.
        """
        return self.via_column, self.via_column.foreign_column

    def get_instance(self, row: 'md_row.TableRow', session):
        """
        Gets a new "relationship" instance.
        """
        if self.load_type == "select":
            return SelectLoadedRelationship(self, row, session or row._session)
        else:
            raise NotImplementedError("Unknown load type {}".format(self.load_type))


# Specific relationship types produced for TableRow objects.

class SelectLoadedRelationship(object):
    """
    A relationship object that uses a separate SELECT statement to load follow-on tables.
    """

    def __init__(self, rel: 'Relationship', row: 'md_row.TableRow', session):
        """
        :param rel: The :class:`.Relationship` that lies underneath this object. 
        :param row: The :class:`.TableRow` this is being loaded from.
        """
        self.relationship = rel
        self.row = row
        self.session = session or row._session

    async def _load(self):
        """
        Loads the rows for this session.
        """
        columns = self.relationship.join_columns
        # this table is the table we're joining onto
        table = self.relationship.via_column.table
        query = md_query.SelectQuery(self.row._session)
        query.set_table(table)
        query.add_condition(columns[0] == self.row.get_column_value(columns[1]))
        return await query.all()

    def __iter__(self):
        raise NotImplementedError("This cannot be iterated over normally")

    def __aiter__(self):
        return self._load()

    def __anext__(self):
        raise NotImplementedError("This is not an async iterator")
