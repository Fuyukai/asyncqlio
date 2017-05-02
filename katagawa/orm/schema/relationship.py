"""
Relationship helpers.
"""
import typing

from katagawa.orm.schema import column as md_column


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
            
            owner_id = Column(Integer, foreign_key=ForeignKey("User.id")
            owner = Relationship(via=owner_id, load="join", use_list=False)
    """
    def __init__(self, via: 'typing.Union[md_column.Column, str]', *,
                 load: str = "select", use_iter: bool = True):
        """
        :param via: The column to load this relationship via.
        
            This can either be a :class:`.Column`, or a str in the format 
            ``<table object name>.<column name>``.
            
            This is **NOT** the foreign column.
        
        :param load: The way to load this relationship.
            The default is "select" - this means that a separate select statement will be issued
            to iterate over the rows of the relationship.
            
            For all possible options, see :ref:`Relationship Loading`.
            
        :param use_iter: Should this relationship use the iterable format?
            This controls if this relationship is created as one to many, or as a many to one/one to
            one relationship.
        """
        #: The via column to use.
        self.via_column = None

        if isinstance(via, str):
            self._via_name = via
        else:
            self._via_name = None
            self.via_column = via

        #: The load type for this relationship.
        self.load_type = load

        #: If this relationship uses the iterable format.
        self.use_iter = use_iter

    def __set_name__(self, owner, name):
        self.table = owner
        self.name = name
