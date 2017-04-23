"""
Classes for query objects.
"""
from katagawa.orm import session as md_session


class SelectQuery(object):
    """
    Represents a SELECT query, which fetches data from the database.
    
    This is not normally created by user code directly, but rather as a result of a 
    :meth:`.Session.select` call.
    
    .. code-block:: python
        sess = db.get_session()
        async with sess:
            query = sess.select(User)  # query is instance of SelectQuery
            
    However, it is possible to create this class manually:
    
    .. code-block:: python
        query = SelectQuery(db.get_session()
        query.set_table(User)
        query.add_condition(User.id == 2)
        user = await query.first()
        
    """

    def __init__(self, session: 'md_session.Session'):
        """
        :param session: The session to bind to this query.
        """
        self.session = session

        #: The table being queried.
        self.table = None

        #: A list of conditions to fulfil.
        self.conditions = []

        # TODO: Order by, limit, etc

    def set_table(self, tbl) -> 'SelectQuery':
        """
        Sets the table to query on.
        
        :param tbl: The :class:`.Table` object to set. 
        :return: This query.
        """
        self.table = tbl
        return self

    def add_condition(self, condition) -> 'SelectQuery':
        """
        Adds a condition to the query/
        
        :param condition: The :class:`.BaseCondition` to add.
        :return: This query.
        """
