"""
Classes for query objects.
"""
import itertools
import typing

from katagawa.orm import session as md_session
from katagawa.orm.operators import BaseOperator


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

    def generate_sql(self) -> typing.Tuple[str, dict]:
        """
        Generates the SQL for this query. 
        """
        counter = itertools.count()

        # calculate the column names
        columns = [r'"{}"."{}"'.format(column.table.__tablename__, column.name)
                   for column in self.table.columns]

        # format the basic select
        fmt = "SELECT {} FROM {} ".format(','.join(columns), self.table.__tablename__)
        # format conditions
        params = {}
        c_sql = []
        for condition in self.conditions:
            # pass the condition offset
            condition_sql, name, val = condition.generate_sql(self.session.bind.emit_param, counter)
            if val is not None:
                # special-case
                # this means it's a coalescing token
                if isinstance(val, dict) and name is None:
                    params.update(val)
                else:
                    params[name] = val

            c_sql.append(condition_sql)

        # append the fmt with the conditions
        # these are assumed to be And if there are multiple!
        if c_sql:
            fmt += "WHERE {}".format(" AND ".join(c_sql))

        return fmt, params

    # Helper methods for natural builder-style queries
    def where(self, *conditions: BaseOperator):
        """
        Adds a WHERE clause to the query. This is a shortcut for :meth:`.add_condition`.
        
        .. code-block:: python
            sess.select(User).where(User.id == 1)
        
        :param conditions: The conditions to use for this WHERE clause.
        :return: This query.
        """
        for condition in conditions:
            self.add_condition(condition)

        return self

    def set_table(self, tbl) -> 'SelectQuery':
        """
        Sets the table to query on.
        
        :param tbl: The :class:`.Table` object to set. 
        :return: This query.
        """
        self.table = tbl
        return self

    def add_condition(self, condition: BaseOperator) -> 'SelectQuery':
        """
        Adds a condition to the query/
        
        :param condition: The :class:`.BaseOperator` to add.
        :return: This query.
        """
        self.conditions.append(condition)
        return self
