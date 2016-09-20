"""
Module containing the actual query
"""
import typing

from katagawa.declarative import DeclarativeMeta
from katagawa.exceptions import TableConflictException
from katagawa.querying.alias import AliasedTable


class BaseQuery(object):
    """
    A BaseQuery object is used to query the database with SELECT statements or otherwise.

    It is produced from a :meth:`katagawa.sessions.session.Session.query` and is used to actually query the database.
    """
    def __init__(self, session, **kwargs):
        """
        Creates a new BaseQuery.

        :param session: The session to bind this query to.
        """
        self.session = session

        # Define a dict of tables to access in this query.
        self.tables = {}

        # Define a list of conditions to generate in the SELECT.
        self.conditions = []

    def select(self, *tables: typing.List[typing.Union[DeclarativeMeta, AliasedTable]]):
        """
        Selects some tables to query.

        :param tables: A list of DeclarativeMeta or aliased tables to query.
        :return: Ourselves.
        """
        for table in tables:
            if isinstance(table, DeclarativeMeta):
                tbl = AliasedTable(table, table.__name__)
            elif isinstance(table, AliasedTable):
                tbl = table
            else:
                raise TypeError("Table should be a declarative table or an aliased table")

            if tbl.alias in self.tables:
                raise TableConflictException("Table {} already exists in query when selecting. Did you mean to "
                                             "alias it?")
            self.tables[tbl.alias] = tbl

        return self
