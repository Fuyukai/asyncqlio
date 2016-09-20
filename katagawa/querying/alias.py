"""
Table aliases.
"""
from katagawa.declarative import DeclarativeMeta


class AliasedTable(object):
    """
    Represents an aliased table.

    This is a thin wrapper around a Declarative table, with a different name representing it.

    Aliases are most commonly used in multi self-joins, where you need to join a table to itself with multiple
    relationships.

    .. code:: python
        owner = AliasedTable(table=models.User, alias="owner")

    The aliased table can then be used normally in a query as if it was a table.
    """
    def __init__(self, table: DeclarativeMeta, alias: str):
        self.table = table
        self.alias = alias

