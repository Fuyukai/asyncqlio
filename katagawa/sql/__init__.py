"""
SQL generators for Katagawa.
"""
import abc
import typing


class Token(abc.ABC):
    """
    Base class for a token.
    """

    __slots__ = ()

    def __init__(self, subtokens: typing.List['Token']):
        """
        :param subtokens: Any subtokens this token has.
        """
        self.subtokens = subtokens

    @abc.abstractproperty
    def name(self):
        """
        Returns the name of the token.

        This is a unique identifier, but is not always related to the actual SQL underneath it.
        """

    @abc.abstractmethod
    def generate_sql(self):
        """
        Generate SQL from this statement.
        :return: The generated SQL.
        """


class Aliased(Token):
    """
    Mixin class for an aliased token.
    """

    __slots__ = ("alias",)

    def __init__(self, subtokens: typing.List['Token'], alias: str):
        """
        :param subtokens: Any subtokens this token has.
        :param alias: The alias this token has.
        """
        super().__init__(subtokens)

        self.alias = alias
