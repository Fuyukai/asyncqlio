"""
SQL generators for Katagawa.
"""
import abc
import typing


class Token(abc.ABC):
    """
    Base class for a token.
    """
    def __init__(self, subtokens: typing.List['Token']):
        """
        :param subtokens: Any subtokens this token has.
        """
        self.subtokens = subtokens

    @abc.abstractproperty
    def name(self):
        """
        Returns the name of the token.
        """

    @abc.abstractmethod
    def generate_sql(self):
        """
        Generate SQL from this statement.
        :return: The generated SQL.
        """
