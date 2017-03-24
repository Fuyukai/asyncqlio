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

    def __init__(self, subtokens: typing.List['Token'] = None):
        """
        :param subtokens: Any subtokens this token has.
        """
        if subtokens is None:
            subtokens = []

        self.subtokens = subtokens

    def __repr__(self):
        return self.generate_sql()

    def consume_tokens(self, name) -> typing.List['Token']:
        """
        Consumes tokens from the current subtokens and returns a new list of these tokens.

        This will remove the tokens from the current subtokens.

        :param name: The name of the token to consume.
        :return: A list of :class:`Token` that match the type.
        """
        returned = []
        for item in self.subtokens[:]:
            if item.name == name:
                returned.append(item)
                self.subtokens.remove(item)

        return returned

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


class WithIdentifier(Token, metaclass=abc.ABCMeta):
    """
    Class for a token that takes in an identifier, for example a column or a table.
    """

    __slots__ = ("identifier",)

    def __init__(self, identifier, subtokens: list = None):
        """
        :param identifier: The name of the column to use.
        :param subtokens: Any subtokens this token has.
        """
        super().__init__(subtokens)

        self.identifier = identifier


class Aliased(WithIdentifier, metaclass=abc.ABCMeta):
    """
    Class for an aliased token.

    This inherits WithIdentifier, as you need an identifier to use an alias.
    """

    __slots__ = ("identifier", "alias",)

    def __init__(self, identifier: str, subtokens: typing.List['Token'] = None, alias: str = None):
        """
        :param identifier: The identifier this token has.
        :param subtokens: Any subtokens this token has.
        :param alias: The alias this token has.
        """
        super().__init__(identifier, subtokens)

        self.alias = alias
