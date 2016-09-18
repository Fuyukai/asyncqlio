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

    def __init__(self, subtokens: typing.List['Token']=None):
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


class Aliased(Token):
    """
    Mixin class for an aliased token.
    """

    __slots__ = ("alias",)

    def __init__(self, subtokens: typing.List['Token'], alias: str=None):
        """
        :param subtokens: Any subtokens this token has.
        :param alias: The alias this token has.
        """
        super().__init__(subtokens)

        self.alias = alias
