"""
Common tokens.

Something like this is the end goal of the reverse tokenizer:

sql_statement = SELECT(
    subtokens=[
        FIELD("table1.id", as="table1_id"),
        FROM("public.user", as="table1"),
        WHERE(
            subtokens=[
                EQ("public.user.id", 1)
            ]
        )
    ]
)

"""
import typing

from katagawa.exceptions import MissingTokenException
from katagawa.sql import Token, Aliased, WithIdentifier


class Select(Token):
    """
    The base token representing a SELECT token.
    """

    def name(self):
        return "SELECT"

    def generate_sql(self):
        """
        Generates the SQL for the SELECT token.

        As this is a base token, it should return the complete SQL for a full select statement.
        """
        # Set the SELECT as the base token.
        generated = "SELECT "

        # Filter in the subtokens.
        # First, we want to append every "FIELD" token.
        fields = self.consume_tokens("FIELD")

        # Join the fetched fields with a `,`.
        generated += ", ".join([field.generate_sql() for field in fields])

        # Get the FROM clause.
        generated += " FROM "
        fr = self.consume_tokens("FROM")
        if len(fr) < 1:
            raise MissingTokenException("Missing any FROM clauses in select statement")

        generated += ", ".join([f.generate_sql(include_from=False) for f in fr])

        # Check if there is an ORDER_BY token.
        ob = self.consume_tokens("ORDERBY")
        if ob:
            # Only use one. There should only ever be one anyway
            ob = ob[0]

            generated += " ORDER BY {}".format(ob.generate_sql())

        # Return the generated SQL.
        return generated


class Field(Aliased):
    """
    Represents a field in a SELECT, or similar.
    """
    __slots__ = ("identifier", "alias",)

    @property
    def name(self):
        return "FIELD"

    def generate_sql(self):
        """
        Generate the SQL for a field.

        If no alias is specified, it will just return the column name.
        Otherwise, it will return the column named aliased with AS.
        """
        if self.alias is None:
            return self.identifier

        else:
            return "{} AS {}".format(self.identifier, self.alias)


class From(Aliased):
    """
    Represents a FROM in a SELECT, or similar.
    """

    @property
    def name(self):
        return "FROM"

    def generate_sql(self, include_from=True):
        """
        Generates the SQL for a FROM clause.

        :param include_from: Include the `FROM` at the beginning? Useful for multiple tables.
        """
        if include_from:
            base = "FROM "
        else:
            base = ""
        base += self.identifier

        if self.alias is not None:
            base += " AS {}".format(self.alias)

        return base


class OrderBy(Token):
    """
    Defines an ORDER BY instruction.
    """
    def __init__(self, columns: typing.List[typing.Tuple[str, str]]):
        """
        :param columns: A list of two-param tuples:
            First param is the column to sort on, and the second the sorting method: 'ASC' or 'DESC'
        """
        self.columns = columns

    @property
    def name(self):
        return "ORDERBY"

    def generate_sql(self):
        return ", ".join(["{} {}".format(column, order.upper()) for (column, order) in self.columns])
