"""
Common tokens.

Something like this is the end goal of the reverse tokenizer:

sql_statement = Select(subtokens=[
    Field("user.id"),
    From("user"),
    Where(subtokens=[
        Eq(Field("user.id"), 2)
    ])
])
"""
import abc
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
        # First, we want to append every "COLUMN" token.
        fields = self.consume_tokens("COLUMN")

        # Join the fetched fields with a `,`.
        generated += ", ".join([field.generate_sql() for field in fields])

        # Get the FROM clause.
        generated += " FROM "
        fr = self.consume_tokens("FROM")
        if len(fr) < 1:
            raise MissingTokenException("Missing any FROM clauses in select statement")

        generated += ", ".join([f.generate_sql(include_from=False) for f in fr])

        # Get any WHERE clauses.
        fr = self.consume_tokens("WHERE")
        if len(fr):
            # THere is a where clause, so add it to the generated.
            generated += " {}".format(fr[0].generate_sql())

        # Check if there is an ORDER_BY token.
        ob = self.consume_tokens("ORDERBY")
        if ob:
            # Only use one. There should only ever be one anyway
            ob = ob[0]

            generated += " ORDER BY {}".format(ob.generate_sql())

        # Check if there is a LIMIT token.
        ob = self.consume_tokens("LIMIT")
        if ob:
            ob = ob[0]
            generated += " {}".format(ob.generate_sql())

        # Return the generated SQL.
        return generated


class Column(Aliased):
    """
    Represents a column in a SELECT, or similar.
    """
    __slots__ = ("identifier", "alias",)

    @property
    def name(self):
        return "COLUMN"

    def generate_sql(self):
        """
        Generate the SQL for a column.

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
        base += '"' + self.identifier + '"'

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


class Limit(WithIdentifier):
    """
    Defines a LIMIT token.
    """

    @property
    def name(self):
        return "LIMIT"

    def generate_sql(self):
        return "LIMIT {}".format(self.identifier)


class Where(Token):
    """
    Defines a WHERE token.

    This is just a container for several equality tokens.
    """

    @property
    def name(self):
        return "WHERE"

    def generate_sql(self):
        # Check if all tokens are Operators.
        if not all(isinstance(token, (Operator, Or, And)) for token in self.subtokens):
            raise TypeError("All tokens in a Where() clause must be operators")

        # If subtokens >= 1, implicitly wrap in an OR.
        if len(self.subtokens) > 1:
            new_token = Or(subtokens=self.subtokens)
        else:
            # Use the first subtoken.
            new_token = self.subtokens[0]

        # Generate the SQL for that new_token.
        genned = new_token.generate_sql()

        # Add it to a WHERE clause.
        final = "WHERE {}".format(genned)

        return final


# region Operators

# These define common SQL operators.
class Or(Token):
    """
    Represents an OR token.
    """

    @property
    def name(self):
        return "OR"

    def generate_sql(self):
        return " OR ".join((token.generate_sql() for token in self.subtokens))


class And(Token):
    """
    Represents an AND token.
    """

    @property
    def name(self):
        return "AND"

    def generate_sql(self):
        return "(" + " AND ".join((token.generate_sql() for token in self.subtokens)) + ")"


class IsNull(Aliased):
    """
    A special type of operator that represents if a token is NULL or not.
    """

    def name(self):
        return "ISNULL"

    def generate_sql(self):
        return "{} IS NULL".format(self.alias)


class IsNotNull(Aliased):
    def name(self):
        return "ISNOTNULL"

    def generate_sql(self):
        return "{} IS NOT NULL".format(self.alias)


class Operator(Token):
    """
    The base class for an operator.

    An operator has three attributes - the field, the other value, and the actual operator itself.
    The field is, obviously, a field object. The value can be either a field or another column to compare along,
    useful for relationships (WHERE table1.field1 = table2.field2), etc.

    This base class implements the actual SQL emitting for you; you only need to define the operator and it will
    autogenerate the SQL.
    """

    def __init__(self, field: Column, value):
        self.field = field

        self.value = value

    @property
    def name(self):
        return "OPERATOR"

    @abc.abstractproperty
    def operator(self):
        """
        :return: The SQL operator that this represents; for example, the Eq() class will return `=` here.
        """

    def generate_sql(self):
        """
        Generates the SQL for this interaction.
        """
        # Check the type of the field object.
        # It should be a field.
        if not isinstance(self.field, Column):
            raise TypeError("Field in an operator must be a field")

        # Use the identifier for this field.
        field = self.field.identifier

        # Next, check if the value is a string or a field object.
        if isinstance(self.value, Column):
            value = self.field.identifier
        else:
            value = str(self.value)

        # Format the string.
        built = '{f} {op} {v}'.format(f=field, op=self.operator, v=value)

        # Return the built string.
        return built


class Eq(Operator):
    """
    Defines an equality operator.
    """

    @property
    def operator(self):
        return "="


class Lt(Operator):
    """
    Defines a less than operator.
    """

    @property
    def operator(self):
        return "<"


class Gt(Operator):
    @property
    def operator(self):
        return ">"


class Ne(Operator):
    @property
    def operator(self):
        return "<>"

# endregion
