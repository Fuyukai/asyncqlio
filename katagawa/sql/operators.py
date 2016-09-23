"""
Operators - stuff like `column = 'value'`, and similar.
"""
import abc

from katagawa.sql import Token
from katagawa.sql.dialects.common import Field


class Operator(Token):
    """
    The base class for an operator.

    An operator has three attributes - the field, the other value, and the actual operator itself.
    The field is, obviously, a field object. The value can be either a field or another column to compare along,
    useful for relationships (WHERE table1.field1 = table2.field2), etc.

    This base class implements the actual SQL emitting for you; you only need to define the operator and it will
    autogenerate the SQL.
    """

    def __init__(self, field: Field, value):
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
        if not isinstance(self.field, Field):
            raise TypeError("Field in an operator must be a field")

        # Use the alias for thie field, because it should be specified.
        # We don't want it to generate the SQL directly as it will want to do `name AS alias`, whereas we only want
        # the alias itself.
        # Alternatively, use `field.identifier` to use the raw identifier, as sometimes a field won't have an alias.
        field = self.field.alias or self.field.identifier

        # Next, check if the value is a string or a field object.
        if isinstance(self.value, Field):
            value = self.value.alias or self.field.identifier
        elif isinstance(self.value, str):
            value = self.value
        else:
            raise TypeError("Value in an operator must be a field or a string")

        # Format the string.
        built = '"{f}" {op} "{v}"'.format(f=field, op=self.operator, v=value)

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
