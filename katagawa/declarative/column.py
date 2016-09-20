from katagawa.sql.types import BaseType


class Column:
    """
    A column is a descriptor on a declarative class which is used to access the underlying value.

    When on an instance, it returns the actual value.
    When on a class, it's used for comparison operators.

    .. code:: python

        class SomeTable(Base):
            id = Column(int)

    """
    def __init__(self, type_: type, **kwargs):
        """
        :param type_: The type of the Column.

        :param default: The default value for this column.
            This is None by default.
        :param nullable: If this field is nullable or not.
            This is True by default.
        :param primary_key: If this field should be a primary key.
            This is False by default.
        :param autoincrement: If this field should autoincrement.
            This is False by default.
        :param unique: If this field is unique inside the database i.e no other row can have a column with the same
        value in it.
            This is False by default.
        """
        if not isinstance(type_, BaseType):
            raise TypeError("Column type must be a BaseType")
        self._type = type_

        # Our name.
        # Set on us by the metaclass.
        self._name = ""

        # The default value.
        self.default = kwargs.pop("default", None)

        # If the value is nullable.
        self.nullable = kwargs.pop("nullable", True)

        # If the value is a primary key.
        self.primary_key = kwargs.pop("primary_key", False)

        # Should this autoincrement?
        self.autoincrement = kwargs.pop("autoincrement")

        # The default value for this column.
        self.default = kwargs.pop("default", None)

        # Type check the default.
        if self.default is not None:
            if not self._type.check_type(self.default):
                raise TypeError("Default value `{}` is not compatible with type `{}`".format(
                    self.default, self._type.name
                ))

        # If this is unique.
        self.unique = kwargs.pop("unique", False)

    def __get__(self, instance, owner):
        """
        Gets our value, when on an instance.
        :param instance: The model instance.
        :param owner: The model class itself.
        :return: The underlying value of the item.
        """
        # Check if we're being accessed on the class.
        if instance is None:
            # Return ourself so we can be accessed on the class directly.
            return self
        # Get the mapper item that we refer to.
        item = instance.__field_mapper__.get(self._name, None)
        if item is None:
            # Set the default, if there is one.
            if self.default is not None:
                instance.__field_mapper__[self._name] = self.default
                item = self.default
        # Cast it using our type
        item = self._type.cast(item)
        return item

    def __set__(self, instance, value):
        """
        Sets our value on an instance.
        """
        if instance is None:
            # ???
            raise Exception("What are you doing")

        is_safe = self._type.check_type(value)
        if not is_safe:
            raise TypeError("Cannot insert `{}` into a column with type `{}`".format(value, self._type.name))

        instance.__mapper__[self._name] = value
