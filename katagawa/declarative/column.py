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
        """
        self._type = type_

        # Our name.
        # Set on us by the metaclass.
        self._name = ""

        # The default value.
        self.default = kwargs.pop("default", None)

        # If the value is nullable.
        self.nullable = kwargs.pop("nullable", False)

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
        item = instance.__mapper__.get(self._name, self.default)
        return item

    def __set__(self, instance, value):
        """
        Sets our value on an instance.
        """
        instance.__mapper__[self._name] = value
