"""
Contains specific types for columns in Katagawa.

These types are specified in the Column constructor.

.. code:: python

    class MyModel(Base):
        __tablename__ = "my_model"

        id = katagawa.Column(katagawa.Integer)
        username = katagawa.Column(katagawa.String)
"""

import abc
import typing


class BaseType(abc.ABC):
    """
    Defines an abstract base class for a type.

    These types define the underlying SQL type that is used for this type, and the restraints that this type can have
    when adding values to it.
    """

    def __init__(self, *args, **kwargs):
        """
        Initializes the BaseType.

        The args and kwargs form here are required because some types (such as strings) may take in optional arguments.
        """
        pass

    @abc.abstractproperty
    def sql_type(self):
        """
        Gets the SQL type for this type.

        For example, the Integer type would signify an ``integer`` SQL type.
        :return: The SQL string that is used to define this type.
        """

    @abc.abstractproperty
    def check_type(self, arg: object):
        """
        Checks the type of the argument provided to ensure it is compatible with the database underneath.

        :param arg: The argument that is to be checked.
        :return: True if the argument is compatible, False if the argument isn't compatible.
        """


# Very basic SQL types.
class Integer(BaseType):
    """
    Defines a 32-bit integer type.
    """
    @property
    def sql_type(self):
        return "INTEGER"

    def check_type(self, arg: object):
            return isinstance(arg, int) and -2147483647 < arg <= 2147483647
