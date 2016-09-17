"""
Declarative items allow the developer to declare models as classes, with class attributes defining the fields of the
underlying tables, for example:

.. code:: python

    class MyTable(Base):
        id = Column(int, primary_key=True)
        name = Column(String(32), nullable=False)
"""
from katagawa.declarative.column import Column


class DeclarativeMeta(type):
    """
    The base metaclass for a Declarative class.

    This implements several utilities, such as the storage of a mapper onto the class which can be used to create a
    new object from the descriptors stored on the class.
    """

    def __new__(mcs, name, bases, cls_dict: dict):
        """
        Creates the new declarative class.
        """
        # First, locate all Column instances in the dict.
        for name, value in cls_dict.items():
            # Check if it's an instance of Column.
            if isinstance(value, Column):
                # TODO: Add to mapper
                # Set the name of the column.
                value._name = name

        # TODO: Flesh this out further.
        # Finally, create the new class.
        c = super().__new__(mcs, name, bases, cls_dict)

        return c

    def __call__(cls, *args, **kwargs):
        """
        Calls the __init__ method of the declarative model, after doing some housework.
        """
        # Create a fresh new instance.
        new_instance = object.__new__(cls)
        # Define a new mapper.
        new_instance.__mapper__ = {}
        # Call the `__init__` method of the new class.
        new_instance.__init__(*args, **kwargs)

        return new_instance


class Base(metaclass=DeclarativeMeta):
    """
    Class inherited from by models.
    """
