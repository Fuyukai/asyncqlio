"""
Declarative items allow the developer to declare models as classes, with class attributes defining the fields of the
underlying tables, for example:

.. code:: python

    class MyTable(Base):
        id = Column(int, primary_key=True)
        name = Column(String(32), nullable=False)
"""
from katagawa.declarative.column import Column
from katagawa.declarative.mapper import Mapper


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
        if "__tablename__" not in cls_dict:
            raise ValueError("Model {} has no `__tablename__` defined".format(name))
        # Add `__fields__` to the class dict.
        cls_dict["__fields__"] = {}
        # Locate all Column instances in the dict.
        for name, value in cls_dict.items():
            # Check if it's an instance of Column.
            if isinstance(value, Column):
                # Set the name of the column.
                value._name = name
                # Update it on `__fields__`.
                cls_dict["__fields__"][name] = value

        # create the new class.
        c = super().__new__(mcs, name, bases, cls_dict)

        return c

    def __init__(cls, name, bases, cls_dict: dict):
        """
        Called after the type is created.
        """
        # `cls` here is the class instance.
        # This means that we can set it on the mapper, with the name.
        cls.mapper[name] = cls

        # Then just call super().
        newcls = super().__init__(cls, bases, cls_dict)
        return newcls

    def __call__(cls, *args, **kwargs):
        """
        Calls the __init__ method of the declarative model, after doing some housework.
        """
        # Create a fresh new instance.
        new_instance = object.__new__(cls)
        # Define a new field mapper.
        # This is separate to the Mapper on the class, as it only defines the storage for fields on an instance.
        new_instance.__field_mapper__ = {}
        # Call the `__init__` method of the new class.
        new_instance.__init__(*args, **kwargs)

        return new_instance


def declarative_base():
    # Semi-voldemort type.
    # This is created with a Mapper instance specific to the Base.
    class Base(metaclass=DeclarativeMeta):
        """
        Class inherited from by models.
        """
        # The mapper used by this base.
        mapper = Mapper()
        # Stub field to prevent errors.
        __tablename__ = None
        # Stub field to indicate this class should have a __fields__ attribute.
        __fields__ = {}

    return Base
