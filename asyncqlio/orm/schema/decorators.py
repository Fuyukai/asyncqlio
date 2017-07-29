"""
Decorator helpers for tables.
"""
import functools

from asyncqlio.orm.schema import table as md_table


def row_attr(func):
    """
    Marks a function as a "row attribute" - something that resolves on a :class:`.TableRow` as well
    as the :class:`.Table` it is a member of.

    .. code-block:: python3

        class User(Table):
            ...

            @row_attr
            def full_name(self: TableRow):
                # this will resolve on TableRow.full_name
                return self.first_name + self.last_name

    This allows writing attributes that resolve from table rows.
    Otherwise, they are treated as regular functions that need to be called on a TableRow object.

    :param func: The function to decorate.
    :return: A wrapper function. The original function can be found via ``.__wrapped__``.
    """

    @functools.wraps(func)
    def wrapper(self):
        if not isinstance(self, md_table.Table):
            raise TypeError("This function cannot be resolved on a Table")

        return func(self)

    wrapper.__row_attr__ = True

    return wrapper


def hidden(func):
    """
    Marks a function as "hidden" - i.e the function can only be resolved on the :class:`.Table` it
    exists on, and **NOT** the :class:`.TableRow` associated with said table.
    """
    func.__hidden__ = True

    return func
