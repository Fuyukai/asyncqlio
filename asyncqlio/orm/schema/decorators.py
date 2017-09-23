"""
Decorator helpers for tables.
"""
import functools


def enforce_bound(func):
    """
    Enforces that a method on a :class:`.Table` cannot be used before the table
    is bound to database via :meth:.DatabaseInterface.bind_tables.

    .. versionadded:: 0.2.0
    """

    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        try:
            self.metadata.bind
        except AttributeError:
            raise RuntimeError("Table must be bound first.")
        return func(self, *args, **kwargs)

    return wrapper
