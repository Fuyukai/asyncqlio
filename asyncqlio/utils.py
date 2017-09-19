"""
Miscellaneous utilities used throughout the library.
"""
import collections.abc


class IterToAiter(collections.abc.Iterator, collections.abc.AsyncIterator):
    """
    Transforms an `__iter__` method into an `__aiter__` method.
    """

    def __init__(self, iterator: collections.abc.Iterator):
        self._it = iterator

    # magic methods
    def __iter__(self):
        return self

    def __next__(self):
        return self._it.__next__()

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return self.__next__()
        except StopIteration:
            raise StopAsyncIteration


def iter_to_aiter(type_):
    """
    Transforms a normal iterable type into an async iterable type.
    """

    def __aiter__(self):
        return IterToAiter(iter(self))

    type_.__aiter__ = __aiter__
    return type_


class Proxy(object):
    """
    Base class for a proxy object.

    Takes the object to proxy through as it's first argument, and proxies all getattr access to
    that object.
    """
    def __init__(self, to_proxy: object):
        self.obb = to_proxy

    def __getattr__(self, item):
        return getattr(self.obb, item)


def separate_statements(sql: str) -> str:
    """
    Separates a SQL script into individual statements.
    """
    start = idx =0
    quoted = False
    sql = " {} ".format(sql)  # padding to avoid IndexErrors
    while idx < len(sql):
        char = sql[idx]
        if not quoted:
            if char == ";":
                stmt = sql[start:idx].strip()
                if stmt:
                    yield stmt
                start = idx + 1
            quoted = char == "'"

        else:
            if char == "'":
                if sql[idx + 1] == "'":
                    idx += 1
                else:
                    quoted = False
        idx += 1

    stmt = sql[start:-1].strip()
    if stmt:
        yield stmt
