"""
Useful metamagic classes, such as async ABCs.
"""

import inspect
from abc import ABCMeta


def make_proxy(name: str):
    """
    Makes a proxy object for magic methods.
    """

    def proxy(self, *args, **kwargs):
        item = self.__getattr__(name)
        return item(*args, **kwargs)

    return proxy


def proxy_to_getattr(*magic_methods: str):
    """
    Proxies a method to to ``__getattr__`` when it would not be normally proxied.

    This is used for magic methods that are slot loaded (``__setattr__`` etc.)

    :param magic_methods: The magic methods to proxy to getattr.
    """

    def _modify_type(obb):
        for item in magic_methods:
            setattr(obb, item, make_proxy(item))

        return obb

    return _modify_type


# Copied from https://github.com/dabeaz/curio/blob/master/curio/meta.py
# Copyright (C) David Beazley (Dabeaz LLC)
# This code is licenced under the MIT licence.

class AsyncABCMeta(ABCMeta):
    """
    Metaclass that gives all of the features of an abstract base class, but
    additionally enforces coroutine correctness on subclasses. If any method
    is defined as a coroutine in a parent, it must also be defined as a
    coroutine in any child.
    """

    def __init__(cls, name, bases, methods):
        coros = {}
        for base in reversed(cls.__mro__):
            coros.update((name, val) for name, val in vars(base).items()
                         if inspect.iscoroutinefunction(val))

        for name, val in vars(cls).items():
            if name in coros and not inspect.iscoroutinefunction(val):
                raise TypeError('Must use async def %s%s' % (name, inspect.signature(val)))
        super().__init__(name, bases, methods)


class AsyncABC(metaclass=AsyncABCMeta):
    pass


class AsyncInstanceType(AsyncABCMeta):
    """
    Metaclass that allows for asynchronous instance initialization and the
    __init__() method to be defined as a coroutine.   Usage:
    class Spam(metaclass=AsyncInstanceType):
        async def __init__(self, x, y):
            self.x = x
            self.y = y
    async def main():
         s = await Spam(2, 3)
         ...
    """

    @staticmethod
    def __new__(meta, clsname, bases, attributes):
        if '__init__' in attributes and not inspect.iscoroutinefunction(attributes['__init__']):
            raise TypeError('__init__ must be a coroutine')
        return super().__new__(meta, clsname, bases, attributes)

    async def __call__(cls, *args, **kwargs):
        self = cls.__new__(cls, *args, **kwargs)
        await self.__init__(*args, **kwargs)
        return self


class AsyncObject(metaclass=AsyncInstanceType):
    pass


...
# END COPIED CODE
