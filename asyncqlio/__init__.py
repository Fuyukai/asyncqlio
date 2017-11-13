"""
Main package for asyncqlio - a Python 3.5+ async ORM built on top of asyncio.

.. currentmodule:: asyncqlio

.. autosummary::
    :toctree:

    db
    orm
    backends

    exc
    meta
"""

__author__ = "Laura Dickinson"
__copyright__ = "Copyright (C) 2017 Laura Dickinson"

__licence__ = "MIT"
__status__ = "Development"

from pkg_resources import DistributionNotFound, get_distribution

try:
    __version__ = get_distribution(__name__).version
except DistributionNotFound:
    # package is not installed
    pass

from asyncqlio.backends.base import BaseConnector, BaseDialect, BaseResultSet, BaseTransaction
# import helpers
from asyncqlio.db import DatabaseInterface
from asyncqlio.exc import *
from asyncqlio.orm.inspection import get_pk, get_row_session
# orm
from asyncqlio.orm.schema.column import Column
from asyncqlio.orm.schema.index import Index
from asyncqlio.orm.schema.relationship import ForeignKey, Relationship
from asyncqlio.orm.schema.table import Table, table_base
from asyncqlio.orm.schema.history import ColumnChange, ValueChange
from asyncqlio.orm.schema.types import BigInt, BigSerial, Boolean, ColumnType, Integer, Real, \
     Serial, SmallInt, SmallSerial, String, Text, Timestamp
from asyncqlio.orm.session import Session
