"""
Inspection module - contains utilities for inspecting Table objects and Row objects.
"""
import typing

from asyncqlio.orm import session as md_session
from asyncqlio.orm.schema import table as md_table


def get_row_session(row: 'md_table.Table') -> 'md_session.Session':
    """
    Gets the :class:`.Session` associated with a :class:`.TableRow`.

    :param row: The :class:`.Table` instance to inspect.
    """
    return row._session


def get_pk(row: 'md_table.Table', as_tuple: bool = True):
    """
    Gets the primary key for a Table row.

    :param row: The :class:`.Table` instance to extract the PK from.
    :param as_tuple: Should this PK always be returned as a tuple?
    """
    pk = row.primary_key
    if as_tuple and not isinstance(pk, tuple):
        return pk,

    return pk


# marker methods
def _set_mangled(row: 'md_table.Table', name: str, mark: typing.Any):
    setattr(row, "_Table__{}".format(name), mark)
    return row


def _get_mangled(row: 'md_table.Table', name: str):
    return getattr(row, "_Table__{}".format(name))
