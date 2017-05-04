"""
Inspection module - contains utilities for inspecting Table objects and Row objects.
"""
import typing

import katagawa.sentinels
from katagawa.orm import session as md_session
from katagawa.orm.schema import row as md_row, column as md_column


def get_row_session(row: 'md_row.TableRow') -> 'md_session.Session':
    """
    Gets the :class:`.Session` associated with a :class:`.TableRow`.
    
    :param row: The :class:`.TableRow` to inspect.
    """
    return row._session


def get_row_history(row: 'md_row.TableRow') \
        -> 'typing.Dict[md_column.Column, typing.Dict[str, typing.Any]]':
    """
    Gets the history for the specified row.
    
    This returns a dict, indexed by Column, with values being another dict with `old` and `new` keys
    that represent the old and new values of the item.
    """
    d = {}
    for column in row.table.iter_columns():
        old_value = row._previous_values.get(column)
        new_value = row.get_column_value(column, return_default=False)

        if new_value is not katagawa.sentinels.NO_VALUE:
            d[column] = {"old": old_value, "new": new_value}

    return d
