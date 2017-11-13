"""
Classes for the history API.

.. currentmodule:: asyncqlio.orm.schema.history
"""
import abc
from typing import Any, Callable, Tuple

from asyncqlio.orm.operators import OperatorResponse
from asyncqlio.orm.schema import column as md_column


class ColumnChange(abc.ABC):
    """
    Represents a column change. This object stores the old value of a column, and the new value
    of a column, and can produce the SQL to update it accordingly.

    For example, a simple change could be:
    .. code-block:: python3

        row = await session.select.from_(User).where(User.id == 1).first()
        row.username = "admin_2"

    A new ColumnChange object is then stored in the row, ready to be retrieved for the SQL
    generator to use.

    Another change could be on an array type:
    .. code-block:: python3

        row = await session.select.from_(User).where(User.id == 1).first()
        row.some_array[0] = 1

    The history of a column on a row can be established with it's ``__history__`` attribute,
    which is a :class:`.ColumnHistory` object relating to the current history of said column.

    """
    def __init__(self, column: 'md_column.Column'):
        """
        :param column: The :class:`.Column` this change is for.
        """
        self.column = column

    @property
    @abc.abstractmethod
    def current_value(self) -> Any:
        """
        :return: The current value for the column, i.e. after the change.
        """

    @abc.abstractmethod
    def handle_change(self, before: Any, after: Any) -> None:
        """
        Handles a change.

        :param before: The value of the column before the change.
        :param after: The value of the column after the change.
        """

    @abc.abstractmethod
    def handle_change_with_history(self, previous: 'ColumnChange', new: Any) -> None:
        """
        Handles a change with history.

        The previous history object is passed to this object, and after processing it will be
        deleted.

        :param previous: The previous :class:`.ColumnChange`.
        :param new: The new value being added.
        """

    @abc.abstractmethod
    def get_update_sql(self, emitter: Callable[[], Tuple[str, str]]) -> 'OperatorResponse':
        """
        :return: The UPDATE SQL (the part after the ``SET``) for this change.
        """


class ValueChange(ColumnChange):
    """
    Represents a basic value change on a column.
    """
    def __init__(self, column):
        super().__init__(column)

        #: The previous value of the column.
        self._previous = None

        #: The new value of the column.
        self._new = None

    @property
    def current_value(self):
        return self._new

    def handle_change(self, before: Any, after: Any):
        self._previous = before
        self._new = after

    def handle_change_with_history(self, previous: 'ColumnChange', new: Any):
        # we don't care about any intermediate changes
        self._previous = previous._previous
        self._new = new

    def get_update_sql(self, emitter: Callable[[], Tuple[str, str]]) -> 'OperatorResponse':
        emitted, name = emitter()
        sql = "{} = {}".format(self.column.quoted_fullname, emitted)
        return OperatorResponse(sql, {name: self._new})
