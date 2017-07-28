from pkgutil import extend_path

from asyncqlio.backends.base import BaseDialect

__path__ = extend_path(__path__, __name__)

DEFAULT_CONNECTOR = "sqlite3"


class Sqlite3Dialect(BaseDialect):
    """
    The dialect for sqlite3.
    """

    @property
    def has_checkpoints(self):
        return True

    @property
    def has_serial(self):
        return False

    @property
    def lastval_method(self):
        return "last_insert_rowid()"

    @property
    def has_returns(self):
        return False

    @property
    def has_ilike(self):
        return False
