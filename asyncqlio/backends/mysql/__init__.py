from pkgutil import extend_path

from asyncqlio.backends.base import BaseDialect

__path__ = extend_path(__path__, __name__)

DEFAULT_CONNECTOR = "aiomysql"


class MysqlDialect(BaseDialect):
    """
    The dialect for MySQL.
    """

    @property
    def has_checkpoints(self):
        return True

    @property
    def has_serial(self):
        return True

    @property
    def lastval_method(self):
        return "LAST_INSERT_ID()"

    @property
    def has_returns(self):
        return False

    @property
    def has_ilike(self):
        # sigh
        return False
