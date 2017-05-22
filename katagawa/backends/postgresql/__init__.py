# used for namespace packages
from pkgutil import extend_path

__path__ = extend_path(__path__, __name__)

from katagawa.backends.base import BaseDialect

DEFAULT_CONNECTOR = "asyncpg"


class PostgresqlDialect(BaseDialect):
    """
    The dialect for Postgres.
    """

    @property
    def has_checkpoints(self):
        return True

    @property
    def has_serial(self):
        return True

    @property
    def lastval_method(self):
        return "LASTVAL()"

    @property
    def has_returns(self):
        return True

    @property
    def has_ilike(self):
        return True
