# used for namespace packages
from pkgutil import extend_path

__path__ = extend_path(__path__, __name__)

from katagawa.backends.base import BaseDialect

DEFAULT_CONNECTOR = "asyncpg"


class PostgresqlDialect(BaseDialect):
    """
    The dialect for Postgres.
    """
    def has_checkpoints(self):
        return True

    def has_serial(self):
        return True
