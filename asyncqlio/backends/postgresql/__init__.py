"""
PostgreSQL backends.

.. currentmodule:: asyncqlio.backends.postgresql

.. autosummary::
    :toctree:

    asyncpg
"""

# used for namespace packages
from pkgutil import extend_path
import re

from asyncqlio.backends.base import BaseDialect
from asyncqlio.orm.schema import index as md_index

__path__ = extend_path(__path__, __name__)


DEFAULT_CONNECTOR = "asyncpg"

idx_regex = re.compile(
    r"CREATE( UNIQUE)? INDEX (\S+) ON (\S+).*\((.+)\)",
    flags=re.IGNORECASE,
)


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

    @property
    def has_default(self):
        return True

    @property
    def has_truncate(self):
        return True

    def get_primary_key_index_name(self, table_name):
        return "{}_pkey".format(table_name)

    def get_unique_column_index_name(self, table_name, column_name):
        return "{}_{}_key".format(table_name, column_name)

    def get_index_sql(self, table_name=None, *, emitter):
        sql = "SELECT * FROM pg_indexes"
        if table_name:
            sql += (" WHERE tablename={}"
                    .format(emitter("table_name")))
        return sql

    def transform_rows_to_indexes(self, *rows):
        for row in rows:
            groups = idx_regex.match(row["indexdef"]).groups()
            unique, name, table, columns = groups
            columns = columns.split(', ')
            index = md_index.Index.with_name(name, *columns, table_name=table, unique=unique)
            yield index
