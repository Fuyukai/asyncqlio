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

from asyncqlio.exc import DatabaseException
from asyncqlio.sentinels import NO_DEFAULT
from asyncqlio.backends.base import BaseDialect
from asyncqlio.orm.schema import column as md_column
from asyncqlio.orm.schema import index as md_index
from asyncqlio.orm.schema import types as md_types

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

    def get_column_sql(self, table_name=None, *, emitter):
        sql = '''
SELECT columns.*, (
  SELECT COUNT(*)
  FROM information_schema.table_constraints
    AS constraints
  JOIN information_schema.constraint_column_usage
    AS usage
    ON constraints.constraint_name=usage.constraint_name
  WHERE constraints.constraint_type='PRIMARY KEY'
    AND constraints.table_name=columns.table_name
    AND usage.column_name=columns.column_name) AS primary_key
FROM information_schema.columns
  AS columns'''
        if table_name:
            sql += " WHERE columns.table_name={}".format(emitter("table_name"))
        return sql

    def get_index_sql(self, table_name=None, *, emitter):
        sql = "SELECT * FROM pg_indexes"
        if table_name:
            sql += (" WHERE tablename={}"
                    .format(emitter("table_name")))
        return sql

    def transform_rows_to_columns(self, *rows, table_name=None):
        for row in rows:
            table_name = row['table_name']
            column_name = row['column_name']
            primary_key = bool(row['primary_key'])
            nullable = row["is_nullable"]
            default = row["column_default"] or NO_DEFAULT
            psql_type = row["data_type"]

            if psql_type == "integer":
                real_type = md_types.Integer
            elif psql_type == "text":
                real_type = md_types.Text
            elif psql_type == "character varying":
                real_type = md_types.String
            elif psql_type == "smallint":
                real_type = md_types.SmallInt
            elif psql_type == "bigint":
                real_type = md_types.BigInt
            elif psql_type == "boolean":
                real_type = md_types.Boolean
            elif psql_type == "real":
                real_type = md_types.Real
            elif psql_type.startswith("timestamp"):
                real_type = md_types.Timestamp
            else:
                raise DatabaseException("Cannot parse type {}".format(psql_type))

            yield md_column.Column.with_name(
                name=column_name,
                type_=real_type(),
                table=table_name,
                nullable=nullable,
                default=default,
                primary_key=primary_key,
            )

    def transform_rows_to_indexes(self, *rows, table_name=None):
        for row in rows:
            groups = idx_regex.match(row["indexdef"]).groups()
            unique, name, table, columns = groups
            columns = columns.split(', ')
            index = md_index.Index.with_name(name, *columns, table=table, unique=unique)
            yield index
