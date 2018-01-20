"""
SQLite3 backends.

.. autosummary::
    :toctree:

    sqlite3
"""


import io
import re
from pkgutil import extend_path

from asyncqlio.exc import DatabaseException, UnsupportedOperationException
from asyncqlio.sentinels import NO_DEFAULT
from asyncqlio.backends.base import BaseDialect
from asyncqlio.orm.schema import column as md_column
from asyncqlio.orm.schema import index as md_index
from asyncqlio.orm.schema import types as md_types

__path__ = extend_path(__path__, __name__)

DEFAULT_CONNECTOR = "aiosqlite3"

find_col_expr = re.compile(r"\((.*)\)")


def _parse_numeric_params(t):
    m = re.search('.*\(([0-9]+)(?:,([0-9]+))?\)', t)
    return [int(v) if v is not None else 0 for v in m.groups()]


class Sqlite3Dialect(BaseDialect):
    """
    The dialect for SQLite3.
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

    @property
    def has_default(self):
        return False

    @property
    def has_truncate(self):
        return False

    @property
    def has_cascade(self):
        return False

    def get_primary_key_index_name(self, table_name):
        return ""

    def get_unique_column_index_name(self, table_name, column_name):
        return ""

    def get_column_sql(self, table_name=None, *, emitter):
        if table_name is None:
            raise UnsupportedOperationException("Sqlite3 can't get all columns in the schema")
        return "PRAGMA table_info({})".format(table_name)

    def get_index_sql(self, table_name=None, *, emitter):
        sql = "SELECT * FROM sqlite_master where type='index'"
        if table_name:
            sql += (" AND tbl_name={}"
                    .format(emitter("table_name")))
        return sql

    def get_upsert_sql(self, table_name, *, on_conflict_update=True):
        sql = io.StringIO()
        params = {"insert"}
        if on_conflict_update:
            params.update({"update", "where"})
            sql.write("UPDATE ")
            sql.write(table_name)
            sql.write(" SET {update} WHERE {where};\n")
        sql.write("INSERT OR IGNORE INTO ")
        sql.write(table_name)
        sql.write(" {insert};")

        return sql.getvalue(), params

    def transform_rows_to_columns(self, *rows, table_name):
        for row in rows:
            column_name = row["name"]
            primary_key = bool(row["pk"])
            nullable = not row["notnull"]
            default = row["dflt_value"] or NO_DEFAULT
            psql_type = row["type"]

            if psql_type == "INTEGER":
                real_type = md_types.Integer
            elif psql_type == "TEXT":
                real_type = md_types.Text
            elif psql_type.startswith("VARCHAR"):
                size = int(psql_type[psql_type.index("(") + 1:-1])
                real_type = md_types.String(size)
            elif psql_type == "SMALLINT":
                real_type = md_types.SmallInt
            elif psql_type == "BIGINT":
                real_type = md_types.BigInt
            elif psql_type == "BOOLEAN":
                real_type = md_types.Boolean
            elif psql_type == "REAL":
                real_type = md_types.Real
            elif psql_type == "TIMESTAMP":
                real_type = md_types.Timestamp
            elif psql_type.startswith("NUMERIC"):
                real_type = md_types.Numeric(*_parse_numeric_params(psql_type)[1:])
            elif psql_type.startswith("DECIMAL"):
                real_type = md_types.Numeric(*_parse_numeric_params(psql_type)[1:])
            else:
                raise DatabaseException("Cannot parse type {}".format(psql_type))

            yield md_column.Column.with_name(
                name=column_name,
                type_=real_type,
                table=table_name,
                nullable=nullable,
                default=default,
                primary_key=primary_key,
            )

    def transform_rows_to_indexes(self, *rows, table_name):
        for row in rows:
            name = row["name"]
            table = row["tbl_name"]
            sql = row["sql"]
            if sql is None:
                continue
            match = find_col_expr.match(sql)
            if match is not None:
                columns = match.groups()[0]
                columns = (name.trim() for name in columns.split(","))
            else:
                columns = ()
            yield md_index.Index.with_name(name, *columns, table=table)
