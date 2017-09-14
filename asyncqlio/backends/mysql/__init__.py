from pkgutil import extend_path
import itertools
import operator

from asyncqlio.backends.base import BaseDialect
from asyncqlio.orm.schema import index as md_index

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
    def has_default(self):
        return True

    @property
    def has_ilike(self):
        # sigh
        return False

    @property
    def has_truncate(self):
        return True

    def get_primary_key_index_name(self, table):
        return "PRIMARY"

    def get_unique_column_index_name(self, table_name, column_name):
        return column_name

    def get_index_sql(self, table_name=None, *, emitter):
        sql = ("SELECT * FROM information_schema.statistics WHERE "
               "table_schema IN (SELECT database() FROM dual)")
        if table_name:
            sql += "AND table_name={}".format(emitter("table_name"))
        return sql

    def transform_rows_to_indexes(self, *rows):
        for name, rows in itertools.groupby(rows, operator.itemgetter('INDEX_NAME')):
            columns = []
            for row in rows:
                columns.append(row['COLUMN_NAME'])
            unique = not row['NON_UNIQUE']
            table = row['TABLE_NAME']
            index = md_index.Index.with_name(name, *columns, table_name=table, unique=unique)
            index.table_name = table
            index.table = None
            yield index
