"""
Contains the DDL session object.
"""

import itertools
import operator
import io
import re

from asyncqlio.backends import mysql, postgresql, sqlite3
from asyncqlio.exc import UnsupportedOperationException
from asyncqlio.orm.schema import column as md_column, types as md_types,\
    table as md_table, index as md_index
from asyncqlio.orm.session import SessionBase


class DDLSession(SessionBase):
    """
    A session for executing DDL statements in.
    """

    async def create_table(self, table_name: str,
                           *items: 'typing.Union[md_column.Column, md_index.Index]',
                           if_not_exists: bool = True):
        """
        Creates a table in this database.

        :param table_name: The name of the table.
        :param items: A list of items to add to the table (columns, indexes, etc).
        :param if_not_exists:
        """
        sql = io.StringIO()
        sql.write("CREATE TABLE ")
        if if_not_exists:
            sql.write("IF NOT EXISTS ")

        sql.write(table_name)

        # copy item names
        primary_key_columns = []
        foreign_key_columns = []
        column_fields = []
        indexes = []

        for i in items:
            # TODO: more items
            if isinstance(i, md_column.Column):
                column_fields.append(i.get_ddl_sql())
                if i.primary_key is True:
                    primary_key_columns.append(i)
                elif i.foreign_key is not None:
                    foreign_key_columns.append(i)
            elif isinstance(i, md_index.Index):
                indexes.append(i)
            else:
                raise TypeError("Cannot create a table with a {}".format(type(i)))

        # this uses spacing to prettify the generated SQL a bit
        sql.write("(\n    ")
        sql.write(",\n    ".join(column_fields))

        # check to see if we actually have any, or if there was a special type used
        # e.g. for sqlite3
        if primary_key_columns:
            sql.write(",\n    PRIMARY KEY (")
            sql.write(", ".join(col.name for col in primary_key_columns))
            sql.write(")")

        sql.write("\n);")

        for index in indexes:
            sql.write("\n")
            sql.write(index.get_ddl_sql())
            sql.write(";")

        return await self.execute(sql.getvalue())

    async def drop_table(self, table_name: str, *,
                         cascade: bool = False,
                         if_exists: bool = True):
        """
        Drops a table.

        :param table_name: The name of the table to drop.
        :param cascade: Should this drop cascade?
        :param if_exists: Should we should only attempt to drop tables that exist?
        """
        base = io.StringIO()
        base.write("DROP TABLE ")
        if if_exists:
            base.write("IF EXISTS ")
        base.write(table_name)
        if cascade:
            base.write(" CASCADE")
        base.write(";")

        return await self.execute(base.getvalue())

    async def rename_table(self, table_name: str, new_name: str):
        """
        Changes a table's name.

        :param table_name: The table to change the name of.
        :param new_name: The new name to give the table.
        """
        if isinstance(self.bind.dialect, (postgresql.PostgresqlDialect, sqlite3.Sqlite3Dialect)):
            fmt = "ALTER TABLE {} RENAME TO {};"
        elif isinstance(self.bind.dialect, mysql.MysqlDialect):
            fmt = "RENAME TABLE {} TO {};"
        else:
            raise UnsupportedOperationException
        return await self.execute(fmt.format(table_name, new_name))

    async def add_column(self, table_name: str, column: 'md_column.Column'):
        """
        Adds a column to a table.

        :param table_name: The name of the table to add the column to.
        :param column: The column object to add to the table.
        """
        ddl = column.get_ddl_sql()
        base = "ALTER TABLE {} ADD COLUMN {};".format(table_name, ddl)
        if isinstance(self.bind.dialect, sqlite3.Sqlite3Dialect):
            base = base.replace("NOT NULL", "NULL")

        return await self.execute(base)

    async def drop_column(self, table_name: str, column_name: str):
        """
        Drops a column in a table.

        :param table_name: The name of the table with the column.
        :param column_name: The name of the column to drop.
        """
        if isinstance(self.bind.dialect, sqlite3.Sqlite3Dialect):
            # sqlite3 is so good guys
            tmp_name = "tmp_modify_{}".format(table_name)
            columns = []
            for column in await self.get_columns(table_name):
                if column.name != column_name:
                    columns.append(column)
            col_names = ", ".join(col.name for col in columns)
            await self.create_table(tmp_name, *columns, *await self.get_indexes(table_name))
            await self.execute("insert into {} select {} from {}"
                               .format(tmp_name, col_names, table_name))
            await self.drop_table(table_name)
            await self.rename_table(tmp_name, table_name)
            return
        # actually use params here
        fmt = "ALTER TABLE {} DROP COLUMN {};".format(table_name, column_name)
        return await self.execute(fmt)

    # column alteration
    async def alter_column_type(self, table_name: str, column_name: str,
                                new_type: 'md_types.ColumnType'):
        """
        Alters the type of a column.

        :param table_name: The table with the column type in it.
        :param column_name: The name of the column to alter the type of.
        :param new_type: The new type to set.
        """
        if isinstance(self.bind.dialect, sqlite3.Sqlite3Dialect):
            # we're in for a a ride
            tmp_name = "tmp_modify_{}".format(table_name)
            columns = []
            if not isinstance(new_type, md_types.ColumnType):
                new_type = new_type()
            for column in await self.get_columns(table_name):
                if column.name == column_name:
                    column.type = new_type
                columns.append(column)
            await self.create_table(tmp_name, *columns, *await self.get_indexes(table_name))
            await self.execute("insert into {} select * from {}".format(tmp_name, table_name))
            await self.drop_table(table_name)
            await self.rename_table(tmp_name, table_name)
            return
        fmt = io.StringIO()
        fmt.write("ALTER TABLE ")
        fmt.write(table_name)
        is_postgres = isinstance(self.bind.dialect, postgresql.PostgresqlDialect)
        if is_postgres:
            fmt.write(" ALTER ")
        elif isinstance(self.bind.dialect, mysql.MysqlDialect):
            fmt.write(" MODIFY ")
        else:
            raise RuntimeError("DB dialect does not support this action.")
        fmt.write(column_name)
        if is_postgres:
            fmt.write(" TYPE ")
        else:
            fmt.write(" ")
        fmt.write(new_type.sql())

        return await self.execute(fmt.getvalue())

    async def create_index(self, table_name: str, column_name: str, name: str,
                           *, unique: bool = False, if_not_exists: bool = False):
        """
        Creates an index on a column.

        :param table_name: The table with the column to be indexed.
        :param column_name: The name of the column to be indexed.
        :param name: The name to give the index.
        :param unique: Whether the index should enforce unique values.
        :param if_not_exists: Whether to use IF NOT EXISTS when making index.
        """
        fmt = io.StringIO()
        fmt.write("CREATE ")
        if unique:
            fmt.write("UNIQUE ")
        fmt.write("INDEX ")
        fmt.write(name)
        if if_not_exists:
            fmt.write(" IF NOT EXISTS")
        fmt.write(" ON ")
        fmt.write(table_name)
        fmt.write("(")
        fmt.write(column_name)
        fmt.write(");")

        await self.execute(fmt.getvalue())

    async def add_foreign_key(self, table_name: str, column_name: str,
                              foreign_table: str, foreign_column: str):
        """
        :param table_name: The table to add a foreign key to.
        :param column_name: The column to make a foreign key.
        :param foreign_table: The table to reference with the foreign key.
        :param foreign_clolumn: The column to reference with the foreign key.
        """
        fmt = ("ALTER TABLE {} ADD FOREIGN KEY ({}) REFERENCES {} ({})"
               .format(table_name, column_name, foreign_table, foreign_coulumn))
        await self.execute(fmt)

    async def get_columns(self, table_name: str = None
                          ) -> 'typing.Generator[md_index.Column, None, None]':
        """
        Yields a :class:`.md_column.Column` for each column in the specified table,
        or for each column in the schema if no table is specified.

        These columns don't point to a :class:`.md_table.Table` since there
        might not be one, but accessing __name__ and __tablename__ of the column's
        table will still work as expected.

        :param table_name: The table to get indexes from, or all tables if omitted
        """
        params = {"table_name": table_name}
        emitter = self.bind.emit_param
        sql = self.bind.dialect.get_column_sql(table_name, emitter=emitter)
        cur = await self.transaction.cursor(sql, params)
        records = await cur.flatten()
        await cur.close()
        return self.bind.dialect.transform_rows_to_columns(*records, table_name=table_name)

    async def get_indexes(self, table_name: str = None
                          ) -> 'typing.Generator[md_index.Index, None, None]':
        """
        Yields a :class:`.md_index.Index` for each index in the specified table,
        or for each index in the schema if no table is specified.

        These indexes don't point to a :class:`.md_table.Table` since there
        might not be one, but they have a table_name attribute.

        :param table_name: The table to get indexes from, or all tables if omitted
        """
        params = {"table_name": table_name}
        emitter = self.bind.emit_param
        sql = self.bind.dialect.get_index_sql(table_name, emitter=emitter)
        cur = await self.transaction.cursor(sql, params)
        records = await cur.flatten()
        await cur.close()
        return self.bind.dialect.transform_rows_to_indexes(*records, table_name=table_name)
