"""
Contains the DDL session object.
"""

import io

from asyncqlio.backends import mysql, postgresql

from asyncqlio.orm.schema import column as md_column, types as md_types
from asyncqlio.orm.session import SessionBase


class DDLSession(SessionBase):
    """
    A session for executing DDL statements in.
    """

    async def create_table(self, table_name: str,
                           *items: 'md_column.Column',
                           if_not_exists: bool = True):
        """
        Creates a table in this database.

        :param table_name: The name of the table.
        :param items: A list of items to add to the table (columns, indexes, etc).
        :param if_not_exists:
        """
        base = io.StringIO()
        base.write("CREATE TABLE ")
        if if_not_exists:
            base.write("IF NOT EXISTS ")

        base.write(table_name)

        # copy item names
        column_fields = []

        for i in items:
            # TODO: more items
            if isinstance(i, md_column.Column):
                column_fields.append(i.get_ddl_sql())

        # calculate primary key
        primary_key_cols = [col for col in items
                            if isinstance(col, md_column.Column) and
                            col.primary_key is True]
        # check to see if we actually have any, or if there was a special type used
        # e.g. for sqlite3
        if primary_key_cols:
            pkey_text = "PRIMARY KEY ({})".format(
                ", ".join("{.name}".format(x) for x in primary_key_cols)
            )
        else:
            pkey_text = ""

        # join it all up
        # this uses spacing to prettify the generated SQL a bit
        base.write("(\n    {}".format(",\n    ".join(column_fields)))
        if pkey_text:
            base.write(",\n    {}".format(pkey_text))

        base.write("\n);")

        return await self.execute(base.getvalue())

    async def drop_table(self, table_name: str, *,
                         cascade: bool = False):
        """
        Drops a table.

        :param table_name: The name of the table to drop.
        :param cascade: Should this drop cascade?
        """
        base = io.StringIO()
        base.write("DROP TABLE ")
        base.write(table_name)
        if cascade:
            base.write(" CASCADE;")
        else:
            base.write(";")

        return await self.execute(base.getvalue())

    async def add_column(self, table_name: str, column: 'md_column.Column'):
        """
        Adds a column to a table.

        :param table_name: The name of the table to add the column to.
        :param column: The column object to add to the table.
        """
        ddl = column.get_ddl_sql()
        base = "ALTER TABLE {} ADD COLUMN {};".format(table_name, ddl)

        return await self.execute(base)

    async def drop_column(self, table_name: str, column_name: str):
        """
        Drops a column in a table.

        :param table_name: The name of the table with the column.
        :param column_name: The name of the column to drop.
        """
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
