"""
Contains the DDL session object.
"""

from asyncqlio.orm.schema import column as md_column
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
        base = "CREATE TABLE "
        if if_not_exists:
            base += "IF NOT EXISTS "

        base += table_name

        # copy item names
        column_fields = []

        for i in items:
            # TODO: more items
            if isinstance(i, md_column.Column):
                c_base = "{} {}".format(i.name, i.type.sql())

                # add attributes as appropriate
                # TODO: More attributes
                if i.nullable and not i.primary_key:
                    c_base += " NULL"
                else:
                    c_base += " NOT NULL"

                if i.unique:
                    c_base += " UNIQUE"

                if i.foreign_key is not None:
                    fk_split = i.foreign_key._ddl_split_fk()
                    c_base += " REFERENCES {} ({})".format(*fk_split)

                column_fields.append(c_base)

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
        base += "(\n    {}".format(",\n    ".join(column_fields))
        if pkey_text:
            base += ",\n    {}".format(pkey_text)

        base += "\n);"

        return await self.execute(base)
