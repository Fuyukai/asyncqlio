.. _connecting:

Connecting to your Database
===========================

When writing a application that uses a database, the first thing you need
to do is to connect to the database. This is achieved through the usage of
the :class:`.DatabaseInterface` provided by the library, using a
**Data Source Name**.

.. code-block:: python3

    db = DatabaseInterface("postgresql://myuser:mypassword@127.0.0.1:5432/db")

You can omit the DSN if you plan on providing it later.

The DSN
-------

Each part of the DSN represents something:

    - ``postgresql`` - The dialect this database is connecting to.
    - ``+asyncpg`` (implicit, not shown) - The driver used to connect.
    - ``myuser`` - The username to connect to the database through.
    - ``mypassword`` - The password for the user. This can be omitted if
      the user does not have a password.
    - ``127.0.0.1`` - The hostname or IP being connected to.
    - ``5432`` - The port being connected to. If omitted, this will use the
      default port.
    - ``db`` - The database name to load.

Opening the Connection
----------------------

Creating a database object does not actually connect to the database; for
that you must use :meth:`.DatabaseInterface.connect` to open a new
connection or connection pool for usage in the database.

.. code-block:: python3

    # if you specified the DSN earlier
    await db.connect()
    # otherwise
    await db.connect(dsn)

Once connected, you can do a test query to verify everything works:

.. code-block:: python3

    async with db.get_transaction() as t:
        print(await (await t.cursor("SELECT 1;")).fetch_row())


