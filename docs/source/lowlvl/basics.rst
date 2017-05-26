.. _lowlvl_basics:

Low Level Basics
================

asyncqlio's low-level API is a database-agnostic SQL API that provides
developers the ability to execute SQL code without worrying about the
underlying driver.

.. code-block:: python

    from asyncqlio.db import DatabaseInterface

    # create the database object to connect to the server.
    db = DatabaseInterface("postgresql+asyncpg://joku@127.0.0.1/joku")

    async def main():
        # connect to the database with db.connect
        await db.connect()
        # create a transaction to execute sql inside of
        async with db.get_transaction() as trans:
            # run a query
            results: BaseResultSet = await trans.cursor("SELECT 1;")
            row = await results.fetch_row()  # row with 1

Transactions
============

Transactions are the way of executing queries without affecting the rest of
the database. **All** agnostic connections require the usage of a
transaction to execute SQL (it is possible to execute SQL purely on a
connection using the driver-specific API, but this is not supported).

The :class:`.BaseTransaction` object is used to abstract away Database API
transaction objects into a common format that can be used in every dialect.
To get a new transaction that is bound to the current connection, use
:meth:`.DatabaseInterface.get_transaction`:

.. code-block:: python

    # tr is a new transaction object
    tr: BaseTransaction = db.get_transaction()
    # this is connected to the current database's connections
    # and will execute on said connection

Transactions MUST be started before execution can happen; this can be achieved
with :meth:`.BaseTransaction.begin`.

.. code-block:: python

    # start the transaction
    # this will usually emit a BEGIN or START TRANSACTION command underneath
    await tr.begin()

SQL can be emitted in the transaction with the usage of
:meth:`.BaseTransaction.execute` and :meth:`.BaseTransaction.cursor`.

.. code-block:: python

    # update some data
    await tr.execute('UPDATE "user" SET level = 3 WHERE "user".xp < 1000')
    # select some rows
    rows = await tr.cursor('SELECT * FROM "user" WHERE level > 5')

:meth:`.BaseTransaction.cursor` returns rows from a select query in the
form of a :meth:`.BaseResultSet`. ResultSets can be iterated over
asynchronously with ``async for`` to select each dict-like row:

.. code-block:: python

    async for row in rows:
        print(row.keys(), row.values())

Once done with the transaction, you can commit it to flush the changes, or
you can rollback to revert any changes.

.. code-block:: python

    if all_went_good:
        # it all went good, save changes
        await tr.commit()
    else:
        # not all went good, rollback changes
        await tr.rollback()

Transactions support the ``async for`` protocol, which will automatically
begin and commit/rollback as appropriate.

.. autoclass:: asyncqlio.backends.base.BaseTransaction
    :members:
    :private-members:

.. autoclass:: asyncqlio.backends.base.BaseResultSet
    :members:
