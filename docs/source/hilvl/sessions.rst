.. jeff sessions
.. _sessions:

Sessions
========

Sessions are one of the key parts of interacting with the database. They provide a wrapper around
a transaction object, providing an API which uses table row instances and query objects to
interacting with the database connected to your application.

Creating a session
------------------

Creating a new :class:`.Session` that is bound to the current database interface is simple via the
usage of :meth:`.DatabaseInterface.get_session`.

.. code-block:: python3

    # create a session bound to our current database
    # this will automatically provide the ability to get transactions from the db
    sess = db.get_session()

    # alternatively, you can create your own sessions bound to the database interface
    # providng a custom subclass or so on
    session = Session(bind=db)

Using the session requires beginning it; behind the scenes this will acquire a new transaction
object from the current connector, and emit a BEGIN statement to start the transaction.

.. code-block:: python3

    # begin and connect the session
    await session.begin()

The session object also supports the ``async with`` protocol, meaning you can have it
automatically open and close without calling the begin/close methods.

.. code-block:: python3

    async with session:
        ...

    # or alternatively
    async with db.get_session() as sess:
        ...

Running SQL
------------

The most basic thing you can do with a session is to run some SQL code, using either
:meth:`.Session.execute` or :meth:`.Session.cursor`. The former is used for queries without a
result, the latter is used to execute and return a result.

For example, to fetch the result of the sum ``1 + 1``, you would use:

.. code-block:: python3

    cursor = await session.cursor("SELECT 1+1;")

This returns an instance of the the low-level object :class:`.BaseResultSet`. To fetch the
result, you can use :meth:`.BaseResultSet.fetch_row`:

.. code-block:: python3

    result = await cursor.fetch_row()
    answer = result["?column?"]  # postgres example
    answer = list(result.values())[0]  # or the list form for cross-db compatability


Inserting Rows
--------------

The session is the one-stop gateway to inserting, updating, or even deleting :ref:`Row Objects` .
There are several methods used: :meth:`.Session.add`, :meth:`.Session.merge`, and
:meth:`.Session.remove` are the high level methods.

 - :meth:`.Session.add` is used for new rows, or rows that have been retrieved from a query

 - :meth:`.Session.merge` is used for rows that already exist in the database

 - :meth:`.Session.remove` is used to delete rows that exist in the database.

For example, to add a user to the DB:

.. code-block:: python3

    u = User(id=1, name="heck")
    await session.add(u)

You can also update a user in the database as long as the row you're providing has a primary key,
and you use the ``merge`` method:

.. code-block:: python3

    u = User(id=1)
    u.name = "not heck"
    await session.merge(u)

Querying with the Session
-------------------------

See :ref:`querying` for an explanation of how to query using the session object.


.. autoclass:: asyncqlio.orm.session.Session
    :noindex:
    :members:

