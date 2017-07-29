.. _tables_columns:

Tables and Columns
==================

Each database in the real database is represented by a table class in the Python land. These
table classes map virtual columns to real columns, allowing access of them in your code easily
and intuitively.

Each table class subclasses an instance of the **table base** - a special object that stores some
metadata about the current database the application is running. The function :func:`.table_base`
can be used to create a new table base object for subclassing.

.. code-block:: python3

    from asyncqlio import table_base
    Table = table_base()

Internally, this makes a clone of a :class:`.Table` object backed by the :class:`.TableMeta`
which is then used to customize your tables.

Defining Tables
---------------

Tables are defined by making a new class inheriting from your table base object, corresponding to
a table in the database.

.. code-block:: python3

    class Server(Table, table_name="server"):
        ...

Note that ``table_name`` is passed explicitly here - this is optional. If no table name is passed
a name will be automatically generated from the table name made all lowercase.

Table classes themselves are technically instances of :class:`.TableMeta`, and as such all the
methods of a TableMeta object are available on a table object.

.. autoclass:: asyncqlio.orm.schema.table.TableMeta
    :noindex:

Adding Columns
--------------

Columns on tables are represented by :class:`.Column` objects - these objects are strictly only
on the table classes and not the rows. They provide useful functions for query building and an
easy way to map data from a request onto a table.

Columns are added to table objects with a simple attribute setting syntax. To add a column to a
table, you only need to do this:

.. code-block:: python3

    class Server(Table, table_name="server"):
        id = Column(Int(), primary_key=True, unique=True)

In this example, a column called ``id`` is added to the table with the type :class:`~.types.Int`,
and is set to be a primary key and unique. Of course, you can name it anything and add a
different type; all that matters is that the object is a :class:`.Column`.

.. autoclass:: asyncqlio.orm.schema.column.Column
    :noindex:
    :members:
    :special-members:


Primary Keys
~~~~~~~~~~~~

Tables can have primary keys, which uniquely identify rows in a table, and are made up of from 1
to N columns in the table. Typically keys with multiple columns are known as
**compound primary keys**. For convenience, an object provides primary keys on table classes.

.. autoclass:: asyncqlio.orm.schema.table.PrimaryKey
    :noindex:
    :members:

    Primary keys will be automatically generated on a table when multiple columns are marked as
``primary_key`` in the constructor, but a :class:`.PrimaryKey` object can be constructed manually
and set on ``Table.primary_key``.

Column Types
~~~~~~~~~~~~

All columns in both SQL and Python have a type - the column type. This defines what data they
store, what operators they can use, and so on. In asyncqlio, the first parameter passed to a
column is its type; this gives it extra functionality and defines how it stores data passed to it
both from the user and the database.

For implementing your own types, see :ref:`creating-col-types`.

.. automodule:: asyncqlio.orm.schema.types
    :noindex:
    :members:

Row Objects
-----------

In asyncqlio, a row object is simply an instance of a :class:`.Table`. To create one, you can
call the table object (much like creating a normal instance of a class):

.. code-block:: python3

    row = User()

To provide values for the columns, you can pass keyword arguments to the constructor
corresponding with the names of the columns, like so:

.. code-block:: python3

    row = User(id=1, name="heck")


