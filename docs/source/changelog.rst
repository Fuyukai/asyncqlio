.. _changelog:

Changelog
=========

0.2.0
-----

 - Prevent ``getattr`` on relationships from causing massive recursion errors.

 - Separate parts of :class:`.Session` out into a new :class:`.SessionBase`.

 - Add new :class:`.DDLSession`, inherited from :class:`.SessionBase`.

 - Fix :meth:`.Sqlite3Transaction.execute` when no params are passed.

 - Add :meth:`.Session.truncate` to truncate tables. Falls back to DELETE FROM if it can't.

 - Add :meth:`.TableMeta.truncate` to truncate tables, which calls :meth:`.Session.truncate`.

 - Add :meth:`.BaseResultSet.flatten` to flatten a result set.

 - Fix the ``aiomysql`` connector to use ANSI sql mode.

 - Add new :class:`.Index` representing an index in a database.

 - Add :meth:`.Table.create` to create tables from :class:`.Table` objects.

 - Add :meth:`.Table.generate_schema` to create a representative Python class from the table.

 - Add :meth:`.DDLSession.get_indexes` to get :class:`.Index` objects from an existing database.

 - Add :meth:`.DDLSesion.create_index` to create an index on an existing database table.

 - Support :class:`.Index` objects in :meth:`.DDLSession.create_table`.

 - Actually generate foreign keys upon table creation.

0.1.0 (released 2017-07-30)
---------------------------

 - Initial release.
