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

 - Add new :class:`.Index` representing an index in a database. (:pr:`30`)

 - Add :meth:`.TableMeta.create` to create tables from :class:`.Table` classes. (:pr:`30`)

 - Add :meth:`.Table.generate_schema` to create a representative Python class from the table.
   (:pr:`30`)

 - Add :meth:`.DDLSession.get_indexes` to get :class:`.Index` objects from an existing database.
   (:pr:`30`)

 - Add :meth:`.DDLSession.create_index` to create an index on an existing database table.
   (:pr:`30`)

 - Support :class:`.Index` objects in :meth:`.DDLSession.create_table`.

 - Actually generate foreign keys upon table creation.

 - Add :class:`.Serial`, :class:`.BigSerial`, and :class:`.SmallSerial` types to support automatic
   incrementation. (:issue:`17`, :pr:`34`)

 - Add :class:`.UpsertQuery`. (:issue:`32`, :pr:`38`)

 - Change :meth:`.DatabaseInterface.emit_param` to globally keep track of the param counter,
   which simplifies a lot of operator code.


0.1.0 (released 2017-07-30)
---------------------------

 - Initial release.
