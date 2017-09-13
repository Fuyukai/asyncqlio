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

0.1.0 (released 2017-07-30)
---------------------------

 - Initial release.
