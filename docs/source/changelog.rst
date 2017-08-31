.. _changelog:

Changelog
=========

0.1.1
-----

 - Prevent ``getattr`` on relationships from causing massive recursion errors.

 - Separate parts of :class:`.Session` out into a new :class:`.SessionBase`.

 - Add new :class:`.DDLSession`, inherited from :class:`.SessionBase`.

 - Fix :meth:`.Sqlite3Transaction.execute` when no params are passed.

 - Add :meth:`.Session.truncate` to truncate tables. Falls back to DELETE FROM if it can't.

 - Add :meth:`.TableMeta.truncate` to truncate tables, which calls :meth:`.Session.truncate`.

 - Add :meth:`.BaseResultSet.flatten` to flatten a result set.

0.1.0 (released 2017-07-30)
---------------------------

 - Initial release.
