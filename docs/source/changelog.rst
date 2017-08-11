.. _changelog:

Changelog
=========

0.1.1
-----

 - Prevent ``getattr`` on relationships from causing massive recursion errors.

 - Separate parts of :class:`.Session` out into a new :class:`.SessionBase`.

 - Add new :class:`.DDLSession`, inherited from :class:`.SessionBase`.


0.1.0 (released 2017-07-30)
---------------------------

 - Initial release.
