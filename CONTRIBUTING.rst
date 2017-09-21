Contributing
============

Contributions are very welcome; the ORM needs all it can to be improved in day to day usage.

Code Style
----------

asyncqlio has a strict code style that must be followed:

 - `PEP 8<https://www.python.org/dev/peps/pep-0008/>_` is to be followed with several exceptions:

    - The line length limit is 100 characters.

    - Docstring style is different

 - All docstrings must use double quotes, not single quotes.

 - Docstrings must always have a newline after the opened ``"""`` and before the closing ``"""``.
   This includes single line docstrings.

 - Absolute imports only. No relative imports are allowed.

 - When importing something inside the ORM, it is typically preferred to use module imports then
   access the attributes of those in type annotations and similar to prevent circular import
   crashes.

Type Annotations
----------------

Ideally, all code should be type annotated. This makes it easier for IDEs and static type
checkers to work.

Type annotations that reference ORM objects must be enclosed in single quotes; type annotations
to refer to other objects must not.
.. code-block:: python3

    # Good
    def assign(column: 'Column', value: str) -> 'Column':
        ...

    # Bad
    def assign(column: Column, value: str) -> Column: ...
    def assign(column: 'Column', value: 'str') -> 'Column': ...

Tests
-----

Unit tests are essential to ensure the library does not play up.

 - When submitting a new feature, all tests must pass and new tests must be added for the new
 feature.

 - When submitting a bugfix, ideally a regression test should be added if possible.

 - No older tests are to be modified unless they directly rely on a fixed bug or a changed feature.

When submitting a PR, tests will be ran automatically against all available dialects and drivers.
Coverage tests will also be ran.
For a PR to be merged, all tests must pass successfully, and project coverage must not decrease
by more than 1% (ideally, it should increase!).

Dialect Details
---------------

If adding a new feature that requires database-specific features, some workarounds should be
considered.

 - Add a dialect method that checks for the existence of the feature.

 - If the feature cannot be used on a dialect, several things can be done:

    - An emulated workaround can be implemented, using dialect methods, which attempts to
      recreate what would happen if the driver was different.

    - If this is not possible, only then can the feature be marked as NotImplemented and issue an
      error to the user.

If adding a new feature that has backend details specific to one database or driver, a dialect
method that smooths the differences from each driver should be added.

