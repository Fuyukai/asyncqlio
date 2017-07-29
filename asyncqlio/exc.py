"""
Exceptions for asyncqlio.
"""


class DatabaseException(Exception):
    """
    The base class for ALL exceptions.

    Catch this if you wish to catch any custom exception raised inside the lib.
    """


class SchemaError(DatabaseException):
    """
    Raised when there is an error in the database schema.
    """


class IntegrityError(DatabaseException):
    """
    Raised when a column's integrity is not preserved (e.g. null or unique violations).
    """


class OperationalError(DatabaseException):
    """
    Raised when an operational error has occurred.
    """


class NoSuchColumnError(DatabaseException):
    """
    Raised when a non-existing column is requested.
    """
