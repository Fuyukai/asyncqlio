"""
Exceptions for Katagawa.
"""


class DatabaseException(Exception):
    """
    The base class for ALL exceptions.
    
    Catch this if you wish to catch any exception raised inside Katagawa.
    """


class IntegrityError(DatabaseException):
    """
    Raised when a column's integrity is not preserved (e.g. null or unique violations).
    """


class NoSuchColumnError(DatabaseException):
    """
    Raised when a non-existing column is requested.
    """
