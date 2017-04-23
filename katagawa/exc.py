"""
Exceptions for Katagawa.
"""


class DatabaseException(Exception):
    """
    The base class for ALL exceptions.
    
    Catch this if you wish to catch any exception raised inside Katagawa.
    """


class NoSuchColumnError(Exception):
    """
    Raised when a non-existing column is requested.
    """
