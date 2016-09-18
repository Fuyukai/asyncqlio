"""
This file contains Katagawa exceptions.
"""


class KatagawaException(Exception):
    """
    The base Exception used by Katagawa.

    All exceptions are derived from this.
    """


class OperationalException(KatagawaException):
    """
    Similar to SQLAlchemy's operational error, this is a catch-all exception for when something internally in the
    database fails.
    """


class MissingTokenException(KatagawaException):
    """
    Raised when a token in the SQL generation is missing.
    """
