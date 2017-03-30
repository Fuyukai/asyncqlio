"""
Miscellaneous utilities used throughout the library.
"""


def quote_name(s: str, *, quote_char: str = '"') -> str:
    """
    Quotes a field or table name in a query.
    
    .. code-block:: python
    
        query = f"SELECT {quote_string('user.id')} FROM {table}"
    
    .. warning::
    
        Do **not** use this to escape user input.
    
    :param s: The string to quote.
    :param quote_char: The character to quote with.
    :return: A new :class:`str` that is quoted.
    """
    final = []
    spl = s.split(".")

    for item in spl:
        final.append('{0}{1}{0}'.format(quote_char, item))

    return " ".join(final)
