"""
Katagawa engines are the actual SQL connections behind the scenes. They actually emit the raw SQL to the database
server, and return the results produced.
"""

import importlib

import dsnparse
import logging

from katagawa.engine.base import BaseEngine

BASE_PATH = "katagawa.engine.backends"

logger = logging.getLogger("Katagawa.engine")


def create_engine(dsn: str, **kwargs) -> BaseEngine:
    """
    Creates an engine from the specified DSN.

    :param dsn: The DSN to use.
    :return: A new :class:`katagawa.engine.base.BaseEngine` that was created from the specified DSN.
    """
    parsed = dsnparse.parse(dsn)
    # Get the DB type and the name of the driver, if applicable.
    db = parsed.schemes[0]
    try:
        driver = parsed.schemes[1]
    except IndexError:
        driver = None

    if driver is None:
        # Import the backend to get the default driver.
        mod = importlib.import_module(BASE_PATH + ".{}".format(db))
        driver = getattr(mod, "__DEFAULT__")

    # Import the driver class.
    path = BASE_PATH + ".{}.{}".format(db, driver)
    logger.debug("Attempting to load database engine {}".format(path))

    # This will raise an ImportError/ModuleNotFoundError (3.6+) if it failed to import, so we don't care about error
    # handling here.
    imported = importlib.import_module(path)

    # Find a class that is a subclass of BaseEngine, and has the same `__module__` as the imported name.
    for i in dir(imported):
        item = getattr(imported, i)
        if issubclass(item, BaseEngine):
            if item.__module__ == path:
                break
    else:
        raise ImportError("Failed to load specified driver")

    # Initialize a new instance of the engine.
    engine = item(dsn, **kwargs)

    return engine