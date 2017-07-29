"""
py.test configuration
"""
import asyncio
import os

import pytest

from asyncqlio import DatabaseInterface

# set the iface as a global so it can be closed later
iface = DatabaseInterface()


@pytest.fixture(scope='module')
async def db() -> DatabaseInterface:
    await iface.connect(dsn=os.environ["ASQL_DSN"])
    return iface


# override for a module scope
@pytest.fixture(scope="module")
def event_loop():
    return asyncio.get_event_loop()

