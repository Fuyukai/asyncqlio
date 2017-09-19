"""
py.test configuration
"""
import asyncio
import os

import pytest

from asyncqlio import DatabaseInterface
from asyncqlio.orm.schema.table import table_base, Table
from asyncqlio.orm.schema.column import Column
from asyncqlio.orm.schema.types import Integer, Text


# global so it can be accessed in other fixtures
iface = DatabaseInterface(dsn=os.environ["ASQL_DSN"])


@pytest.fixture(scope="module")
async def db() -> DatabaseInterface:
    await iface.connect()
    yield iface
    await iface.close()


@pytest.fixture(scope="module")
async def table() -> Table:
    class Test(table_base()):
        id = Column(Integer(), primary_key=True)
        name = Column(Text())
        email = Column(Text())
    async with iface.get_ddl_session() as session:
        await session.create_table(Test.__tablename__,
                                   *Test.iter_columns())
    iface.bind_tables(Test)
    yield Test
    async with iface.get_ddl_session() as session:
        await session.drop_table(Test.__tablename__)


# override for a module scope
@pytest.fixture(scope="module")
def event_loop():
    return asyncio.get_event_loop()
