"""
Tests methods of DDLSession.
"""

import pytest

from asyncqlio import DatabaseInterface

from asyncqlio.exc import DatabaseException

from asyncqlio.orm.schema.column import Column
from asyncqlio.orm.schema.types import Integer, String, Real

# mark all test_ functions as coroutines
pytestmark = pytest.mark.asyncio

table_name = "test"


async def test_create_table(db: DatabaseInterface):
    async with db.get_ddl_session() as sess:
        await sess.create_table(
            table_name,
            Column.with_name("id", Integer(), primary_key=True,),
            Column.with_name("username", String()),
            Column.with_name("email", String())
        )
    async with db.get_session() as sess:
        assert await sess.execute("select * from {}".format(table_name)) == "SELECT 0"


async def test_add_column(db: DatabaseInterface):
    async with db.get_ddl_session() as sess:
        await sess.add_column(table_name,
                              Column.with_name("age", Integer()))
    async with db.get_session() as sess:
        result = await sess.fetch("select count(*) from information_schema.columns where table_name={param_0}",
                                  {'param_0': table_name})
        assert result['count'] == 4


async def test_drop_column(db: DatabaseInterface):
    async with db.get_ddl_session() as sess:
        await sess.drop_column('test', 'email')
    async with db.get_session() as sess:
        result = await sess.fetch("select count(*) from information_schema.columns where table_name={param_0}",
                                  {'param_0': table_name})
        assert result['count'] == 3


async def tset_alter_column_type(db: DatabaseInterface):
    async with db.get_ddl_session() as sess:
        await sess.alter_column_type('test', 'age', Real())
    async with db.get_session() as sess:
        await sess.execute("insert into {param_0} values (1, 'test', 1.5)",
                           {'param_0': table_name})
        result = await sess.fetch("select age from {param_0}", {"param_0": table_name})
        assert result['age'] == 1.5


async def test_drop_table(db: DatabaseInterface):
    async with db.get_ddl_session() as sess:
        await sess.drop_table("test")
    async with db.get_session() as sess:
        with pytest.raises(DatabaseException):
            await sess.execute("select * from test")