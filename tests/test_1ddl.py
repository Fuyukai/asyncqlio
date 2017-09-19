"""
Tests methods of DDLSession.
"""

import pytest

from asyncqlio import DatabaseInterface

from asyncqlio.backends import postgresql, mysql, sqlite3
from asyncqlio.exc import DatabaseException

from asyncqlio.orm.schema.column import Column
from asyncqlio.orm.schema.types import Integer, String, Real

# mark all test_ functions as coroutines
pytestmark = pytest.mark.asyncio

table_name = "test"


async def get_num_indexes(db: DatabaseInterface) -> int:
    count = 0
    async with db.get_ddl_session() as sess:
        for _ in await sess.get_indexes(table_name):
            count += 1
    return count


async def get_num_columns(db: DatabaseInterface) -> int:
    count = 0
    async with db.get_ddl_session() as sess:
        for _ in await sess.get_columns(table_name):
            count += 1
    return count


async def test_create_table(db: DatabaseInterface):
    async with db.get_ddl_session() as sess:
        await sess.create_table(
            table_name,
            Column.with_name("id", Integer(), primary_key=True),
            Column.with_name("name", String(128)),
            Column.with_name("balance", Real())
        )
    async with db.get_session() as sess:
        assert await sess.fetch("select * from {}".format(table_name)) is None


async def test_add_column(db: DatabaseInterface):
    async with db.get_ddl_session() as sess:
        await sess.add_column(table_name,
                              Column.with_name("age", Integer()))
    assert await get_num_columns(db) == 4


async def test_drop_column(db: DatabaseInterface):
    async with db.get_ddl_session() as sess:
        await sess.drop_column(table_name, "balance")
    assert await get_num_columns(db) == 3


async def test_alter_column_type(db: DatabaseInterface):
    async with db.get_ddl_session() as sess:
        await sess.alter_column_type(table_name, "age", Real())
    async with db.get_session() as sess:
        await sess.execute("insert into {} values (1, 'Drizzt Do''Urden', 1.5)".format(table_name))
        result = await sess.fetch("select age from {}".format(table_name))
        assert result['age'] == 1.5


async def test_create_index(db: DatabaseInterface):
    if isinstance(db.dialect, sqlite3.Sqlite3Dialect):
        num_indexes = 1  # sqlite3 does't index primary keys
    else:
        num_indexes = 2
    async with db.get_ddl_session() as sess:
        await sess.create_index(table_name, "name", "index_name")
    assert await get_num_indexes(db) == num_indexes


async def test_create_unique_index(db: DatabaseInterface):
    if isinstance(db.dialect, sqlite3.Sqlite3Dialect):
        num_indexes = 2  # sqlite3 does't index primary keys
    else:
        num_indexes = 3
    async with db.get_ddl_session() as sess:
        await sess.create_index(table_name, "age", "index_age", unique=True)
    assert await get_num_indexes(db) == num_indexes
    fmt = "insert into {} values ({{}}, 'test', 10);".format(table_name)
    async with db.get_session() as sess:
        await sess.execute(fmt.format(100))
        with pytest.raises(DatabaseException):
            await sess.execute(fmt.format(101))


async def test_drop_table(db: DatabaseInterface):
    async with db.get_ddl_session() as sess:
        await sess.drop_table(table_name)
    async with db.get_session() as sess:
        with pytest.raises(DatabaseException):
            await sess.execute("select * from {}".format(table_name))
