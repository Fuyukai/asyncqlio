"""
Tests methods of DDLSession.
"""

import pytest

from asyncqlio import DatabaseInterface

from asyncqlio.backends import postgresql, mysql, sqlite3
from asyncqlio.exc import DatabaseException

from asyncqlio.orm.schema.column import Column
from asyncqlio.orm.schema.types import Integer, Text, Real

# mark all test_ functions as coroutines
pytestmark = pytest.mark.asyncio

table_name = "test"


async def get_num_indexes(db: DatabaseInterface) -> int:
    if isinstance(db.dialect, postgresql.PostgresqlDialect):
        query = ("select count(*) from pg_indexes where table_name = {}"
                 .format(db.connector.emit_param("table_name")))
    elif isinstance(db.dialect, mysql.MysqlDialect):
        query = ("select count(*) from information_schema.statistics where "
                 "table_schema in (select database() from dual)")
    else:
        raise RuntimeError
    params = {"table_name": table_name}
    async with db.get_session() as sess:
        res = await sess.fetch(query, params)
    return next(iter(res.values()))


async def get_num_columns(db: DatabaseInterface) -> int:
    if isinstance(db.dialect, (postgresql.PostgresqlDialect, mysql.MysqlDialect)):
        query = ("select count(*) from information_schema.columns where table_name={}"
                 .format(db.connector.emit_param("table_name")))
    else:
        raise RuntimeError
    params = {"table_name": table_name}
    async with db.get_session() as sess:
        res = await sess.fetch(query, params)
    return next(iter(res.values()))


async def test_create_table(db: DatabaseInterface):
    async with db.get_ddl_session() as sess:
        await sess.create_table(
            table_name,
            Column.with_name("id", Integer(), primary_key=True),
            Column.with_name("name", Text()),
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
        await sess.drop_column(table_name, "name")
    assert await get_num_columns(db) == 3


async def test_alter_column_type(db: DatabaseInterface):
    async with db.get_ddl_session() as sess:
        await sess.alter_column_type(table_name, "age", Real())
    async with db.get_session() as sess:
        await sess.execute("insert into {} values (1, 3.0, 1.5)".format(table_name))
        result = await sess.fetch("select age from {}".format(table_name))
        assert result['age'] == 1.5


async def test_create_index(db: DatabaseInterface):
    async with db.get_ddl_session() as sess:
        await sess.create_index(table_name, "balance", "index_balance")
    assert await get_num_indexes(db) == 2  # one exists for id already


async def test_create_unique_index(db: DatabaseInterface):
    async with db.get_ddl_session() as sess:
        await sess.create_index(table_name, "age", "index_age", unique=True)
    assert await get_num_indexes(db) == 3
    fmt = "insert into {} values ({{}}, 20, 10);".format(table_name)
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
