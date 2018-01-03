"""
Tests methods of Session.
"""

import pytest

from asyncqlio import DatabaseInterface
from asyncqlio.orm.schema.table import Table

# mark all test_ functions as coroutines
pytestmark = pytest.mark.asyncio

kwargs = {
    "name": "test{}",
    "email": "test{}@example.com",
}


async def test_insert(db: DatabaseInterface, table: Table):
    async with db.get_session() as sess:
        name = kwargs["name"]
        email = kwargs["email"]
        rows = []
        for i in range(50):
            rows.append(table(id=i, name=name.format(i), email=email.format(i)))
        await sess.insert.rows(*rows)


async def test_fetch(db: DatabaseInterface, table: Table):
    async with db.get_session() as sess:
        res = await sess.fetch('select * from {}'.format(table.__tablename__))
    for attr, value in kwargs.items():
        assert res[attr] == value.format(res["id"])


async def test_rollback(db: DatabaseInterface, table: Table):
    sess = db.get_session()
    try:
        await sess.start()
        await sess.execute('delete from {} where 1=1'.format(table.__tablename__))
        await sess.rollback()
    finally:
        await sess.close()
    async with db.get_session() as sess:
        res = await sess.cursor('select count(*) from test')
        assert (await res.fetch_row())[0] > 0


async def test_select(db: DatabaseInterface, table: Table):
    async with db.get_session() as sess:
        res = await sess.select(table).where(table.id == 1).first()
    for attr, value in kwargs.items():
        assert getattr(res, attr, object()) == value.format(res.id)


async def test_update(db: DatabaseInterface, table: Table):
    name = "test2"
    async with db.get_session() as sess:
        await sess.update(table).set(table.name, name).where(table.id > 10)
    async with db.get_session() as sess:
        results = await sess.select(table).where(table.id > 10).all()
        async for result in results:
            assert result.name == name


async def test_upsert(db: DatabaseInterface, table: Table):
    async with db.get_session() as sess:
        query = sess.insert.rows(table(id=1, name="upsert", email="notupdated"))
        query = query.on_conflict(table.id).update(table.name)
        await query.run()
    async with db.get_session() as sess:
        res = await sess.select(table).where(table.id == 1).first()
    assert table.name == "upsert"
    assert table.email != "notupdated"


async def test_upsert_multiple_constriants(db: DatabaseInterface, table: Table):
    idx_name = "{}_email_idx".format(table.__tablename__)
    async with db.get_ddl_session() as sess:
        await sess.create_index(table.__tablename__, idx_name, "email", "name", unique=True)
    for i in range(51, 53):
        async with db.get_session() as sess:
            query = sess.insert.rows(table(id=51, name="test1", email="test1@example.com"))
            query = query.on_conflict(table.name, table.email).nothing()
            await query.run()
    async with db.get_session() as sess:
        res = await sess.select(table).where(table.id == 52).first()
    assert res is None


async def test_merge(db: DatabaseInterface, table: Table):
    id_ = 100
    async with db.get_session() as sess:
        await sess.execute("insert into {} values ({}, 'test', '', 0, 0)"
                           .format(table.__tablename__, id_))
    async with db.get_session() as sess:
        await sess.merge(table(id=id_))


async def test_delete(db: DatabaseInterface, table: Table):
    async with db.get_session() as sess:
        await sess.delete(table).where(table.id == 1)
    async with db.get_session() as sess:
        res = await sess.select(table).where(table.id == 1).first()
    assert res is None


async def test_truncate(db: DatabaseInterface, table: Table):
    async with db.get_session() as sess:
        await sess.truncate(table)
    async with db.get_session() as sess:
        res = await sess.select(table).first()
    assert res is None


async def test_numeric_decimal(db: DatabaseInterface, table: Table):
    async with db.get_session() as sess:
        query = sess.insert.rows(table(id=110, name="numeric", email="", lat=12.010, lon=12.010))
        await query.run()
    async with db.get_session() as sess:
        res = await sess.select(table).where(table.id == 110).first()

    import decimal

    assert str(res.lat) == "12.010"
    assert str(res.lon) == "12.01"
