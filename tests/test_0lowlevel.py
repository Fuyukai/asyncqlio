"""
Tests the low-level API.
"""

import pytest

from asyncqlio import BaseTransaction, DatabaseInterface

# mark all test_ functions as coroutines
pytestmark = pytest.mark.asyncio


async def test_db_connected(db: DatabaseInterface):
    assert db.connected
    assert db.connector is not None


async def test_acquire_transaction(db: DatabaseInterface):
    tr = db.get_transaction()

    assert isinstance(tr, BaseTransaction)


async def test_transaction_use(db: DatabaseInterface):
    tr = db.get_transaction()
    await tr.begin()

    # this just ensures the connection doesn't error
    await tr.execute("SELECT 1 + 1;")
    await tr.rollback()
    await tr.close()

async def test_transaction_fetch_one(db: DatabaseInterface):
    tr = db.get_transaction()
    await tr.begin()

    cursor = await tr.cursor("SELECT 1 + 1;")
    async with cursor:
        row = await cursor.fetch_row()
    # rowdict
    assert row[0] == 2
    await tr.rollback()
    await tr.close()


async def test_transaction_fetch_multiple(db: DatabaseInterface):
    tr = db.get_transaction()
    await tr.begin()

    cursor = await tr.cursor('SELECT 1 AS result UNION ALL SELECT 2;')
    previous = 0
    async with cursor:
        async for row in cursor:
            assert row["result"] > previous
            previous = row["result"]

    await tr.rollback()
    await tr.close()


async def test_transaction_fetch_many(db: DatabaseInterface):
    tr = db.get_transaction()
    await tr.begin()

    cursor = await tr.cursor('SELECT 1 AS result UNION ALL SELECT 2;')
    async with cursor:
        rows = await cursor.fetch_many(n=2)

    assert rows[0]["result"] == 1
    assert rows[1]["result"] == 2

    await tr.rollback()
    await tr.close()
