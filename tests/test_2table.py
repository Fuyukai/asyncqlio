"""
Tests methods of Table.
"""

import pytest

from asyncqlio.db import DatabaseInterface
from asyncqlio.exc import DatabaseException

from asyncqlio.orm.schema.column import Column
from asyncqlio.orm.schema.index import Index
from asyncqlio.orm.schema.relationship import Relationship, ForeignKey
from asyncqlio.orm.schema.table import table_base as table_base
from asyncqlio.orm.schema.types import Integer, Text, String

# mark all test_ functions as coroutines
pytestmark = pytest.mark.asyncio

Table = table_base()

person_body = '''class Person(Table):
    id = Column(Integer(), primary_key=True)
    ssn = Column(Integer(), unique=True)
    name = Column(String(128))
    age = Column(Integer())
    idx_name = Index(name)
    idx_age = Index(age)
    cars = Relationship(left="Person.id", right="Car.owner_id")
'''


car_body = '''class Car(Table):
    id = Column(Integer(), primary_key=True)
    owner_id = Column(Integer(), foreign_key=ForeignKey("Person.ssn"))
    make = Column(String(32))
    model = Column(String(32))
    year = Column(Integer())
    idx_make_model = Index(make, model)
    idx_year = Index(year)
'''

class_bodies = [person_body, car_body]

for body in class_bodies:
    exec(body)

tables = [Person, Car]


async def test_create_table(db: DatabaseInterface):
    db.bind_tables(Table)
    for table in tables:
        await table.create()


async def test_indexes(db: DatabaseInterface):
    async with db.get_ddl_session() as sess:
        for table in tables:
            for index in await sess.get_indexes(table.__tablename__):
                assert table.get_index(index.name) is not None


async def test_generate_schema():
    for table, body in zip(tables, class_bodies):
        assert table.generate_schema() == body


async def test_drop_table():
    for table in tables:
        try:
            await table.drop(cascade=True)
        except DatabaseException:
            pass
