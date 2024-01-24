# -*- coding: utf-8 -*-
from unittest import skipIf
import os
import datetime
import time
import subprocess
from uuid import UUID

from prom import query
from prom.query import Query
from prom.compat import *
from prom.config import Schema, DsnConnection, Field

# needed to test postgres interface
try:
    from prom.interface.postgres import PostgreSQL
except ImportError:
    PostgreSQL = None

import prom
import prom.interface

from . import (
    IsolatedAsyncioTestCase,
    _BaseTestInterface,
)


@skipIf(
    PostgreSQL is None,
    "Skipping Postgres config because dependencies not installed"
)
class ConfigTest(IsolatedAsyncioTestCase):
    def test_configure_postgres(self):
        dsn = 'prom.interface.postgres.PostgreSQL://uname:passw@localhost/db'
        prom.configure(dsn)
        i = prom.get_interface()
        self.assertTrue(i is not None)

        dsn += '#postgres'
        prom.configure(dsn)
        i = prom.get_interface('postgres')
        self.assertTrue(i is not None)

        dsn = 'prom.interface.postgres.BogusSdjaInterface://host/dbname#bogus2'
        with self.assertRaises(AttributeError):
            prom.configure(dsn)

    def test_dsn(self):
        tests = [
            (
                "".join([
                    "prom.interface.postgres.PostgreSQL",
                    "://username:password@localhost:5000",
                    "/database?option=1&var=2#fragment",
                ]),
                {
                    'username': "username",
                    'interface_name': "prom.interface.postgres.PostgreSQL",
                    'database': "database",
                    'host': "localhost",
                    'port': 5000,
                    'password': "password",
                    'name': 'fragment',
                    'options': {
                        'var': 2,
                        'option': 1
                    }
                }
            ),
            (
                "prom.interface.postgres.PostgreSQL://localhost:5/database2",
                {
                    'interface_name': "prom.interface.postgres.PostgreSQL",
                    'database': "database2",
                    'host': "localhost",
                    'port': 5,
                }
            ),
            (
                "prom.interface.postgres.PostgreSQL://localhost/db3",
                {
                    'interface_name': "prom.interface.postgres.PostgreSQL",
                    'database': "db3",
                    'host': "localhost",
                }
            ),
            (
                "".join([
                    "prom.interface.postgres.PostgreSQL",
                    "://localhost",
                    "/db3?var1=1&var2=2&var3=true&var4=False#name",
                ]),
                {
                    'interface_name': "prom.interface.postgres.PostgreSQL",
                    'database': "db3",
                    'host': "localhost",
                    'name': "name",
                    'options': {
                        'var1': 1,
                        'var2': 2,
                        'var3': True,
                        'var4': False
                    }
                }
            ),
        ]

        for t in tests:
            c = DsnConnection(t[0])
            for attr, val in t[1].items():
                self.assertEqual(
                    val,
                    getattr(c, attr),
                    t[0],
                )


@skipIf(
    PostgreSQL is None,
    "Skipping Postgres test because dependencies not installed"
)
class InterfaceTest(_BaseTestInterface):

    interface_class = PostgreSQL

    async def test_table_persist(self):
        i = self.get_interface()
        s = self.get_schema()
        r = await i.has_table(str(s))
        self.assertFalse(r)

        r = await i.set_table(s)

        r = await i.has_table(str(s))
        self.assertTrue(r)

        # make sure it persists
        await i.close()
        i = self.get_interface()
        self.assertTrue(await i.has_table(str(s)))

    async def test_set_table_postgres(self):
        """test some postgres specific things"""
        i = self.get_interface()
        s = self.get_schema(
            _id=Field(int, autopk=True),
            four=Field(float, True, size=10),
            five=Field(float, True),
            six=Field(int, True, max_size=9000000000),
        )
        r = await i.set_table(s)
        d = {
            'four': 1.987654321,
            'five': 1.98765,
            'six': 4000000000,
        }
        pk = await i.insert(s, d)
        odb = await i.one(s, Query().is__id(pk))
        for k, v in d.items():
            self.assertEqual(v, odb[k])

#     def test_db_disconnect(self):
#         """make sure interface can recover if the db disconnects mid script
#         execution"""
#         self.skip_test("this test does not work with docker")
# 
#         i, s = self.get_table()
#         _id = self.insert(i, s, 1)[0]
#         d = i.get_one(s, Query().eq__id(_id))
#         self.assertLess(0, len(d))
# 
#         testdata.restart_service("postgresql")
# 
#         d = i.get_one(s, Query().eq__id(_id))
#         self.assertLess(0, len(d))
# 
#     def test_no_connection(self):
#         """this will make sure prom handles it gracefully if there is no
#         connection available ever"""
#         self.skip_test("this test does not work with docker")
# 
#         postgresql = testdata.stop_service("postgresql", ignore_failure=False)
#         time.sleep(1)
# 
#         try:
#             i = self.create_interface()
#             s = self.get_schema()
#             q = Query()
#             with self.assertRaises(prom.InterfaceError):
#                 i.get(s, q)
# 
#         finally:
#             postgresql.start()

    async def test_render_sql_eq(self):
        orm_class = self.get_orm_class(
            ts=Field(datetime.datetime, True),
            interface=self.get_interface()
        )

        fstr, fargs = orm_class.query.eq_ts(day=10).render(placeholder=True)
        self.assertTrue('EXTRACT(DAY FROM "ts") = %s' in fstr, fstr)
        self.assertEqual(10, fargs[0])

        q = orm_class.query.eq_ts(day=11, hour=12)
        fstr, fargs = q.render(placeholder=True)
        self.assertTrue('EXTRACT(DAY FROM "ts") = %s' in fstr, fstr)
        self.assertTrue('AND EXTRACT(HOUR FROM "ts") = %s' in fstr, fstr)
        self.assertEqual(11, fargs[0])
        self.assertEqual(12, fargs[1])

        fstr, fargs = orm_class.query.eq_ts(None).render(placeholder=True)
        self.assertTrue('"ts" IS NOT DISTINCT FROM %s' in fstr, fstr)

        fstr, fargs = orm_class.query.ne_ts(None).render(placeholder=True)
        self.assertTrue('"ts" IS DISTINCT FROM %s' in fstr, fstr)

        with self.assertRaises(KeyError):
            q = orm_class.query.is_ts(bogus=5)
            fstr, fargs = q.render(placeholder=True)

    async def test_render_sql_in(self):
        orm_class = self.get_orm_class(
            ts=Field(datetime.datetime, True),
            interface=self.get_interface()
        )

        fstr, fargs = orm_class.query.in_ts(day=10).render(placeholder=True)
        self.assertTrue('EXTRACT(DAY FROM "ts") IN (%s)' in fstr)

        q = orm_class.query.in_ts(day=[11, 13], hour=12)
        fstr, fargs = q.render(placeholder=True)
        self.assertTrue('EXTRACT(DAY FROM "ts") IN (%s, %s)' in fstr, fstr)
        self.assertTrue('AND EXTRACT(HOUR FROM "ts") IN (%s)' in fstr, fstr)
        self.assertEqual([11, 13, 12], fargs)

        with self.assertRaises(KeyError):
            q = orm_class.query.in_ts(bogus=5)
            fstr, fargs = q.render(placeholder=True)

    async def test_no_db_error(self):
        # we want to replace the db with a bogus db error
        i, s = self.get_table()
        config = i.config
        config.database = 'this_is_a_bogus_db_name'
        fields = {
            'foo': 1,
            'bar': 'v1',
        }
        with self.assertRaises(prom.InterfaceError):
            rd = await i.insert(s, fields)

    async def test_uuid_pk(self):
        i, s = self.get_table(
            _id=Field(UUID, True, autopk=True),
            foo=Field(int, True),
        )

        pk = await i.insert(s, {"foo": 1})
        self.assertEqual(36, len(str(pk)))

        d = dict(await i.one(s, query.Query().is__id(pk)))
        self.assertEqual(1, d["foo"])
        self.assertEqual(pk, d["_id"])

    async def test_invalid_text_repr(self):
        i = self.get_interface()
        s1 = self.get_schema(
            _id=Field(UUID, True, autopk=True),
            foo=Field(int),
        )
        s2 = self.get_schema(
            fk=Field(s1, True),
        )

        pk = await i.insert(s1, {"foo": 1})
        await i.insert(s2, {"fk": pk})

        # make sure psycopg2.errors.InvalidTextRepresentation doesn't get
        # misrepresented
        with self.assertRaises(prom.InterfaceError):
            o3 = await i.insert(s2, {"fk": "foo"})

    async def test_get_fields_postgres(self):
        i = self.get_interface()
        s = self.get_schema(
            _id=Field(UUID, True, pk=True),
            foo=Field(str, True, ignore_case=True),
        )

        await i.set_table(s)

        fields = await i.get_fields(s)
        self.assertTrue(fields["foo"]["ignore_case"])
        self.assertEqual(UUID, fields["_id"]["field_type"])

