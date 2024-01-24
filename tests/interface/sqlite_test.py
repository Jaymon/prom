# -*- coding: utf-8 -*-
import os
import datetime

import prom
from prom.interface.sqlite import SQLite
from prom import query, InterfaceError
from prom.interface.sqlite import SQLite, DatetimeType
from prom.interface import configure, set_interface, get_interface
from prom.model import Orm
from prom.config import Field, DsnConnection
from prom.compat import *
from prom.query import Query

from . import (
    IsolatedAsyncioTestCase,
    _BaseTestInterface,
)


class ConfigTest(IsolatedAsyncioTestCase):
    def test_configure_sqlite(self):
        dsn = 'prom.interface.sqlite.SQLite:///path/to/db'
        i = configure(dsn)
        self.assertTrue(i.config.path)

    def test_dsn(self):
        tests = [
            (
                "prom.interface.sqlite.SQLite://../this/is/the/path",
                {
                    'interface_name': "prom.interface.sqlite.SQLite",
                    'host': None,
                    'database': '../this/is/the/path'
                }
            ),
            (
                "prom.interface.sqlite.SQLite://./this/is/the/path",
                {
                    'interface_name': "prom.interface.sqlite.SQLite",
                    'host': None,
                    'database': './this/is/the/path'
                }
            ),
            (
                "prom.interface.sqlite.SQLite:///this/is/the/path",
                {
                    'interface_name': "prom.interface.sqlite.SQLite",
                    'host': None,
                    'database': '/this/is/the/path'
                }
            ),
            (
                "prom.interface.sqlite.SQLite://:memory:#fragment_name",
                {
                    'interface_name': "prom.interface.sqlite.SQLite",
                    'host': ":memory:",
                    'database': "",
                    'name': 'fragment_name'
                }
            ),
            (
                "".join([
                    "prom.interface.sqlite.SQLite",
                    "://:memory:",
                    "?option=1&var=2#fragment_name",
                ]),
                {
                    'interface_name': "prom.interface.sqlite.SQLite",
                    'host': ":memory:",
                    'database': "",
                    'name': 'fragment_name',
                    'options': {
                        'var': 2,
                        'option': 1
                    }
                }
            ),
            (
                "prom.interface.sqlite.SQLite://:memory:",
                {
                    'interface_name': "prom.interface.sqlite.SQLite",
                    'host': ":memory:",
                    'database': "",
                }
            ),
            (
                "prom.interface.sqlite.SQLite:///db4",
                {
                    'interface_name': "prom.interface.sqlite.SQLite",
                    'host': None,
                    'database': "/db4",
                }
            ),
            (
                "prom.interface.sqlite.SQLite:///relative/path/to/db/4.sqlite",
                {
                    'interface_name': "prom.interface.sqlite.SQLite",
                    'host': None,
                    'database': "/relative/path/to/db/4.sqlite",
                }
            ),
            (
                "prom.interface.sqlite.SQLite:///abs/path/to/db/4.sqlite",
                {
                    'interface_name': "prom.interface.sqlite.SQLite",
                    'host': None,
                    'database': "/abs/path/to/db/4.sqlite",
                }
            ),
            (
                "".join([
                    "prom.interface.sqlite.SQLite",
                    ":///abs/path/to/db/4.sqlite",
                    "?var1=1&var2=2",
                ]),
                {
                    'interface_name': "prom.interface.sqlite.SQLite",
                    'host': None,
                    'database': "/abs/path/to/db/4.sqlite",
                    'options': {
                        'var1': 1,
                        'var2': 2
                    }
                }
            ),
            (
                "".join([
                    "prom.interface.sqlite.SQLite",
                    ":///abs/path/to/db/4.sqlite",
                    "?var1=1&var2=2#name",
                ]),
                {
                    'interface_name': "prom.interface.sqlite.SQLite",
                    'host': None,
                    'database': "/abs/path/to/db/4.sqlite",
                    'name': "name",
                }
            ),
            (
                "".join([
                    "prom.interface.sqlite.SQLite",
                    ":///abs/path/to/db/4.sqlite",
                    "?var1=1&var2=2#name",
                ]),
                {
                    'interface_name': "prom.interface.sqlite.SQLite",
                    'host': None,
                    'database': "/abs/path/to/db/4.sqlite",
                    'name': "name",
                    'options': {
                        'var1': 1,
                        'var2': 2
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


class DatetimeTypeTest(IsolatedAsyncioTestCase):
    def test_convert(self):
        s = "2020-03-25T19:34:05.00005Z"
        dt = DatetimeType.convert(s)
        self.assertEqual(50, dt.microsecond)
        self.assertEqual(
            "2020-03-25T19:34:05.000050Z",
            dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        )

        s = "2020-03-25T19:34:05.05Z"
        dt = DatetimeType.convert(s)
        self.assertEqual(50000, dt.microsecond)
        self.assertEqual(
            "2020-03-25T19:34:05.050000Z",
            dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        )

        s = "2020-03-25T19:34:05.0506Z"
        dt = DatetimeType.convert(s)
        self.assertEqual(50600, dt.microsecond)
        self.assertEqual(
            "2020-03-25T19:34:05.050600Z",
            dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        )

        s = "2020-03-25T19:34:05.050060Z"
        dt = DatetimeType.convert(s)
        self.assertEqual(50060, dt.microsecond)
        self.assertEqual(
            "2020-03-25T19:34:05.050060Z",
            dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        )

        s = "2020-03-25T19:34:05.000057Z"
        dt = DatetimeType.convert(s)
        self.assertEqual(57, dt.microsecond)
        self.assertEqual(
            "2020-03-25T19:34:05.000057Z",
            dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        )

        s = "2020-03-25T19:34:05.057Z"
        dt = DatetimeType.convert(s)
        self.assertEqual(57000, dt.microsecond)
        self.assertEqual(
            "2020-03-25T19:34:05.057000Z",
            dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        )

        s = "2020-03-25T19:34:05.057035Z"
        dt = DatetimeType.convert(s)
        self.assertEqual(57035, dt.microsecond)
        self.assertEqual(
            "2020-03-25T19:34:05.057035Z",
            dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        )


class InterfaceTest(_BaseTestInterface):

    interface_class = SQLite

    async def test_change_interface(self):
        """This is testing the actual interface, not the db connection or
        anything"""
        connection_name = "change-interface"
        path = self.get_file("inter1.db")
        dsn = "sqlite://{}#{}".format(path, connection_name)
        configure(dsn)

        interface = get_interface(connection_name)
        schema = self.get_schema()

        await interface.set_table(schema)
        self.assertTrue(
            await interface.has_table(schema)
        )
        self.assertTrue(interface.is_connected())

        path = self.get_file("inter2.db")
        dsn = "sqlite://{}#{}".format(path, connection_name)

        with self.assertRaises(ValueError):
            configure(dsn)

        await interface.close()
        configure(dsn)
        interface = get_interface(connection_name)
        self.assertFalse(
            await interface.has_table(schema)
        )

    async def test_create_path(self):
        i = self.get_interface()
        config = i.config

        d = self.create_dir()
        config.host = os.path.join(d, "create_path", "db.sqlite")

        await i.connect(config)
        self.assertTrue(i.is_connected())

    async def test_db_disconnect_1(self):
        """make sure interface can recover if the db disconnects mid script
        execution, SQLite is a bit different than postgres which is why this
        method is completely original"""
        i, s = await self.create_table()

        _id = (await self.insert(i, s, 1))[0]
        d = await i.one(s, Query().eq__id(_id))
        self.assertGreater(len(d), 0)

        await i._connection.close()
        self.assertTrue(i.is_connected())

        _id = (await self.insert(i, s, 1))[0]
        d = await i.one(s, Query().eq__id(_id))
        self.assertGreater(len(d), 0)

    async def test_db_disconnect_2(self):
        i = self.get_interface()
        async def callback(connection, **kwargs):
            if getattr(connection, "attempt", False):
                await connection.close()
                connection.attempt = False
            (await connection.cursor()).execute("SELECT true")

        async with i.connection() as connection:
            connection.attempt = True
            await i.execute(callback, connection=connection)

        async with i.connection() as connection:
            await i.execute(callback, connection=connection)

    async def test_unsafe_delete_table_strange_name(self):
        """this makes sure https://github.com/firstopinion/prom/issues/47 is
        fixed, the problem was table names weren't escaped and so if they
        started with a number or something like that SQLite would choke"""
        table_name = "1{}".format(self.get_ascii(32))
        i, s = await self.create_table(table_name=table_name)
        await self.insert(i, s, 5)

        r = await i.count(s)
        self.assertEqual(5, r)

        await i.unsafe_delete_table(table_name)

        r = await i.count(s)
        self.assertEqual(0, r)

        await i.unsafe_delete_table(table_name)
        self.assertFalse(await i.has_table(table_name))

        await i.unsafe_delete_tables()
        self.assertFalse(await i.has_table(table_name))

        await i.unsafe_delete_tables()
        self.assertFalse(await i.has_table(table_name))

    async def test_unsafe_delete_table_ref(self):
        i, s1 = await self.create_table()
        i, s2 = await self.create_table(
            interface=i,
            s1_id=Field(s1, True),
            s1_2_id=Field(s1, True),
        )

        self.assertTrue(await i.has_table(s1))
        await i.unsafe_delete_table(s1)
        self.assertFalse(await i.has_table(s1))

        await i.close()
        self.assertFalse(i.is_connected())
        self.assertTrue(await i.has_table(s2))
        await i.unsafe_delete_tables()
        self.assertFalse(await i.has_table(s2))

    async def test_in_memory_db(self):
        i, s = self.get_table()
        config = i.config
        config.database = ":memory:"

        await i.connect(config)
        self.assertTrue(i.is_connected())

        _id = (await self.insert(i, s, 1))[0]
        self.assertTrue(_id)

    async def test_get_fields_float(self):
        """I'm not completely sure what this is testing anymore but I'm sure it
        was a bug from some app that used ActiveRecord and I was trying to
        read the sqlite db that app produced using prom. This doesn't work
        as a postgres test because ZFLOAT isn't a valid field type"""
        sql = "CREATE TABLE ZFOOBAR (ZFLOAT FLOAT)"

        i = self.get_interface()
        await i.raw(sql, ignore_result=True)
        self.assertTrue(await i.has_table("ZFOOBAR"))

        fields = await i.get_fields("ZFOOBAR")
        self.assertEqual(float, fields["ZFLOAT"]["field_type"])

