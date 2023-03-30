# -*- coding: utf-8 -*-
from __future__ import unicode_literals, division, print_function, absolute_import
import os
import datetime

import prom
from prom.interface.sqlite import SQLite
from prom import query, InterfaceError
from prom.interface.sqlite import SQLite, DatetimeType
from prom.interface import configure
from prom.model import Orm
from prom.config import Field, DsnConnection
from prom.compat import *
from prom.query import Query

from . import _BaseTestInterface, _BaseTestConfig, BaseTestCase, testdata


class ConfigTest(_BaseTestConfig):
    def test_configure_sqlite(self):
        dsn = 'prom.interface.sqlite.SQLite:///path/to/db'
        i = prom.configure(dsn)
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
                "prom.interface.sqlite.SQLite://:memory:?option=1&var=2#fragment_name",
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
                "prom.interface.sqlite.SQLite:///abs/path/to/db/4.sqlite?var1=1&var2=2",
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
                "prom.interface.sqlite.SQLite:///abs/path/to/db/4.sqlite?var1=1&var2=2#name",
                {
                    'interface_name': "prom.interface.sqlite.SQLite",
                    'host': None,
                    'database': "/abs/path/to/db/4.sqlite",
                    'name': "name",
                }
            ),
            (
                "prom.interface.sqlite.SQLite:///abs/path/to/db/4.sqlite?var1=1&var2=2#name",
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


class DatetimeTypeTest(BaseTestCase):
    def test_convert(self):
        s = "2020-03-25T19:34:05.00005Z"
        dt = DatetimeType.convert(s)
        self.assertEqual(50, dt.microsecond)
        self.assertEqual("2020-03-25T19:34:05.000050Z", dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ"))

        s = "2020-03-25T19:34:05.05Z"
        dt = DatetimeType.convert(s)
        self.assertEqual(50000, dt.microsecond)
        self.assertEqual("2020-03-25T19:34:05.050000Z", dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ"))

        s = "2020-03-25T19:34:05.0506Z"
        dt = DatetimeType.convert(s)
        self.assertEqual(50600, dt.microsecond)
        self.assertEqual("2020-03-25T19:34:05.050600Z", dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ"))

        s = "2020-03-25T19:34:05.050060Z"
        dt = DatetimeType.convert(s)
        self.assertEqual(50060, dt.microsecond)
        self.assertEqual("2020-03-25T19:34:05.050060Z", dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ"))

        s = "2020-03-25T19:34:05.000057Z"
        dt = DatetimeType.convert(s)
        self.assertEqual(57, dt.microsecond)
        self.assertEqual("2020-03-25T19:34:05.000057Z", dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ"))

        s = "2020-03-25T19:34:05.057Z"
        dt = DatetimeType.convert(s)
        self.assertEqual(57000, dt.microsecond)
        self.assertEqual("2020-03-25T19:34:05.057000Z", dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ"))

        s = "2020-03-25T19:34:05.057035Z"
        dt = DatetimeType.convert(s)
        self.assertEqual(57035, dt.microsecond)
        self.assertEqual("2020-03-25T19:34:05.057035Z", dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ"))


class InterfaceTest(_BaseTestInterface):
    @classmethod
    def create_interface(cls):
        return cls.find_interface(SQLite)

    def test_change_interface(self):
        """This is testing the actual interface, not the db connection or anything"""
        class InterTorm(Orm):
            connection_name = "change-interface"
            #connection_name = ""
            pass

        path = testdata.get_file("inter1.db")
        dsn = "sqlite://{}#{}".format(path, InterTorm.connection_name)
        configure(dsn)
        InterTorm.install()
        self.assertTrue(InterTorm.interface.has_table(InterTorm.table_name))

        path = testdata.get_file("inter2.db")
        dsn = "sqlite://{}#{}".format(path, InterTorm.connection_name)
        configure(dsn)
        self.assertFalse(InterTorm.interface.has_table(InterTorm.table_name))

    def test_field_datetime_type(self):
        s = self.get_schema(
            self.get_table_name(),
            foo=Field(datetime.datetime)
        )
        i = self.create_interface()
        i.set_table(s)

        foo = testdata.get_past_datetime().astimezone(datetime.timezone.utc)
        pk = i.insert(s, {"foo": foo})
        r = i.get_one(s, query.Query().eq__id(pk))
        self.assertEqual(foo, r["foo"])

    def test_get_fields_float(self):
        sql = "\n".join([
            "CREATE TABLE ZFOOBAR (",
            "ZPK INTEGER PRIMARY KEY,",
            "ZINTEGER INTEGER,",
            "ZFLOAT FLOAT,",
            "ZTIMESTAMP TIMESTAMP,",
            "ZVARCHAR VARCHAR)",
        ])

        i = self.create_interface()
        r = i.raw(sql, cursor_result=True)
        self.assertTrue(i.has_table("ZFOOBAR"))

        fields = i.get_fields("ZFOOBAR")
        self.assertEqual(float, fields["ZFLOAT"]["field_type"])

    def test_create_path(self):
        i = self.create_interface()
        config = i.config

        d = testdata.create_dir()
        config.host = os.path.join(d, "create_path", "db.sqlite")

        i.connect(config)
        self.assertTrue(i.connected)

    def test_db_disconnect_1(self):
        """make sure interface can recover if the db disconnects mid script execution,
        SQLite is a bit different than postgres which is why this method is completely
        original"""
        i, s = self.get_table()
        _id = self.insert(i, s, 1)[0]
        d = i.get_one(s, Query().eq__id(_id))
        self.assertGreater(len(d), 0)

        i._connection.close()

        _id = self.insert(i, s, 1)[0]
        d = i.get_one(s, Query().eq__id(_id))
        self.assertGreater(len(d), 0)

    def test_db_disconnect_2(self):
        i = self.get_interface()
        def callback(connection, **kwargs):
            if getattr(connection, "attempt", False):
                connection.close()
                connection.attempt = False
            connection.cursor().execute("SELECT true")

        with i.connection() as connection:
            connection.attempt = True
            i.execute(callback, connection=connection)

        with i.connection() as connection:
            i.execute(callback, connection=connection)

    def test_unsafe_delete_table_strange_name(self):
        """this makes sure https://github.com/firstopinion/prom/issues/47 is fixed,
        the problem was table names weren't escaped and so if they
        started with a number or something like that SQLite would choke"""
        table_name = "1{}".format(testdata.get_ascii(32))
        i = self.create_interface()
        s = self.get_schema(table_name)
        self.insert(i, s, 5)
        r = i.count(s)
        self.assertEqual(5, r)

        i.unsafe_delete_table(table_name)

        r = i.count(s)
        self.assertEqual(0, r)

        i._delete_table(table_name)
        self.assertFalse(i.has_table(table_name))

        i.unsafe_delete_tables()
        self.assertFalse(i.has_table(table_name))

        i.unsafe_delete_tables()
        self.assertFalse(i.has_table(table_name))

    def test_unsafe_delete_table_ref(self):
        inter = self.get_interface()
        m = testdata.create_module([
            "import prom",
            "",
            "class Foo(prom.Orm):",
            "    table_name = 'dtref_foo'",
            "",
            "class Bar(prom.Orm):",
            "    table_name = 'dtref_bar'",
            "    foo_id=prom.Field(Foo, True)",
            "    foo2_id=prom.Field(Foo, True)",
            ""
        ])

        Foo = m.module().Foo
        Foo.interface = inter
        Bar = m.module().Bar
        Bar.interface = inter

        Foo.install()
        Bar.install()

        self.assertTrue(Foo.interface.has_table("dtref_foo"))
        Foo.interface.unsafe_delete_table("dtref_foo")
        self.assertFalse(Foo.interface.has_table("dtref_foo"))

        Bar.interface.close()
        self.assertFalse(Bar.interface.is_connected())
        self.assertTrue(Bar.interface.has_table("dtref_bar"))
        Bar.interface.unsafe_delete_tables()
        self.assertFalse(Bar.interface.has_table("dtref_bar"))

    def test_in_memory_db(self):
        i, s = self.get_table()
        i.close()
        config = i.config
        config.database = ":memory:"

        i.connect(config)
        self.assertTrue(i.connected)

        _id = self.insert(i, s, 1)[0]
        self.assertTrue(_id)

