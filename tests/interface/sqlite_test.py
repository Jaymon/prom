# -*- coding: utf-8 -*-
from __future__ import unicode_literals, division, print_function, absolute_import
import os
import datetime

import testdata

from prom import query, InterfaceError
from prom.interface.sqlite import SQLite
from prom.config import Field
from prom.compat import *

from . import BaseTestInterface


class InterfaceSQLiteTest(BaseTestInterface):
    @classmethod
    def create_interface(cls):
        return cls.create_sqlite_interface()

    def test_fields_timestamp(self):
        table_name = self.get_table_name()
        schema = self.get_schema(table_name, ZTIMESTAMP=Field(datetime.datetime))
        q = query.Query()
        epoch = datetime.datetime(1970, 1, 1)
        timestamp = (datetime.datetime.utcnow() - epoch).total_seconds()

        i = self.create_interface()
        i.set_table(schema)

        sql = "INSERT INTO {} (ZTIMESTAMP) VALUES ({:.5f})".format(table_name, timestamp)
        r = i.query(sql, ignore_result=True)

        r = i.get_one(schema, q)
        self.assertEqual((r["ZTIMESTAMP"] - epoch).total_seconds(), round(timestamp, 5))

        timestamp = -62167219200
        sql = "INSERT INTO {} (ZTIMESTAMP) VALUES ({})".format(table_name, timestamp)
        r = i.query(sql, ignore_result=True)
        r = i.get_one(schema, q.offset(1))
        self.assertEqual(r["ZTIMESTAMP"], datetime.datetime.min)

        timestamp = 106751991167
        sql = "INSERT INTO {} (ZTIMESTAMP) VALUES ({})".format(table_name, timestamp)
        r = i.query(sql, ignore_result=True)
        r = i.get_one(schema, q.offset(2))
        self.assertEqual(r["ZTIMESTAMP"], datetime.datetime(5352, 11, 1, 10, 52, 47))

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
        r = i.query(sql, cursor_result=True)
        self.assertTrue(i.has_table("ZFOOBAR"))

        fields = i.get_fields("ZFOOBAR")
        self.assertEqual(float, fields["ZFLOAT"]["field_type"])

    def test_create_path(self):
        i = self.create_interface()
        config = i.connection_config

        d = testdata.create_dir()
        config.host = os.path.join(d, "create_path", "db.sqlite")

        i.connect(config)
        self.assertTrue(i.connected)

    def test_db_disconnect(self):
        """make sure interface can recover if the db disconnects mid script execution,
        SQLite is a bit different than postgres which is why this method is completely
        original"""
        i, s = self.get_table()
        _id = self.insert(i, s, 1)[0]
        q = query.Query()
        q.is__id(_id)
        d = i.get_one(s, q)
        self.assertGreater(len(d), 0)

        i._connection.close()

        _id = self.insert(i, s, 1)[0]
        q = query.Query()
        q.is__id(_id)
        d = i.get_one(s, q)
        self.assertGreater(len(d), 0)

    def test_no_connection(self):
        """noop, this doesn't really apply to SQLite"""
        pass

    def test_delete_table_strange_name(self):
        """this makes sure https://github.com/firstopinion/prom/issues/47 is fixed,
        the problem was table names weren't wrapped with single quotes and so if they
        started with a number or something like that SQLite would choke"""
        table_name = "1{}".format(testdata.get_ascii(32))
        i = self.create_interface()
        s = self.get_schema(table_name)
        self.insert(i, s, 5)
        r = i.count(s)
        self.assertEqual(5, r)

        i.delete_table(table_name)

        r = i.count(s)
        self.assertEqual(0, r)

        i._delete_table(table_name)
        self.assertFalse(i.has_table(table_name))

        i.delete_tables(disable_protection=True)
        self.assertFalse(i.has_table(table_name))

        i.delete_tables(disable_protection=True)
        self.assertFalse(i.has_table(table_name))

    def test_delete_table_ref(self):
        path = testdata.create_module("dtref", [
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

        from dtref import Foo, Bar

        Foo.install()
        Bar.install()

        self.assertTrue(Foo.interface.has_table("dtref_foo"))
        Foo.interface.delete_table("dtref_foo")
        self.assertFalse(Foo.interface.has_table("dtref_foo"))

        Bar.interface.close()
        self.assertFalse(Bar.interface.is_connected())
        self.assertTrue(Bar.interface.has_table("dtref_bar"))
        Bar.interface.delete_tables(disable_protection=True)
        self.assertFalse(Bar.interface.has_table("dtref_bar"))

    def test_in_memory_db(self):
        i, s = self.get_table()
        i.close()
        config = i.connection_config
        config.database = ":memory:"

        i.connect(config)
        self.assertTrue(i.connected)

        _id = self.insert(i, s, 1)[0]
        self.assertTrue(_id)

    def test_list_field(self):
        from prom import Field, Orm
        class ListFieldOrm(Orm):
            interface = self.get_interface()
            foo = Field(list)

        with self.assertRaises(ValueError):
            ListFieldOrm.install()

        lf = ListFieldOrm()
        lf.foo = [testdata.get_words(), testdata.get_words()]
        with self.assertRaises(ValueError):
            lf.save()


    # NOTE -- the x is in front of the db so this goes last because it causes
    # test_delete_table_ref to fail when it isn't last (not sure why)
#     def test_xdb_deleted(self):
#         """If the db file is deleted while this is connected anything in memory
#         will stay in memory, so the connection has no idea that the db has been
#         deleted, I could check for the existence of the db but it doesn't seem
#         worth it since it will throw errors on write, but will still allow reading,
#         this is totally an edge case"""
#         i, s = self.get_table()
#         path = i.connection_config.database
#         _id = self.insert(i, s, 1)[0]
#         d = i.get_one(s, query.Query().is__id(_id))
#         self.assertGreater(len(d), 0)
# 
#         os.unlink(path)
# 
#         self.assertFalse(os.path.isfile(path))
#         with self.assertRaises(InterfaceError):
#             _id = self.insert(i, s, 1)[0]
#         self.assertFalse(os.path.isfile(path))
# 
#         #d = i.get_one(s, query.Query().is__id(_id))
#         #pout.v(dict(d))
#         i.close()


# not sure I'm a huge fan of this solution to remove common parent from testing queue
# http://stackoverflow.com/questions/1323455/python-unit-test-with-base-and-sub-class
del(BaseTestInterface)

