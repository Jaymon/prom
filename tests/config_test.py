# -*- coding: utf-8 -*-
from __future__ import unicode_literals, division, print_function, absolute_import
import os
import datetime

import testdata

from . import BaseTestCase, EnvironTestCase
import prom
from prom.model import Orm
from prom.config import Schema, Connection, DsnConnection, Index
from prom.config import Field, ObjectField, JsonField
from prom.compat import *


class SchemaTest(BaseTestCase):
    def test___init__(self):
        """
        I had set the class .fields and .indexes attributes to {} instead of None, so you
        could only ever create one instance of Schema, this test makes sure that's been fixed
        """
        s = Schema("foo")
        self.assertTrue(isinstance(s.fields, dict))
        self.assertTrue(isinstance(s.indexes, dict))

        s2 = Schema("bar")
        self.assertTrue(isinstance(s.fields, dict))
        self.assertTrue(isinstance(s.indexes, dict))

        s = Schema(
            "foo",
            bar=Field(int),
            che=Field(str, True),
            barche=Index("bar", "che")
        )
        self.assertTrue("bar" in s.fields)
        self.assertTrue("che" in s.fields)
        self.assertTrue("barche" in s.indexes)

    def test___getattr__(self):
        s = Schema("foo")

        with self.assertRaises(AttributeError):
            s.foo

        s.set_field("foo", Field(int, True))
        self.assertTrue(isinstance(s.foo, Field))

    def test_set_field(self):
        s = Schema("foo")

        with self.assertRaises(ValueError):
            s.set_field("", int)

        with self.assertRaises(ValueError):
            s.set_field("foo", "bogus")

        s.set_field("foo", Field(int))
        with self.assertRaises(ValueError):
            s.set_field("foo", int)

        s = Schema("foo")
        s.set_field("foo", Field(int, unique=True))
        self.assertTrue("foo" in s.fields)
        self.assertTrue("foo" in s.indexes)

        s = Schema("foo")
        s.set_field("foo", Field(int, ignore_case=True))
        self.assertTrue(s.foo.options["ignore_case"])

    def test_set_index(self):
        s = Schema("foo")
        s.set_field("bar", Field(int, True))
        s.set_field("che", Field(str))

        with self.assertRaises(ValueError):
            s.set_index("foo", Index())

        with self.assertRaises(ValueError):
            s.set_index("", Index("bar", "che"))

        s.set_index("bar_che", Index("che", "bar"))
        with self.assertRaises(ValueError):
            s.set_index("bar_che", Index("che", "bar"))

        s.set_index("testing", Index("che", unique=True))
        self.assertTrue(s.indexes["testing"].unique)

    def test_primary_key(self):
        s = self.get_schema()
        self.assertEqual(s._id, s.pk)


class DsnConnectionTest(BaseTestCase):

    def test_environ(self):
        os.environ['PROM_DSN'] = "prom.interface.postgres.PostgreSQL://localhost:5000/database#i0"
        os.environ['PROM_DSN_1'] = "prom.interface.postgres.PostgreSQL://localhost:5000/database#i1"
        os.environ['PROM_DSN_2'] = "prom.interface.postgres.PostgreSQL://localhost:5000/database#i2"
        os.environ['PROM_DSN_4'] = "prom.interface.postgres.PostgreSQL://localhost:5000/database#i4"
        prom.configure_environ()
        self.assertTrue('i0' in prom.get_interfaces())
        self.assertTrue('i1' in prom.get_interfaces())
        self.assertTrue('i2' in prom.get_interfaces())
        self.assertTrue('i3' not in prom.get_interfaces())
        self.assertTrue('i4' not in prom.get_interfaces())

        prom.interface.interfaces.pop('i0', None)
        prom.interface.interfaces.pop('i1', None)
        prom.interface.interfaces.pop('i2', None)
        prom.interface.interfaces.pop('i3', None)
        prom.interface.interfaces.pop('i4', None)

    def test_dsn(self):
        tests = [
            (
                "prom.interface.postgres.PostgreSQL://username:password@localhost:5000/database?option=1&var=2#fragment",
                {
                    'username': "username",
                    'interface_name': "prom.interface.postgres.PostgreSQL",
                    'database': "database",
                    'host': "localhost",
                    'port': 5000,
                    'password': "password",
                    'name': 'fragment',
                    'options': {
                        'var': "2",
                        'option': "1"
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
                "prom.interface.postgres.PostgreSQL://localhost/db3?var1=1&var2=2#name",
                {
                    'interface_name': "prom.interface.postgres.PostgreSQL",
                    'database': "db3",
                    'host': "localhost",
                    'name': "name",
                    'options': {
                        'var1': "1",
                        'var2': "2"
                    }
                }
            ),
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
                    'host': None,
                    'database': ":memory:",
                    'name': 'fragment_name'
                }
            ),
            (
                "prom.interface.sqlite.SQLite://:memory:?option=1&var=2#fragment_name",
                {
                    'interface_name': "prom.interface.sqlite.SQLite",
                    'host': None,
                    'database': ":memory:",
                    'name': 'fragment_name',
                    'options': {
                        'var': "2",
                        'option': "1"
                    }
                }
            ),
            (
                "prom.interface.sqlite.SQLite://:memory:",
                {
                    'interface_name': "prom.interface.sqlite.SQLite",
                    'host': None,
                    'database': ":memory:",
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
                        'var1': "1",
                        'var2': "2"
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
                        'var1': "1",
                        'var2': "2"
                    }
                }
            ),
        ]

        for t in tests:
            c = DsnConnection(t[0])
            for attr, val in t[1].items():
                self.assertEqual(val, getattr(c, attr))


class ConnectionTest(BaseTestCase):
    def test_interface_is_unique_each_time(self):
        c = Connection(
            interface_name="prom.interface.sqlite.SQLite",
            database=":memory:",
        )

        iids = set()
        inters = set()
        for x in range(10):
            inter = c.interface
            iid = id(inter)
            self.assertFalse(iid in iids)
            iids.add(iid)
            inters.add(inter)

    def test___init__(self):

        c = Connection(
            interface_name="prom.interface.sqlite.SQLite",
            database="dbname",
            port=5000,
            some_random_thing="foo"
        )

        self.assertEqual(5000, c.port)
        self.assertEqual("dbname", c.database)
        self.assertEqual({"some_random_thing": "foo"}, c.options)


class ObjectFieldTest(EnvironTestCase):
    field_class = ObjectField

    def get_orm(self, default=None):
        class EnvironObjectOrm(Orm):
            interface = self.create_interface()
            body = self.field_class(True, default=default)

        return EnvironObjectOrm()

    def get_sqlite_orm(self):
        class IMethodPickleOrmSQLite(Orm):
            interface = self.create_sqlite_interface()
            body = self.field_class(True)

        return IMethodPickleOrmSQLite()

    def get_postgres_orm(self):
        class IMethodPickleOrmPostgres(Orm):
            interface = self.create_postgres_interface()
            body = self.field_class(True)

        return IMethodPickleOrmPostgres()

    def test_default(self):
        o = self.get_orm(default=dict)
        o.body["foo"] = 1
        self.assertEqual(1, o.body["foo"])

    def test_imethods_pickle(self):
        o = self.get_orm()
        o.body = {"foo": 1}
        o.save()

        o2 = type(o).query.get_pk(o.pk)
        self.assertEqual(o.body, o2.body)

    def test_modify(self):
        o = self.get_orm()
        o.body = {"bar": 1}
        o.save()

        o.body["che"] = 2
        o.save()

        o2 = type(o).query.get_pk(o.pk)
        self.assertEqual(o.body, o2.body)

    def test_iget_iset_override(self):
        o = self.get_orm()
        ocls = type(o)

        ocls_iget = ocls.body.iget
        ocls_iset = ocls.body.iset

        @ocls.body.igetter
        def body(cls, field_val):
            self.assertTrue(isinstance(field_val, dict))
            return field_val

        @ocls.body.isetter
        def body(cls, field_val, *args, **kwargs):
            self.assertTrue(isinstance(field_val, dict))
            return field_val

        o.body = {"bar": 1, "igetter": 0, "isetter": 0}
        o.save()
        self.assertTrue(isinstance(o.body, dict))

        o2 = type(o).query.get_pk(o.pk)
        self.assertTrue(isinstance(o2.body, dict))

        ocls.body.iget = ocls_iget
        ocls.body.iset = ocls_iset


class JsonFieldTest(ObjectFieldTest):
    field_class = JsonField


class FieldTest(EnvironTestCase):
#     def test_dict_type(self):
#         class DOrm(Orm):
#             foo = Field(dict)
# 
#         o = DOrm()
#         o.foo = {"bar": 1, "che": 2}
#         o.save()

    def test_datetime_jsonable(self):
        class FDatetimeOrm(Orm):
            foo = Field(datetime.datetime)

        o = FDatetimeOrm()
        o.foo = datetime.datetime.min
        r = o.jsonable()
        self.assertTrue("foo" in r)

    def test_default(self):
        class FDefaultOrm(Orm):
            foo = Field(int, default=0)
            bar = Field(int)

        o = FDefaultOrm()
        foo = o.schema.foo
        self.assertEqual(0, foo.fdefault(o, None))
        self.assertEqual(0, o.foo)

        bar = o.schema.bar
        self.assertEqual(None, bar.fdefault(o, None))
        self.assertEqual(None, o.bar)

    def test_fcrud(self):

        class FCrudOrm(Orm):
            foo = Field(int)

            @foo.fsetter
            def foo(self, v):
                return 0 if v is None else v

            @foo.fgetter
            def foo(self, v):
                return None if v is None else v + 1

            @foo.fdeleter
            def foo(self, val):
                return None

        o = FCrudOrm()

        self.assertEqual(None, o.foo)
        o.foo = 0
        self.assertEqual(1, o.foo)
        self.assertEqual(1, o.foo)

        pk = o.save()
        self.assertEqual(2, o.foo)
        self.assertEqual(2, o.foo)

        del o.foo
        self.assertEqual(None, o.foo)
        pk = o.save()
        self.assertEqual(None, o.foo)

        o.foo = 10
        self.assertEqual(11, o.foo)
        self.assertEqual(11, o.foo)

        o.foo = None
        self.assertEqual(1, o.foo)

    def test_icrud(self):
        class ICrudOrm(Orm):
            foo = Field(int)

            @foo.isetter
            def foo(self, v, is_update, is_modified):
                if is_modified:
                    v = v + 1
                elif is_update:
                    v = v - 1
                else:
                    v = 0
                return v

            @foo.igetter
            def foo(self, v):
                if v > 1:
                    v = v * 100
                return v

        o = ICrudOrm()
        self.assertEqual(None, o.foo)

        o.save()
        self.assertEqual(0, o.foo)

        o.foo = 5
        self.assertEqual(5, o.foo)

        o.save()
        self.assertEqual(600, o.foo)

        o2 = o.query.get_pk(o.pk)
        self.assertEqual(600, o2.foo)


    def test_fdeleter(self):
        class FDOrm(Orm):
            foo = Field(int)

            @foo.fdeleter
            def foo(self, val):
                return None

        o = FDOrm()
        o.foo = 1
        del o.foo
        self.assertEqual(None, o.foo)

    def test_property(self):
        class FieldPropertyOrm(Orm):
            foo = Field(int)

            @foo.fgetter
            def foo(self, val):
                return val

            @foo.fsetter
            def foo(self, val):
                return int(val) + 10 if (val is not None) else val

        o = FieldPropertyOrm()

        o.foo = 1
        self.assertEqual(11, o.foo)

        o.foo = 2
        self.assertEqual(12, o.foo)

        o.foo = None
        self.assertEqual(None, o.foo)

    def test_ref(self):
        testdata.create_module("ref", "\n".join([
                "import prom",
                "class Foo(prom.Orm):",
                "    che = prom.Field(str)",
                "",
                "class Bar(prom.Orm):",
                "    foo_id = prom.Field(Foo)",
                ""
            ])
        )

        from ref import Foo, Bar

        self.assertTrue(isinstance(Bar.schema.fields['foo_id'].schema, Schema))
        self.assertTrue(issubclass(Bar.schema.fields['foo_id'].type, long))

    def test_string_ref(self):
        testdata.create_modules({
            "stringref.foo": "\n".join([
                "import prom",
                "class Foo(prom.Orm):",
                "    interface = None",
                "    bar_id = prom.Field('stringref.bar.Bar')",
                ""
            ]),
            "stringref.bar": "\n".join([
                "import prom",
                "class Bar(prom.Orm):",
                "    interface = None",
                "    foo_id = prom.Field('stringref.foo.Foo')",
                ""
            ])
        })

        from stringref.foo import Foo
        from stringref.bar import Bar

        self.assertTrue(isinstance(Foo.schema.fields['bar_id'].schema, Schema))
        self.assertTrue(issubclass(Foo.schema.fields['bar_id'].type, long))
        self.assertTrue(isinstance(Bar.schema.fields['foo_id'].schema, Schema))
        self.assertTrue(issubclass(Bar.schema.fields['foo_id'].type, long))

    def test___init__(self):
        f = Field(str, True)
        self.assertTrue(f.required)
        self.assertTrue(issubclass(f.type, str))

        with self.assertRaises(TypeError):
            f = Field()

        f = Field(int, max_length=100)
        self.assertTrue(issubclass(f.type, int))
        self.assertEqual(f.options['max_length'], 100)


