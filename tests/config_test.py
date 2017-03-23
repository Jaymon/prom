import os

import testdata

from . import BaseTestCase
import prom
from prom.model import Orm
from prom.config import Schema, Connection, DsnConnection, Index
from prom.config import Field, ObjectField, JsonField


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
                "Backend://../this/is/the/path",
                {
                    'interface_name': "Backend",
                    'host': '..',
                    'database': 'this/is/the/path'
                }
            ),
            (
                "Backend://./this/is/the/path",
                {
                    'interface_name': "Backend",
                    'host': '.',
                    'database': 'this/is/the/path'
                }
            ),
            (
                "Backend:///this/is/the/path",
                {
                    'interface_name': "Backend",
                    'host': None,
                    'database': 'this/is/the/path'
                }
            ),
            (
                "Backend://:memory:#fragment_name",
                {
                    'interface_name': "Backend",
                    'host': ":memory:",
                    'name': 'fragment_name'
                }
            ),
            (
                "Backend://:memory:?option=1&var=2#fragment_name",
                {
                    'interface_name': "Backend",
                    'host': ":memory:",
                    'name': 'fragment_name',
                    'options': {
                        'var': "2",
                        'option': "1"
                    }
                }
            ),
            (
                "Backend://:memory:",
                {
                    'interface_name': "Backend",
                    'host': ":memory:",
                }
            ),
            (
                "some.Backend://username:password@localhost:5000/database?option=1&var=2#fragment",
                {
                    'username': "username",
                    'interface_name': "some.Backend",
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
                "a.long.backend.Interface://localhost:5/database2",
                {
                    'interface_name': "a.long.backend.Interface",
                    'database': "database2",
                    'host': "localhost",
                    'port': 5,
                }
            ),
            (
                "Interface://localhost/db3",
                {
                    'interface_name': "Interface",
                    'database': "db3",
                    'host': "localhost",
                }
            ),
            (
                "Interface:///db4",
                {
                    'interface_name': "Interface",
                    'database': "db4",
                }
            ),
            (
                "Interface:///relative/path/to/db/4.sqlite",
                {
                    'interface_name': "Interface",
                    'database': "relative/path/to/db/4.sqlite",
                }
            ),
            (
                "Interface:////abs/path/to/db/4.sqlite",
                {
                    'interface_name': "Interface",
                    'database': "/abs/path/to/db/4.sqlite",
                }
            ),
            (
                "Interface:////abs/path/to/db/4.sqlite?var1=1&var2=2",
                {
                    'interface_name': "Interface",
                    'database': "/abs/path/to/db/4.sqlite",
                    'options': {
                        'var1': "1",
                        'var2': "2"
                    }
                }
            ),
            (
                "Interface:////abs/path/to/db/4.sqlite?var1=1&var2=2#name",
                {
                    'interface_name': "Interface",
                    'database': "/abs/path/to/db/4.sqlite",
                    'name': "name",
                }
            ),
            (
                "Interface:////abs/path/to/db/4.sqlite?var1=1&var2=2#name",
                {
                    'interface_name': "Interface",
                    'database': "/abs/path/to/db/4.sqlite",
                    'name': "name",
                    'options': {
                        'var1': "1",
                        'var2': "2"
                    }
                }
            ),
            (
                "Interface://localhost/db3?var1=1&var2=2#name",
                {
                    'interface_name': "Interface",
                    'database': "db3",
                    'host': "localhost",
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
           for attr, val in t[1].iteritems():
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
            database="dbname",
            port=5000,
            some_random_thing="foo"
        )

        self.assertEqual(5000, c.port)
        self.assertEqual("dbname", c.database)
        self.assertEqual({"some_random_thing": "foo"}, c.options)

    def test_host(self):
        tests = [
            ("localhost:8000", ["localhost", 8000]),
            ("localhost", ["localhost", 0]),
            ("//localhost", ["localhost", 0]),
            ("http://localhost:10", ["localhost", 10]),
            ("http://some.crazydomain.com", ["some.crazydomain.com", 0]),
            ("http://some.crazydomain.com:1000", ["some.crazydomain.com", 1000]),
            ("http://:memory:", [":memory:", 0]),
            (":memory:", [":memory:", 0]),
            ("//:memory:", [":memory:", 0]),
        ]

        for t in tests:
            p = Connection()
            p.host = t[0]
            self.assertEqual(t[1][0], p.host)
            self.assertEqual(t[1][1], p.port)

        p = Connection()
        p.port = 555
        p.host = "blah.example.com"
        self.assertEqual("blah.example.com", p.host)
        self.assertEqual(555, p.port)

        p.host = "blah.example.com:43"
        self.assertEqual("blah.example.com", p.host)
        self.assertEqual(43, p.port)


class ObjectFieldTest(BaseTestCase):
    field_class = ObjectField

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

    def test_imethods_pickle_sqlite(self):
        o = self.get_sqlite_orm()
        o.body = {"foo": 1}
        o.save()

        o2 = type(o).query.get_pk(o.pk)
        self.assertEqual(o.body, o2.body)

    def test_imethods_pickle_postgres(self):
        o = self.get_postgres_orm()
        o.body = {"bar": 1}
        o.save()

        o2 = type(o).query.get_pk(o.pk)
        self.assertEqual(o.body, o2.body)

    def test_modify(self):
        o = self.get_sqlite_orm()
        o.body = {"bar": 1}
        o.save()

        o.body["che"] = 2
        o.save()

        o2 = type(o).query.get_pk(o.pk)
        self.assertEqual(o.body, o2.body)

    def test_iget_iset_override(self):
        o = self.get_sqlite_orm()
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


class FieldTest(BaseTestCase):
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


