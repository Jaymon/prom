from unittest import TestCase, skipIf
import os
import sys
import random
import string
import logging
import decimal
import datetime
import time
import subprocess
import pickle

import testdata

# needed to test prom with greenthreads
try:
    import gevent
except ImportError as e:
    gevent = None

from prom import query
from prom.model import Orm
from prom.config import Schema, Connection, DsnConnection, Field
from prom.interface.postgres import PostgreSQL
from prom.interface.sqlite import SQLite
import prom
import prom.interface


# configure root logger
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
log_handler = logging.StreamHandler(stream=sys.stderr)
log_formatter = logging.Formatter('[%(levelname)s] %(message)s')
log_handler.setFormatter(log_formatter)
logger.addHandler(log_handler)


os.environ.setdefault('PROM_SQLITE_URL', 'prom.interface.SQLite://:memory:')


def setUpModule():
    """
    http://docs.python.org/2/library/unittest.html#setupmodule-and-teardownmodule
    """
    i = get_interface()
    i.delete_tables(disable_protection=True)


def get_orm_class(table_name=None):
    i, s = get_table(table_name=table_name)
    t = Torm
    t.schema = s
    t.interface = i
    return t


def get_orm(table_name=None):
    i, s = get_table(table_name=table_name)
    t = Torm()
    t.schema = s
    t.interface = i
    return t


def get_interface():
    config = DsnConnection(os.environ["PROM_POSTGRES_URL"])
    i = PostgreSQL()
#    config = DsnConnection(os.environ["PROM_SQLITE_URL"])
#    i = SQLite()
    i.connect(config)
    assert i.connected
    return i


def get_table_name():
    """return a random table name"""
    return "".join(random.sample(string.ascii_lowercase, random.randint(5, 15)))


def get_table(table_name=None):
    """
    return an interface and schema for a table in the db

    return -- tuple -- interface, schema
    """
    i = get_interface()
    s = get_schema(table_name)
    i.set_table(s)
    return i, s


def get_schema(table_name=None):
    if not table_name:
        table_name = get_table_name()

    s = Schema(
        table_name,
        foo=(int, True),
        bar=(str, True),
        index_ifoobar=("foo", "bar")
    )

    return s


def get_query():
    i, s = get_table()
    Torm.schema = s
    Torm.interface = i
    return query.Query(Torm)


def insert(interface, schema, count, **kwargs):
    """
    insert count rows into schema using interface
    """
    _ids = []

    for i in xrange(1, count + 1):
        q = query.Query()
        q.set_fields({
            'foo': i,
            'bar': 'value {}'.format(i)
        })
        d = interface.set(schema, q, **kwargs)

        assert 'foo' in d
        assert 'bar' in d
        assert schema._id in d
        _ids.append(d[schema._id])

    return _ids

class Torm(Orm):
    pass

class Torm2Query(query.Query):
    pass
class Torm2(Orm):
    pass

class OrmTest(TestCase):

    def setUp(self):
        i, s = get_table()
        Torm.schema = s
        prom.set_interface(i)
        prom.set_interface(i, "torm2")

        Torm2.schema = s
        Torm2.connection_name = "torm2"

    def test_none(self):
        s = Schema(
            get_table_name(),
            foo=Field(int),
            bar=Field(str),
        )
        Torm.schema = s

        t1 = Torm()
        t2 = Torm(foo=None, bar=None)
        self.assertEqual(t1.fields, t2.fields)

        t1.set()
        t2.set()

        t11 = Torm.query.get_pk(t1.pk)
        t22 = Torm.query.get_pk(t2.pk)
        ff = lambda orm: orm.schema.normal_fields
        self.assertEqual(ff(t11), ff(t22))
        self.assertEqual(ff(t1), ff(t11))
        self.assertEqual(ff(t2), ff(t22))

        t3 = Torm(foo=1)
        self.assertEqual(1, t3.foo)
        self.assertEqual(None, t3.bar)
        t3.set()
        self.assertEqual(1, t3.foo)
        self.assertEqual(None, t3.bar)
        t3 = Torm.query.get_pk(t3.pk)
        self.assertEqual(1, t3.foo)
        self.assertEqual(None, t3.bar)

    def test_jsonable(self):
        table_name = get_table_name()
        Torm.schema = Schema(
            table_name,
            foo=(int, True),
            bar=(str, True),
            che=(str, False),
        )

        t = Torm.create(foo=1, bar="blah")
        d = t.jsonable()
        self.assertEqual(1, d['foo'])
        self.assertEqual("blah", d['bar'])
        self.assertEqual("", d['che'])

    def test__normalize_field(self):
        table_name = get_table_name()
        class TNF(Torm):
            field_names = set()
            schema = Schema(
                table_name,
                foo=(int, True),
                bar=(str, True),
                che=(str, False),
            )

            def _normalize_field(self, field_name, field_val):
                self.field_names.add(field_name)

                if field_name == 'random_1':
                    self.random_2 = True

                return super(TNF, self)._normalize_field(field_name, field_val)

        t = TNF()

        t.random_val = 'foo'
        self.assertTrue('random_val' in t.field_names)

        t.random_1 = True
        self.assertTrue(t.random_1)
        self.assertTrue(t.random_2)

        t = TNF(random_1=True)
        with self.assertRaises(AttributeError):
            t.random_1
        self.assertTrue(t.random_2)

    def test_modify(self):
        table_name = get_table_name()
        class TM(Torm):
            schema = Schema(
                table_name,
                bar=(str, True),
                che=(str, False),
            )

            def _normalize_field(self, field_name, field_val):
                if field_name == 'che':
                    if not field_val.startswith('boom'):
                        raise ValueError("what the heck?")

                return field_val

        t = TM(bar='bam')

        with self.assertRaises(ValueError):
            t = TM(che='bam')

        t = TM(che='boom')

    def test_unicode(self):
        """
        Jarid was having encoding issues, so I'm finally making sure prom only ever
        returns unicode strings
        """
        table_name = get_table_name()
        Torm.schema = Schema(
            table_name,
            foo=(unicode, True),
            bar=(str, True),
            che=(str, False),
            baz=(int, False),
        )

        t = Torm.create(
            foo=testdata.get_unicode_name(),
            bar=testdata.get_unicode_words(),
            che=testdata.get_unicode_words().encode('utf-8'),
            baz=testdata.get_int(1, 100000)
        )

        t2 = Torm.query.get_pk(t.pk)

        self.assertEqual(t.foo, t2.foo)
        self.assertEqual(t.bar, t2.bar)
        self.assertEqual(t.che, t2.che.encode('utf-8'))
        self.assertTrue(isinstance(t.baz, int))

    def test_query(self):
        _ids = insert(Torm.interface, Torm.schema, 5)
        lc = Torm.query.in__id(_ids).count()
        self.assertEqual(len(_ids), lc)

    def test___int__(self):
        _id = insert(Torm.interface, Torm.schema, 1)[0]
        t = Torm.query.get_pk(_id)
        self.assertEqual(_id, int(t))

    def test_query_class(self):
        """make sure you can set the query class and it is picked up correctly"""
        class QueryClassTormQuery(query.Query):
            pass

        class QueryClassTorm(Orm):
            query_class = QueryClassTormQuery
            pass

        self.assertEqual(QueryClassTorm.query_class, QueryClassTormQuery)
        self.assertEqual(Torm.query_class, query.Query)

    def test_property_autodiscover(self):
        testdata.create_module("fooq", "\n".join([
            "import prom",
            "",
            "class FooQuery(prom.Query):",
            "    pass",
            "",
            "class Foo(prom.Orm):",
            "    schema = prom.Schema('foo')",
            "    query_class = FooQuery",
            "",
            "class BarQuery(prom.Query):",
            "    pass",
            "",
            "class Bar(Foo):",
            "    schema = prom.Schema('bar')",
            "    query_class = BarQuery",
            "    pass",
            "",
            "class CheQuery(prom.Query):",
            "    pass",
        ]))

        import fooq

        # first try with the instance calling first
        f = fooq.Foo()
        self.assertEqual(f.query_class, fooq.Foo.query_class)

        f = fooq.Foo()
        self.assertEqual(f.query.__class__.__name__, fooq.Foo.query.__class__.__name__)

        f = fooq.Foo()
        self.assertEqual(f.interface, fooq.Foo.interface)

        # now try with the class calling first
        b = fooq.Bar()
        self.assertEqual(fooq.Bar.query_class, b.query_class)

        b = fooq.Bar()
        self.assertEqual(fooq.Bar.query.__class__.__name__, b.query.__class__.__name__)

        b = fooq.Bar()
        self.assertEqual(fooq.Bar.interface, b.interface)

        # now make sure we can manipulate it
        fooq.Foo.query_class = fooq.CheQuery
        f = fooq.Foo()
        self.assertEqual(fooq.CheQuery, f.query_class)
        self.assertEqual(fooq.CheQuery, fooq.Foo.query_class)
        self.assertEqual(fooq.CheQuery, f.query.__class__)
        self.assertEqual(fooq.CheQuery, fooq.Foo.query.__class__)

    def test_interface(self):
        i = Torm.interface
        self.assertFalse(i is None)

        i = Torm2.interface
        self.assertFalse(i is None)

        # now let's make sure a different orm with a bad connection name gets flagged
        class TormInterfaceOrm(Orm):
            connection_name = "blkasdfjksdafjdkfklsd"
            pass

        with self.assertRaises(KeyError):
            i = TormInterfaceOrm.interface

    def test___init__(self):
        t = Torm(foo=1)
        self.assertTrue('foo' in t.modified_fields)
        self.assertEqual(1, t.foo)

    def test_set(self):
        t = Torm()
        with self.assertRaises(KeyError):
            t.set()

        t = Torm(foo=1, bar="value 1", this_is_ignored="as it should be")
        self.assertEqual(1, t.foo)
        self.assertEqual("value 1", t.bar)
        self.assertFalse(hasattr(t, '_id'))
        self.assertTrue(t.is_modified())
        t.set()
        self.assertTrue(hasattr(t, '_id'))
        self.assertFalse(t.is_modified())

        t.foo = 2
        t.bar = "value 2"
        self.assertTrue(t.is_modified())
        t.set()
        self.assertEqual(2, t.foo)
        self.assertEqual("value 2", t.bar)

        # set should only update timestamps and stuff without changing unmodified values
        self.assertFalse(t.is_modified())
        r = t.set()
        self.assertTrue(r)

        # make sure it persisted
        t.interface.close()
        t2 = Torm.query.is__id(t._id).get_one()
        self.assertFalse(t2.is_modified())
        self.assertEqual(2, t2.foo)
        self.assertEqual("value 2", t2.bar)
        self.assertEqual(t.fields, t2.fields)

    def test_delete(self):
        t = Torm(foo=1, bar="value 1")
        r = t.delete()
        self.assertFalse(r)
        t.set()
        self.assertTrue(t.pk)
        _id = t.pk

        t.delete()
        self.assertFalse(t.pk)
        self.assertTrue(t.is_modified())

        # make sure it persists
        t.interface.close()
        t2 = Torm.query.get_pk(_id)
        self.assertEqual(None, t2)

    def test_create(self):
        t = Torm.create(foo=1000, bar="value1000")
        self.assertLess(0, t.pk)
        self.assertEqual(1000, t.foo)
        self.assertEqual("value1000", t.bar)

    def test_fields(self):
        t = Torm.create(foo=1000, bar="value1000")
        d = t.fields
        for f in t.schema.fields:
            self.assertTrue(f in d)

        # just make sure changing the dict doesn't mess up the Orm instance
        d[t.schema.pk] = d[t.schema.pk] + 1
        self.assertNotEqual(d[t.schema.pk], t.pk)

    def test_pickling(self):
        t = Torm(foo=10000, bar="value10000")

        p = pickle.dumps(t)
        t2 = pickle.loads(p)
        self.assertEqual(t.fields, t2.fields)
        self.assertEqual(t.modified_fields, t2.modified_fields)

        t.set()
        p = pickle.dumps(t)
        t2 = pickle.loads(p)
        self.assertEqual(t.fields, t2.fields)
        self.assertEqual(t.modified_fields, t2.modified_fields)

        t2.foo += 1
        t2.set()

        t3 = Torm.query.get_pk(t2.pk)
        self.assertEqual(t3.fields, t2.fields)


class PromTest(TestCase):

    def setUp(self):
        prom.interface.interfaces = {}

    def test_configure(self):
        dsn = 'prom.interface.postgres.PostgreSQL://username:password@localhost/db'
        prom.configure(dsn)
        i = prom.get_interface()
        self.assertTrue(i is not None)

        dsn += '#postgres'
        prom.configure(dsn)
        i = prom.get_interface('postgres')
        self.assertTrue(i is not None)

        dsn = 'bogus.earaskdfaksfk.Interface://host/dbname#postgres'
        with self.assertRaises(ValueError):
            prom.configure(dsn)

        dsn = 'bogus.earaskdfaksfk.Interface://host/dbname'
        with self.assertRaises(ValueError):
            prom.configure(dsn)

        dsn = 'bogus.earaskdfaksfk.Interface://host/dbname#bogus1'
        with self.assertRaises(ImportError):
            prom.configure(dsn)

        dsn = 'prom.interface.postgres.BogusSdjaksdfInterface://host/dbname#bogus2'
        with self.assertRaises(AttributeError):
            prom.configure(dsn)

    def test_failure_set(self):
        """test to make sure setting on a table that doesn't exist doesn't actually fail"""
        class FailureSetTorm(Orm):
            interface = get_interface()
            schema = get_schema()

        f = FailureSetTorm(foo=1, bar="value 1")
        f.set()
        self.assertTrue(f.pk)

    def test_failure_get(self):
        """test to make sure getting on a table that doesn't exist works without raising
        an error
        """
        class FailureGetTorm(Orm):
            interface = get_interface()
            schema = get_schema()

        f = FailureGetTorm(foo=1, bar="value 1")
        f.query.get_one()
        # we succeeded if no error was raised

class ConfigSchemaTest(TestCase):

    def test___init__(self):
        """
        I had set the class .fields and .indexes attributes to {} instead of None, so you
        could only ever create one instance of Schema, this test makes sure that's been fixed
        """
        s = Schema("foo")
        self.assertTrue(isinstance(s.fields, dict))
        self.assertTrue(isinstance(s.indexes, dict))
        s.foo = int, True

        s2 = Schema("bar")
        self.assertTrue(isinstance(s.fields, dict))
        self.assertTrue(isinstance(s.indexes, dict))
        s2.foo = str, True

        s = Schema(
            "foo",
            bar=(int,),
            che=(str, True),
            index_barche=("bar", "che")
        )
        self.assertTrue("bar" in s.fields)
        self.assertTrue("che" in s.fields)
        self.assertTrue("barche" in s.indexes)

    def test___getattr__(self):
        s = Schema("foo")

        with self.assertRaises(AttributeError):
            s.foo

        s.foo = int, True

        self.assertEqual("foo", s.foo)

    def test_set_field(self):
        s = Schema("foo")

        with self.assertRaises(ValueError):
            s.set_field("", int)

        with self.assertRaises(ValueError):
            s.set_field("foo", "bogus")

        s.set_field("foo", prom.Field(int))
        self.assertEqual({'name': "foo", 'type': int, 'required': False, 'unique': False}, s.fields["foo"])

        with self.assertRaises(ValueError):
            s.set_field("foo", int)

        s.set_field("bar", prom.Field(int, True))
        self.assertEqual({'name': "bar", 'type': int, 'required': True, 'unique': False}, s.fields["bar"])

        s.set_field("che", prom.Field(int, True, size=10))
        self.assertEqual({'name': "che", 'type': int, 'required': True, "size": 10, 'unique': False}, s.fields["che"])

        with self.assertRaises(ValueError):
            s.set_field("baz", prom.Field(int, True, min_size=10))

        s = Schema("foo")
        s.set_field("foo", prom.Field(int, size=10, max_size=50))
        self.assertEqual({'name': "foo", 'type': int, 'required': False, "size": 10, 'unique': False}, s.fields["foo"])

        s = Schema("foo")
        s.set_field("foo", prom.Field(int, min_size=10, max_size=50))
        self.assertEqual(
            {'name': "foo", 'type': int, 'required': False, "min_size": 10, "max_size": 50, 'unique': False},
            s.fields["foo"]
        )
        self.assertFalse("foo" in s.indexes)

        s = Schema("foo")
        s.set_field("foo", prom.Field(int, unique=True))
        self.assertEqual({'name': "foo", 'type': int, 'required': False, 'unique': True}, s.fields["foo"])
        self.assertEqual({'name': "foo", 'fields': ["foo"], 'unique': True}, s.indexes["foo"])

        s = Schema("foo")
        s.set_field("foo", prom.Field(int, ignore_case=True))
        self.assertEqual({'name': "foo", 'type': int, 'required': False, 'ignore_case': True, 'unique': False}, s.fields["foo"])

    def test___setattr__field(self):
        s = Schema("foo")
        s.bar = int, True
        self.assertEqual({'name': "bar", 'type': int, 'required': True, 'unique': False}, s.fields["bar"])

        s.che = int
        self.assertEqual({'name': "che", 'type': int, 'required': False, 'unique': False}, s.fields["che"])

        s.foobar = int,
        self.assertEqual({'name': "foobar", 'type': int, 'required': False, 'unique': False}, s.fields["foobar"])

        s.baz = int, True, {"size": 10}
        self.assertEqual({'name': "baz", 'type': int, 'required': True, "size": 10, 'unique': False}, s.fields["baz"])

        with self.assertRaises(ValueError):
            s.che = str,

    def test_set_index(self):
        s = Schema("foo")
        s.bar = int, True
        s.che = str

        with self.assertRaises(ValueError):
            s.set_index("foo", [])

        s.set_index("", [s.bar, s.che])
        self.assertEqual({'name': "bar_che", 'fields': ["bar", "che"], 'unique': False}, s.indexes["bar_che"])
        with self.assertRaises(ValueError):
            s.set_index("bar_che", ["che", "bar"])

        s.set_index("testing", [s.che], unique=True)
        self.assertEqual({'name': "testing", 'fields': ["che"], 'unique': True}, s.indexes["testing"])

    def test___setattr__index(self):
        s = Schema("foo")
        s.foo = int,
        s.bar = int, True
        s.che = str

        s.index = s.bar, s.che
        self.assertEqual({'name': "bar_che", 'fields': ["bar", "che"], 'unique': False}, s.indexes["bar_che"])

        s.index_chebar = s.che, s.bar
        self.assertEqual({'name': "chebar", 'fields': ["che", "bar"], 'unique': False}, s.indexes["chebar"])

        s.index_test = s.che
        self.assertEqual({'name': "test", 'fields': ["che"], 'unique': False}, s.indexes["test"])

        s.index_test_2 = s.bar,
        self.assertEqual({'name': "test_2", 'fields': ["bar"], 'unique': False}, s.indexes["test_2"])

        s.unique_test3 = s.foo,
        self.assertEqual({'name': "test3", 'fields': ["foo"], 'unique': True}, s.indexes["test3"])

    def test_primary_key(self):
        s = Schema("foo")
        s.bar = int, False

        self.assertEqual(s._id, s.pk)


class ConfigDsnConnectionTest(TestCase):

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


class ConfigConnectionTest(TestCase):
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


class ConfigFieldTest(TestCase):

    def test_decorator(self):

        class FieldDecTest(object):
            table_name = "field_dec"

            @prom.Field(int)
            def foo(self, val):
                return int(val) + 10

            bar = prom.Field(str)

        pout.v(FieldDecTest.foo)
        instance = FieldDecTest()
        pout.v(instance.foo)

        pout.v(Schema.get_instance(FieldDecTest))
        return

        f2 = prom.Field(str)
        pout.b("foo class set")
        FieldDecTest.foo = f2
        pout.b("foo instance set")
        instance.foo = f2

        b2 = prom.Field(str)
        pout.b("bar instance set")
        instance.bar = b2


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

        pout.v(Foo.schema.fields['bar_id']["field"].type)

        self.assertTrue(isinstance(Foo.schema.fields['bar_id'].ref_schema, prom.Schema))
        self.assertTrue(isinstance(Bar.schema.fields['foo_id'].ref_schema, prom.Schema))

    def test___init__(self):
        f = prom.Field(str, True)
        self.assertTrue(f.required)
        self.assertTrue(issubclass(f.type, str))

        with self.assertRaises(TypeError):
            f = prom.Field()

        f = prom.Field(int, max_length=100)
        self.assertTrue(issubclass(f.type, int))
        self.assertEqual(f.options['max_length'], 100)


class BaseTestInterface(TestCase):
    def create_interface(self):
        raise NotImplementedError()

    def get_interface(self):
        i = self.create_interface()
        i.connect()
        assert i.connected
        return i

    def get_query(self):
        i, s = self.get_table()
        Torm.schema = s
        Torm.interface = i
        return query.Query(Torm)

    def get_table(self, table_name=None):
        """
        return an interface and schema for a table in the db

        return -- tuple -- interface, schema
        """
        i = self.get_interface()
        s = get_schema(table_name)
        i.set_table(s)
        return i, s

    def test_connect(self):
        i = self.get_interface()

    def test_close(self):
        i = self.get_interface()
        i.close()
        #self.assertTrue(i.connection is None)
        self.assertFalse(i.connected)

    def test_query(self):
        i = get_interface()
        rows = i.query('SELECT 1')
        self.assertGreater(len(rows), 0)

    def test_transaction_error(self):
        i = self.get_interface()
        with self.assertRaises(StopIteration):
            with i.transaction():
                raise StopIteration()

#        with self.assertRaises(RuntimeError):
#            with i.transaction():
#                raise RuntimeError()

    def test_set_table(self):
        i = self.get_interface()
        s = get_schema()
        r = i.has_table(s.table)
        self.assertFalse(r)

        r = i.set_table(s)

        r = i.has_table(s.table)
        self.assertTrue(r)

        # make sure it persists
        # TODO -- this should only be tested in postgres, the SQLite :memory: db
        # goes away when the connections is closed
#        i.close()
#        i = self.get_interface()
#        self.assertTrue(i.has_table(s.table))

        # make sure known indexes are there
        indexes = i.get_indexes(s)
        count = 0
        for known_index_name, known_index_d in s.indexes.iteritems():
            for index_name, index_fields in indexes.iteritems():
                if known_index_d['fields'] == index_fields:
                    count += 1

        self.assertEqual(len(s.indexes), count)

        # make sure more exotic datatypes are respected
        s_ref = get_schema()
        i.set_table(s_ref)
        s_ref_id = insert(i, s_ref, 1)[0]

        s = prom.Schema(
            get_table_name(),
            one=(bool, True),
            two=(int, True, dict(size=50)),
            three=(decimal.Decimal,),
            four=(float, True, dict(size=10)),
            six=(long, True,),
            seven=(int, False, dict(ref=s_ref)),
            eight=(datetime.datetime,),
            nine=(datetime.date,),
        )
        r = i.set_table(s)
        d = {
            'one': True,
            'two': 50,
            'three': decimal.Decimal('1.5'),
            'four': 1.987654321,
            'six': 40000,
            'seven': s_ref_id,
            'eight': datetime.datetime(2005, 7, 14, 12, 30),
            'nine': datetime.date(2005, 9, 14),
        }
        o = i.insert(s, d)
        q = query.Query()
        q.is__id(o[s.pk])
        odb = i.get_one(s, q)
        #d['five'] = 1.98765
        for k, v in d.iteritems():
            self.assertEqual(v, odb[k])

    def test_get_tables(self):
        i = self.get_interface()
        s = get_schema()
        r = i.set_table(s)
        r = i.get_tables()
        self.assertTrue(s.table in r)

        r = i.get_tables(s.table)
        self.assertTrue(s.table in r)

    def test_delete_table(self):
        i = self.get_interface()
        s = get_schema()

        r = i.set_table(s)
        self.assertTrue(i.has_table(s.table))

        r = i.delete_table(s)
        self.assertFalse(i.has_table(s.table))

        # make sure it persists
        i.close()
        i = self.get_interface()
        self.assertFalse(i.has_table(s.table))

    def test_delete_tables(self):

        i = self.get_interface()
        s1 = get_schema()
        i.set_table(s1)
        s2 = get_schema()
        i.set_table(s2)

        self.assertTrue(i.has_table(s1))
        self.assertTrue(i.has_table(s2))

        # make sure you can't shoot yourself in the foot willy nilly
        with self.assertRaises(ValueError):
            i.delete_tables()

        i.delete_tables(disable_protection=True)

        self.assertFalse(i.has_table(s1))
        self.assertFalse(i.has_table(s2))

    def test_insert(self):
        i, s = self.get_table()
        d = {
            'foo': 1,
            'bar': 'this is the value',
        }

        rd = i.insert(s, d)
        self.assertGreater(rd[s._id], 0)

    def test_set_insert(self):
        """test just the insert portion of set"""
        i, s = self.get_table()
        q = query.Query()

        q.set_fields({
            'foo': 1,
            'bar': 'this is the value',
        })

        rd = i.set(s, q)
        self.assertGreater(rd[s._id], 0)

    def test_get_sql(self):
        i = self.get_interface()
        s = get_schema()
        q = query.Query()
        q.in__id(range(1, 5))
        sql, sql_args = i.get_SQL(s, q)
        self.assertTrue('_id' in sql)
        self.assertEqual(4, len(sql_args))

        q.gt_foo(5)

        sql, sql_args = i.get_SQL(s, q)
        self.assertTrue('foo' in sql)
        self.assertTrue('AND' in sql)
        self.assertEqual(5, len(sql_args))

        q.asc_foo().desc_bar()
        sql, sql_args = i.get_SQL(s, q)
        self.assertTrue('ORDER BY' in sql)
        self.assertTrue('ASC' in sql)
        self.assertTrue('DESC' in sql)

        q.set_limit(222).set_offset(111)

        sql, sql_args = i.get_SQL(s, q)
        self.assertTrue('LIMIT' in sql)
        self.assertTrue('OFFSET' in sql)
        self.assertTrue('222' in sql)
        self.assertTrue('111' in sql)

    def test_get_one(self):
        i, s = self.get_table()
        _ids = insert(i, s, 2)

        for _id in _ids:
            q = query.Query()
            q.is__id(_id)
            d = i.get_one(s, q)
            self.assertEqual(d[s._id], _id)

        q = query.Query()
        q.is__id(12334342)
        d = i.get_one(s, q)
        self.assertEqual({}, d)

    def test_get_one_offset(self):
        """make sure get_one() works as expected when an offset is set"""
        i, s = self.get_table()
        q = query.Query()
        q.set_fields({
            'foo': 1,
            'bar': 'v1',
        })
        rd = i.set(s, q)

        q = query.Query()
        q.set_fields({
            'foo': 2,
            'bar': 'v2',
        })
        rd2 = i.set(s, q)

        q = query.Query()
        q.desc__id().set_offset(1)
        d = i.get_one(s, q)
        self.assertEqual(d['_id'], rd['_id'])

        # just make sure to get expected result if no offset
        q = query.Query()
        q.desc__id()
        d = i.get_one(s, q)
        self.assertEqual(d['_id'], rd2['_id'])

        q = query.Query()
        q.desc__id().set_offset(2)
        d = i.get_one(s, q)
        self.assertEqual({}, d)

        q = query.Query()
        q.desc__id().set_offset(1).set_limit(5)
        d = i.get_one(s, q)
        self.assertEqual(d['_id'], rd['_id'])

        q = query.Query()
        q.desc__id().set_page(2)
        d = i.get_one(s, q)
        self.assertEqual(d['_id'], rd['_id'])

        q = query.Query()
        q.desc__id().set_page(2).set_limit(5)
        d = i.get_one(s, q)
        self.assertEqual({}, d)

    def test_get(self):
        i, s = self.get_table()
        _ids = insert(i, s, 5)

        q = query.Query()
        q.in__id(_ids)
        l = i.get(s, q)
        self.assertEqual(len(_ids), len(l))
        for d in l:
            self.assertTrue(d[s._id] in _ids)

        q.set_limit(2)
        l = i.get(s, q)
        self.assertEqual(2, len(l))
        for d in l:
            self.assertTrue(d[s._id] in _ids)

    def test_get_no_where(self):
        """test get with no where clause"""
        i, s = self.get_table()
        _ids = insert(i, s, 5)

        q = None
        l = i.get(s, q)
        self.assertEqual(5, len(l))

    def test_get_pagination(self):
        """test get but moving through the results a page at a time to make sure limit and offset works"""
        i, s = self.get_table()
        _ids = insert(i, s, 12)

        q = query.Query()
        q.set_limit(5)
        count = 0
        for p in xrange(1, 5):
            q.set_page(p)
            l = i.get(s, q)
            for d in l:
                self.assertTrue(d[s._id] in _ids)

            count += len(l)

        self.assertEqual(12, count)

    def test_count(self):
        i, s = self.get_table()

        # first try it with no rows
        q = query.Query()
        r = i.count(s, q)
        self.assertEqual(0, r)

        # now try it with rows
        _ids = insert(i, s, 5)
        q = query.Query()
        r = i.count(s, q)
        self.assertEqual(5, r)

    def test_delete(self):
        # try deleting with no table
        i = self.get_interface()
        s = get_schema()
        q = query.Query().is_foo(1)
        r = i.delete(s, q)

        i, s = self.get_table()

        # try deleting with no values in the table
        q = query.Query()
        q.is_foo(1)
        r = i.delete(s, q)
        self.assertEqual(0, r)

        _ids = insert(i, s, 5)

        # delete all the inserted values
        q = query.Query()
        q.in__id(_ids)
        l = i.get(s, q)
        self.assertEqual(5, len(l))
        r = i.delete(s, q)
        self.assertEqual(5, r)

        # verify rows are deleted
        l = i.get(s, q)
        self.assertEqual(0, len(l))

        # make sure it stuck
        i.close()
        i = self.get_interface()
        l = i.get(s, q)
        self.assertEqual(0, len(l))

    def test_update(self):
        i, s = self.get_table()
        q = query.Query()
        d = {
            'foo': 1,
            'bar': 'value 1',
        }

        rd = i.insert(s, d)
        self.assertGreater(rd[s._id], 0)

        d = {
            'foo': 2,
            'bar': 'value 2',
        }
        q.set_fields(d)
        q.is_field(s._id, rd[s._id])

        ud = i.update(s, q)

        self.assertEqual(ud['foo'], d['foo'])
        self.assertEqual(ud['bar'], d['bar'])

        # let's pull it out and make sure it persisted
        q = query.Query()
        q.is__id(rd[s._id])
        gd = i.get_one(s, q)
        self.assertEqual(ud['foo'], gd['foo'])
        self.assertEqual(ud['bar'], gd['bar'])
        self.assertEqual(rd[s._id], gd[s._id])

    def test_ref(self):
        i = self.get_interface()
        table_name_1 = "".join(random.sample(string.ascii_lowercase, random.randint(5, 15)))
        table_name_2 = "".join(random.sample(string.ascii_lowercase, random.randint(5, 15)))

        s_1 = Schema(
            table_name_1,
            foo=(int, True)
        )
        s_2 = Schema(
            table_name_2,
            s_pk=(int, True, dict(ref=s_1)),
        )

        i.set_table(s_1)
        i.set_table(s_2)

        d1 = i.insert(s_1, {'foo': 1})
        pk1 = d1[s_1.pk]

        d2 = i.insert(s_2, {'s_pk': pk1})
        pk2 = d2[s_2.pk]

        q2 = query.Query()
        q2.is__id(pk2)
        # make sure it exists and is visible
        r = i.get_one(s_2, q2)
        self.assertGreater(len(r), 0)

        q1 = query.Query()
        q1.is__id(pk1)
        i.delete(s_1, q1)

        r = i.get_one(s_2, q2)
        self.assertEqual({}, r)

    def test_weak_ref(self):
        i = self.get_interface()
        table_name_1 = "".join(random.sample(string.ascii_lowercase, random.randint(5, 15)))
        table_name_2 = "".join(random.sample(string.ascii_lowercase, random.randint(5, 15)))

        s_1 = Schema(
            table_name_1,
            foo=(int, True)
        )
        s_2 = Schema(
            table_name_2,
            s_pk=(int, False, dict(weak_ref=s_1)),
        )

        i.set_table(s_1)
        i.set_table(s_2)

        d1 = i.insert(s_1, {'foo': 1})
        pk1 = d1[s_1.pk]

        d2 = i.insert(s_2, {'s_pk': pk1})
        pk2 = d2[s_2.pk]
        q2 = query.Query()
        q2.is__id(pk2)
        # make sure it exists and is visible
        r = i.get_one(s_2, q2)
        self.assertGreater(len(r), 0)

        q1 = query.Query()
        q1.is__id(pk1)
        i.delete(s_1, q1)

        r = i.get_one(s_2, q2)
        self.assertGreater(len(r), 0)
        self.assertIsNone(r['s_pk'])

    def test_handle_error_ref(self):
        i = self.get_interface()
        table_name_1 = get_table_name()
        table_name_2 = get_table_name()

        s_1 = Schema(
            table_name_1,
            foo=(int, True)
        )
        s_2 = Schema(
            table_name_2,
            bar=(int, True),
            s_pk=(int, True, dict(ref=s_1)),
        )

        q2 = query.Query()
        q2.is_bar(1)

        r = i.get_one(s_2, q2)
        self.assertTrue(i.has_table(table_name_1))
        self.assertTrue(i.has_table(table_name_2))

    def test__get_fields(self):
        i, s = self.get_table()
        fields = set([u'_created', u'_id', u'_updated', u'bar', u'foo'])
        ret_fields = i._get_fields(s)
        self.assertEqual(fields, ret_fields)

    def test__set_all_fields(self):
        i, s = self.get_table()
        s.che = str, True
        q = query.Query()
        q.set_fields({
            'foo': 1,
            'bar': 'v1',
            'che': "this field will cause the query to fail",
        })

        with self.assertRaises(ValueError):
            ret = i._set_all_fields(s)

        s = get_schema(table_name=s.table)
        s.che = str, False
        ret = i._set_all_fields(s)
        self.assertTrue(ret)

    def test_handle_error_column(self):
        i, s = self.get_table()
        s.che = str, True
        q = query.Query()
        q.set_fields({
            'foo': 1,
            'bar': 'v1',
            'che': "this field will cause the query to fail",
        })

        with self.assertRaises(prom.InterfaceError):
            rd = i.set(s, q)

        s = get_schema(table_name=s.table)
        s.che = str, False
        rd = i.set(s, q)
        self.assertTrue('che' in rd)

    def test_null_values(self):
        i = self.get_interface()
        table_name = get_table_name()

        s = Schema(
            table_name,
            foo=Field(int, False),
            bar=Field(int, False),
        )

        # add one with non NULL foo
        q = query.Query()
        q.set_bar(1).set_foo(2)
        d1 = i.set(s, q)

        # and one with NULL foo
        q = query.Query()
        q.set_bar(1)
        d2 = i.set(s, q)

        q = query.Query()
        q.is_bar(1).is_foo(None)
        r = i.get_one(s, q)
        self.assertEqual(d2['_id'], r['_id'])

        q = query.Query()
        q.is_bar(1).not_foo(None)
        r = i.get_one(s, q)
        self.assertEqual(d1['_id'], r['_id'])

    def test_transaction_nested_fail_1(self):
        """make sure 2 new tables in a wrapped transaction work as expected"""
        i = self.get_interface()
        table_name_1 = get_table_name()
        table_name_2 = get_table_name()

        s1 = Schema(
            table_name_1,
            foo=(int, True)
        )
        s2 = Schema(
            table_name_2,
            bar=(int, True),
            s_pk=(int, True, dict(ref=s1)),
        )

        with i.transaction() as connection:
            q1 = query.Query()
            q1.set_foo(1)
            d1 = i.set(s1, q1, connection=connection)

            q2 = query.Query()
            q2.set_bar(2).set_s_pk(d1['_id'])
            d2 = i.set(s2, q2, connection=connection)

        q1 = query.Query()
        q1.is__id(d1['_id'])
        r1 = i.get_one(s1, q1)
        self.assertEqual(r1['_id'], d1['_id'])

        q2 = query.Query()
        q2.is__id(d2['_id'])
        r2 = i.get_one(s2, q2)
        self.assertEqual(r2['_id'], d2['_id'])
        self.assertEqual(r2['s_pk'], d1['_id'])

    def test_transaction_nested_fail_2(self):
        """make sure 2 tables where the first one already exists works in a nested transaction"""
        i = self.get_interface()
        table_name_1 = "{}_1".format(get_table_name())
        table_name_2 = "{}_2".format(get_table_name())

        s1 = Schema(
            table_name_1,
            foo=(int, True)
        )
        i.set_table(s1)

        s2 = Schema(
            table_name_2,
            bar=(int, True),
            s_pk=(int, True, dict(ref=s1)),
        )

        with i.transaction() as connection:
            q1 = query.Query()
            q1.set_foo(1)
            d1 = i.set(s1, q1, connection=connection)

            q2 = query.Query()
            q2.set_bar(2).set_s_pk(d1['_id'])
            d2 = i.set(s2, q2, connection=connection)

        q1 = query.Query()
        q1.is__id(d1['_id'])
        r1 = i.get_one(s1, q1)
        self.assertEqual(r1['_id'], d1['_id'])

        q2 = query.Query()
        q2.is__id(d2['_id'])
        r2 = i.get_one(s2, q2)
        self.assertEqual(r2['_id'], d2['_id'])
        self.assertEqual(r2['s_pk'], d1['_id'])

    def test_transaction_nested_fail_3(self):
        """make sure 2 tables where the first one already exists works, and second one has 2 refs"""
        i = self.get_interface()
        table_name_1 = "{}_1".format(get_table_name())
        table_name_2 = "{}_2".format(get_table_name())

        s1 = Schema(
            table_name_1,
            foo=(int, True)
        )
        i.set_table(s1)

        s2 = Schema(
            table_name_2,
            bar=(int, True),
            s_pk=(int, True, dict(ref=s1)),
            s_pk2=(int, True, dict(ref=s1)),
        )

        q1 = query.Query()
        q1.set_foo(1)
        d1 = i.set(s1, q1)
        q1 = query.Query()
        q1.set_foo(1)
        d2 = i.set(s1, q1)

        q2 = query.Query()
        q2.set_bar(2).set_s_pk(d1['_id']).set_s_pk2(d2['_id'])
        d2 = i.set(s2, q2)

        q1 = query.Query()
        q1.is__id(d1['_id'])
        r1 = i.get_one(s1, q1)
        self.assertEqual(r1['_id'], d1['_id'])

        q2 = query.Query()
        q2.is__id(d2['_id'])
        r2 = i.get_one(s2, q2)
        self.assertEqual(r2['_id'], d2['_id'])
        self.assertEqual(r2['s_pk'], d1['_id'])

    def test_transaction_nested_fail_4(self):
        """ran into a bug where this reared its head and data was lost"""
        i = self.get_interface()
        table_name_1 = "{}_1".format(get_table_name())
        table_name_2 = "{}_2".format(get_table_name())
        table_name_3 = "{}_3".format(get_table_name())

        # these 2 tables exist before the transaction starts
        s1 = Schema(
            table_name_1,
            foo=(int, True)
        )
        i.set_table(s1)

        s2 = Schema(
            table_name_2,
            bar=(int, True),
            s_pk=(int, True, dict(ref=s1)),
            s_pk2=(int, True, dict(ref=s1)),
        )
        i.set_table(s2)

        # this is the table that will be created in the transaction
        s3 = Schema(
            table_name_3,
            che=(int, True),
            s_pk=(int, True, dict(ref=s1)),
        )

        q1 = query.Query()
        q1.set_foo(1)
        d1 = i.set(s1, q1)

        q1 = query.Query()
        q1.set_foo(12)
        d12 = i.set(s1, q1)

        self.assertEqual(0, i.count(s2, query.Query()))

        with i.transaction() as connection:

            # create something and put in table 2
            q2 = query.Query()
            q2.set_bar(2).set_s_pk(d1['_id']).set_s_pk2(d12['_id'])
            d2 = i.set(s2, q2, connection=connection)

            # now this should cause the stuff to fail
            # it fails on the select because a new transaction isn't started, so 
            # it just discards all the current stuff and adds the table, had this
            # been a mod query (eg, insert) it would not have failed, this is fixed
            # by wrapping selects in a transaction if an active transaction is found
            q3 = query.Query()
            q3.is_s_pk(d1['_id'])
            d3 = i.get(s3, q3, connection=connection)

        self.assertEqual(1, i.count(s2, query.Query()))

    def test_transaction_context(self):
        i = self.get_interface()
        table_name_1 = "{}_1".format(get_table_name())
        table_name_2 = "{}_2".format(get_table_name())

        # these 2 tables exist before the transaction starts
        s1 = Schema(
            table_name_1,
            foo=(int, True)
        )
        i.set_table(s1)

        s2 = Schema(
            table_name_2,
            bar=(int, True),
            s_pk=(int, True, dict(ref=s1)),
        )
        i.set_table(s2)


        d1 = {}
        d2 = {}

        try:
            with i.transaction() as connection:
                q1 = query.Query()
                q1.set_foo(1)
                d1 = i.set(s1, q1, connection=connection)

                with i.transaction(connection):
                    q2 = query.Query()
                    q2.set_bar(2).set_s_pk(d1['_id'])
                    d2 = i.set(s2, q2, connection=connection)

                    raise RuntimeError("testing")

        except Exception, e:
            pass

        self.assertEqual(0, i.count(s1, query.Query().is__id(d1['_id'])))
        self.assertEqual(0, i.count(s2, query.Query().is__id(d2['_id'])))

    def test_unique(self):
        i = get_interface()
        s = get_schema()
        s.should_be_unique = int, True, dict(unique=True)
        i.set_table(s)

        d = i.insert(s, {'foo': 1, 'bar': 'v1', 'should_be_unique': 1})

        with self.assertRaises(prom.InterfaceError):
            d = i.insert(s, {'foo': 2, 'bar': 'v2', 'should_be_unique': 1})

    def test_index_ignore_case(self):
        i = self.get_interface()
        s = Schema(
            get_table_name(),
            foo=(str, True, dict(ignore_case=True)),
            bar=(str, True),
            index_foo=('foo', 'bar'),
        )
        i.set_table(s)

        v = 'justin-lee@mail.com'
        d = i.insert(s, {'foo': v, 'bar': 'bar'})
        q = query.Query()
        q.is_foo(v)
        r = i.get_one(s, q)
        self.assertGreater(len(r), 0)

        lv = list(v)
        for x in xrange(len(v)):
            lv[x] = lv[x].upper()
            qv = "".join(lv)
            q = query.Query()
            q.is_foo(qv)
            r = i.get_one(s, q)
            self.assertGreater(len(r), 0)
            lv[x] = lv[x].lower()

        d = i.insert(s, {'foo': 'FoO', 'bar': 'bar'})
        q = query.Query()
        q.is_foo('foo')
        r = i.get_one(s, q)
        self.assertGreater(len(r), 0)
        self.assertEqual(r['foo'], 'FoO')

        q = query.Query()
        q.is_foo('Foo').is_bar('BAR')
        r = i.get_one(s, q)
        self.assertEqual(len(r), 0)

        q = query.Query()
        q.is_foo('FoO').is_bar('bar')
        r = i.get_one(s, q)
        self.assertGreater(len(r), 0)
        self.assertEqual(r['foo'], 'FoO')

        d = i.insert(s, {'foo': 'foo2', 'bar': 'bar'})
        q = query.Query()
        q.is_foo('foo2')
        r = i.get_one(s, q)
        self.assertGreater(len(r), 0)
        self.assertEqual(r['foo'], 'foo2')

    def test_in_sql(self):
        i, s = self.get_table()
        _ids = insert(i, s, 5)

        q = query.Query()
        q.in__id(_ids)
        l = list(i.get(s, q))

        self.assertEqual(len(l), 5)

    def test_sort_list(self):
        q = self.get_query()
        insert(q.orm.interface, q.orm.schema, 10)

        q2 = q.copy()
        foos = list(q2.select_foo().values())
        random.shuffle(foos)

        q3 = q.copy()
        rows = list(q3.select_foo().in_foo(foos).asc_foo(foos).values())
        for i, r in enumerate(rows):
            self.assertEqual(foos[i], r)

        q4 = q.copy()
        rfoos = list(reversed(foos))
        rows = list(q4.select_foo().in_foo(foos).desc_foo(foos).values())
        for i, r in enumerate(rows):
            self.assertEqual(rfoos[i], r)

        # now test a string value
        qb = q.copy()
        bars = list(qb.select_bar().values())
        random.shuffle(bars)

        qb = q.copy()
        rows = list(qb.in_bar(bars).asc_bar(bars).get())
        for i, r in enumerate(rows):
            self.assertEqual(bars[i], r.bar)

        # make sure limits and offsets work
        qb = q.copy()
        rows = list(qb.in_bar(bars).asc_bar(bars).get(5))
        for i, r in enumerate(rows):
            self.assertEqual(bars[i], r.bar)

        qb = q.copy()
        rows = list(qb.in_bar(bars).asc_bar(bars).get(2, 2))
        for i, r in enumerate(rows, 3):
            self.assertEqual(bars[i], r.bar)

        # make sure you can select on one row and sort on another
        qv = q.copy()
        vs = list(qv.select_foo().select_bar().values())
        random.shuffle(vs)

        qv = q.copy()
        rows = list(qv.select_foo().asc_bar((v[1] for v in vs)).values())
        for i, r in enumerate(rows):
            self.assertEqual(vs[i][0], r)

    def test_transaction_context_manager(self):
        """make sure the with transaction() context manager works as expected"""
        i, s = self.get_table()
        _id = None
        with i.transaction() as connection:
            _id = insert(i, s, 1, connection=connection)[0]

        self.assertTrue(_id)

        q = query.Query()
        q.is__id(_id)
        d = i.get_one(s, q)
        self.assertGreater(len(d), 0)

        with self.assertRaises(RuntimeError):
            with i.transaction() as connection:
                _id = insert(i, s, 1, connection=connection)[0]
                raise RuntimeError("this should fail")

        q = query.Query()
        q.is__id(_id)
        d = i.get_one(s, q)
        self.assertEqual(len(d), 0)

    def test__normalize_date_SQL(self):
        """this tests the common date kwargs you can use (in both SQLight and Postgres)
        if we ever add other backends this might need to be moved out of the general
        generator test"""
        i = self.get_interface()
        s = Schema(
            get_table_name(),
            foo=(datetime.datetime, True),
            index_foo=('foo'),
        )
        i.set_table(s)

        d20 = i.insert(s, {'foo': datetime.datetime(2014, 4, 20)})
        d21 = i.insert(s, {'foo': datetime.datetime(2014, 4, 21)})

        q = query.Query()
        q.is_foo(day=20)
        d = i.get_one(s, q)
        self.assertEqual(d['_id'], d20['_id'])

        q = query.Query()
        q.is_foo(day=21, month=4)
        d = i.get_one(s, q)
        self.assertEqual(d['_id'], d21['_id'])

        q = query.Query()
        q.is_foo(day=21, month=3)
        d = i.get_one(s, q)
        self.assertFalse(d)


class InterfaceSQLiteTest(BaseTestInterface):
    def create_interface(self):
        config = DsnConnection(os.environ["PROM_SQLITE_URL"])
        i = SQLite(config)
        return i

    def test_db_disconnect(self):
        """make sure interface can recover if the db disconnects mid script execution,
        SQLite is a bit different than postgres which is why this method is completely
        original"""
        i, s = self.get_table()
        _id = insert(i, s, 1)[0]
        q = query.Query()
        q.is__id(_id)
        d = i.get_one(s, q)
        self.assertGreater(len(d), 0)

        i._connection.close()

        _id = insert(i, s, 1)[0]
        q = query.Query()
        q.is__id(_id)
        d = i.get_one(s, q)
        self.assertGreater(len(d), 0)

    def test_no_connection(self):
        """noop, this doesn't really apply to SQLite"""
        pass

class InterfacePostgresTest(BaseTestInterface):
    def create_interface(self):
        config = DsnConnection(os.environ["PROM_POSTGRES_URL"])
        i = PostgreSQL(config)
        return i

    def test_set_table_postgres(self):
        """test some postgres specific things"""
        i = self.get_interface()
        s = prom.Schema(
            get_table_name(),
            four=(float, True, dict(size=10)),
            five=(float, True,),
            six=(long, True,),
        )
        r = i.set_table(s)
        d = {
            'four': 1.987654321,
            'five': 1.98765,
            'six': 4000000000,
        }
        o = i.insert(s, d)
        q = query.Query()
        q.is__id(o[s.pk])
        odb = i.get_one(s, q)
        for k, v in d.iteritems():
            self.assertEqual(v, odb[k])

    def test_db_disconnect(self):
        """make sure interface can recover if the db disconnects mid script execution"""
        i, s = self.get_table()
        _id = insert(i, s, 1)[0]
        q = query.Query()
        q.is__id(_id)
        d = i.get_one(s, q)
        self.assertGreater(len(d), 0)

        exit_code = subprocess.check_call("sudo /etc/init.d/postgresql restart", shell=True)
        time.sleep(1)

        q = query.Query()
        q.is__id(_id)
        d = i.get_one(s, q)
        self.assertGreater(len(d), 0)

    def test_no_connection(self):
        """this will make sure prom handles it gracefully if there is no connection available ever"""
        exit_code = subprocess.check_call("sudo /etc/init.d/postgresql stop", shell=True)
        time.sleep(1)

        try:
            i = self.create_interface()
            s = get_schema()
            q = query.Query()
            with self.assertRaises(prom.InterfaceError):
                i.get(s, q)

        finally:
            exit_code = subprocess.check_call("sudo /etc/init.d/postgresql start", shell=True)
            time.sleep(1)

    def test__normalize_val_SQL(self):
        i = self.get_interface()
        s = Schema(
            "fake_table_name",
            ts=Field(datetime.datetime, True)
        )

        #kwargs = dict(day=int(datetime.datetime.utcnow().strftime('%d')))
        kwargs = dict(day=10)
        fstr, fargs = i._normalize_val_SQL(s, {'symbol': '='}, 'ts', None, kwargs)
        self.assertEqual("EXTRACT(DAY FROM ts) = %s", fstr)
        self.assertEqual(10, fargs[0])

        kwargs = dict(day=11, hour=12)
        fstr, fargs = i._normalize_val_SQL(s, {'symbol': '='}, 'ts', None, kwargs)
        self.assertEqual("EXTRACT(DAY FROM ts) = %s AND EXTRACT(HOUR FROM ts) = %s", fstr)
        self.assertEqual(11, fargs[0])
        self.assertEqual(12, fargs[1])

        fstr, fargs = i._normalize_val_SQL(s, {'symbol': '=', 'none_symbol': 'IS'}, 'ts', None)
        self.assertEqual("ts IS %s", fstr)

        fstr, fargs = i._normalize_val_SQL(s, {'symbol': '!=', 'none_symbol': 'IS NOT'}, 'ts', None)
        self.assertEqual("ts IS NOT %s", fstr)

        kwargs = dict(bogus=5)
        with self.assertRaises(KeyError):
            fstr, fargs = i._normalize_val_SQL(s, {'symbol': '='}, 'ts', None, kwargs)

    def test__normalize_list_SQL(self):
        i = self.get_interface()
        s = Schema(
            "fake_table_name",
            ts=Field(datetime.datetime, True)
        )

        kwargs = dict(day=[10])
        fstr, fargs = i._normalize_list_SQL(s, {'symbol': 'IN'}, 'ts', None, kwargs)
        self.assertEqual("EXTRACT(DAY FROM ts) IN (%s)", fstr)
        self.assertEqual(kwargs['day'], fargs)

        kwargs = dict(day=[11, 13], hour=[12])
        fstr, fargs = i._normalize_list_SQL(s, {'symbol': 'IN'}, 'ts', None, kwargs)
        self.assertEqual("EXTRACT(DAY FROM ts) IN (%s, %s) AND EXTRACT(HOUR FROM ts) IN (%s)", fstr)
        self.assertEqual(kwargs['day'], fargs[0:2])
        self.assertEqual(kwargs['hour'], fargs[2:])

        kwargs = dict(bogus=[5])
        with self.assertRaises(KeyError):
            fstr, fargs = i._normalize_list_SQL(s, {'symbol': 'IN'}, 'ts', None, kwargs)

    def test__id_insert(self):
        """this fails, so you should be really careful if you set _id and make sure you
        set the auto-increment appropriately"""
        return 
        interface, schema = self.get_table()
        start = 5
        stop = 10
        for i in xrange(start, stop):
            q = query.Query()
            q.set_fields({
                '_id': i,
                'foo': i,
                'bar': 'v{}'.format(i)
            })
            d = interface.set(schema, q)

        for i in xrange(0, stop):
            q = query.Query()
            q.set_fields({
                'foo': stop + 1,
                'bar': 'v{}'.format(stop + 1)
            })
            d = interface.set(schema, q)

    def test_no_db_error(self):
        # we want to replace the db with a bogus db error
        i, s = self.get_table()
        config = i.connection_config
        config.database = 'this_is_a_bogus_db_name'
        i = PostgreSQL(config)
        q = query.Query()
        q.set_fields({
            'foo': 1,
            'bar': 'v1',
        })
        with self.assertRaises(prom.InterfaceError):
            rd = i.set(s, q)


def has_spiped():
    ret = False
    try:
        c = subprocess.check_call("which spiped", shell=True)
        ret = True
    except CalledProcessError:
        ret = False
    return ret


class InterfacePGBouncerTest(InterfacePostgresTest):
    def create_interface(self):
        config = DsnConnection(os.environ["PROM_PGBOUNCER_URL"])
        i = PostgreSQL(config)
        return i

    def test_no_connection(self):
        """this will make sure prom handles it gracefully if there is no connection
        available ever. We have to wrap this for pgbouncer because PGBouncer can
        hold the connections if there is no db waiting for the db to come back up
        for all sorts of timeouts, and it's just easier to reset pg boucner than
        configure it for aggressive test timeouts.
        """
        subprocess.check_call("sudo stop pgbouncer", shell=True)
        time.sleep(1)

        try:
            super(InterfacePGBouncerTest, self).test_no_connection()

        finally:
            subprocess.check_call("sudo start pgbouncer", shell=True)
            time.sleep(1)

    @skipIf(not has_spiped(), "No Spiped installed")
    def test_dropped_pipe(self):
        """handle a secured pipe like spiped or stunnel restarting while there were
        active connections

        NOTE -- currently this is very specific to our environment, this test will most
        likely always be skipped unless you're testing on our Vagrant box
        """
        # TODO -- make this more reproducible outside of our environment
        i, s = self.get_table()
        _id = insert(i, s, 1)[0]
        q = query.Query()
        q.is__id(_id)
        d = i.get_one(s, q)
        self.assertGreater(len(d), 0)

        exit_code = subprocess.check_call("sudo restart spiped-pg-server", shell=True)
        time.sleep(1)

        q = query.Query()
        q.is__id(_id)
        d = i.get_one(s, q)
        self.assertGreater(len(d), 0)

        exit_code = subprocess.check_call("sudo restart spiped-pg-client", shell=True)
        time.sleep(1)

        q = query.Query()
        q.is__id(_id)
        d = i.get_one(s, q)
        self.assertGreater(len(d), 0)


class IteratorTest(TestCase):
    def get_iterator(self, count=5, limit=5, page=0):
        q = get_query()
        insert(q.orm.interface, q.orm.schema, count)
        i = q.get(limit, page)
        return i

    def test_ifilter(self):
        count = 3
        _q = get_query()
        insert(_q.orm.interface, _q.orm.schema, count)
        q = _q.copy()

        l = q.get()
        self.assertEqual(3, len(list(l)))

        l.ifilter = lambda o: o.pk == 1
        self.assertEqual(2, len(list(l)))

    def test_list_compatibility(self):
        count = 3
        _q = get_query()
        insert(_q.orm.interface, _q.orm.schema, count)

        q = _q.copy()
        l = q.get()

        self.assertTrue(bool(l))
        self.assertEqual(count, l.count())
        self.assertEqual(range(1, count + 1), list(l.foo))

        l.reverse()
        self.assertEqual(list(reversed(xrange(1, count + 1))), list(l.foo))

        r = l.pop(0)
        self.assertEqual(count, r.foo)

        r = l.pop()
        self.assertEqual(1, r.foo)

        pop_count = 0
        while l:
            pop_count += 1
            l.pop()
        self.assertGreater(pop_count, 0)

    def test_all_len(self):
        count = 10
        q = get_query()
        insert(q.orm.interface, q.orm.schema, count)
        g = q.select_foo().desc_bar().set_limit(5).set_offset(1).all()
        self.assertEqual(count, len(g))

    def test_all(self):
        count = 15
        q = get_query()
        insert(q.orm.interface, q.orm.schema, count)
        q.set_limit(5)
        g = q.all()

        self.assertEqual(1, g[0].foo)
        self.assertEqual(2, g[1].foo)
        self.assertEqual(3, g[2].foo)
        self.assertEqual(6, g[5].foo)
        self.assertEqual(13, g[12].foo)

        with self.assertRaises(IndexError):
            g[count + 5]

        for i, x in enumerate(g):
            if i > 7: break
        self.assertEqual(9, g[8].foo)

        gcount = 0
        for x in g: gcount += 1
        self.assertEqual(count, gcount)

        gcount = 0
        for x in g: gcount += 1
        self.assertEqual(count, gcount)

        self.assertEqual(count, len(g))

        g = q.all()
        self.assertEqual(count, len(g))

    def test_values(self):
        count = 5
        q = get_query()
        insert(q.orm.interface, q.orm.schema, count)
        q.set_bar()
        g = q.get().values()
        icount = 0
        for i, v in enumerate(g, 1):
            self.assertEqual(u"value {}".format(i), v)
            icount += 1
        self.assertEqual(count, icount)

        q.fields_set = []
        q.set_bar().set_foo()
        g = q.get().values()
        icount = 0
        for i, v in enumerate(g, 1):
            icount += 1
            self.assertEqual(u"value {}".format(i), v[0])
            self.assertEqual(i, v[1])
        self.assertEqual(count, icount)

        q.fields_set = []
        i = q.get()
        with self.assertRaises(ValueError):
            g = i.values()
            for v in g: pass

    def test___iter__(self):
        count = 5
        i = self.get_iterator(count)

        rcount = 0
        for t in i:
            rcount += 1
        self.assertEqual(count, rcount)

        rcount = 0
        for t in i:
            self.assertTrue(isinstance(t, Torm))
            rcount += 1
        self.assertEqual(count, rcount)

    def test___getitem__(self):
        count = 5
        i = self.get_iterator(count)
        for x in xrange(count):
            self.assertEqual(i[x].pk, i.results[x]['_id'])

        with self.assertRaises(IndexError):
            i[count + 1]

    def test___len__(self):
        count = 5
        i = self.get_iterator(count)
        self.assertEqual(len(i), count)

    def test___getattr__(self):
        count = 5
        i = self.get_iterator(count)
        rs = list(i.foo)
        self.assertEqual(count, len(rs))

        with self.assertRaises(KeyError):
            i.kadfjkadfjkhjkgfkjfkjk_bogus_field

    def test_pk(self):
        count = 5
        i = self.get_iterator(count)
        rs = list(i.pk)
        self.assertEqual(count, len(rs))

    def test_has_more(self):
        limit = 3
        count = 5
        q = get_query()
        insert(q.orm.interface, q.orm.schema, count)

        i = q.get(limit, 0)
        self.assertTrue(i.has_more)

        i = q.get(limit, 2)
        self.assertFalse(i.has_more)

        i = q.get(limit, 1)
        self.assertTrue(i.has_more)

        i = q.get(0, 0)
        self.assertFalse(i.has_more)


class QueryTest(TestCase):
    def test_query_ref(self):
        t1 = get_orm_class()
        t2 = get_orm_class()
        t2.schema.che = Field(int, ref=t1)
        orm_classpath = "{}.{}".format(t1.__module__, t1.__name__)
        q = t2.query.ref(orm_classpath)
        self.assertEqual([], q.fields_where)

        q = t2.query.ref(orm_classpath, 1)
        self.assertEqual('che', q.fields_where[0][1])
        self.assertEqual(1, q.fields_where[0][2])

    def test_query_ref_2(self):
        i = get_interface()
        prom.set_interface(i)

        testdata.create_modules({
            "qr2.foo": "\n".join([
                "import prom",
                "class Foo(prom.Orm):",
                "    schema = prom.Schema(",
                "        'qr2_foo',",
                "        foo=prom.Field(int, True),",
                "        bar=prom.Field(str, True),",
                "    )",
                ""
                "class Bar(prom.Orm):",
                "    schema = prom.Schema(",
                "        'qr2_Bar',",
                "        foo=prom.Field(int, True),",
                "        bar=prom.Field(str, True),",
                "        che=prom.Field(int, ref=Foo)",
                "    )",
                ""
            ])
        })

        from qr2.foo import Foo as t1, Bar as t2

        ti1 = t1.create(foo=11, bar='11')
        ti12 = t1.create(foo=12, bar='12')

        ti2 = t2.create(foo=21, bar='21', che=ti1.pk)
        ti22 = t2.create(foo=22, bar='22', che=ti12.pk)

        orm_classpath = "{}.{}".format(t2.__module__, t2.__name__)

        l = list(ti1.query.ref(orm_classpath, ti12.pk).select_foo().values())
        self.assertEqual(22, l[0])

        l = list(ti1.query.ref(orm_classpath, ti1.pk).select_foo().all().values())
        self.assertEqual(21, l[0])

        l = list(ti1.query.ref(orm_classpath, ti1.pk).select_foo().get().values())
        self.assertEqual(21, l[0])

        l = list(ti1.query.ref(orm_classpath, ti1.pk).select_foo().values())
        self.assertEqual(21, l[0])

        l = list(ti1.query.ref(orm_classpath).select_foo().all().values())
        self.assertEqual(2, len(l))

    def test_null_iterator(self):
        """you can now pass empty lists to in and nin and not have them throw an
        error, instead they return an empty iterator"""
        _q = get_query()
        insert(_q.orm.interface, _q.orm.schema, 1)

        q = _q.copy()
        r = q.in_foo([]).get()
        self.assertFalse(r)
        count = 0
        for x in r:
            count += 0
        self.assertEqual(0, count)
        self.assertEqual(0, len(r))

    def test_field_datetime(self):
        _q = get_query()

        q = _q.copy()
        q.is__created(day=int(datetime.datetime.utcnow().strftime('%d')))
        r = q.get()
        self.assertFalse(r)

        insert(q.orm.interface, q.orm.schema, 1)

        q = _q.copy()
        q.is__created(day=int(datetime.datetime.utcnow().strftime('%d')))
        r = q.get()
        self.assertEqual(1, len(r))

        day = int(datetime.datetime.utcnow().strftime('%d'))

        q = _q.copy()
        q.in__created(day=day)
        r = q.get()
        self.assertEqual(1, len(r))

        q = _q.copy()
        q.in__created(day=[day, day + 1])
        r = q.get()
        self.assertEqual(1, len(r))

    def test_pk_fields(self):
        tclass = get_orm_class()
        q = tclass.query.is_pk(1)
        q.in_pk([1, 2, 3])
        q.gte_pk(5).lte_pk(1).lt_pk(1).gte_pk(5)
        q.desc_pk()
        q.asc_pk()
        q.set_pk()

        for where_tuple in q.fields_where:
            self.assertEqual(where_tuple[1], tclass.schema.pk)

        for sort_tuple in q.fields_sort:
            self.assertEqual(sort_tuple[1], tclass.schema.pk)

        for set_tuple in q.fields_set:
            self.assertEqual(set_tuple[0], tclass.schema.pk)

        #self.assertEqual(q.fields_where[0][1], tclass.schema.pk)

    def test_get_pks(self):
        tclass = get_orm_class()
        t = tclass()
        t.foo = 1
        t.bar = "bar1"
        t.set()

        t2 = tclass()
        t2.foo = 2
        t2.bar = "bar2"
        t2.set()

        pks = [t.pk, t2.pk]

        res = tclass.query.get_pks(pks)
        self.assertEqual(2, len(res))
        self.assertEqual(list(res.pk), pks)

    def test_value(self):
        q = get_query()
        q.set_foo()
        v = q.value()
        self.assertEqual(None, v)
        count = 2
        insert(q.orm.interface, q.orm.schema, count)
        v = q.value()
        self.assertEqual(1, v)

        q.set_bar()
        v = q.value()
        self.assertEqual(1, v[0])
        self.assertEqual(u"value 1", v[1])

    def test_values(self):
        count = 2
        q = get_query()
        q.set_foo()
        insert(q.orm.interface, q.orm.schema, count)

        icount = 0
        for i, v in enumerate(q.values(), 1):
            icount += 1
            self.assertEqual(i, v)
        self.assertEqual(count, icount)

        q.set_bar()
        icount = 0
        for i, v in enumerate(q.values(), 1):
            icount += 1
            self.assertEqual(i, v[0])
            self.assertEqual(u"value {}".format(i), v[1])
        self.assertEqual(count, icount)

        icount = 0
        for v in q.values(limit=1):
            icount += 1
        self.assertEqual(1, icount)

    def test_pk(self):
        q = get_query()
        v = q.pk()
        self.assertEqual(None, v)
        count = 2
        insert(q.orm.interface, q.orm.schema, count)

        v = q.pk()
        self.assertEqual(1, v)

    def test_pks(self):
        q = get_query()
        v = list(q.pks())
        self.assertEqual(0, len(v))
        count = 2
        insert(q.orm.interface, q.orm.schema, count)

        v = list(q.pks())
        self.assertEqual(2, len(v))

    def test___iter__(self):
        count = 5
        q = get_query()
        insert(q.orm.interface, q.orm.schema, count)

        rcount = 0
        for t in q:
            rcount += 1

        self.assertEqual(count, rcount)

    def test_has(self):
        q = get_query()
        self.assertFalse(q.has())

        count = 1
        insert(q.orm.interface, q.orm.schema, count)
        self.assertTrue(q.has())

    def test_all(self):
        count = 10
        q = get_query()
        insert(q.orm.interface, q.orm.schema, count)

        q.set_limit(1)
        rcount = 0
        for r in q.all():
            rcount += 1
        self.assertEqual(count, rcount)

        q.set_limit(6).set_offset(0)
        rcount = 0
        for r in q.all():
            rcount += 1
        self.assertEqual(count, rcount)

    def test_in_field(self):
        q = query.Query()
        q.in_foo([])
        self.assertFalse(q.can_get)

        q = query.Query()
        q.in_foo([1, 2])
        self.assertEqual(q.fields_where[0][2], [1, 2,])

        q = query.Query()
        q.in_foo([1])
        self.assertEqual(q.fields_where[0][2], [1])

        q = query.Query()
        q.in_foo([1, 2])
        self.assertEqual(q.fields_where[0][2], [1, 2])

        q = query.Query()
        q.in_foo(xrange(1, 3))
        self.assertEqual(q.fields_where[0][2], [1, 2,])

        q = query.Query()
        q.in_foo((x for x in [1, 2]))
        self.assertEqual(q.fields_where[0][2], [1, 2,])

    def test_fields_set(self):
        q = query.Query()
        fields_select = ['foo', 'bar', 'che']
        fields = dict(zip(fields_select, [None] * len(fields_select)))
        q.set_fields(*fields_select)
        self.assertEqual(fields_select, q.fields_select)
        self.assertEqual(fields, q.fields)

        q = query.Query()
        q.set_fields(fields)
        self.assertEqual(fields_select, q.fields_select)
        self.assertEqual(fields, q.fields)

        q = query.Query()
        q.set_fields(fields_select)
        self.assertEqual(fields_select, q.fields_select)
        self.assertEqual(fields, q.fields)

        q = query.Query()
        q.set_fields(**fields)
        self.assertEqual(fields_select, q.fields_select)
        self.assertEqual(fields, q.fields)

    def test_fields_select(self):
        q = query.Query()
        fields_select = ['foo', 'bar', 'che']
        q.select_fields(*fields_select)
        self.assertEqual(fields_select, q.fields_select)

        q = query.Query()
        q.select_fields(fields_select)
        self.assertEqual(fields_select, q.fields_select)

        q = query.Query()
        q.select_fields(fields_select, 'baz')
        self.assertEqual(fields_select + ['baz'], q.fields_select)

    def test_child_magic(self):

        class ChildQuery(query.Query):
            pass

        q = ChildQuery()
        q.is_foo(1) # if there is no error, it passed

        with self.assertRaises(AttributeError):
            q.aksdlfjldks_foo(2)

    def test__split_method(self):

        tests = [
            ("get_foo", ("get", "foo")),
            ("is_foo", ("is", "foo")),
            ("gt_foo_bar", ("gt", "foo_bar")),
        ]

        q = query.Query()

        for t in tests:
            r = q._split_method(t[0])
            self.assertEqual(t[1], r)

        with self.assertRaises(ValueError):
            q._split_method("testing")

    def test___getattr__(self):
        q = query.Query()
        q.is_foo(1)
        self.assertEqual(1, len(q.fields_where))
        self.assertEqual(["is", "foo", 1, {}], q.fields_where[0])

        with self.assertRaises(AttributeError):
            q.testsfsdfsdft_fieldname(1, 2, 3)

    def test_where_field_methods(self):
        tests = [
            ("is_field", ["foo", 1], ["is", "foo", 1, {}]),
            ("not_field", ["foo", 1], ["not", "foo", 1, {}]),
            ("lte_field", ["foo", 1], ["lte", "foo", 1, {}]),
            ("lt_field", ["foo", 1], ["lt", "foo", 1, {}]),
            ("gte_field", ["foo", 1], ["gte", "foo", 1, {}]),
            ("gt_field", ["foo", 1], ["gt", "foo", 1, {}]),
            ("in_field", ["foo", (1, 2, 3)], ["in", "foo", [1, 2, 3], {}]),
            ("nin_field", ["foo", (1, 2, 3)], ["nin", "foo", [1, 2, 3], {}]),
        ]

        q = query.Query("foo")

        for i, t in enumerate(tests):
            cb = getattr(q, t[0])
            r = cb(*t[1])
            self.assertEqual(q, r)
            self.assertEqual(t[2], q.fields_where[i])

        # ("between_field", ["foo", 1, 2], [["lte", "foo", 1], ["gte", "foo", 2]]),
        q = query.Query("foo")
        q.between_field("foo", 1, 2)
        self.assertEqual([["lte", "foo", 1, {}], ["gte", "foo", 2, {}]], q.fields_where)

    def test_sort_field_methods(self):
        tests = [
            ("sort_field", ["foo", 1], [1, "foo", None]),
            ("sort_field", ["foo", -1], [-1, "foo", None]),
            ("sort_field", ["foo", 5], [1, "foo", None]),
            ("sort_field", ["foo", -5], [-1, "foo", None]),
            ("asc_field", ["foo"], [1, "foo", None]),
            ("desc_field", ["foo"], [-1, "foo", None]),
        ]

        q = query.Query("foo")

        for i, t in enumerate(tests):
            cb = getattr(q, t[0])
            r = cb(*t[1])
            self.assertEqual(q, r)
            self.assertEqual(t[2], q.fields_sort[i])

        with self.assertRaises(ValueError):
            q.sort_field("foo", 0)

    def test_bounds_methods(self):
        q = query.Query("foo")
        q.set_limit(10)
        self.assertEqual((10, 0, 11), q.get_bounds())

        q.set_page(1)
        self.assertEqual((10, 0, 11), q.get_bounds())

        q.set_offset(15)
        self.assertEqual((10, 15, 11), q.get_bounds())

        q.set_page(2)
        self.assertEqual((10, 10, 11), q.get_bounds())

        q.set_page(3)
        self.assertEqual((10, 20, 11), q.get_bounds())

        q.set_page(0)
        self.assertEqual((10, 0, 11), q.get_bounds())

        q.set_page(-10)
        self.assertEqual((10, 0, 11), q.get_bounds())

        q.set_offset(0)
        self.assertEqual((10, 0, 11), q.get_bounds())

        q.set_offset(-10)
        self.assertEqual((10, 0, 11), q.get_bounds())

        q.set_limit(0)
        self.assertEqual((0, 0, 0), q.get_bounds())

        q.set_limit(-10)
        self.assertEqual((0, 0, 0), q.get_bounds())

    def test_set(self):
        i, s = get_table()
        class TestSetTorm(Orm):
            interface = i
            schema = s

        q = query.Query(orm=TestSetTorm)
        d = q.set_fields(foo=1, bar="value 1").set()
        for field_name in ['_id', 'foo', 'bar']:
            self.assertTrue(field_name in d)
            self.assertTrue(d[field_name])

    def test_delete(self):
        tclass = get_orm_class()
        first_pk = insert(tclass.interface, tclass.schema, 1)[0]

        with self.assertRaises(ValueError):
            r = tclass.query.delete()

        r = tclass.query.is_foo(1).delete()
        self.assertEqual(1, r)

        r = tclass.query.is_foo(1).delete()
        self.assertEqual(0, r)

    def test_get(self):
        i, s = get_table()
        _ids = insert(i, s, 2)
        class TestGetTorm(Orm):
            interface = i
            schema = s

        q = query.Query(orm=TestGetTorm)
        for o in q.get():
            self.assertEqual(type(o), TestGetTorm)
            self.assertTrue(o._id in _ids)
            self.assertFalse(o.is_modified())

    def test_get_one(self):
        i, s = get_table()
        _ids = insert(i, s, 1)
        class TestGetOneTorm(Orm):
            interface = i
            schema = s

        q = query.Query(orm=TestGetOneTorm)
        o = q.get_one()
        self.assertEqual(type(o), TestGetOneTorm)
        self.assertTrue(o._id in _ids)
        self.assertFalse(o.is_modified())

    def test_first_and_last(self):
        tclass = get_orm_class()
        first_pk = insert(tclass.interface, tclass.schema, 1)[0]

        t = tclass.query.first()
        self.assertEqual(first_pk, t.pk)

        t = tclass.query.last()
        self.assertEqual(first_pk, t.pk)

        last_pk = insert(tclass.interface, tclass.schema, 1)[0]
        t = tclass.query.first()
        self.assertEqual(first_pk, t.pk)

        t = tclass.query.last()
        self.assertEqual(last_pk, t.pk)


@skipIf(gevent is None, "Skipping Gevent test because gevent module not installed")
class XInterfacePostgresGeventTest(InterfacePostgresTest):
    """this class has an X to start so that it will run last when all tests are run"""
    @classmethod
    def setUpClass(cls):
        import gevent.monkey
        gevent.monkey.patch_all()

        import prom.gevent
        prom.gevent.patch_all()

    def create_interface(self):
        orig_url = os.environ["PROM_POSTGRES_URL"]
        os.environ["PROM_POSTGRES_URL"] += '?async=1&pool_maxconn=3&pool_class=prom.gevent.ConnectionPool'
        try:
            i = super(XInterfacePostgresGeventTest, self).create_interface()
        finally:
            os.environ["PROM_POSTGRES_URL"] = orig_url

        return i

    def test_concurrency(self):
        i = self.get_interface()
        start = time.time()
        for _ in range(4):
            gevent.spawn(i.query, 'select pg_sleep(1)')

        gevent.wait()
        stop = time.time()
        elapsed = stop - start
        self.assertTrue(elapsed >= 2.0 and elapsed < 3.0)

#    def test_monkey_patch(self):
#        i = self.get_interface()
#        prom.interface.set_interface(i, "foo")
#        i = self.get_interface()
#        prom.interface.set_interface(i, "bar")
#
#        for n in ['foo', 'bar']:
#            i = prom.interface.get_interface(n)
#            start = time.time()
#            for _ in range(4):
#                gevent.spawn(i.query, 'select pg_sleep(1)')
#
#            gevent.wait()
#            stop = time.time()
#            elapsed = stop - start
#            self.assertTrue(elapsed >= 1.0 and elapsed < 2.0)

    def test_concurrent_error_recovery(self):
        """when recovering from an error in a green thread environment one thread
        could have added the table while the other thread was asleep, this will
        test to make sure two threads failing at the same time will both recover
        correctly"""
        i = self.get_interface()
        s = get_schema()
        #i.set_table(s)
        for x in range(1, 3):
            gevent.spawn(i.insert, s, {'foo': x, 'bar': str(x)})

        gevent.wait()

        q = query.Query()
        r = list(i.get(s, q))
        self.assertEqual(2, len(r))

    def test_table_recovery(self):
        i = self.get_interface()
        s = get_schema()

        q = query.Query()
        l = i.get(s, q)
        self.assertEqual([], l)


# not sure I'm a huge fan of this solution to remove common parent from testing queue
# http://stackoverflow.com/questions/1323455/python-unit-test-with-base-and-sub-class
del(BaseTestInterface)

