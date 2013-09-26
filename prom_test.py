from unittest import TestCase
import os
import sys
import random
import string
import logging
import decimal
import datetime
import time
import subprocess

import testdata

from prom import query, Orm
from prom.config import Schema, Connection, DsnConnection
from prom.interface import postgres
from prom.interface.postgres import Interface as PGInterface
import prom

# configure root logger
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
log_handler = logging.StreamHandler(stream=sys.stderr)
log_formatter = logging.Formatter('[%(levelname)s] %(message)s')
log_handler.setFormatter(log_formatter)
logger.addHandler(log_handler)

def setUpModule():
    """
    http://docs.python.org/2/library/unittest.html#setupmodule-and-teardownmodule
    """
    i = get_interface()
    i.delete_tables(disable_protection=True)

def get_interface():
    config = DsnConnection(os.environ["PROM_POSTGRES_URL"])

    i = PGInterface()
    i.connect(config)
    assert i.connection is not None
    assert i.connected
    return i

def get_table_name():
    """return a random table name"""
    return "".join(random.sample(string.ascii_lowercase, random.randint(5, 15)))

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

def get_table(table_name=None):
    """
    return an interface and schema for a table in the db
    
    return -- tuple -- interface, schema
    """
    i = get_interface()
    s = get_schema(table_name)
    i.set_table(s)
    return i, s

def insert(interface, schema, count):
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
        d = interface.set(schema, q)

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

    def test_normalize(self):
        table_name = get_table_name()
        Torm.schema = Schema(
            table_name,
            foo=(int, True),
            bar=(str, True),
            che=(str, False),
        )

        with self.assertRaises(KeyError):
            Torm.normalize({})

        d = Torm.normalize({'foo': 1, 'bar': 'bar1'})
        self.assertTrue('foo' in d)
        self.assertTrue('bar' in d)
        self.assertFalse('che' in d)

        d_in = {'foo': 2, 'bar': 'bar2', 'che': 'che2'}
        d = Torm.normalize(d_in)
        self.assertEquals(d, d_in)

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

    def test_query_class(self):
        """
        I just wanted to make sure you can set the query class and it is picked up
        correctly, also, defining a class in a function is a special case that I wanted
        to see how it was handled
        """
        class QueryClassTormQuery(query.Query):
            pass

        class QueryClassTorm(Orm):
            query_class = QueryClassTormQuery
            pass

        self.assertEqual(QueryClassTorm.query_class, QueryClassTormQuery)
        self.assertEqual(Torm.query_class, query.Query)
        self.assertEqual(Torm2.query_class, Torm2Query)

    def test_interface(self):
        i = Torm.interface
        self.assertFalse(i is None)

        i = Torm2.interface
        self.assertFalse(i is None)

        # even though connection name has changed, interface was cached, so there shouldn't
        # be a problem, (I think this should be expected behavior, how often would this really happen?)
        Torm2.connection_name = "blkasdfjksdafjdkfklsd"
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


class PromTest(TestCase):

    def setUp(self):
        prom.interfaces = {}

    def test_configure(self):
        dsn = 'prom.interface.postgres.Interface://username:password@localhost/db'
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
        class FailureSetTorm(Orm):
            interface = get_interface()
            schema = get_schema()

        f = FailureSetTorm(foo=1, bar="value 1")
        f.set()
        self.assertTrue(f.pk)

    def test_failure_get(self):
        """
        test to make sure getting on a table that doesn't exist works without raising
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

        s.set_field("foo", int)
        self.assertEqual({'name': "foo", 'type': int, 'required': False}, s.fields["foo"])

        with self.assertRaises(ValueError):
            s.set_field("foo", int)

        s.set_field("bar", int, True)
        self.assertEqual({'name': "bar", 'type': int, 'required': True}, s.fields["bar"])

        s.set_field("che", int, True, {"size": 10})
        self.assertEqual({'name': "che", 'type': int, 'required': True, "size": 10}, s.fields["che"])

        with self.assertRaises(ValueError):
            s.set_field("baz", int, True, {"min_size": 10})

        s = Schema("foo")
        s.set_field("foo", int, options={"size": 10, "max_size": 50})
        self.assertEqual({'name': "foo", 'type': int, 'required': False, "size": 10}, s.fields["foo"])

        s = Schema("foo")
        s.set_field("foo", int, options={"min_size": 10, "max_size": 50})
        self.assertEqual({'name': "foo", 'type': int, 'required': False, "min_size": 10, "max_size": 50}, s.fields["foo"])
        self.assertFalse("foo" in s.indexes)

        s = Schema("foo")
        s.set_field("foo", int, options={"unique": True})
        self.assertEqual({'name': "foo", 'type': int, 'required': False}, s.fields["foo"])
        self.assertEqual({'name': "foo", 'fields': ["foo"], 'unique': True}, s.indexes["foo"])

        s = Schema("foo")
        s.set_field("foo", int, options={"ignore_case": True})
        self.assertEqual({'name': "foo", 'type': int, 'required': False, 'ignore_case': True}, s.fields["foo"])

    def test___setattr__field(self):
        s = Schema("foo")
        s.bar = int, True
        self.assertEqual({'name': "bar", 'type': int, 'required': True}, s.fields["bar"])

        s.che = int
        self.assertEqual({'name': "che", 'type': int, 'required': False}, s.fields["che"])

        s.foobar = int,
        self.assertEqual({'name': "foobar", 'type': int, 'required': False}, s.fields["foobar"])

        s.baz = int, True, {"size": 10}
        self.assertEqual({'name': "baz", 'type': int, 'required': True, "size": 10}, s.fields["baz"])

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
        os.environ['PROM_DSN'] = "prom.interface.postgres.Interface://localhost:5000/database#i0"
        os.environ['PROM_DSN_1'] = "prom.interface.postgres.Interface://localhost:5000/database#i1"
        os.environ['PROM_DSN_2'] = "prom.interface.postgres.Interface://localhost:5000/database#i2"
        os.environ['PROM_DSN_4'] = "prom.interface.postgres.Interface://localhost:5000/database#i4"
        prom.configure_environ()
        self.assertTrue('i0' in prom.interfaces)
        self.assertTrue('i1' in prom.interfaces)
        self.assertTrue('i2' in prom.interfaces)
        self.assertTrue('i3' not in prom.interfaces)
        self.assertTrue('i4' not in prom.interfaces)

        prom.interfaces.pop('i0', None)
        prom.interfaces.pop('i1', None)
        prom.interfaces.pop('i2', None)
        prom.interfaces.pop('i3', None)
        prom.interfaces.pop('i4', None)

    def test_dsn(self):
        tests = [
            (
                "some.Backend://username:password@localhost:5000/database?option=1&var=2#fragment",
                {
                    'username': "username",
                    'interface_name': "some.Backend",
                    'database': "database",
                    'host': "localhost",
                    'port': 5000,
                    'password': "password",
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
            ("http://localhost:10", ["localhost", 10]),
            ("http://some.crazydomain.com", ["some.crazydomain.com", 0]),
            ("http://some.crazydomain.com:1000", ["some.crazydomain.com", 1000]),
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


class InterfacePostgresTest(TestCase):
    def test_connect(self):
        i = get_interface()

    def test_close(self):
        i = get_interface()
        i.close()
        self.assertTrue(i.connection is None)
        self.assertFalse(i.connected)

    def test_query(self):
        i = get_interface()
        rows = i.query('SELECT version()')
        self.assertGreater(len(rows), 0)

    def test_set_table(self):
        i = get_interface()
        s = get_schema()
        r = i.has_table(s.table)
        self.assertFalse(r)

        r = i.set_table(s)

        r = i.has_table(s.table)
        self.assertTrue(r)

        # make sure it persists
        i.close()
        i = get_interface()
        self.assertTrue(i.has_table(s.table))

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
            five=(float, True,),
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
            'five': 1.987654321,
            'six': 4000000000,
            'seven': s_ref_id,
            'eight': datetime.datetime(2005, 7, 14, 12, 30),
            'nine': datetime.date(2005, 9, 14),
        }
        o = i.insert(s, d)
        q = query.Query()
        q.is__id(o[s.pk])
        odb = i.get_one(s, q)
        d['five'] = 1.98765
        self.assertEqual(d, odb)

    def test_get_tables(self):
        i = get_interface()
        s = get_schema()
        r = i.set_table(s)
        r = i.get_tables()
        self.assertTrue(s.table in r)

        r = i.get_tables(s.table)
        self.assertTrue(s.table in r)

    def test_delete_table(self):
        i = get_interface()
        s = get_schema()

        r = i.set_table(s)
        self.assertTrue(i.has_table(s.table))

        r = i.delete_table(s)
        self.assertFalse(i.has_table(s.table))

        # make sure it persists
        i.close()
        i = get_interface()
        self.assertFalse(i.has_table(s.table))

    def test_delete_tables(self):

        i = get_interface()
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
        i, s = get_table()
        d = {
            'foo': 1,
            'bar': 'this is the value',
        }

        rd = i.insert(s, d)
        self.assertGreater(rd[s._id], 0)

    def test_set_insert(self):
        """test just the insert portion of set"""
        i, s = get_table()
        q = query.Query()

        q.set_fields({
            'foo': 1,
            'bar': 'this is the value',
        })

        rd = i.set(s, q)
        self.assertGreater(rd[s._id], 0)

    def test_get_sql(self):
        i = get_interface()
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
        i, s = get_table()
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

    def test_get(self):
        i, s = get_table()
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
        i, s = get_table()
        _ids = insert(i, s, 5)

        q = None
        l = i.get(s, q)
        self.assertEqual(5, len(l))

    def test_get_pagination(self):
        """test get but moving through the results a page at a time to make sure limit and offset works"""
        i, s = get_table()
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
        i, s = get_table()

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
        i, s = get_table()
        _ids = insert(i, s, 5)

        q = query.Query()
        q.in__id(_ids)
        l = i.get(s, q)
        self.assertEqual(5, len(l))

        i.delete(s, q)

        l = i.get(s, q)
        self.assertEqual(0, len(l))

        # make sure it stuck
        i.close()
        i = get_interface()
        l = i.get(s, q)
        self.assertEqual(0, len(l))

    def test_update(self):
        i, s = get_table()
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
        i = get_interface()
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
        i = get_interface()
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
        i = get_interface()
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

    def test_handle_error_column(self):
        i, s = get_table()
        s.che = str, True
        q = query.Query()
        q.set_fields({
            'foo': 1,
            'bar': 'v1',
            'che': "this field will cause the query to fail",
        })

        with self.assertRaises(postgres.psycopg2.ProgrammingError):
            rd = i.set(s, q)

    def test_transaction_nested_fail_1(self):
        """make sure 2 new tables in a wrapped transaction work as expected"""
        i = get_interface()
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

        i.transaction_start()
        q1 = query.Query()
        q1.set_foo(1)
        d1 = i.set(s1, q1)

        q2 = query.Query()
        q2.set_bar(2).set_s_pk(d1['_id'])
        d2 = i.set(s2, q2)
        i.transaction_stop()

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
        i = get_interface()
        table_name_1 = get_table_name()
        table_name_2 = get_table_name()
#        table_name_1 = "table_1"
#        table_name_2 = "table_2"

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

        i.transaction_start()
        q1 = query.Query()
        q1.set_foo(1)
        d1 = i.set(s1, q1)

        q2 = query.Query()
        q2.set_bar(2).set_s_pk(d1['_id'])
        d2 = i.set(s2, q2)
        i.transaction_stop()

        q1 = query.Query()
        q1.is__id(d1['_id'])
        r1 = i.get_one(s1, q1)
        self.assertEqual(r1['_id'], d1['_id'])

        q2 = query.Query()
        q2.is__id(d2['_id'])
        r2 = i.get_one(s2, q2)
        self.assertEqual(r2['_id'], d2['_id'])
        self.assertEqual(r2['s_pk'], d1['_id'])

    def test_unique(self):
        i = get_interface()
        s = get_schema()
        s.should_be_unique = int, True, dict(unique=True)
        i.set_table(s)

        d = i.insert(s, {'foo': 1, 'bar': 'v1', 'should_be_unique': 1})

        with self.assertRaises(Exception):
            d = i.insert(s, {'foo': 2, 'bar': 'v2', 'should_be_unique': 1})

    def test_index_ignore_case(self):
        i = get_interface()
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
        i, s = get_table()
        _ids = insert(i, s, 5)

        q = query.Query()
        q.in__id(_ids)
        l = list(i.get(s, q))

        self.assertEqual(len(l), 5)

    def test_no_db_error(self):
        # we want to replace the db with a bogus db error
        i, s = get_table()
        #i = get_interface()
        config = i.connection_config
        config.database = 'this_is_a_bogus_db_name'
        i = PGInterface(config)
        q = query.Query()
        q.set_fields({
            'foo': 1,
            'bar': 'v1',
        })
        with self.assertRaises(postgres.psycopg2.OperationalError):
            rd = i.set(s, q)


class QueryTest(TestCase):

    def test_in_field(self):
        q = query.Query()
        q.in_foo([1, 2])
        self.assertEqual(q.fields_where[0][2], [1, 2,])

        q = query.Query()
        q.in_foo([1])
        self.assertEqual(q.fields_where[0][2], [1])

        q = query.Query()
        q.in_foo([1, 2])
        self.assertEqual(q.fields_where[0][2], [1, 2])

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
        self.assertEqual(["is", "foo", 1], q.fields_where[0])

        with self.assertRaises(AttributeError):
            q.testsfsdfsdft_fieldname(1, 2, 3)

    def test_where_field_methods(self):
        tests = [
            ("is_field", ["foo", 1], ["is", "foo", 1]),
            ("not_field", ["foo", 1], ["not", "foo", 1]),
            ("lte_field", ["foo", 1], ["lte", "foo", 1]),
            ("lt_field", ["foo", 1], ["lt", "foo", 1]),
            ("gte_field", ["foo", 1], ["gte", "foo", 1]),
            ("gt_field", ["foo", 1], ["gt", "foo", 1]),
            ("in_field", ["foo", (1, 2, 3)], ["in", "foo", (1, 2, 3)]),
            ("nin_field", ["foo", (1, 2, 3)], ["nin", "foo", (1, 2, 3)]),
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
        self.assertEqual([["lte", "foo", 1], ["gte", "foo", 2]], q.fields_where)

    def test_sort_field_methods(self):
        tests = [
            ("sort_field", ["foo", 1], [1, "foo"]),
            ("sort_field", ["foo", -1], [-1, "foo"]),
            ("sort_field", ["foo", 5], [1, "foo"]),
            ("sort_field", ["foo", -5], [-1, "foo"]),
            ("asc_field", ["foo"], [1, "foo"]),
            ("desc_field", ["foo"], [-1, "foo"]),
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

    def test_db_disconnect(self):
        """make sure interface can recover if the db disconnects mid script execution"""
        i, s = get_table()
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

    def test_transaction_context_manager(self):
        """make sure the with transaction() context manager works as expected"""
        i, s = get_table()
        _id = None
        with i.transaction():
            _id = insert(i, s, 1)[0]

        self.assertTrue(_id)

        q = query.Query()
        q.is__id(_id)
        d = i.get_one(s, q)
        self.assertGreater(len(d), 0)

        with self.assertRaises(RuntimeError):
            with i.transaction():
                _id = insert(i, s, 1)[0]
                raise RuntimeError("this should fail")

        q = query.Query()
        q.is__id(_id)
        d = i.get_one(s, q)
        self.assertEqual(len(d), 0)






