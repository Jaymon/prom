from unittest import TestCase
import os
import sys
import random
import string

from prom import query
from prom.config import Schema, Connection


from prom.interface.postgres import Interface as PGInterface

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

        s.set_index("testing", [s.che], True)
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

        self.assertEqual(s._id, s.primary_key)

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

    def get_interface(self):
        # TODO change all this to use an environment variable DSN
        config = Connection()
        config.database = "vagrant"
        config.username = "vagrant"
        config.password = "vagrant"
        config.host = "localhost"

        i = PGInterface()
        i.connect(config)
        self.assertTrue(i.connection is not None)
        self.assertTrue(i.connected)
        return i

    def get_schema(self, table_name=None):
        if not table_name:
            table_name = "".join(random.sample(string.ascii_lowercase, random.randint(5, 15)))

        s = Schema(
            table_name,
            foo=(int, True),
            bar=(str, True),
            index_ifoobar=("foo", "bar")
        )

        return s

    def get_table(self, table_name=None):
        """
        return an interface and schema for a table in the db
        
        return -- tuple -- interface, schema
        """
        i = self.get_interface()
        s = self.get_schema(table_name)
        i.set_table(s)
        return i, s

    def insert(self, interface, schema, count):
        """
        insert count rows into schema using interface
        """
        _ids = []

        for i in xrange(1, count + 1):
            d = {
                'foo': i,
                'bar': 'value {}'.format(i)
            }
            d = interface.set(schema, d)

            self.assertTrue('foo' in d)
            self.assertTrue('bar' in d)
            self.assertTrue(schema._id in d)
            _ids.append(d[schema._id])

        return _ids

    def test_connect(self):
        i = self.get_interface()

    def test_close(self):
        i = self.get_interface()
        i.close()
        self.assertTrue(i.connection is None)
        self.assertFalse(i.connected)

    def test_query(self):
        i = self.get_interface()
        rows = i.query('SELECT version()')
        self.assertGreater(len(rows), 0)

    def test_set_table(self):
        i = self.get_interface()
        s = self.get_schema()
        r = i.has_table(s.table)
        self.assertFalse(r)

        r = i.set_table(s)

        r = i.has_table(s.table)
        self.assertTrue(r)

        # make sure it persists
        i.close()
        i = self.get_interface()
        self.assertTrue(i.has_table(s.table))

        # make sure known indexes are there
        indexes = i.get_indexes(s)
        count = 0
        for known_index_name, known_index_d in s.indexes.iteritems():
            for index_name, index_fields in indexes.iteritems():
                if known_index_d['fields'] == index_fields:
                    count += 1

        self.assertEqual(len(s.indexes), count)

    def test_get_tables(self):
        i = self.get_interface()
        s = self.get_schema()
        r = i.set_table(s)
        r = i.get_tables()
        self.assertTrue(s.table in r)

        r = i.get_tables(s.table)
        self.assertTrue(s.table in r)

    def test_delete_table(self):
        i = self.get_interface()
        s = self.get_schema()

        r = i.set_table(s)
        self.assertTrue(i.has_table(s.table))

        r = i.delete_table(s)
        self.assertFalse(i.has_table(s.table))

        # make sure it persists
        i.close()
        i = self.get_interface()
        self.assertFalse(i.has_table(s.table))

    def test_insert(self):
        i = self.get_interface()
        s = self.get_schema()
        i.set_table(s)

        d = {
            'foo': 1,
            'bar': 'this is the value',
        }

        rd = i.insert(s, d)
        self.assertGreater(rd[s._id], 0)

    def test_get_sql(self):
        i = self.get_interface()
        s = self.get_schema()
        q = query.Query()
        q.in__id(*range(1, 5))
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
        _ids = self.insert(i, s, 2)

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
        i, s = self.get_table()
        _ids = self.insert(i, s, 5)

        q = query.Query()
        q.in__id(*_ids)
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
        i, s = self.get_table()
        _ids = self.insert(i, s, 5)

        q = None
        l = i.get(s, q)
        self.assertEqual(5, len(l))

    def test_get_pagination(self):
        i, s = self.get_table()
        _ids = self.insert(i, s, 12)

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
        _ids = self.insert(i, s, 5)
        q = query.Query()
        r = i.count(s, q)
        self.assertEqual(5, r)

    def test_delete(self):
        i, s = self.get_table()
        _ids = self.insert(i, s, 5)

        q = query.Query()
        q.in__id(*_ids)
        l = i.get(s, q)
        self.assertEqual(5, len(l))

        i.delete(s, q)

        l = i.get(s, q)
        self.assertEqual(0, len(l))

        # make sure it stuck
        i.close()
        i = self.get_interface()
        l = i.get(s, q)
        self.assertEqual(0, len(l))

    def test_update(self):
        i = self.get_interface()
        s = self.get_schema()
        i.set_table(s)

        d = {
            'foo': 1,
            'bar': 'value 1',
        }

        rd = i.insert(s, d)
        self.assertGreater(rd[s._id], 0)

        rd['foo'] = 2
        rd['bar'] = 'value 2'
        d = dict(rd)

        ud = i.update(s, d[s._id], d)

        self.assertEqual(ud['foo'], d['foo'])
        self.assertEqual(ud['bar'], d['bar'])
        self.assertEqual(ud[s._id], d[s._id])


class QueryTest(TestCase):

    def test_split_method(self):

        tests = [
            ("get_foo", ("get", "foo")),
            ("is_foo", ("is", "foo")),
            ("gt_foo_bar", ("is", "foo_bar")),
        ]

        q = query.Query("foo")

        for t in tests:
            r = q.split_method(t[0])
            self.assertEqual(t[1], r)

        with self.assertRaises(ValueError):
            q.split_method("testing")

    def test___getattr__(self):
        
        q = query.Query("foo")
        q.is_foo(1)
        self.assertEqual(1, len(q.fields_where))
        self.assertEqual(["is", "foo", 1], q.fields_where[0])

        with self.assertRaises(AttributeError):
            q.testsfsdfsdft_fieldname(1, 2, 3)

    def test_where_field_methods(self):
        tests = [
            ("set_field", ["foo", 1], ["is", "foo", 1]),
            ("is_field", ["foo", 1], ["is", "foo", 1]),
            ("not_field", ["foo", 1], ["not", "foo", 1]),
            ("lte_field", ["foo", 1], ["lte", "foo", 1]),
            ("lt_field", ["foo", 1], ["lt", "foo", 1]),
            ("gte_field", ["foo", 1], ["gte", "foo", 1]),
            ("gt_field", ["foo", 1], ["gt", "foo", 1]),
            ("in_field", ["foo", 1, 2, 3], ["in", "foo", (1, 2, 3)]),
            ("nin_field", ["foo", 1, 2, 3], ["nin", "foo", (1, 2, 3)]),
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

