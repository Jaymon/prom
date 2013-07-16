from unittest import TestCase
import os
import sys

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
        s = Schema(
            "foobar_table",
            foo=(int, True),
            bar=(str, True),
            index_ifoobar=("foo", "bar")
        )

        r = i.has_table(s.table)
        self.assertFalse(r)

        r = i.set_table(s)

        r = i.has_table(s.table)
        self.assertTrue(r)


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

