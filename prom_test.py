from unittest import TestCase
import os
import sys

from prom import query, connection
from prom.config import Schema

from prom.interface.postgres import Interface as PGInterface

class SchemaTest(TestCase):
    def test_define(self):
        s = Schema()
        s.foo = int, True
        s.bar = str, False, 32
        s.baz = str

        pout.v(s.fields)

        s.index_che = s.foo, s.bar
        s.index_created = s._created
        s.unique = s.foo

        pout.v(s.indexes)

        s.index_this_will_fail = 'testfieldname'





class ConnectionConfigTest(TestCase):
    def test_host(self):
        tests = [
            ("localhost:8000", ["localhost", 8000]),
            ("localhost", ["localhost", 0]),
            ("http://localhost:10", ["localhost", 10]),
            ("http://some.crazydomain.com", ["some.crazydomain.com", 0]),
            ("http://some.crazydomain.com:1000", ["some.crazydomain.com", 1000]),
        ]

        for t in tests:
            p = connection.Config()
            p.host = t[0]
            self.assertEqual(t[1][0], p.host)
            self.assertEqual(t[1][1], p.port)

        p = connection.Config()
        p.port = 555
        p.host = "blah.example.com"
        self.assertEqual("blah.example.com", p.host)
        self.assertEqual(555, p.port)

        p.host = "blah.example.com:43"
        self.assertEqual("blah.example.com", p.host)
        self.assertEqual(43, p.port)

class InterfacePostgresTest(TestCase):

    def get_interface(self):
        config = connection.Config()
        config.database = "vagrant"
        config.username = "vagrant"
        config.password = "vagrant"
        config.host = "localhost"

        i = PGInterface()
        i.connect(config)
        self.assertTrue(i.connection is not None)
        return i

    def test_connect(self):
        i = self.get_interface()

    def test_query(self):
        i = self.get_interface()
        rows = i.query('SELECT version()')
        self.assertGreater(len(rows), 0)





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

