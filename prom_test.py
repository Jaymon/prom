import unittest
import os
import sys

from prom import query


class PromQueryTest(unittest.TestCase):

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

