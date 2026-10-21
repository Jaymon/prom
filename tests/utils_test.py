# -*- coding: utf-8 -*-

from . import TestCase
from prom.utils import (
    make_list,
)


class MakeListTest(TestCase):
    def test_list(self):
        a = [
            [1, 2, 3],
            4,
        ]
        self.assertEqual([1, 2, 3, 4], make_list(a))

        a = [
            "one",
            "two",
            "three",
        ]
        self.assertEqual(a, make_list(a))

        a = "one"
        self.assertEqual([a], make_list([a]))

        a = 1
        self.assertEqual([a], make_list([a]))

    def test_non_list(self):
        r = make_list(1)
        self.assertEqual([1], r)

        r = make_list("foo")
        self.assertEqual(["foo"], r)

        r = make_list(self.get_past_datetime())
        self.assertEqual(1, len(r))

