# -*- coding: utf-8 -*-
from __future__ import unicode_literals, division, print_function, absolute_import

import testdata

from . import BaseTestCase, TestCase
from prom.utils import get_objects


class GetObjectsTest(TestCase):
    def test_relative_import(self):
        basedir = testdata.create_modules({
            "relimp.foo.bar": [
                "class Bar(object): pass",
                "",
            ],
            "relimp.foo.che": [
                "class Che(object): pass",
                ""
            ],
            "relimp.foo": [
                "class Foo(object): pass",
                ""
            ],
            "relimp": [
                "class Relimp(object): pass",
                ""
            ],
            "relimp.baz": [
                "class Baz(object): pass",
                ""
            ]

        })

        with self.assertRaises(ImportError):
            module, klass = get_objects('...too.far', "relimp.foo.bar")

        module, klass = get_objects('...baz.Baz', "relimp.foo.bar")
        self.assertEqual("relimp.baz", module.__name__)
        self.assertEqual("Baz", klass.__name__)

        module, klass = get_objects('..che.Che', "relimp.foo.bar")
        self.assertEqual("relimp.foo.che", module.__name__)
        self.assertEqual("Che", klass.__name__)

        module, klass = get_objects('...foo.Foo', "relimp.foo.bar")
        self.assertEqual("relimp.foo", module.__name__)
        self.assertEqual("Foo", klass.__name__)

        module, klass = get_objects('relimp.Relimp', "relimp.foo.bar")
        self.assertEqual("relimp", module.__name__)
        self.assertEqual("Relimp", klass.__name__)

