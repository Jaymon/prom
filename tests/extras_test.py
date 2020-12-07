# -*- coding: utf-8 -*-
from __future__ import unicode_literals, division, print_function, absolute_import
import time
from threading import Thread

from prom.extras.model import MagicOrm
from prom.extras.query import CacheQuery
from . import TestCase, EnvironTestCase, testdata
from .query_test import QueryTest


class MagicOrmTest(EnvironTestCase):
    def create_1(self, **kwargs):
        class O1(MagicOrm):
            table_name = self.get_table_name("o1_magicorm")
            interface = self.get_interface()
            bar = MagicOrm.Field(bool)
            che = MagicOrm.Field(str)

        return O1(**kwargs)

    def create_2(self, **kwargs):
        o1 = self.create_1()
        class O2(MagicOrm):
            table_name = self.get_table_name("o2_magicorm")
            interface = self.get_interface()
            o1_id = MagicOrm.Field(o1.__class__)

        return O2(**kwargs)

    def test_pk(self):
        o = self.create_1(_id=200)
        self.assertEqual(200, o.o1_id)

    def test_is(self):
        o = self.create_1(bar=True, che="che")

        self.assertTrue(o.is_bar())
        self.assertTrue(o.is_che("che"))
        self.assertFalse(o.is_che("bar"))

        o.bar = False
        self.assertFalse(o.is_bar())

    def test_fk(self):
        o1 = self.create_1(bar=False, che="1")
        o1.save()
        self.assertLess(0, o1.pk)
        self.assertFalse(o1.bar)

        o2 = self.create_2(o1_id=o1.pk)

        r1 = o2.o1
        self.assertEqual(o1.pk, r1.pk)
        self.assertEqual(o1.bar, r1.bar)
        self.assertEqual(o1.che, r1.che)

    def test_jsonable(self):
        o = self.create_1(_id=500, bar=False, che="1")
        d = o.jsonable()
        self.assertTrue(o.pk_name in d)
        self.assertFalse("_id" in d)

    def test_attribute_error(self):
        o = self.create_2()
        self.assertIsNone(o.o1)
        with self.assertRaises(AttributeError):
            o.blahblah

    def test___getattr___error(self):
        class O4(MagicOrm):
            @property
            def foo(self):
                raise KeyError("This error should not be buried")

        o = O4()
        with self.assertRaises(KeyError):
            o.foo

        class O3(MagicOrm):
            @property
            def foo(self):
                raise ValueError("This error should not be buried")

        o = O3()
        with self.assertRaises(ValueError):
            o.foo


class CacheQueryTest(QueryTest):
    def setUp(self):
        CacheQuery.cached = {} # clear cache between tests
        super(CacheQueryTest, self).setUp()

    def get_orm_class(self, *args, **kwargs):
        orm_class = super(CacheQueryTest, self).get_orm_class(*args, **kwargs)
        orm_class.query_class = CacheQuery
        orm_class.query_class.cache_activate(True)
        return orm_class

    def test_cache_hit(self):
        orm_class = self.get_orm_class()
        self.insert(orm_class, 10)

        start = time.time()
        q = orm_class.query
        ref_pks = q.pks()
        stop = time.time()
        ref_duration = stop - start

        self.assertEqual(10, len(ref_pks))
        self.assertFalse(q.cache_hit)

        ref_pks = list(ref_pks)
        for x in range(10):
            start = time.time()
            q = orm_class.query
            pks = q.pks()
            stop = time.time()
            duration = stop - start
            #self.assertLess(duration, ref_duration)
            self.assertTrue(q.cache_hit)
            self.assertEqual(ref_pks, list(pks))

    def test_cache_contextmanager(self):
        orm_class = self.get_orm_class()
        orm_class.query_class.cache_activate(False)
        self.insert(orm_class, 10)

        with orm_class.query.cache(60):
            self.assertTrue(orm_class.query_class.cache_namespace.active)

        self.assertFalse(orm_class.query_class.cache_namespace.active)

    def test_cache_threading(self):

        orm_class = self.get_orm_class()
        orm_class.query_class.cache_activate(False)

        def one():
            with orm_class.query.cache():
                time.sleep(0.5)
                self.assertTrue(orm_class.query_class.cache_namespace.active)

        for x in range(500):
            self.assertFalse(orm_class.query_class.cache_namespace.active)

        t1 = Thread(target=one)
        t1.start()
        t1.join()

        self.assertFalse(orm_class.query_class.cache_namespace.active)

