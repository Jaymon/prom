# -*- coding: utf-8 -*-
from __future__ import unicode_literals, division, print_function, absolute_import
from unittest import skipIf
import os
import datetime
import time
import subprocess
from uuid import UUID

# needed to test prom with greenthreads
try:
    import gevent
except ImportError as e:
    gevent = None

from prom import query
from prom.query import Query, Field as QueryField
from prom.compat import *
from prom.config import Schema, DsnConnection, Field
from prom.interface.postgres import PostgreSQL
import prom
import prom.interface

from . import BaseTestInterface, testdata


class InterfaceTest(BaseTestInterface):
    @classmethod
    def create_interface(cls):
        return cls.create_postgres_interface()

    def test_table_persist(self):
        i = self.get_interface()
        s = self.get_schema()
        r = i.has_table(str(s))
        self.assertFalse(r)

        r = i.set_table(s)

        r = i.has_table(str(s))
        self.assertTrue(r)

        # make sure it persists
        i.close()
        i = self.get_interface()
        self.assertTrue(i.has_table(str(s)))

    def test_set_table_postgres(self):
        """test some postgres specific things"""
        i = self.get_interface()
        s = prom.Schema(
            self.get_table_name(),
            _id=Field(int, pk=True),
            four=Field(float, True, size=10),
            five=Field(float, True),
            six=Field(long, True),
        )
        r = i.set_table(s)
        d = {
            'four': 1.987654321,
            'five': 1.98765,
            'six': 4000000000,
        }
        pk = i.insert(s, d)
        q = Query()
        q.is__id(pk)
        odb = i.get_one(s, q)
        for k, v in d.items():
            self.assertEqual(v, odb[k])

    def test_db_disconnect(self):
        """make sure interface can recover if the db disconnects mid script execution"""
        i, s = self.get_table()
        _id = self.insert(i, s, 1)[0]
        d = i.get_one(s, Query().eq__id(_id))
        self.assertLess(0, len(d))

        testdata.restart_service("postgresql")

        d = i.get_one(s, Query().eq__id(_id))
        self.assertLess(0, len(d))

    def test_no_connection(self):
        """this will make sure prom handles it gracefully if there is no connection available ever"""
        postgresql = testdata.stop_service("postgresql", ignore_failure=False)
        time.sleep(1)

        try:
            i = self.create_interface()
            s = self.get_schema()
            q = Query()
            with self.assertRaises(prom.InterfaceError):
                i.get(s, q)

        finally:
            postgresql.start()

    def test__normalize_val_SQL_eq(self):
        orm_class = self.get_orm_class(
            ts=Field(datetime.datetime, True)
        )

        fstr, fargs = orm_class.query.eq_ts(day=10).render(placeholder=True)
        self.assertTrue('EXTRACT(DAY FROM "ts") = %s' in fstr)
        self.assertEqual(10, fargs[0])

        fstr, fargs = orm_class.query.eq_ts(day=11, hour=12).render(placeholder=True)
        self.assertTrue('EXTRACT(DAY FROM "ts") = %s AND EXTRACT(HOUR FROM "ts") = %s' in fstr)
        self.assertEqual(11, fargs[0])
        self.assertEqual(12, fargs[1])

        fstr, fargs = orm_class.query.eq_ts(None).render(placeholder=True)
        self.assertTrue('"ts" IS %s' in fstr)

        fstr, fargs = orm_class.query.ne_ts(None).render(placeholder=True)
        self.assertTrue('"ts" IS NOT %s' in fstr)

        with self.assertRaises(KeyError):
            fstr, fargs = orm_class.query.is_ts(bogus=5).render(placeholder=True)

    def test__normalize_val_SQL_in(self):
        orm_class = self.get_orm_class(
            ts=Field(datetime.datetime, True)
        )

        fstr, fargs = orm_class.query.in_ts(day=10).render(placeholder=True)
        self.assertTrue('EXTRACT(DAY FROM "ts") IN (%s)' in fstr)

        fstr, fargs = orm_class.query.in_ts(day=[11, 13], hour=12).render(placeholder=True)
        self.assertTrue('EXTRACT(DAY FROM "ts") IN (%s, %s) AND EXTRACT(HOUR FROM "ts") IN (%s)' in fstr)
        self.assertEqual([11, 13, 12], fargs)

        with self.assertRaises(KeyError):
            fstr, fargs = orm_class.query.in_ts(bogus=5).render(placeholder=True)

    def test_no_db_error(self):
        # we want to replace the db with a bogus db error
        i, s = self.get_table()
        config = i.connection_config
        config.database = 'this_is_a_bogus_db_name'
        i = PostgreSQL(config)
        fields = {
            'foo': 1,
            'bar': 'v1',
        }
        with self.assertRaises(prom.InterfaceError):
            rd = i.insert(s, fields)

    def test_uuid_pk(self):
        i, s = self.create_schema(
            _id=Field(UUID, True, pk=True),
            foo=Field(int, True),
        )

        pk = i.insert(s, {"foo": 1})
        self.assertEqual(36, len(pk))

        q = query.Query().is__id(pk)
        d = dict(i.get_one(s, q))
        self.assertEqual(1, d["foo"])
        self.assertEqual(pk, d["_id"])

    def test_invalid_text_repr(self):
        orm_class1 = self.get_orm_class(
            _id=Field(UUID, True, pk=True),
        )
        orm_class2 = self.get_orm_class(
            fk=Field(orm_class1, True),
            interface=orm_class1.interface,
        )

        o = orm_class1()
        o.save()

        o2 = orm_class2(fk=o.pk)
        o2.save()

        # make sure psycopg2.errors.InvalidTextRepresentation doesn't get misrepresented
        with self.assertRaises(prom.InterfaceError):
            o3 = orm_class2(fk='foo')
            o3.save()


@skipIf(gevent is None, "Skipping Gevent test because gevent module not installed")
class XInterfaceGeventTest(InterfaceTest):
    """this class has an X to start so that it will run last when all tests are run"""
    @classmethod
    def setUpClass(cls):
        import prom.interface.postgres.gevent
        prom.interface.postgres.gevent.patch_all()
        super(XInterfacePostgresGeventTest, cls).setUpClass()

    @classmethod
    def create_interface(cls):
        orig_url = os.environ["PROM_POSTGRES_DSN"]
        query_str = '?async=1&pool_maxconn=3&pool_class=prom.interface.postgres.gevent.ConnectionPool'
        os.environ["PROM_POSTGRES_DSN"] += query_str
        try:
            i = super(XInterfacePostgresGeventTest, cls).create_interface()

        finally:
            os.environ["PROM_POSTGRES_DSN"] = orig_url

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

    def test_concurrent_error_recovery(self):
        """when recovering from an error in a green thread environment one thread
        could have added the table while the other thread was asleep, this will
        test to make sure two threads failing at the same time will both recover
        correctly"""
        i = self.get_interface()
        s = self.get_schema()
        #i.set_table(s)
        for x in range(1, 3):
            gevent.spawn(i.insert, s, {'foo': x, 'bar': str(x)})

        gevent.wait()

        q = Query()
        r = list(i.get(s, q))
        self.assertEqual(2, len(r))

    def test_table_recovery(self):
        i = self.get_interface()
        s = self.get_schema()

        q = Query()
        l = i.get(s, q)
        self.assertEqual([], l)


# https://docs.python.org/2/library/unittest.html#load-tests-protocol
# def load_tests(loader, tests, pattern):
#     suite = TestSuite()
#     for tc in [InterfacePostgresTest, InterfacePGBouncerTest, XInterfacePostgresGeventTest]:
#         suite.addTests(loader.loadTestsFromTestCase(tc))
#     return suite

# not sure I'm a huge fan of this solution to remove common parent from testing queue
# http://stackoverflow.com/questions/1323455/python-unit-test-with-base-and-sub-class
# this works better than the load_tests method above because if I add a new TestCase I don't
# have to add it to load_tests specifically
del(BaseTestInterface)

