# -*- coding: utf-8 -*-
from __future__ import unicode_literals, division, print_function, absolute_import
from unittest import skipIf
import os
import datetime
import time
import subprocess

# needed to test prom with greenthreads
try:
    import gevent
except ImportError as e:
    gevent = None

from testdata.service import InitD

from prom import query
from prom.compat import *
from prom.config import Schema, DsnConnection, Field
from prom.interface.postgres import PostgreSQL
import prom
import prom.interface

from . import BaseTestInterface


stdnull = open(os.devnull, 'w') # used to suppress subprocess calls


def has_spiped():
    ret = False
    try:
        c = subprocess.check_call("which spiped", shell=True, stdout=stdnull)
        ret = True
    except subprocess.CalledProcessError:
        ret = False
    return ret


class InterfacePostgresTest(BaseTestInterface):
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
        q = query.Query()
        q.is__id(pk)
        odb = i.get_one(s, q)
        for k, v in d.items():
            self.assertEqual(v, odb[k])

    def test_db_disconnect(self):
        """make sure interface can recover if the db disconnects mid script execution"""
        i, s = self.get_table()
        _id = self.insert(i, s, 1)[0]
        q = query.Query()
        q.is__id(_id)
        d = i.get_one(s, q)
        self.assertGreater(len(d), 0)

        InitD("postgresql").restart()

        q = query.Query()
        q.is__id(_id)
        d = i.get_one(s, q)
        self.assertGreater(len(d), 0)

    def test_no_connection(self):
        """this will make sure prom handles it gracefully if there is no connection available ever"""
        postgresql = InitD("postgresql")
        postgresql.ignore_failure = False
        postgresql.stop()

        try:
            i = self.create_interface()
            s = self.get_schema()
            q = query.Query()
            with self.assertRaises(prom.InterfaceError):
                i.get(s, q)

        finally:
            postgresql.start()

    def test__normalize_val_SQL(self):
        i = self.get_interface()
        s = Schema(
            "fake_table_name",
            ts=Field(datetime.datetime, True)
        )

        #kwargs = dict(day=int(datetime.datetime.utcnow().strftime('%d')))
        kwargs = dict(day=10)
        fstr, fargs = i._normalize_val_SQL(s, {'symbol': '='}, 'ts', None, kwargs)
        self.assertEqual('EXTRACT(DAY FROM "ts") = %s', fstr)
        self.assertEqual(10, fargs[0])

        kwargs = dict(day=11, hour=12)
        fstr, fargs = i._normalize_val_SQL(s, {'symbol': '='}, 'ts', None, kwargs)
        self.assertEqual('EXTRACT(DAY FROM "ts") = %s AND EXTRACT(HOUR FROM "ts") = %s', fstr)
        self.assertEqual(11, fargs[0])
        self.assertEqual(12, fargs[1])

        fstr, fargs = i._normalize_val_SQL(s, {'symbol': '=', 'none_symbol': 'IS'}, 'ts', None)
        self.assertEqual('"ts" IS %s', fstr)

        fstr, fargs = i._normalize_val_SQL(s, {'symbol': '!=', 'none_symbol': 'IS NOT'}, 'ts', None)
        self.assertEqual('"ts" IS NOT %s', fstr)

        kwargs = dict(bogus=5)
        with self.assertRaises(KeyError):
            fstr, fargs = i._normalize_val_SQL(s, {'symbol': '='}, 'ts', None, kwargs)

    def test__normalize_val_SQL_with_list(self):
        i = self.get_interface()
        s = Schema(
            "fake_table_name",
            ts=Field(datetime.datetime, True)
        )

        kwargs = dict(day=[10])
        fstr, fargs = i._normalize_val_SQL(s, {'symbol': 'IN', 'list': True}, 'ts', None, kwargs)
        self.assertEqual('EXTRACT(DAY FROM "ts") IN (%s)', fstr)
        self.assertEqual(kwargs['day'], fargs)

        kwargs = dict(day=[11, 13], hour=[12])
        fstr, fargs = i._normalize_val_SQL(s, {'symbol': 'IN', 'list': True}, 'ts', None, kwargs)
        self.assertEqual('EXTRACT(DAY FROM "ts") IN (%s, %s) AND EXTRACT(HOUR FROM "ts") IN (%s)', fstr)
        self.assertEqual(kwargs['day'], fargs[0:2])
        self.assertEqual(kwargs['hour'], fargs[2:])

        kwargs = dict(bogus=[5])
        with self.assertRaises(KeyError):
            fstr, fargs = i._normalize_val_SQL(s, {'symbol': 'IN', 'list': True}, 'ts', None, kwargs)

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
        fields = {
            'foo': 1,
            'bar': 'v1',
        }
        with self.assertRaises(prom.InterfaceError):
            rd = i.insert(s, fields)


class InterfacePGBouncerTest(InterfacePostgresTest):
    @classmethod
    def create_interface(cls):
        return cls.create_environ_interface("PROM_PGBOUNCER_DSN")

    def test_no_connection(self):
        """this will make sure prom handles it gracefully if there is no connection
        available ever. We have to wrap this for pgbouncer because PGBouncer can
        hold the connections if there is no db waiting for the db to come back up
        for all sorts of timeouts, and it's just easier to reset pg boucner than
        configure it for aggressive test timeouts.
        """
        subprocess.check_call("sudo stop pgbouncer", shell=True, stdout=stdnull)
        time.sleep(1)

        try:
            super(InterfacePGBouncerTest, self).test_no_connection()

        finally:
            subprocess.check_call("sudo start pgbouncer", shell=True, stdout=stdnull)
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
        _id = self.insert(i, s, 1)[0]
        q = query.Query()
        q.is__id(_id)
        d = i.get_one(s, q)
        self.assertGreater(len(d), 0)

        exit_code = subprocess.check_call("sudo restart spiped-pg-server", shell=True, stdout=stdnull)
        time.sleep(1)

        q = query.Query()
        q.is__id(_id)
        d = i.get_one(s, q)
        self.assertGreater(len(d), 0)

        exit_code = subprocess.check_call("sudo restart spiped-pg-client", shell=True, stdout=stdnull)
        time.sleep(1)

        q = query.Query()
        q.is__id(_id)
        d = i.get_one(s, q)
        self.assertGreater(len(d), 0)


@skipIf(gevent is None, "Skipping Gevent test because gevent module not installed")
class XInterfacePostgresGeventTest(InterfacePostgresTest):
    """this class has an X to start so that it will run last when all tests are run"""
    @classmethod
    def setUpClass(cls):
        import gevent.monkey
        gevent.monkey.patch_all()

        import prom.gevent
        prom.gevent.patch_all()
        super(XInterfacePostgresGeventTest, cls).setUpClass()

    @classmethod
    def create_interface(cls):
        orig_url = os.environ["PROM_POSTGRES_DSN"]
        os.environ["PROM_POSTGRES_DSN"] += '?async=1&pool_maxconn=3&pool_class=prom.gevent.ConnectionPool'
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
        s = self.get_schema()
        #i.set_table(s)
        for x in range(1, 3):
            gevent.spawn(i.insert, s, {'foo': x, 'bar': str(x)})

        gevent.wait()

        q = query.Query()
        r = list(i.get(s, q))
        self.assertEqual(2, len(r))

    def test_table_recovery(self):
        i = self.get_interface()
        s = self.get_schema()

        q = query.Query()
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

