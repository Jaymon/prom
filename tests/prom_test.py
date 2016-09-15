from unittest import skipIf
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

# needed to test prom with greenthreads
try:
    import gevent
except ImportError as e:
    gevent = None

from . import BaseTestCase
from prom import query
from prom.model import Orm
from prom.config import Schema, Connection, DsnConnection, Field, Index
from prom.interface.postgres import PostgreSQL
from prom.interface.sqlite import SQLite
from prom.interface.base import Interface
import prom
import prom.interface


# configure root logger
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
log_handler = logging.StreamHandler(stream=sys.stderr)
log_formatter = logging.Formatter('[%(levelname)s] %(message)s')
log_handler.setFormatter(log_formatter)
logger.addHandler(log_handler)


#os.environ.setdefault('PROM_SQLITE_URL', 'prom.interface.sqlite.SQLite://:memory:')


stdnull = open(os.devnull, 'w') # used to suppress subprocess calls


# def setUpModule():
#     """
#     http://docs.python.org/2/library/unittest.html#setupmodule-and-teardownmodule
#     """
#     i = get_interface()
#     i.delete_tables(disable_protection=True)
#     prom.set_interface(i)


# DEPRECATED -- 3-15-2016 -- just here for the module methods until they are removed
class DeprecatedShiv(BaseTestCase):
	def runTest(self): pass
# DEPRECATED -- 3-15-2016 -- use the BaseTestCase methods instead
def get_orm_class(table_name=None):
    t = DeprecatedShiv()
    return t.get_orm_class(table_name)
def get_orm(table_name=None, **fields):
    t = DeprecatedShiv()
    return t.get_orm(table_name, **fields)
def get_interface():
    t = DeprecatedShiv()
    return t.get_interface()
def get_table_name(table_name=None):
    t = DeprecatedShiv()
    return t.get_table_name(table_name)
def get_table(table_name=None):
    t = DeprecatedShiv()
    return t.get_table(table_name)
def get_schema(table_name=None, **fields_or_indexes):
    t = DeprecatedShiv()
    return t.get_schema(table_name, **fields_or_indexes)
def get_query():
    t = DeprecatedShiv()
    return t.get_query()
def insert(interface, schema, count, **kwargs):
    t = DeprecatedShiv()
    return t.insert(interface, schema, count, **kwargs)


class PromTest(BaseTestCase):

    def setUp(self):
        prom.interface.interfaces = {}

    def test_configure(self):
        dsn = 'prom.interface.postgres.PostgreSQL://username:password@localhost/db'
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
        """test to make sure setting on a table that doesn't exist doesn't actually fail"""
        class FailureSetTorm(Orm):
            interface = get_interface()
            schema = get_schema()

        f = FailureSetTorm(foo=1, bar="value 1")
        f.set()
        self.assertTrue(f.pk)

    def test_failure_get(self):
        """test to make sure getting on a table that doesn't exist works without raising
        an error
        """
        class FailureGetTorm(Orm):
            interface = get_interface()
            schema = get_schema()

        f = FailureGetTorm(foo=1, bar="value 1")
        f.query.get_one()
        # we succeeded if no error was raised


class BaseTestInterface(BaseTestCase):
    @classmethod
    def create_interface(cls):
        raise NotImplementedError()

    def test_connect(self):
        i = self.get_interface()

    def test_close(self):
        i = self.get_interface()
        i.close()
        #self.assertTrue(i.connection is None)
        self.assertFalse(i.connected)

    def test_query(self):
        i = self.get_interface()
        rows = i.query('SELECT 1')
        self.assertGreater(len(rows), 0)

    def test_transaction_error(self):
        i = self.get_interface()
        with self.assertRaises(StopIteration):
            with i.transaction():
                raise StopIteration()

#        with self.assertRaises(RuntimeError):
#            with i.transaction():
#                raise RuntimeError()

    def test_set_table(self):
        i = self.get_interface()
        s = get_schema()
        r = i.has_table(s.table)
        self.assertFalse(r)

        r = i.set_table(s)

        r = i.has_table(s.table)
        self.assertTrue(r)

        # make sure it persists
        # TODO -- this should only be tested in postgres, the SQLite :memory: db
        # goes away when the connections is closed
#        i.close()
#        i = self.get_interface()
#        self.assertTrue(i.has_table(s.table))

        # make sure known indexes are there
        indexes = i.get_indexes(s)
        count = 0
        for known_index_name, known_index in s.indexes.items():
            for index_name, index_fields in indexes.items():
                if known_index.fields == index_fields:
                    count += 1

        self.assertEqual(len(s.indexes), count)

        # make sure more exotic datatypes are respected
        s_ref = get_schema()
        i.set_table(s_ref)
        s_ref_id = insert(i, s_ref, 1)[0]

        s = prom.Schema(
            get_table_name(),
            _id=Field(int, pk=True),
            one=Field(bool, True),
            two=Field(int, True, size=50),
            three=Field(decimal.Decimal),
            four=Field(float, True, size=10),
            six=Field(long, True,),
            seven=Field(int, False, ref=s_ref),
            eight=Field(datetime.datetime),
            nine=Field(datetime.date),
        )
        r = i.set_table(s)
        d = {
            'one': True,
            'two': 50,
            'three': decimal.Decimal('1.5'),
            'four': 1.987654321,
            'six': 40000,
            'seven': s_ref_id,
            'eight': datetime.datetime(2005, 7, 14, 12, 30),
            'nine': datetime.date(2005, 9, 14),
        }
        pk = i.insert(s, d)
        q = query.Query()
        q.is__id(pk)
        odb = i.get_one(s, q)
        #d['five'] = 1.98765
        for k, v in d.items():
            self.assertEqual(v, odb[k])

    def test_get_tables(self):
        i = self.get_interface()
        s = get_schema()
        r = i.set_table(s)
        r = i.get_tables()
        self.assertTrue(s.table in r)

        r = i.get_tables(s.table)
        self.assertTrue(s.table in r)

    def test_query_modified_table(self):
        i = self.get_interface()
        s = prom.Schema(
            'test_table',
            one=Field(int, True)
        )
        i.set_table(s)

        # Add new column
        s.set_field("two", Field(int, False))
        q = query.Query()
        q.is_two(None)

        # Test if query succeeds
        i.get_one(s, q)

    def test_delete_table(self):
        i = self.get_interface()
        s = get_schema()

        r = i.set_table(s)
        self.assertTrue(i.has_table(s.table))

        r = i.delete_table(s)
        self.assertFalse(i.has_table(s.table))

        # make sure it persists
        i.close()
        i = self.get_interface()
        self.assertFalse(i.has_table(s.table))

    def test_delete_tables(self):

        i = self.get_interface()
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
        i, s = self.get_table()
        d = {
            'foo': 1,
            'bar': 'this is the value',
        }

        pk = i.insert(s, d)
        self.assertGreater(pk, 0)

#     def test_set_insert(self):
#         """test just the insert portion of set"""
#         i, s = self.get_table()
#         q = query.Query()
# 
#         q.set_fields({
#             'foo': 1,
#             'bar': 'this is the value',
#         })
# 
#         rd = i.set(s, q)
#         self.assertTrue(rd[s._id.name], 0)
# 
    def test_get_sql(self):
        i = self.get_interface()
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
        i, s = self.get_table()
        _ids = insert(i, s, 2)

        for _id in _ids:
            q = query.Query()
            q.is__id(_id)
            d = i.get_one(s, q)
            self.assertEqual(d[s._id.name], _id)

        q = query.Query()
        q.is__id(12334342)
        d = i.get_one(s, q)
        self.assertEqual({}, d)

    def test_get_one_offset(self):
        """make sure get_one() works as expected when an offset is set"""
        i, s = self.get_table()
        q = query.Query()
        q.set_fields({
            'foo': 1,
            'bar': 'v1',
        })
        pk = i.insert(s, q.fields)

        q = query.Query()
        q.set_fields({
            'foo': 2,
            'bar': 'v2',
        })
        pk2 = i.insert(s, q.fields)

        q = query.Query()
        q.desc__id().set_offset(1)
        d = i.get_one(s, q)
        self.assertEqual(d['_id'], pk)

        # just make sure to get expected result if no offset
        q = query.Query()
        q.desc__id()
        d = i.get_one(s, q)
        self.assertEqual(d['_id'], pk2)

        q = query.Query()
        q.desc__id().set_offset(2)
        d = i.get_one(s, q)
        self.assertEqual({}, d)

        q = query.Query()
        q.desc__id().set_offset(1).set_limit(5)
        d = i.get_one(s, q)
        self.assertEqual(d['_id'], pk)

        q = query.Query()
        q.desc__id().set_page(2)
        d = i.get_one(s, q)
        self.assertEqual(d['_id'], pk)

        q = query.Query()
        q.desc__id().set_page(2).set_limit(5)
        d = i.get_one(s, q)
        self.assertEqual({}, d)

    def test_get(self):
        i, s = self.get_table()
        _ids = insert(i, s, 5)

        q = query.Query()
        q.in__id(_ids)
        l = i.get(s, q)
        self.assertEqual(len(_ids), len(l))
        for d in l:
            self.assertTrue(d[s._id.name] in _ids)

        q.set_limit(2)
        l = i.get(s, q)
        self.assertEqual(2, len(l))
        for d in l:
            self.assertTrue(d[s._id.name] in _ids)

    def test_get_no_where(self):
        """test get with no where clause"""
        i, s = self.get_table()
        _ids = insert(i, s, 5)

        q = None
        l = i.get(s, q)
        self.assertEqual(5, len(l))

    def test_get_pagination(self):
        """test get but moving through the results a page at a time to make sure limit and offset works"""
        i, s = self.get_table()
        _ids = insert(i, s, 12)

        q = query.Query()
        q.set_limit(5)
        count = 0
        for p in xrange(1, 5):
            q.set_page(p)
            l = i.get(s, q)
            for d in l:
                self.assertTrue(d[s._id.name] in _ids)

            count += len(l)

        self.assertEqual(12, count)

    def test_count(self):
        i, s = self.get_table()

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
        # try deleting with no table
        i = self.get_interface()
        s = get_schema()
        q = query.Query().is_foo(1)
        r = i.delete(s, q)

        i, s = self.get_table()

        # try deleting with no values in the table
        q = query.Query()
        q.is_foo(1)
        r = i.delete(s, q)
        self.assertEqual(0, r)

        _ids = insert(i, s, 5)

        # delete all the inserted values
        q = query.Query()
        q.in__id(_ids)
        l = i.get(s, q)
        self.assertEqual(5, len(l))
        r = i.delete(s, q)
        self.assertEqual(5, r)

        # verify rows are deleted
        l = i.get(s, q)
        self.assertEqual(0, len(l))

        # make sure it stuck
        i.close()
        i = self.get_interface()
        l = i.get(s, q)
        self.assertEqual(0, len(l))

    def test_update(self):
        i, s = self.get_table()
        q = query.Query()
        d = {
            'foo': 1,
            'bar': 'value 1',
        }

        pk = i.insert(s, d)
        self.assertGreater(pk, 0)

        d = {
            'foo': 2,
            'bar': 'value 2',
        }
        q.set_fields(d)
        q.is__id(pk)

        row_count = i.update(s, d, q)

        # let's pull it out and make sure it persisted
        q = query.Query()
        q.is__id(pk)
        gd = i.get_one(s, q)
        self.assertEqual(d['foo'], gd['foo'])
        self.assertEqual(d['bar'], gd['bar'])
        self.assertEqual(pk, gd["_id"])

    def test_ref(self):
        i = self.get_interface()
        table_name_1 = "".join(random.sample(string.ascii_lowercase, random.randint(5, 15)))
        table_name_2 = "".join(random.sample(string.ascii_lowercase, random.randint(5, 15)))

        s_1 = Schema(
            table_name_1,
            _id=Field(int, pk=True),
            foo=Field(int, True)
        )
        s_2 = Schema(
            table_name_2,
            _id=Field(int, pk=True),
            s_pk=Field(s_1, True),
        )

        i.set_table(s_1)
        i.set_table(s_2)

        pk1 = i.insert(s_1, {'foo': 1})

        pk2 = i.insert(s_2, {'s_pk': pk1})

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
        i = self.get_interface()
        table_name_1 = "".join(random.sample(string.ascii_lowercase, random.randint(5, 15)))
        table_name_2 = "".join(random.sample(string.ascii_lowercase, random.randint(5, 15)))

        s_1 = Schema(
            table_name_1,
            _id=Field(int, pk=True),
            foo=Field(int, True)
        )
        s_2 = Schema(
            table_name_2,
            _id=Field(int, pk=True),
            s_pk=Field(s_1, False),
        )

        i.set_table(s_1)
        i.set_table(s_2)

        pk1 = i.insert(s_1, {'foo': 1})

        pk2 = i.insert(s_2, {'s_pk': pk1})
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
        i = self.get_interface()
        table_name_1 = get_table_name()
        table_name_2 = get_table_name()

        s_1 = Schema(
            table_name_1,
            _id=Field(int, pk=True),
            foo=Field(int, True)
        )
        s_2 = Schema(
            table_name_2,
            _id=Field(int, pk=True),
            bar=Field(int, True),
            s_pk=Field(s_1),
        )

        q2 = query.Query()
        q2.is_bar(1)

        r = i.get_one(s_2, q2)
        self.assertTrue(i.has_table(table_name_1))
        self.assertTrue(i.has_table(table_name_2))

    def test__get_fields(self):
        i, s = self.get_table()
        fields = set([u'_id', u'bar', u'foo'])
        ret_fields = i._get_fields(s)
        self.assertEqual(fields, ret_fields)

    def test__set_all_fields(self):
        i, s = self.get_table()
        s.set_field("che", Field(str, True))
        q = query.Query()
        q.set_fields({
            'foo': 1,
            'bar': 'v1',
            'che': "this field will cause the query to fail",
        })

        with self.assertRaises(ValueError):
            ret = i._set_all_fields(s)

        s = get_schema(table_name=s.table)
        s.che = str, False
        ret = i._set_all_fields(s)
        self.assertTrue(ret)

    def test_handle_error_column(self):
        i, s = self.get_table()
        s.set_field("che", Field(str, True)) # it's required
        fields = {
            'foo': 1,
            'bar': 'v1',
            'che': "this field will cause the query to fail",
        }

        with self.assertRaises(prom.InterfaceError):
            rd = i.insert(s, fields)

        s = get_schema(table_name=s.table)
        s.set_field("che", Field(str, False)) # not required so error recovery can fire
        pk = i.insert(s, fields)
        self.assertLess(0, pk)

    def test_null_values(self):
        i = self.get_interface()
        s = Schema(
            get_table_name(),
            _id=Field(int, pk=True),
            foo=Field(int, False),
            bar=Field(int, False),
        )

        # add one with non NULL foo
        pk1 = i.insert(s, {"bar": 1, "foo": 2})

        # and one with NULL foo
        pk2 = i.insert(s, {"bar": 1})

        r = i.get_one(s, query.Query().is_bar(1).is_foo(None))
        self.assertEqual(pk2, r['_id'])

        r = i.get_one(s, query.Query().is_bar(1).not_foo(None))
        self.assertEqual(pk1, r['_id'])

    def test_transaction_nested_fail_1(self):
        """make sure 2 new tables in a wrapped transaction work as expected"""
        i = self.get_interface()
        table_name_1 = get_table_name()
        table_name_2 = get_table_name()

        s1 = Schema(
            table_name_1,
            _id=Field(int, pk=True),
            foo=Field(int, True)
        )
        s2 = Schema(
            table_name_2,
            _id=Field(int, pk=True),
            bar=Field(int, True),
            s_pk=Field(s1),
        )

        with i.transaction() as connection:
            pk1 = i.insert(s1, {"foo": 1}, connection=connection)
            pk2 = i.insert(s2, {"bar": 2, "s_pk": pk1}, connection=connection)

        q1 = query.Query()
        q1.is__id(pk1)
        r1 = i.get_one(s1, q1)
        self.assertEqual(pk1, r1['_id'])

        q2 = query.Query()
        q2.is__id(pk2)
        r2 = i.get_one(s2, q2)
        self.assertEqual(pk2, r2['_id'])
        self.assertEqual(pk1, r2['s_pk'])

    def test_transaction_nested_fail_2(self):
        """make sure 2 tables where the first one already exists works in a nested transaction"""
        i = self.get_interface()

        s1 = get_schema(
            foo=Field(int, True)
        )
        i.set_table(s1)

        s2 = get_schema(
            bar=Field(int, True),
            s_pk=Field(s1, True),
        )

        with i.transaction() as connection:
            pk1 = i.insert(s1, {"foo": 1}, connection=connection)
            pk2 = i.insert(s2, {"bar": 2, "s_pk": pk1}, connection=connection)

        r1 = i.get_one(s1, query.Query().is__id(pk1))
        self.assertEqual(pk1, r1['_id'])

        r2 = i.get_one(s2, query.Query().is__id(pk1))
        self.assertEqual(pk2, r2['_id'])
        self.assertEqual(r2['s_pk'], pk1)

    def test_transaction_nested_fail_3(self):
        """make sure 2 tables where the first one already exists works, and second one has 2 refs"""
        i = self.get_interface()
        table_name_1 = "{}_1".format(get_table_name())
        table_name_2 = "{}_2".format(get_table_name())

        s1 = get_schema(
            foo=Field(int, True)
        )
        i.set_table(s1)

        s2 = get_schema(
            bar=Field(int, True),
            s_pk=Field(s1, True),
            s_pk2=Field(s1, True),
        )

        pk1 = i.insert(s1, {"foo": 1})
        pk2 = i.insert(s1, {"foo": 1})
        pk3 = i.insert(s2, {"bar": 2, "s_pk": pk1, "s_pk2": pk2})

        r1 = i.get_one(s1, query.Query().is__id(pk1))
        self.assertEqual(r1['_id'], pk1)

        r2 = i.get_one(s2, query.Query().is__id(pk3))
        self.assertEqual(r2['_id'], pk3)
        self.assertEqual(r2['s_pk'], pk1)
        self.assertEqual(r2['s_pk2'], pk2)

    def test_transaction_nested_fail_4(self):
        """ran into a bug where this reared its head and data was lost"""
        i = self.get_interface()

        # these 2 tables exist before the transaction starts
        s1 = get_schema(
            foo=Field(int, True)
        )
        i.set_table(s1)

        s2 = get_schema(
            bar=Field(int, True),
            s_pk=Field(s1, True),
            s_pk2=Field(s1, True),
        )
        i.set_table(s2)

        # this is the table that will be created in the transaction
        s3 = get_schema(
            che=Field(int, True),
            s_pk=Field(s1, True),
        )

        pk1 = i.insert(s1, {"foo": 1})
        pk12 = i.insert(s1, {"foo": 12})

        self.assertEqual(0, i.count(s2, query.Query()))

        with i.transaction() as connection:

            # create something and put in table 2
            pk2 = i.insert(s2, {"bar": 2, "s_pk": pk1, "s_pk2": pk12}, connection=connection)

            # now this should cause the stuff to fail
            # it fails on the select because a new transaction isn't started, so 
            # it just discards all the current stuff and adds the table, had this
            # been a mod query (eg, insert) it would not have failed, this is fixed
            # by wrapping selects in a transaction if an active transaction is found
            q3 = query.Query()
            q3.is_s_pk(pk1)
            pk3 = i.get(s3, q3, connection=connection)

        self.assertEqual(1, i.count(s2, query.Query()))

    def test_transaction_context(self):
        i = self.get_interface()
        table_name_1 = "{}_1".format(get_table_name())
        table_name_2 = "{}_2".format(get_table_name())

        # these 2 tables exist before the transaction starts
        s1 = Schema(
            table_name_1,
            _id=Field(int, pk=True),
            foo=Field(int, True)
        )
        i.set_table(s1)

        s2 = Schema(
            table_name_2,
            _id=Field(int, pk=True),
            bar=Field(int, True),
            s_pk=Field(int, True, ref=s1),
        )
        i.set_table(s2)

        pk1 = 0
        pk2 = 0

        try:
            with i.transaction() as connection:
                pk1 = i.insert(s1, {"foo": 1}, connection=connection)

                with i.transaction(connection):
                    pk2 = i.set(s2, {"bar": 2, "s_pk": pk1}, connection=connection)
                    raise RuntimeError("testing")

        except Exception, e:
            pass

        self.assertEqual(0, i.count(s1, query.Query().is__id(pk1)))
        self.assertEqual(0, i.count(s2, query.Query().is__id(pk2)))

    def test_unique(self):
        i = get_interface()
        s = get_schema()
        s.set_field("should_be_unique", Field(int, True, unique=True))
        i.set_table(s)

        d = i.insert(s, {'foo': 1, 'bar': 'v1', 'should_be_unique': 1})

        with self.assertRaises(prom.InterfaceError):
            d = i.insert(s, {'foo': 2, 'bar': 'v2', 'should_be_unique': 1})

    def test_index_ignore_case(self):
        i = self.get_interface()
        s = Schema(
            get_table_name(),
            _id=Field(int, pk=True),
            foo=Field(str, True, ignore_case=True),
            bar=Field(str, True),
            index_foo=Index('foo', 'bar'),
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
        i, s = self.get_table()
        _ids = insert(i, s, 5)

        q = query.Query()
        q.in__id(_ids)
        l = list(i.get(s, q))

        self.assertEqual(len(l), 5)

    def test_sort_order(self):
        q = self.get_query()
        self.insert(q.orm_class.interface, q.orm_class.schema, 10)

        q2 = q.copy()
        foos = list(q2.select_foo().asc__id().values())
        foos.sort()

        for x in range(2, 9):
            q3 = q.copy()
            rows = list(q3.select_foo().asc_foo().set_limit(1).set_page(x).values())
            #pout.v(x, foos[x], rows[0])
            self.assertEqual(foos[x - 1], rows[0])

            q3 = q.copy()
            row = q3.select_foo().asc_foo().set_limit(1).set_page(x).value()
            self.assertEqual(foos[x - 1], row)

            q3 = q.copy()
            row = q3.select_foo().asc_foo().set_limit(1).set_page(x).value()
            self.assertEqual(foos[x - 1], row)

            q3 = q.copy()
            rows = list(q3.select_foo().in_foo(foos).asc_foo(foos).set_limit(1).set_page(x).values())
            self.assertEqual(foos[x - 1], rows[0])

            q3 = q.copy()
            row = q3.select_foo().in_foo(foos).asc_foo(foos).set_limit(1).set_page(x).value()
            self.assertEqual(foos[x - 1], row)

        for x in range(1, 9):
            q3 = q.copy()
            rows = list(q3.select_foo().asc_foo().set_limit(x).set_offset(x).values())
            #pout.v(x, foos[x], rows[0])
            self.assertEqual(foos[x], rows[0])

            q3 = q.copy()
            row = q3.select_foo().asc_foo().set_limit(x).set_offset(x).value()
            self.assertEqual(foos[x], row)

            q3 = q.copy()
            row = q3.select_foo().asc_foo().set_limit(x).set_offset(x).value()
            self.assertEqual(foos[x], row)

            q3 = q.copy()
            rows = list(q3.select_foo().in_foo(foos).asc_foo(foos).set_limit(1).set_offset(x).values())
            self.assertEqual(foos[x], rows[0])

            q3 = q.copy()
            row = q3.select_foo().in_foo(foos).asc_foo(foos).set_limit(1).set_offset(x).value()
            self.assertEqual(foos[x], row)

    def test_sort_list(self):
        q = self.get_query()
        self.insert(q.orm_class.interface, q.orm_class.schema, 10)

        q2 = q.copy()
        foos = list(q2.select_foo().values())
        random.shuffle(foos)

        q3 = q.copy()
        rows = list(q3.select_foo().in_foo(foos).asc_foo(foos).values())
        for i, r in enumerate(rows):
            self.assertEqual(foos[i], r)

        return
        q4 = q.copy()
        rfoos = list(reversed(foos))
        rows = list(q4.select_foo().in_foo(foos).desc_foo(foos).values())
        for i, r in enumerate(rows):
            self.assertEqual(rfoos[i], r)

        qb = q.copy()
        rows = list(qb.in_foo(foos).asc_foo(foos).get(2, 2))
        for i, r in enumerate(rows, 2):
            self.assertEqual(foos[i], r.foo)

        # now test a string value
        qb = q.copy()
        bars = list(qb.select_bar().values())
        random.shuffle(bars)

        qb = q.copy()
        rows = list(qb.in_bar(bars).asc_bar(bars).get())
        for i, r in enumerate(rows):
            self.assertEqual(bars[i], r.bar)

        # make sure limits and offsets work
        qb = q.copy()
        rows = list(qb.in_bar(bars).asc_bar(bars).get(5))
        for i, r in enumerate(rows):
            self.assertEqual(bars[i], r.bar)

        qb = q.copy()
        rows = list(qb.in_bar(bars).asc_bar(bars).get(2, 2))
        for i, r in enumerate(rows, 2):
            self.assertEqual(bars[i], r.bar)

        # make sure you can select on one row and sort on another
        qv = q.copy()
        vs = list(qv.select_foo().select_bar().values())
        random.shuffle(vs)

        qv = q.copy()
        rows = list(qv.select_foo().asc_bar((v[1] for v in vs)).values())
        for i, r in enumerate(rows):
            self.assertEqual(vs[i][0], r)

    def test_transaction_context_manager(self):
        """make sure the with transaction() context manager works as expected"""
        i, s = self.get_table()
        _id = None
        with i.transaction() as connection:
            _id = insert(i, s, 1, connection=connection)[0]

        self.assertTrue(_id)

        q = query.Query()
        q.is__id(_id)
        d = i.get_one(s, q)
        self.assertGreater(len(d), 0)

        with self.assertRaises(RuntimeError):
            with i.transaction() as connection:
                _id = insert(i, s, 1, connection=connection)[0]
                raise RuntimeError("this should fail")

        q = query.Query()
        q.is__id(_id)
        d = i.get_one(s, q)
        self.assertEqual(len(d), 0)

    def test__normalize_date_SQL(self):
        """this tests the common date kwargs you can use (in both SQLight and Postgres)
        if we ever add other backends this might need to be moved out of the general
        generator test"""
        i = self.get_interface()
        s = Schema(
            get_table_name(),
            foo=Field(datetime.datetime, True),
            _id=Field(int, True, pk=True),
            index_foo=Index('foo'),
        )
        i.set_table(s)

        pk20 = i.insert(s, {'foo': datetime.datetime(2014, 4, 20)})
        pk21 = i.insert(s, {'foo': datetime.datetime(2014, 4, 21)})

        q = query.Query()
        q.is_foo(day=20)
        d = i.get_one(s, q)
        self.assertEqual(d['_id'], pk20)

        q = query.Query()
        q.is_foo(day=21, month=4)
        d = i.get_one(s, q)
        self.assertEqual(d['_id'], pk21)

        q = query.Query()
        q.is_foo(day=21, month=3)
        d = i.get_one(s, q)
        self.assertFalse(d)


class InterfaceSQLiteTest(BaseTestInterface):
    @classmethod
    def create_interface(cls):
        return cls.create_sqlite_interface()

    def test_db_disconnect(self):
        """make sure interface can recover if the db disconnects mid script execution,
        SQLite is a bit different than postgres which is why this method is completely
        original"""
        i, s = self.get_table()
        _id = insert(i, s, 1)[0]
        q = query.Query()
        q.is__id(_id)
        d = i.get_one(s, q)
        self.assertGreater(len(d), 0)

        i._connection.close()

        _id = insert(i, s, 1)[0]
        q = query.Query()
        q.is__id(_id)
        d = i.get_one(s, q)
        self.assertGreater(len(d), 0)

    def test_no_connection(self):
        """noop, this doesn't really apply to SQLite"""
        pass


class InterfacePostgresTest(BaseTestInterface):
    @classmethod
    def create_interface(cls):
        return cls.create_postgres_interface()

    def test_set_table_postgres(self):
        """test some postgres specific things"""
        i = self.get_interface()
        s = prom.Schema(
            get_table_name(),
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
        for k, v in d.iteritems():
            self.assertEqual(v, odb[k])

    def test_db_disconnect(self):
        """make sure interface can recover if the db disconnects mid script execution"""
        i, s = self.get_table()
        _id = insert(i, s, 1)[0]
        q = query.Query()
        q.is__id(_id)
        d = i.get_one(s, q)
        self.assertGreater(len(d), 0)

        exit_code = subprocess.check_call("sudo /etc/init.d/postgresql restart", shell=True, stdout=stdnull)
        time.sleep(1)

        q = query.Query()
        q.is__id(_id)
        d = i.get_one(s, q)
        self.assertGreater(len(d), 0)

    def test_no_connection(self):
        """this will make sure prom handles it gracefully if there is no connection available ever"""
        exit_code = subprocess.check_call("sudo /etc/init.d/postgresql stop", shell=True, stdout=stdnull)
        time.sleep(1)

        try:
            i = self.create_interface()
            s = get_schema()
            q = query.Query()
            with self.assertRaises(prom.InterfaceError):
                i.get(s, q)

        finally:
            exit_code = subprocess.check_call("sudo /etc/init.d/postgresql start", shell=True, stdout=stdnull)
            time.sleep(1)

    def test__normalize_val_SQL(self):
        i = self.get_interface()
        s = Schema(
            "fake_table_name",
            ts=Field(datetime.datetime, True)
        )

        #kwargs = dict(day=int(datetime.datetime.utcnow().strftime('%d')))
        kwargs = dict(day=10)
        fstr, fargs = i._normalize_val_SQL(s, {'symbol': '='}, 'ts', None, kwargs)
        self.assertEqual("EXTRACT(DAY FROM ts) = %s", fstr)
        self.assertEqual(10, fargs[0])

        kwargs = dict(day=11, hour=12)
        fstr, fargs = i._normalize_val_SQL(s, {'symbol': '='}, 'ts', None, kwargs)
        self.assertEqual("EXTRACT(DAY FROM ts) = %s AND EXTRACT(HOUR FROM ts) = %s", fstr)
        self.assertEqual(11, fargs[0])
        self.assertEqual(12, fargs[1])

        fstr, fargs = i._normalize_val_SQL(s, {'symbol': '=', 'none_symbol': 'IS'}, 'ts', None)
        self.assertEqual("ts IS %s", fstr)

        fstr, fargs = i._normalize_val_SQL(s, {'symbol': '!=', 'none_symbol': 'IS NOT'}, 'ts', None)
        self.assertEqual("ts IS NOT %s", fstr)

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
        self.assertEqual("EXTRACT(DAY FROM ts) IN (%s)", fstr)
        self.assertEqual(kwargs['day'], fargs)

        kwargs = dict(day=[11, 13], hour=[12])
        fstr, fargs = i._normalize_val_SQL(s, {'symbol': 'IN', 'list': True}, 'ts', None, kwargs)
        self.assertEqual("EXTRACT(DAY FROM ts) IN (%s, %s) AND EXTRACT(HOUR FROM ts) IN (%s)", fstr)
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


def has_spiped():
    ret = False
    try:
        c = subprocess.check_call("which spiped", shell=True, stdout=stdnull)
        ret = True
    except subprocess.CalledProcessError:
        ret = False
    return ret


class InterfacePGBouncerTest(InterfacePostgresTest):
    @classmethod
    def create_interface(cls):
        config = DsnConnection(os.environ["PROM_PGBOUNCER_URL"])
        i = PostgreSQL(config)
        return i

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
        _id = insert(i, s, 1)[0]
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
        orig_url = os.environ["PROM_POSTGRES_URL"]
        os.environ["PROM_POSTGRES_URL"] += '?async=1&pool_maxconn=3&pool_class=prom.gevent.ConnectionPool'
        try:
            i = super(XInterfacePostgresGeventTest, cls).create_interface()

        finally:
            os.environ["PROM_POSTGRES_URL"] = orig_url

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
        s = get_schema()
        #i.set_table(s)
        for x in range(1, 3):
            gevent.spawn(i.insert, s, {'foo': x, 'bar': str(x)})

        gevent.wait()

        q = query.Query()
        r = list(i.get(s, q))
        self.assertEqual(2, len(r))

    def test_table_recovery(self):
        i = self.get_interface()
        s = get_schema()

        q = query.Query()
        l = i.get(s, q)
        self.assertEqual([], l)


# not sure I'm a huge fan of this solution to remove common parent from testing queue
# http://stackoverflow.com/questions/1323455/python-unit-test-with-base-and-sub-class
del(BaseTestInterface)

