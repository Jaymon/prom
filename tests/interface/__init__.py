# -*- coding: utf-8 -*-
from __future__ import unicode_literals, division, print_function, absolute_import
from unittest import TestSuite
import random
import string
import decimal
import datetime

from datatypes import Datetime

from prom import query, InterfaceError
from prom.config import Schema, Field, Index
from prom.query import Query
from prom.compat import *
import prom

from .. import BaseTestCase, testdata


class BaseTestInterface(BaseTestCase):
    @classmethod
    def create_interface(cls):
        raise NotImplementedError()

    def test_connect_close(self):
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
        if is_py2:
            with self.assertRaises(StopIteration):
                with i.transaction():
                    raise StopIteration()
        else:
            with self.assertRaises(RuntimeError):
                with i.transaction():
                    raise RuntimeError()

    def test_set_table(self):
        i = self.get_interface()
        s = self.get_schema()
        r = i.has_table(str(s))
        self.assertFalse(r)

        r = i.set_table(s)

        r = i.has_table(str(s))
        self.assertTrue(r)

        # make sure known indexes are there
        indexes = i.get_indexes(s)
        count = 0
        for known_index_name, known_index in s.indexes.items():
            for index_name, index_fields in indexes.items():
                if known_index.field_names == index_fields:
                    count += 1

        self.assertEqual(len(s.indexes), count)

        # make sure more exotic datatypes are respected
        s_ref = self.get_schema()
        i.set_table(s_ref)
        s_ref_id = self.insert(i, s_ref, 1)[0]

        s = prom.Schema(
            self.get_table_name(),
            _id=Field(int, pk=True),
            one=Field(bool, True),
            two=Field(int, True, size=50),
            three=Field(decimal.Decimal),
            four=Field(float, True, size=10),
            seven=Field(s_ref, False),
            eight=Field(datetime.datetime),
            nine=Field(datetime.date),
        )
        r = i.set_table(s)
        d = {
            'one': True,
            'two': 50,
            'three': decimal.Decimal('1.5'),
            'four': 1.987654321,
            'seven': s_ref_id,
            'eight': datetime.datetime(2005, 7, 14, 12, 30, tzinfo=datetime.timezone.utc),
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
        s = self.get_schema()
        r = i.set_table(s)
        r = i.get_tables()
        self.assertTrue(str(s) in r)

        r = i.get_tables(str(s))
        self.assertTrue(str(s) in r)

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

    def test_unsafe_delete_table(self):
        i = self.get_interface()
        s = self.get_schema()

        r = i.set_table(s)
        self.assertTrue(i.has_table(str(s)))

        r = i.unsafe_delete_table(s)
        self.assertFalse(i.has_table(str(s)))

        # make sure it persists
        i.close()
        i = self.get_interface()
        self.assertFalse(i.has_table(str(s)))

    def test_unsafe_delete_tables(self):
        i = self.get_interface()
        s1 = self.get_schema()
        i.set_table(s1)
        s2 = self.get_schema()
        i.set_table(s2)

        self.assertTrue(i.has_table(s1))
        self.assertTrue(i.has_table(s2))

        i.unsafe_delete_tables()

        self.assertFalse(i.has_table(s1))
        self.assertFalse(i.has_table(s2))

    def test_readonly(self):
        s = self.get_schema()
        i = self.get_interface()
        i.readonly(True)

        with self.assertRaises(InterfaceError):
            i.set_table(s)
        self.assertFalse(i.has_table(s))

        i.readonly(False)
        i.set_table(s)
        self.assertTrue(i.has_table(s))

    def test_custom_pk_int(self):
        i, s = self.get_table(_id=Field(int, pk=True), bar=Field(str))

        pk = i.insert(s, {"bar": "barvalue"})
        self.assertLess(0, pk)

        d = i.get_one(s, query.Query().select__id().is__id(pk))
        self.assertEqual(pk, d["_id"])

        pk_custom = testdata.get_int()
        pk = i.insert(s, {"_id": pk_custom})
        self.assertEqual(pk_custom, pk)

        d = i.get_one(s, query.Query().select__id().is__id(pk_custom))
        self.assertEqual(pk_custom, d["_id"])

    def test_field_bool(self):
        """There was a bug where SQLite boolean field always returned True, this
        tests to make sure that is fixed and it won't happen again"""
        i, s = self.get_table(bar=Field(bool), che=Field(bool))
        pk = i.insert(s, {"bar": False, "che": True})

        q = query.Query().is__id(pk)
        d = dict(i.get_one(s, q))
        self.assertFalse(d["bar"])
        self.assertTrue(d["che"])

    def test_field_dict(self):
        i, s = self.create_schema(
            foo=Field(dict)
        )

        foo = {"bar": 1, "che": "che 1"}
        pk = i.insert(s, {"foo": foo})

        d = i.get_one(s, Query().eq__id(pk))
        self.assertEqual(1, d["foo"]["bar"])
        self.assertEqual("che 1", d["foo"].get("che"))

    def test_field_datetime_datatypes(self):
        """Makes sure datatypes.Datetime works for the different interfaces"""
        orm_class = self.get_orm_class(bar=Field(datetime.datetime))
        o = orm_class.create(bar=Datetime())

        dt = Datetime()
        o2 = orm_class.query.lt_bar(dt).one()
        self.assertEqual(o.bar, o2.bar)

    def test_field_datetime_iso8601(self):
        """make sure ISO 8601 formatted datestamps can be added to the db

        NOTE -- postgres actually validates the datestamp on insert, so it will 
            reject bad datestamps on insert while sqlite will gladly insert them
            and then it fails while pulling them out, this is not ideal
        """
        i, s = self.get_table(bar=Field(datetime.datetime))

        dts = [
            {
                "input": "2020-03-25T19:34:05.05Z",
                "output": {
                    "microsecond": 50000
                }
            },
            {
                "input": "2019-10-08 20:18:59.00005",
                "output": {
                    "microsecond": 50
                }
            },

#             { # this fails in postgres
#                 "input": "1570565939.566850",
#                 "output": {
#                     "year": 2019,
#                     "month": 10,
#                     "day": 8,
#                     "hour": 20,
#                     "minute": 18,
#                     "second": 59,
#                     "microsecond": 566850,
#                 }
#             },

            {
                "input": "20191008T201859",
                "output": {
                    "year": 2019,
                    "month": 10,
                    "day": 8,
                    "hour": 20,
                    "minute": 18,
                    "second": 59,
                }
            },
            {
                "input": "2019-10-08 20:18:59.566855Z",
                "output": {
                    "year": 2019,
                    "month": 10,
                    "day": 8,
                    "hour": 20,
                    "minute": 18,
                    "second": 59,
                    "microsecond": 566855,
                }
            },
            {
                "input": "2019-10-08T20:18:59.566855",
                "output": {
                    "year": 2019,
                    "month": 10,
                    "day": 8,
                    "hour": 20,
                    "minute": 18,
                    "second": 59,
                    "microsecond": 566855,
                }
            },
            {
                "input": "20191008T201859.566855",
                "output": {
                    "year": 2019,
                    "month": 10,
                    "day": 8,
                    "hour": 20,
                    "minute": 18,
                    "second": 59,
                    "microsecond": 566855,
                }
            },
            {
                "input": "2019-10-08 20:18:59.566Z",
                "output": {
                    "year": 2019,
                    "month": 10,
                    "day": 8,
                    "hour": 20,
                    "minute": 18,
                    "second": 59,
                    "microsecond": 566000,
                }
            },
            {
                "input": "2019-10-08 20:18:59",
                #"input": "2019-10-08 20:18:59.",
                "output": {
                    "year": 2019,
                    "month": 10,
                    "day": 8,
                    "hour": 20,
                    "minute": 18,
                    "second": 59,
                    "microsecond": 0,
                }
            },
        ]

        for dt in dts:
            if isinstance(dt["output"], dict):
                pk = i.insert(s, {"bar": dt["input"]})
                q = query.Query().is__id(pk)
                d = dict(i.get_one(s, q))

                for k, v in dt["output"].items():
                    self.assertEqual(v, getattr(d["bar"], k))

            else:
                with self.assertRaises((dt["output"], InterfaceError), msg=dt["input"]):
                    pk = i.insert(s, {"bar": dt["input"]})
                    q = query.Query().is__id(pk)
                    i.get_one(s, q)

    def test_insert(self):
        i, s = self.get_table()
        d = {
            'foo': 1,
            'bar': 'this is the value',
        }

        pk = i.insert(s, d)
        self.assertGreater(pk, 0)

    def test_get_sql(self):
        i = self.get_interface()
        s = self.get_schema()
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

        q.limit(222).offset(111)

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
            self.assertEqual(d[s._id.name], _id)

        q = query.Query()
        q.is__id(12334342)
        d = i.get_one(s, q)
        self.assertEqual({}, d)

    def test_get_one_offset(self):
        """make sure get_one() works as expected when an offset is set"""
        i, s = self.get_table()
        q = query.Query()
        q.set({
            'foo': 1,
            'bar': 'v1',
        })
        pk = i.insert(s, q.fields_set.fields)

        q = query.Query()
        q.set({
            'foo': 2,
            'bar': 'v2',
        })
        pk2 = i.insert(s, q.fields_set.fields)

        q = query.Query()
        q.desc__id().offset(1)
        d = i.get_one(s, q)
        self.assertEqual(d['_id'], pk)

        # just make sure to get expected result if no offset
        q = query.Query()
        q.desc__id()
        d = i.get_one(s, q)
        self.assertEqual(d['_id'], pk2)

        q = query.Query()
        q.desc__id().offset(2)
        d = i.get_one(s, q)
        self.assertEqual({}, d)

        q = query.Query()
        q.desc__id().offset(1).limit(5)
        d = i.get_one(s, q)
        self.assertEqual(d['_id'], pk)

        q = query.Query()
        q.desc__id().page(2)
        d = i.get_one(s, q)
        self.assertEqual(d['_id'], pk)

        q = query.Query()
        q.desc__id().page(2).limit(5)
        d = i.get_one(s, q)
        self.assertEqual({}, d)

    def test_get(self):
        i, s = self.get_table()
        _ids = self.insert(i, s, 5)

        q = query.Query()
        q.in__id(_ids)
        l = i.get(s, q)
        self.assertEqual(len(_ids), len(l))
        for d in l:
            self.assertTrue(d[s._id.name] in _ids)

        q.limit(2)
        l = i.get(s, q)
        self.assertEqual(2, len(l))
        for d in l:
            self.assertTrue(d[s._id.name] in _ids)

    def test_get_no_where(self):
        """test get with no where clause"""
        i, s = self.get_table()
        _ids = self.insert(i, s, 5)

        q = None
        l = i.get(s, q)
        self.assertEqual(5, len(l))

    def test_get_pagination(self):
        """test get but moving through the results a page at a time to make sure limit and offset works"""
        i, s = self.get_table()
        _ids = self.insert(i, s, 12)

        q = query.Query()
        q.limit(5)
        count = 0
        for p in range(1, 5):
            q.page(p)
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
        _ids = self.insert(i, s, 5)
        q = query.Query()
        r = i.count(s, q)
        self.assertEqual(5, r)

    def test_delete(self):
        # try deleting with no table
        i = self.get_interface()
        s = self.get_schema()
        q = query.Query().is_foo(1)
        r = i.delete(s, q)

        return

        i, s = self.get_table()

        # try deleting with no values in the table
        q = query.Query()
        q.is_foo(1)
        r = i.delete(s, q)
        self.assertEqual(0, r)

        _ids = self.insert(i, s, 5)

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
        q.set(d)
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
        table_name_1 = self.get_table_name()
        table_name_2 = self.get_table_name()

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

    def test_get_fields(self):
        i = self.get_interface()
        s = self.get_schema_all(i)
        fields = i.get_fields(str(s))

        for field_name, field in s:
            field2 = fields[field_name]
            if issubclass(field.interface_type, decimal.Decimal):
                self.assertEqual(float, field2["field_type"])

            else:
                self.assertEqual(field.interface_type, field2["field_type"])
            self.assertEqual(field.is_pk(), field2["pk"])
            self.assertEqual(field.required, field2["field_required"], field_name)

    def test__handle_field_error(self):
        i, s = self.get_table()
        s.set_field("che", Field(str, True))
        q = query.Query()
        q.set({
            'foo': 1,
            'bar': 'v1',
            'che': "this field will cause the query to fail",
        })

        self.assertFalse(i._handle_field_error(s, e=None))
#         with self.assertRaises(ValueError):
#             ret = i._set_all_fields(s)

        s = self.get_schema(table_name=str(s))
        s.che = str, False
        self.assertTrue(i._handle_field_error(s, e=None))
#         ret = i._set_all_fields(s)
#         self.assertTrue(ret)

    def test_handle_error_subquery(self):
        Foo = self.get_orm_class()
        Bar = self.get_orm_class()

        bar_q = Bar.query.select_foo()
        foo_ids = list(Foo.query.select_pk().in_foo(bar_q).values())
        self.assertEqual([], foo_ids) # no error means it worked

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

        s = self.get_schema(table_name=str(s))
        s.set_field("che", Field(str, False)) # not required so error recovery can fire
        pk = i.insert(s, fields)
        self.assertLess(0, pk)

    def test_null_values(self):
        i = self.get_interface()
        s = Schema(
            self.get_table_name(),
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
        s1 = self.get_schema(
            _id=Field(int, pk=True),
            foo=Field(int, True)
        )
        s2 = self.get_schema(
            _id=Field(int, pk=True),
            bar=Field(int, True),
            s_pk=Field(s1),
        )

        with i.transaction() as connection:
            pk1 = i.insert(s1, {"foo": 1}, connection=connection)
            pk2 = i.insert(s2, {"bar": 2, "s_pk": pk1}, connection=connection)

        r1 = i.get_one(s1, Query().eq__id(pk1))
        self.assertEqual(pk1, r1['_id'])

        r2 = i.get_one(s2, Query().eq__id(pk2))
        self.assertEqual(pk2, r2['_id'])
        self.assertEqual(pk1, r2['s_pk'])

    def test_transaction_nested_fail_2(self):
        """make sure 2 tables where the first one already exists works in a nested transaction"""
        i = self.get_interface()

        s1 = self.get_schema(
            foo=Field(int, True)
        )
        i.set_table(s1)

        s2 = self.get_schema(
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
        table_name_1 = "{}_1".format(self.get_table_name())
        table_name_2 = "{}_2".format(self.get_table_name())

        s1 = self.get_schema(
            foo=Field(int, True)
        )
        i.set_table(s1)

        s2 = self.get_schema(
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
        s1 = self.get_schema(
            foo=Field(int, True)
        )
        i.set_table(s1)

        s2 = self.get_schema(
            bar=Field(int, True),
            s_pk=Field(s1, True),
            s_pk2=Field(s1, True),
        )
        i.set_table(s2)

        # this is the table that will be created in the transaction
        s3 = self.get_schema(
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
            pout.b()
            pk3 = i.get(s3, Query().eq_s_pk(pk1), connection=connection)

        self.assertEqual(1, i.count(s2, query.Query()))

    def test_transaction_context(self):
        i = self.get_interface()
        table_name_1 = "{}_1".format(self.get_table_name())
        table_name_2 = "{}_2".format(self.get_table_name())

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
            s_pk=Field(s1, True),
        )
        i.set_table(s2)

        pk1 = 0
        pk2 = 0

        try:
            with i.transaction() as connection:
                pk1 = i.insert(s1, {"foo": 1}, connection=connection)

                with i.transaction(connection) as connection:
                    pk2 = i.set(s2, {"bar": 2, "s_pk": pk1}, connection=connection)
                    raise RuntimeError("testing")

        except Exception as e:
            pass

        self.assertEqual(0, i.count(s1, query.Query().is__id(pk1)))
        self.assertEqual(0, i.count(s2, query.Query().is__id(pk2)))

    def test_unique(self):
        i = self.get_interface()
        s = self.get_schema()
        s.set_field("should_be_unique", Field(int, True, unique=True))
        i.set_table(s)

        d = i.insert(s, {'foo': 1, 'bar': 'v1', 'should_be_unique': 1})

        #with self.assertRaises(prom.InterfaceError):
        with self.assertRaises(prom.UniqueError):
            d = i.insert(s, {'foo': 2, 'bar': 'v2', 'should_be_unique': 1})

    def test_ignore_case_primary_key(self):
        i = self.get_interface()
        s = self.get_schema(
            _id=Field(str, True, ignore_case=True, pk=True),
            foo=Field(int, True),
        )
        i.set_table(s)

        pk = i.insert(s, {'_id': "FOO", "foo": 1})

        with self.assertRaises(InterfaceError):
            i.insert(s, {'_id': "foo", "foo": 2})

    def test_ignore_case_field(self):
        i = self.get_interface()
        s = self.get_schema(
            foo=Field(str, True, ignore_case=True),
        )
        i.set_table(s)

        pk = i.insert(s, {'foo': "FOO"})
        pk2 = i.insert(s, {'foo': "BAR"})

        d = i.get_one(s, query.Query().eq_foo("foo"))
        self.assertEqual(pk, d["_id"])

        d = i.get_one(s, query.Query().eq_foo("baR"))
        self.assertEqual(pk2, d["_id"])

    def test_ignore_case_index(self):
        i = self.get_interface()
        s = Schema(
            self.get_table_name(),
            _id=Field(int, pk=True),
            foo=Field(str, True, ignore_case=True),
            bar=Field(str, True),
            index_foo=Index('foo', 'bar'),
        )
        i.set_table(s)

        v = 'foo-bar@example.com'
        d = i.insert(s, {'foo': v, 'bar': 'bar'})
        q = query.Query()
        q.is_foo(v)
        r = i.get_one(s, q)
        self.assertGreater(len(r), 0)

        # change the case of v
        lv = list(v)
        for x in range(len(v)):
            lv[x] = lv[x].upper()
            qv = "".join(lv)
            q = query.Query()
            q.eq_foo(qv)
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
        _ids = self.insert(i, s, 5)

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
            rows = list(q3.select_foo().asc_foo().limit(1).page(x).values())
            #pout.v(x, foos[x], rows[0])
            self.assertEqual(foos[x - 1], rows[0])

            q3 = q.copy()
            row = q3.select_foo().asc_foo().limit(1).page(x).value()
            self.assertEqual(foos[x - 1], row)

            q3 = q.copy()
            row = q3.select_foo().asc_foo().limit(1).page(x).value()
            self.assertEqual(foos[x - 1], row)

            q3 = q.copy()
            rows = list(q3.select_foo().in_foo(foos).asc_foo(foos).limit(1).page(x).values())
            self.assertEqual(foos[x - 1], rows[0])

            q3 = q.copy()
            row = q3.select_foo().in_foo(foos).asc_foo(foos).limit(1).page(x).value()
            self.assertEqual(foos[x - 1], row)

        for x in range(1, 9):
            q3 = q.copy()
            rows = list(q3.select_foo().asc_foo().limit(x).offset(x).values())
            #pout.v(x, foos[x], rows[0])
            self.assertEqual(foos[x], rows[0])

            q3 = q.copy()
            row = q3.select_foo().asc_foo().limit(x).offset(x).value()
            self.assertEqual(foos[x], row)

            q3 = q.copy()
            row = q3.select_foo().asc_foo().limit(x).offset(x).value()
            self.assertEqual(foos[x], row)

            q3 = q.copy()
            rows = list(q3.select_foo().in_foo(foos).asc_foo(foos).limit(1).offset(x).values())
            self.assertEqual(foos[x], rows[0])

            q3 = q.copy()
            row = q3.select_foo().in_foo(foos).asc_foo(foos).limit(1).offset(x).value()
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

        q4 = q.copy()
        rfoos = list(reversed(foos))
        rows = list(q4.select_foo().in_foo(foos).desc_foo(foos).values())
        for i, r in enumerate(rows):
            self.assertEqual(rfoos[i], r)

        qb = q.copy()
        rows = list(qb.in_foo(foos).asc_foo(foos).limit(2).offset(2).get())
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
        rows = list(qb.in_bar(bars).asc_bar(bars).limit(5).get())
        for i, r in enumerate(rows):
            self.assertEqual(bars[i], r.bar)

        qb = q.copy()
        rows = list(qb.in_bar(bars).asc_bar(bars).limit(2).offset(2).get())
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
            fields = self.get_fields(s)
            _id = i.insert(s, fields, connection=connection)
            #_id = self.insert(i, s, 1, connection=connection)[0]

        self.assertTrue(_id)

        q = query.Query()
        q.is__id(_id)
        d = i.get_one(s, q)
        self.assertGreater(len(d), 0)

        with self.assertRaises(RuntimeError):
            with i.transaction() as connection:
                fields = self.get_fields(s)
                _id = i.insert(s, fields, connection=connection)
                #_id = self.insert(i, s, 1, connection=connection)[0]
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
            self.get_table_name(),
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

    def test_group_field_name(self):
        i = self.get_interface()
        s = Schema(
            self.get_table_name(),
            _id=Field(int, True, pk=True),
            group=Field(str, True),
        )
        i.set_table(s)

        text = testdata.get_words()
        pk = i.insert(s, {'group': text})

        q = query.Query().is__id(pk)
        d = dict(i.get_one(s, q))
        self.assertEqual(text, d["group"])
        self.assertEqual(pk, d["_id"])

    def test_bignumber(self):
        i = self.get_interface()
        s = self.get_schema(
            foo=Field(int, True, max_size=int("9" * 78)),
        )
        i.set_table(s)

        foo = int("5" * 78)
        pk = i.insert(s, {"foo": foo})

        q = query.Query().is__id(pk)
        d = i.get_one(s, q)
        self.assertEqual(foo, d["foo"])

    def test_text_size_constraint(self):
        # both min and max size
        i, s = self.create_schema(
            foo=Field(str, True, min_size=10, max_size=20),
        )

        with self.assertRaises(InterfaceError):
            i.insert(s, {"foo": testdata.get_ascii(30)})

        with self.assertRaises(InterfaceError):
            i.insert(s, {"foo": testdata.get_ascii(5)})

        pk = i.insert(s, {"foo": testdata.get_ascii(15)})
        self.assertLess(0, pk)

        # just size
        i, s = self.create_schema(
            foo=Field(str, True, size=10),
        )

        with self.assertRaises(InterfaceError):
            i.insert(s, {"foo": testdata.get_ascii(20)})

        with self.assertRaises(InterfaceError):
            i.insert(s, {"foo": testdata.get_ascii(5)})

        pk = i.insert(s, {"foo": testdata.get_ascii(10)})
        self.assertLess(0, pk)

        # just max size
        i, s = self.create_schema(
            foo=Field(str, True, max_size=10),
        )

        with self.assertRaises(InterfaceError):
            foo = testdata.get_ascii(20)
            pk = i.insert(s, {"foo": foo})

        foo = testdata.get_ascii(5)
        pk = i.insert(s, {"foo": foo})
        d = i.get_one(s, query.Query().is__id(pk))
        self.assertEqual(foo, d["foo"])

        foo = testdata.get_ascii(10)
        pk = i.insert(s, {"foo": foo})
        d = i.get_one(s, query.Query().is__id(pk))
        self.assertEqual(foo, d["foo"])


    def test_upsert_pk(self):
        i, s = self.create_schema(
            foo=Field(int, True),
            bar=Field(str, True),
            che=Field(str, False),
            ifoobar=Index("foo", "bar", unique=True),
        )

        d = {"foo": 1, "bar": "bar 1"}
        pk = i.insert(s, d)

        # makes sure conflict update works as expected
        di = {"_id": pk, "foo": 2, "bar": "bar 2"}
        du = {"foo": 3}
        pk2 = i.upsert(s, di, du, ["_id"])
        self.assertEqual(pk, pk2)
        d = i.get_one(s, query.Query().is__id(pk))
        self.assertEqual(du["foo"], d["foo"])
        self.assertEqual("bar 1", d["bar"])

        # makes sure insert works as expected
        di = {"foo": 3, "bar": "bar 3"}
        du = {"che": "che 3"}
        pk3 = i.upsert(s, di, du, ["foo", "bar"])
        self.assertNotEqual(pk, pk3)
        d = i.get_one(s, query.Query().is__id(pk3))
        self.assertEqual(di["foo"], d["foo"])
        self.assertEqual(di["bar"], d["bar"])

    def test_upsert_index(self):
        i, s = self.create_schema(
            foo=Field(str, True),
            bar=Field(str, True),
            che=Field(str, True, default=""),
            baz=Field(int, True),
            foo_bar_che=Index("foo", "bar", "che", unique=True),
        )

        di = {"foo": "1", "bar": "1", "che": "1", "baz": 1}

        du = {"baz": 1}
        pk = i.upsert(s, di, du, ["foo", "bar", "che"])
        d = i.get_one(s, Query().is__id(pk))
        self.assertEqual(1, d["baz"])
        self.assertEqual(pk, d["_id"])

        du = {"baz": 2}
        pk = i.upsert(s, di, du, ["foo", "bar", "che"])
        d = i.get_one(s, Query().is__id(pk))
        self.assertEqual(2, d["baz"])
        self.assertEqual(pk, d["_id"])

    def test_stacktraces(self):
        i, s = self.create_schema(
            foo=Field(str, True),
        )

        try:
            i.insert(s, {"bar": 10}) # there is no bar field so this should fail

        except InterfaceError as e:
            # we want to make sure we aren't wrapping errors again and again
            self.assertFalse(isinstance(e.e, InterfaceError))


# https://docs.python.org/2/library/unittest.html#load-tests-protocol
def load_tests(loader, tests, pattern):
    return TestSuite()

