# -*- coding: utf-8 -*-
import random
import string
import decimal
import datetime

from datatypes import Datetime

from prom import query, InterfaceError
from prom.exception import PlaceholderError
from prom.config import Schema, Field, Index
from prom.query import Query
from prom.compat import *
import prom

from .. import (
    InterfaceData,
    IsolatedAsyncioTestCase,
)


class _BaseTestInterface(IsolatedAsyncioTestCase):
    """This contains tests that are common to all interfaces, it is extended by
    Postgres's Interface and SQLite's Interface classes
    """
    async def test_connection___str__(self):
        i = self.get_interface()
        async with i.connection() as conn:
            s = f"{conn}"
            self.assertRegex(s, r"0x[a-f0-9]+")

    async def test_connect_close(self):
        i = self.get_interface()
        await i.connect()
        await i.close()
        self.assertFalse(i.connected)

    async def test_raw_simple(self):
        i = self.get_interface()
        rows = await i.raw('SELECT 1')
        self.assertGreater(len(rows), 0)

    async def test_raw_mismatched_placeholders(self):
        i, s = await self.create_table()

        with self.assertRaises(PlaceholderError):
            await i.raw(
                "SELECT * FROM {} WHERE {} = {} AND {} = {}".format(
                    s,
                    "foo",
                    i.PLACEHOLDER,
                    "bar",
                    i.PLACEHOLDER,
                ),
                [1]
            )

    async def test_transaction_error(self):
        i = self.get_interface()
        with self.assertRaises(RuntimeError):
            async with i.transaction():
                raise RuntimeError()

    async def test_set_table_1(self):
        i, s = self.get_table()
        r = await i.has_table(str(s))
        self.assertFalse(r)

        r = await i.set_table(s)

        r = await i.has_table(str(s))
        self.assertTrue(r)

        # make sure known indexes are there
        indexes = await i.get_indexes(s)
        count = 0
        for known_index_name, known_index in s.indexes.items():
            for index_name, index_fields in indexes.items():
                if known_index.field_names == index_fields:
                    count += 1

        self.assertEqual(len(s.indexes), count)

        # make sure more exotic datatypes are respected
        s_ref = self.get_schema()
        await i.set_table(s_ref)
        s_ref_id = (await self.insert(i, s_ref, 1))

        s = self.get_schema(
            _id=Field(int, autopk=True),
            one=Field(bool, True),
            two=Field(int, True, size=50),
            three=Field(decimal.Decimal),
            four=Field(float, True, size=10),
            seven=Field(s_ref, False),
            eight=Field(datetime.datetime),
            nine=Field(datetime.date),
        )
        r = await i.set_table(s)
        d = {
            'one': True,
            'two': 50,
            'three': decimal.Decimal('1.5'),
            'four': 1.987654321,
            'seven': s_ref_id,
            'eight': datetime.datetime(
                2005,
                7,
                14,
                12,
                30,
                tzinfo=datetime.timezone.utc
            ),
            'nine': datetime.date(2005, 9, 14),
        }
        pk = await i.insert(s, d)
        q = Query().eq__id(pk)
        odb = await i.one(s, q)
        for k, v in d.items():
            self.assertEqual(v, odb[k])

    async def test_get_tables(self):
        i, s = self.get_table()
        r = await i.set_table(s)
        r = await i.get_tables()
        self.assertTrue(str(s) in r)

        r = await i.get_tables(str(s))
        self.assertTrue(str(s) in r)

    async def test_get_with_modified_table(self):
        """Make sure SELECT statements with a non-existent field don't fail.

        NOTE -- SQLite 3.40.1 is not raising an error on selecting non-existent
            fields, this feels like a change to me

        NOTE -- Psycopg3 also changed how it handles None/NULL values:
            https://www.psycopg.org/psycopg3/docs/basic/from_pg2.html#you-cannot-use-is-s
        """
        i, s = await self.create_table(one=Field(int, True))

        # Add new column
        s.set_field("two", Field(int, False))

        # Test if select query succeeds
        await i.get(s, Query().eq_two(None))

    async def test_unsafe_delete_table_single(self):
        i, s = self.get_table()

        r = await i.set_table(s)
        self.assertTrue(await i.has_table(str(s)))

        r = await i.unsafe_delete_table(s)
        self.assertFalse(await i.has_table(str(s)))

        # make sure it persists
        await i.close()
        i = self.get_interface()
        self.assertFalse(await i.has_table(str(s)))

    async def test_unsafe_delete_tables_nofk(self):
        i = self.get_interface()
        s1 = self.get_schema()
        await i.set_table(s1)
        s2 = self.get_schema()
        await i.set_table(s2)

        self.assertTrue(await i.has_table(s1))
        self.assertTrue(await i.has_table(s2))

        await i.unsafe_delete_tables()

        self.assertFalse(await i.has_table(s1))
        self.assertFalse(await i.has_table(s2))

    async def test_unsafe_delete_tables_fk(self):
        i = self.get_interface()

        s1 = self.get_schema("s1")
        await i.set_table(s1)
        s2 = self.get_schema("s2", s1_id=Field(s1, True))
        await i.set_table(s2)

        self.assertTrue(await i.has_table(s1))
        self.assertTrue(await i.has_table(s2))

        await i.unsafe_delete_tables()

        self.assertFalse(await i.has_table(s1))
        self.assertFalse(await i.has_table(s2))

    async def test_readonly(self):
        i, s = self.get_table()
        await i.readonly(True)

        with self.assertRaises(InterfaceError):
            await i.set_table(s)
        self.assertFalse(await i.has_table(s))

        await i.readonly(False)
        await i.set_table(s)
        self.assertTrue(await i.has_table(s))

    async def test_custom_pk_int(self):
        i, s = await self.create_table(
            _id=Field(int, autopk=True),
            bar=Field(str)
        )

        pk = await i.insert(s, {"bar": "barvalue"})
        self.assertLess(0, pk)

        d = await i.one(s, Query().select__id().eq__id(pk))
        self.assertEqual(pk, d["_id"])

        pk_custom = self.get_int()
        pk = await i.insert(s, {"_id": pk_custom})
        self.assertEqual(pk_custom, pk)

        d = await i.one(s, Query().select__id().eq__id(pk_custom))
        self.assertEqual(pk_custom, d["_id"])

    async def test_field_bool(self):
        """There was a bug where SQLite boolean field always returned True, this
        tests to make sure that is fixed and it won't happen again"""
        i, s = await self.create_table(bar=Field(bool), che=Field(bool))
        pk = await i.insert(s, {"bar": False, "che": True})

        d = dict(await i.one(s, Query().eq__id(pk)))
        self.assertFalse(d["bar"])
        self.assertTrue(d["che"])

    async def test_field_dict(self):
        i, s = await self.create_table(
            foo=Field(dict)
        )

        foo = {"bar": 1, "che": "che 1"}
        pk = await i.insert(s, {"foo": foo})

        d = await i.one(s, Query().eq__id(pk))
        self.assertEqual(1, d["foo"]["bar"])
        self.assertEqual("che 1", d["foo"].get("che"))

    async def test_field_datetime_datatypes(self):
        """Makes sure datatypes.Datetime works for the different interfaces"""
        i, s = await self.create_table(
            bar=Field(datetime.datetime),
        )

        dt = Datetime()
        pk = await i.insert(s, {"bar": dt})

        d = await i.one(s, Query().eq_bar(dt))
        self.assertEqual(dt, d["bar"])

    async def test_field_datetime_string(self):
        """
        https://github.com/Jaymon/prom/issues/84
        """
        i, s = await self.create_table(
            bar=Field(datetime.datetime),
        )

        datestr = "2019-10-08 20:18:59.566Z"
        pk = await i.insert(s, {"bar": datestr})

        d = await i.one(s, Query().eq__id(pk))
        self.assertEqual(Datetime(datestr), d["bar"])

    async def test_field_datetime_iso8601(self):
        """make sure ISO 8601 formatted datestamps can be added to the db

        NOTE -- postgres actually validates the datestamp on insert, so it will 
            reject bad datestamps on insert while sqlite will gladly insert them
            and then it fails while pulling them out, this is not ideal
        """
        i, s = await self.create_table(
            bar=Field(datetime.datetime),
        )

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
                pk = await i.insert(s, {"bar": dt["input"]})
                d = dict(await i.one(s, Query().eq__id(pk)))

                for k, v in dt["output"].items():
                    self.assertEqual(v, getattr(d["bar"], k), dt["input"])

            else:
                output = (dt["output"], InterfaceError)
                with self.assertRaises(output, msg=dt["input"]):
                    pk = await i.insert(s, {"bar": dt["input"]})
                    i.one(s, Query().eq__id(pk))

    async def test_field_none(self):
        """Make sure we can query fields using None"""
        i, s = await self.create_table(
            one=Field(int, True),
            two=Field(int, False)
        )

        pk_eq_null = await i.insert(s, {"one": 1})
        pk_ne_null = await i.insert(s, {"one": 1, "two": 2})

        r = await i.get(s, Query().eq_two(None))
        self.assertEqual(1, len(r))
        self.assertEqual(pk_eq_null, r[0]["_id"])

        r = await i.get(s, Query().ne_two(None))
        self.assertEqual(1, len(r))
        self.assertEqual(pk_ne_null, r[0]["_id"])

    async def test_insert_1(self):
        i, s = await self.create_table()

        d = {
            'foo': 1,
            'bar': 'this is the value',
        }

        pk = await i.insert(s, d)
        self.assertGreater(pk, 0)

    async def test_inserts(self):
        i, s = await self.create_table()
        count = 1000

        self.assertEqual(0, await i.count(s, Query()))
        def rows():
            for x in range(count):
                yield (x, f"s{x}")

        await i.inserts(
            s,
            ["foo", "bar"],
            rows(),
        )

        self.assertEqual(count, await i.count(s, Query()))

    async def test_render_sql_1(self):
        i, s = self.get_table()
        q = Query()

        q.in__id(range(1, 5))
        sql, sql_args = i.render_sql(s, q)
        self.assertTrue('_id' in sql)
        self.assertEqual(4, len(sql_args))

        q.gt_foo(5)

        sql, sql_args = i.render_sql(s, q)
        self.assertTrue('foo' in sql)
        self.assertTrue('AND' in sql)
        self.assertEqual(5, len(sql_args))

        q.asc_foo().desc_bar()
        sql, sql_args = i.render_sql(s, q)
        self.assertTrue('ORDER BY' in sql)
        self.assertTrue('ASC' in sql)
        self.assertTrue('DESC' in sql)

        q.limit(222).offset(111)

        sql, sql_args = i.render_sql(s, q)
        self.assertTrue('LIMIT' in sql)
        self.assertTrue('OFFSET' in sql)
        self.assertTrue('222' in sql)
        self.assertTrue('111' in sql)

    async def test_one_1(self):
        i, s = await self.create_table()
        _ids = await self.insert(i, s, 2)

        for _id in _ids:
            d = await i.one(s, Query().eq__id(_id))
            self.assertEqual(d[s._id.name], _id)

        d = await i.one(s, Query().eq__id(12334342))
        self.assertEqual({}, d)

    async def test_one_offset(self):
        """make sure get_one() works as expected when an offset is set"""
        i, s = await self.create_table()

        q = Query().set({
            'foo': 1,
            'bar': 'v1',
        })
        pk = await i.insert(s, q.fields_set.fields)

        q = Query().set({
            'foo': 2,
            'bar': 'v2',
        })
        pk2 = await i.insert(s, q.fields_set.fields)

        q = Query().desc__id().offset(1)
        d = await i.one(s, q)
        self.assertEqual(d['_id'], pk)

        # just make sure to get expected result if no offset
        q = Query().desc__id()
        d = await i.one(s, q)
        self.assertEqual(d['_id'], pk2)

        q = Query().desc__id().offset(2)
        d = await i.one(s, q)
        self.assertEqual({}, d)

        q = Query().desc__id().offset(1).limit(5)
        d = await i.one(s, q)
        self.assertEqual(d['_id'], pk)

        q = Query().desc__id().page(2)
        d = await i.one(s, q)
        self.assertEqual(d['_id'], pk)

        q = Query().desc__id().page(2).limit(5)
        d = await i.one(s, q)
        self.assertEqual({}, d)

    async def test_get_1(self):
        i, s = await self.create_table()
        _ids = await self.insert(i, s, 5)

        q = Query().in__id(_ids)
        l = await i.get(s, q)
        self.assertEqual(len(_ids), len(l))
        for d in l:
            self.assertTrue(d[s._id.name] in _ids)

        q.limit(2)
        l = await i.get(s, q)
        self.assertEqual(2, len(l))
        for d in l:
            self.assertTrue(d[s._id.name] in _ids)

    async def test_get_no_where(self):
        """test get with no where clause"""
        i, s = await self.create_table()
        _ids = await self.insert(i, s, 5)

        l = await i.get(s, None)
        self.assertEqual(5, len(l))

    async def test_get_pagination_1(self):
        """test get but moving through the results a page at a time to make sure
        limit and offset works"""
        i, s = await self.create_table()
        _ids = await self.insert(i, s, 12)

        q = Query().limit(5)
        count = 0
        for p in range(1, 5):
            q.page(p)
            l = await i.get(s, q)
            for d in l:
                self.assertTrue(d[s._id.name] in _ids)

            count += len(l)

        self.assertEqual(12, count)

    async def test_get_pagination_offset_only(self):
        offset = 5
        i, s = await self.create_table()
        _ids = (await self.insert(i, s, 10))[offset:]

        count = 0
        q = Query().offset(offset)
        for d in await i.get(s, q):
            self.assertTrue(d[s._id.name] in _ids)
            count += 1
        self.assertEqual(offset, count)

    async def test_count(self):
        i, s = await self.create_table()

        # first try it with no rows
        r = await i.count(s, Query())
        self.assertEqual(0, r)

        # now try it with rows
        _ids = await self.insert(i, s, 5)
        r = await i.count(s, Query())
        self.assertEqual(5, r)

    async def test_delete(self):
        # try deleting with no table
        i, s = self.get_table()
        r = await i.delete(s, Query().eq_foo(1))

        # try deleting with no values in the table
        r = await i.delete(s, Query().eq_foo(1))
        self.assertEqual(0, r)

        _ids = await self.insert(i, s, 5)
        q = Query().in__id(_ids)

        l = await i.get(s, q)
        self.assertEqual(5, len(l))

        # delete all the inserted values
        r = await i.delete(s, q)
        self.assertEqual(5, r)

        # verify rows are deleted
        l = await i.get(s, q)
        self.assertEqual(0, len(l))

        # make sure it stuck
        await i.close()
        #i = self.get_interface()
        l = await i.get(s, q)
        self.assertEqual(0, len(l))

    async def test_update(self):
        i, s = await self.create_table()
        d = {
            'foo': 1,
            'bar': 'value 1',
        }

        pk = await i.insert(s, d)
        self.assertGreater(pk, 0)

        d = {
            'foo': 2,
            'bar': 'value 2',
        }
        q = Query().set(d).eq__id(pk)
        row_count = await i.update(s, d, q)

        # let's pull it out and make sure it persisted
        gd = await i.one(s, Query().eq__id(pk))
        self.assertEqual(d['foo'], gd['foo'])
        self.assertEqual(d['bar'], gd['bar'])
        self.assertEqual(pk, gd["_id"])

    async def test_ref_strong(self):
        i = self.get_interface()
        s_1 = self.get_schema(
            _id=Field(int, autopk=True),
            foo=Field(int, True)
        )
        s_2 = self.get_schema(
            _id=Field(int, autopk=True),
            s_pk=Field(s_1, True),
        )

        await i.set_table(s_1)
        await i.set_table(s_2)

        pk1 = await i.insert(s_1, {'foo': 1})
        pk2 = await i.insert(s_2, {'s_pk': pk1})
        q2 = Query().eq__id(pk2)

        # make sure it exists and is visible
        r = await i.one(s_2, q2)
        self.assertGreater(len(r), 0)

        q1 = Query().eq__id(pk1)
        await i.delete(s_1, q1)

        r = await i.one(s_2, q2)
        self.assertEqual({}, r)

    async def test_ref_weak(self):
        i = self.get_interface()
        s_1 = self.get_schema(
            _id=Field(int, autopk=True),
            foo=Field(int, True)
        )
        s_2 = self.get_schema(
            _id=Field(int, autopk=True),
            s_pk=Field(s_1, False),
        )

        await i.set_table(s_1)
        await i.set_table(s_2)

        pk1 = await i.insert(s_1, {'foo': 1})
        pk2 = await i.insert(s_2, {'s_pk': pk1})
        q2 = Query().eq__id(pk2)

        # make sure it exists and is visible
        r = await i.one(s_2, q2)
        self.assertGreater(len(r), 0)

        q1 = Query().eq__id(pk1)
        await i.delete(s_1, q1)

        r = await i.one(s_2, q2)
        self.assertGreater(len(r), 0)
        self.assertIsNone(r['s_pk'])

    async def test_handle_error_ref(self):
        i = self.get_interface()
        s_1 = self.get_schema(
            _id=Field(int, autopk=True),
            foo=Field(int, True)
        )
        s_2 = self.get_schema(
            _id=Field(int, autopk=True),
            bar=Field(int, True),
            s_pk=Field(s_1),
        )

        r = await i.one(s_2, Query().eq_bar(1))
        self.assertTrue(await i.has_table(s_1.table_name))
        self.assertTrue(await i.has_table(s_2.table_name))

    async def test_get_fields_1(self):
        i = self.get_interface()
        s = self.get_schema_all(i)
        await i.set_table(s)

        fields = await i.get_fields(str(s))

        for field_name, field in s:
            field2 = fields[field_name]
            if issubclass(field.interface_type, decimal.Decimal):
                self.assertEqual(float, field2["field_type"])

            else:
                self.assertEqual(field.interface_type, field2["field_type"])
            self.assertEqual(field.is_pk(), field2["pk"])
            self.assertEqual(
                field.required,
                field2["field_required"],
                field_name
            )

    async def test__handle_field_error(self):
        """Make sure fields can be added if they can take NULL as a value in
        the db"""
        i, s = await self.create_table()

        # this field can't take NULL for a value so adding it should fail
        s.set_field("che", Field(str, True))
        self.assertFalse(await i._handle_field_error(s, e=None))

        s = self.get_schema()
        await i.set_table(s)
        # this field can take NULL as a value so adding it should succeed
        s.set_field("che", Field(str, False))
        self.assertTrue(await i._handle_field_error(s, e=None))

    async def test_handle_error_column(self):
        i, s = await self.create_table()
        s.set_field("che", Field(str, True)) # it's required
        fields = {
            'foo': 1,
            'bar': 'v1',
            'che': "this field will cause the query to fail",
        }

        with self.assertRaises(prom.InterfaceError):
            rd = await i.insert(s, fields)

        s = self.get_schema()
        s.set_field("che", Field(str, False)) # not required
        pk = await i.insert(s, fields)
        self.assertLess(0, pk)

    async def test_handle_error_subquery(self):
        Foo = self.get_orm_class()
        Bar = self.get_orm_class()
        i = Foo.interface

        bar_q = Bar.query.select_foo()
        foo_ids = await i.get(Foo.schema, Foo.query.select_pk().in_foo(bar_q))
        self.assertEqual([], foo_ids) # no error means it worked

    async def test_error_unsupported_type(self):
        class Che(object): pass
        i, s = await self.create_table()

        q = Query().select_foo()
        q.in_bar([1, 2]).eq_che(Che())

        with self.assertRaises(PlaceholderError):
            await i.get(s, q)

    async def test_create_custom_error(self):
        i = self.get_interface()
        class CustomError(Exception): pass

        e = CustomError()
        e2 = i.create_error(e)
        self.assertIsInstance(e2, CustomError)

    async def test_null_values(self):
        i, s = await self.create_table(
            foo=Field(int, False),
            bar=Field(int, False)
        )

        # add one with non NULL foo
        pk1 = await i.insert(s, {"bar": 1, "foo": 2})

        # and one with NULL foo
        pk2 = await i.insert(s, {"bar": 1})

        r = await i.one(s, Query().eq_bar(1).eq_foo(None))
        self.assertEqual(pk2, r['_id'])

        r = await i.one(s, Query().eq_bar(1).ne_foo(None))
        self.assertEqual(pk1, r['_id'])

    async def test_transaction_nested_fail_1(self):
        """make sure 2 new tables in a wrapped transaction work as expected"""
        i = self.get_interface()

        s1 = self.get_schema(
            foo=Field(int, True)
        )
        s2 = self.get_schema(
            bar=Field(int, True),
            s_pk=Field(s1),
        )

        async with i.transaction() as connection:
            pk1 = await i.insert(s1, {"foo": 1}, connection=connection)
            pk2 = await i.insert(
                s2,
                {"bar": 2, "s_pk": pk1},
                connection=connection
            )

        r1 = await i.one(s1, Query().eq__id(pk1))
        self.assertEqual(pk1, r1['_id'])

        r2 = await i.one(s2, Query().eq__id(pk2))
        self.assertEqual(pk2, r2['_id'])
        self.assertEqual(pk1, r2['s_pk'])

    async def test_transaction_nested_fail_2(self):
        """make sure 2 tables where the first one already exists works in a
        nested transaction"""
        i = self.get_interface()

        s1 = self.get_schema(
            foo=Field(int, True)
        )
        i.set_table(s1)

        s2 = self.get_schema(
            bar=Field(int, True),
            s_pk=Field(s1, True),
        )

        async with i.transaction() as connection:
            pk1 = await i.insert(s1, {"foo": 1}, connection=connection)
            pk2 = await i.insert(
                s2,
                {"bar": 2, "s_pk": pk1},
                connection=connection
            )

        r1 = await i.one(s1, query.Query().eq__id(pk1))
        self.assertEqual(pk1, r1['_id'])

        r2 = await i.one(s2, Query().eq__id(pk1))
        self.assertEqual(pk2, r2['_id'])
        self.assertEqual(r2['s_pk'], pk1)

    async def test_transaction_nested_fail_3(self):
        """make sure 2 tables where the first one already exists works, and
        second one has 2 refs"""
        i = self.get_interface()

        s1 = self.get_schema(
            foo=Field(int, True)
        )
        i.set_table(s1)

        s2 = self.get_schema(
            bar=Field(int, True),
            s_pk=Field(s1, True),
            s_pk2=Field(s1, True),
        )

        pk1 = await i.insert(s1, {"foo": 1})
        pk2 = await i.insert(s1, {"foo": 1})
        pk3 = await i.insert(s2, {"bar": 2, "s_pk": pk1, "s_pk2": pk2})

        r1 = await i.one(s1, Query().eq__id(pk1))
        self.assertEqual(r1['_id'], pk1)

        r2 = await i.one(s2, Query().eq__id(pk3))
        self.assertEqual(r2['_id'], pk3)
        self.assertEqual(r2['s_pk'], pk1)
        self.assertEqual(r2['s_pk2'], pk2)

    async def test_transaction_nested_fail_4(self):
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

        pk1 = await i.insert(s1, {"foo": 1})
        pk12 = await i.insert(s1, {"foo": 12})

        self.assertEqual(0, await i.count(s2, Query()))

        async with i.transaction() as connection:

            # create something and put in table 2
            pk2 = await i.insert(
                s2,
                {"bar": 2, "s_pk": pk1, "s_pk2": pk12},
                connection=connection
            )

            # now this should cause the stuff to fail
            # it fails on the select because a new transaction isn't started, so 
            # it just discards all the current stuff and adds the table, had
            # this been a mod query (eg, insert) it would not have failed, this
            # is fixed by wrapping selects in a transaction if an active
            # transaction is found
            pk3 = await i.get(s3, Query().eq_s_pk(pk1), connection=connection)

        self.assertEqual(1, await i.count(s2, Query()))

    async def test_transaction_connection_1(self):
        """This is the best test for seeing if transactions are working as
        expected"""
        i, s = await self.create_table()
        conn = await i.get_connection()

        await conn.transaction_start(prefix="c1")

        self.assertIsNotNone(conn.interface)
        await i.insert(s, self.get_fields(s), connection=conn)
        self.assertIsNotNone(conn.interface)

        await conn.execute("SELECT true")
        await conn.transaction_start(prefix="c2")
        await conn.execute("SELECT true")
        await conn.transaction_start(prefix="c3")
        await conn.execute("SELECT true")
        await conn.transaction_start(prefix="c4")

        self.assertIsNotNone(conn.interface)
        await i.insert(s, self.get_fields(s), connection=conn)
        self.assertIsNotNone(conn.interface)

        await conn.transaction_stop()
        await conn.execute("SELECT true")
        await conn.transaction_stop()
        await conn.execute("SELECT true")
        await conn.transaction_stop()
        await conn.execute("SELECT true")
        await conn.transaction_stop()

    async def test_transaction_connection_2(self):
        """Make sure descendant txs inherit settings from ancestors unless
        explicitely overridden"""
        i = self.get_interface()
        conn = await i.get_connection()

        await conn.transaction_start(nest=False)
        tx = conn.transaction_current()
        self.assertFalse(tx["ignored"])
        self.assertFalse(tx["nest"])

        await conn.transaction_start()
        tx = conn.transaction_current()
        self.assertTrue(tx["ignored"])
        self.assertFalse(tx["nest"])

        await conn.transaction_start(nest=True)
        tx = conn.transaction_current()
        self.assertFalse(tx["ignored"])
        self.assertTrue(tx["nest"])

        await conn.transaction_start()
        tx = conn.transaction_current()
        self.assertFalse(tx["ignored"])
        self.assertTrue(tx["nest"])

        # finish the two previous txs that allowed nesting
        await conn.transaction_stop()
        await conn.transaction_stop()

        # a new tx that doesn't set nest should inherit ancestor's setting
        await conn.transaction_start()
        tx = conn.transaction_current()
        self.assertTrue(tx["ignored"])
        self.assertFalse(tx["nest"])

    async def test_transaction_context_1(self):
        # these 2 tables exist before the transaction starts
        i, s1 = await self.create_table(
            foo=Field(int, True)
        )

        i, s2 = await self.create_table(
            interface=i,
            bar=Field(int, True),
            s_pk=Field(s1, True),
        )

        pk1 = 0
        pk2 = 0

        try:
            async with i.transaction() as connection:
                pk1 = await i.insert(s1, {"foo": 1}, connection=connection)

                async with i.transaction(connection) as connection:
                    pk2 = await i.set(
                        s2,
                        {"bar": 2, "s_pk": pk1},
                        connection=connection
                    )
                    raise RuntimeError("testing")

        except Exception as e:
            pass

        self.assertEqual(0, await i.count(s1, Query().eq__id(pk1)))
        self.assertEqual(0, await i.count(s2, Query().eq__id(pk2)))

    async def test_transaction_context_manager(self):
        """make sure the with transaction() context manager works as expected"""
        i, s = self.get_table()

        async with i.transaction() as connection:
            fields = self.get_fields(s)
            _id = await i.insert(s, fields, connection=connection)

        self.assertTrue(_id)

        d = await i.one(s, Query().eq__id(_id))
        self.assertGreater(len(d), 0)

        with self.assertRaises(RuntimeError):
            async with i.transaction() as connection:
                fields = self.get_fields(s)
                _id = await i.insert(s, fields, connection=connection)
                raise RuntimeError("this should fail")

        d = await i.one(s, Query().eq__id(_id))
        self.assertEqual(len(d), 0)

    async def test_transaction_no_nest(self):
        """Make sure you can turn off nesting in transactions"""
        i, s = await self.create_table()

        try:
            async with i.transaction(nest=False, prefix="tx_no_nest") as conn:
                await i.insert(s, self.get_fields(s))

                async with i.transaction() as conn:
                    await i.insert(s, self.get_fields(s))
                    raise RuntimeError("testing")

        except Exception as e:
            pass

        self.assertEqual(0, await i.count(s, Query()))

    async def test_unique(self):
        i, s = self.get_table()
        s.set_field("should_be_unique", Field(int, True, unique=True))
        await i.set_table(s)

        d = await i.insert(s, {'foo': 1, 'bar': 'v1', 'should_be_unique': 1})

        #with self.assertRaises(prom.InterfaceError):
        with self.assertRaises(prom.UniqueError):
            d = await i.insert(
                s,
                {'foo': 2, 'bar': 'v2', 'should_be_unique': 1}
            )

    async def test_ignore_case_primary_key(self):
        i, s = await self.create_table(
            _id=Field(str, True, ignore_case=True, pk=True),
            foo=Field(int, True),
        )

        pk = await i.insert(s, {'_id': "FOO", "foo": 1})

        with self.assertRaises(InterfaceError):
            await i.insert(s, {'_id': "foo", "foo": 2})

    async def test_ignore_case_field(self):
        i, s = await self.create_table(
            foo=Field(str, True, ignore_case=True),
        )

        pk = await i.insert(s, {'foo': "FOO"})
        pk2 = await i.insert(s, {'foo': "BAR"})

        d = await i.one(s, Query().eq_foo("foo"))
        self.assertEqual(pk, d["_id"])

        d = await i.one(s, Query().eq_foo("baR"))
        self.assertEqual(pk2, d["_id"])

    async def test_ignore_case_index(self):
        i, s = await self.create_table(
            foo=Field(str, True, ignore_case=True),
            bar=Field(str, True),
            index_foo=Index('foo', 'bar'),
        )

        v = 'foo-bar@example.com'
        d = await i.insert(s, {'foo': v, 'bar': 'bar'})
        r = await i.one(s, Query().eq_foo(v))
        self.assertGreater(len(r), 0)

        # change the case of v
        lv = list(v)
        for x in range(len(v)):
            lv[x] = lv[x].upper()
            qv = "".join(lv)
            r = await i.one(s, Query().eq_foo(qv))
            self.assertGreater(len(r), 0)
            lv[x] = lv[x].lower()

        d = await i.insert(s, {'foo': 'FoO', 'bar': 'bar'})
        r = await i.one(s, Query().eq_foo('foo'))
        self.assertGreater(len(r), 0)
        self.assertEqual(r['foo'], 'FoO')

        r = await i.one(s, Query().eq_foo('Foo').eq_bar('BAR'))
        self.assertEqual(len(r), 0)

        r = await i.one(s, Query().eq_foo('FoO').eq_bar('bar'))
        self.assertGreater(len(r), 0)
        self.assertEqual(r['foo'], 'FoO')

        d = await i.insert(s, {'foo': 'foo2', 'bar': 'bar'})
        r = await i.one(s, Query().eq_foo('foo2'))
        self.assertGreater(len(r), 0)
        self.assertEqual(r['foo'], 'foo2')

    async def test_in_sql(self):
        i, s = await self.create_table()
        _ids = await self.insert(i, s, 5)

        l = list(await i.get(s, Query().in__id(_ids)))
        self.assertEqual(len(l), 5)

    async def test_render_date_field_sql(self):
        """this tests the common date kwargs you can use (in both SQLight and
        Postgres) if we ever add other backends this might need to be moved out
        of the general generator test"""
        i, s = await self.create_table(
            foo=Field(datetime.datetime, True),
            index_foo=Index('foo'),
        )

        pk20 = await i.insert(s, {'foo': Datetime(2014, 4, 20)})
        pk21 = await i.insert(s, {'foo': Datetime(2014, 4, 21)})

        d = await i.one(s, Query().eq_foo(day=20))
        self.assertEqual(d['_id'], pk20)

        d = await i.one(s, Query().eq_foo(day=21, month=4))
        self.assertEqual(d['_id'], pk21)

        d = await i.one(s, Query().eq_foo(day=21, month=3))
        self.assertFalse(d)

    async def test_group_field_name(self):
        i, s = await self.create_table(
            group=Field(str, True),
        )

        text = self.get_words()
        pk = await i.insert(s, {'group': text})
        d = dict(await i.one(s, Query().eq__id(pk)))
        self.assertEqual(text, d["group"])
        self.assertEqual(pk, d["_id"])

    async def test_bignumber(self):
        i, s = await self.create_table(
            foo=Field(int, True, max_size=int("9" * 78)),
        )

        foo = int("5" * 78)
        pk = await i.insert(s, {"foo": foo})
        d = await i.one(s, Query().eq__id(pk))
        self.assertEqual(foo, d["foo"])

    async def test_text_size_constraint(self):
        # both min and max size
        i, s = await self.create_table(
            foo=Field(str, True, min_size=10, max_size=20),
        )

        with self.assertRaises(InterfaceError):
            await i.insert(s, {"foo": self.get_ascii(30)})

        with self.assertRaises(InterfaceError):
            await i.insert(s, {"foo": self.get_ascii(5)})

        pk = await i.insert(s, {"foo": self.get_ascii(15)})
        self.assertLess(0, pk)

        # just size
        i, s = await self.create_table(
            foo=Field(str, True, size=10),
        )

        with self.assertRaises(InterfaceError):
            await i.insert(s, {"foo": self.get_ascii(20)})

        with self.assertRaises(InterfaceError):
            await i.insert(s, {"foo": self.get_ascii(5)})

        pk = await i.insert(s, {"foo": self.get_ascii(10)})
        self.assertLess(0, pk)

        # just max size
        i, s = await self.create_table(
            foo=Field(str, True, max_size=10),
        )

        with self.assertRaises(InterfaceError):
            pk = await i.insert(s, {"foo": self.get_ascii(20)})

        foo = self.get_ascii(5)
        pk = await i.insert(s, {"foo": foo})
        d = await i.one(s, Query().eq__id(pk))
        self.assertEqual(foo, d["foo"])

        foo = self.get_ascii(10)
        pk = await i.insert(s, {"foo": foo})
        d = await i.one(s, Query().eq__id(pk))
        self.assertEqual(foo, d["foo"])

    async def test_upsert_pk(self):
        i, s = await self.create_table(
            foo=Field(int, True),
            bar=Field(str, True),
            che=Field(str, False),
            ifoobar=Index("foo", "bar", unique=True),
        )

        d = {"foo": 1, "bar": "bar 1"}
        pk = await i.insert(s, d)

        # makes sure conflict update works as expected
        di = {"_id": pk, "foo": 2, "bar": "bar 2"}
        du = {"foo": 3}
        pk2 = await i.upsert(s, di, du, ["_id"])
        self.assertEqual(pk, pk2)
        d = await i.one(s, Query().eq__id(pk))
        self.assertEqual(du["foo"], d["foo"])
        self.assertEqual("bar 1", d["bar"])

        # makes sure insert works as expected
        di = {"foo": 3, "bar": "bar 3"}
        du = {"che": "che 3"}
        pk3 = await i.upsert(s, di, du, ["foo", "bar"])
        self.assertNotEqual(pk, pk3)
        d = await i.one(s, Query().eq__id(pk3))
        self.assertEqual(di["foo"], d["foo"])
        self.assertEqual(di["bar"], d["bar"])

    async def test_upsert_index(self):
        i, s = await self.create_table(
            foo=Field(str, True),
            bar=Field(str, True),
            che=Field(str, True, default=""),
            baz=Field(int, True),
            foo_bar_che=Index("foo", "bar", "che", unique=True),
        )

        di = {"foo": "1", "bar": "1", "che": "1", "baz": 1}

        du = {"baz": 1}
        pk = await i.upsert(s, di, du, ["foo", "bar", "che"])
        d = await i.one(s, Query().eq__id(pk))
        self.assertEqual(1, d["baz"])
        self.assertEqual(pk, d["_id"])

        du = {"baz": 2}
        pk = await i.upsert(s, di, du, ["foo", "bar", "che"])
        d = await i.one(s, Query().eq__id(pk))
        self.assertEqual(2, d["baz"])
        self.assertEqual(pk, d["_id"])

    async def test_stacktraces(self):
        i, s = await self.create_table(
            foo=Field(str, True),
        )

        try:
            # there is no bar field so this should fail
            await i.insert(s, {"bar": 10})

        except InterfaceError as e:
            # we want to make sure we aren't wrapping errors again and again
            self.assertFalse(isinstance(e.e, InterfaceError))

