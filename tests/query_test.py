# -*- coding: utf-8 -*-
import datetime
import re
import inspect
import random

from . import (
    EnvironTestCase,
    TestCase,
)
from prom.query import (
    Query,
    QueryBounds,
    QueryField,
    QueryFields,
    Iterator,
)
from prom.config import Field
from prom.compat import *


class QueryFieldTest(TestCase):
    def test___new__(self):
        q = self.get_query()
        f = QueryField(q, "MAX(foo)")
        self.assertEqual("foo", f.name)
        self.assertEqual("MAX", f.function_name)

    def test_in_set_clause(self):
        q = self.get_query()
        q.set_field("foo", 1)

        qf = q.fields_set.get_field("foo")
        self.assertTrue(qf.in_set_clause())
        self.assertFalse(qf.in_where_clause())

    def test_in_where_clause(self):
        q = self.get_query()
        q.eq_field("foo", 1)

        qf = q.fields_where.get_field("foo")
        self.assertTrue(qf.in_where_clause())
        self.assertFalse(qf.in_set_clause())


class QueryFieldsTest(TestCase):
    def test_fields(self):
        q = self.get_query()
        fs = QueryFields()
        fs.append(QueryField(q, "foo", 1))
        fs.append(QueryField(q, "foo", 2))
        fs.append(QueryField(q, "bar", 3))
        #fs.append(QueryField(q, "che", 4))

        fields = fs.fields
        self.assertEqual(2, fields["foo"])
        self.assertEqual(3, fields["bar"])
        #self.assertEqual(4, fields["che"])

    def test___bool__(self):
        fs = QueryFields()
        self.assertFalse(fs)

        q = self.get_query()
        fs.append(QueryField(q, "foo", 1))
        self.assertTrue(fs)

    def test_names(self):
        q = self.get_query()
        fs = QueryFields()

        fs.append(QueryField(q, "foo", None))
        fs.append(QueryField(q, "bar", None))
        fs.append(QueryField(q, "foo", None))
        self.assertEqual(["foo", "bar"], list(fs.names()))


class QueryBoundsTest(TestCase):
    def test_find_more_index(self):
        b = QueryBounds()
        b.limit = 3

        b.page = 0
        index = b.find_more_index()
        self.assertEqual(3, index)

        b.page = 2
        index = b.find_more_index()
        self.assertEqual(6, index)

    def test___nonzero__(self):
        b = QueryBounds()

        self.assertFalse(b)

    def test_offset_from_page(self):
        lc = QueryBounds()
        lc.page = 2
        self.assertEqual(1, lc.offset)

        lc = QueryBounds()
        lc.limit = 5
        lc.page = 2
        self.assertEqual(5, lc.offset)
        self.assertEqual(5, lc.limit)

    def test_non_paginate_limit(self):
        lc = QueryBounds()

        self.assertEqual((0, 0), lc.get())

        lc.limit = 10

        self.assertEqual((10, 0), lc.get())

        lc.page = 1
        self.assertEqual((10, 0), lc.get())

        lc.offset = 15
        self.assertEqual((10, 15), lc.get())

        lc.page = 2
        self.assertEqual((10, 10), lc.get())

        lc.page = 3
        self.assertEqual((10, 20), lc.get())

        lc.page = 0
        self.assertEqual((10, 0), lc.get())

        with self.assertRaises(ValueError):
            lc.page = -10

        lc.offset = 0
        self.assertEqual((10, 0), lc.get())

        with self.assertRaises(ValueError):
            lc.offset = -10

        lc.limit = 0
        self.assertEqual((0, 0), lc.get())

        with self.assertRaises(ValueError):
            lc.limit = -10

    def test_paginate_limit(self):
        lc = QueryBounds()

        lc.limit = 10
        lc.paginate = True
        self.assertEqual(11, lc.limit_paginate)
        self.assertEqual((11, 0), lc.get())

        lc.page = 3
        self.assertEqual((11, 20), lc.get())

        lc.offset = 15
        self.assertEqual((11, 15), lc.get())

        lc.paginate = False
        self.assertEqual((10, 15), lc.get())


class QueryTest(EnvironTestCase):
    async def test_cursor(self):
        q = self.get_query()
        cursor = await q.cursor()
        self.assertFalse(inspect.iscoroutine(cursor))
        self.assertIsNotNone(cursor)

    async def test_get_1(self):
        orm_class = self.get_orm_class()
        _ids = await self.insert(orm_class, 2)

        count = 0
        async for o in await orm_class.query.get():
            count += 1
            self.assertEqual(orm_class, type(o))
            self.assertTrue(o._id in _ids)
            self.assertFalse(o.is_modified())
        self.assertEqual(count, len(_ids))

    async def test_get_all(self):
        count = 10
        q = self.get_query()
        await self.insert(q, count)

        # if no limit is set then it should go through all results
        rcount = 0
        async for r in await q.copy().get():
            rcount += 1
        self.assertEqual(count, rcount)

        # if there is a limit then all should only go until that limit
        rcount = 0
        async for r in await q.copy().limit(1).get():
            rcount += 1
        self.assertEqual(1, rcount)

        # only go until the end of the results
        rcount = 0
        async for r in await q.copy().limit(6).offset(6).get():
            rcount += 1
        self.assertEqual(4, rcount)

    async def test_get_pk(self):
        orm_class = self.get_orm_class()
        pks = await self.insert(orm_class, 2)

        res = await orm_class.query.select_pk().in_pk(pks).get()
        rpks = [r async for r in res]
        self.assertEqual(2, len(rpks))
        self.assertEqual(pks, rpks)

    async def test_get_values(self):
        q = self.get_query()

        count = 2
        pks = await self.insert(q, count)

        vals = [r async for r in q.copy().select_foo()]
        self.assertEqual(count, len(vals))
        for v in vals:
            self.assertTrue(isinstance(v, int))

        vals = [r async for r in q.copy().select_foo().select_bar()]
        self.assertEqual(count, len(vals))
        for v in vals:
            self.assertTrue(isinstance(v, list))

        vals = [r async for r in q.copy().select_foo().limit(1)]
        self.assertEqual(1, len(vals))

    async def test_one_1(self):
        orm_class = self.get_orm_class()
        _ids = await self.insert(orm_class, 2)

        o = await orm_class.query.one()
        self.assertEqual(orm_class, type(o))
        self.assertTrue(o._id in _ids)
        self.assertFalse(o.is_modified())

    async def test_one_value(self):
        _q = self.get_query()

        v = await _q.copy().select_foo().one()
        self.assertEqual(None, v)

        count = 2
        pks = await self.insert(_q, count)

        o = await _q.copy().eq_pk(pks[0]).one()
        v = await _q.copy().select_foo().eq_pk(o.pk).one()
        self.assertEqual(o.foo, v)

        v = await _q.copy().select_foo().select_bar().eq_pk(o.pk).one()
        self.assertEqual(o.foo, v[0])
        self.assertEqual(o.bar, v[1])

    async def test_count(self):
        orm_class = self.get_orm_class()
        await self.insert(orm_class, 10)

        self.assertEqual(5, await orm_class.query.offset(5).count())
        self.assertEqual(5, await orm_class.query.limit(5).count())
        self.assertEqual(10, await orm_class.query.count())

    async def test_has(self):
        q = self.get_query()
        self.assertFalse(await q.has())

        await self.insert(q, 1)
        self.assertTrue(await q.has())

    async def test_insert_and_update(self):
        orm_class = self.get_orm_class(
            foo=Field(int),
            bar=Field(str),
            _created=None,
            _updated=None
        )
        fields = dict(foo=1, bar="value 1")

        pk = (await orm_class.query.set(fields).insert())["_id"]
        self.assertLess(0, pk)

        o = await orm_class.query.eq_pk(pk).one()
        self.assertEqual(pk, o.pk)
        self.assertEqual(1, o.foo)
        self.assertTrue("value 1", o.bar)

        fields["foo"] = 2
        fields["bar"] = "value 2"
        rows = await orm_class.query.set(fields).eq_pk(pk).update()
        self.assertEqual(1, len(rows))

        o2 = await orm_class.query.eq_pk(pk).one()
        self.assertEqual(2, o2.foo)
        self.assertEqual("value 2", o2.bar)

    async def test_update_bubble_up(self):
        """
        https://github.com/jaymon/prom/issues/11
        """
        orm_class = self.get_orm_class(
            foo=Field(int),
            bar=Field(str),
            _created=None,
            _updated=None
        )

        fields = {
            "foo": 1,
            "bar": None,
        }
        pk = await orm_class.query.set(fields).insert()
        ret = await (orm_class.query
            .set_foo(2)
            .set_bar("bar 2")
            .ne_bar(None)
            .update()
        )
        self.assertEqual(0, len(ret))

        ret = await (orm_class.query
            .set_foo(2)
            .set_bar("bar 2")
            .eq_bar(None)
            .update()
        )
        self.assertEqual(1, len(ret))

    async def test_delete(self):
        orm_class = self.get_orm_class()
        pk = await self.insert(orm_class, 1)

        with self.assertRaises(ValueError):
            r = await orm_class.query.delete()

        r = await orm_class.query.eq_pk(pk).delete()
        self.assertEqual(1, r)

        r = await orm_class.query.eq_pk(pk).delete()
        self.assertEqual(0, r)

    async def test___aiter__(self):
        count = 5
        q = self.get_query()
        await self.insert(q, count)

        rcount = 0
        async for _ in q:
            rcount += 1

        self.assertEqual(count, rcount)

    def test_set(self):
        q = self.get_query()
        field_names = list(q.schema.fields.keys())
        fields = dict(zip(field_names, [None] * len(field_names)))
        q.set(**fields)
        self.assertEqual(fields, {f.name: f.value for f in q.fields_set})

        q = self.get_query()
        q.set(fields)
        self.assertEqual(fields, {f.name: f.value for f in q.fields_set})

    def test_select_1(self):
        q = self.get_query()
        fields_select = list(q.schema.fields.keys())
        q.select(*fields_select[0:-1])
        self.assertEqual(fields_select[0:-1], list(q.fields_select.names()))

        q = self.get_query()
        q.select(fields_select)
        self.assertEqual(fields_select, list(q.fields_select.names()))

        q = self.get_query()
        q.select(fields_select[0:-1], fields_select[-1])
        self.assertEqual(fields_select, list(q.fields_select.names()))

        # make sure chaining works
        q = self.get_query()
        q.select(fields_select[0]).select(*fields_select[1:])
        self.assertEqual(fields_select, list(q.fields_select.names()))

    def test_select_all(self):
        orm_class = self.get_orm_class()
        q = orm_class.query.select("*")
        self.assertRegex(q.render(), r"(?m)SELECT\s+\*\s+FROM")

    def test_schemas(self):
        Foo = self.get_orm_class()
        Bar = self.get_orm_class()

        bar_q = Bar.query.select_foo()
        foo_q = Foo.query.select_pk().in_bar(bar_q)

        schemas = foo_q.schemas
        self.assertEqual(2, len(schemas))
        self.assertEqual(Foo.schema, schemas[0])
        self.assertEqual(String(Bar.schema), String(schemas[1]))

    def test_render(self):
        q = self.get_query()

        q.eq_foo(1)
        q.eq_bar("two")
        r = q.render()
        self.assertRegex(r, r"foo[^=]+=\s*1")
        self.assertRegex(r, r"bar[^=]+=\s*'two'")

    def test_find_methods_1(self):
        q = self.get_query()

        opm, fn = q.find_methods("eq_foo_bar")
        opm2, fn2 = q.find_methods("foo_bar_eq")
        self.assertEqual("eq_field", opm.__name__)
        self.assertEqual(opm2.__name__, opm.__name__)
        self.assertEqual("foo_bar", fn)
        self.assertEqual(fn2, fn)

        with self.assertRaises(AttributeError):
            q.find_methods("baklsdkf_foo_bar")

        with self.assertRaises(AttributeError):
            q.find_methods("baklsdkf_field")

        with self.assertRaises(AttributeError):
            q.find_methods("_field")

        with self.assertRaises(AttributeError):
            q.find_methods("baklsdkf")

    def test_find_methods_2(self):
        q = self.get_query()

        method_name = "eq_{}".format(
            random.choice(list(q.schema.fields.keys()))
        )
        r = q.find_methods(method_name)
        self.assertEqual("eq_field", r[0].__name__)
        self.assertTrue(r[1] in set(q.schema.fields.keys()))

        with self.assertRaises(AttributeError):
            q.find_methods("testing")

        q = self.get_query()
        q.orm_class = None
        tests = [
            ("gt_foo_bar", ("gt_field", "foo_bar")),
        ]

        for t in tests:
            r = q.find_methods(t[0])
            self.assertEqual(t[1][0], r[0].__name__)
            self.assertEqual(t[1][1], r[1])

    async def test_like(self):
        _q = self.get_query()
        await self.insert(_q, 5)
        for bar in ["bar che", "foo bar", "foo bar che"]:
            await self.insert_fields(_q, bar=bar)

        count = await _q.copy().like_bar("bar%").count()
        self.assertEqual(1, count)

        count = await _q.copy().like_bar("%bar").count()
        self.assertEqual(1, count)

        count = await _q.copy().like_bar("%bar%").count()
        self.assertEqual(3, count)

        count = await _q.copy().nlike_bar("bar%").count()
        self.assertEqual(7, count)

        count = await _q.copy().nlike_bar("%bar").count()
        self.assertEqual(7, count)

        count = await _q.copy().nlike_bar("%bar%").count()
        self.assertEqual(5, count)

        count = await _q.copy().like_bar("bar____").count()
        self.assertEqual(1, count)

        count = await _q.copy().like_bar("____bar").count()
        self.assertEqual(1, count)

        count = await _q.copy().like_bar("____bar____").count()
        self.assertEqual(1, count)

    async def test_between(self):
        q = self.get_query()
        await self.insert(q, 5)

        vals = [r async for r in q.select_pk().between_pk(2, 4)]
        self.assertEqual(3, len(vals))
        for v in vals:
            self.assertTrue(v >= 2 and v <= 4)

    async def test_query_ref_1(self):
        inter = self.get_interface()
        modpath = self.create_module([
            "import prom",
            "",
            "class t1(prom.Orm):",
            "    table_name = 'qr2_t1'",
            "    foo=prom.Field(int, True)",
            "    bar=prom.Field(str, True)",
            ""
            "class t2(prom.Orm):",
            "    table_name = 'qr2_t2'",
            "    foo=prom.Field(int, True)",
            "    bar=prom.Field(str, True)",
            "    che=prom.Field(t1, True)",
            ""
        ])


        t1 = modpath.module().t1
        t1.interface = inter

        t2 = modpath.module().t2
        t2.interface = inter

        ti1 = await self.insert_fields(t1, foo=11, bar='11')
        ti12 = await self.insert_fields(t1, foo=12, bar='12')

        ti2 = await self.insert_fields(t2, foo=21, bar='21', che=ti1)
        ti22 = await self.insert_fields(t2, foo=22, bar='22', che=ti12)

        orm_classpath = "{}.{}".format(t2.__module__, t2.__name__)

        q = t1.query.ref(orm_classpath).select_foo().eq_pk(ti12)
        l = [r async for r in q]
        self.assertEqual(22, l[0])
        self.assertEqual(1, len(l))

        q = t1.query.ref(orm_classpath).select_foo().eq_pk(ti1)
        l = [r async for r in q]
        self.assertEqual(21, l[0])
        self.assertEqual(1, len(l))

        q = t1.query.ref(orm_classpath).select_foo().eq_pk(ti1)
        l = [r async for r in q]
        self.assertEqual(21, l[0])
        self.assertEqual(1, len(l))

        q = t1.query.ref(orm_classpath).select_foo().eq_pk(ti1)
        l = [r async for r in q]
        self.assertEqual(21, l[0])
        self.assertEqual(1, len(l))

        q = t1.query.ref(orm_classpath).select_foo()
        l = [r async for r in q]
        self.assertEqual(2, len(l))

    async def test_query_ref_2(self):
        inter = self.get_interface()
        modpath = self.create_module([
            "import prom",
            "",
            "class T1(prom.Orm):",
            "    table_name = 'qre_t1'",
            ""
            "class T2(prom.Orm):",
            "    table_name = 'qre_t2'",
            "    t1_id=prom.Field(T1, True)",
            ""
            "class T3(prom.Orm):",
            "    table_name = 'qre_t3'",
            ""
        ])

        T1 = modpath.module().T1
        T1.interface = inter

        T2 = modpath.module().T2
        T2.interface = inter

        T3 = modpath.module().T3
        T3.interface = inter

        t1a = await self.insert_fields(T1)
        t1b = await self.insert_fields(T1)
        t2 = await self.insert_fields(T2, t1_id=t1a)

        classpath = "{}.{}".format(T2.__module__, T2.__name__)

        r = await T1.query.ref(classpath).eq_pk(t1a).count()
        self.assertEqual(1, r)

        r = await T1.query.ref(classpath).eq_pk(t1b).count()
        self.assertEqual(0, r)

    async def test_field_datetime(self):
        _q = self.get_query()

        q = _q.copy()
        q.eq__created(day=int(datetime.datetime.utcnow().strftime('%d')))
        r = [r async for r in q]
        self.assertFalse(r)

        pk = await self.insert(q, 1)

        # get the object out so we can use it to query
        o = await _q.copy().eq_pk(pk).one()
        dt = o._created
        day = int(dt.strftime('%d'))

        q = _q.copy().in__created(day=day)
        r = [r async for r in q]
        self.assertEqual(1, len(r))

        q = _q.copy().eq__created(day=day)
        r = [r async for r in q]
        self.assertEqual(1, len(r))

        q = _q.copy().in__created(day=[day, day + 1])
        r = [r async for r in q]
        self.assertEqual(1, len(r))

    def test_pk_fields_1(self):
        orm_class = self.get_orm_class()
        q = orm_class.query
        q.gte_pk(5).lte_pk(1).lt_pk(1).gte_pk(5)
        q.desc_pk()
        q.asc_pk()
        q.set_pk(None)

        for where_field in q.fields_where:
            self.assertEqual(where_field.name, "_id")

        for sort_field in q.fields_sort:
            self.assertEqual(sort_field.name, "_id")

        for set_field in q.fields_set:
            self.assertEqual(set_field.name, "_id")

    async def test_pk_fields_2(self):
        orm_class = self.get_orm_class()
        v = await orm_class.query.select_pk().one()
        self.assertEqual(None, v)

        count = 2
        await self.insert(orm_class, count)

        v = await orm_class.query.select_pk().asc_pk().one()
        self.assertEqual(1, v)

    async def test_pk_fields_3(self):
        orm_class = self.get_orm_class()

        v = [r async for r in orm_class.query.select_pk()]
        self.assertEqual(0, len(v))

        count = 2
        await self.insert(orm_class, count)

        v = [r async for r in orm_class.query.select_pk()]
        self.assertEqual(2, len(v))

    async def test_in_field_1(self):
        q = self.get_query().in_foo([])
        self.assertEqual([], [r async for r in q])

        q = self.get_query().in_foo([1, 2])
        self.assertEqual(q.fields_where[0].value, [1, 2])

        q = self.get_query().in_foo([1])
        self.assertEqual(q.fields_where[0].value, [1])

        q = self.get_query().in_foo([1, 2])
        self.assertEqual(q.fields_where[0].value, [1, 2])

        q = self.get_query().in_foo(range(1, 3))
        self.assertEqual(q.fields_where[0].value, [1, 2])

        q = self.get_query().in_foo((x for x in [1, 2]))
        self.assertEqual(q.fields_where[0].value, [1, 2])

    async def test_in_field_empty(self):
        """you can now pass empty lists to in and nin and not have them throw an
        error, instead they return an empty iterator"""
        q = self.get_query()
        await self.insert(q, 1)

        r = [r async for r in q.in_foo([])]
        self.assertFalse(r)
        count = 0
        for x in r:
            count += 0
        self.assertEqual(0, count)
        self.assertEqual(0, len(r))

    def test_child_magic(self):
        orm_class = self.get_orm_class()
        class ChildQuery(Query):
            pass
        orm_class.query_class = ChildQuery

        q = orm_class.query
        q.eq_foo(1) # if there is no error, it passed

        with self.assertRaises(AttributeError):
            q.aksdlfjldks_foo(2)

    def test_properties(self):
        q = self.get_query()
        r = q.schema
        self.assertTrue(r)

        r = q.interface
        self.assertEqual(r, q.orm_class.interface)
        self.assertTrue(r)

        q.orm_class = None
        self.assertFalse(q.schema)
        self.assertFalse(q.interface)

    def test___getattr__(self):
        q = self.get_query().eq_foo(1)
        self.assertEqual(1, len(q.fields_where))
        self.assertEqual("eq", q.fields_where[0].operator)

        with self.assertRaises(AttributeError):
            q.testsfsdfsdft_fieldname(1, 2, 3)

    def test_append_operation(self):
        tests = [
            ("eq_field", ["foo", 1], ["eq", "foo", 1]),
            ("ne_field", ["foo", 1], ["ne", "foo", 1]),
            ("lte_field", ["foo", 1], ["lte", "foo", 1]),
            ("lt_field", ["foo", 1], ["lt", "foo", 1]),
            ("gte_field", ["foo", 1], ["gte", "foo", 1]),
            ("gt_field", ["foo", 1], ["gt", "foo", 1]),
            ("in_field", ["foo", (1, 2, 3)], ["in", "foo", [1, 2, 3]]),
            ("nin_field", ["foo", (1, 2, 3)], ["nin", "foo", [1, 2, 3]]),
        ]

        for i, t in enumerate(tests):
            q = self.get_query()
            cb = getattr(q, t[0])
            r = cb(*t[1])
            self.assertEqual(q, r)
            self.assertEqual(t[2][0], q.fields_where[0].operator)
            self.assertEqual(t[2][1], q.fields_where[0].name)
            self.assertEqual(t[2][2], q.fields_where[0].value)

        q = self.get_query()
        q.between_field("foo", 1, 2)
        self.assertEqual("gte", q.fields_where[0].operator)
        self.assertEqual("lte", q.fields_where[1].operator)

    def test_append_sort(self):
        tests = [
            ("append_sort", [1, "foo"], [1, "foo"]),
            ("append_sort", [-1, "foo"], [-1, "foo"]),
            ("append_sort", [5, "foo"], [1, "foo"]),
            ("append_sort", [-5, "foo"], [-1, "foo"]),
            ("asc_field", ["foo"], [1, "foo"]),
            ("desc_field", ["foo"], [-1, "foo"]),
        ]

        q = self.get_query()
        for i, t in enumerate(tests):
            cb = getattr(q, t[0])
            r = cb(*t[1])
            self.assertEqual(q, r)
            self.assertEqual(t[2][0], q.fields_sort[i].direction)
            self.assertEqual(t[2][1], q.fields_sort[i].name)

        with self.assertRaises(ValueError):
            q.append_sort(0, "foo")

    def test_bounds_methods(self):
        q = self.get_query()
        q.limit(10)
        self.assertEqual((10, 0), q.bounds.get())

        q.page(1)
        self.assertEqual((10, 0), q.bounds.get())

        q.offset(15)
        self.assertEqual((10, 15), q.bounds.get())

        q.page(2)
        self.assertEqual((10, 10), q.bounds.get())

        q.page(3)
        self.assertEqual((10, 20), q.bounds.get())

        q.page(0)
        self.assertEqual((10, 0), q.bounds.get())

        q.offset(0)
        self.assertEqual((10, 0), q.bounds.get())

        q.limit(0)
        self.assertEqual((0, 0), q.bounds.get())

    def test_copy(self):
        q1 = self.get_query()
        q2 = q1.copy()

        q1.eq_foo(1)
        self.assertEqual(1, len(q1.fields_where))
        self.assertEqual(0, len(q2.fields_where))

        self.assertNotEqual(id(q1), id(q2))
        self.assertNotEqual(id(q1.fields_where), id(q2.fields_where))
        self.assertNotEqual(id(q1.bounds), id(q2.bounds))

    def test_or_clause(self):
        q = self.get_query()
        q.eq_foo(1).OR.gte_foo(10).OR.ne_foo(None).eq_bar(1)
        query_str = re.sub(r"\s+", " ", q.render())
        clause = " ".join([
            '(',
            '"foo" = 1',
            'OR "foo" >= 10',
            'OR "foo" IS DISTINCT FROM NULL',
            ')',
            'AND "bar" = 1'
        ])
        self.assertTrue(clause in query_str, query_str)

    async def test_compound_queries(self):
        o1 = self.get_orm_class(v1=Field(int))
        o2 = self.get_orm_class(v2=Field(int))

        for v in [1, 2, 3]:
            await self.insert_fields(o1, v1=v)

        for v in [2, 3, 4]:
            await self.insert_fields(o2, v2=v)

        inter = o1.interface
        schema = o1.schema

        ret = await o1.query.difference(
            o1.query.select_v1(),
            o2.query.select_v2().eq_v2(2)
        ).get()
        self.assertEqual(set([1, 3]), set([r async for r in ret]))

        ret = await o1.query.intersect(
            o1.query.select_v1(),
            o2.query.select_v2()
        ).count()
        self.assertEqual(2, ret)

        ret = await o1.query.intersect(
            o1.query.select_v1(),
            o2.query.select_v2()
        ).get()
        self.assertEqual(set([3, 2]), set([r async for r in ret]))

        ret = await o1.query.union(
            o1.query.select_v1().eq_v1(1),
            o2.query.select_v2().eq_v2(2)
        ).get()
        self.assertEqual(set([1, 2]), set([r async for r in ret]))

        ret = await o1.query.union(
            o1.query.select_v1().eq_v1(1),
            o2.query.select_v2().eq_v2(2)
        ).limit(1).get()
        self.assertEqual(1, len([r async for r in ret]))

    async def test_model_name(self):
        o1_class = self.get_orm_class(
            bar=Field(str),
            model_name="o1qmodel",
        )
        o2_class = self.get_orm_class(
            o1_id=Field(o1_class),
            model_name="o2qmodel",
        )

        o1 = await self.insert_orm(o1_class, bar="1")
        o2 = await self.insert_orm(o2_class, o1_id=o1.pk)

        o2r = await o2_class.query.eq_o1qmodel(o1).one()
        self.assertEqual(o2.pk, o2r.pk)

        o2r = await o2_class.query.eq_field(o1.model_name, o1).one()
        self.assertEqual(o2.pk, o2r.pk)

        o2r = await o2_class.query.eq_field(o1.models_name, o1).one()
        self.assertEqual(o2.pk, o2r.pk)

    def test_raw_field_1(self):
        orm_class = self.get_orm_class()

        q = orm_class.query.raw_field(
            "MAX(foo) = {}".format(orm_class.interface.PLACEHOLDER),
            1
        ).eq_bar("blah blah").render()

        self.assertTrue("MAX(foo) = 1" in q)

    async def test_sort_order(self):
        q = self.get_query()
        await self.insert(q, 10)

        foos = [r async for r in q.copy().select_foo().asc_pk()]
        foos.sort()

        for x in range(2, 9):
            q3 = q.copy().select_foo().asc_foo().limit(1).page(x)
            rows = [r async for r in q3]
            self.assertEqual(foos[x - 1], rows[0])

            row = await q.copy().select_foo().asc_foo().limit(1).page(x).one()
            self.assertEqual(foos[x - 1], row)

            q3 = await q.copy().select_foo().asc_foo().limit(1).page(x).one()
            self.assertEqual(foos[x - 1], row)

            q3 = q.copy().select_foo().in_foo(foos)
            q3.asc_foo(foos).limit(1).page(x)
            rows = [r async for r in q3]
            self.assertEqual(foos[x - 1], rows[0])

            q3 = q.copy().select_foo().in_foo(foos)
            q3.asc_foo(foos).limit(1).page(x)
            row = await q3.one()
            self.assertEqual(foos[x - 1], row)

        for x in range(1, 9):
            q3 = q.copy().select_foo().asc_foo().limit(x).offset(x)
            rows = [r async for r in q3]
            self.assertEqual(foos[x], rows[0])

            q3 = q.copy()
            row = await q.copy().select_foo().asc_foo().limit(x).offset(x).one()
            self.assertEqual(foos[x], row)

            row = await q.copy().select_foo().asc_foo().limit(x).offset(x).one()
            self.assertEqual(foos[x], row)

            q3 = q.copy().select_foo().in_foo(foos)
            q3.asc_foo(foos).limit(1).offset(x)
            rows = [r async for r in q3]
            self.assertEqual(foos[x], rows[0])

            q3 = q.copy().select_foo().in_foo(foos)
            q3.asc_foo(foos).limit(1).offset(x)
            row = await q3.one()
            self.assertEqual(foos[x], row)

    async def test_sort_list(self):
        q = self.get_query()
        await self.insert(q, 10)

        q2 = q.copy()
        foos = [r async for r in q.copy().select_foo()]
        random.shuffle(foos)

        rows = await q.copy().select_foo().in_foo(foos).asc_foo(foos).tolist()
        for i, r in enumerate(rows):
            self.assertEqual(foos[i], r)

        rfoos = list(reversed(foos))
        rows = await q.copy().select_foo().in_foo(foos).desc_foo(foos).tolist()
        for i, r in enumerate(rows):
            self.assertEqual(rfoos[i], r)

        q3 = q.copy()
        rows = await q3.in_foo(foos).asc_foo(foos).limit(2).offset(2).tolist()
        for i, r in enumerate(rows, 2):
            self.assertEqual(foos[i], r.foo)

        # now test a string value
        qb = q.copy()
        bars = await q.copy().select_bar().tolist()
        random.shuffle(bars)

        qb = q.copy()
        rows = await q.copy().in_bar(bars).asc_bar(bars).tolist()
        for i, r in enumerate(rows):
            self.assertEqual(bars[i], r.bar)

        # make sure limits and offsets work
        qb = q.copy()
        rows = await q.copy().in_bar(bars).asc_bar(bars).limit(5).tolist()
        for i, r in enumerate(rows):
            self.assertEqual(bars[i], r.bar)

        qb = q.copy()
        rows = await qb.in_bar(bars).asc_bar(bars).limit(2).offset(2).tolist()
        for i, r in enumerate(rows, 2):
            self.assertEqual(bars[i], r.bar)

        # make sure you can select on one row and sort on another
        vs = await q.copy().select_foo().select_bar().tolist()
        random.shuffle(vs)

        rows = await q.copy().select_foo().asc_bar((v[1] for v in vs)).tolist()
        for i, r in enumerate(rows):
            self.assertEqual(vs[i][0], r)

    async def test_incr_field_name(self):
        """Makes sure field_name = field_name + <N> works as expected

        In SQLite this will work on insert but in Postgres it fails
        """
        q = self.get_query(
            foo=Field(int, default=0),
            _created=None,
            _updated=None
        )
        pks = await self.insert(q, 3)

        foos = {}
        os = await q.copy().in_pk(pks).get()
        async for o in os:
            foos[o.pk] = o.foo

        await q.copy().incr_foo().in_pk(pks).update()
        os = await q.copy().in_pk(pks).get()
        async for o in os:
            self.assertLess(foos[o.pk], o.foo)
            self.assertEqual(foos[o.pk], o.foo - 1)

        # make sure decrement works also
        await q.copy().incr_foo(-1).in_pk(pks).update()
        os = await q.copy().in_pk(pks).get()
        async for o in os:
            self.assertEqual(foos[o.pk], o.foo)

    async def test_incr_field_subquery(self):
        """Makes sure field_name = (SELECT ...) + <N> works as expected"""
        q = self.get_query(
            foo=Field(int),
            _created=None,
            _updated=None
        )

        pk1 = (await q.copy().incr_foo(
            field_val=q.copy().select_foo(function_name="MAX"),
        ).insert())["_id"]

        foo1 = await q.copy().select_foo().eq_pk(pk1).one()
        self.assertEqual(1, foo1)

        await q.copy().incr_foo(
            field_val=q.copy().select_foo(function_name="MAX"),
        ).eq_pk(pk1).update()

        foo2 = await q.copy().select_foo().eq_pk(pk1).one()
        self.assertEqual(2, foo2)

    async def test_set_subquery(self):
        q = self.get_query(
            foo=Field(int),
            _created=None,
            _updated=None
        )

        pk1 = await self.insert(q, 1)

        pk2 = (await q.copy().set_foo(
            q.copy().select("MAX(foo)").eq_pk(pk1)
        ).insert())["_id"]

        foo1 = await q.copy().select_foo().eq_pk(pk1).one()
        foo2 = await q.copy().select_foo().eq_pk(pk2).one()
        self.assertEqual(foo1, foo2)

    async def test_select_sql_method(self):
        q = self.get_query(foo=Field(int))
        pk = await self.insert(q, 1)

        foo1 = await q.copy().select_foo().eq_pk(pk).one()
        foo2 = await q.copy().select("MAX(foo)").one()
        self.assertEqual(foo1, foo2)


class IteratorTest(EnvironTestCase):
    async def get_iterator(self, count=5, limit=5, page=0):
        q = self.get_query()
        await self.insert(q, count)
        i = await q.limit(limit).page(page).get()
        return i

    async def test___repr__(self):
        """https://github.com/Jaymon/prom/issues/137"""
        orm_class = await self.create_orms()
        it = await orm_class.query.get()
        s = it.__repr__()
        self.assertNotEqual("[]", s)
        await it.close()

    async def test___init__(self):
        index = 4
        orm_class = self.get_orm_class()
        pks = await self.insert(orm_class, 10)

        it = await orm_class.query.gt_pk(pks[index]).get()
        self.assertLess(0, await it.count())

        async for o in it:
            self.assertLess(pks[index], o.pk)

    async def test___getitem___slicing(self):
        orm_class = self.get_orm_class()
        pks = await self.insert(orm_class, 10)

        it = await orm_class.query.select_pk().asc_pk().get()

        self.assertEqual(pks[-5:6], await (await it[-5:6]).tolist())
        self.assertEqual(pks[2:5], await (await it[2:5]).tolist())

        self.assertEqual(pks[2:], await (await it[2:]).tolist())
        self.assertEqual(pks[:2], await (await it[:2]).tolist())

        with self.assertRaises(ValueError):
            await it[1:2:2]

        await it.close()

    async def test___getitem___positive_index(self):
        orm_class = self.get_orm_class()
        pks = await self.insert(orm_class, 10)

        it = await orm_class.query.asc_pk().get()

        self.assertEqual(pks[0], (await it[0]).pk)
        self.assertEqual(pks[-1], (await it[len(pks) - 1]).pk)
        with self.assertRaises(IndexError):
            await it[len(pks)]

        it = await orm_class.query.offset(4).limit(2).asc_pk().get()
        self.assertEqual(pks[4], (await it[0]).pk)
        self.assertEqual(pks[5], (await it[1]).pk)
        with self.assertRaises(IndexError):
            await it[3]

        await it.close()

    async def test___getitem___negative_index(self):
        orm_class = self.get_orm_class()
        pks = await self.insert(orm_class, 10)

        it = await orm_class.query.asc_pk().get()
        self.assertEqual(pks[-1], (await it[-1]).pk)
        self.assertEqual(pks[-2], (await it[-2]).pk)
        with self.assertRaises(IndexError):
            await it[-(len(pks) + 5)]

        await it.close()

    async def test_custom(self):
        """make sure setting a custom Iterator class works normally and wrapped
        by an AllIterator()"""
        count = 3
        orm_class = self.get_orm_class()
        await self.insert(orm_class, count)

        class CustomIterator(Iterator):
            def filter(self, o):
                return not o.pk == 1
        orm_class.iterator_class = CustomIterator

        self.assertEqual(count - 1, len(await orm_class.query.tolist()))

    async def test_filter(self):
        q = self.get_query()
        await self.insert(q, 3)

        l = await q.copy().tolist()
        self.assertEqual(3, len(l))

        def ifilter(o): return o.pk == 1

        l = await q.copy().tolist()
        l2 = await q.copy().filter(ifilter).tolist()
        self.assertEqual(len(list(filter(ifilter, l))), len(l2))

    async def test_select_fields(self):
        count = 5
        q = self.get_query()
        await self.insert(q, count)

        g = await q.copy().select_bar().get()
        icount = 0
        async for v in g:
            self.assertTrue(isinstance(v, basestring))
            icount += 1
        self.assertEqual(count, icount)

        g = await q.copy().select_bar().select_foo().get()
        icount = 0
        async for v in g:
            icount += 1
            self.assertTrue(isinstance(v[0], basestring))
            self.assertTrue(isinstance(v[1], int))
        self.assertEqual(count, icount)

    async def test___aiter__(self):
        count = 5
        i = await self.get_iterator(count)

        rcount = 0
        async for t in i:
            rcount += 1
        self.assertEqual(count, rcount)

        with self.assertRaises(ValueError):
            async for t in i:
                pass

    async def test_count(self):
        count = 5
        i = await self.get_iterator(count)
        self.assertEqual(count, await i.count())
        await i.close()

        orm_class = i.orm_class
        i = await orm_class.query.limit(3).get()
        self.assertEqual(3, await i.count())
        await i.close()

    async def test_has_more_1(self):
        limit = 3
        count = 5
        q = self.get_query()
        pks = await self.insert(q, count)

        i = await q.limit(limit).page(0).get()
        self.assertTrue(await i.has_more())
        await i.close()

        i = await q.limit(limit).page(3).get()
        self.assertFalse(await i.has_more())
        await i.close()

        i = await q.limit(limit).page(1).get()
        self.assertTrue(await i.has_more())
        await i.close()

        i = await q.limit(0).page(0).get()
        self.assertFalse(await i.has_more())
        await i.close()

    async def test_has_more_limit(self):
        limit = 4
        count = 10
        q = self.get_query()
        pks = await self.insert(q, count)

        rpks = await q.select_pk().limit(limit).asc_pk().tolist()
        self.assertEqual(pks[:limit], rpks)

