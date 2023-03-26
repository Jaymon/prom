# -*- coding: utf-8 -*-
from __future__ import unicode_literals, division, print_function, absolute_import
import datetime
import time
from threading import Thread
import sys
import re

import testdata

from . import BaseTestCase, EnvironTestCase, TestCase, SkipTest
from prom.query import (
    Query,
    Bounds,
    Field,
    Fields,
    Iterator,
)
from prom.config import Field as OrmField
from prom.compat import *
import prom


class FieldTest(BaseTestCase):
    def test___new__(self):
        q = self.get_query()
        f = Field(q, "MAX(foo)")
        #f = Field("MAX(foo)", schema=testdata.mock(field_name="foo"))
        self.assertEqual("foo", f.name)
        self.assertEqual("MAX", f.function_name)


class FieldsTest(BaseTestCase):
    def test_fields(self):
        q = self.get_query()
        fs = Fields()
        fs.append(Field(q, "foo", 1))
        fs.append(Field(q, "foo", 2))
        fs.append(Field(q, "bar", 3))
        #fs.append(Field(q, "che", 4))

        fields = fs.fields
        self.assertEqual(2, fields["foo"])
        self.assertEqual(3, fields["bar"])
        #self.assertEqual(4, fields["che"])

    def test___bool__(self):
        fs = Fields()
        self.assertFalse(fs)

        q = self.get_query()
        fs.append(Field(q, "foo", 1))
        self.assertTrue(fs)

    def test_names(self):
        q = self.get_query()
        fs = Fields()

        fs.append(Field(q, "foo", None))
        fs.append(Field(q, "bar", None))
        fs.append(Field(q, "foo", None))
        self.assertEqual(["foo", "bar"], list(fs.names()))


class BoundsTest(TestCase):
    def test_find_more_index(self):
        b = Bounds()
        b.limit = 3

        b.page = 0
        index = b.find_more_index()
        self.assertEqual(3, index)

        b.page = 2
        index = b.find_more_index()
        self.assertEqual(6, index)

    def test___nonzero__(self):
        b = Bounds()

        self.assertFalse(b)

    def test_offset_from_page(self):
        lc = Bounds()
        lc.page = 2
        self.assertEqual(1, lc.offset)

        lc = Bounds()
        lc.limit = 5
        lc.page = 2
        self.assertEqual(5, lc.offset)
        self.assertEqual(5, lc.limit)

    def test_non_paginate_limit(self):
        lc = Bounds()

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
        lc = Bounds()

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
    def test_query_syntactic_sugar(self):
        Foo = self.get_orm_class()
        self.insert(Foo, 5)

        pk = Foo.query.select_pk().value_pk(3)
        self.assertEqual(3, pk)

        pkl = list(Foo.query.select_pk().values_pk([2]))
        self.assertEqual(2, pkl[0])

        o = Foo.query.one_pk(1)
        self.assertEqual(1, o.pk)

        ol = list(Foo.query.get_pk([1]))
        self.assertEqual(1, ol[0].pk)

    def test_select_all(self):
        Foo = self.get_orm_class()
        q = Foo.query.select("*")
        self.assertRegex(q.render(), r"(?m)SELECT\s+\*\s+FROM")

    def test_schemas(self):
        Foo = self.get_orm_class()
        Bar = self.get_orm_class()

        bar_q = Bar.query.select_foo()
        foo_q = Foo.query.select_pk().in_foo(bar_q)

        schemas = foo_q.schemas
        self.assertEqual(2, len(schemas))
        self.assertEqual(Foo.schema, schemas[0])
        self.assertEqual(String(Bar.schema), String(schemas[1]))

    def test_render(self):
        q = self.get_query()

        q.is_foo(1)
        q.is_bar("two")
        r = q.render()
        self.assertRegex(r, r"foo[^=]+=\s*1")
        self.assertRegex(r, r"bar[^=]+=\s*'two'")

    def test_find_methods_1(self):
        q = self.get_query()

        opm, qm, fn = q.find_methods("eq_foo_bar")
        opm2, qm2, fn2 = q.find_methods("foo_bar_eq")
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

        method_name = "is_{}".format(testdata.random.choice(list(q.schema.fields.keys())))
        r = q.find_methods(method_name)
        self.assertEqual("is_field", r[0].__name__)
        self.assertTrue(r[2] in set(q.schema.fields.keys()))

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
            self.assertEqual(t[1][1], r[2])

    def test_find_methods_3(self):
        q = Query()
        om, qm, fn = q.find_methods("one_pk")
        self.assertEqual(q.eq_field, om)
        self.assertEqual(q.one, qm)
        self.assertEqual("pk", fn)

        with self.assertRaises(AttributeError):
            q.find_methods("foo_pk")

    def test_like(self):
        _q = self.get_query()
        self.insert(_q, 5)
        for bar in ["bar che", "foo bar", "foo bar che"]:
            self.insert_fields(_q, bar=bar)

        count = _q.copy().like_bar("bar%").count()
        self.assertEqual(1, count)

        count = _q.copy().like_bar("%bar").count()
        self.assertEqual(1, count)

        count = _q.copy().like_bar("%bar%").count()
        self.assertEqual(3, count)

        count = _q.copy().nlike_bar("bar%").count()
        self.assertEqual(7, count)

        count = _q.copy().nlike_bar("%bar").count()
        self.assertEqual(7, count)

        count = _q.copy().nlike_bar("%bar%").count()
        self.assertEqual(5, count)

        count = _q.copy().like_bar("bar____").count()
        self.assertEqual(1, count)

        count = _q.copy().like_bar("____bar").count()
        self.assertEqual(1, count)

        count = _q.copy().like_bar("____bar____").count()
        self.assertEqual(1, count)

    def test_between(self):
        _q = self.get_query()
        self.insert(_q, 5)

        q = _q.copy()
        vals = list(q.select_pk().between_pk(2, 4))
        self.assertEqual(3, len(vals))
        for v in vals:
            self.assertTrue(v >= 2 and v <= 4)

    def test_ref_threading(self):
        basedir = testdata.create_modules({
            "rtfoo.rtbar.tqr1": [
                "import prom",
                "",
                "class Foo(prom.Orm):",
                "    table_name = 'thrd_qr2_foo'",
                "    one=prom.Field(int, True)",
                "",
            ],
            "rtfoo.rtbar.tqr2": [
                "import prom",
                "from rtfoo.rtbar.tqr1 import Foo",
                "",
                "class Bar(prom.Orm):",
                "    table_name = 'thrd_qr2_bar'",
                "    one=prom.Field(int, True)",
                "    foo_id=prom.Field(Foo, True)",
                ""
            ],
        })

        tqr1 = basedir.module("rtfoo.rtbar.tqr1")
        sys.modules.pop("rtfoo.rtbar.tqr2.Bar", None)
        #tqr2 = basedir.module("tqr2")
        def target():
            q = tqr1.Foo.query.ref("rtfoo.rtbar.tqr2.Bar")
            f = tqr1.Foo()
            q = f.query.ref("rtfoo.rtbar.tqr2.Bar")

        t1 = Thread(target=target)
        # if we don't get stuck in a deadlock this test passes
        t1.start()
        t1.join()

    def test_query_ref_1(self):
        inter = self.get_interface()
        testdata.create_modules({
            "qr2": [
                "import prom",
                "",
                "class Foo(prom.Orm):",
                "    table_name = 'qr2_foo'",
                "    foo=prom.Field(int, True)",
                "    bar=prom.Field(str, True)",
                ""
                "class Bar(prom.Orm):",
                "    table_name = 'qr2_bar'",
                "    foo=prom.Field(int, True)",
                "    bar=prom.Field(str, True)",
                "    che=prom.Field(Foo, True)",
                ""
            ]
        })
        from qr2 import Foo as t1, Bar as t2
        t1.interface = inter
        t2.interface = inter

        ti1 = t1.create(foo=11, bar='11')
        ti12 = t1.create(foo=12, bar='12')

        ti2 = t2.create(foo=21, bar='21', che=ti1.pk)
        ti22 = t2.create(foo=22, bar='22', che=ti12.pk)

        orm_classpath = "{}.{}".format(t2.__module__, t2.__name__)

        l = list(ti1.query.ref(orm_classpath).select_foo().is_pk(ti12.pk).get())
        self.assertEqual(22, l[0])
        self.assertEqual(1, len(l))

        l = list(ti1.query.ref(orm_classpath).select_foo().is_pk(ti1.pk).get())
        self.assertEqual(21, l[0])
        self.assertEqual(1, len(l))

        l = list(ti1.query.ref(orm_classpath).select_foo().is_pk(ti1.pk).get())
        self.assertEqual(21, l[0])
        self.assertEqual(1, len(l))

        l = list(ti1.query.ref(orm_classpath).select_foo().is_pk(ti1.pk).get())
        self.assertEqual(21, l[0])
        self.assertEqual(1, len(l))

        l = list(ti1.query.ref(orm_classpath).select_foo().get())
        self.assertEqual(2, len(l))

    def test_query_ref_2(self):
        inter = self.get_interface()
        testdata.create_modules({
            "qre": "\n".join([
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
        })
        from qre import T1, T2, T3
        T1.interface = inter
        T2.interface = inter
        T3.interface = inter

        t1a = T1.create()
        t1b = T1.create()
        t2 = T2.create(t1_id=t1a.pk)

        classpath = "{}.{}".format(T2.__module__, T2.__name__)

        r = T1.query.ref(classpath).is_pk(t1a.pk).count()
        self.assertEqual(1, r)

        r = T1.query.ref(classpath).is_pk(t1b.pk).count()
        self.assertEqual(0, r)

    def test_null_iterator(self):
        """you can now pass empty lists to in and nin and not have them throw an
        error, instead they return an empty iterator"""
        _q = self.get_query()
        self.insert(_q, 1)

        q = _q.copy()
        r = q.in_foo([]).get()
        self.assertFalse(r)
        count = 0
        for x in r:
            count += 0
        self.assertEqual(0, count)
        self.assertEqual(0, len(r))

    def test_field_datetime(self):
        _q = self.get_query()

        q = _q.copy()
        q.is__created(day=int(datetime.datetime.utcnow().strftime('%d')))
        r = q.get()
        self.assertFalse(r)

        pk = self.insert(q, 1)[0]

        # get the object out so we can use it to query
        o = _q.copy().one_pk(pk)
        dt = o._created
        day = int(dt.strftime('%d'))

        q = _q.copy()
        q.in__created(day=day)
        r = q.get()
        self.assertEqual(1, len(r))

        q = _q.copy()
        q.is__created(day=day)
        r = q.get()
        self.assertEqual(1, len(r))

        q = _q.copy()
        q.in__created(day=[day, day + 1])
        r = q.get()
        self.assertEqual(1, len(r))

    def test_pk_fields(self):
        tclass = self.get_orm_class()
        q = tclass.query
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

    def test_get_pk(self):
        tclass = self.get_orm_class()
        t = tclass()
        t.foo = 1
        t.bar = "bar1"
        t.save()

        t2 = tclass()
        t2.foo = 2
        t2.bar = "bar2"
        t2.save()

        pks = [t.pk, t2.pk]

        res = tclass.query.get_pk(pks)
        self.assertEqual(2, len(res))
        self.assertEqual(list(res.pk), pks)

    def test_value_query(self):
        _q = self.get_query()

        v = _q.copy().select_foo().value()
        self.assertEqual(None, v)

        count = 2
        pks = self.insert(_q, count)
        o = _q.copy().one_pk(pks[0])

        v = _q.copy().select_foo().is_pk(o.pk).value()
        self.assertEqual(o.foo, v)

        v = _q.copy().select_foo().select_bar().is_pk(o.pk).value()
        self.assertEqual(o.foo, v[0])
        self.assertEqual(o.bar, v[1])

    def test_pk(self):
        orm_class = self.get_orm_class()
        v = orm_class.query.select_pk().one()
        self.assertEqual(None, v)
        count = 2
        self.insert(orm_class, count)

        v = orm_class.query.select_pk().asc_pk().one()
        self.assertEqual(1, v)

    def test_pks(self):
        orm_class = self.get_orm_class()
        q = self.get_query()
        v = list(orm_class.query.select_pk().get())
        self.assertEqual(0, len(v))
        count = 2
        self.insert(orm_class, count)

        v = list(orm_class.query.select_pk().get())
        self.assertEqual(2, len(v))

    def test___iter__(self):
        count = 5
        q = self.get_query()
        self.insert(q, count)

        rcount = 0
        for t in q:
            rcount += 1

        self.assertEqual(count, rcount)

    def test_has(self):
        q = self.get_query()
        self.assertFalse(q.has())

        count = 1
        self.insert(q, count)
        self.assertTrue(q.has())

    def test_all(self):
        count = 10
        q = self.get_query()
        self.insert(q, count)

        # if no limit is set then it should go through all results
        rcount = 0
        for r in q.copy().all():
            rcount += 1
        self.assertEqual(count, rcount)

        # if there is a limit then all should only go until that limit
        rcount = 0
        for r in q.copy().limit(1).all():
            rcount += 1
        self.assertEqual(1, rcount)

        # only go until the end of the results
        rcount = 0
        for r in q.copy().limit(6).offset(6).all():
            rcount += 1
        self.assertEqual(4, rcount)

    def test_in_field(self):
        q = self.get_query()
        q.in_foo([])
        self.assertEqual([], list(q.get()))

        q = self.get_query()
        q.in_foo([1, 2])
        self.assertEqual(q.fields_where[0].value, [1, 2,])

        q = self.get_query()
        q.in_foo([1])
        self.assertEqual(q.fields_where[0].value, [1])

        q = self.get_query()
        q.in_foo([1, 2])
        self.assertEqual(q.fields_where[0].value, [1, 2])

        q = self.get_query()
        q.in_foo(range(1, 3))
        self.assertEqual(q.fields_where[0].value, [1, 2,])

        q = self.get_query()
        q.in_foo((x for x in [1, 2]))
        self.assertEqual(q.fields_where[0].value, [1, 2,])

    def test_set(self):
        q = self.get_query()
        field_names = list(q.schema.fields.keys())
        fields = dict(zip(field_names, [None] * len(field_names)))
        q.set(**fields)
        self.assertEqual(fields, {f.name: f.value for f in q.fields_set})

        q = self.get_query()
        q.set(fields)
        self.assertEqual(fields, {f.name: f.value for f in q.fields_set})

    def test_select(self):
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

    def test_child_magic(self):

        orm_class = self.get_orm_class()
        class ChildQuery(Query):
            pass
        orm_class.query_class = ChildQuery

        q = orm_class.query
        q.is_foo(1) # if there is no error, it passed

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
        q = self.get_query()
        q.is_foo(1)
        self.assertEqual(1, len(q.fields_where))
        self.assertEqual("eq", q.fields_where[0].operator)

        with self.assertRaises(AttributeError):
            q.testsfsdfsdft_fieldname(1, 2, 3)

    def test_append_operation(self):
        tests = [
            ("is_field", ["foo", 1], ["eq", "foo", 1]),
            ("not_field", ["foo", 1], ["ne", "foo", 1]),
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

    def test_insert_and_update(self):
        orm_class = self.get_orm_class()
        q = orm_class.query
        o = orm_class(foo=1, bar="value 1")
        fields = o.to_interface()
        pk = q.copy().set(fields).insert()
        o = q.copy().one_pk(pk)
        self.assertLess(0, pk)
        self.assertTrue(o._created)
        self.assertTrue(o._updated)

        fields["foo"] = 2
        fields["bar"] = "value 2"
        row_count = q.copy().set(fields).is_pk(pk).update()
        self.assertEqual(1, row_count)

        o2 = q.copy().one_pk(pk)

        self.assertEqual(2, o2.foo)
        self.assertEqual("value 2", o2.bar)
        self.assertEqual(o._created, o2._created)
        self.assertEqual(o._updated, o2._updated)

    def test_update_bubble_up(self):
        """
        https://github.com/jaymon/prom/issues/11
        """
        orm = self.get_orm()
        orm.schema.set_field("che", prom.Field(str, False))
        orm.foo = 1
        orm.bar = "bar 1"
        orm.che = None
        orm.save()

        ret = orm.query.set_foo(2).set_bar("bar 2").not_che(None).update()
        self.assertEqual(0, ret)

        ret = orm.query.set_foo(2).set_bar("bar 2").is_che(None).update()
        self.assertEqual(1, ret)

    def test_delete(self):
        tclass = self.get_orm_class()
        first_pk = self.insert(tclass, 1)[0]

        with self.assertRaises(ValueError):
            r = tclass.query.delete()

        r = tclass.query.is_pk(first_pk).delete()
        self.assertEqual(1, r)

        r = tclass.query.is_pk(first_pk).delete()
        self.assertEqual(0, r)

    def test_get_1(self):
        TestGetTorm = self.get_orm_class()
        _ids = self.insert(TestGetTorm, 2)

        q = TestGetTorm.query
        for o in q.get():
            self.assertEqual(type(o), TestGetTorm)
            self.assertTrue(o._id in _ids)
            self.assertFalse(o.is_modified())

    def test_one(self):
        TestGetOneTorm = self.get_orm_class()
        _ids = self.insert(TestGetOneTorm, 2)

        q = TestGetOneTorm.query
        o = q.one()
        self.assertEqual(type(o), TestGetOneTorm)
        self.assertTrue(o._id in _ids)
        self.assertFalse(o.is_modified())

    def test_copy(self):
        q1 = self.get_query()
        q2 = q1.copy()

        q1.is_foo(1)
        self.assertEqual(1, len(q1.fields_where))
        self.assertEqual(0, len(q2.fields_where))

        self.assertNotEqual(id(q1), id(q2))
        self.assertNotEqual(id(q1.fields_where), id(q2.fields_where))
        self.assertNotEqual(id(q1.bounds), id(q2.bounds))

    def test_values_query(self):
        _q = self.get_query()

        count = 2
        pks = self.insert(_q, count)

        vals = _q.copy().select_foo().values()
        self.assertEqual(count, len(vals))
        for v in vals:
            self.assertTrue(isinstance(v, int))

        vals = _q.copy().select_foo().select_bar().values()
        self.assertEqual(count, len(vals))
        for v in vals:
            self.assertTrue(isinstance(v, list))

        vals = _q.copy().select_foo().limit(1).values()
        self.assertEqual(1, len(vals))

    def test_count(self):
        orm_class = self.get_orm_class()
        self.insert(orm_class, 10)

        self.assertEqual(5, orm_class.query.offset(5).count())
        self.assertEqual(5, orm_class.query.limit(5).count())
        self.assertEqual(10, orm_class.query.count())

    def test_or_clause(self):
        q = self.get_query()
        q.eq_foo(1).OR.gte_foo(10).OR.ne_foo(None).eq_bar(1)
        query_str = re.sub(r"\s+", " ", q.render())
        self.assertTrue(
            '( "foo" = 1 OR "foo" >= 10 OR "foo" IS NOT NULL ) AND "bar" = 1' in query_str
        )

    def test_compound_queries(self):
        o1 = self.get_orm_class(v1=OrmField(int))
        o2 = self.get_orm_class(v2=OrmField(int))

        for v in [1, 2, 3]:
            o1.create(v1=v)

        for v in [2, 3, 4]:
            o2.create(v2=v)

        inter = o1.interface
        schema = o1.schema

        ret = o1.query.difference(
            o1.query.select_v1(),
            o2.query.select_v2().eq_v2(2)
        ).all()
        self.assertEqual(set([1, 3]), set(ret))

        ret = o1.query.intersect(
            o1.query.select_v1(),
            o2.query.select_v2()
        ).count()
        self.assertEqual(2, ret)

        ret = o1.query.intersect(
            o1.query.select_v1(),
            o2.query.select_v2()
        ).all()
        self.assertEqual(set([3, 2]), set(ret))

        ret = o1.query.union(
            o1.query.select_v1().eq_v1(1),
            o2.query.select_v2().eq_v2(2)
        ).all()
        self.assertEqual(set([1, 2]), set(ret))

        ret = o1.query.union(
            o1.query.select_v1().eq_v1(1),
            o2.query.select_v2().eq_v2(2)
        ).limit(1).all()
        self.assertEqual(1, len(ret))


class IteratorTest(EnvironTestCase):
    def get_iterator(self, count=5, limit=5, page=0):
        q = self.get_query()
        self.insert(q, count)
        i = q.limit(limit).page(page).get()
        return i

    def test___repr__(self):
        """https://github.com/Jaymon/prom/issues/137"""
        orm_class = self.create_orms()

        it = orm_class.query.get()
        s = it.__repr__()
        self.assertNotEqual("[]", s)

    def test___init__(self):
        count = 10
        orm_class = self.get_orm_class()
        self.insert(orm_class, count)

        q = orm_class.query.gt_pk(5)

        it = Iterator(q)
        self.assertLess(0, len(it))

        for o in it:
            self.assertLess(5, o.pk)

    def test___getitem___slicing(self):
        count = 10
        orm_class = self.get_orm_class()
        pks = self.insert(orm_class, count)

        it = orm_class.query.select_pk().asc_pk().get()

        self.assertEqual(pks[-5:6], list(it[-5:6]))
        self.assertEqual(pks[2:5], list(it[2:5]))

        self.assertEqual(pks[2:], list(it[2:]))
        self.assertEqual(pks[:2], list(it[:2]))

        with self.assertRaises(ValueError):
            it[1:2:2]

    def test___getitem___positive_index(self):
        count = 10
        orm_class = self.get_orm_class()
        orm_class.install()
        pks = self.insert(orm_class, count)

        q = orm_class.query.asc_pk()
        it = Iterator(q)

        self.assertEqual(pks[0], it[0].pk)
        self.assertEqual(pks[-1], it[len(pks) - 1].pk)
        with self.assertRaises(IndexError):
            it[len(pks)]

        q = orm_class.query.offset(4).limit(2).asc_pk()
        it = Iterator(q)
        self.assertEqual(pks[4], it[0].pk)
        self.assertEqual(pks[5], it[1].pk)
        with self.assertRaises(IndexError):
            it[3]

    def test___getitem___negative_index(self):
        count = 10
        orm_class = self.get_orm_class()
        pks = self.insert(orm_class, count)

        q = orm_class.query.asc_pk()
        it = Iterator(q)

        self.assertEqual(it[-1].pk, pks[-1])
        self.assertEqual(it[-2].pk, pks[-2])
        with self.assertRaises(IndexError):
            it[-(len(pks) + 5)]

    def test_copy(self):
        count = 10
        orm_class = self.get_orm_class()
        self.insert(orm_class, count)

        q = orm_class.query.asc_pk()
        it1 = Iterator(q)
        it2 = it1.copy()
        it2.reverse()
        self.assertNotEqual(list(v for v in it1), list(v for v in it2))

    def test_custom(self):
        """make sure setting a custom Iterator class works normally and wrapped
        by an AllIterator()"""
        count = 3
        orm_class = self.get_orm_class()
        self.insert(orm_class, count)

        self.assertEqual(count, len(list(orm_class.query.get())))

        class CustomIterator(Iterator):
            def ifilter(self, o):
                return not o.pk == 1
        orm_class.iterator_class = CustomIterator

        self.assertEqual(count - 1, len(list(orm_class.query.get())))
        self.assertEqual(count - 1, len(list(orm_class.query.all())))

    def test_ifilter(self):
        count = 3
        _q = self.get_query()
        self.insert(_q, count)

        l = _q.copy().get()
        self.assertEqual(3, len(list(l)))

        l = _q.copy().get()
        def ifilter(o): return o.pk == 1
        l.ifilter = ifilter
        l2 = _q.copy().get()
        self.assertEqual(len(list(filter(ifilter, l2))), len(list(l)))

    def test_reverse(self):
        """Iterator.reverse() reverses the iterator in place"""
        count = 10
        orm_class = self.get_orm_class()
        pks = self.insert(orm_class, count)
        pks.reverse()

        q = orm_class.query.asc_pk()
        it = Iterator(q)
        it.reverse()
        for i, o in enumerate(it):
            self.assertEqual(pks[i], o.pk)

        q = orm_class.query.asc_pk()
        it = Iterator(q)
        for i, o in enumerate(reversed(it)):
            self.assertEqual(pks[i], o.pk)

    def test_all_1(self):
        count = 15
        q = self.get_query()
        pks = self.insert(q, count)
        self.assertLess(0, len(pks))
        g = q.all()

        self.assertEqual(1, g[0].pk)
        self.assertEqual(2, g[1].pk)
        self.assertEqual(3, g[2].pk)
        self.assertEqual(6, g[5].pk)
        self.assertEqual(13, g[12].pk)

        with self.assertRaises(IndexError):
            g[count + 5]

        for i, x in enumerate(g):
            if i > 7: break
        self.assertEqual(9, g[8].pk)

        gcount = 0
        for x in g: gcount += 1
        self.assertEqual(count, gcount)

        gcount = 0
        for x in g: gcount += 1
        self.assertEqual(count, gcount)

        self.assertEqual(count, len(g))

        g = q.all()
        self.assertEqual(count, len(g))

    def test_all_limit(self):
        count = 15
        q = self.get_query()
        self.insert(q, count)
        q.limit(5)
        g = q.all()

        self.assertEqual(3, g[2].pk)
        with self.assertRaises(IndexError):
            g[6]

    def test_values(self):
        count = 5
        _q = self.get_query()
        self.insert(_q, count)

        g = _q.copy().select_bar().get()
        icount = 0
        for v in g:
            self.assertTrue(isinstance(v, basestring))
            icount += 1
        self.assertEqual(count, icount)

        g = _q.copy().select_bar().select_foo().get()
        icount = 0
        for v in g:
            icount += 1
            self.assertTrue(isinstance(v[0], basestring))
            self.assertTrue(isinstance(v[1], int))
        self.assertEqual(count, icount)

    def test___iter__(self):
        count = 5
        i = self.get_iterator(count)

        rcount = 0
        for t in i:
            rcount += 1
        self.assertEqual(count, rcount)

        rcount = 0
        for t in i:
            self.assertTrue(isinstance(t, prom.Orm))
            rcount += 1
        self.assertEqual(count, rcount)

    def test___len__(self):
        count = 5
        i = self.get_iterator(count)
        self.assertEqual(len(i), count)

        orm_class = i.orm_class

        i = orm_class.query.limit(3).get()
        self.assertEqual(3, len(i))

    def test___getattr__(self):
        count = 5
        i = self.get_iterator(count)
        rs = list(i.foo)
        self.assertEqual(count, len(rs))

        with self.assertRaises(AttributeError):
            i.kadfjkadfjkhjkgfkjfkjk_bogus_field

    def test_pk(self):
        count = 5
        i = self.get_iterator(count)
        rs = list(i.pk)
        self.assertEqual(count, len(rs))

    def test_has_more_1(self):
        limit = 3
        count = 5
        q = self.get_query()
        pks = self.insert(q.orm_class, count)
        self.assertEqual(count, len(pks))

        i = q.limit(limit).page(0).get()

        self.assertTrue(i.has_more())

        i = q.limit(limit).page(3).get()
        self.assertFalse(i.has_more())

        i = q.limit(limit).page(1).get()
        self.assertTrue(i.has_more())

        i = q.limit(0).page(0).get()
        self.assertFalse(i.has_more())

    def test_has_more_limit(self):
        limit = 4
        count = 10
        q = self.get_query()
        pks = self.insert(q, count)

        it = q.select_pk().limit(limit).asc_pk().get()
        self.assertEqual(pks[:limit], list(it))

