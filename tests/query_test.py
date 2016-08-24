from unittest import TestCase
import datetime
import time
from threading import Thread

import testdata

from . import BaseTestCase
from prom.query import Query, Limit, Iterator, Fields, CacheQuery
import prom


class FieldsTest(TestCase):
    def test_has(self):
        fs = Fields()
        fs.append("foo", ["foo", 1])
        fs.append("foo", ["foo", 2])
        self.assertTrue("foo" in fs)
        self.assertTrue(fs.has("foo"))

        self.assertFalse("bar" in fs)
        self.assertFalse(fs.has("bar"))

    def test_get(self):
        fs = Fields()
        fs.append("foo", ["foo", 1])
        self.assertEqual(1, len(fs.get("foo")))

        fs.append("foo", ["foo", 2])
        self.assertEqual(2, len(fs.get("foo")))

        fs.append("bar", ["bar", "one"])
        self.assertEqual(1, len(fs.get("bar")))
        self.assertEqual(1, len(fs.get("bar")))
        self.assertEqual(2, len(fs.get("foo")))


class LimitTest(TestCase):
    def test_offset_from_page(self):
        lc = Limit()
        lc.page = 2
        self.assertEqual(1, lc.offset)

        lc = Limit()
        lc.limit = 5
        lc.page = 2
        self.assertEqual(5, lc.offset)
        self.assertEqual(5, lc.limit)

    def test_non_paginate_limit(self):
        lc = Limit()

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
        lc = Limit()

        lc.limit = 10
        lc.paginate = True
        self.assertEqual(11, lc.limit)
        self.assertEqual((11, 0), lc.get())

        lc.page = 3
        self.assertEqual((11, 20), lc.get())

        lc.offset = 15
        self.assertEqual((11, 15), lc.get())

        lc.paginate = False
        self.assertEqual((10, 15), lc.get())


class QueryTest(BaseTestCase):
    def test_unique(self):
        orm_class = self.get_orm_class()

        orm_class.create(foo=2, bar="1")
        start = datetime.datetime.utcnow()
        stop = start + datetime.timedelta(seconds=86400)

        for x in range(3):
            orm_class.create(foo=1, bar=str(x))

        for x in range(10, 15):
            orm_class.create(foo=x, bar=str(x))

        base_q = orm_class.query.gte__created(start).lte__created(stop)
        unique_q = base_q.copy().unique_foo()
        foos = unique_q.copy().get().values()
        foo_count = unique_q.copy().count()
        self.assertEqual(foo_count, len(foos))

        foos_all = base_q.copy().select_foo().get().values()
        foo_all_count = base_q.copy().select_foo().count()
        self.assertLess(foo_count, len(foos_all))
        self.assertEqual(foo_all_count, len(foos_all))

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
                "from tqr1 import Foo",
                "",
                "class Bar(prom.Orm):",
                "    table_name = 'thrd_qr2_bar'",
                "    one=prom.Field(int, True)",
                "    foo_id=prom.Field(Foo, True)",
                ""
            ]
        })

        import sys

        tqr1 = basedir.module("rtfoo.rtbar.tqr1")
        sys.modules.pop("rtfoo.rtbar.tqr2.Bar", None)
        #tqr2 = basedir.module("tqr2")
        def target():
            q = tqr1.Foo.ref("rtfoo.rtbar.tqr2.Bar")
            f = tqr1.Foo()
            q = f.query.ref("rtfoo.rtbar.tqr2.Bar")

        t1 = Thread(target=target)
        # if we don't get stuck in a deadlock this test passes
        t1.start()
        t1.join()

    def test_query_ref(self):
        testdata.create_modules({
            "qr2": "\n".join([
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
            ])
        })

        from qr2 import Foo as t1, Bar as t2

        ti1 = t1.create(foo=11, bar='11')
        ti12 = t1.create(foo=12, bar='12')

        ti2 = t2.create(foo=21, bar='21', che=ti1.pk)
        ti22 = t2.create(foo=22, bar='22', che=ti12.pk)

        orm_classpath = "{}.{}".format(t2.__module__, t2.__name__)

        l = list(ti1.query.ref(orm_classpath, ti12.pk).select_foo().values())
        self.assertEqual(22, l[0])

        l = list(ti1.query.ref(orm_classpath, ti1.pk).select_foo().all().values())
        self.assertEqual(21, l[0])

        l = list(ti1.query.ref(orm_classpath, ti1.pk).select_foo().get().values())
        self.assertEqual(21, l[0])

        l = list(ti1.query.ref(orm_classpath, ti1.pk).select_foo().values())
        self.assertEqual(21, l[0])

        l = list(ti1.query.ref(orm_classpath).select_foo().all().values())
        self.assertEqual(2, len(l))

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

        # get the ojbect out so we can use it to query
        o = _q.copy().get_pk(pk)
        dt = o._created
        day = int(dt.strftime('%d'))

        q = _q.copy()
        q.is__created(day=day)
        r = q.get()
        self.assertEqual(1, len(r))


        q = _q.copy()
        q.in__created(day=day)
        r = q.get()
        self.assertEqual(1, len(r))

        q = _q.copy()
        q.in__created(day=[day, day + 1])
        r = q.get()
        self.assertEqual(1, len(r))

    def test_pk_fields(self):
        tclass = self.get_orm_class()
        q = tclass.query.is_pk(1)
        q.in_pk([1, 2, 3])
        q.gte_pk(5).lte_pk(1).lt_pk(1).gte_pk(5)
        q.desc_pk()
        q.asc_pk()
        q.set_pk()

        for where_tuple in q.fields_where:
            self.assertEqual(where_tuple[1], "_id")

        for sort_tuple in q.fields_sort:
            self.assertEqual(sort_tuple[1], "_id")

        for set_tuple in q.fields_set:
            self.assertEqual(set_tuple[0], "_id")

        #self.assertEqual(q.fields_where[0][1], tclass.schema.pk)

    def test_get_pks(self):
        tclass = self.get_orm_class()
        t = tclass()
        t.foo = 1
        t.bar = "bar1"
        t.set()

        t2 = tclass()
        t2.foo = 2
        t2.bar = "bar2"
        t2.set()

        pks = [t.pk, t2.pk]

        res = tclass.query.get_pks(pks)
        self.assertEqual(2, len(res))
        self.assertEqual(list(res.pk), pks)

    def test_value(self):
        _q = self.get_query()

        v = _q.copy().select_foo().value()
        self.assertEqual(None, v)

        count = 2
        pks = self.insert(_q, count)
        o = _q.copy().get_pk(pks[0])

        v = _q.copy().select_foo().value()
        self.assertEqual(o.foo, v)

        v = _q.copy().select_foo().select_bar().value()
        self.assertEqual(o.foo, v[0])
        self.assertEqual(o.bar, v[1])

    def test_values(self):
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

        vals = _q.copy().select_foo().values(limit=1)
        self.assertEqual(1, len(vals))

    def test_pk(self):
        orm_class = self.get_orm_class()
        v = orm_class.query.pk()
        self.assertEqual(None, v)
        count = 2
        self.insert(orm_class, count)

        v = orm_class.query.asc_pk().pk()
        self.assertEqual(1, v)

    def test_pks(self):
        orm_class = self.get_orm_class()
        q = self.get_query()
        v = list(orm_class.query.pks())
        self.assertEqual(0, len(v))
        count = 2
        self.insert(orm_class, count)

        v = list(orm_class.query.pks())
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

        q.set_limit(1)
        rcount = 0
        for r in q.all():
            rcount += 1
        self.assertEqual(count, rcount)

        q.set_limit(6).set_offset(0)
        rcount = 0
        for r in q.all():
            rcount += 1
        self.assertEqual(count, rcount)

    def test_in_field(self):
        q = self.get_query()
        q.in_foo([])
        self.assertFalse(q.can_get)

        q = self.get_query()
        q.in_foo([1, 2])
        self.assertEqual(q.fields_where[0][2], [1, 2,])

        q = self.get_query()
        q.in_foo([1])
        self.assertEqual(q.fields_where[0][2], [1])

        q = self.get_query()
        q.in_foo([1, 2])
        self.assertEqual(q.fields_where[0][2], [1, 2])

        q = self.get_query()
        q.in_foo(xrange(1, 3))
        self.assertEqual(q.fields_where[0][2], [1, 2,])

        q = self.get_query()
        q.in_foo((x for x in [1, 2]))
        self.assertEqual(q.fields_where[0][2], [1, 2,])

    def test_fields_set(self):
        q = self.get_query()
        fields_select = ['foo', 'bar', 'che']
        fields = dict(zip(fields_select, [None] * len(fields_select)))
        q.set_fields(*fields_select)
        self.assertEqual(fields_select, q.fields_select.names())
        self.assertEqual(fields, q.fields)

        q = self.get_query()
        q.set_fields(fields)
        self.assertEqual(fields_select, q.fields_select.names())
        self.assertEqual(fields, q.fields)

        q = self.get_query()
        q.set_fields(fields_select)
        self.assertEqual(fields_select, q.fields_select.names())
        self.assertEqual(fields, q.fields)

        q = self.get_query()
        q.set_fields(**fields)
        self.assertEqual(fields_select, q.fields_select.names())
        self.assertEqual(fields, q.fields)

    def test_fields_select(self):
        q = self.get_query()
        fields_select = ['foo', 'bar', 'che']
        q.select_fields(*fields_select)
        self.assertEqual(fields_select, q.fields_select.names())

        q = self.get_query()
        q.select_fields(fields_select)
        self.assertEqual(fields_select, q.fields_select.names())

        q = self.get_query()
        q.select_fields(fields_select, 'baz')
        self.assertEqual(fields_select + ['baz'], q.fields_select.names())

    def test_child_magic(self):

        orm_class = self.get_orm_class()
        class ChildQuery(Query):
            pass
        orm_class.query_class = ChildQuery

        q = orm_class.query
        q.is_foo(1) # if there is no error, it passed

        with self.assertRaises(AttributeError):
            q.aksdlfjldks_foo(2)

    def test__split_method(self):

        tests = [
            ("get_foo", ("get", "foo")),
            ("is_foo", ("is", "foo")),
            ("gt_foo_bar", ("gt", "foo_bar")),
        ]

        q = self.get_query()
        q.orm_class = None

        for t in tests:
            r = q._split_method(t[0])
            self.assertEqual(t[1], r)

        with self.assertRaises(ValueError):
            q._split_method("testing")

    def test_properties(self):
        q = self.get_query()
        r = q.schema
        self.assertTrue(r)

        r = q.interface
        self.assertTrue(r)

        q.orm_class = None
        self.assertFalse(q.schema)
        self.assertFalse(q.interface)

    def test___getattr__(self):
        q = self.get_query()
        q.is_foo(1)
        self.assertEqual(1, len(q.fields_where))
        self.assertEqual(["is", "foo", 1, {}], q.fields_where[0])

        with self.assertRaises(AttributeError):
            q.testsfsdfsdft_fieldname(1, 2, 3)

    def test_where_field_methods(self):
        tests = [
            ("is_field", ["foo", 1], ["is", "foo", 1, {}]),
            ("not_field", ["foo", 1], ["not", "foo", 1, {}]),
            ("lte_field", ["foo", 1], ["lte", "foo", 1, {}]),
            ("lt_field", ["foo", 1], ["lt", "foo", 1, {}]),
            ("gte_field", ["foo", 1], ["gte", "foo", 1, {}]),
            ("gt_field", ["foo", 1], ["gt", "foo", 1, {}]),
            ("in_field", ["foo", (1, 2, 3)], ["in", "foo", [1, 2, 3], {}]),
            ("nin_field", ["foo", (1, 2, 3)], ["nin", "foo", [1, 2, 3], {}]),
        ]


        q = self.get_query()
        for i, t in enumerate(tests):
            cb = getattr(q, t[0])
            r = cb(*t[1])
            self.assertEqual(q, r)
            self.assertEqual(t[2], q.fields_where[i])

        # ("between_field", ["foo", 1, 2], [["lte", "foo", 1], ["gte", "foo", 2]]),
        q = self.get_query()
        q.between_field("foo", 1, 2)
        self.assertEqual([["lte", "foo", 1, {}], ["gte", "foo", 2, {}]], q.fields_where.fields)

    def test_sort_field_methods(self):
        tests = [
            ("sort_field", ["foo", 1], [1, "foo", None]),
            ("sort_field", ["foo", -1], [-1, "foo", None]),
            ("sort_field", ["foo", 5], [1, "foo", None]),
            ("sort_field", ["foo", -5], [-1, "foo", None]),
            ("asc_field", ["foo"], [1, "foo", None]),
            ("desc_field", ["foo"], [-1, "foo", None]),
        ]

        q = self.get_query()
        for i, t in enumerate(tests):
            cb = getattr(q, t[0])
            r = cb(*t[1])
            self.assertEqual(q, r)
            self.assertEqual(t[2], q.fields_sort[i])

        with self.assertRaises(ValueError):
            q.sort_field("foo", 0)

    def test_bounds_methods(self):
        q = self.get_query()
        q.set_limit(10)
        self.assertEqual((10, 0), q.bounds.get())

        q.set_page(1)
        self.assertEqual((10, 0), q.bounds.get())

        q.set_offset(15)
        self.assertEqual((10, 15), q.bounds.get())

        q.set_page(2)
        self.assertEqual((10, 10), q.bounds.get())

        q.set_page(3)
        self.assertEqual((10, 20), q.bounds.get())

        q.set_page(0)
        self.assertEqual((10, 0), q.bounds.get())

#         q.set_page(-10)
#         self.assertEqual((10, 0), q.limit.get())

        q.set_offset(0)
        self.assertEqual((10, 0), q.bounds.get())

#         q.set_offset(-10)
#         self.assertEqual((10, 0), q.bounds.get())

        q.set_limit(0)
        self.assertEqual((0, 0), q.bounds.get())

#         q.set_limit(-10)
#         self.assertEqual((0, 0), q.limit.get())

    def test_insert_and_update(self):

        IUTorm = self.get_orm_class()
        q = IUTorm.query
        pk = q.copy().set_fields(foo=1, bar="value 1").insert()
        o = q.copy().get_pk(pk)
        self.assertLess(0, pk)
        self.assertTrue(o._created)
        self.assertTrue(o._updated)

        row_count = q.copy().set_fields(foo=2, bar="value 2").is_pk(pk).update()
        self.assertEqual(1, row_count)

        #time.sleep(0.1)
        o2 = q.copy().get_pk(pk)
        self.assertEqual(2, o2.foo)
        self.assertEqual("value 2", o2.bar)
        self.assertEqual(o._created, o2._created)
        self.assertNotEqual(o._updated, o2._updated)

    def test_update_bubble_up(self):
        """
        https://github.com/firstopinion/prom/issues/11
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

    def test_get(self):
        TestGetTorm = self.get_orm_class()
        _ids = self.insert(TestGetTorm, 2)

        q = TestGetTorm.query
        for o in q.get():
            self.assertEqual(type(o), TestGetTorm)
            self.assertTrue(o._id in _ids)
            self.assertFalse(o.is_modified())

    def test_get_one(self):
        TestGetOneTorm = self.get_orm_class()
        _ids = self.insert(TestGetOneTorm, 2)

        q = TestGetOneTorm.query
        o = q.get_one()
        self.assertEqual(type(o), TestGetOneTorm)
        self.assertTrue(o._id in _ids)
        self.assertFalse(o.is_modified())

    def test_first_and_last(self):
        tclass = self.get_orm_class()
        first_pk = self.insert(tclass, 1)[0]

        t = tclass.query.first()
        self.assertEqual(first_pk, t.pk)

        t = tclass.query.last()
        self.assertEqual(first_pk, t.pk)

        last_pk = self.insert(tclass, 1)[0]
        t = tclass.query.first()
        self.assertEqual(first_pk, t.pk)

        t = tclass.query.last()
        self.assertEqual(last_pk, t.pk)

    def test_copy(self):
        q1 = self.get_query()
        q2 = q1.copy()

        q1.is_foo(1)
        self.assertEqual(1, len(q1.fields_where))
        self.assertEqual(0, len(q2.fields_where))

        self.assertNotEqual(id(q1), id(q2))
        self.assertNotEqual(id(q1.fields_where), id(q2.fields_where))
        self.assertNotEqual(id(q1.bounds), id(q2.bounds))


class IteratorTest(BaseTestCase):
    def get_iterator(self, count=5, limit=5, page=0):
        q = self.get_query()
        self.insert(q, count)
        i = q.get(limit, page)
        return i

    def test_custom(self):
        """make sure setting a custom Iterator class works normally and wrapped
        by an AllIterator()"""
        count = 3
        orm_class = self.get_orm_class()
        self.insert(orm_class, count)

        self.assertEqual(count, len(list(orm_class.query.get())))

        class CustomIterator(Iterator):
            def _filtered(self, o):
                return not o.pk == 1
        orm_class.iterator_class = CustomIterator


        self.assertEqual(count - 1, len(list(orm_class.query.get())))
        self.assertEqual(count - 1, len(list(orm_class.query.set_limit(1).all())))

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
        self.assertEqual(len(filter(ifilter, l2)), len(list(l)))

    def test_list_compatibility(self):
        count = 3
        _q = self.get_query()
        self.insert(_q, count)

        q = _q.copy()
        l = q.get()

        self.assertTrue(bool(l))
        self.assertEqual(count, l.count())
        self.assertEqual(range(1, count + 1), list(l.pk))

        l.reverse()
        self.assertEqual(list(reversed(xrange(1, count + 1))), list(l.pk))

        r = l.pop(0)
        self.assertEqual(count, r.pk)

        r = l.pop()
        self.assertEqual(1, r.pk)

        pop_count = 0
        while l:
            pop_count += 1
            l.pop()
        self.assertGreater(pop_count, 0)

    def test_all_len(self):
        count = 10
        q = self.get_query()
        self.insert(q, count)
        g = q.select_foo().desc_bar().set_limit(5).set_offset(1).all()
        self.assertEqual(count, len(g))

    def test_all(self):
        count = 15
        q = self.get_query()
        self.insert(q, count)
        q.set_limit(5)
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

    def test_values(self):
        count = 5
        _q = self.get_query()
        self.insert(_q, count)

        g = _q.copy().select_bar().get().values()
        icount = 0
        for v in g:
            self.assertTrue(isinstance(v, basestring))
            icount += 1
        self.assertEqual(count, icount)

        g = _q.copy().select_bar().select_foo().get().values()
        icount = 0
        for v in g:
            icount += 1
            self.assertTrue(isinstance(v[0], basestring))
            self.assertTrue(isinstance(v[1], int))
        self.assertEqual(count, icount)

        i = _q.copy().get()
        with self.assertRaises(ValueError):
            g = i.values()

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

    def test___getitem__(self):
        count = 5
        i = self.get_iterator(count)
        for x in range(count):
            self.assertEqual(i[x].pk, i.results[x]['_id'])

        with self.assertRaises(IndexError):
            i[count + 1]

    def test___len__(self):
        count = 5
        i = self.get_iterator(count)
        self.assertEqual(len(i), count)

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

    def test_has_more(self):
        limit = 3
        count = 5
        q = self.get_query()
        self.insert(q.orm, count)

        i = q.get(limit, 0)
        self.assertTrue(i.has_more)

        i = q.get(limit, 2)
        self.assertFalse(i.has_more)

        i = q.get(limit, 1)
        self.assertTrue(i.has_more)

        i = q.get(0, 0)
        self.assertFalse(i.has_more)


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
            self.assertLess(duration, ref_duration)
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

        #pout.v(orm_class.query.cache_namespace)
        #pout.v(orm_class.query.cache_namespace)

