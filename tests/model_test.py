# -*- coding: utf-8 -*-
from __future__ import unicode_literals, division, print_function, absolute_import
import pickle
import json
import datetime

import testdata

from . import BaseTestCase, EnvironTestCase
from prom.compat import *
from prom.model import Orm, OrmPool
from prom.config import Field, Index, ObjectField, JsonField
import prom


class PickleOrm(Orm):
    """This is only needed to test the test_pickling() method"""
    foo = Field(int, True)
    bar = Field(str, True)
    ifoobar = Index("foo", "bar")


class OrmPoolTest(BaseTestCase):
    def test_lifecycle(self):
        orm_class = self.get_orm_class()
        pks = self.insert(orm_class, 10)

        pool = OrmPool(orm_class, 1)

        o = pool[pks[0]]
        self.assertEqual(pks[0], o.pk)
        o = pool[pks[0]]
        self.assertEqual(pks[0], o.pk)
        o = pool[pks[0]]
        self.assertEqual(pks[0], o.pk)

        pool[pks[1]]
        self.assertEqual([2], list(pool.pq.keys()))

        pool[pks[0]]
        self.assertEqual([1], list(pool.pq.keys()))

        pool[pks[1]]
        self.assertEqual([2], list(pool.pq.keys()))

        pool[pks[0]]
        self.assertEqual([1], list(pool.pq.keys()))

        pool = OrmPool(orm_class, len(pks) - 1)
        for pk in pks:
            o = pool[pk]
            self.assertEqual(pk, o.pk)

        self.assertEqual(list(pool.pq.keys())[0], pks[1])


class OrmTest(EnvironTestCase):
#     def test_alt_fieldtypes(self):
#         class FTOrm(Orm):
#             table_name = "ftorm_table"
#             one = Field(dict)
#             two = Field(list)
#             #three = Field(json)
#             #four = Field(object)
# 
#         o = FTOrm()
#         o.one = {"foo": 1, "bar": 2}
#         o.two = [1, 2, 3, 4]
#         o.save()
#         pout.v(o.fields)
# 
#         o2 = o.query.get_pk(o.pk)
#         pout.v(o2.fields)

    def test_hydrate_1(self):
        """make sure you can add/update and change the primary key and all of that
        works as expected"""
        orm_class = self.get_orm_class()

        o = orm_class(foo=1, bar="1")

        o.save()
        self.assertLess(0, o.pk)

        with self.assertRaises(orm_class.interface.UniqueError):
            o2 = orm_class(_id=o.pk, foo=2, bar="2")
            o2.save()

        o3 = o.query.get_pk(o.pk)
        self.assertTrue(o3.is_hydrated())

        o3._id = o.pk + 1
        self.assertNotEqual(o.pk, o3.pk)

        o3.save()
        o4 = o3.query.get_pk(o3.pk)
        self.assertEqual(o3.pk, o4.pk)
        self.assertNotEqual(o.pk, o3.pk)

    def test_hydrate_2(self):
        orm_class = self.get_orm_class(
            foo=Field(int, True),
            bar=Field(str, default=lambda *_, **__: "lambda bar"),
        )

        o = orm_class.hydrate(foo=1)
        pout.v(o.fields)


    def test_no_pk(self):
        orm_class = self.get_orm_class()
        orm_class._id = None

        pks = self.insert(orm_class, 1)

        om1 = orm_class.query.get_one()
        om2 = orm_class.query.is_foo(om1.foo).get_one()
        self.assertEqual(om1.foo, om2.foo)

    def test_readonly(self):
        pass

    def test_int_pk(self):
        """Postgres was returning longs for primary keys in py2.7, this was different
        behavior than SQLite and python 3, which returns int since 2.7+ transparently
        handles ints of arbitrary size, this makes sure that ints are returned for
        primary key"""
        orm_class = self.get_orm_class()
        o = orm_class.create(foo=1, bar="1")
        self.assertTrue(isinstance(o.pk, int))

    def test_create_pk(self):
        """there was a bug that if you set the pk then it wouldn't set the updated
        or created datestamps, this makes sure that is fixed"""
        orm_class = self.get_orm_class()
        pk = testdata.get_int()
        o = orm_class.create(foo=1, bar="1", _id=pk)
        self.assertEqual(pk, o.pk)

    def test_change_pk(self):

        # create a row at pk 1
        orm_class = self.get_orm_class()
        o = orm_class.create(foo=1, bar="one")
        self.assertEqual(1, o.pk)

        # move that row to pk 2
        o._id = 2
        o.save()

        o2 = o.query.get_pk(o.pk)
        for k in o.schema.fields.keys():
            self.assertEqual(getattr(o, k), getattr(o2, k))

        o2.foo = 11
        o2.save()

        # now move it back to pk 1
        o2._id = 1
        o2.save()

        o3 = o.query.get_pk(o2.pk)
        for k in o2.schema.fields.keys():
            self.assertEqual(getattr(o2, k), getattr(o3, k))

        # we should only have 1 row in the db, our row we've changed from 1 -> 2 -> 1
        self.assertEqual(1, o2.query.count())

    def test_overrides(self):
        class FOIndexOverride(Orm):
            table_name = "FOIndexOverride_table"
            _created = None
            index_created = None

        s = FOIndexOverride.schema
        self.assertFalse("index_created" in s.indexes)
        self.assertFalse("_created" in s.fields)

    def test_field_iset(self):
        """make sure a field with an iset method will be called at the correct time"""
        class FOFieldISetOrm(Orm):
            table_name = "FOFieldISetOrm_table"
            foo = Field(int)
            @foo.isaver
            def foo(cls, val, is_update, is_modified):
                val = 100 if is_update else 10
                return val

        #o = FOFieldISetOrm(foo=1)
        o = FOFieldISetOrm()
        o.insert()
        self.assertEqual(10, o.foo)

        o.foo = 20
        o.update()
        self.assertEqual(100, o.foo)

    def test_field_iget(self):
        """make sure a field with an iget method will be called at the correct time"""
        class FOFieldIGetOrm(Orm):
            table_name = "FOFieldIGetOrm_table"
            foo = Field(int)
            @foo.igetter
            def foo(cls, val):
                return 1000

        o = FOFieldIGetOrm()
        o.foo = 20
        o.insert()

        o2 = o.query.get_pk(o.pk)
        self.assertEqual(1000, o2.foo)

    def test_iget_iset_insert_update(self):
        """Topher noticed when you set both iset and iget to wrap a value (in this case
        to have a dict become a json string as it goes into the db but a dict any other
        time) that the value would be iset correctly on insert/update, but then it wouldn't
        go back to the iget value on success, this test makes sure that is fixed"""

        class IGetSetInsertUpdateOrm(Orm):
            table_name = "IGetSetInsertUpdateOrm_table"
            #interface = self.get_interface()
            foo = Field(str)
            @foo.isaver
            def foo(self, val, is_update, is_modified):
                if val is None: return val
                return json.dumps(val)

            @foo.igetter
            def foo(cls, val):
                if val is None: return val
                return json.loads(val)

        o = IGetSetInsertUpdateOrm()
        o.foo = {"foo": 1, "bar": 2}
        self.assertTrue(isinstance(o.foo, dict))
        o.insert()
        self.assertTrue(isinstance(o.foo, dict))

        o.foo = {"foo": 2, "bar": 1}
        self.assertTrue(isinstance(o.foo, dict))
        o.update()
        self.assertTrue(isinstance(o.foo, dict))

        o2 = o.query.get_pk(o.pk)
        self.assertTrue(isinstance(o2.foo, dict))
        self.assertEqual(o.foo, o2.foo)


    def test_field_getattr(self):
        class FOFieldGAOrm(Orm):
            table_name = "fofgaorm_table"
            foo = Field(int)
            @foo.fsetter
            def foo(self, val):
                return getattr(self, "bar", 10)

            bar = Field(int)
            @bar.fsetter
            def bar(self, val):
                return getattr(self, "foo", 10)

        # this test passes if it doesn't raise an exception
        o = FOFieldGAOrm()

    def test_field_lifecycle(self):
        class FOParentOrm(Orm):
            table_name = "foorm_table"
            foo = Field(int)

        o = FOParentOrm.create(foo=1)
        self.assertEqual(1, o.foo)

        o.foo = 2
        self.assertTrue("foo" in o.modified_fields)

        o.save()
        o2 = o.query.get_pk(o.pk)
        self.assertEqual(2, o.foo)

        del o.foo
        self.assertEqual(None, o.foo)
        self.assertTrue("foo" in o.modified_fields)

    def test___delattr__(self):
        class DAOrm(Orm):
            table_name = "daorm_table"
            foo = Field(int)
            bar = Field(str)

        o = DAOrm()
        o.foo = 1
        o.bar = "1"
        self.assertEqual(1, o.foo)
        self.assertEqual("1", o.bar)

        del o.foo
        self.assertEqual(None, o.foo)

        del o.bar
        self.assertEqual(None, o.bar)

        o.che = "yay"
        self.assertEqual("yay", o.che)

        del o.che
        with self.assertRaises(AttributeError):
            o.che

    def test___setattr__(self):
        class SAOrm(Orm):
            table_name = "saorm_table"
            foo = Field(int)
            bar = Field(str)

        o = SAOrm()
        o.foo = 1
        o.bar = "1"
        self.assertTrue(o.modified_fields)

        o.save()
        self.assertFalse(o.modified_fields)

        o.foo = 2
        self.assertTrue(o.modified_fields)

        o.save()
        self.assertFalse(o.modified_fields)

        o.foo = None
        o.bar = None
        self.assertEqual(2, len(o.modified_fields))

    def test_creation(self):

        class COrm(Orm):
            foo = Field(int)
            bar = Field(str)

        s = COrm.schema
        self.assertTrue(s.foo)
        self.assertTrue(s.bar)
        self.assertTrue(s.pk)
        self.assertTrue(s._created)
        self.assertTrue(s._updated)

    def test_none(self):
        orm_class = self.get_orm_class()
        orm_class.foo.required = False
        orm_class.bar.required = False

        t1 = orm_class()
        t2 = orm_class(foo=None, bar=None)
        self.assertEqual(t1.fields, t2.fields)

        t1.save()
        t2.save()

        t11 = orm_class.query.get_pk(t1.pk)
        t22 = orm_class.query.get_pk(t2.pk)
        ff = lambda orm: orm.schema.normal_fields
        self.assertEqual(ff(t11), ff(t22))
        self.assertEqual(ff(t1), ff(t11))
        self.assertEqual(ff(t2), ff(t22))

        t3 = orm_class(foo=1)
        self.assertEqual(1, t3.foo)
        self.assertEqual(None, t3.bar)
        t3.save()
        self.assertEqual(1, t3.foo)
        self.assertEqual(None, t3.bar)
        t3 = orm_class.query.get_pk(t3.pk)
        self.assertEqual(1, t3.foo)
        self.assertEqual(None, t3.bar)

    def test_jsonable(self):
        orm_class = self.get_orm_class()
        orm_class.dt = Field(datetime.datetime)
        t = orm_class()
        t.hydrate(foo=1, bar="blah", dt=datetime.datetime.utcnow())
        d = t.jsonable()
        self.assertEqual(1, d['foo'])
        self.assertEqual("blah", d['bar'])
        self.assertTrue("dt" in d)

        t = orm_class()
        t.hydrate(foo=1)
        d = t.jsonable()
        pout.v(d)
        self.assertEqual(1, d['foo'])
        self.assertFalse("bar" in d)

    def test_modify_1(self):
        class TM(Orm):
            table_name = self.get_table_name()

            bar = Field(str, True)

            che = Field(str, False)

            @che.fsetter
            def che(self, field_val):
                if field_val is None: return field_val
                if not field_val.startswith('boom'):
                    raise ValueError("what the heck?")
                return field_val

        t = TM(bar='bam')

        with self.assertRaises(ValueError):
            t = TM(che='bam')

        t = TM(che='boom')

        self.assertIsNone(t.pk)
        self.assertIsNone(t._created)
        self.assertIsNone(t._updated)

    def test_modify_2(self):
        class TM(Orm):
            table_name = self.get_table_name()

            foo = Field(str, False)
            bar = JsonField(False)
            che = ObjectField(False)

        t = TM()
        self.assertTrue(t.is_modified())
        for k in ["bar", "che"]:
            self.assertTrue(k in t.modified_fields)

        t.bar["foo"] = 1
        t.save()

        t2 = t.query.get_pk(t.pk)
        self.assertEqual(t.bar["foo"], t2.bar["foo"])

        t2.bar["foo"] = 2
        t2.save()

        t3 = t.query.get_pk(t.pk)
        self.assertEqual(t3.bar["foo"], t2.bar["foo"])

    def test_modify_none(self):
        class TModifyNone(Orm):
            table_name = self.get_table_name()
            foo = Field(str, False)

        o = TModifyNone()
        o.foo = 1
        o.save()

        o2 = o.query.get_pk(o.pk)
        o2.foo = None
        o2.save()
        self.assertIsNone(o2.foo)

        o3 = o.query.get_pk(o.pk)
        self.assertIsNone(o3.foo)

    def test_unicode(self):
        """
        Jarid was having encoding issues, so I'm finally making sure prom only ever
        returns unicode strings
        """
        orm_class = self.get_orm_class()
        table_name = self.get_table_name()
        orm_class.schema = self.get_schema(
            self.get_table_name(),
            foo=Field(unicode, True),
            bar=Field(str, True),
            che=Field(str, False),
            baz=Field(int, False),
        )

        t = orm_class.create(
            foo=testdata.get_unicode_name(),
            bar=testdata.get_unicode_words(),
            che=testdata.get_unicode_words().encode('utf-8'),
            baz=testdata.get_int(1, 100000)
        )

        t2 = orm_class.query.get_pk(t.pk)

        self.assertEqual(t.foo, t2.foo)
        self.assertEqual(t.bar, t2.bar)
        #self.assertEqual(t.che, t2.che.encode('utf-8'))

        self.assertEqual(t.che.decode("utf-8"), t2.che)
        self.assertTrue(isinstance(t.baz, int))

    def test_query(self):
        orm_class = self.get_orm_class()
        pks = self.old_insert(orm_class.interface, orm_class.schema, 5)
        lc = orm_class.query.in_pk(pks).count()
        self.assertEqual(len(pks), lc)

    def test___int__(self):
        orm_class = self.get_orm_class()
        pk = self.old_insert(orm_class.interface, orm_class.schema, 1)[0]
        t = orm_class.query.get_pk(pk)
        self.assertEqual(pk, int(t))

    def test_query_class(self):
        """make sure you can set the query class and it is picked up correctly"""
        class QueryClassTormQuery(prom.Query):
            pass

        class QueryClassTorm(Orm):
            query_class = QueryClassTormQuery
            pass

        other_orm_class = self.get_orm_class()
        self.assertEqual(QueryClassTorm.query_class, QueryClassTormQuery)
        self.assertEqual(other_orm_class.query_class, prom.Query)

    def test_property_autodiscover(self):
        testdata.create_module("fooq", "\n".join([
            "import prom",
            "",
            "class FooQuery(prom.Query):",
            "    pass",
            "",
            "class Foo(prom.Orm):",
            "    schema = prom.Schema('foo')",
            "    query_class = FooQuery",
            "",
            "class BarQuery(prom.Query):",
            "    pass",
            "",
            "class Bar(Foo):",
            "    schema = prom.Schema('bar')",
            "    query_class = BarQuery",
            "    pass",
            "",
            "class CheQuery(prom.Query):",
            "    pass",
        ]))

        import fooq

        # first try with the instance calling first
        f = fooq.Foo()
        self.assertEqual(f.query_class, fooq.Foo.query_class)

        f = fooq.Foo()
        self.assertEqual(f.query.__class__.__name__, fooq.Foo.query.__class__.__name__)

        f = fooq.Foo()
        self.assertEqual(f.interface, fooq.Foo.interface)

        # now try with the class calling first
        b = fooq.Bar()
        self.assertEqual(fooq.Bar.query_class, b.query_class)

        b = fooq.Bar()
        self.assertEqual(fooq.Bar.query.__class__.__name__, b.query.__class__.__name__)

        b = fooq.Bar()
        self.assertEqual(fooq.Bar.interface, b.interface)

        # now make sure we can manipulate it
        fooq.Foo.query_class = fooq.CheQuery
        f = fooq.Foo()
        self.assertEqual(fooq.CheQuery, f.query_class)
        self.assertEqual(fooq.CheQuery, fooq.Foo.query_class)
        self.assertEqual(fooq.CheQuery, f.query.__class__)
        self.assertEqual(fooq.CheQuery, fooq.Foo.query.__class__)

    def test_interface(self):
        i = self.get_interface()
        #i = Torm.interface
        self.assertFalse(i is None)

        class TormInterface2Orm(Orm):
            pass

        i = TormInterface2Orm.interface
        self.assertFalse(i is None)

        # now let's make sure a different orm with a bad connection name gets flagged
        class TormInterfaceOrm(Orm):
            connection_name = "blkasdfjksdafjdkfklsd"
            pass

        with self.assertRaises(KeyError):
            i = TormInterfaceOrm.interface

    def test___init__(self):
        orm_class = self.get_orm_class()
        t = orm_class(foo=1)
        self.assertTrue('foo' in t.modified_fields)
        self.assertEqual(1, t.foo)

    def test_save(self):
        orm_class = self.get_orm_class()
        t = orm_class()
        with self.assertRaises(KeyError):
            t.save()

        t = orm_class(foo=1, bar="value 1", this_is_ignored="as it should be")
        self.assertEqual(1, t.foo)
        self.assertEqual("value 1", t.bar)
        self.assertIsNone(t.pk)
        self.assertTrue(t.is_modified())
        t.save()
        self.assertIsNotNone(t.pk)
        self.assertFalse(t.is_modified())

        t.foo = 2
        t.bar = "value 2"
        self.assertTrue(t.is_modified())
        t.save()
        self.assertEqual(2, t.foo)
        self.assertEqual("value 2", t.bar)

        # set should only update timestamps and stuff without changing unmodified values
        self.assertFalse(t.is_modified())
        r = t.save()
        self.assertTrue(r)

        # make sure it persisted
        t.interface.close()
        t2 = orm_class.query.is_pk(t.pk).get_one()
        self.assertFalse(t2.is_modified())
        self.assertEqual(2, t2.foo)
        self.assertEqual("value 2", t2.bar)
        self.assertEqual(t.fields, t2.fields)

    def test_delete(self):
        t = self.get_orm(foo=1, bar="value 1")
        r = t.delete()
        self.assertFalse(r)
        t.save()
        self.assertTrue(t.pk)
        _id = t.pk

        t.delete()
        self.assertFalse(t.pk)
        self.assertTrue(t.is_modified())

        # make sure it persists
        t.interface.close()
        t2 = t.query.get_pk(_id)
        self.assertEqual(None, t2)

    def test_create(self):
        orm_class = self.get_orm_class()
        t = orm_class.create(foo=1000, bar="value1000")
        self.assertLess(0, t.pk)
        self.assertEqual(1000, t.foo)
        self.assertEqual("value1000", t.bar)

    def test_fields(self):
        orm_class = self.get_orm_class()
        t = orm_class.create(foo=1000, bar="value1000")
        d = t.fields
        for f in t.schema.fields:
            self.assertTrue(f in d)

        # just make sure changing the dict doesn't mess up the Orm instance
        d["_id"] = d["_id"] + 1
        self.assertNotEqual(d["_id"], t.pk)

    def test_pickling(self):
        t = PickleOrm(foo=10000, bar="value10000")

        p = pickle.dumps(t)
        t2 = pickle.loads(p)
        self.assertEqual(t.fields, t2.fields)
        self.assertEqual(t.modified_fields, t2.modified_fields)

        t.save()
        p = pickle.dumps(t)
        t2 = pickle.loads(p)
        self.assertEqual(t.fields, t2.fields)
        self.assertEqual(t.modified_fields, t2.modified_fields)

        t2.foo += 1
        t2.save()

        t3 = PickleOrm.query.get_pk(t2.pk)
        self.assertEqual(t3.fields, t2.fields)

    def test_transaction(self):
        """with transaction context managers weren't working correctly when the
        second insert would fail, the first insert was still going through, this
        test helped me reproduce, diagnose, and fix the problem"""
        # CRUD
        class TransTorm1(Orm):
            table_name = "trans_torm_1"
            foo = Field(str, True)

            @classmethod
            def creation(cls, d):
                with cls.interface.transaction():
                    d['foo'] = "foo"
                    tt = cls.create(**d)

                    d['tt1_id'] = tt.pk
                    m = TransTorm2.create(**d)

                return tt

        class TransTorm2(Orm):
            table_name = "trans_torm_2"
            bar = Field(str, True, max_size=10)
            tt1_id = Field(TransTorm1, True)

        TransTorm1.install()
        TransTorm2.install()

        # actual test starts here

        self.assertEqual(0, TransTorm1.query.count())

        #d = {"bar": testdata.get_ascii(32)}
        d = {}
        #with self.assertRaises(prom.InterfaceError):
        with self.assertRaises(Exception):
            tt = TransTorm1.creation(d)

        self.assertEqual(0, TransTorm1.query.count())

    def test_non_int_primary_key(self):
        class Nipk(Orm):
            table_name = "non_int_pk_1"
            _id = Field(str, True, pk=True, max_size=64)

        class Nipk2(Orm):
            table_name = "non_int_pk_2"
            nipk_id = Field(Nipk, True)

        # since our pk no longer is auto-increment we always have to provide it
        with self.assertRaises(prom.InterfaceError):
            Nipk.create()

        n = Nipk.create(_id="pk1")
        self.assertEqual("pk1", n.pk)
        self.assertEqual("pk1", n._id)

        with self.assertRaises(ValueError):
            pk = int(n)
        self.assertEqual("pk1", str(n))

        with self.assertRaises(prom.InterfaceError):
            Nipk.create(_id="pk1")

        n2 = Nipk2.create(nipk_id=n.pk)
        self.assertEqual(n.pk, n2.nipk_id)

    def test_failure_save(self):
        """test to make sure saving on a table that doesn't exist doesn't actually fail"""
        class FailureSetTorm(Orm):
            interface = self.get_interface()
            schema = self.get_schema()

        f = FailureSetTorm(foo=1, bar="value 1")
        f.save()
        self.assertTrue(f.pk)

    def test_failure_get(self):
        """test to make sure getting on a table that doesn't exist works without raising
        an error
        """
        class FailureGetTorm(Orm):
            interface = self.get_interface()
            schema = self.get_schema()

        f = FailureGetTorm(foo=1, bar="value 1")
        f.query.get_one()
        # we succeeded if no error was raised

    def test_fk(self):
        mpath = testdata.create_module(contents=[
            "from prom import Field, Orm",
            "",
            "class Foo(Orm):",
            "    pass",
            "",
            "class Bar(Orm):",
            "    foo_id = Field(Foo, True)",
            "",
            "class Che(Orm):",
            "    foo_id = Field(Foo, False)",
            "    bar_id = Field(Bar, True)",
            "",
            "class Boo(Orm):",
            "    pass",
        ])

        Foo = mpath.module.Foo
        Bar = mpath.module.Bar
        Che = mpath.module.Che
        Boo = mpath.module.Boo

        b = Bar(foo_id=5)
        self.assertEqual(5, b.fk(Foo))

        c = Che(foo_id=10, bar_id=20)
        self.assertEqual(10, c.fk(Foo))
        self.assertEqual(20, c.fk(Bar))

        c = Che()
        self.assertEqual(None, c.fk(Foo))
        self.assertEqual(None, c.fk(Bar))
        with self.assertRaises(ValueError):
            c.fk(Boo)

    def test_subquery_1(self):
        count = 10
        mpath = testdata.create_module(contents=[
            "from prom import Field, Orm",
            "",
            "class Foo(Orm):",
            "    pass",
            "",
            "class Bar(Orm):",
            "    foo_id = Field(Foo, True)",
        ])

        Foo = mpath.module.Foo
        Bar = mpath.module.Bar

        foo_ids = self.insert(Foo, count)
        for foo_id in foo_ids:
            Bar.create(foo_id=foo_id)

        q = Bar.query.in_foo_id(Foo.query.select_pk())
        self.assertEqual(count, len(q.get()))

        q = Bar.query.in_foo_id(Foo.query.select_pk().gt_pk(count + 10000))
        self.assertEqual(0, len(q.get()))

        q = Bar.query.is_foo_id(Foo.query.select_pk().limit(1))
        self.assertEqual(1, len(q.get()))

    def test_subquery_2(self):
        """Similar test as subquery_1 but makes sure query_class works as expected also"""
        count = 10
        mpath = testdata.create_module(contents=[
            "from prom import Field, Orm, Query",
            "",
            "class Foo(Orm):",
            "    pass",
            "",
            "class BarQuery(Query):",
            "    pass",
            "",
            "class Bar(Orm):",
            "    foo_id = Field(Foo, True)",
            "    query_class = BarQuery",
        ])

        Foo = mpath.module.Foo
        Bar = mpath.module.Bar

        foo_ids = self.insert(Foo, count)
        for foo_id in foo_ids:
            Bar.create(foo_id=foo_id)

        q = Bar.query.in_foo_id(Foo.query.select_pk())
        self.assertEqual(count, len(q.get()))

