# -*- coding: utf-8 -*-
from __future__ import unicode_literals, division, print_function, absolute_import
import pickle
import json
import datetime

from datatypes import Datetime

from . import BaseTestCase, EnvironTestCase, testdata
from prom.compat import *
from prom.model import Orm, OrmPool
from prom.config import Field, Index
from prom.query import Query
import prom


class OrmPoolTest(EnvironTestCase):
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
    def test_custom__id_pk(self):
        orm_class = self.get_orm_class(
            _id=Field(str, True, max_size=36, pk=True)
        )
        o = orm_class.create(_id="foo")
        o2 = orm_class.query.one()
        self.assertEqual(o.pk, o2.pk)

    def test_overridden_field_aliases(self):
        """Makes sure if we override a parent's defined Field instance the aliases
        will still be set and point back to the overridden field"""
        orm_class = self.get_orm_class(_id=None, _created=None, _updated=None)

        o = orm_class()
        self.assertIsNone(o._id)
        self.assertIsNone(o.pk)
        self.assertIsNone(o._created)
        self.assertIsNone(o.created)
        self.assertIsNone(o._updated)
        self.assertIsNone(o.updated)

    def test_field_access(self):
        orm_class = self.get_orm_class()

        o = orm_class(foo=1, bar="2")
        self.assertEqual(1, o.foo)
        self.assertIsNone(o.pk)
        with self.assertRaises(AttributeError):
            o.fkdasljfdkfsalk

        orm_class = self.get_orm_class()
        orm_class._id = None
        o = orm_class(foo=1, bar="2")

        self.assertIsNone(o.pk)
        self.assertIsNone(o._id)
        self.assertEqual(1, o.foo)
        with self.assertRaises(AttributeError):
            o.fkdasljfdkfsalk

    def test_aliases_1(self):
        class Foo(Orm):
            ip_address = Field(str, False, aliases=["ip"])

        ip = "1.2.3.4"

        f = Foo(ip="1.2.3.4")
        self.assertEqual(ip, f.ip)
        self.assertEqual(ip, f.ip_address)

        f = Foo()
        f.ip = ip
        self.assertEqual(ip, f.ip)
        self.assertEqual(ip, f.ip_address)

        f = Foo(ip="1.2.3.4")
        del f.ip
        self.assertIsNone(f.ip)
        self.assertIsNone(f.ip_address)

    def test_aliases_2(self):
        orm_class = self.get_orm_class(
            foo=Field(int, alias="foo2"),
        )

        o = orm_class(foo2=1)
        self.assertEqual(1, o.foo)

    def test_alias_pk(self):
        orm_class = self.get_orm_class(model_name="o1")
        o = self.insert_orm(orm_class)
        self.assertEqual(o._id, o.o1_id)
        self.assertEqual(o._id, o.o1_pk)
        self.assertEqual(o._id, o.pk)

    def test_removed_field(self):
        orm_class = self.get_orm_class()
        o = orm_class.create(foo=1, bar="2")
        o.save()

        orm_class.schema.fields.pop("bar")

        for count, o in enumerate(orm_class.query, 1):
            pass
        self.assertLess(0, count)

    def test_required_field_not_set_update(self):
        orm_class = self.get_orm_class()

        o = orm_class(foo=1, bar="two")

        o.save()

        # create a new instance and make it look like an existing instance
        o2 = orm_class(
            foo=o.foo,
            bar=o.bar,
        )
        o2._interface_pk = o.pk

        fields = o2.to_interface()
        self.assertFalse("_created" in fields)

        o._created = None
        with self.assertRaises(KeyError):
            fields = o.to_interface()

        del o._created
        fields = o.to_interface()
        self.assertFalse("_created" in fields)

    def test_f_class_definition(self):
        class FCD(Orm):
            _id = _created = _updated = None
            class foo(Field):
                type = int
                def fset(self, orm, v):
                    return super().fset(orm, v) + 1
            class bar(Field):
                type = int
                def fget(self, orm, v):
                    return super().fget(orm, v) + 2

        o = FCD(foo=1, bar=1)
        self.assertEqual(2, o.foo)
        self.assertEqual(3, o.bar)

        class Foo(Field):
            type = int
            def fset(self, orm, v):
                return super().fset(orm, v) + 1
        class Bar(Field):
            type = int
            def fget(self, orm, v):
                return super().fget(orm, v) + 2
        orm_class = self.get_orm_class(
            foo=Foo,
            bar=Bar,
        )

        o = orm_class(foo=1, bar=1)
        self.assertEqual(2, o.foo)
        self.assertEqual(3, o.bar)

    def test_to_interface_insert(self):
        orm_class = self.get_orm_class(
            foo=Field(int, True, default=1),
            bar=Field(str, False)
        )
        o = orm_class()

        fields = o.to_interface()
        self.assertTrue("foo" in fields)
        self.assertFalse("bar" in fields)

        orm_class = self.get_orm_class()
        o = orm_class()

        # missing foo
        with self.assertRaises(KeyError):
            fields = o.to_interface()

        o.foo = 1

        # missing bar
        with self.assertRaises(KeyError):
            fields = o.to_interface()

        o.bar = "2"

        fields = o.to_interface()
        self.assertFalse("_id" in fields)

    def test_to_interface_update(self):
        orm_class = self.get_orm_class()
        o = orm_class(foo=1, bar="2")
        o.save()

        fields = o.to_interface()
        self.assertEqual(1, len(fields)) # _updated would be the only field

        o.foo = None
        with self.assertRaises(KeyError):
            fields = o.to_interface()
        o.foo = 1

        o._id = None
        with self.assertRaises(KeyError):
            fields = o.to_interface()

    def test_created_updated(self):
        orm_class = self.get_orm_class()

        now = Datetime()

        o = orm_class(foo=1, bar="1")
        self.assertIsNone(o._created)
        self.assertIsNone(o._updated)

        o.save()
        self.assertLess(now, o._created)
        self.assertLess(now, o._updated)

        _created = o._created
        _updated = o._updated
        o.foo=2
        o.save()
        self.assertEqual(_created, o._created)
        self.assertLess(_updated, o._updated)

    def test_created_set(self):
        orm_class = self.get_orm_class()
        o = orm_class(foo=1, bar="1")

        _created = testdata.get_past_datetime().replace(tzinfo=datetime.timezone.utc)
        o._created = _created
        o.save()

        o2 = o.query.eq_pk(o.pk).one()
        self.assertEqual(_created, o2._created)

        _created2 = testdata.get_past_datetime().replace(tzinfo=datetime.timezone.utc)
        o2._created = _created2
        o2.save()

        o3 = o.query.eq_pk(o.pk).one()
        self.assertEqual(_created2, o3._created)

    def test_updated_set(self):
        orm_class = self.get_orm_class()
        o = orm_class(foo=1, bar="1")

        _updated = testdata.get_past_datetime().replace(tzinfo=datetime.timezone.utc)
        o._updated = _updated
        o.save()

        o2 = o.query.eq_pk(o.pk).one()
        self.assertEqual(_updated, o2._updated)

        _updated2 = testdata.get_past_datetime().replace(tzinfo=datetime.timezone.utc)
        o2._updated = _updated2
        o2.save()

        o3 = o.query.eq_pk(o.pk).one()
        self.assertEqual(_updated2, o3._updated)

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

        o3 = o.query.one_pk(o.pk)
        self.assertTrue(o3.is_hydrated())

        o3._id = o.pk + 1
        self.assertNotEqual(o.pk, o3.pk)

        o3.save()
        o4 = o3.query.one_pk(o3.pk)
        self.assertEqual(o3.pk, o4.pk)
        self.assertNotEqual(o.pk, o3.pk)

    def test_hydrate_2(self):
        orm_class = self.get_orm_class(
            foo=Field(int, True),
            bar=Field(str, default=lambda *_, **__: "lambda bar"),
        )

        o = orm_class.hydrate(foo=1)
        self.assertEqual("lambda bar", o.bar)

    def test_no_pk(self):
        orm_class = self.get_orm_class(_id=None)

        pks = self.insert(orm_class, 1)

        om1 = orm_class.query.one()
        om2 = orm_class.query.is_foo(om1.foo).one()
        self.assertEqual(om1.foo, om2.foo)

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

        o2 = o.query.one_pk(o.pk)

        for k in o.schema.fields.keys():
            self.assertEqual(getattr(o, k), getattr(o2, k), k)

        o2.foo = 11
        o2.save()

        # now move it back to pk 1
        o2._id = 1
        o2.save()

        o3 = o.query.one_pk(o2.pk)
        for k in o2.schema.fields.keys():
            self.assertEqual(getattr(o2, k), getattr(o3, k))

        # we should only have 1 row in the db, our row we've changed from 1 -> 2 -> 1
        self.assertEqual(1, o2.query.count())

    def test_overrides(self):
        orm_class = self.get_orm_class(_created=None)

        s = orm_class.schema
        self.assertTrue("_updated" in s.fields)
        self.assertFalse("_created" in s.fields)

    def test_field_iset(self):
        """make sure a field with an iset method will be called at the correct time"""
        orm_class = self.get_orm_class(
            foo=Field(int, iset=lambda self, *_, **__: 100 if self.is_update() else 10)
        )

        o = orm_class()
        o.insert()
        self.assertEqual(10, o.foo)

        o.foo = 20
        o.update()
        self.assertEqual(100, o.foo)

    def test_field_iget(self):
        """make sure a field with an iget method will be called at the correct time"""
        orm_class = self.get_orm_class(
            foo=Field(int, iget=lambda *_, **__: 1000)
        )
        o = orm_class()
        o.foo = 20
        o.insert()

        o2 = o.query.one_pk(o.pk)
        self.assertEqual(1000, o2.foo)

    def test_iget_iset_insert_update(self):
        """Topher noticed when you set both iset and iget to wrap a value (in this case
        to have a dict become a json string as it goes into the db but a dict any other
        time) that the value would be iset correctly on insert/update, but then it wouldn't
        go back to the iget value on success, this test makes sure that is fixed"""

        class IGetSetInsertUpdateOrm(Orm):
            table_name = self.get_table_name()
            interface = self.get_interface()
            foo = Field(str)

            @foo.isetter
            def foo(self, val):
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

        o2 = o.query.one_pk(o.pk)
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
        orm_class = self.get_orm_class(
            foo=Field(int)
        )

        o = orm_class.create(foo=1)
        self.assertEqual(1, o.foo)

        o.foo = 2
        self.assertTrue("foo" in o.modified_fields)

        o.save()
        o2 = o.query.one_pk(o.pk)
        self.assertEqual(2, o.foo)

        del o.foo
        self.assertEqual(None, o.foo)
        self.assertFalse("foo" in o.modified_fields)

    def test___delattr__(self):
        orm_class = self.get_orm_class(
            foo=Field(int),
            bar=Field(str)
        )

        o = orm_class()
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
        orm_class = self.get_orm_class(
            foo=Field(int),
            bar=Field(str)
        )

        o = orm_class()
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

    def test___getattr___orm_1(self):
        foo_class = self.get_orm_class(model_name="foo")
        bar_class = self.get_orm_class(foo_id=Field(foo_class, True))

        foo_id = self.insert_fields(foo_class)
        bar = bar_class(foo_id=foo_id)

        foo = bar.foo
        self.assertEqual(foo_id, foo.pk)

        foo2_id = self.insert_fields(foo_class)
        foo2 = foo_class.query.eq_pk(foo2_id).one()

        bar.foo = foo2
        bar.save()
        self.assertEqual(foo2.pk, bar.foo_id)

        foo2 = bar.foo
        self.assertEqual(foo2_id, foo2.pk)
        self.assertNotEqual(foo_id, foo2_id)

    def test___getattr___orm_2(self):
        o1_class = self.get_orm_class(
            bar=Field(bool),
            che=Field(str),
            model_name="o1",
        )
        o2_class = self.get_orm_class(o1_id=Field(o1_class))
        o1 = self.insert_orm(o1_class, bar=False, che="1")
        self.assertLess(0, o1.pk)
        self.assertFalse(o1.bar)

        o2 = self.insert_orm(o2_class, o1_id=o1.pk)
        r1 = o2.o1
        self.assertEqual(o1.pk, r1.pk)
        self.assertEqual(o1.bar, r1.bar)
        self.assertEqual(o1.che, r1.che)

    def test___getattr___orm_3(self):
        """Makes sure referencing a foreign key field by the model_name returns
        None. This was originally in extras.MagicOrmTest"""
        o1_class = self.get_orm_class(
            bar=Field(bool),
            che=Field(str),
            model_name="o1",
        )
        o2_class = self.get_orm_class(o1_id=Field(o1_class))

        o1 = self.insert_orm(o1_class)
        o2 = self.insert_orm(o2_class)

        self.assertIsNone(o2.o1)
        with self.assertRaises(AttributeError):
            o2.blahblah

    def test___getattr___models_name(self):
        """Makes sure that getting an attribute by models_name works as expected"""
        o1_class = self.get_orm_class(
            bar=Field(str),
            model_name="o1",
            #models_name="o1s",
        )
        o2_class = self.get_orm_class(
            o1_id=Field(o1_class),
            model_name="o2",
        )

        o1 = self.insert_orm(o1_class, bar="1")
        self.insert_orm(o1_class, bar="ignored")

        o2 = self.insert_orm(o2_class, o1_id=o1.pk)
        o2 = self.insert_orm(o2_class, o1_id=o1.pk)

        count = 0
        for o2r in o1.o2s:
            count += 1
            self.assertEqual(o1.pk, o2.o1_id)
        self.assertEqual(2, count)

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

        t11 = orm_class.query.one_pk(t1.pk)
        t22 = orm_class.query.one_pk(t2.pk)
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
        t3 = orm_class.query.one_pk(t3.pk)
        self.assertEqual(1, t3.foo)
        self.assertEqual(None, t3.bar)

    def test_jsonable(self):
        orm_class = self.get_orm_class()
        orm_class.dt = Field(datetime.datetime)
        t = orm_class.hydrate(foo=1, bar="blah", dt=datetime.datetime.utcnow())
        d = t.jsonable()
        self.assertEqual(1, d['foo'])
        self.assertEqual("blah", d['bar'])
        self.assertTrue("dt" in d)

        t = orm_class.hydrate(foo=1)
        d = t.jsonable()
        self.assertEqual(1, d['foo'])
        self.assertFalse("bar" in d)

    def test_jsonable_name(self):
        orm_class = self.get_orm_class(
            foo=Field(int, jsonable_name="bar")
        )

        o = orm_class(foo=1)
        d = o.jsonable()
        self.assertFalse("foo" in d)
        self.assertTrue("bar" in d)

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
        orm_class = self.get_orm_class(
            foo=Field(str, False),
            bar=Field(dict, False, default=dict),
            che=Field(dict, False, default=dict),
        )

        t = orm_class()
        self.assertTrue(t.is_modified())
        for k in ["bar", "che"]:
            self.assertTrue(k in t.modified_fields)

        t.bar["foo"] = 1
        t.save()

        t2 = t.query.one_pk(t.pk)
        self.assertEqual(t.bar["foo"], t2.bar["foo"])

        t2.bar["foo"] = 2
        t2.save()

        t3 = t.query.one_pk(t.pk)
        self.assertEqual(t3.bar["foo"], t2.bar["foo"])

    def test_modify_none(self):
        orm_class = self.get_orm_class(
            foo=Field(str, False)
        )

        o = orm_class()
        o.foo = 1
        o.save()

        o2 = o.query.one_pk(o.pk)
        o2.foo = None
        o2.save()
        self.assertIsNone(o2.foo)

        o3 = o.query.one_pk(o.pk)
        self.assertIsNone(o3.foo)

    def test_modified_1(self):
        orm_class = self.get_orm_class()
        o = orm_class(foo=1, bar="2")

        mfs = o.modified_fields
        self.assertEqual(2, len(mfs))
        for field_name in ["foo", "bar"]:
            self.assertTrue(field_name in mfs)

        o.save()

        mfs = o.modified_fields
        self.assertEqual(0, len(mfs))

        o.foo += 1
        mfs = o.modified_fields
        self.assertEqual(1, len(mfs))
        self.assertTrue("foo" in mfs)

        o2 = o.requery()
        mfs = o2.modified_fields
        self.assertEqual(0, len(mfs))

    def test_unicode(self):
        """
        Jarid was having encoding issues, so I'm finally making sure prom only ever
        returns unicode strings
        """
        orm_class = self.get_orm_class(
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

        t2 = orm_class.query.one_pk(t.pk)

        self.assertEqual(t.foo, t2.foo)
        self.assertEqual(t.bar, t2.bar)
        #self.assertEqual(t.che, t2.che.encode('utf-8'))

        self.assertEqual(t.che.decode("utf-8"), t2.che)
        self.assertTrue(isinstance(t.baz, int))

    def test_query(self):
        orm_class = self.get_orm_class()
        pks = self.insert(orm_class, 5)
        lc = orm_class.query.in_pk(pks).count()
        self.assertEqual(len(pks), lc)

    def test___int__(self):
        orm_class = self.get_orm_class()
        pk = self.insert(orm_class, 1)[0]
        t = orm_class.query.one_pk(pk)
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
        m = testdata.create_module([
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
        ])

        fooq = m.module()

        # first try with the instance calling first
        f = fooq.Foo()
        self.assertEqual(f.query_class, fooq.Foo.query_class)

        f = fooq.Foo()
        self.assertEqual(f.query.__class__.__name__, fooq.Foo.query.__class__.__name__)

        f = fooq.Foo()
        #self.assertEqual(f.interface, fooq.Foo.interface)

        # now try with the class calling first
        b = fooq.Bar()
        self.assertEqual(fooq.Bar.query_class, b.query_class)

        b = fooq.Bar()
        self.assertEqual(fooq.Bar.query.__class__.__name__, b.query.__class__.__name__)

        b = fooq.Bar()
        #self.assertEqual(fooq.Bar.interface, b.interface)

        # now make sure we can manipulate it
        fooq.Foo.query_class = fooq.CheQuery
        f = fooq.Foo()
        self.assertEqual(fooq.CheQuery, f.query_class)
        self.assertEqual(fooq.CheQuery, fooq.Foo.query_class)
        self.assertEqual(fooq.CheQuery, f.query.__class__)
        self.assertEqual(fooq.CheQuery, fooq.Foo.query.__class__)

    def test_interface(self):
        i = self.get_interface()
        self.assertFalse(i is None)

        orm_class = self.get_orm_class()

        i = orm_class.interface
        self.assertFalse(i is None)

        # now let's make sure a different orm with a bad connection name gets flagged
        orm_class = self.get_orm_class(
            connection_name="blkasdfjksdafjdkfklsd"
        )

        with self.assertRaises(KeyError):
            i = orm_class.interface

    def test___init__(self):
        orm_class = self.get_orm_class()
        t = orm_class(foo=1)
        self.assertTrue('foo' in t.modified_fields)
        self.assertEqual(1, t.foo)

    def test___init___default_fset(self):
        orm_class = self.get_orm_class(
            foo=Field(int, default=5),
            bar=Field(int, fset=lambda o, v: 6 if v is None else v),
            che=Field(int)
        )

        o = orm_class()
        self.assertEqual(5, o.foo)
        self.assertEqual(6, o.bar)
        self.assertIsNone(o.che)

        o.modify(che=7)
        self.assertEqual(5, o.foo)
        self.assertEqual(6, o.bar)
        self.assertEqual(7, o.che)

        o = orm_class(foo=1)
        self.assertEqual(1, o.foo)
        self.assertEqual(6, o.bar)
        self.assertIsNone(o.che)

        o.modify(che=7, bar=8)
        self.assertEqual(1, o.foo)
        self.assertEqual(8, o.bar)
        self.assertEqual(7, o.che)

        o = orm_class(foo=1, bar=2, che=3)
        self.assertEqual(1, o.foo)
        self.assertEqual(2, o.bar)
        self.assertEqual(3, o.che)

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
        t2 = orm_class.query.is_pk(t.pk).one()
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
        t2 = t.query.one_pk(_id)
        self.assertEqual(None, t2)

    def test_create_1(self):
        orm_class = self.get_orm_class()
        t = orm_class.create(foo=1000, bar="value1000")
        self.assertLess(0, t.pk)
        self.assertEqual(1000, t.foo)
        self.assertEqual("value1000", t.bar)

    def test_create_2(self):
        """https://github.com/Jaymon/prom/issues/124"""
        kwargs = {
            "foo": 1,
            "bar": "2"
        }
        orm_class = self.get_orm_class()

        o = orm_class.create(**kwargs)
        self.assertTrue(isinstance(o._created, datetime.datetime))

        kwargs["_id"] = o.pk + 1
        o2 = orm_class.create(**kwargs)
        self.assertTrue(isinstance(o._created, datetime.datetime))

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
        mpath = testdata.create_module([
            "from prom import Orm, Field, Index",
            "",
            "class PickleOrm(Orm):",
            "    foo = Field(int, True)",
            "    bar = Field(str, True)",
            "    ifoobar = Index('foo', 'bar')",
        ])

        PickleOrm = mpath.module().PickleOrm
        PickleOrm.interface = self.get_interface()
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

        t3 = PickleOrm.query.one_pk(t2.pk)
        self.assertEqual(t3.fields, t2.fields)

    def test_transaction(self):
        """with transaction context managers weren't working correctly when the
        second insert would fail, the first insert was still going through, this
        test helped me reproduce, diagnose, and fix the problem"""
        # CRUD
        class TransTorm1(Orm):
            interface = self.get_interface()
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
            interface = TransTorm1.interface
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
        Nipk = self.get_orm_class(
            _id=Field(str, True, pk=True, max_size=64)
        )
        Nipk2 = self.get_orm_class(
            nipk_id=Field(Nipk, True)
        )

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
        orm_class = self.get_orm_class()
        f = orm_class(foo=1, bar="value 1")
        f.save()
        self.assertTrue(f.pk)

    def test_failure_get(self):
        """test to make sure getting on a table that doesn't exist works without raising
        an error
        """
        orm_class = self.get_orm_class()
        o = orm_class(foo=1, bar="value 1")
        o2 = o.query.one()
        # we succeeded if no error was raised

    def test_fk(self):
        mpath = testdata.create_module([
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

        Foo = mpath.module().Foo
        Bar = mpath.module().Bar
        Che = mpath.module().Che
        Boo = mpath.module().Boo

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
        Foo = self.get_orm_class()
        Bar = self.get_orm_class(
            foo_id=Field(Foo, True),
        )

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
        class BarQuery(Query): pass
        Foo = self.get_orm_class()
        Bar = self.get_orm_class(
            foo_id=Field(Foo, True),
            query_class=BarQuery,
        )

        foo_ids = self.insert(Foo, count)
        for foo_id in foo_ids:
            Bar.create(foo_id=foo_id)

        q = Bar.query.in_foo_id(Foo.query.select_pk())
        self.assertEqual(count, len(q.get()))

    def test_upsert_1(self):
        orm_class = self.get_orm_class(
            foo=Field(str, True),
            bar=Field(str, True),
            che=Field(str, True, default=""),
            baz=Field(int, True),
            foo_bar_che=Index("foo", "bar", "che", unique=True),
        )

        o = orm_class({"foo": "1", "bar": "1", "che": "1", "baz": 1})
        o.upsert()
        o2 = orm_class.query.eq_pk(o.pk).one()
        self.assertEqual(o.baz, o2.baz)
        self.assertEqual(o.pk, o2.pk)

        # we only want to upsert on specific occasions where we know we've set
        # the conflict values
        with self.assertRaises(ValueError):
            o2.baz = 2
            o2.upsert()

        o3 = orm_class({"foo": "1", "bar": "1", "che": "1", "baz": 2})
        o3.upsert()
        self.assertEqual(o.pk, o3.pk)
        o4 = orm_class.query.eq_pk(o3.pk).one()
        self.assertEqual(o3.baz, o4.baz)

    def test_upsert_nochange(self):
        orm_class = self.get_orm_class(
            foo=Field(int, True),
            bar=Field(int, True),
            upsert_index=Index("foo", "bar", unique=True),
        )

        o = orm_class(foo=1, bar=1)
        o.upsert()

        with self.assertRaises(ValueError):
            o.upsert()

    def test_load(self):
        orm_class = self.get_orm_class(
            foo=Field(int, True),
            bar=Field(int, True),
            che=Field(int, True),
            baz=Field(int, True),
            che_baz=Index("che", "baz", unique=True),
            _updated=None,
        )

        o = orm_class(foo=1, che=1, baz=1)

        self.assertFalse(o.load())

        o.bar = 1
        o.save()

        o2 = orm_class(che=1, baz=1)
        self.assertTrue(o2.load())
        self.assertEqual(o.pk, o2.pk)
        for field_name, field_value in o.fields.items():
            self.assertEqual(field_value, getattr(o2, field_name), field_name)

        o2.save()
        for field_name, field_value in o.fields.items():
            self.assertEqual(field_value, getattr(o2, field_name), field_name)

        o3 = orm_class(pk=o2.pk)
        o3.load()
        for field_name, field_value in o.fields.items():
            self.assertEqual(field_value, getattr(o3, field_name), field_name)

