# -*- coding: utf-8 -*-
import os
import datetime
import decimal
import math
from typing import Self

from datatypes import Enum

from prom.compat import *
import prom
from prom.model import Orm
from prom.config import (
    Schema,
    Connection,
    DsnConnection,
    Index,
    Field,
)

from . import IsolatedAsyncioTestCase, EnvironTestCase


class SchemaTest(IsolatedAsyncioTestCase):
    def test_field_index_property(self):
        s = self.get_schema(
            foo=Field(str, index=True)
        )
        self.assertTrue("foo", s.indexes)
        self.assertFalse(s.indexes["foo"].unique)

    def test___init__(self):
        """I had set the class .fields and .indexes attributes to {} instead of
        None, so you could only ever create one instance of Schema, this test
        makes sure that's been fixed
        """
        s = Schema("foo")
        self.assertTrue(isinstance(s.fields, dict))
        self.assertTrue(isinstance(s.indexes, dict))

        s2 = Schema("bar")
        self.assertTrue(isinstance(s.fields, dict))
        self.assertTrue(isinstance(s.indexes, dict))

        s = Schema(
            "foo",
            bar=Field(int),
            che=Field(str, True),
            barche=Index("bar", "che")
        )
        self.assertTrue("bar" in s.fields)
        self.assertTrue("che" in s.fields)
        self.assertTrue("barche" in s.indexes)

    def test___getattr__(self):
        s = Schema("foo")

        with self.assertRaises(AttributeError):
            s.foo

        s.set_field("foo", Field(int, True))
        self.assertTrue(isinstance(s.foo, Field))

    def test_set_field(self):
        s = Schema("foo")

        with self.assertRaises(ValueError):
            s.set_field("", int)

        with self.assertRaises(ValueError):
            s.set_field("foo", "bogus")

        s.set_field("foo", Field(int))
        with self.assertRaises(ValueError):
            s.set_field("foo", int)

        s = Schema("foo")
        s.set_field("foo", Field(int, unique=True))
        self.assertTrue("foo" in s.fields)
        self.assertTrue("foo" in s.indexes)

        s = Schema("foo")
        s.set_field("foo", Field(int, ignore_case=True))
        self.assertTrue(s.foo.options["ignore_case"])

    def test_set_index(self):
        s = Schema("foo")
        s.set_field("bar", Field(int, True))
        s.set_field("che", Field(str))

        with self.assertRaises(ValueError):
            s.set_index("foo", Index())

        with self.assertRaises(ValueError):
            s.set_index("", Index("bar", "che"))

        s.set_index("bar_che", Index("che", "bar"))
        with self.assertRaises(ValueError):
            s.set_index("bar_che", Index("che", "bar"))

        s.set_index("testing", Index("che", unique=True))
        self.assertTrue(s.indexes["testing"].unique)

    def test_aliases_1(self):
        s = self.get_schema(
            foo=Field(int, aliases=["bar", "che"])
        )

        self.assertEqual(s.pk, s._id)
        self.assertEqual(s.foo, s.bar)
        self.assertEqual(s.foo, s.che)

        with self.assertRaises(AttributeError):
            s.faosdfjadkfljlk_does_not_exist

    def test_aliases_primary_key(self):
        s = self.get_schema()
        self.assertEqual(s._id, s.pk)

    def test_aliases_created_updated(self):
        orm_class = self.get_orm_class()
        s = orm_class.schema

        self.assertEqual(s._created, s.created)
        self.assertEqual(s._updated, s.updated)

        q = orm_class.query.lte_created(datetime.datetime.utcnow())
        self.assertTrue("_created" in q.fields_where)

        q = orm_class.query.lte_updated(datetime.datetime.utcnow())
        self.assertTrue("_updated" in q.fields_where)

    def test_field_name(self):
        s = self.get_schema(_id=None)
        with self.assertRaises(AttributeError):
            s.field_name("bogus")

        r = s.field_name("bogus", None)
        self.assertIsNone(r)

    def test_field_model_name(self):
        o1_class = self.get_orm_class(
            bar=Field(str),
        )
        o2_class = self.get_orm_class(
            o1_id=Field(o1_class),
        )

        field_name = o2_class.schema.field_model_name(o1_class.model_name)
        self.assertEqual("o1_id", field_name)

    def test_persisted_fields(self):
        s = self.get_schema(
            foo=Field(str, persist=False),
            bar=Field(str),
            che=Field(int, persist=False),
        )

        pfields = s.persisted_fields
        self.assertTrue("bar" in pfields)
        for fn in ["foo", "che"]:
            self.assertFalse(fn in pfields)


class DsnConnectionTest(IsolatedAsyncioTestCase):
    """Any general tests should go here and always use sqlite because that's
    installed with python. If you have SQLite or PostgreSQL specific connection
    config tests those should go into the appropriate insterface ConfigTest
    """
    def test_environ(self):
        os.environ['PROM_DSN'] = "prom.interface.sqlite.SQLite://:memory:#i0"
        os.environ['PROM_DSN_1'] = "SQLite://:memory:#i1"
        os.environ['PROM_DSN_2'] = "sqlite://:memory:#i2"
        os.environ['PROM_DSN_4'] = "sqlite://:memory:#i4"
        prom.configure_environ()
        self.assertTrue('i0' in prom.get_interfaces())
        self.assertTrue('i1' in prom.get_interfaces())
        self.assertTrue('i2' in prom.get_interfaces())
        self.assertTrue('i3' not in prom.get_interfaces())
        self.assertTrue('i4' not in prom.get_interfaces())

        prom.interface.interfaces.pop('i0', None)
        prom.interface.interfaces.pop('i1', None)
        prom.interface.interfaces.pop('i2', None)
        prom.interface.interfaces.pop('i3', None)
        prom.interface.interfaces.pop('i4', None)

    def test_dsn_options_type(self):
        dsn = "prom.interface.sqlite.SQLite:///tmp/sqlite.db?timeout=20.0"
        c = DsnConnection(dsn)
        self.assertTrue(isinstance(c.options["timeout"], float))

    def test_readonly(self):
        dsn = "SQLite:///tmp/sqlite.db?readonly=1"
        c = DsnConnection(dsn)
        self.assertTrue(c.readonly)

    def test_bad_classpath(self):
        dsn = 'bogus.earaskdfaksfk.Interface://host/dbname#bogus2'
        with self.assertRaises(ImportError):
            DsnConnection(dsn)

        dsn = 'bogus.earaskdfaksfk.Interface://host/dbname'
        with self.assertRaises(ImportError):
            DsnConnection(dsn)

        dsn = 'bogus.earaskdfaksfk.Interface://host/dbname#bogus1'
        with self.assertRaises(ImportError):
            DsnConnection(dsn)


class ConnectionTest(IsolatedAsyncioTestCase):
    def test_interface_is_unique_each_time(self):
        c = Connection(
            interface_name="prom.interface.sqlite.SQLite",
            database=":memory:",
        )

        iids = set()
        inters = set()
        for x in range(10):
            inter = c.interface
            iid = id(inter)
            self.assertFalse(iid in iids)
            iids.add(iid)
            inters.add(inter)

    def test___init__(self):
        c = Connection(
            interface_name="prom.interface.sqlite.SQLite",
            database="dbname",
            port=5000,
            some_random_thing="foo"
        )

        self.assertEqual(5000, c.port)
        self.assertEqual("dbname", c.database)
        self.assertEqual({"some_random_thing": "foo"}, c.options)


class FieldTest(EnvironTestCase):
    async def test_modified_pk(self):
        orm_class = self.get_orm_class()
        o = orm_class(foo=1, bar="2")
        f = o.schema.pk

        im = f.modified(o, 100)
        self.assertTrue(im)

        im = f.modified(o, None)
        self.assertFalse(im)

        await o.save()

        im = f.modified(o, None)
        self.assertTrue(im)

        im = f.modified(o, o.pk + 1)
        self.assertTrue(im)

        im = f.modified(o, o.pk)
        self.assertFalse(im)

    def test_doc(self):
        help_str = "this is the foo field"
        orm_class = self.get_orm_class(
            foo=Field(int, doc=help_str)
        )

        f = orm_class.schema.foo
        self.assertEqual(help_str, f.doc)

    def test_type_std(self):
        std_types = (
            str,
            bytes,
            bool,
            long,
            int,
            float,
            bytearray,
            decimal.Decimal,
            datetime.datetime,
            datetime.date,
        )

        for field_type in std_types:
            f = Field(field_type)
            self.assertEqual(field_type, f.type)
            self.assertEqual(field_type, f.interface_type)
            self.assertIsNone(f.schema)
            self.assertFalse(f.is_serialized())

    def test_type_dict(self):
            f = Field(dict)
            self.assertEqual(dict, f.original_type)
            self.assertEqual(dict, f.interface_type)
            self.assertEqual(dict, f.type)
            self.assertIsNone(f.schema)
            self.assertFalse(f.is_serialized())

    def test_type_pickle(self):
        class Foo(object): pass
        pickle_types = (
            list,
            set,
            Foo,
        )

        for field_type in pickle_types:
            f = Field(field_type)
            self.assertEqual(field_type, f.original_type)
            self.assertEqual(bytes, f.interface_type)
            self.assertEqual(bytes, f.type)
            self.assertIsNone(f.schema)
            self.assertTrue(f.is_serialized())

    def test_type_fk(self):
        orm_class = self.get_orm_class()

        f = Field(orm_class)
        self.assertEqual(orm_class, f.original_type)
        self.assertEqual(long, f.interface_type)
        self.assertEqual(long, f.type)
        self.assertIsNotNone(f.schema)
        self.assertFalse(f.is_serialized())

    async def test_type_fk_get(self):
        """ https://github.com/Jaymon/prom/issues/145 """
        class PrimaryKey(Field):
            def __init__(self):
                super().__init__(str, pk=True)
            def iset(self, orm, v):
                return str(v)
            def iget(self, orm, v):
                return int(v)

        foo_class = self.get_orm_class(
            _id=PrimaryKey(),
            _created=None,
            _updated=None
        )
        bar_class = self.get_orm_class(
            foo_id=Field(foo_class),
            _created=None,
            _updated=None
        )

        f = await foo_class.create(pk=1)
        b = await bar_class.create(foo_id=f.pk)

        foo_pk = await bar_class.query.select_foo_id().eq_pk(b.pk).one()
        self.assertTrue(isinstance(foo_pk, int))

    async def test_serialize_lifecycle(self):
        orm_class = self.get_orm_class(
            foo=Field(dict, False)
        )

        o = orm_class()
        self.assertIsNone(o.foo)

        o.foo = {"bar": 1, "che": "two"}
        self.assertTrue(isinstance(o.foo, dict))
        await o.save()
        self.assertTrue(isinstance(o.foo, dict))

        o2 = await o.query.eq_pk(o.pk).one()
        self.assertTrue(isinstance(o2.foo, dict))
        self.assertEqual(1, o2.foo["bar"])
        self.assertEqual("two", o2.foo["che"])

    def test_choices(self):
        orm_class = self.get_orm_class(
            foo=Field(int, choices=set([1, 2, 3]))
        )
        o = orm_class()

        for x in range(1, 4):
            o.foo = x
            self.assertEqual(x, o.foo)

        o.foo = 1
        with self.assertRaises(ValueError):
            o.foo = 4
        self.assertEqual(1, o.foo)

        o.foo = None
        self.assertEqual(None, o.foo)

    def test_iget(self):
        orm_class = self.get_orm_class(
            foo=Field(int, iget=lambda o, v: bool(v))
        )

        o = orm_class()

        o.from_interface({"foo": 1})
        self.assertTrue(o.foo)
        self.assertTrue(isinstance(o.foo, bool))

    async def test_iget_really_big_int(self):
        """Postgres will return Decimal for a really big int, SQLite will return
        an int. I'm not sure it's worth slowing down every select statement to
        check if an int is a decimal since this is such a rare use case but this
        test is here if I ever do want to address it

        https://github.com/Jaymon/prom/issues/162
        """
        orm_class = self.get_orm_class(
            foo=Field(int, True, precision=78)
        )
        o = await orm_class.create(foo=int("9" * 77))
        o2 = await orm_class.query.eq_pk(o.pk).one()
        self.assertTrue(isinstance(o2.foo, (int, decimal.Decimal)))

    def test_iset(self):
        dt = datetime.datetime.utcnow()
        orm_class = self.get_orm_class(
            foo=Field(int, iset=lambda o, v: datetime.datetime.utcnow())
        )

        o = orm_class()
        self.assertIsNone(o.foo)

        fields = o.to_interface()
        self.assertLess(dt, fields["foo"])

        fields2 = o.to_interface()
        self.assertLess(fields["foo"], fields2["foo"])

    def test_qset(self):
        class IqueryOrm(Orm):
            foo = Field(int)

            @foo.qsetter
            def foo(query_field, v):
                return 10

        q = IqueryOrm.query.eq_foo("foo")
        self.assertEqual(10, q.fields_where[0].value)

    def test_datetime_jsonable_1(self):
        orm_class = self.get_orm_class(
            foo=Field(datetime.datetime)
        )

        o = orm_class()
        o.foo = self.get_past_datetime()
        r = o.jsonable()
        self.assertTrue("foo" in r)

    def test_get_default(self):
        class FDefaultOrm(Orm):
            foo = Field(int, default=0)
            bar = Field(int)

        o = FDefaultOrm()
        foo = o.schema.foo
        self.assertEqual(0, foo.get_default(o, None))
        self.assertEqual(0, o.foo)

        bar = o.schema.bar
        self.assertEqual(None, bar.get_default(o, None))
        self.assertEqual(None, o.bar)

    async def test_fcrud(self):
        class FCrudOrm(Orm):
            interface = self.get_interface()
            foo = Field(int)

            @foo.fsetter
            def foo(self, v):
                return 0 if v is None else v

            @foo.fgetter
            def foo(self, v):
                ret = None if v is None else v + 1
                return ret

            @foo.fdeleter
            def foo(self, val):
                return None

        o = FCrudOrm(foo=0)

        self.assertEqual(1, o.foo)
        self.assertEqual(1, o.foo)

        await o.save()
        self.assertEqual(2, o.foo)
        self.assertEqual(2, o.foo)

        del o.foo
        self.assertEqual(None, o.foo)
        await o.save()
        self.assertEqual(None, o.foo)

        o.foo = 10
        self.assertEqual(11, o.foo)
        self.assertEqual(11, o.foo)

        o.foo = None
        self.assertEqual(1, o.foo)

    async def test_icrud(self):
        class ICrudOrm(Orm):
            interface = self.get_interface()
            foo = Field(int)

            @foo.isetter
            def foo(self, v):
                if self.is_update():
                    v = v - 1
                else:
                    v = 0
                return v

            @foo.igetter
            def foo(self, v):
                if v > 1:
                    v = v * 100
                return v

        o = ICrudOrm()
        self.assertEqual(None, o.foo)

        await o.save()
        self.assertEqual(0, o.foo)

        o.foo = 5
        self.assertEqual(5, o.foo)

        await o.save()
        self.assertEqual(400, o.foo)

        o2 = await o.query.eq_pk(o.pk).one()
        self.assertEqual(400, o2.foo)

    def test_fdel(self):
        orm_class = self.get_orm_class()

        o = orm_class()

        self.assertFalse(o.schema.fields["foo"].modified(o, o.foo))

        o.foo = 1
        self.assertTrue(o.schema.fields["foo"].modified(o, o.foo))

        del o.foo
        self.assertFalse(o.schema.fields["foo"].modified(o, o.foo))

        with self.assertRaises(KeyError):
            o.to_interface()

    def test_fdeleter(self):
        class FDOrm(Orm):
            foo = Field(int)

            @foo.fdeleter
            def foo(self, val):
                return None

        o = FDOrm()
        o.foo = 1
        del o.foo
        self.assertEqual(None, o.foo)

    def test_property(self):
        class FieldPropertyOrm(Orm):
            foo = Field(int)

            @foo.fgetter
            def foo(self, val):
                return val

            @foo.fsetter
            def foo(self, val):
                return int(val) + 10 if (val is not None) else val

        o = FieldPropertyOrm()

        o.foo = 1
        self.assertEqual(11, o.foo)

        o.foo = 2
        self.assertEqual(12, o.foo)

        o.foo = None
        self.assertEqual(None, o.foo)

    def test_ref(self):
        m = self.create_module([
            "import prom",
            "class Foo(prom.Orm):",
            "    che = prom.Field(str)",
            "",
            "class Bar(prom.Orm):",
            "    foo_id = prom.Field(Foo)",
            ""
        ])

        Foo = m.module().Foo
        Bar = m.module().Bar

        self.assertTrue(
            isinstance(Bar.schema.fields['foo_id'].schema, Schema)
        )
        self.assertTrue(
            issubclass(Bar.schema.fields['foo_id'].interface_type, long)
        )

    def test_string_ref(self):
        modname = self.get_module_name()
        d = self.create_modules({
            "foo": [
                "import prom",
                "class Foo(prom.Orm):",
                "    interface = None",
                "    bar_id = prom.Field('{}.bar.Bar')".format(modname),
                ""
            ],
            "bar": [
                "import prom",
                "class Bar(prom.Orm):",
                "    interface = None",
                "    foo_id = prom.Field('{}.foo.Foo')".format(modname),
                ""
            ],
        }, modname)

        Foo = d.module("{}.foo".format(modname)).Foo
        Bar = d.module("{}.bar".format(modname)).Bar

        self.assertTrue(isinstance(Foo.schema.fields['bar_id'].schema, Schema))
        self.assertTrue(
            issubclass(Foo.schema.fields['bar_id'].interface_type, long)
        )
        self.assertTrue(isinstance(Bar.schema.fields['foo_id'].schema, Schema))
        self.assertTrue(
            issubclass(Bar.schema.fields['foo_id'].interface_type, long)
        )

    def test___init__(self):
        f = Field(str, True)
        self.assertTrue(f.required)
        self.assertTrue(issubclass(f.type, str))

        with self.assertRaises(TypeError):
            f = Field()

        f = Field(int, max_length=100)
        self.assertTrue(issubclass(f.type, int))
        self.assertEqual(f.options['max_length'], 100)

    def test___set_name___instance(self):
        class FooSN(Orm):
            bar = Field(str)
            _created = None
            _updated = None
            _id = None

        o = FooSN(bar="bar")
        self.assertEqual("bar", o.bar)

    def test___set_name___class(self):
        class FooSN(Orm):
            class bar(Field):
                type = str
            _created = None
            _updated = None
            _id = None

        o = FooSN(bar="bar")
        self.assertEqual("bar", o.bar)

    def test_size_info_1(self):
        """Makes sure sizes get set correctly, I've evidently had a bug in this
        for years (1-26-2023)"""
        f = Field(int, True, max_size=100)
        self.assertEqual(100, f.options["max_size"])
        r = f.size_info()
        self.assertEqual((0, 100), r["bounds"])

        f = Field(int, True, size=100)
        self.assertEqual(100, f.options["size"])
        r = f.size_info()
        self.assertEqual((100, 100), r["bounds"])

        f = Field(int, True, size=100)
        self.assertEqual(100, f.options["size"])

        f = Field(int, True, size=100, max_size=500)
        self.assertEqual(100, f.options["size"])
        self.assertFalse("max_size" in f.options)

        f = Field(int, True, min_size=100)
        self.assertEqual(100, f.options["min_size"])

        f = Field(int, True, min_size=100, max_size=500)
        self.assertEqual(100, f.options["min_size"])
        self.assertEqual(500, f.options["max_size"])

    def test_size_info_2(self):
        # A field with precision 65, scale 30 must round to an absolute value
        # less than 10^35
        f = Field(float, precision=65, scale=30)
        r = f.size_info()
        self.assertLessEqual(r["size"], math.pow(10, 35))

        f = Field(str, size=32)
        r = f.size_info()
        self.assertLessEqual(32, r["size"])
        self.assertLessEqual(32, r["precision"])

        f = Field(int)
        r = f.size_info()
        self.assertLessEqual(2147483647, r["size"])

        f = Field(float, size="15.6")
        r = f.size_info()
        self.assertTrue(r["has_size"])
        self.assertEqual(21, r["precision"])
        self.assertEqual(6, r["scale"])

        f = Field(float, precision=78, scale=18)
        r = f.size_info()
        self.assertFalse(r["has_size"])
        self.assertTrue(r["has_precision"])

        f = Field(int, size=100)
        r = f.size_info()
        self.assertTrue(r["has_size"])
        self.assertEqual(100, r["size"])

        f = Field(int, precision=78)
        r = f.size_info()
        self.assertEqual(int("9" * 78), r["size"])

    def test_size_info_2(self):
        f = Field(int, True, max_size=int("9" * 78))
        r = f.size_info()
        self.assertEqual(78, r["precision"])

    def test_regex(self):
        orm_class = self.get_orm_class(
            foo=Field(str, True, regex=r"^[abcd]+$")
        )

        with self.assertRaises(ValueError):
            orm_class(foo="aazabbbbbddddd")

        o = orm_class(foo="aaaabbbbbddddd")
        self.assertTrue(o.foo)

    async def test_persist(self):
        orm_class = self.get_orm_class(
            foo=Field(str, True, persist=False),
        )

        o = orm_class(foo="foo value")
        fields = o.to_interface()
        self.assertFalse("foo" in fields)

    async def test_private(self):
        f = Field(str, private=True)
        self.assertTrue(f.is_private())
        self.assertFalse(f.is_jsonable())

        f = Field(str, private=False)
        self.assertFalse(f.is_private())
        self.assertTrue(f.is_jsonable())

    async def test_jsonable(self):
        orm_class = self.get_orm_class(
            foo=Field(int, private=True),
        )
        o = orm_class(pk=1234, foo="foo value")
        fields = o.jsonable()
        self.assertFalse("foo" in fields)

    async def test_self_type(self):
        """Fields can use `typing.Self` to create a foreign key to the
        same model"""
        orm_class = self.get_orm_class(
            foo_id=Field(Self, False),
        )

        field = orm_class.schema.foo_id
        self.assertTrue(issubclass(field.type, int))
        self.assertEqual(field.schema, orm_class.schema)

        with self.assertRaises(ValueError):
            self.get_orm_class(
                foo_id=Field(Self, True),
            )

#     async def test_lifecycle_methods(self):
#         class FooField(Field):
#             def fget(self, orm, v):
#                 pout.v(f"fget {v}")
#                 return v
# 
#             def iget(self, orm, v):
#                 pout.v(f"iget {v}")
#                 return v
# 
#             def iset(self, orm, v):
#                 pout.v(f"iset {v}")
#                 return v
# 
#             def fset(self, orm, v):
#                 pout.v(f"fset {v}")
#                 return v
# 
#             def fdel(self, orm, v):
#                 pout.v(f"fdel {v}")
#                 return v
# 
#             def qset(self, query, v):
#                 pout.v(f"qset {v}")
#                 return v
# 
#             def jset(self, orm, field_name, v):
#                 pout.v(f"jset {v}")
#                 return field_name, v
# 
#         orm_class = self.get_orm_class(
#             foo=FooField(int, True)
#         )
# 
#         o = orm_class(foo=1)
# 
#         pout.b("save")
#         pout.v(o.modified_field_names, o.modified_fields)
#         await o.save()
#         pout.v(o.modified_field_names)
#         return
# 
#         pout.b("query")
#         o2 = await orm_class.query.eq_foo(1).one()
#         pout.v(o2.modified_field_names, o.modified_fields)


class SerializedFieldTest(EnvironTestCase):
    def get_orm(self, field_type=dict, default=None, default_factory=None):
        orm_class = self.get_orm_class(
            body=Field(
                field_type,
                default=default,
                default_factory=default_factory,
            )
        )
        return orm_class()

    def test_default(self):
        o = self.get_orm(default_factory=dict)
        o.body["foo"] = 1
        self.assertEqual(1, o.body["foo"])

    async def test_imethods_pickle(self):
        o = self.get_orm()
        o.body = {"foo": 1}
        await o.save()

        o2 = await o.requery()
        self.assertEqual(o.body, o2.body)

    async def test_modify(self):
        o = self.get_orm()
        o.body = {"bar": 1}
        await o.save()

        o.body["che"] = 2
        await o.save()

        o2 = await o.requery()
        self.assertEqual(o.body, o2.body)

    async def test_other_types(self):
        types = (
            list,
            set
        )

        for field_type in types:
            o = self.get_orm(field_type)
            o.body = field_type(range(100))
            await o.save()

            o2 = await o.requery()
            self.assertEqual(o.body, o2.body)


class EnumFieldTest(EnvironTestCase):
    """Moved over and integrated from extras.config on 1-24-2024"""
    def test_enum(self):
        class FooEnum(Enum):
            FOO = 1
            BAR = 2

        OE = self.get_orm_class(type=Field(FooEnum))

        o = OE()
        o.type = "bar"
        self.assertEqual(FooEnum.BAR, o.type)

        q = o.query.eq_type("foo")
        self.assertEqual(FooEnum.FOO, q.fields_where[0].value)
        self.assertEqual(1, q.fields_where[0].value)

        o.type = 2
        self.assertEqual(FooEnum.BAR, o.type)
        self.assertEqual(2, o.type)

        o.type = FooEnum.BAR
        self.assertEqual(FooEnum.BAR, o.type)
        self.assertEqual(2, o.type)

    async def test_save(self):
        class FooEnum(Enum):
            FOO = 1

        OE = self.get_orm_class(type=Field(FooEnum))

        o = await OE.create(type="FOO")
        f = o.schema.fields["type"]
        self.assertEqual(FooEnum.FOO.value, o.type)
        self.assertFalse(f.is_serialized())
        self.assertTrue(issubclass(f.interface_type, int))

