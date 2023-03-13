# -*- coding: utf-8 -*-
from __future__ import unicode_literals, division, print_function, absolute_import

from datatypes import Enum

from prom.model import Orm
from prom.extras.config import Field
from prom.extras.model import MagicOrm
from prom.extras.testdata import ModelData
from . import BaseTestCase, EnvironTestCase, testdata


class MagicOrmTest(EnvironTestCase):
    def create_1(self, **kwargs):
        o1_class = self.get_orm_class(
            bar = Field(bool),
            che = Field(str),
            model_name="o1",
            parent_class=MagicOrm,
        )
        return self.insert_orm(o1_class, **kwargs)

    def create_2(self, **kwargs):
        o1 = self.create_1()
        o2_class = self.get_orm_class(
            o1_id=Field(type(o1)),
            parent_class=MagicOrm,
        )
        return self.insert_orm(o2_class, **kwargs)

    def test_is(self):
        o = self.create_1(bar=True, che="che")

        self.assertTrue(o.is_bar())
        self.assertTrue(o.is_che("che"))
        self.assertFalse(o.is_che("bar"))

        o.bar = False
        self.assertFalse(o.is_bar())

    def test_jsonable(self):
        o = self.create_1(_id=500, bar=False, che="1")
        d = o.jsonable()
        self.assertTrue(o.pk_name in d)
        self.assertFalse("_id" in d)

    def test___getattr___error(self):
        class O4(MagicOrm):
            @property
            def foo(self):
                raise KeyError("This error should not be buried")

        o = O4()
        with self.assertRaises(KeyError):
            o.foo

        class O3(MagicOrm):
            @property
            def foo(self):
                raise ValueError("This error should not be buried")

        o = O3()
        with self.assertRaises(ValueError):
            o.foo


class FieldTest(EnvironTestCase):
    def test_enum(self):
        class FooEnum(Enum):
            FOO = 1
            BAR = 2

        class OE(MagicOrm):
            type = Field(FooEnum)

        o = OE()
        o.type = "bar"
        self.assertEqual(FooEnum.BAR, o.type)

        q = o.query.is_type("foo")
        self.assertEqual(FooEnum.FOO, q.fields_where[0].value)
        self.assertEqual(1, q.fields_where[0].value)

        o.type = 2
        self.assertEqual(FooEnum.BAR, o.type)
        self.assertEqual(2, o.type)

        o.type = FooEnum.BAR
        self.assertEqual(FooEnum.BAR, o.type)
        self.assertEqual(2, o.type)

    def test_save(self):
        class FooEnum(Enum):
            FOO = 1

        OE = self.get_orm_class(type=Field(FooEnum))

        o = OE.create(type="FOO")
        f = o.schema.fields["type"]
        self.assertEqual(FooEnum.FOO.value, o.type)
        self.assertFalse(f.is_serialized())
        self.assertTrue(issubclass(f.interface_type, int))


class ModelDataTest(BaseTestCase):
    def test_ref(self):
        ref_class = self.get_orm_class()
        orm_class = self.get_orm_class(ref_id=Field(ref_class, True))

        self.assertEqual(0, ref_class.query.count())

        orm = testdata.get_orm(orm_class, ignore_refs=False)

        self.assertEqual(1, ref_class.query.count())

    def test_model_name(self):
        modpath = self.create_module([
            "from prom import Orm, Field",
            "",
            "class Foo(Orm):",
            "    bar = Field(str)",
        ])

        m = modpath.module()

        foo = self.get_foo()
        self.assertIsNotNone(foo)
        self.assertIsInstance(foo, Orm)

        class OtherData(testdata.TestData):
            def get_bar(self, *args, **kwargs):
                return self.get_foo()

        d = OtherData()
        foo2 = d.get_bar()
        self.assertTrue(type(foo) is type(foo2))

    def test_references(self):
        count = 2
        foo_class = self.get_orm_class(model_name="foo")
        bar_class = self.get_orm_class(foo_id=Field(foo_class))

        foo = self.insert_orm(foo_class)

        bars = self.get_orms(bar_class, count=count, foo=foo, related_refs=False)

        bcount = 0
        for b in bars:
            self.assertEqual(foo.pk, b.foo_id)
            bcount += 1
        self.assertEqual(2, count)

