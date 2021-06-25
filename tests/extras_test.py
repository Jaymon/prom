# -*- coding: utf-8 -*-
from __future__ import unicode_literals, division, print_function, absolute_import

from datatypes import Enum

from prom.extras.config import Field
from prom.extras.model import MagicOrm
from . import TestCase, EnvironTestCase, testdata


class MagicOrmTest(EnvironTestCase):
    def test_aliases_1(self):
        class Foo(MagicOrm):
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

    def create_1(self, **kwargs):
        class O1(MagicOrm):
            table_name = self.get_table_name("o1_magicorm")
            interface = self.get_interface()
            bar = Field(bool)
            che = Field(str)

        return O1(**kwargs)

    def create_2(self, **kwargs):
        o1 = self.create_1()
        class O2(MagicOrm):
            table_name = self.get_table_name("o2_magicorm")
            interface = self.get_interface()
            o1_id = Field(o1.__class__)

        return O2(**kwargs)

    def test_pk(self):
        o = self.create_1(_id=200)
        self.assertEqual(200, o.o1_id)

    def test_is(self):
        o = self.create_1(bar=True, che="che")

        self.assertTrue(o.is_bar())
        self.assertTrue(o.is_che("che"))
        self.assertFalse(o.is_che("bar"))

        o.bar = False
        self.assertFalse(o.is_bar())

    def test_fk(self):
        o1 = self.create_1(bar=False, che="1")
        o1.save()
        self.assertLess(0, o1.pk)
        self.assertFalse(o1.bar)

        o2 = self.create_2(o1_id=o1.pk)

        r1 = o2.o1
        self.assertEqual(o1.pk, r1.pk)
        self.assertEqual(o1.bar, r1.bar)
        self.assertEqual(o1.che, r1.che)

    def test_jsonable(self):
        o = self.create_1(_id=500, bar=False, che="1")
        d = o.jsonable()
        self.assertTrue(o.pk_name in d)
        self.assertFalse("_id" in d)

    def test_attribute_error(self):
        o = self.create_2()
        self.assertIsNone(o.o1)
        with self.assertRaises(AttributeError):
            o.blahblah

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

        class OE(MagicOrm):
            type = Field(FooEnum)

        o = OE.create(type="FOO")
        f = o.schema.fields["type"]
        self.assertEqual(FooEnum.FOO.value, o.type)
        self.assertFalse(f.is_serialized())
        self.assertTrue(issubclass(f.interface_type, int))

