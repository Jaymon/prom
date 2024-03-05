# -*- coding: utf-8 -*-

from prom.model import Orm
from prom.config import Field
from prom.extras.model import MagicOrm
from . import IsolatedAsyncioTestCase, EnvironTestCase


class MagicOrmTest(EnvironTestCase):
    async def create_1(self, **kwargs):
        o1_class = self.get_orm_class(
            bar = Field(bool),
            che = Field(str),
            model_name="o1",
            parent_class=MagicOrm,
        )
        return await self.insert_orm(o1_class, **kwargs)

    async def create_2(self, **kwargs):
        o1 = self.create_1()
        o2_class = self.get_orm_class(
            o1_id=Field(type(o1)),
            parent_class=MagicOrm,
        )
        return await self.insert_orm(o2_class, **kwargs)

    async def test_is(self):
        o = await self.create_1(bar=True, che="che")

        self.assertTrue(o.is_bar())
        self.assertTrue(o.is_che("che"))
        self.assertFalse(o.is_che("bar"))

        o.bar = False
        self.assertFalse(o.is_bar())

    async def test_jsonable(self):
        o = await self.create_1(_id=500, bar=False, che="1")
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


class ModelDataTest(IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        # we import this here because it has .get_orm_class and .get_orm
        # methods that will stomp the normal ones
        from prom.extras.testdata import ModelData

    @classmethod
    def tearDownClass(cls):
        # we remove ModelData here so it doesn't poison other tests
        from prom.extras.testdata import ModelData
        cls.data.delete_class(ModelData)

    async def test_references_1(self):
        testdata = self.InterfaceData
        ref_class = testdata.get_orm_class()
        orm_class = testdata.get_orm_class(ref_id=Field(ref_class, True))

        self.assertEqual(0, await ref_class.query.count())

        orm = await self.get_orm(orm_class, ignore_refs=False)

        self.assertEqual(1, await ref_class.query.count())

    async def test_references_2(self):
        count = 2
        testdata = self.InterfaceData
        foo_class = testdata.get_orm_class(model_name="foo")
        bar_class = testdata.get_orm_class(foo_id=Field(foo_class))

        foo = await self.insert_orm(foo_class)

        bars = await self.get_orms(
            bar_class,
            count=count,
            foo=foo,
            related_refs=False
        )

        bcount = 0
        for b in bars:
            self.assertEqual(foo.pk, b.foo_id)
            bcount += 1
        self.assertEqual(2, bcount)

    async def test_internal_call(self):
        """
        https://github.com/Jaymon/prom/issues/166
        """
        modpath = self.create_module([
            "from prom import Orm, Field",
            "",
            "class Foo(Orm):",
            "    one = Field(str)",
            "",
            "class Bar(Orm):",
            "    two = Field(str)",
        ], load=True)

        did_run = {}

        class _OtherData(self.ModelData.__class__):
            async def get_foo_fields(s, **kwargs):
                pout.h()
                bar = await s.get_bar(**kwargs)
                self.assertEqual("Bar", bar.__class__.__name__)
                self.assertNotIsInstance(bar, kwargs["orm_class"])
                did_run["get_foo_fields"] = True

        d = _OtherData()
        foo = await d.get_foo() # this method runs the asserts
        self.assertTrue(did_run["get_foo_fields"])

    async def test___getattribute__1(self):
        """
        https://github.com/Jaymon/prom/issues/166
        """
        modpath = self.create_module([
            "from prom import Orm, Field",
            "",
            "class Foo(Orm):",
            "    one = Field(str)",
            "",
            "class Bar(Orm):",
            "    two = Field(str)",
        ], load=True)

        did_run = {}

        class _OtherData(self.ModelData.__class__):
            async def get_foo_fields(self, **kwargs):
                did_run["get_foo_fields"] = True
                return await super().get_orm_fields(**kwargs)

        d = _OtherData()
        r = await d.get_foo_fields()
        self.assertTrue(did_run["get_foo_fields"])

    async def test___getattribute__2(self):
        modpath = self.create_module([
            "from prom import Orm, Field",
            "",
            "class Foo(Orm):",
            "    one = Field(str)",
            "",
            "class Bar(Orm):",
            "    foo_id = Field(Foo)",
        ], load=True)

        class _OtherData(self.ModelData.__class__):
            async def get_foo(self, **kwargs):
                f = await self.get_orm(**kwargs)
                b = await self.get_bar(**kwargs)
                f.get_foo_get_bar = b
                return f

            async def get_bar(self, **kwargs):
                return await self.get_orm(**kwargs)

        d = _OtherData()

        f = await d.get_foo()
        self.assertEqual("foo", f.model_name)
        self.assertEqual("bar", f.get_foo_get_bar.model_name)

    async def test___getattribute__3(self):
        modpath = self.create_module([
            "from prom import Orm, Field",
            "",
            "class Foo(Orm):",
            "    one = Field(str)",
        ], load=True)

        f = await self.get_foo()
        self.assertEqual("foo", f.model_name)

    def test__parse_method_name(self):
        modpath = self.create_module([
            "from prom import Orm, Field",
            "",
            "class Foo(Orm):",
            "    pass",
        ], load=True)

        class _OtherData(self.ModelData.__class__):
            """Override to make it easier to test ._parse_method_names in
            isolation"""
            def __getattr__(self, k):
                return super().__getattr__(k)

            def __getattribute__(self, k):
                return super().__getattribute__(k)

        d = _OtherData()

        r = d._parse_method_name("create_foo_instance")
        self.assertEqual("foo", r[0].model_name)
        self.assertEqual("create_orm_instance", r[1].__name__)

        r = d._parse_method_name("get_foos")
        self.assertEqual("Foo", r[0].__name__)
        self.assertEqual("get_orms", r[1].__name__)
        self.assertEqual("foo", r[0].model_name)

        r = d._parse_method_name("foobarche")
        self.assertIsNone(r[0])
        self.assertIsNone(r[1])

        r = d._parse_method_name("get_orm_fields")
        self.assertIsNone(r[0])
        self.assertIsNone(r[1])

    async def test_children_create(self):
        """
        https://github.com/Jaymon/prom/issues/166
        """
        modpath = self.create_module([
            "from prom import Orm, Field",
            "",
            "class Foo(Orm):",
            "    one = Field(str)",
            "",
            "class Bar(Orm):",
            "    foo_id = Field(Foo, True)",
            "    two = Field(str)",
        ])
        m = modpath.module()
        m.Foo.interface = self.get_interface()
        m.Bar.interface = m.Foo.interface

        count = 2

        foo = await self.create_foo(bar_count=count)
        bar_count = 0
        async for b in await foo.bars:
            bar_count += 1
            self.assertIsInstance(b, m.Bar)
            self.assertEqual(foo.pk, b.foo_id)
        self.assertEqual(count, bar_count)

        foo = await self.create_foo(bars_count=count)
        bar_count = 0
        async for b in await foo.bars:
            bar_count += 1
            self.assertIsInstance(b, m.Bar)
            self.assertEqual(foo.pk, b.foo_id)
        self.assertEqual(count, bar_count)

    def test__gets_count(self):
        testdata = self.InterfaceData
        orm_class = testdata.get_orm_class()

        modeldata = testdata.ModelData
        self.assertEqual(
            2,
            modeldata._gets_count(
                orm_class,
                **{f"{orm_class.model_name}_count": 2}
            )
        )
        self.assertEqual(
            4,
            modeldata._gets_count(
                orm_class,
                **{f"{orm_class.models_name}_count": 4}
            )
        )
        self.assertEqual(
            5,
            modeldata._gets_count(orm_class, **{"count": 5})
        )

    async def test__dispatch_method(self):
        testdata = self.InterfaceData
        modeldata = self.ModelData
        orm_class = testdata.get_orm_class()

        fields = await modeldata._dispatch_method(
            orm_class,
            modeldata.get_orm_fields
        )
        self.assertTrue(isinstance(fields, dict))

        o = await modeldata._dispatch_method(
            orm_class,
            modeldata.get_orm
        )
        self.assertTrue(isinstance(o, orm_class))

