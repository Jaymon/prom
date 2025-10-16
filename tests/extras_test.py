# -*- coding: utf-8 -*-

from prom.config import Field
from prom.extras.testdata import ModelData
from prom.model import Orm

from . import IsolatedAsyncioTestCase


class TestCase(IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        # remove the interface data class
        #cls.data.delete_class(InterfaceData)

        # we need to specifically set the ModelData so this class can use it
        # because parent classes get rid of it because it stomps methods
        # found in .testdata
        cls.data.add_class(ModelData)


class ModelDataTest(TestCase):
    async def test_references_1(self):
        testdata = self.InterfaceData
        ref_class = testdata.get_orm_class()
        orm_class = testdata.get_orm_class(ref_id=Field(ref_class, True))

        self.assertEqual(0, await ref_class.query.count())

        md = self.ModelData
        orm = await md.get_orm(orm_class, ignore_refs=False)

        self.assertEqual(1, await ref_class.query.count())

    async def test_references_2(self):
        count = 2
        testdata = self.InterfaceData
        foo_class = testdata.get_orm_class(model_name="foo")
        bar_class = testdata.get_orm_class(foo_id=Field(foo_class))
        foo = await testdata.insert_orm(foo_class)

        md = self.ModelData
        bars = await md.get_orms(
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
                bar = await s.get_bar(**kwargs)
                self.assertEqual("Bar", bar.__class__.__name__)
                self.assertNotIsInstance(bar, kwargs["orm_class"])
                did_run["get_foo_fields"] = True
                return {}

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

    def test__parse_dispatch(self):
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

        r = d._parse_dispatch("create_foo_instance")
        self.assertEqual("foo", r[0].model_name)
        self.assertEqual("create_orm_instance", r[1].__name__)

        r = d._parse_dispatch("get_foos")
        self.assertEqual("Foo", r[0].__name__)
        self.assertEqual("get_orms", r[1].__name__)
        self.assertEqual("foo", r[0].model_name)

        r = d._parse_dispatch("foobarche")
        self.assertIsNone(r[0])
        self.assertIsNone(r[1])

        r = d._parse_dispatch("get_orm_fields")
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

    async def test__dispatch_method_1(self):
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

    async def test__dispatch_method_2(self):
        """
        https://github.com/Jaymon/prom/issues/182
        """
        modpath = self.create_module([
            "from prom import Orm",
            "",
            "class Parent(Orm):",
            "    pass",
            "",
            "class Child(Parent):",
            "    pass",
            "",
            "class GrandChild(Child):",
            "    pass",
        ], load=True)

        class _OtherData(self.ModelData.__class__):
            async def get_parent_fields(self, **kwargs):
                fields = await self.get_orm_fields(**kwargs)
                fields["properties"] = {"parent_fields": True}
                return fields

        d = _OtherData()

        pout.v(Orm.orm_classes)
        pout.v(Orm.orm_classes.finder.class_keys)

        fields = await d.get_grand_child_fields()
        self.assertTrue(fields["properties"]["parent_fields"])

        c = await d.get_child()
        self.assertTrue(c.parent_fields)

    async def test_magic_keywords(self):
        testdata = self.InterfaceData
        orm_class = testdata.get_orm_class()
        orm_fields = await testdata.get_orm_fields(
            orm_class,
            properties={"foo_prop": 1, "bar_prop": 2},
            fields={"foo_field": 3, "bar_field": 4},
        )

        self.assertTrue("foo_prop" in orm_fields["properties"])
        self.assertTrue("foo_field" in orm_fields)


class MockModelDataTest(TestCase):
    """Holds the tests specifically for the ModelData methods that add 
    orm mocking functionality"""
    async def test_get_schema(self):
        testdata = self.ModelData
        schema = testdata.get_schema()
        self.assertTrue(schema.table_name)
        self.assertTrue(schema.fields)

    async def test_get_orm_class(self):
        testdata = self.ModelData
        orm_class = testdata.get_orm_class()
        self.assertTrue(issubclass(orm_class, Orm))

    async def test_get_orm_1(self):
        testdata = self.ModelData
        orm = await testdata.get_orm()
        self.assertTrue(isinstance(orm, Orm))

    async def test_get_orms(self):
        testdata = self.ModelData
        orms = await testdata.get_orms()
        for orm in orms:
            self.assertTrue(orm.fields)

    async def test_create_orm_1(self):
        testdata = self.ModelData
        orm = await testdata.create_orm(interface=self.get_interface())
        self.assertTrue(orm.pk)

    async def test_create_orms(self):
        testdata = self.ModelData
        orms = await testdata.create_orms(interface=self.get_interface())
        for orm in orms:
            self.assertTrue(orm.pk)

    async def test_ref_type(self):
        testdata = self.ModelData
        orm_class = testdata.get_orm_class()
        ref_class = testdata.get_orm_class(
            fields={"fk_id": orm_class}
        )
        self.assertTrue(ref_class.schema.fields["fk_id"].is_ref())

    async def test_ref_instance(self):
        testdata = self.ModelData
        orm_class = testdata.get_orm_class()
        ref_class = testdata.get_orm_class(refs=[orm_class])

        o = await testdata.get_orm(ref_class)
        self.assertTrue(isinstance(o, Orm))

