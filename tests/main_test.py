# -*- coding: utf-8 -*-
from __future__ import unicode_literals, division, print_function, absolute_import
import os

from prom import query
from prom.model import Orm
import prom
from captain.client import Captain

from . import BaseTestCase as BTC, testdata

from prom.cli.dump import get_orm_classes, get_table_map, get_subclasses
import prom.interface


class Script(Captain):
    script_quiet = False

    def __init__(self, subcommand):
        self.cmd_prefix = "python -m prom {}".format(subcommand.replace("_", "-"))
        super(Script, self).__init__("")


class BaseTestCase(BTC):
    command = ""

#     def setUpClass(cls):
#         from unittest import SkipTest
#         raise SkipTest()

    def create_script(self, command=""):
        if not command:
            command = self.command
        s = Script(command)
        environ = s.environ

        # we want to cleanup the environment of any rogue PROM_DSNs
        environ = {r[0]: r[1] for r in environ.items() if not r[0].startswith("PROM")}

        for i, inter in enumerate(self.get_interfaces()):
            environ["PROM_DSN_{}".format(i)] = inter.connection_config.dsn

        s.environ = environ
        return s


class GenerateTest(BaseTestCase):
    command = "generate"

    def test_stdout(self):
        t1 = self.get_table()

        c = self.create_script()

        r = c.run()
        fields = [
            "_id = Field(long, True, pk=True)",
            "foo = Field(int, True)",
            "bar = Field(str, True)",
            "_updated = None",
            "_created = None",
        ]
        for field in fields:
            self.assertTrue(field in r)

    def test_outfile(self):
        t1 = self.get_table()
        base_d = testdata.create_dir()
        path_f = "{}/outfile.py".format(base_d)
        c = self.create_script()

        c.run("--out-file=\"{}\"".format(path_f))
        with open(path_f) as fp:
            r = fp.read()

        fields = [
            "_id = Field(long, True, pk=True)",
            "foo = Field(int, True)",
            "bar = Field(str, True)",
            "_updated = None",
            "_created = None",
        ]
        for field in fields:
            self.assertTrue(field in r)

    def test_everything(self):
        i = self.get_interface()
        s = self.get_schema_all(i)
        c = self.create_script()
        r = c.run()
        self.assertTrue("Field(Decimal" in r)
        self.assertTrue("Field(datetime" in r)


class DumpTest(BaseTestCase):
    command = "dump"

    def test_get_orm_classes(self):
        testdata.create_modules({
            "ormobjects.foo": "\n".join([
                "import prom",
                "class Foo(prom.Orm):",
                "    table_name = 'oo_foo'",
                "    bar = prom.Field(int)",
                ""
            ]),
            "ormobjects.bar": "\n".join([
                "import prom",
                "class Bar(prom.Orm):",
                "    table_name = 'oo_bar'",
                "    foo = prom.Field(int)",
                ""
            ]),
            "ormobjects.bar.che": "\n".join([
                "import prom",
                "class Che(prom.Orm):",
                "    table_name = 'oo_che'",
                "    baz = prom.Field(int)",
                ""
            ])
        })

        orms = get_orm_classes("ormobjects")
        self.assertEqual(3, len(orms))

        orms = get_orm_classes("ormobjects.foo")
        self.assertEqual(1, len(orms))
        self.assertEqual("Foo", orms.pop().__name__)

        #s = set(['ormobjects_foo', 'ormobjects_bar', 'ormobjects_che'])
        orms = get_orm_classes("ormobjects.foo.Foo")
        self.assertEqual(1, len(orms))
        self.assertEqual("Foo", orms.pop().__name__)


    def test_get_table_map(self):
        prom.configure("prom.interface.sqlite.SQLite://:memory:#conn1")
        prom.configure("prom.interface.sqlite.SQLite://:memory:#conn2")

        testdata.create_modules({
            "tablemap": [
                "import prom",
                "class Foo(prom.Orm):",
                "    table_name = 'oo_foo'",
                "    connection_name = 'conn1'",
                "    bar = prom.Field(int)",
                ""
            ],
            "tablemap.bam": [
                "import prom",
                "from .bar import Foo2, Baz",
                "class Bam(prom.Orm):",
                "    table_name = 'oo_bam'",
                "    connection_name = 'conn1'",
                "    baz_id = prom.Field(Baz)",
                "    foo2_id = prom.Field(Foo2)",
                "",
            ],
            "tablemap.bar": [
                "import prom",
                "class Bar(prom.Orm):",
                "    table_name = 'oo_bar'",
                "    connection_name = 'conn2'",
                "",
                "class Che(prom.Orm):",
                "    table_name = 'oo_che'",
                "    connection_name = 'conn2'",
                "    bar_id = prom.Field(Bar)",
                "",
                "class Foo2(prom.Orm):",
                "    table_name = 'oo_foo2'",
                "    connection_name = 'conn1'",
                "    bar = prom.Field(int)",
                "",
                "class Baz(prom.Orm):",
                "    table_name = 'oo_baz'",
                "    connection_name = 'conn1'",
                "    foo2_id = prom.Field(Foo2)",
            ],
        })

        tables = get_table_map(["tablemap"])
        self.assertTrue("conn1" in tables)
        self.assertTrue("conn2" in tables)
        self.assertEqual(tables["conn2"]["table_names"], ["oo_bar", "oo_che"])

        table_names = tables["conn1"]["table_names"]
        i_foo2 = table_names.index("oo_foo2")
        i_baz = table_names.index("oo_baz")
        i_bam = table_names.index("oo_bam")
        self.assertTrue(i_foo2 < i_baz < i_bam)
        self.assertEqual(4, len(table_names))
        #self.assertEqual(tables["conn1"]["table_names"], ["oo_foo2", "oo_baz", "oo_bam", "oo_foo"])

        #prom.interface.interfaces.pop("conn1")
        #prom.interface.interfaces.pop("conn2")

    def test_dump_restore(self):
        path = testdata.create_module("dumpprom", [
            "import prom",
            "class Foo(prom.Orm):",
            "    table_name = 'dp_foo'",
            "    bar = prom.Field(int)",
            ""
        ])

        self.insert(path.module.Foo, 10)

        #self.insert(_q, 10)

        c = self.create_script("dump")
        c.environ["PYTHONPATH"] += ":{}".format(path.directory)
        directory = testdata.create_dir("dbdump")
        r = c.run("--directory={} dumpprom".format(directory))
        self.assertTrue("dumping table dp_foo")

        c = self.create_script("restore")
        r = c.run("--directory={}".format(directory))
        self.assertTrue("restored table dp_foo")

    def test_get_subclasses(self):
        # this is a test for the get_subclasses function, get_subclasses was originally
        # written and put in prom, but I pulled it out since it didn't make sense to have
        # something in prom that prom didn't even use, even if I was going to use it on
        # prom modules, anyway, when I find the right home for get_subclasses() I can use
        # this test
        testdata.create_modules({
            "gs.foo": "\n".join([
                "import prom",
                "class Foo(prom.Orm):",
                "    schema=prom.Schema(",
                "        'gs_foo',",
                "        bar=prom.Field(int)",
                "    )",
                ""
            ]),
            "gs.bar": "\n".join([
                "import prom",
                "class Bar(prom.Orm):",
                "    schema=prom.Schema(",
                "        'gs_bar',",
                "        foo=prom.Field(int)",
                "    )",
                ""
            ]),
            "gs.bar.che": "\n".join([
                "import prom",
                "class Che(prom.Orm):",
                "    schema=prom.Schema(",
                "        'gs_che',",
                "        baz=prom.Field(int)",
                "    )",
                ""
            ])
        })

        s = set(['gs_foo', 'gs_bar', 'gs_che'])
        orms = get_subclasses("gs", Orm)
        self.assertEqual(s, set([str(o.schema) for o in orms]))



