# -*- coding: utf-8 -*-
from __future__ import unicode_literals, division, print_function, absolute_import

from prom import query
from prom.model import Orm
import prom
from captain.client import Captain

from . import BaseTestCase, testdata


class Script(Captain):
    script_quiet = False

    def __init__(self, subcommand):
        self.cmd_prefix = "python -m prom {}".format(subcommand.replace("_", "-"))
        super(Script, self).__init__("")


class GenerateTest(BaseTestCase):

    def create_script(self):
        s = Script("generate")
        script_env = s.env

        # we want to cleanup the environment of any rogue PROM_DSNs
        script_env{r[0]: r[1] for r in script_env if not r[0].startswith("PROM")}

        for i, inter in enumerate(self.get_interfaces()):
            script_env["PROM_DSN_{}".format(i)] = inter.connection_config.dsn

        s.env = script_env
        return s

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

