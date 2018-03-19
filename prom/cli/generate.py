# -*- coding: utf-8 -*-
from __future__ import unicode_literals, division, print_function, absolute_import

from captain import exit as console, echo
from captain.decorators import arg

from ..interface import get_interfaces
from .. import Field, __version__
from ..utils import Stream


def get_table_info(*table_names):
    """Returns a dict with table_name keys mapped to the Interface that table exists in

    :param *table_names: the tables you are searching for
    """
    ret = {}
    if table_names:
        for table_name in table_names:
            for name, inter in get_interfaces().items():
                if inter.has_table(table_name):
                    yield table_name, inter, inter.get_fields(table_name)

    else:
        for name, inter in get_interfaces().items():
            table_names = inter.get_tables()
            for table_name in table_names:
                yield table_name, inter, inter.get_fields(table_name)


def get_field_def(field_name, field_d):
    field_required = field_d["field_required"]
    if "ref_table_name" in field_d:
        field_type = field_d["ref_table_name"].title().replace("_", "")
    else:
        try:
            field_type = field_d["field_type"].__name__
        except KeyError:
            raise ValueError("Could not find python type for field {}".format(field_name))

    arg_bits = [field_type, str(field_required)]
    if field_d["pk"]:
        arg_bits.append("pk={}".format(field_d["pk"]))

    return "    {} = Field({})".format(field_name, ", ".join(arg_bits))


@arg("table_names", nargs="*", help="the table(s) to generate a prom.Orm for")
@arg("--out-file", "-o", dest="stream", type=Stream, default="", help="Write to a file path, default stdout")
def main_generate(table_names, stream):
    """This will print out valid prom python code for given tables that already exist
    in a database.

    This is really handy when you want to bootstrap an existing database to work
    with prom and don't want to manually create Orm objects for the tables you want
    to use, let `generate` do it for you
    """
    with stream.open() as fp:
        fp.write_line("from datetime import datetime, date")
        fp.write_line("from decimal import Decimal")
        fp.write_line("from prom import Orm, Field")
        fp.write_newlines()

        for table_name, inter, fields in get_table_info(*table_names):
            fp.write_line("class {}(Orm):".format(table_name.title().replace("_", "")))
            fp.write_line("    table_name = '{}'".format(table_name))
            if inter.connection_config.name:
                fp.write_line("    connection_name = '{}'".format(inter.connection_config.name))

            fp.write_newlines()
            magic_field_names = set(["_id", "_created", "_updated"])

            if "_id" in fields:
                fp.write_line(get_field_def("_id", fields.pop("_id")))
                magic_field_names.discard("_id")

            for field_name, field_d in fields.items():
                fp.write_line(get_field_def(field_name, field_d))

            for magic_field_name in magic_field_names:
                if magic_field_name not in fields:
                    fp.write_line("    {} = None".format(magic_field_name))

            fp.write_newlines(2)


