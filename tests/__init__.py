# -*- coding: utf-8 -*-
from __future__ import unicode_literals, division, print_function, absolute_import
import os
import sys
import random
import string
import datetime
import logging
import decimal
import tempfile
from uuid import uuid4

import testdata
from testdata import TestCase, SkipTest

from prom import query
from prom.compat import *
from prom.model import Orm
from prom.config import Schema, DsnConnection, Field, Index
from prom.interface.base import Interface, Connection
from prom.interface import get_interfaces, find_environ
from prom.utils import make_dict
import prom


testdata.basic_logging(
    levels={
        "prom": "DEBUG",
        #"prom": "ERROR",
        #"prom": "INFO",
        "datatypes": "WARNING",
    }
)


logger = logging.getLogger(__name__)


class BaseTestCase(TestCase):

    interfaces = set()

    @classmethod
    def get_interfaces(cls):
        """Return all currently configured interfaces in a list"""
        return list(cls.interfaces)

    @classmethod
    def setUpClass(cls):
        """make sure there is a default interface for any class"""
        for inter in cls.create_environ_interfaces():
            try:
                inter.unsafe_delete_tables()
            except inter.InterfaceError as e:
                logger.exception(e)

            finally:
                inter.close()

    def tearDown(self):
        self.tearDownClass()

    @classmethod
    def tearDownClass(cls):
        for inter in cls.interfaces:
            inter.close()
        cls.interfaces = set()

    @classmethod
    def get_interface(cls):
        i = cls.create_interface()
        i.connect()
        return i

    @classmethod
    def create_interface(cls):
        for inter in cls.create_environ_interfaces():
            return inter
        #return cls.create_sqlite_interface()

    @classmethod
    def create_dsn_interface(cls, dsn):
        conn = DsnConnection(dsn)
        inter = conn.interface
        cls.interfaces.add(inter)
        return inter

    @classmethod
    def create_environ_connections(cls, dsn_env_name="PROM_TEST_DSN"):
        """creates all the connections that are defined in the environment under
        <dsn_env_name>_N where N can be any integer"""
        if dsn_index := os.environ.get("PROM_TEST_DSN_INDEX", 0):
            for conn in find_environ(f"{dsn_env_name}_{dsn_index}"):
                yield conn

        else:
            for conn in find_environ(dsn_env_name):
                yield conn

    @classmethod
    def create_environ_interfaces(cls):
        """find any interfaces that match dsn_env_name and yield them"""
        for conn in cls.create_environ_connections():
            inter = conn.interface
            cls.interfaces.add(inter)
            yield inter

    @classmethod
    def find_interface(cls, interface_class):
        for inter in cls.create_environ_interfaces():
            if isinstance(inter, interface_class):
                return inter

        raise ValueError("No {} found, set PROM_TEST_DSN".format(interface_class))

    @classmethod
    def create_sqlite_interface(cls):
        from prom.interface.sqlite import SQLite
        inter = cls.find_interface(SQLite)
        return inter

    @classmethod
    def create_postgres_interface(cls):
        from prom.interface.postgres import PostgreSQL
        inter = cls.find_interface(PostgreSQL)
        return inter

    def get_table(self, table_name=None, interface=None, **fields_or_indexes):
        """
        return an interface and schema for a table in the db

        return -- tuple -- interface, schema
        """
        i = interface or self.get_interface()
        s = self.get_schema(table_name, **fields_or_indexes)
        i.set_table(s)
        return i, s

    def get_table_name(self, table_name=None, prefix=""):
        """return a random table name"""
        if table_name: return table_name
        return "{}{}_table".format(
            prefix,
            "".join(random.sample(string.ascii_lowercase, random.randint(5, 15)))
        )

    def get_orm_class(self, table_name=None, prefix="orm_class", **properties):
        tn = self.get_table_name(table_name, prefix=prefix)
        parent_class = properties.get("parent_class", Orm)

        properties["table_name"] = tn

        if "interface" not in properties:
            if "connection_name" not in properties:
                properties["interface"] = self.get_interface()

        has_field = False
        for k, v in properties.items():
            if isinstance(v, Field):
                has_field = True
                break

            elif isinstance(v, type) and issubclass(v, Field):
                has_field = True
                break

        if not has_field:
            properties.update({
                "foo": Field(int, True),
                "bar": Field(str, True),
                "ifoobar": Index("foo", "bar"),
            })

        orm_class = type(
            String(tn),
            (parent_class,),
            properties,
        )

        return orm_class

    def get_orm(self, table_name=None, prefix="orm", **fields):
        orm_class = self.get_orm_class(table_name, prefix=prefix)
        t = orm_class(**fields)
        return t

    def create_orms(self, table_name=None, count=0, **fields):
        """Create count orms at table_name with fields

        :returns: Orm class, the Orm class created with table_name
        """
        orm_class = self.get_orm_class(table_name, **fields)
        count = count or testdata.get_int(1, 10)
        self.insert(orm_class, count)
        return orm_class

    def create_orm(self, table_name=None, **fields):
        orm_class = self.get_orm(table_name, **fields)
        fs = self.get_fields(orm_class.schema)
        return orm_class.create(fs)

    def find_orm_class(self, v):
        if issubclass(v, Orm):
            orm_class = v

        elif isinstance(v, query.Query):
            orm_class = v.orm_class

        else:
            orm_class = getattr(v, "orm_class", None)
            if not orm_class:
                raise ValueError("Could not find Orm class")

        return orm_class

    def get_schema(self, table_name=None, prefix="schema", **fields_or_indexes):
        if not fields_or_indexes:
            fields_or_indexes.setdefault("foo", Field(int, True))
            fields_or_indexes.setdefault("bar", Field(str, True))
            fields_or_indexes.setdefault("ifoobar", Index("foo", "bar"))

        fields_or_indexes.setdefault("_id", Field(int, True, pk=True))

        # remove any None values
        for k in list(fields_or_indexes.keys()):
            if not fields_or_indexes[k]:
                fields_or_indexes.pop(k)

        s = Schema(
            self.get_table_name(table_name, prefix=prefix),
            **fields_or_indexes
        )
        return s

    def create_schema(self, inter=None, table_name=None, **fields_or_indexes):
        if not inter:
            inter = self.get_interface()

        s = self.get_schema(table_name, **fields_or_indexes)
        inter.set_table(s)
        return inter, s

    def find_schema(self, v):
        if isinstance(v, Schema):
            schema = v

        elif isinstance(v, query.Query):
            schema = v.orm_class.schema

        elif issubclass(v, Orm):
            schema = v.schema

        else:
            schema = getattr(v, "schema", None)
            if not schema:
                raise ValueError("Could not find Schema")

        return schema

    def get_schema_all(self, inter=None):
        """return a schema that has a field for all supported standard field types"""

        # this is for foreign key fields
        orm_class = self.get_orm_class()
        if inter:
            orm_class.interface = inter
            orm_class.install()

        s = Schema(
            self.get_table_name(),
            _id=Field(long, pk=True),
            a_bool_y=Field(bool, True),
            a_bool_n=Field(bool, False),
            a_sint_y=Field(int, True, size=50),
            a_sint_n=Field(int, False, size=50),
            a_dec_y=Field(decimal.Decimal, True),
            a_dec_n=Field(decimal.Decimal, False),
            a_float_y=Field(float, True, size=10),
            a_float_n=Field(float, False, size=10),
            a_long_y=Field(long, True),
            a_long_n=Field(long, False),
            a_fk_y=Field(orm_class, True),
            a_fk_n=Field(orm_class, False),
            a_dt_y=Field(datetime.datetime, True),
            a_dt_n=Field(datetime.datetime, False),
            a_d_y=Field(datetime.date, True),
            a_d_n=Field(datetime.date, False),
            a_int_y=Field(int, True),
            a_int_n=Field(int, False),
            a_str_y=Field(str, True),
            a_str_n=Field(str, False),
            a_vchar_y=Field(str, True, max_size=512),
            a_vchar_n=Field(str, False, max_size=512),
            a_char_y=Field(str, True, size=32),
            a_char_n=Field(str, False, size=32),
        )

        if inter:
            #i = self.get_interface()
            inter.set_table(s)

        return s

    def get_query(self, table_name=None, prefix="query"):
        orm_class = self.get_orm_class(table_name, prefix=prefix)
        return orm_class.query

    def get_fields(self, schema, **field_kwargs):
        """return the fields of orm with randomized data"""
        fields = {}
        for k, v in schema.fields.items():
            if v.is_pk(): continue
            if v.is_ref(): continue

            if issubclass(v.interface_type, basestring):
                fields[k] = testdata.get_words()

            elif issubclass(v.interface_type, bool):
                fields[k] = True if random.randint(0, 1) == 1 else False

            elif issubclass(v.interface_type, int):
                fields[k] = testdata.get_int32()

            elif issubclass(v.interface_type, long):
                fields[k] = testdata.get_int64()

            elif issubclass(v.interface_type, datetime.datetime):
                fields[k] = testdata.get_past_datetime()

            elif issubclass(v.interface_type, float):
                fields[k] = testdata.get_float()

            else:
                raise ValueError("{}".format(v.interface_type))

        fields.update(field_kwargs)
        return fields

    def insert(self, *args, **kwargs):
        """most typically you will call this with (object, count)"""
        pks = []
        if len(args) == 3:
            #o = (interface, schema)
            o = (args[0], args[1])
            count = args[2]

        else:
            o = args[0]
            count = args[1]

        for i in range(count):
            pks.append(self.insert_fields(o))

        return pks

    def insert_fields(self, o, fields=None, **fields_kwargs):
        fields = make_dict(fields, fields_kwargs)

        if isinstance(o, query.Query):
            schema = o.orm_class.schema
            pk = self.insert_fields(o.orm_class, fields)

        elif isinstance(o, tuple):
            interface, schema = o
            fields = self.get_fields(schema, **fields)
            pk = interface.insert(schema, fields)

        else:
            orm_class = None
            if isinstance(o, Orm):
                orm_class = type(o)
                schema = orm_class.schema

            elif issubclass(o, Orm):
                orm_class = o
                schema = orm_class.schema

            if orm_class:
                fields = self.get_fields(schema, **fields)
                o = orm_class.create(fields)
                pk = o.pk

            else:
                raise ValueError("couldn't insert for object {}".format(o))

        pk_name = schema.pk_name
        if pk_name:
            assert pk > 0

        return pk

    def insert_orm(self, orm_class, fields=None, **fields_kwargs):
        pk = self.insert_fields(orm_class, fields, **fields_kwargs)
        return orm_class.query.eq_pk(pk).one()


class EnvironTestCase(BaseTestCase):
    """This will run all the tests with multple environments (eg, both SQLite and Postgres)"""
    interface = None

    @classmethod
    def create_interface(cls):
        return cls.create_dsn_interface(cls.interface.connection_config.dsn)

    def run(self, *args, **kwargs):
        for inter in self.create_environ_interfaces():
            type(self).interface = inter
            super().run(*args, **kwargs)

    def countTestCases(self):
        ret = super().countTestCases()
        multiplier = len(list(self.create_environ_connections())) # the number of interfaces
        return ret * multiplier

