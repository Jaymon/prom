# -*- coding: utf-8 -*-
from __future__ import unicode_literals, division, print_function, absolute_import
from unittest import TestCase, SkipTest
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

from prom import query
from prom.compat import *
from prom.model import Orm
from prom.config import Schema, DsnConnection, Field, Index
from prom.interface.postgres import PostgreSQL
from prom.interface.sqlite import SQLite
from prom.interface.base import Interface
from prom.interface import get_interfaces
import prom


testdata.basic_logging()


#os.environ.setdefault('PROM_SQLITE_DSN', 'prom.interface.sqlite.SQLite://:memory:')
os.environ.setdefault(
    'PROM_SQLITE_DSN',
    'prom.interface.sqlite.SQLite://{}.sqlite'.format(os.path.join(tempfile.gettempdir(), str(uuid4())))
)


class BaseTestCase(TestCase):

    connections = set()

    def tearDown(self):
        self.tearDownClass()

    @classmethod
    def get_interfaces(cls):
        """Return all currently configured interfaces in a list"""
        return get_interfaces().values()

    @classmethod
    def setUpClass(cls):
        """make sure there is a default interface for any class"""
        i = cls.get_interface()
        i.delete_tables(disable_protection=True)
        prom.set_interface(i)

    @classmethod
    def tearDownClass(cls):
        for inter in cls.connections:
            inter.close()
        cls.connections = set()

    @classmethod
    def get_interface(cls):
        i = cls.create_interface()
        i.connect()
        assert i.connected
        return i

    @classmethod
    def create_interface(cls):
        return cls.create_postgres_interface()

    @classmethod
    def create_sqlite_interface(cls):
        return cls.create_environ_interface("PROM_SQLITE_DSN")

    @classmethod
    def create_postgres_interface(cls):
        return cls.create_environ_interface("PROM_POSTGRES_DSN")

    @classmethod
    def create_environ_interface(cls, environ_key):
        config = DsnConnection(os.environ[environ_key])
        inter = config.interface
        cls.connections.add(inter)
        return inter

    def get_table(self, table_name=None):
        """
        return an interface and schema for a table in the db

        return -- tuple -- interface, schema
        """
        i = self.get_interface()
        s = self.get_schema(table_name)
        i.set_table(s)
        return i, s

    def get_table_name(self, table_name=None):
        """return a random table name"""
        if table_name: return table_name
        return "{}_table".format(
            "".join(random.sample(string.ascii_lowercase, random.randint(5, 15)))
        )

    def get_orm_class(self, table_name=None):
        tn = self.get_table_name(table_name)
        class Torm(Orm):
            table_name = tn
            interface = self.get_interface()
            foo = Field(int, True)
            bar = Field(str, True)
            ifoobar = Index("foo", "bar")

        return Torm

    def get_orm(self, table_name=None, **fields):
        orm_class = self.get_orm_class(table_name)
        t = orm_class(**fields)
        return t

    def get_schema(self, table_name=None, **fields_or_indexes):
        if not fields_or_indexes:
            fields_or_indexes.setdefault("foo", Field(int, True))
            fields_or_indexes.setdefault("bar", Field(str, True))
            fields_or_indexes.setdefault("ifoobar", Index("foo", "bar"))

        fields_or_indexes.setdefault("_id", Field(long, True, pk=True))

        s = Schema(
            self.get_table_name(table_name),
            **fields_or_indexes
        )
        return s

    def get_schema_all(self, inter=None):
        """return a schema that has a field for all supported standard field types"""
        orm_class = self.get_orm_class()
        if inter:
            orm_class.interface = inter
            i = inter
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

    def get_query(self, table_name=None):
        orm_class = self.get_orm_class(table_name)
        return orm_class.query

    def get_fields(self, schema, **field_kwargs):
        """return the fields of orm with randomized data"""
        fields = {}
        for k, v in schema.fields.items():
            if v.is_pk(): continue

            if issubclass(v.type, basestring):
                fields[k] = testdata.get_words()

            elif issubclass(v.type, int):
                fields[k] = testdata.get_int32()

            elif issubclass(v.type, long):
                fields[k] = testdata.get_int64()

            elif issubclass(v.type, datetime.datetime):
                fields[k] = testdata.get_past_datetime()

            elif issubclass(v.type, float):
                fields[k] = testdata.get_float()

            elif issubclass(v.type, bool):
                fields[k] = True if random.randint(0, 1) == 1 else False

            else:
                raise ValueError("{}".format(v.type))

        fields.update(field_kwargs)
        return fields

    def insert(self, *args, **kwargs):
        """most typically you will call this with (object, count)"""
        pks = []
        if isinstance(args[0], Interface):
            interface = args[0]
            schema = args[1]
            count = args[2]

            for i in range(count):
                fields = self.get_fields(schema)
                pks.append(interface.insert(schema, fields, **kwargs))

        elif isinstance(args[0], query.Query):
            q = args[0].copy()
            q.reset()
            count = args[1]
            for i in range(count):
                fields = self.get_fields(q.orm_class.schema)
                pks.append(q.copy().set(fields).insert())

        elif issubclass(args[0], Orm):
            orm_class = args[0]
            count = args[1]
            for i in range(count):
                fields = self.get_fields(orm_class.schema)
                o = orm_class.create(fields)
                pks.append(o.pk)

        assert count == len(pks)
        for pk in pks:
            assert pk > 0
        return pks

    def old_insert(self, interface, schema, count, **kwargs):
        """insert count rows into schema using interface"""
        pks = []
        for i in range(count):
            fields = self.get_fields(schema)
            pk = interface.insert(schema, fields, **kwargs)

            assert pk > 0
            pks.append(pk)

        return pks


class EnvironTestCase(BaseTestCase):
    """This will run all the tests with multple environments (eg, both SQLite and Postgres)"""

    @classmethod
    def setUpClass(cls):
        """make sure there is a default interface for any class"""
        for i in cls.create_interfaces():
            i.delete_tables(disable_protection=True)
            prom.set_interface(i)

    @classmethod
    def create_interfaces(cls):
        return [
            cls.create_environ_interface("PROM_POSTGRES_DSN"),
            cls.create_environ_interface("PROM_SQLITE_DSN")
        ]

    @classmethod
    def create_interface(cls):
        return cls.create_environ_interface("PROM_DSN")

    def run(self, *args, **kwargs):
        for i in self.create_interfaces():
            os.environ["PROM_DSN"] = i.connection_config.dsn
            prom.set_interface(i)
            super(EnvironTestCase, self).run(*args, **kwargs)

    def countTestCases(self):
        ret = super(EnvironTestCase, self).countTestCases()
        multiplier = 2 # the number of interfaces returned from create_interfaces()
        return ret * multiplier

