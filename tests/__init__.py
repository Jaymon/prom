from unittest import TestCase
import os
import sys
import random
import string
import datetime
import logging

import testdata

from prom import query
from prom.model import Orm
from prom.config import Schema, DsnConnection, Field, Index
from prom.interface.postgres import PostgreSQL
from prom.interface.sqlite import SQLite
from prom.interface.base import Interface
import prom


# configure root logger
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
log_handler = logging.StreamHandler(stream=sys.stderr)
log_formatter = logging.Formatter('[%(levelname)s] %(message)s')
log_handler.setFormatter(log_formatter)
logger.addHandler(log_handler)


os.environ.setdefault('PROM_SQLITE_URL', 'prom.interface.SQLite://:memory:')


class BaseTestCase(TestCase):
    @classmethod
    def setUpClass(cls):
        """make sure there is a default interface for any class"""
        i = cls.get_interface()
        i.delete_tables(disable_protection=True)
        prom.set_interface(i)

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
        config = DsnConnection(os.environ["PROM_SQLITE_URL"])
        i = SQLite(config)
        return i

    @classmethod
    def create_postgres_interface(cls):
        config = DsnConnection(os.environ["PROM_POSTGRES_URL"])
        i = PostgreSQL(config)
        return i

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

    def get_query(self):
        orm_class = self.get_orm_class()
        return orm_class.query

    def get_fields(self, schema):
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


