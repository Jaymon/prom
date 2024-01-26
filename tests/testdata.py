# -*- coding: utf-8 -*-
import os
import random
import string
import datetime
import decimal

# make this module act like global testdata
from testdata import *
from testdata import __getattr__

from prom.compat import *
from prom.query import Query
from prom.model import Orm
from prom.config import Schema, DsnConnection, Field, Index, AutoIncrement
from prom.interface import find_environ
from prom.utils import make_dict


class InterfaceData(TestData):

    interfaces = set()

    def get_interfaces(self):
        """Return all currently configured interfaces in a list"""
        return list(self.interfaces)

    def get_interface(self, interface=None):
        return interface or self.create_interface()

    def create_interface(self):
        if interface_class := getattr(self, "interface_class", None):
            return self.find_interface(interface_class)

        else:
            for inter in self.create_environ_interfaces():
                return inter

    def create_dsn_interface(self, dsn):
        conn = DsnConnection(dsn)
        inter = conn.interface
        self.interfaces.add(inter)
        return inter

    def create_environ_connections(self, dsn_env_name="PROM_TEST_DSN"):
        """creates all the connections that are defined in the environment under
        <dsn_env_name>_N where N can be any integer"""
        found = False
        if dsn_index := os.environ.get("PROM_TEST_DSN_INDEX", 0):
            for conn in find_environ(f"{dsn_env_name}_{dsn_index}"):
                found = True
                yield conn

        else:
            for conn in find_environ(dsn_env_name):
                found = True
                yield conn

        if not found:
            raise ValueError("No connection found, set PROM_TEST_DSN")

    def create_environ_interfaces(self):
        """find any interfaces that match dsn_env_name and yield them"""
        for conn in self.create_environ_connections():
            inter = conn.interface
            self.interfaces.add(inter)
            yield inter

    def get_environ_interface_classes(self):
        for conn in self.create_environ_connections():
            yield conn.interface_class

    def find_interface(self, interface_class):
        for inter in self.create_environ_interfaces():
            if isinstance(inter, interface_class):
                return inter

    def get_table_name(self, table_name=None, prefix=""):
        """return a random table name"""
        if table_name: return table_name
        return "{}{}_table".format(
            prefix,
            "".join(
                random.sample(string.ascii_lowercase, random.randint(5, 15))
            )
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

    async def create_orms(self, table_name=None, count=0, **fields):
        """Create count orms at table_name with fields

        :returns: Orm class, the Orm class created with table_name
        """
        orm_class = self.get_orm_class(table_name, **fields)
        count = count or self.get_int(1, 10)
        await self.insert(orm_class, count)
        return orm_class

    async def create_orm(self, table_name=None, **fields):
        orm_class = self.get_orm(table_name, **fields)
        fs = self.get_fields(orm_class.schema)
        return await orm_class.create(fs)

    def find_orm_class(self, v):
        if issubclass(v, Orm):
            orm_class = v

        elif isinstance(v, Query):
            orm_class = v.orm_class

        else:
            orm_class = getattr(v, "orm_class", None)
            if not orm_class:
                raise ValueError("Could not find Orm class")

        return orm_class

    def get_table(self, interface=None, table_name=None, **fields_or_indexes):
        """Return an interface and schema for a table in the db

        This does not try and create anything

        :param interface: Interface, the interface you want to use
        :param table_name: str, the table name, uses .get_table_name() if None
        :param **fields_or_indexes: keys will be field name and the values
            should be Field or Index instances
        :returns: tuple[Interface, Schema]
        """
        interface = self.get_interface(interface)
        schema = self.get_schema(table_name, **fields_or_indexes)
        return interface, schema

    async def create_table(self, *args, **kwargs):
        interface, schema = self.get_table(*args, **kwargs)
        await interface.set_table(schema)
        return interface, schema

    def get_schema(self, table_name=None, prefix="schema", **fields_or_indexes):
        if not fields_or_indexes:
            fields_or_indexes.setdefault("foo", Field(int, True))
            fields_or_indexes.setdefault("bar", Field(str, True))
            fields_or_indexes.setdefault("ifoobar", Index("foo", "bar"))

        fields_or_indexes.setdefault("_id", AutoIncrement())

        # remove any None values
        for k in list(fields_or_indexes.keys()):
            if not fields_or_indexes[k]:
                fields_or_indexes.pop(k)

        s = Schema(
            self.get_table_name(table_name, prefix=prefix),
            **fields_or_indexes
        )
        return s

    def find_schema(self, v):
        if isinstance(v, Schema):
            schema = v

        elif isinstance(v, Query):
            schema = v.orm_class.schema

        elif issubclass(v, Orm):
            schema = v.schema

        else:
            schema = getattr(v, "schema", None)
            if not schema:
                raise ValueError("Could not find Schema")

        return schema

    def get_schema_all(self, inter=None):
        """return a schema that has a field for all supported standard field
        types"""

        # this is for foreign key fields
        orm_class = self.get_orm_class()
        if inter:
            orm_class.interface = inter
            orm_class.install()

        s = Schema(
            self.get_table_name(),
            _id=AutoIncrement(),
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
                fields[k] = self.get_words()

            elif issubclass(v.interface_type, bool):
                fields[k] = True if random.randint(0, 1) == 1 else False

            elif issubclass(v.interface_type, int):
                fields[k] = self.get_int32()

            elif issubclass(v.interface_type, long):
                fields[k] = self.get_int64()

            elif issubclass(v.interface_type, datetime.datetime):
                fields[k] = self.get_past_datetime()

            elif issubclass(v.interface_type, float):
                fields[k] = self.get_float()

            else:
                raise ValueError("{}".format(v.interface_type))

        fields.update(field_kwargs)
        return fields

    def get_insert_fields(self, *args, **kwargs):
        """Gets everything needed to insert fields into a table

        :param *args:
            - interface, schema, count
            - query, count
            - (interface, schema), count
            - orm_class, count
            - orm instance, count
            - orm_class
            - orm instance
        :returns: tuple[Interface, Schema, Orm, list[dict]], the list will be
            count dicts with fields to insert using Schema
        """
        if len(args) == 1:
            o = args[0]
            count = 1

        elif len(args) == 2:
            o = args[0]
            count = args[1]

        elif len(args) == 3:
            o = (args[0], args[1])
            count = args[2]

        else:
            raise ValueError(f"Passed in {len(args)} arguments")

        if isinstance(o, Query):
            orm_class = o.orm_class
            interface = o.interface
            schema = orm_class.schema

        elif isinstance(o, tuple):
            orm_class = None
            interface, schema = o

        elif isinstance(o, Orm):
            orm_class = type(o)
            schema = orm_class.schema
            interface = o.interface

        elif issubclass(o, Orm):
            orm_class = o
            schema = orm_class.schema
            interface = o.interface

        else:
            raise ValueError("couldn't get fields for object {}".format(o))

        fields_list = []
        for i in range(count):
            fields_list.append(self.get_fields(schema, **kwargs))

        return interface, schema, orm_class, fields_list

    async def insert(self, *args, **kwargs):
        """This has the same signature as .get_insert_fields but inserts the
        generated fields into the db

        :param *args: passed through to .get_insert_fields
        :param **kwargs: passed through to .get_insert_fields
        :returns: int|list, if only one row was inserted then this will return
            the pk of that row, if multiple rows were inserted it will return
            a list of primary keys for all the rows inserted
        """
        pks = []

        interface, schema, orm_class, fields = self.get_insert_fields(
            *args,
            **kwargs
        )
        for fs in fields:
            pks.append(await interface.insert(schema, fs))

        return pks if len(pks) > 1 else pks[0]

    async def insert_fields(self, o, fields=None, **fields_kwargs):
        """Insert fields

        :param o: Query|tuple[Interface, Schema]|Orm
        :param fields: dict|None
        :param **fields_kwargs: merged with fields
        :returns: Any, the primary key for the row containing the inserted
            fields
        """
        fields = make_dict(fields, fields_kwargs)
        return await self.insert(o, 1, **fields)

    async def insert_orm(self, orm_class, fields=None, **fields_kwargs):
        """Insert fields into orm_class and return an orm instance

        :param orm_class: Orm, the orm class
        :param fields: dict|None
        :param **fields_kwargs: merged with fields
        :returns: Orm, an instance of orm_class that represents a row in the db
        """
        pk = await self.insert_fields(orm_class, fields, **fields_kwargs)
        return await orm_class.query.eq_pk(pk).one()

