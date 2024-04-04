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
    """Hold all helper methods to create db tables and get tests ready

    These testdata methods are specifically to test prom itself and should
    never be used outside of prom's tests
    """

    interfaces = set()
    """Every interface that is created gets added to this set so that it can be
    closed"""

    def get_interfaces(self):
        """Return all currently configured interfaces in a list"""
        return list(self.interfaces)

    def get_interface(self, interface=None):
        """Get an interface, this does not connect the interface

        :param Interface: Interface, if you pass one in that will be returned
            instead of a new interface created
        :returns: Interface
        """
        return interface or self.create_interface()

    def create_interface(self):
        """Create an interface and return it, this does not connect the
        interface

        NOTE -- this uses a class property `interface_class` that needs to be
        set in a test setup method like TestCase.setUp or the like, if that
        class property is set then it will use that class to create the
        interface, otherwise it just creates the first found interface

        :return: Interface
        """
        if interface_class := getattr(self, "interface_class", None):
            return self.find_interface(interface_class)

        else:
            for inter in self.create_environ_interfaces():
                return inter

    def create_dsn_interface(self, dsn):
        """Create an interface with the given DSN

        :param dsn: str, the DSN string
        :returns: Interface
        """
        conn = DsnConnection(dsn)
        inter = conn.interface
        self.interfaces.add(inter)
        return inter

    def create_environ_connections(self, dsn_env_name="PROM_TEST_DSN"):
        """creates all the connections that are defined in the environment under
        <dsn_env_name>_N where N can be any integer

        NOTE -- since this can create multiple connections if the environment
        variables are postfixed with _N (eg, *_1, *_2) you can set the
        environment variable `PROM_TEST_DSN_INDEX` to an index to limit
        connection creation to only that index, so to limit to just *_1:

            export PROM_TEST_DSN_INDEX=1

        :param dsn_env_name: str, the name of the environment variable that 
            should be read to create the connections
        :returns: generator[Connection]
        """
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
        """find any interfaces that match dsn_env_name and yield them

        This is basically .create_environ_connections but wraps the connection
        instance in an Interface instance

        :returns: generator[Interface]
        """
        for conn in self.create_environ_connections():
            inter = conn.interface
            self.interfaces.add(inter)
            yield inter

    def get_environ_interface_classes(self):
        """Returns all the Interface classes of the given environment

        :returns: generator[type], when you don't want the Connection instance,
            you don't want an Interface instance, you just want the Connection
            class
        """
        for conn in self.create_environ_connections():
            yield conn.interface_class

    def find_interface(self, interface_class):
        """Find an Interface that matches `interface_class`

        :returns: Interface
        """
        for inter in self.create_environ_interfaces():
            if isinstance(inter, interface_class):
                return inter

    def get_table_name(self, table_name=None, prefix=""):
        """return a random table name

        :param table_name: str, if passed in then this will be returned
        :param prefix: str, if passed in then a table name will be randomly
            generated that starts with this
        :returns: str, a usable table name
        """
        if table_name:
            return table_name

        return "{}{}_table".format(
            prefix,
            "".join(
                random.sample(string.ascii_lowercase, random.randint(5, 15))
            )
        )

    def get_orm_class(self, table_name=None, prefix="orm_class", **properties):
        """Return an orm class

        :param table_name: str, the table the orm class will wrap
        :param prefix: str, the class name will start with this
        :param **properties: these will be set on the orm class, if they are
            Field instances then those will be set as schema fields, if they
            are Index instances they will be schema indexes
            - interface: Interface, the interface the returned class will use
            - connection_name: str, an interface will be created with this
                connection name
        :returns: type, an Orm child class
        """
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
        """Returns an Orm instance

        :param table_name: passed through to `.get_orm_class`
        :param prefix: passed through to `.get_orm_class`
        :param **fields: these are used to instantiate an instance of the
            created orm class
        :returns: Orm
        """
        orm_class = self.get_orm_class(table_name, prefix=prefix)
        t = orm_class(**fields)
        return t

    async def create_orms(self, table_name=None, count=0, **fields):
        """Create count orms at table_name with fields

        see `.get_orm_class` for the arguments

        :param table_name: passed through to `.get_orm_class`
        :param prefix: passed through to `.get_orm_class`
        :param **fields: passed through to `.get_orm_class`
        :param count: int, how many orms you want created
        :returns: type, the Orm class, the Orm class created with table_name
        """
        orm_class = self.get_orm_class(table_name, **fields)
        count = count or self.get_int(1, 10)
        await self.insert(orm_class, count)
        return orm_class

    async def create_orm(self, table_name=None, **fields):
        """Creat an orm with the given field values

        :param table_name: passed through to `.get_orm`
        :param prefix: passed through to `.get_orm`
        :param **fields: passed through to `.get_orm`
        :returns: Orm, this will be persisted in the db
        """
        orm_class = self.get_orm(table_name, **fields)
        fs = self.get_fields(orm_class.schema)
        return await orm_class.create(fs)

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
        """Create a table on the db

        :param *args: passed through to `.get_table`
        :param **kwargs: passed through to `.get_table`
        :returns: tuple[Interface, Schema]
        """
        interface, schema = self.get_table(*args, **kwargs)
        await interface.set_table(schema)
        return interface, schema

    def get_schema(self, table_name=None, prefix="schema", **fields_or_indexes):
        """Get a Schema instance

        :param table_name: str, the table name you want the schema to have, if
            not given it will be automatically created using `prefix`
        :param prefix`: str, used to create the table name
        :param **fields_or_indexes: fields will be Field instances with the key
            being the name. Indexes will be Index instances with key as the
            index name
        :returns: Schema
        """
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

    def get_query(self, table_name=None, prefix="query", **properties):
        """Get a query instance

        :param table_name: str, if passed in the table will be named this
        :param prefix: str, a random table name will be created using this
            prefix
        :returns: Query, this query will belong to an Orm, so Query.orm_class
            should return the orm class
        """
        orm_class = self.get_orm_class(
            table_name,
            prefix=prefix,
            **properties
        )
        return orm_class.query

    def get_fields(self, schema, **field_kwargs):
        """return the fields of orm with randomized data

        :param schema: Schema, values for all the fields in this schema will be
            randomly generated
        :param **field_kwargs: specific field values you want the returned dict
            to have
        :returns: dict[str, Any]
        """
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

        Internal method used by the other .insert* methods

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
            d = await interface.insert(schema, fs)
            if pk_name := schema.pk_name:
                pks.append(d[pk_name])

            else:
                pks.append(None)

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

