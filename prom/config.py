# -*- coding: utf-8 -*-
from __future__ import unicode_literals, division, print_function, absolute_import
import decimal
import datetime
import inspect
import re
import base64
import json
import logging

import dsnparse
from datatypes import Datetime

from .compat import *
from . import utils


logger = logging.getLogger(__name__)


class Connection(object):
    """
    set the paramaters you want to use to connect to an interface

    https://github.com/Jaymon/Mingo/blob/master/Mingo/MingoConfig.php
    """
    name = ""
    """string -- the name of this connection (eg, Postgres, or SQLite)"""

    interface_name = ""
    """string -- full Interface class name -- the interface the connection should use to talk with the db"""

    host = ""
    """the hostname"""

    database = ""
    """the db name to use, in postgres, this is the database name"""

    port = 0
    """the host port"""

    username = ""
    """the username to use to to connect to the db"""

    password = ""
    """the password for the username"""

    readonly = False
    """Set to true to make the connection readonly"""

    options = None
    """any other db options, these can be interface implementation specific"""

    @property
    def interface(self):
        """Return a new Interface instance using this configuration"""
        interface_class = self.interface_class
        return interface_class(self)

    def __init__(self, fields=None, **fields_kwargs):
        """
        set all the values by passing them into this constructor, any unrecognized kwargs get put into .options

        :example:
            c = Connection(
                host="127.0.0.1",
                database="dbname",
                port=5000,
                some_random_thing="foo"
            )

            print(c.port) # 5000
            print(c.options) # {"some_random_thing": "foo"}
        """
        kwargs = utils.make_dict(fields, fields_kwargs)
        if "interface_name" not in kwargs:
            raise ValueError("no interface_name passed into Connection")

        self.options = {}

        for key, val in kwargs.items():
            if hasattr(self, key):
                setattr(self, key, val)
            else:
                self.options[key] = val

        interface_module, interface_class = utils.get_objects(self.interface_name)
        self.interface_class = interface_class
        interface_class.configure(self)


class DsnConnection(Connection):
    """
    sets up a connection configuration using a "prom dsn" or connection string

    the prom dsn is in the form:

        <full.python.path.InterfaceClass>://<username>:<password>@<host>:<port>/<database>?<options=val&query=string>#<name>

    This is useful to allow connections coming in through environment variables as described in :
        http://www.12factor.net/backing-services

    It tooks its inspiration from this project:
        https://github.com/kennethreitz/dj-database-url

    http://en.wikipedia.org/wiki/Connection_string
    http://en.wikipedia.org/wiki/Data_source_name
    """
    dsn = ""
    """holds the raw dsn string that was parsed"""

    def __init__(self, dsn):
        # get the scheme, which is actually our interface_name
        kwargs = self.parse(dsn)
        super(DsnConnection, self).__init__(**kwargs)

    @classmethod
    def parse(cls, dsn):
        d = dsnparse.ParseResult.parse(dsn)

        # remap certain values
        d["name"] = d.pop("fragment")
        d["interface_name"] = cls.normalize_scheme(d.pop("scheme"))
        d["database"] = d.pop("path")
        d["options"] = cls.normalize_options(d.pop("query"))
        d["host"] = d.pop("hostname")
        d["readonly"] = bool(d["options"].pop("readonly", cls.readonly))

        # get rid of certain values
        d.pop("params", None)
        d.pop("query_str", None)
        return d

    @classmethod
    def normalize_scheme(cls, v):
        ret = v
        d = {
            "prom.interface.sqlite.SQLite": set(["sqlite"]),
            "prom.interface.postgres.PostgreSQL": set(["postgres", "postgresql", "psql"])
        }

        kv = v.lower()
        for interface_name, vals in d.items():
            if kv in vals:
                ret = interface_name
                break

        return ret

    @classmethod
    def normalize_options(cls, d):
        if not d: return d

        for k, v in d.items():
            if isinstance(v, basestring):
                if re.match(r"^\d+\.\d+$", v):
                    d[k] = float(v)

                elif re.match(r"^\d+$", v):
                    d[k] = int(v)

                elif re.match(r"^true$", v, flags=re.I):
                    d[k] = True

                elif re.match(r"^false$", v, flags=re.I):
                    d[k] = False

        return d


class Schema(object):
    """
    handles all table schema definition

    the table schema definition includes the table name, the fields the table has, and
    the indexes that are on the table
    """
    instances = {}
    """class variable, holds different schema instances for various orms"""

    table_name = ""
    """string -- set the table name for this schema instance"""

    fields = None
    """dict -- all the fields this schema instance will use"""

    indexes = None
    """dict -- all the indexes this schema will have"""

    lookup = None
    """dict -- field information lookup table, basically an internal cache"""

    @property
    def normal_fields(self):
        """fields that aren't magic (eg, don't start with an underscore)"""
        return {f:v for f, v in self.fields.items() if not f.startswith('_')}

    @property
    def required_fields(self):
        """The normal required fields (eg, no magic fields like _id are included)"""
        return {f:v for f, v in self.normal_fields.items() if v.required}

    @property
    def ref_fields(self):
        """Return FK reference fields"""
        return {f:v for f, v in self.normal_fields.items() if v.is_ref()}

    @property
    def magic_fields(self):
        """the magic fields for the schema"""
        return {f:v for f, v in self.fields.items() if f.startswith('_')}

    @property
    def pk_name(self):
        """returns the primary key name for this schema"""
        try:
            pk_field = self.__getattr__("pk")
            pk_name = pk_field.name
        except AttributeError:
            pk_name = None
        return pk_name

    @classmethod
    def get_instance(cls, orm_class, **kwargs):
        """return a Schema singleton instance for the given orm_class

        if there isn't already an instance in cache then a new instance will be
        created. If a Schema instance is already in cache then it will be returned

        :param orm_class: Orm, the class to create the schema for
        :returns: Schema
        """
        table_name = orm_class.table_name
        if table_name not in cls.instances:
            s = cls(table_name)
            s.orm_class = orm_class

            seen_properties = set()
            for klass in inspect.getmro(orm_class)[:-1]:
                for k, v in vars(klass).items():
                    k = String(k)
                    if k not in seen_properties:
                        if isinstance(v, (Field, Index)):
                            s.set(k, v)

                        elif isinstance(v, type):
                            # We've defined a Field class inline of the Orm, so
                            # we want to instantiate it and set it in all the places
                            if issubclass(v, Field):
                                field = v.get_instance()
                                s.set(k, field)

                        seen_properties.add(k)

            #s.orm_class = orm_class
            cls.instances[table_name] = s

        else:
            s = cls.instances[table_name]
            s.orm_class = orm_class

        return s

    def __init__(self, table_name, **fields_or_indexes):
        """Create an instance

        every Orm should have a .schema attribute that is an instance of this class

        :example:
            schema = Schema(
                "table_name"
                field1=Field(int, True),
                field2=Field(str),
                index_fields=Index("field1", "field2")
            )

        :param table_name: string, the table name
        :param **fields_or_indexes: a dict of field name or index keys with tuple values,
            see __getattr__ for more details
        """
        self.fields = {}
        self.indexes = {}
        self.table_name = String(table_name)
        self.orm_class = None
        self.lookup = {
            "names": {},
            "pk": None,
        }

        for name, val in fields_or_indexes.items():
            self.set(name, val)

    def __str__(self):
        return self.table_name

    def __iter__(self):
        """iterate through all the fields defined in this schema

        :returns: tuple, (name, Field)
        """
        return (f for f in self.fields.items())

    def set(self, name, val):
        if isinstance(val, Field):
            self.set_field(name, val)

        elif isinstance(val, Index):
            self.set_index(name, val)

        else:
            raise TypeError("not a Field or Index instance")

    def __getattr__(self, name):
        """
        return the Field instance for the given name

        return -- string -- the string value of the attribute name, eg, self.foo returns "foo"
        """
        try:
            return self.lookup["names"][name]

        except KeyError:
            raise AttributeError("No {} field in schema {}".format(name, self.table_name))

    def set_field(self, field_name, field):
        if not field_name: raise ValueError("field_name is empty")
        if field_name in self.fields: raise ValueError("{} already exists and cannot be changed".format(field_name))
        if not isinstance(field, Field): raise ValueError("{} is not a Field instance".format(type(field)))

        field.name = field_name
        field.orm_class = self.orm_class

        if field.unique:
            self.set_index(field_name, Index(field_name, unique=True))

        self.fields[field_name] = field

        for fn in field.names:
            self.lookup["names"][fn] = field

        if field.is_pk():
            self.lookup["pk"] = field
            self.lookup["names"]["pk"] = field

        return self

    def set_index(self, index_name, index):
        """
        add an index to the schema

        for the most part, you will use the __getattr__ method of adding indexes for a more fluid interface,
        but you can use this if you want to get closer to the bare metal

        index_name -- string -- the name of the index
        index -- Index() -- an Index instance
        """
        if not index_name:
            raise ValueError("index_name must have a value")
        if index_name in self.indexes:
            raise ValueError("index_name {} has already been defined on {}".format(
                index_name, str(self.indexes[index_name].fields)
            ))
        if not isinstance(index, Index): raise ValueError("{} is not an Index instance".format(type(index)))

        index.name = index_name
        index.orm_class = self.orm_class

        self.indexes[index_name] = index
        return self

    def field_name(self, k):
        """
        get the field name of k

        most of the time, the field_name of k will just be k, but this makes special
        allowance for k's like "pk" which will return _id
        """
        return self.__getattr__(k).name

    def create_orm(self, orm_class=None):
        """If you have a schema but don't have an Orm for it, you can call this method
        and have an orm_class created that will have the fields and table_name of this
        schema

        :param orm_class: Orm, if you want your generated class to have a certain
            parent class you can pass in the parent class you want
        :returns: Orm, your child orm populated with information from this schema
        """
        if not orm_class:
            from .model import Orm # avoid circular dependency
            orm_class = Orm

        child_class = type(
            ByteString(self.table_name) if is_py2 else String(self.table_name),
            (orm_class,),
            {"table_name": self.table_name, "schema": self}
        )

        # clear all the Field and Index properties in anticipation of adding the
        # fields from the Schema
        seen_properties = set()
        for klass in inspect.getmro(child_class):
            for k, v in vars(klass).items():
                if k not in seen_properties:
                    if isinstance(v, (Field, Index)):
                        #setattr(child_class, k, property(lambda: AttributeError))
                        setattr(child_class, k, None)
                        #delattr(child_class, k)

                    elif isinstance(v, type) and issubclass(v, Field):
                        setattr(child_class, k, None)

                    seen_properties.add(k)

        for field_name, field in self:
            setattr(child_class, field_name, field)

        return child_class


class Index(object):
    """Each index on the table is configured using this class"""

    def __init__(self, *fields, **options):
        """
        initialize an index

        index_fields -- list -- the string field_names this index will index on, fields have to be already added
            to this schema index
        **options -- dict --
            unique -- boolean -- True if the index should be unique, false otherwise
        """
        if not fields:
            raise ValueError("fields list is empty")

        self.name = ""
        self.fields = list(map(str, fields))
        self.options = options
        self.unique = options.get("unique", False)


class FieldMeta(type):
    """Allows a class definition to be a descriptor also

    I don't love this solution, but I like the syntax of just being able to define
    a subclass inside an Orm and have it work. The problem is an embedded class isn't,
    by default, a descriptor instance, so it wasn't calling fset, fget, and fdel.

    This makes it so an embedded field class will be treated as a descriptor. It
    uses an embedded .instance property to actually perform the operations
    """
    def __get__(cls, obj, classtype=None):
        if obj is None:
            # class is requesting this property, so return it
            return cls

        ret = cls.get_instance().__get__(obj, classtype)
        return ret

    def __set__(cls, *args, **kwargs):
        ret = cls.get_instance().__set__(*args, **kwargs)
        return ret

    def __delete__(cls, *args, **kwargs):
        ret = cls.get_instance().__delete__(*args, **kwargs)
        return ret


class _Field(object):
    """Each column in the database is configured using this class

    You can set a couple getters and setters on this object in order to fine tune
    control over how a field in the db is set and fetched from the instance and
    also how it is set and fetched from the interface

    In model.Orm, there are popluate() and depart() class methods, when populate
    is called, each Field will have its iget() method called. When depart() is called, 
    each Field will have its iset() method called. By customizing these, you can
    control functionality when a Field is going to be read or written to the db

    to customize the field on set and get from the db, you can use the decorators,
    which are classmethods:

        foo = Field(str, True)

        @foo.igetter
        def foo(self, val):
            # do custom things
            return val

        @foo.isetter
        def foo(self, val):
            # do custom things
            return val

    NOTE -- the iset/iget methods are different than traditional python getters
    and setters because they always need to return a value and they always take in a
    value

    There are also fget/fset/fdel methods that can be set to customize behavior
    on when a value is set on a particular orm instance, so if you wanted to make sure
    that bar was always an int when it is set, you could:

        bar = Field(int, True)

        @bar.fsetter
        def bar(self, val):
            # do custom things
            return int(val)

        @bar.fgetter
        def bar(self, val):
            return int(val)

        @bar.jsonabler
        def bar(self, val):
            # convert val to something json safe
            return val

    NOTE -- the fget/fset/fdel methods are different than traditional python getters
    and setters because they always need to return a value and they always take in a
    value

    https://docs.python.org/2/howto/descriptor.html
    """
    required = False
    """True if this field is required"""

    options = None
    """In the instance, this will be a dict of key/val pairs containing extra information
    about the field"""

    default = None
    """Default value for this field"""

    help = ""
    """The description/help message for this field"""

    name = ""
    """The field name"""

    choices = None
    """A set of values that this field can be, if set then no other values can be set"""

    unique = False
    """True if this field is unique"""

    instance = None
    """Don't touch this unless you know what you're doing. Holds internal cached instance"""

    @property
    def schema(self):
        """return the schema instance if this is reference to another table

        see .set_type() for an explanation on why we defer figuring this out until now
        """
        if not hasattr(self, "_schema"):
            field_type = self.original_type
            module, klass = utils.get_objects(field_type)
            schema = klass.schema
            if not schema:
                raise ValueError("Field type {} is not an Orm class".format(field_type))

            self._schema = schema

        return self._schema

    @property
    def type(self):
        """alias of the interface type, this is really here so you can set it to
        a value if you are inline defining a field, this will get passed to the
        __init__ method and then be used to set ._interface_type"""
        return self.interface_type

    @property
    def interface_type(self):
        """Returns the type that will be used in the interface to create the table

        see .set_type() for an explanation on why we defer this until here
        """
        if self._interface_type is None:
            if self.is_serialized():
                self._interface_type = str

            else:
                s = self.schema
                if s:
                    self._interface_type = s.pk.type

                else:
                    self._interface_type = self.original_type

        return self._interface_type

    @property
    def ref(self):
        """Returns the FK reference orm class"""
        schema = self.schema
        return schema.orm_class if schema else None

    @property
    def names(self):
        names = []
        if self.name:
            names.append(self.name)

        names.extend(self.options.get("names", []))
        names.extend(self.options.get("aliases", []))
        return names

    @classmethod
    def get_instance(cls, **kwargs):
        if not cls.instance:
            cls.instance = cls(cls.type, cls.required, cls.options)
        return cls.instance

    def __init__(self, field_type, field_required=False, field_options=None, **field_options_kwargs):
        """
        create a field

        :param field_type: type, the python type of the field, so for a string you would pass str, integer: int,
            boolean: bool, float: float, big int: long
        :param field_required: boolean, true if this field has to be there to insert
        :param field_options: dict, everything else in key: val notation. Current options:
            size -- int -- the size you want the string to be, or the int to be
            min_size -- int -- the minimum size
            max_size -- int -- if you want a varchar, set this
            unique -- boolean -- True to set a unique index on this field, this is just for convenience and is
                equal to self.set_index(field_name, [field_name], unique=True). this is a convenience option
                to set a unique index on the field without having to add a separate index statement
            ignore_case -- boolean -- True to ignore case if this field is used in indexes
            default -- mixed -- defaults to None, can be anything the db can support
        :param **field_options_kwargs: dict, will be combined with field_options
        """
        # we aren't guaranteed to have this field's name when the descriptor is
        # created, so this will be the field name this descriptor will use to 
        # set the value onto the orm
        self.orm_field_name = "_instance_{}_val".format(id(self))

        # we keep a hash of the field's value when it was pulled from the
        # interface (see .iget) so we know if the field has been modified
        self.orm_interface_hash = "_interface_{}_hash".format(id(self))

        field_options = utils.make_dict(field_options, field_options_kwargs)

        d = self.get_size(field_options)
        field_options.update(d)

        self.orm_class = field_options.pop("orm_class", None)

        choices = field_options.pop("choices", set())
        if choices or not self.choices:
            self.choices = choices

        for k in list(field_options.keys()):
            if hasattr(self, k):
                setattr(self, k, field_options.pop(k))

        self.options = field_options
        self.required = field_required or self.is_pk()

        self.set_type(field_type)

    def get_size(self, field_options):
        """Figure out if this field has any size information"""
        d = {}

        min_size = field_options.pop("min_size", -1)
        max_size = field_options.pop("max_size", -1)
        size = field_options.pop("size", -1)

        if size > 0:
            d['size'] = size
        else:
            if min_size > 0 and max_size < 0:
                raise ValueError("min_size option was set with no corresponding max_size")

            elif min_size < -1 and max_size > 0:
                d['max_size'] = max_size

            elif min_size >= 0 and max_size >= 0:
                d['min_size'] = min_size
                d['max_size'] = max_size

        return d

    def set_type(self, field_type):
        """Try to infer as much about the type as can be inferred at this moment

        Because the Field support string classpaths (eg, "modname.Classname") we can't
        figure everything out in this method, so we figure out as much as we can
        and then defer everything else to the .interface_type and .schema properties, this
        allows the parser to hopefully finish loading the modules before we have to
        parse the classpath to find the foreign key schema

        :param field_type: mixed, the field type passed into __init__
        """
        std_types = (
            bool,
            long,
            int,
            float,
            bytearray,
            decimal.Decimal,
            datetime.datetime,
            datetime.date,
        )
        if is_py2:
            std_types = (basestring,) + std_types
        else:
            std_types = basestring + std_types

        json_types = (
            dict,
            list,
        )

        pickle_types = (
            set,
        )

        self.original_type = field_type
        self.serializer = ""
        self._interface_type = None
        self._schema = None

        if isinstance(field_type, type):
            if issubclass(field_type, std_types):
                self._interface_type = field_type

            elif issubclass(field_type, json_types):
                self.serializer = self.options.pop("serializer", "json")
                self._interface_type = str

            elif issubclass(field_type, pickle_types):
                self.serializer = self.options.pop("serializer", "pickle")
                self._interface_type = str

            else:
                schema = getattr(field_type, "schema", None)
                if schema:
                    self._schema = schema

                else:
                    # We have just some random class that isn't an Orm
                    self.serializer = self.options.pop("serializer", "pickle")
                    self._interface_type = str

        elif isinstance(field_type, Schema):
            self._schema = field_type

        else:
            # check if field_type is a string classpath so we have to defer
            # setting the type
            if isinstance(field_type, basestring):
                # no ._schema property will make .schema treat .original_type as
                # a classpath
                del self._schema

            else:
                raise ValueError("Unknown field type {}".format(field_type))

    def is_pk(self):
        """return True if this field is a primary key"""
        return self.options.get("pk", False)

    def is_ref(self):
        """return true if this field foreign key references the primary key of another orm"""
        return bool(self.schema)

    def is_required(self):
        """Return True if this field is required to save into the interface"""
        return self.required

    def is_serialized(self):
        """Return True if this field should be serialized"""
        return True if self.serializer else False

    def fget(self, orm, val):
        """Called anytime the field is accessed through the Orm (eg, Orm.foo)

        :param orm: Orm, the Orm instance the field is being accessed on
        :param val: mixed, the current value of the field
        :returns: mixed
        """
        #pout.v("fget {}".format(self.name))
        return val

    def fgetter(self, v):
        """decorator for setting field's fget function"""
        self.fget = v
        return self

    def iget(self, orm, val):
        """Called anytime the field is being returned from the interface to the orm

        think of this as when the orm receives the field value from the interface

        :param orm: Orm
        :param val: mixed, the current value of the field
        :returns: mixed
        """
        #pout.v("iget {}".format(self.name))
        if self.is_serialized():
            val = self.decode(val)

        orm.__dict__[self.orm_interface_hash] = self.hash(orm, val)

        return val

    def igetter(self, v):
        """decorator for the method called when a field is pulled from the database"""
        self.iget = v
        return self

    def fset(self, orm, val):
        """This is called on Orm instantiation and any time field is set (eg Orm.foo = ...)

        on Orm creation val will be None if the field wasn't pass to Orm.__init__ otherwise
        it will be the value passed into Orm.__init__

        :param orm: Orm, the Orm instance the field is being set on
        :param val: mixed, the current value of the field
        :returns: mixed
        """
        #pout.v("fset {}".format(self.name))
        if val is not None and self.choices:
            if val not in self.choices:
                raise ValueError("Value {} not in {} value choices".format(val, self.name))
        return val

    def fsetter(self, v):
        """decorator for setting field's fset function"""
        self.fset = v
        return self

    def iset(self, orm, val):
        """Called anytime the field is being fetched to send to the interface

        think of this as when the interface is going to get the field value

        :param orm: Orm
        :param val: mixed, the current value of the field
        :returns: mixed
        """
        #pout.v("iset {}".format(self.name))
        if self.is_serialized():
            val = self.encode(val)
        return val

    def isetter(self, v):
        """decorator for setting field's fset function"""
        self.iset = v
        return self

    def fdel(self, orm, val):
        orm.__dict__.pop(self.orm_interface_hash, None)
        return None

    def fdeleter(self, v):
        """decorator for setting field's fdel function"""
        self.fdel = v
        return self

    def idel(self, orm, val):
        """Called when the field is being deleted from the db

        :param orm: Orm
        :param val: mixed, the current value of the field
        :returns: mixed
        """
        orm.__dict__.pop(self.orm_interface_hash, None)
        return None if self.is_pk() else val

    def ideleter(self, v):
        """decorator for setting field's idel function"""
        self.idel = v
        return self

    def fdefault(self, orm, val):
        """On a new Orm instantiation, this will be called for each field and if
        val equals None then this will decide how to use self.default to set
        the default value of the field

        If you just want to set a default value you won't need to override this
        method because you can just pass default into the field instantiation
        and it will get automatically used in this method

        :param orm: Orm, the Orm instance being created
        :param val: mixed, the current value of the field (usually None)
        :returns: mixed
        """
        #pout.v("fdefault {}".format(self.name))
        ret = val
        if val is None:
            if callable(self.default):
                ret = self.default()

            elif self.default is None:
                ret = self.default

            elif isinstance(self.default, (dict, list, set)):
                ret = type(self.default)()

            else:
                ret = self.default

        return ret

    def fdefaulter(self, v):
        """decorator for returning the field's default value

        decorator for setting field's fdefault function"""
        self.fdefault = v
        return self

    def iquery(self, query, val):
        """This will be called when setting the field onto a query instance

        :example:
            o = Orm(foo=1)
            o.query.eq_foo(1) # iquery called here

        :param query: Query
        :param val: mixed, the fields value
        :returns: mixed
        """
        return val

    def iquerier(self, v):
        """decorator for the method called when this field is used in a SELECT query"""
        self.iquery = v
        return self

    def jsonable(self, orm, val):
        if val is None:
            val = self.fdefault(orm, val)

        if val is not None:
            format_str = ""
            if isinstance(val, (datetime.datetime, datetime.date)):
                val = Datetime(val).iso_8601()
        return val

    def jsonabler(self, v):
        """Decorator for the method called for a field when an Orm's .jsonable method
        is called"""
        self.jsonable = v
        return self

    def modified(self, orm, val):
        """Returns True if val has been modified in orm

        :param orm: Orm
        :param val: mixed, the current value of the field
        :returns: bool, True if val is different than the interface val
        """
        if self.is_serialized():
            return True

        ret = True
        ihash = orm.__dict__.get(self.orm_interface_hash, None)
        if ihash:
            ret = self.hash(orm, val) != ihash

        else:
            ret = val is not None

        return ret

    def hash(self, orm, val):
        if self.is_serialized():
            return None
        return hash(val)

    def encode(self, val):
        if val is None: return val

        if self.serializer == "pickle":
            return base64.b64encode(pickle.dumps(val, pickle.HIGHEST_PROTOCOL))

        elif self.serializer == "json":
            return json.dumps(val)

        else:
            raise ValueError("Unknown serializer {}".format(self.serializer))

    def decode(self, val):
        if val is None: return val

        if self.serializer == "pickle":
            return pickle.loads(base64.b64decode(val))

        elif self.serializer == "json":
            return json.loads(val)

        else:
            raise ValueError("Unknown serializer {}".format(self.serializer))

    def fval(self, orm):
        """return the raw value that this property is holding internally for the orm instance"""
        try:
            val = orm.__dict__[self.orm_field_name]
        except KeyError as e:
            #raise AttributeError(str(e))
            val = None

        return val

    def __get__(self, orm, classtype=None):
        """This is the wrapper that will actually be called when the field is
        fetched from the instance, this is a little different than Python's built-in
        @property fget method because it will pull the value from a shadow variable in
        the instance and then call fget"""
        if orm is None:
            # class is requesting this property, so return it
            return self

        raw_val = self.fval(orm)
        ret = self.fget(orm, raw_val)

        # we want to compensate for default values right here, so if the raw val
        # is None but the new val is not then we save the returned value, this
        # allows us to handle things like dict with no surprises
        if raw_val is None:
            if ret is not None:
                orm.__dict__[self.orm_field_name] = ret

        return ret

    def __set__(self, orm, val):
        """this is the wrapper that will actually be called when the field is
        set on the instance, your fset method must return the value you want set,
        this is different than Python's built-in @property setter because the
        fset method *NEEDS* to return something"""
        val = self.fset(orm, val)
        orm.__dict__[self.orm_field_name] = val

    def __delete__(self, orm):
        """the wrapper for when the field is deleted, for the most part the default
        fdel will almost never be messed with, this is different than Python's built-in
        @property deleter because the fdel method *NEEDS* to return something and it
        accepts the current value as an argument"""
        val = self.fdel(orm, self.fval(orm))
        orm.__dict__[self.orm_field_name] = val


if is_py2:
    class Field(_Field):
        __metaclass__ = FieldMeta

else:
    # python 2 parser will fail on metaclass=... syntax, so work around that
    #
    # Order matters for the parent classes
    # https://docs.python.org/3/library/enum.html#restricted-enum-subclassing
    exec("class Field(_Field, metaclass=FieldMeta): pass")


