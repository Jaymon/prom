# -*- coding: utf-8 -*-
import decimal
import datetime
import inspect
import re
import json
import logging
import uuid
import enum

import dsnparse
from datatypes import (
    Datetime,
    cachedproperty,
)
from datatypes.enum import (
    find_value,
)

from .compat import *
from . import utils


logger = logging.getLogger(__name__)


class Connection(object):
    """set the paramaters you want to use to connect to an interface

    https://github.com/Jaymon/Mingo/blob/master/Mingo/MingoConfig.php
    """
    name = ""
    """string -- the name of this connection (eg, Postgres, or SQLite)"""

    interface_name = ""
    """string -- full Interface class name -- the interface the connection
    should use to talk with the db"""

    interface_class = None
    """Holds the interface class this connection should use"""

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
        set all the values by passing them into this constructor, any
        unrecognized kwargs get put into .options

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

        interface_module, interface_class = utils.get_objects(
            self.interface_name
        )
        self.interface_class = interface_class

    def get(self, key, default=None):
        """Works similar dict.get"""
        return getattr(self, key, default)


class DsnConnection(Connection):
    """
    sets up a connection configuration using a "prom dsn" or connection string

    the prom dsn is in the form:

        <SCHEME>://<HOST>/<PATH>?<QUERY>#<FRAGMENT>

    Where <SCHEME> is:

        <full.python.path.InterfaceClass>

    And <HOST> is:

        ://<username>:<password>@<host>:<port>

    And <PATH> is:

        /<database>

    And <QUERY> is:

        ?<options=val&query=string>

    And <FRAGMENT> is:

        #<connection-name>

    This is useful to allow connections coming in through environment variables
    as described in :
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
        super().__init__(**kwargs)

    def parse(self, dsn):
        parser = dsnparse.parse(dsn)

        d = parser.fields
        d["dsn"] = parser.parser.dsn

        # remap certain values
        d["name"] = d.pop("fragment")
        d["interface_name"] = self.normalize_scheme(d.pop("scheme"))
        d["database"] = parser.database
        d["options"] = d.pop("query_params", {})
        d["host"] = d.pop("hostname")
        d["readonly"] = bool(d["options"].pop("readonly", self.readonly))

        # get rid of certain values
        d.pop("params", None)

        return d

    def normalize_scheme(self, v):
        ret = v
        d = {
            "prom.interface.sqlite.SQLite": set(["sqlite"]),
            "prom.interface.postgres.PostgreSQL": set(
                ["postgres", "postgresql", "psql"]
            )
        }

        kv = v.lower()
        for interface_name, vals in d.items():
            if kv in vals:
                ret = interface_name
                break

        return ret


class Schema(object):
    """
    handles all table schema definition

    the table schema definition includes the table name, the fields the table
    has, and the indexes that are on the table
    """
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
        """The normal required fields (eg, no magic fields like _id are
        included)
        """
        return {f:v for f, v in self.normal_fields.items() if v.required}

    @property
    def ref_fields(self):
        """Return FK reference fields"""
        return {f:v for f, v in self.normal_fields.items() if v.is_ref()}

    @property
    def magic_fields(self):
        """the magic fields for the schema, magic fields start with an
        underscore
        """
        return {f:v for f, v in self.fields.items() if f.startswith('_')}

    @property
    def pk_fields(self):
        pk_fields = {}
        for pk_name in self.pk_names:
            pk_fields[pk_name] = self.fields[pk_name]
        return pk_fields

    @property
    def pk_name(self):
        """returns the primary key name for this schema"""
        try:
            pk_field = self.__getattr__("pk")
            pk_name = pk_field.name

        except AttributeError:
            pk_name = None

        return pk_name

    @property
    def pk_names(self):
        """Returns all the field names comprising the primary key as a list"""
        pk_name = self.pk_name
        return [pk_name] if pk_name else []

    @property
    def schemas(self):
        """Find and return all the schemas that are needed for this schema to 
        install successfully

        Another way to put this is all the schemas this Schema touches

        :returns: list, a list of Schema instances, self will be at the end
        """
        schemas = []
        for f in self.fields.values():
            if s := f.schema:
                schemas.extend(s.schemas)
        schemas.append(self)

        return schemas

    def __init__(self, table_name, orm_class=None, **fields_or_indexes):
        """Create an instance

        every Orm should have a .schema attribute that is an instance of this
        class

        :example:
            schema = Schema(
                "table_name"
                field1=Field(int, True),
                field2=Field(str),
                index_fields=Index("field1", "field2")
            )

        :param table_name: string, the table name
        :param **fields_or_indexes: a dict of field name or index keys with
            tuple values, see __getattr__ for more details
        """
        self.fields = {}
        self.indexes = {}
        self.table_name = String(table_name)
        self.orm_class = orm_class
        self.lookup = {
            "field_names": {},
            # holds parent's Field.names that have been set to None in children
            # classes. Basically, if a parent class sets foo and then a child
            # class later on sets foo = None then foo.names will be here
            "field_names_deleted": {},
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
            raise TypeError("Not a Field or Index instance")

    def __getattr__(self, field_name):
        """return the Field instance for the given name

        :returns: str, the string value of the attribute name, eg, self.foo
            returns "foo"
        """
        try:
            return self.get_field(field_name)
            #return self.lookup["field_names"][name]

        except KeyError:
            raise AttributeError(
                "No {} field in Schema {}".format(field_name, self.table_name)
            )

    def __contains__(self, field_name):
        return self.has_field(field_name)

    def has_field(self, field_name):
        """Return True if schema contains field_name"""
        return field_name in self.lookup["field_names"]

    def set_field(self, field_name, field):
        if not field_name:
            raise ValueError("field_name is empty")

        if field_name in self.fields:
            raise ValueError(
                f"{field_name} already exists and cannot be changed"
            )

        if not isinstance(field, Field):
            raise ValueError(f"{type(field)} is not a Field instance")

        field.__set_name__(self.orm_class, field_name)

        if field.unique:
            self.set_index(field_name, Index(field_name, unique=True))

        if field.index:
            self.set_index(field_name, Index(field_name))

        self.fields[field_name] = field

        for fn in field.names:
            if fn in self.lookup["field_names"] and fn in field.aliases:
                self.lookup["field_names"].pop(fn)
                logger.warning(
                    " ".join([
                        "{} ignored alias {} for {} because it is".format(
                            self,
                            fn,
                            field.name,
                        ),
                        "used by another field",
                    ])
                )

            else:
                self.lookup["field_names"][fn] = field

        return self

    def get_field(self, field_name):
        return self.lookup["field_names"][field_name]

    def set_index(self, index_name, index):
        """Add an index to the schema

        for the most part, you will use the __getattr__ method of adding
        indexes for a more fluid interface, but you can use this if you want to
        get closer to the bare metal

        :param index_name: str, the name of the index
        :param index: Index, an Index instance
        """
        if not index_name:
            raise ValueError("index_name must have a value")

        if index_name in self.indexes:
            raise ValueError(
                "index_name {} has already been defined on {}".format(
                    index_name, str(self.indexes[index_name].field_names)
                )
            )

        if not isinstance(index, Index):
            raise ValueError(f"{type(index)} is not an Index instance")

        index.name = index_name
        index.orm_class = self.orm_class

        self.indexes[index_name] = index
        return self

    def field_name(self, field_name, *default):
        """Get the canonical field name of field_name

        most of the time, the field name of field_name will just be field_name,
        but this checks the configured aliases to return the canonical name

        :param field_name: str, the field_name you want the canonical field
            name for
        :param *default: mixed, if present this will be returned instead of
            AttributeError raised if field_name doesn't exist
        """
        try:
            return self.__getattr__(field_name).name

        except AttributeError:
            if default:
                return default[0]

            else:
                if field_name in self.lookup["field_names_deleted"]:
                    try:
                        return self.lookup["field_names_deleted"][field_name]
                    except KeyError as e:
                        raise AttributeError(field_name) from e

                else:
                    raise

    def field_model_name(self, field_name):
        """Check field_name against all the field's ref model names to see if
        there is a match, this is separate from .field_name because there are
        times when this behavior might not be desirable

        :param field_name: str, the field/model name you want the canonical
            field name for
        :returns: str, the canonical field name
        """
        for fn, field in self.fields.items():
            if field_name in field.ref_names:
                return fn

        raise AttributeError(field_name)


class Index(object):
    """Each index on the table is configured using this class
    """
    def __init__(self, *field_names, **options):
        """initialize an index

        :param *field_names: the string field names this index will index on,
            fields have to be already added to this schema index
        :param **options:
            - unique: bool, True if the index should be unique, false otherwise
        """
        if not field_names:
            raise ValueError("field_names list is empty")

        self.name = ""
        self.field_names = list(map(String, field_names))
        self.options = options
        self.unique = options.get("unique", False)


class Field(object):
    """Each column in the database is configured using this class

    You can set some getters and setters on this object in order to fine tune
    control over how a field in the db is set and fetched from the instance and
    also how it is set and fetched from the interface

    to customize the field on set and get from the db, you can use the
    decorators, which are classmethods:

    :Example:
        foo = Field(str, True)

        @foo.igetter
        def foo(self, val):
            # do custom things when pulling foo from the database
            return val

        @foo.isetter
        def foo(self, val):
            # do custom things when putting foo into the database
            return val

    There are also fget/fset/fdel methods that can be set to customize behavior
    on when a value is set on a particular orm instance, so if you wanted to
    make sure that bar was always an int when it is set, you could:

    :Example:
        bar = Field(int, True)

        @bar.fsetter
        def bar(self, val):
            # do custom things
            return int(val)

        @bar.fgetter
        def bar(self, val):
            return int(val)

        @bar.jsonabler
        def bar(self, name, val):
            # convert val to something json safe, you can also change the name
            return name, val

    NOTE -- all these get/set/delete methods are different than traditional
    python getters and setters because they always need to return a value and
    they always take in the object instance manipulating them and the value.

    NOTE -- Foreign key Field instances can be passed orm instances from other
    classes because those classes will call the FK's field methods when
    getting/setting the field

    You can also configure the Field using a class:

    :Example:
        class bar(Field):
            type = int
            required = True
            options = {
                "default": 0,
                "unique": True,
                "max_size": 512,
            }

            def fget(self, orm, v):
                print("fget")
                return v

            def iget(self, orm, v):
                print("iget")
                return v

            def fset(self, orm, v):
                print("fset")
                return v

            def fdel(self, orm, v):
                print("fdel")
                return v

            def iquery(self, query, v):
                print("iquery")
                return v

            def jsonable(self, orm, field_name, v):
                print("jsonable")
                return field_name, v

    https://docs.python.org/2/howto/descriptor.html
    """
    required = False
    """True if this field is required"""

    options = None
    """In the instance, this will be a dict of key/val pairs containing extra
    information about the field"""

    default = None
    """Default value for this field"""

    help = ""
    """The description/help message for this field"""

    name = ""
    """The field name"""

    choices = None
    """A set of values that this field can be, if set then no other values can
    be set"""

    unique = False
    """True if this field is unique indexed"""

    index = False
    """True if this field is indexed"""

    orm_class = None
    """Holds the model class this field is defined on"""

    @cachedproperty()
    def schema(self):
        """return the schema instance if this field is a reference to another
        table

        see .set_type() for an explanation on why we defer figuring this out
        until now
        """
        field_type = self.original_type
        module, klass = utils.get_objects(field_type)
        schema = klass.schema
        if not schema:
            raise ValueError(
                "Field type {} is not an Orm class".format(field_type)
            )

        return schema

    @property
    def type(self):
        """alias of the interface type, this is really here so you can set it
        to a value if you are inline defining a field, this will get passed to
        the __init__ method and then be used to set ._interface_type"""
        return self.interface_type

    @property
    def interface_type(self):
        """Returns the type that will be used in the interface to create the
        table

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
    def ref_class(self):
        return self.ref

    @property
    def names(self):
        names = set()
        if self.name:
            names.add(self.name)

        names.update(self.aliases)

        return names

    @property
    def aliases(self):
        names = set()
        names.update(self.options.get("names", []))
        names.update(self.options.get("aliases", []))

        if alias := self.options.get("alias", ""):
            names.add(alias)

        if self.is_pk():
            names.add("pk")
            if self.orm_class:
                model_name = self.orm_class.model_name
                names.add(f"{model_name}_id")
                names.add(f"{model_name}_pk")

        return names

    @property
    def ref_names(self):
        """Return the reference names for this field

        :returns: set[str], basically a set of .ref.model_name and
            .ref.models_name
        """
        names = set()
        if ref_class := self.ref:
            names.add(ref_class.model_name)
            names.add(ref_class.models_name)
        return names

    @property
    def interface_options(self):
        """When the interface is doing stuff with the field it might call this
        method to make sure it has all the options it needs to use the field
        """
        if self.is_ref():
            options = self.schema.pk.options
            options.update(self.options)

        else:
            options = self.options

        return options

    def __init__(
        self,
        field_type,
        field_required=False,
        field_options=None,
        **field_options_kwargs
    ):
        """
        create a field

        :param field_type: type, the python type of the field, so for a string
            you would pass str, integer: int, boolean: bool, float: float
        :param field_required: boolean, true if this field has to be there to
            insert
        :param field_options: Current options:
            * size: int -- the size you want the string to be, or the int to be
            * min_size: int -- the minimum size
            * max_size: int -- if you want a varchar, set this
            * unique: bool -- True to set a unique index on this field, this is
                just for convenience and is equal to self.set_index(field_name,
                [field_name], unique=True). this is a convenience option to set
                a unique index on the field without having to add a separate
                index statement
            * ignore_case: bool -- True to ignore case if this field is used in
                indexes
            * default: mixed -- defaults to None, can be anything the db can
                support
            * jsonable_name: str, the name of the field when Orm.jsonable() is
                called
            * jsonable_field: bool, if False then this field won't be part of
                Orm.jsonable() output
            * jsonable: str|callable|bool,
                - str, will be set to jsonable_name
                - callable, will be used as self.jsonable
                - bool, will set jsonable_field to False
            * empty: bool, (default is True), set to False if the value cannot
                be empty when being sent to the db (empty is None, "", 0,
                or False)
            * pk: bool, True to make this field the primary key
            * auto: bool, True to tag this field as an auto-generated field.
                Used with an int to set it to an auto-increment, use it with
                UUID to have uuids auto generated
            * autopk: bool, shortcut for setting pk=True, auto=True
            * fvalue: callable[Any], called in the default .fset method to
                normalize the value before setting the value as an attribute on
                the orm
            * ivalue: callable[Any], called in the default .iset method to
                normalize the value before sending the value to the db
        :param **field_options_kwargs: dict, will be combined with
            field_options
        """
        field_options = utils.make_dict(field_options, field_options_kwargs)

        d = self.get_size(field_options)
        field_options.update(d)

        name = field_options.pop("name", "")
        orm_class = field_options.pop("orm_class", None)

        choices = field_options.pop("choices", set())
        if choices or not self.choices:
            self.choices = choices

        if "jsonable" in field_options:
            jsonable = field_options.pop("jsonable")
            if isinstance(jsonable, str):
                field_options.setdefault("jsonable_name", jsonable)

            elif isinstance(jsonable, bool):
                field_options.setdefault("jsonable_field", jsonable)

            else:
                field_options["jsonable"] = jsonable

        if autopk := field_options.pop("autopk", False):
            field_options["pk"] = autopk
            field_options["auto"] = autopk

        for k in list(field_options.keys()):
            if hasattr(self, k):
                setattr(self, k, field_options.pop(k))

        self.options = field_options
        self.required = field_required or self.is_pk()

        self.set_type(field_type)

        # this class is being created manually so mimic what the python parser
        # would do
        if name and orm_class:
            self.__set_name__(orm_class, name)

        elif orm_class:
            self.orm_class = orm_class

        elif name:
            self.__set_name__(orm_class, name)

    def __set_name__(self, orm_class, name):
        """This is called right after __init__

        https://docs.python.org/3/howto/descriptor.html#customized-names

        This is only called when an instance is created while a class is being
        parsed/created, so if you just created an instance of Field you would
        need to call this method manually

        :param orm_class: type, the orm class this Field will belong to
        :param name: str, the field's public name on the orm class
        """
        self.orm_class = orm_class
        self.name = name

        # the field name this descriptor will use to set the value onto the orm
        # instance
        self.orm_field_name = f"_{name}"

        # we keep a hash of the field's value when it was pulled from the
        # interface (see .iget) so we know if the field has been modified
        self.orm_interface_hash = f"_interface_{name}_hash"

        if orm_class:
            logger.debug(
                "Field descriptor {}.{} created with internal name {}".format(
                    orm_class.__name__,
                    name,
                    self.orm_field_name
                )
            )

    def get_size(self, field_options):
        """Figure out if this field has any size information"""
        d = {}

        min_size = field_options.pop("min_size", -1)
        max_size = field_options.pop("max_size", -1)
        size = field_options.pop("size", None)

        if size is not None:
            d['size'] = size

        else:
            if min_size > 0 and max_size < 0:
                d['min_size'] = min_size

            elif min_size < 0 and max_size > 0:
                d['max_size'] = max_size

            elif min_size > 0 and max_size > 0:
                d['min_size'] = min_size
                d['max_size'] = max_size

        return d

    def size_info(self):
        """Figure out actual sizing information with all the ways we can
        calculate size now

        :returns: dict, will always have "has_size" and "has_precision" keys
            * if "has_size" key is True then "size" key will exist
            * if "has_precision" is True then "precision" and "scale" keys will
                exist
            * if "bounds" key exists it will be a tuple (min_size, max_size)
        """
        ret = {
            "has_size": False,
            "has_precision": False,
            "original": {},
        }
        options = self.interface_options

        if "size" in options:
            ret["size"] = options["size"]
            ret["original"]["size"] = options["size"]
            ret["has_size"] = True
            ret["bounds"] = (ret["size"], ret["size"])

        if "max_size" in options:
            ret["size"] = max(options["max_size"], ret.get("size", 0))
            ret["original"]["max_size"] = options["max_size"]
            ret["has_size"] = True
            ret["bounds"] = (ret.get("min_size", 0), ret["size"])

        if "min_size" in options:
            ret["size"] = max(options["min_size"], ret.get("size", 0))
            ret["original"]["min_size"] = options["min_size"]
            ret["has_size"] = True
            ret["bounds"] = (options["min_size"], ret["size"])

        if ret["has_size"]:
            # if size is like 15.6 then that would be considered 21
            # precision with a scale of 6 (ie, you can have 15 digits before
            # the decimal point and 6 after)
            parts = str(ret["size"]).split(".")
            if len(parts) > 1:
                ret["scale"] = int(parts[1])
                ret["precision"] = int(parts[0]) + ret["scale"]

                # the set size was actually precision and scale so we don't
                # have sizing information
                ret["has_precision"] = True
                ret.pop("size")

        if "precision" in options:
            # So the number 23.5141 has a precision of 6 and a scale of 4
            precision = options["precision"]
            ret["precision"] = int(precision)
            ret["original"]["precision"] = precision
            ret["has_precision"] = True
            ret["scale"] = 0

            if "scale" in options:
                scale = options["scale"]
                ret["scale"] = int(scale)
                ret["original"]["scale"] = scale

        interface_type = self.interface_type
        if issubclass(interface_type, (int, float, decimal.Decimal)):
            if "precision" in ret and "size" not in ret:
                if ret["scale"]:
                    ret["size"] = float("{}.{}".format(
                        "9" * (ret["precision"] - ret["scale"]),
                        "9" * ret["scale"],
                    ))

                else:
                    ret["size"] = int("9" * ret["precision"])

            elif "precision" not in ret and "size" not in ret:
                # this is 32bit, it might be worth setting defined size to
                # 64bit
                ret["size"] = 2147483647
                ret["precision"] = len(str(ret["size"]))

            elif "precision" not in ret and "size" in ret:
                ret["precision"] = len(str(ret["size"]))

        elif issubclass(interface_type, (str, bytes, bytearray)):
            if ret["has_size"]:
                if not ret["has_precision"]:
                    ret["precision"] = ret["size"]
                    ret["scale"] = 0

            elif ret["has_precision"]:
                ret["size"] = ret["precision"]

        return ret

    def set_type(self, field_type):
        """Try to infer as much about the type as can be inferred at this
        moment

        Because the Field supports string classpaths (eg, "modname.Classname")
        we can't figure everything out in this method, so we figure out as much
        as we can and then defer everything else to the .interface_type and
        .schema properties, this allows the parser to hopefully finish loading
        the modules before we have to parse the classpath to find the foreign
        key schema

        :param field_type: mixed, the field type passed into __init__
        """
        self.original_type = field_type
        self.serializer = ""
        self._interface_type = None
        self.schema = None

        if isinstance(field_type, type):
            std_types = (
                str,
                bool,
                int,
                float,
                bytes,
                bytearray,
                decimal.Decimal,
                datetime.datetime,
                datetime.date,
                uuid.UUID,
                dict,
            )

            json_types = ()

            pickle_types = (
                set,
                list,
            )

            if issubclass(field_type, std_types):
                self._interface_type = field_type

            elif issubclass(field_type, enum.Enum):
                # https://docs.python.org/3/library/enum.html
                for enum_property in field_type:
                    self._interface_type = type(enum_property.value)
                    break

            elif issubclass(field_type, json_types):
                self.serializer = self.options.pop("serializer", "json")
                self._interface_type = str

            elif issubclass(field_type, pickle_types):
                self.serializer = self.options.pop("serializer", "pickle")
                self._interface_type = bytes

            else:
                schema = getattr(field_type, "schema", None)
                if schema:
                    self.schema = schema

                else:
                    # We have just some random class that isn't an Orm
                    self.serializer = self.options.pop("serializer", "pickle")
                    self._interface_type = bytes

        elif isinstance(field_type, Schema):
            self.schema = field_type

        else:
            # check if field_type is a string classpath so we have to defer
            # setting the type
            if isinstance(field_type, basestring):
                # no .schema property will make .schema treat .original_type
                # as a classpath
                del self.schema

            else:
                raise ValueError("Unknown field type {}".format(field_type))

    def is_pk(self):
        """return True if this field is a primary key"""
        return self.options.get("pk", False)

    def is_auto(self):
        """Return True if this field auto-generates its value somehow"""
        return self.options.get("auto", False)

    def is_ref(self):
        """return true if this field foreign key references the primary key of
        another orm"""
        return bool(self.schema)

    def is_required(self):
        """Return True if this field is required to save into the interface"""
        return self.required

    def is_serialized(self):
        """Return True if this field should be serialized"""
        return True if self.serializer else False

    def is_enum(self):
        """Return True if this field represents an enum value"""
        return issubclass(self.original_type, enum.Enum)

    def is_jsonable(self):
        """Returns True if this field should be in .jsonable output"""
        return self.options.get("jsonable_field", True)

    def fget(self, orm, val):
        """Called anytime the field is accessed through the Orm (eg, Orm.foo)

        :param orm: Orm, the Orm instance the field is being accessed on
        :param val: mixed, the current value of the field
        :returns: mixed
        """
        logger.debug(f"fget {orm.__class__.__name__}.{self.name}")
        if self.is_ref():
            # Foreign Keys get passed through their Field methods
            val = self.schema.pk.fget(orm, val)
        return val

    def fgetter(self, v):
        """decorator for setting field's fget function"""
        self.fget = v
        return self

    def iget(self, orm, val):
        """Called anytime the field is being returned from the interface to the
        orm

        think of this as when the orm receives the field value from the
        interface

        :param orm: Orm, the Orm instance the field is being set on. This can
            be None if the select query had selected fields so a full orm
            instance isn't being returned but rather just the selected values
        :param val: mixed, the current value of the field
        :returns: mixed
        """
        logger.debug(f"iget {orm.__class__.__name__}.{self.name}")

        if self.is_ref():
            # Foreign Keys get passed through their Field methods
            val = self.schema.pk.iget(orm, val)

        else:
            if self.is_serialized():
                val = self.decode(val)

            if orm:
                orm.__dict__[self.orm_interface_hash] = self.hash(orm, val)

        return val

    def igetter(self, v):
        """decorator for the method called when a field is pulled from the
        database"""
        self.iget = v
        return self

    def fset(self, orm, val):
        """This is called on Orm instantiation and any time field is set (eg
        Orm.foo = ...)

        on Orm creation val will be None if the field wasn't passed to
        Orm.__init__ otherwise it will be the value passed into Orm.__init__

        :param orm: Orm, the Orm instance the field is being set on
        :param val: mixed, the current value of the field
        :returns: mixed
        """
        logger.debug(f"fset {orm.__class__.__name__}.{self.name}")

        if self.is_enum():
            val = self._fset_enum(orm, val)

        else:
            if val is not None:
                if fvalues := self.options.get("fvalue", None):
                    if callable(fvalues):
                        fvalues = [fvalues]

                    for fvalue in fvalues:
                        val = fvalue(val)

                if self.choices and val not in self.choices:
                    raise ValueError(
                        "Value {} not in {} value choices".format(
                            val,
                            self.name
                        )
                    )

                if regex := self.options.get("regex", ""):
                    if not re.search(regex, val):
                        raise ValueError(
                            "regex failed for {}.{}".format(
                                orm.__class__.__name__,
                                self.name
                            )
                        )

            if self.is_ref():
                # Foreign Keys get passed through their Field methods
                val = self.schema.pk.fset(orm, val)

        return val

    def _fset_enum(self, orm, val):
        """This is set using .fsetter in .set_type when an Enum is identified
        """
        if val is not None:
            val = find_value(self.original_type, val)

        return val

    def fsetter(self, v):
        """decorator for setting field's fset function"""
        self.fset = v
        return self

    def iset(self, orm, val):
        """Called anytime the field is being fetched to send to the interface

        think of this as when the interface is going to get the field value or
        when the field is being sent to the db. Alot of the value checks like
        required and empty are in Orm.to_interface(), this just returns the
        value and nothing else

        :param orm: Orm
        :param val: Any, the current value of the field
        :returns: Any
        """
        logger.debug(f"iset {orm.__class__.__name__}.{self.name}")

        if val is not None:
            if ivalues := self.options.get("ivalue", None):
                if callable(ivalues):
                    ivalues = [ivalues]

                for ivalue in ivalues:
                    val = ivalue(val)

        if self.is_ref():
            # Foreign Keys get passed through their Field methods
            val = self.schema.pk.iset(orm, val)

        else:
            if self.is_serialized():
                val = self.encode(val)

        return val

    def isetter(self, v):
        """decorator for setting field's fset function"""
        self.iset = v
        return self

    def fdel(self, orm, val):
        logger.debug(f"fdel {orm.__class__.__name__}.{self.name}")
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
        logger.debug(f"idel {orm.__class__.__name__}.{self.name}")
        orm.__dict__.pop(self.orm_interface_hash, None)
        return None if self.is_pk() else val

    def ideleter(self, v):
        """decorator for setting field's idel function"""
        self.idel = v
        return self

    def fdefault(self, orm, val):
        """On a new Orm instantiation, this will be called for each field and
        if val equals None then this will decide how to use self.default to set
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

    def iquery(self, query_field, val):
        """This will be called when setting the field onto a query instance

        :example:
            o = Orm(foo=1)
            o.query.eq_foo(1) # iquery called here

        :param query_field: QueryField
        :param val: mixed, the fields value
        :returns: mixed
        """
        if self.is_enum():
            val = self._iquery_enum(query_field, val)

        else:
            if self.is_ref():
                # Foreign Keys get passed through their Field methods
                val = self.schema.pk.iquery(query_field, val)

        return val

    def _iquery_enum(self, query_field, val):
        """This is set using .iquerier in .set_type when an Enum is identified
        """
        if val is not None:
            val = find_value(self.original_type, val)

        return val

    def iquerier(self, v):
        """decorator for the method called when this field is used in a SELECT
        query"""
        self.iquery = v
        return self

    def jsonable(self, orm, name, val):
        """This is called in Orm.jsonable() to set the field name nad value

        :param orm: Orm, the instance currently calling jsonable
        :param name: str, the Orm field name that will be used if the
            jsonable_name option isn't set
        :param val: Any, the field's value
        :returns: tuple[str, Any], (name, val) where the name will be the
            jsonable field name and the value will be the jsonable value
        """
        if self.options.get("jsonable_field", True):
            logger.debug(
                f"jsonable {orm.__class__.__name__}.{self.name} -> {name}"
            )

            if self.is_ref():
                # Foreign Keys get passed through their Field methods
                _, val = self.schema.pk.jsonable(
                    orm,
                    name,
                    val
                )

            else:
                if val is None:
                    val = self.fdefault(orm, val)

                if val is not None:
                    format_str = ""
                    if isinstance(val, (datetime.datetime, datetime.date)):
                        val = Datetime(val).isoformat()

                    elif isinstance(val, uuid.UUID):
                        val = str(val)

            return self.options.get("jsonable_name", name), val

        else:
            logger.debug(
                f"jsonable ignored for {orm.__class__.__name__}.{self.name}"
            )
            return "", None

    def jsonabler(self, v):
        """Decorator for the method called for a field when an Orm's .jsonable
        method is called"""
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

        elif isinstance(val, dict):
            return None

        return hash(val)

    def encode(self, val):
        if val is None: return val

        if self.serializer == "pickle":
            return pickle.dumps(val, pickle.HIGHEST_PROTOCOL)

        elif self.serializer == "json":
            return json.dumps(val)

        else:
            raise ValueError("Unknown serializer {}".format(self.serializer))

    def decode(self, val):
        if val is None: return val

        if self.serializer == "pickle":
            #return pickle.loads(base64.b64decode(val))
            return pickle.loads(val)

        elif self.serializer == "json":
            return json.loads(val)

        else:
            raise ValueError("Unknown serializer {}".format(self.serializer))

    def fval(self, orm):
        """return the raw value that this property is holding internally for
        the orm instance"""
        try:
            val = orm.__dict__[self.orm_field_name]

        except KeyError as e:
            #raise AttributeError(str(e))
            val = None

        return val

    def __get__(self, orm, orm_class=None):
        """This is the wrapper that will actually be called when the field is
        fetched from the instance, this is a little different than Python's
        built-in @property fget method because it will pull the value from a
        shadow variable in the instance and then call fget"""
        if orm is None:
            # class is requesting this property, so return it
            return self

        raw_val = self.fval(orm)
        ret = self.fget(orm, raw_val)

        # we want to compensate for default values right here, so if the raw
        # val is None but the new val is not then we save the returned value,
        # this allows us to handle things like dict with no surprises
        if raw_val is None:
            if ret is not None:
                orm.__dict__[self.orm_field_name] = ret

        return ret

    def __set__(self, orm, val):
        """this is the wrapper that will actually be called when the field is
        set on the instance, your fset method must return the value you want
        set, this is different than Python's built-in @property setter because
        the fset method *NEEDS* to return something
        """
        val = self.fset(orm, val)
        orm.__dict__[self.orm_field_name] = val

    def __delete__(self, orm):
        """the wrapper for when the field is deleted, for the most part the
        default fdel will almost never be messed with, this is different than
        Python's built-in @property deleter because the fdel method *NEEDS* to
        return something and it accepts the current value as an argument"""
        val = self.fdel(orm, self.fval(orm))
        orm.__dict__[self.orm_field_name] = val


class AutoUUID(Field):
    """an auto-generating UUID field, by default this will be set as primary
    key"""
    def __init__(self, **kwargs):
        kwargs.setdefault("pk", True)
        kwargs.setdefault("auto", True)
        kwargs.setdefault("size", 36)
        super().__init__(
            field_type=uuid.UUID,
            field_required=True,
            **kwargs
        )


class AutoIncrement(Field):
    """an auto-incrementing Serial field, by default this will be set as
    primary key"""
    def __init__(self, **kwargs):
        kwargs.setdefault("pk", True)
        kwargs.setdefault("auto", True)
        kwargs.setdefault("max_size", 9223372036854775807)
        super().__init__(
            field_type=int,
            field_required=True,
            **kwargs
        )


class AutoDatetime(Field):
    """A special field that will create a datetime according to triggers

    :Example:
        # datetime will be auto-populated on creation
        created = AutoDatetime(created=True)

        # datetime will be auto-populated on creation and update
        updated = AutoDatetime(updated=True)

    The triggers are:
        created: create a datetime in the field when the Orm is being created
            (added to the db for the first time)
        updated: update the datetime when the Orm is created and when it is
            updated in the db
    """
    def __init__(self, **kwargs):
        kwargs.setdefault("created", kwargs.get("create", True))
        kwargs.setdefault("updated", kwargs.get("update", False))
        kwargs.setdefault("auto", True)
        if kwargs["updated"]:
            kwargs["created"] = False

        super().__init__(
            field_type=datetime.datetime,
            field_required=True,
            **kwargs,
        )

    def iset(self, orm, val):
        if self.options.get("updated", False):
            return self.updated_iset(orm, val)

        elif self.options.get("created", False):
            return self.created_iset(orm, val)

        else:
            return super().iset(orm, val)

    def created_iset(self, orm, val):
        if not val and orm.is_insert():
            val = Datetime()
        return val

    def updated_iset(self, orm, val):
        if val:
            if not self.modified(orm, val):
                val = Datetime()

        else:
            val = Datetime()

        return val

