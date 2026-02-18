import decimal
import datetime
import inspect
import re
import json
import logging
import uuid
import enum
from typing import Any, Self
from dataclasses import MISSING, _MISSING_TYPE

import dsnparse
from datatypes import (
    Datetime,
    cachedproperty,
)
from datatypes.enum import (
    find_value,
    find_enum,
)

from .compat import *
from . import utils


Type = type


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
        return {f:v for f, v in self.fields.items() if not f.startswith("_")}

    @property
    def required_fields(self):
        """The normal required fields (eg, no magic fields like _id are
        included)
        """
        return {f:v for f, v in self.normal_fields.items() if v.is_required()}

    @property
    def persisted_fields(self):
        """The fields that should be saved in the db"""
        return {f:v for f, v in self.fields.items() if v.is_persisted()}

    @property
    def ref_fields(self):
        """Return FK reference fields"""
        return {f:v for f, v in self.normal_fields.items() if v.is_ref()}

    @property
    def magic_fields(self):
        """the magic fields for the schema, magic fields start with an
        underscore
        """
        return {f:v for f, v in self.fields.items() if f.startswith("_")}

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
            raise TypeError(
                f"Type {type(val)} is not a Field or Index instance"
            )

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
    """Each column in the database is configured using this descriptor class

    This implements the descriptor protocol:
        https://docs.python.org/3/howto/descriptor.html

    This implements a subscet of a dataclass field arguments:
        https://docs.python.org/3/library/dataclasses.html#dataclasses.field

    You can set some getters and setters on this object in order to fine tune
    control over how a field in the db is set and fetched from the instance and
    also how it is set and fetched from the interface

    to customize the field on set and get from the db, you can use the
    decorators, which are classmethods:

    :example:
        bar = Field(int, True)

        @bar.fsetter
        def bar(self, val):
            # Orm.bar = ...
            return int(val)

        @bar.fgetter
        def bar(self, val):
            # Orm.bar
            return int(val)

        @bar.fdeleter
        def bar(self, val):
            # del Orm.bar
            return None

        @bar.isetter
        def bar(self, val):
            # insert/update Orm.bar in the db
            return int(val)

        @bar.igetter
        def bar(self, val):
            # get bar column from the db and set into Orm.bar
            return int(val)

        @bar.qset
        def bar(self, query_field, val):
            # Query field is set (different signature)
            return val

        @bar.jset
        def bar(self, orm, name, val):
            # Orm.jsonable (different signature
            return name, val

    .. note:: All these get/set/delete methods are different than traditional
    python getters and setters because they always need to return a value and
    they always take in the object instance manipulating them and the value.

    .. note:: Foreign key Field instances can be passed orm instances from
    other classes because those classes will call the FK's field methods when
    getting/setting the field

    You can also configure the Field using a class:

    :example:
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

            def fset(self, orm, v):
                print("fset")
                return v

            def fdel(self, orm, v):
                print("fdel")
                return v

            def iget(self, orm, v):
                print("iget")
                return v

            def iset(self, orm, v):
                print("iset")
                return v

            def qset(self, query, v):
                print("qset")
                return v

            def jset(self, orm, field_name, v):
                # notice the signature and return val are different than the
                # other methods
                print("jset")
                return field_name, v
    """
    required = False
    """True if this field is required"""

    options = None
    """In the instance, this will be a dict of key/val pairs containing extra
    information about the field"""

    default: Any = None
    """Default value for this field"""

    default_factory: Callable[[], Any]|None = None

    doc = ""
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

        if field_type is Self:
            klass = self.orm_class

        else:
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
            if "_id" in names or self.name == "_id":
                names.add("id")
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
            # this needs to be a new dict so we don't accidently change
            # something important
            options = {**self.schema.pk.options, **self.options}

        else:
            options = self.options

        return options

    def __init__(
        self,
        field_type: Type,
        required: bool = False,
        *,

        name: str = "",
        orm_class: Type|None = None,

        default: Any = None,
        default_factory: Callable[[], Any]|None = None,

        private: bool = False,
        persist: bool = True,

        jsonable: bool = True, # deprecated in favor of `private`
        jsonable_name: str = "",

        fget: Callable[[object, Any], Any]|None = None,
        fset: Callable[[object, Any], Any]|None = None,
        fdel: Callable[[object, Any], Any]|None = None,
        iget: Callable[[object, Any], Any]|None = None,
        iset: Callable[[object, Any], Any]|None = None,
        jset: Callable[[object, str, Any], tuple[str, Any]]|None = None,
        qset: Callable[[object, Any], Any]|None = None,

        size: int = -1,
        min_size: int = -1,
        max_size: int = -1,

        pk: bool = False,
        auto: bool = False,

        unique: bool = False,
        index: bool = False,
        ignore_case: bool = False,
        empty: bool = True,
        choices: Collection|None = None,
        regex: str = "",

        doc: str = "",

        **options,
    ):
        """Create a field

        :param field_type: the python type of the field, so for a string
            you would pass str, integer: int, boolean: bool, float: float
        :param required: True if this field has to be there to insert
        :keyword size: the size you want the string to be, or the int to be
        :keyword min_size: the minimum size
        :keyword max_size: if you want a varchar, set this
        :keyword unique: True to set a unique index on this field, this is
            just for convenience and is equal to `Schema.set_index(field_name,
            [field_name], unique=True)`. this is a convenience option to set
            a unique index on the field without having to add a separate
            index statement
        :keyword index: True if this field is indexed
        :keyword ignore_case: True to ignore case if this field is used in
            indexes
        :keyword default: defaults to None, can be anything the db can support
        :keyword default_factory: a zero-argument callable that will be called
            to create the default value, this is only used if `default=None`
        :keyword jsonable_name: str, the name of the field when `Orm.jsonable`
            is called
        :keyword jsonable: If False then this field will not be part of the
            `Orm.jsonable` return value
        :keyword empty: (default is True), set to False if the value cannot
            be empty when being sent to the db (empty is None, "", 0,
            or False)
        :keyword persist: True then the field will be persisted into the db,
            if False, then the field will only be on the instance it was
            set on, this is handy to hook into lifecycle methods using the
            value but not save the value in the db (like passwords)
        :keyword private: True to mark this field as private, meaning it won't
            show up in jsonable
        :keyword pk: True to make this field the primary key
        :keyword auto: True to tag this field as an auto-generated field.
            Used with an int to set it to an auto-increment, use it with
            UUID to have uuids auto generated
        :keyword choices: a set of values that field value has to be in in
            order to be considered valid before persisting
        :keyword regex: a string that the value has to match in order to
            be persisted
        :keyword doc: The description of this field
        :keyword **options: anything else
        """
        d = self.get_size(
            size=size,
            min_size=min_size,
            max_size=max_size,
        )
        options.update(d)

        for k in list(options.keys()):
            if hasattr(self, k):
                setattr(self, k, options.pop(k))

        self.options = options

        if choices or not self.choices:
            self.choices = choices

        options.setdefault("jsonable_field", jsonable)
        if jsonable_name:
            options["jsonable_name"] = jsonable_name

        options["pk"] = pk
        options["auto"] = auto

        options["empty"] = empty and not pk and not auto
        options["ignore_case"] = ignore_case
        options["regex"] = regex

        if default is not None and default_factory is not None:
            raise ValueError("Cannot specify both default and default_factory")

        elif default_factory is not None:
            if not callable(default_factory):
                raise ValueError("Uncallable default_factory")

        self.default = default
        self.default_factory = default_factory

        self.required = required or self.is_pk()
        self.unique = unique
        self.index = index
        self.doc = doc

        options.setdefault("persist", persist)
        options.setdefault("private", private)

        if fget:
            self.fget = fget

        if fset:
            self.fset = fset

        if fdel:
            self.fdel = fdel

        if iget:
            self.iget = iget

        if iset:
            self.iset = iset

        if jset:
            self.jset = jset

        if qset:
            self.qset = qset

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
                    self.orm_field_name,
                )
            )

    def get_size(self, size, min_size, max_size):
        """Internal method. Figure out if this field has any size information
        """
        d = {}

        if size >= 0:
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

            elif field_type is Self:
                # no .schema property will make .schema treat .original_type
                # as `.orm_class`
                del self.schema

                if self.required == True:
                    raise ValueError("Self fields cannot be required")

            else:
                raise ValueError("Unknown field type {}".format(field_type))

    def is_pk(self) -> bool:
        """return True if this field is a primary key"""
        return self.options.get("pk", False)

    def is_auto(self) -> bool:
        """Return True if this field auto-generates its value somehow"""
        return self.options.get("auto", False)

    def is_ref(self) -> bool:
        """return true if this field foreign key references the primary key of
        another orm"""
        return bool(self.schema)

    def is_required(self) -> bool:
        """Return True if this field is required to save into the interface"""
        return self.required and self.is_persisted()

    def is_persisted(self) -> bool:
        """Return True if field should be persisted in the db"""
        return self.options.get("persist", True)

    def is_private(self) -> bool:
        """Return True if field should be considered private/internal"""
        return self.options.get("private", True)

    def is_serialized(self) -> bool:
        """Return True if this field should be serialized"""
        return True if self.serializer else False

    def is_enum(self) -> bool:
        """Return True if this field represents an enum value"""
        try:
            return issubclass(self.original_type, enum.Enum)

        except TypeError:
            return False

    def is_jsonable(self) -> bool:
        """Returns True if this field should be in .jsonable output"""
        return (
            self.options.get("jsonable_field", True)
            and not self.is_private()
        )
#         return self.options.get("jsonable_field", True)

    def _get_orm_value(self, orm):
        """Internal method. Get the raw value that this property is holding
        internally for the orm instance"""
        try:
            return orm.__dict__[self.orm_field_name]

        except KeyError as e:
            raise AttributeError(self.orm_field_name) from e

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

    def get_default(self, orm, val):
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
        if val is None:
            if self.default_factory is not None:
                ret = self.default_factory()

            else:
                ret = self.default

        else:
            ret = val

        return ret

    ###########################################################################
    # File set/get methods
    ###########################################################################

    def fgetter(self, v):
        """decorator for setting field's fget function"""
        self.fget = v
        return self

    def fsetter(self, v):
        """decorator for setting field's fset function"""
        self.fset = v
        return self

    def fdeleter(self, v):
        """decorator for setting field's fdel function"""
        self.fdel = v
        return self

    def fget(self, orm, val):
        """Called anytime the field is accessed through the Orm (eg, Orm.foo)

        :param orm: Orm, the Orm instance the field is being accessed on
        :param val: mixed, the current value of the field
        :returns: mixed
        """
        return val

    def fset(self, orm, val):
        return val

    def fdel(self, orm, val):
        return None

    def to_value(self, orm, val):
        """This is called on Orm instantiation and any time field is set (eg
        Orm.foo = ...)

        on Orm creation val will be None if the field wasn't passed to
        Orm.__init__ otherwise it will be the value passed into Orm.__init__

        :param orm: Orm, the Orm instance the field is being set on
        :param val: mixed, the current value of the field
        :returns: mixed
        """
        orm_class = orm.__class__ if orm else self.orm_class
        logger.debug(f"{orm_class.__name__}.{self.name}.to_value")

        val = self.fset(orm, val)

        if val is not None:
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

            if self.is_enum():
                val = find_enum(self.original_type, val)
                #val = find_value(self.original_type, val)

        if self.is_ref():
            # Foreign Keys get passed through their Field methods
            val = self.schema.pk.to_value(None, val)

        return val

    def from_value(self, orm, val):
        """This is the wrapper that will actually be called when the field is
        fetched from the instance, this is a little different than Python's
        built-in @property fget method because it will pull the value from a
        shadow variable in the instance and then call fget"""
        logger.debug(f"{orm.__class__.__name__}.{self.name}.from_value")

        val = self.fget(orm, val)

        if self.is_ref():
            # Foreign Keys get passed through their Field methods
            val = self.schema.pk.from_value(orm, val)

        return val

    def del_value(self, orm, val):
        """Internal wrapper method for `.fdel`"""
        orm_class = orm.__class__ if orm else self.orm_class
        logger.debug(f"{orm_class.__name__}.{self.name}.del_value")

        val = self.fdel(orm, val)

        return val

    def __get__(self, orm, orm_class=None):
        if orm is None:
            # class is requesting this property, so return it
            return self

        try:
            raw_val = self._get_orm_value(orm)

        except AttributeError:
            raw_val = None
            ret = self.get_default(orm, raw_val)

        else:
            ret = self.from_value(orm, raw_val)

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
        val = self.to_value(orm, val)
        orm.__dict__[self.orm_field_name] = val

    def __delete__(self, orm):
        """Descriptor `del` method. This will call `.del_value` and if
        the returned value is None then it will delete the field from `orm`
        so the `orm` will be in a state like it had never had a value for
        field. If value is anything other than None then `orm` will have
        that value set and this acts more like a setter"""
        try:
            val = self.del_value(orm, self._get_orm_value(orm))

            if val is None:
                orm.__dict__.pop(self.orm_field_name, None)
                orm.__dict__.pop(self.orm_interface_hash, None)

            else:
                orm.__dict__[self.orm_field_name] = val

        except AttributeError:
            pass

    ###########################################################################
    # Interface persist set/get methods
    ###########################################################################

    def igetter(self, v):
        """decorator for the method called when a field is pulled from the
        database"""
        self.iget = v
        return self

    def isetter(self, v):
        """decorator for setting field's fset function"""
        self.iset = v
        return self

    def iget(self, orm, val):
        return val

    def iset(self, orm, val):
        return val

    def to_interface(self, orm, val):
        """Called anytime the field is being fetched to send to the interface

        think of this as when the interface is going to get the field value or
        when the field is being sent to the db. Alot of the value checks like
        required and empty are in Orm.to_interface(), this just returns the
        value and nothing else

        :param orm: Orm
        :param val: Any, the current value of the field
        :returns: Any
        """
        orm_class = orm.__class__ if orm else self.orm_class
        logger.debug(f"{orm_class.__name__}.{self.name}.to_interface")

        val = self.iset(orm, val)

        if self.is_ref():
            # Foreign Keys get passed through their Field methods
            val = self.schema.pk.to_interface(None, val)

        elif self.is_enum():
            val = find_value(self.original_type, val)

        elif self.is_serialized():
            val = self.encode(val)

        return val

    def from_interface(self, orm, val):
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
        orm_class = orm.__class__ if orm else self.orm_class
        logger.debug(f"{orm_class.__name__}.{self.name}.from_interface")

        val = self.iget(orm, val)

        if self.is_ref():
            # Foreign Keys get passed through their Field methods
            val = self.schema.pk.from_interface(None, val)

        elif self.is_enum():
            val = find_enum(self.original_type, val)

        else:
            if self.is_serialized():
                val = self.decode(val)

            if orm:
                orm.__dict__[self.orm_interface_hash] = self.hash(orm, val)

        return val

    def del_interface(self, orm, val):
        """Called when the field is being deleted from the db

        When you delete an orm instance from the db then it resorts to how it
        would look if a new orm instance was created and it has never been
        saved. This method helps the orm instance that was just deleted get
        back to that pre-saved state

        :param orm: Orm
        :param val: mixed, the current value of the field
        :returns: mixed
        """
        orm_class = orm.__class__ if orm else self.orm_class
        logger.debug(f"{orm_class.__name__}.{self.name}.del_interface")
        orm.__dict__.pop(self.orm_interface_hash, None)
        return None if self.is_pk() else val

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

    ###########################################################################
    # Query and json set methods
    ###########################################################################

    def qsetter(self, v):
        """decorator for the method called when this field is used in a SELECT
        query"""
        self.qset = v
        return self

    def jsetter(self, v):
        """Decorator for the method called for a field when an Orm's .jsonable
        method is called"""
        self.jset = v
        return self

    def qset(self, query_field, val):
        return val

    def jset(self, orm, name, val):
        if val is None:
            val = self.get_default(orm, val)

        return name, val

    def to_query(self, query_field, val):
        """This will be called when setting the field onto a query instance

        :example:
            o = Orm(foo=1)
            o.query.eq_foo(1) # to_query called here

        :param query_field: QueryField
        :param val: mixed, the fields value
        :returns: mixed
        """
        val = self.qset(query_field, val)

        if self.is_enum():
            if val is not None:
                val = find_value(self.original_type, val)

        elif self.is_ref():
            # Foreign Keys get passed through their Field methods
            val = self.schema.pk.qset(query_field, val)

        return val

    def jsonable(self, orm, name, val):
        """This is called in Orm.jsonable() to set the field name and value

        :param orm: Orm, the instance currently calling jsonable
        :param name: str, the Orm field name that will be used if the
            jsonable_name option isn't set
        :param val: Any, the field's value
        :returns: tuple[str, Any], (name, val) where the name will be the
            jsonable field name and the value will be the jsonable value
        """
        orm_class = orm.__class__ if orm else self.orm_class
        logger.debug(f"{orm_class.__name__}.{self.name}.jsonable")

        if self.is_jsonable():
            if self.is_ref():
                # Foreign Keys get passed through their Field methods
                _, val = self.schema.pk.jsonable(None, name, val)
                name = self.options.get("jsonable_name", name)

            else:
                name = self.options.get("jsonable_name", name)

                if self.is_enum():
                    val = find_value(self.original_type, val)

                name, val = self.jset(orm, name, val)

                if val is not None:
                    format_str = ""
                    if isinstance(val, (datetime.datetime, datetime.date)):
                        val = Datetime(val).isoformat()

                    elif isinstance(val, uuid.UUID):
                        val = str(val)

            return name, val

        else:
            return "", None

    def configure(self, orm_class: type, schema: Schema, name: str) -> None:
        """Called once from `Orm.create_schema` when the schema is created
        and is a hook to let fields customize themselves

        :param orm_class: type[Orm]
        :param schema: the schema belonging to `orm_class`
        :param name: the field's name in `orm_class` and `schema.fields`
        """
        return


class AutoUUID(Field):
    """an auto-generating UUID field, by default this will be set as primary
    key"""
    def __init__(self, **kwargs):
        kwargs.setdefault("pk", True)
        kwargs.setdefault("auto", True)
        kwargs.setdefault("size", 36)
        super().__init__(uuid.UUID, True, **kwargs)


class AutoIncrement(Field):
    """an auto-incrementing Serial field, by default this will be set as
    primary key"""
    def __init__(self, **kwargs):
        kwargs.setdefault("pk", True)
        kwargs.setdefault("auto", True)
        kwargs.setdefault("max_size", 9223372036854775807)
        super().__init__(int, True, **kwargs)


class AutoDatetime(Field):
    """A special field that will create a datetime according to triggers

    :example:
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

        super().__init__(datetime.datetime, True, **kwargs)

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

