# -*- coding: utf-8 -*-
from __future__ import unicode_literals, division, print_function, absolute_import
import datetime
import inspect
import re
import base64
import json
import logging

import dsnparse

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

#     @property
#     def interface_class(self):
#         """Return the configured interface class object that can be used to create new instances"""
#         interface_module, interface_class = utils.get_objects(self.interface_name)
#         return interface_class

    @property
    def interface(self):
        """Return a new Interface instance using this configuration"""
        interface_class = self.interface_class
        return interface_class(self)

    def __init__(self, fields=None, **fields_kwargs):
        """
        set all the values by passing them into this constructor, any unrecognized kwargs get put into .options

        example --
            c = Connection(
                host="127.0.0.1",
                database="dbname",
                port=5000,
                some_random_thing="foo"
            )

            print c.port # 5000
            print c.options # {"some_random_thing": "foo"}
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

    This is useful to allow connections coming in through environment variables as described
    http://www.12factor.net/backing-services

    It tooks its inspiration from this project https://github.com/kennethreitz/dj-database-url

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

    table_name = u""
    """string -- set the table name for this schema instance"""

    fields = None
    """dict -- all the fields this schema instance will use"""

    indexes = None
    """dict -- all the indexes this schema will have"""

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
            return pk_field.name

        except AttributeError:
            pk_field = None
            return None

    @classmethod
    def get_instance(cls, orm_class):
        table_name = orm_class.table_name
        if table_name not in cls.instances:
            s = cls(table_name)
            seen_properties = set()
            for klass in inspect.getmro(orm_class)[:-1]:
                for k, v in vars(klass).items():
                    if k not in seen_properties:
                        if isinstance(v, (Field, Index)):
                            v.orm_class = orm_class
                            s.set(k, v)
                        seen_properties.add(k)

            #s.orm_class = orm_class
            cls.instances[table_name] = s

        else:
            s = cls.instances[table_name]

        s.orm_class = orm_class
        return s

    def __init__(self, table_name, **fields_or_indexes):
        """
        create an instance

        every Orm should have a .schema attribute that is an instance of this class

        example --

            schema = Schema(
                "table_name"
                field1=Field(int, True),
                field2=Field(str),
                index_fields=Index("field1", "field2")
            )

        table_name -- string -- the table name
        **fields_or_indexes -- a dict of field name or index keys with tuple values, see __getattr__ for more details
        """
        self.fields = {}
        self.indexes = {}
        self.table_name = str(table_name)

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
        if name in self.fields:
            return self.fields[name]

        else:
            if name == "pk":
                for field_name, field in self.fields.items():
                    if field.options.get('pk', False):
                        return field

            raise AttributeError("No {} field in schema {}".format(name, self.table_name))

    def set_field(self, field_name, field):
        if not field_name: raise ValueError("field_name is empty")
        if field_name in self.fields: raise ValueError("{} already exists and cannot be changed".format(field_name))
        if not isinstance(field, Field): raise ValueError("{} is not a Field instance".format(type(field)))

        field.name = field_name
        if field.options['unique']:
            self.set_index(field_name, Index(field_name, unique=True))

        self.fields[field_name] = field
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
        self.indexes[index_name] = index
        return self

    def field_name(self, k):
        """
        get the field name of k

        most of the time, the field_name of k will just be k, but this makes special
        allowance for k's like "pk" which will return _id
        """
        return self.__getattr__(k).name


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


class Field(object):
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
        def foo(self, val, is_update, is_modified):
            # do custom things
            return val

    NOTE -- the iset/iget methods are different than traditional python getters
    and setters because they always need to return a value and they always take in a
    value

    There are also fget/fset/fdel methods that can be set to customize behavior
    on when a value is set on a particular instance, so if you wanted to make sure
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
    """
    @property
    def schema(self):
        """return the schema instance if this is reference to another table"""
        if not hasattr(self, "_schema"):
            ret = None
            o = self._type
            if isinstance(o, type):
                ret = getattr(o, "schema", None)

            elif isinstance(o, Schema):
                ret = o

            else:
                module, klass = utils.get_objects(o)
                ret = klass.schema

            self._schema = ret

        return self._schema

    @property
    def type(self):
        ret = self._type
        if not isinstance(ret, type) or hasattr(ret, "schema"):
            s = self.schema
            ret = s.pk.type

        return ret

    @property
    def ref(self):
        """Returns the FK reference orm class"""
        schema = self.schema
        return schema.orm_class if schema else None

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
        field_options = utils.make_dict(field_options, field_options_kwargs)
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

        field_options.setdefault("unique", False)
        field_options.update(d)

        self.fgetter(field_options.pop("fget", self.default_fget))
        self.fsetter(field_options.pop("fset", self.default_fset))
        self.fdeleter(field_options.pop("fdel", self.default_fdel))
        self.fdefaulter(field_options.pop("fdefault", self.default_fdefault))

        self.igetter(field_options.pop("iget", self.default_iget))
        self.isaver(field_options.pop("isave", self.default_isave))
        self.imodifier(field_options.pop("imodify", self.default_imodify))
        self.imodify_inserter(field_options.pop("imodify_insert", self.default_imodify_insert))
        self.imodify_updater(field_options.pop("imodify_update", self.default_imodify_update))
        self.idefaulter(field_options.pop("idefault", self.default_idefault))
        self.idefault_inserter(field_options.pop("idefault_insert", self.default_idefault_insert))
        self.idefault_updater(field_options.pop("idefault_update", self.default_idefault_update))
        self.iquerier(field_options.pop("iquery", self.default_iquery))

        self.jsonabler(field_options.pop("jsonable", self.default_jsonable))

        self.name = field_options.pop("name", "")
        # this creates a numeric dict key that can't be accessed as an attribute
        self.instance_field_name = str(id(self))
        self._type = field_type
        self.default = field_options.pop("default", None)
        self.options = field_options
        self.required = field_required if field_required else self.is_pk()
        self.orm_class = None

    def is_pk(self):
        """return True if this field is a primary key"""
        return self.options.get("pk", False)

    def is_ref(self):
        """return true if this field foreign key references the primary key of another orm"""
        return bool(self.schema)

    def is_required(self):
        return self.required

    def default_fget(self, instance, val):
        return self.fdefault(instance, val)

    def default_fset(self, instance, val):
        return val

    def default_fdel(self, instance, val):
        return None

    def default_fdefault(self, instance, val):
        ret = val
        if val is None:
            if callable(self.default):
                ret = self.default()

            elif self.default is None:
                ret = self.default

            elif isinstance(self.default, (dict, list, set, object)):
                ret = type(self.default)()

            else:
                ret = self.default

        return ret

    def default_isave(self, instance, val, is_update, is_modified):
        if is_modified:
            val = self.imodify(instance, val)
            if is_update:
                val = self.imodify_update(instance, val)

            else:
                val = self.imodify_insert(instance, val)

        else:
            val = self.idefault(instance, val)
            if is_update:
                val = self.idefault_update(instance, val)

            else:
                val = self.idefault_insert(instance, val)

        return val

    def default_idefault(self, instance, val):
        return val

    def default_idefault_insert(self, instance, val):
        return val

    def default_idefault_update(self, instance, val):
        return val

    def default_imodify(self, instance, val):
        return val

    def default_imodify_insert(self, instance, val):
        return val

    def default_imodify_update(self, instance, val):
        return val

    def default_iget(self, instance, val):
        return self.fdefault(instance, val)

    def default_iquery(self, instance, val):
        return val

    def default_jsonable(self, instance, val):
        if val is None:
            val = self.fdefault(instance, val)

        if val is not None:
            format_str = ""

            if isinstance(val, datetime.datetime):
                format_str = "%Y-%m-%dT%H:%M:%S.%fZ"
            elif isinstance(val, datetime.date):
                format_str = "%Y-%m-%d"

            if format_str:
                try:
                    val = datetime.datetime.strftime(val, format_str)

                except ValueError as e:
                    # strftime can fail on dates <1900
                    # Note that Python 2.7, 3.0 and 3.1 have errors before the year 1900,
                    # Python 3.2 has errors before the year 1000. Additionally, pre-3.2
                    # versions interpret years between 0 and 99 as between 1969 and 2068.
                    # Python versions from 3.3 onward support all positive years in
                    # datetime (and negative years in time.strftime), and time.strftime
                    # doesn't do any mapping of years between 0 and 99.
                    # https://stackoverflow.com/a/32206673/5006
                    logger.warning(e, exc_info=True)

                    # we correct this issue by just giving it a dumb year,
                    # creating the timestamp and then replacing the year, we can
                    # do this semi-confidently because our format_str doesn't have
                    # day of the week (eg, Monday), we account for leap years
                    # just in case
                    orig_year = val.year
                    if (orig_year % 4) == 0:
                        if (orig_year % 100) == 0:
                            if (orig_year % 400) == 0:
                                placeholder_year = 2000

                            else:
                                placeholder_year = 1900

                        else:
                            placeholder_year = 2012

                    else:
                        placeholder_year = 1997

                    dt = val.replace(year=placeholder_year)
                    val = datetime.datetime.strftime(dt, format_str)
                    val = re.sub(r"^{}".format(placeholder_year), str(orig_year), val)

        return val

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

    def fdefaulter(self, v):
        """decorator for returning the field's default value

        decorator for setting field's fdefault function"""
        self.fdefault = v
        return self

    def igetter(self, v):
        """decorator for the method called when a field is pulled from the database"""
        self.iget = v
        return self

    def isaver(self, v):
        """decorator for the method called when a field is saved into the database"""
        self.isave = v
        return self

    def imodifier(self, v):
        """decorator for the method called when an insert/update database query is going
        to be used and that field has been modified"""
        self.imodify = v
        return self

    def imodify_inserter(self, v):
        """decorator for the method called when an insert database query is going
        to be used and that field has been modified"""
        self.imodify_insert = v
        return self

    def imodify_updater(self, v):
        """decorator for the method called when an update database query is going
        to be used and that field has been modified"""
        self.imodify_update = v
        return self

    def idefaulter(self, v):
        """decorator for the method called when an update/insert database query is going
        to be used and that field hasn't been touched"""
        self.idefault = v
        return self

    def idefault_inserter(self, v):
        """decorator for the method called when an update/insert database query is going
        to be used and that field hasn't been touched"""
        self.idefault_insert = v
        return self

    def idefault_updater(self, v):
        """decorator for the method called when an update/insert database query is going
        to be used and that field hasn't been touched"""
        self.idefault_update = v
        return self

    def iquerier(self, v):
        """decorator for the method called when this field is used in a SELECT query"""
        self.iquery = v
        return self

    def jsonabler(self, v):
        """Decorator for the method called for a field when an Orm's .jsonable method
        is called"""
        self.jsonable = v

    def fval(self, instance):
        """return the raw value that this property is holding internally for instance"""
        try:
            val = instance.__dict__[self.instance_field_name]
        except KeyError as e:
            #raise AttributeError(str(e))
            val = None

        return val

    def __get__(self, instance, classtype=None):
        """This is the wrapper that will actually be called when the field is
        fetched from the instance, this is a little different than Python's built-in
        @property fget method because it will pull the value from a shadow variable in
        the instance and then call fget"""
        if instance is None:
            # class is requesting this property, so return it
            return self

        raw_val = self.fval(instance)
        ret = self.fget(instance, raw_val)

        # we want to compensate for default values right here, so if the raw val
        # is None but the new val is not then we save the returned value, this
        # allows us to handle things like dict with no surprises
        if raw_val is None:
            if ret is not None:
                instance.__dict__[self.instance_field_name] = ret

        return ret

    def __set__(self, instance, val):
        """this is the wrapper that will actually be called when the field is
        set on the instance, your fset method must return the value you want set,
        this is different than Python's built-in @property setter because the
        fset method *NEEDS* to return something"""
        val = self.fset(instance, val)
        instance.__dict__[self.instance_field_name] = val

    def __delete__(self, instance):
        """the wrapper for when the field is deleted, for the most part the default
        fdel will almost never be messed with, this is different than Python's built-in
        @property deleter because the fdel method *NEEDS* to return something and it
        accepts the current value as an argument"""
        val = self.fdel(instance, self.fval(instance))
        instance.__dict__[self.instance_field_name] = val
        #self.__set__(instance, val)


class ObjectField(Field):
    """A special field type for when you just want to shove an object in a field

    this will just pickle and base64 the object so it can be stored in a text field
    and then it will do the opposite when you pull it out, so basically, you can dump
    anything you want in this field and it will be saved and restored transparently

    I thought about doing Field(object, ...) but building it in that way actually
    proved to be more complicated than I thought, you could pass in some type and if it
    was that type then it would set the default methods to pickle_iset/iget, but I couldn't
    decide what type to pass in, if you pass in pickle, then you need to check for pickle and
    cPickle to decide, you can't pass in something like `object` without complicating 
    how foreign keys are figured out, so ultimately, I've decided to just have it be
    a separate class"""
    def __init__(self, field_required=False, default=None):
        """
        unlike the normal Field class, you can't set any options or a type on this
        Field, because it is a pickled object, so it can't be unique, it doesn't have
        a size, etc.. Likewise, the field type is always str
        """
        super(ObjectField, self).__init__(
            field_type=str,
            field_required=field_required,
            default=default,
        )

    def encode(self, val):
        if val is None: return val
        return base64.b64encode(pickle.dumps(val, pickle.HIGHEST_PROTOCOL))

    def decode(self, val):
        if val is None: return val
        return pickle.loads(base64.b64decode(val))

    def isaver(self, isave):
        def master_isave(cls, val, is_update, is_modified):
            v = isave(cls, val, is_update, is_modified)
            return self.encode(val)
            #return iset(cls, v, is_update, is_modified)
        return super(ObjectField, self).isaver(master_isave)

    def igetter(self, iget):
        def master_iget(cls, val):
            v = self.decode(val)
            return iget(cls, v)
        return super(ObjectField, self).igetter(master_iget)


class JsonField(ObjectField):
    """Similar to ObjectField but stores json in the db"""
    def __init__(self, field_required=False, default=dict):
        super(JsonField, self).__init__(
            field_required=field_required,
            default=default,
        )

    def encode(self, val):
        if val is None: return val
        return json.dumps(val)

    def decode(self, val):
        if val is None: return val
        return json.loads(val)

