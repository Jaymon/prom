import types
import urlparse
import datetime
import inspect

import re

from . import utils


class Connection(object):
    """
    set the paramaters you want to use to connect to an interface

    https://github.com/Jaymon/Mingo/blob/master/Mingo/MingoConfig.php
    """
    name = ""
    """string -- the name of this connection (eg, Postgres, or SQLite)"""

    interface_name = ""
    """string -- full Interface class name -- the interface the connection should use to talk with the db"""

    database = ""
    """the db name to use, in postgres, this is the database name"""

    port = 0
    """the host port"""

    username = ""
    """the username to use to to connect to the db"""

    password = ""
    """the password for the username"""

    options = None
    """any other db options, these can be interface implementation specific"""

    @property
    def host(self):
        """the db host"""
        if not hasattr(self, '_host'): self._host = None
        return self._host

    @host.setter
    def host(self, h):
        """
        check host for a :port, and split that off into the .port attribute if there
        """
        # normalize the host so urlparse can parse it correctly
        # http://stackoverflow.com/questions/9530950/parsing-hostname-and-port-from-string-or-url#comment12075005_9531210
        if re.search(ur'\:memory\:', h, re.I):
            h = re.sub(ur'(?:\S+|^)\/\/', '', h)
            self._host = h

        else:
            if not re.match(ur'(?:\S+|^)\/\/', h):
                h = "//{}".format(h)

            o = urlparse.urlparse(h)

            self._host = o.hostname
            if o.port: self.port = o.port

    def __init__(self, **kwargs):
        """
        set all the values by passing them into this constructor, any unrecognized kwargs get put into .options

        example --
            c = Connection(
                database="dbname",
                port=5000,
                some_random_thing="foo"
            )

            print c.port # 5000
            print c.options # {"some_random_thing": "foo"}
        """
        self.options = {}

        for key, val in kwargs.iteritems():
            if hasattr(self, key):
                setattr(self, key, val)
            else:
                self.options[key] = val


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
    def __init__(self, dsn):
        # get the scheme, which is actually our interface_name
        first_colon = dsn.find(':')
        interface_name = dsn[0:first_colon]
        dsn_url = dsn[first_colon+1:]
        dsn_url, is_memory = re.subn(ur'\/\/\:memory\:', u'//memory', dsn_url, flags=re.I)
        url = urlparse.urlparse(dsn_url)
        self.dsn = dsn

        # parse the query into options
        options = {}
        if url.query:
            for k, kv in urlparse.parse_qs(url.query, True).iteritems():
                if len(kv) > 1:
                    options[k] = kv
                else:
                    options[k] = kv[0]

        d = {
            'interface_name': interface_name,
            'database': url.path[1:],
        }

        if url.hostname:
            if is_memory:
                d['host'] = u':memory:'
            else:
                d['host'] = url.hostname

        if url.port:
            d['port'] = url.port

        if url.username:
            d['username'] = url.username

        if url.password:
            d['password'] = url.password

        if url.fragment:
            d['name'] = url.fragment

        if options:
            d['options'] = options

        super(DsnConnection, self).__init__(**d)


class Schema(object):
    """
    handles all table schema definition

    the table schema definition includes the table name, the fields the table has, and
    the indexes that are on the table
    """

    instances = {}
    """class variable, holds different schema instances for various orms"""

    table = u""
    """string -- set the table name for this schema instance"""

    fields = None
    """dict -- all the fields this schema instance will use"""

    indexes = None
    """dict -- all the indexes this schema will have"""

    @property
    def normal_fields(self):
        """fields that aren't magic (eg, aren't _id, _created, _updated)"""
        return {f:v for f, v in self.fields.items() if not f.startswith('_')}

    @property
    def required_fields(self):
        """The normal required fields (eg, no magic fields like _id are included)"""
        return {f:v for f, v in self.normal_fields.items() if v.required}

    @property
    def magic_fields(self):
        """the magic fields for the schema"""
        return {f:v for f, v in self.fields.items() if f.startswith('_')}

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
                            s.set(k, v)
                        seen_properties.add(k)

            cls.instances[table_name] = s

        return cls.instances[table_name]

    def __init__(self, table, **fields_or_indexes):
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

        table -- string -- the table name
        **fields_or_indexes -- a dict of field name or index keys with tuple values, see __getattr__ for more details
        """
        self.fields = {}
        self.indexes = {}
        self.table = str(table)

        for name, val in fields_or_indexes.items():
            self.set(name, val)

    def __str__(self):
        return self.table

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
            if name == u"pk":
                for field_name, field in self.fields.items():
                    if field.options.get('pk', False):
                        return field

            raise AttributeError("No {} field in schema {}".format(name, self.table))

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
        self.fields = map(str, fields)
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

    to customize the field, you can use the decorator:

        foo = Field(str, True)

        @foo.igetter
        def foo(self, val, is_update, is_modified):
            # do custom things
            return val

        @foo.isetter
        def foo(self, val, is_update, is_modified):
            # do custom things
            return val


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

    NOTE -- the fget/fset/fdel methods are different than tradiitional python getters
    and setters because they always need to return a value and they always take in a
    value
    """

    @property
    def schema(self):
        """return the schema instance if this is reference to another table"""
        if not hasattr(self, "_schema"):
            ret = None
            o = self._type
            if isinstance(o, types.TypeType):
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
        if not isinstance(ret, types.TypeType) or hasattr(ret, "schema"):
            s = self.schema
            ret = s.pk.type

        return ret

    def __init__(self, field_type, field_required=False, field_options=None, **field_options_kwargs):
        """
        create a field

        field_type -- type -- the python type of the field, so for a string you would pass str, integer: int,
            boolean: bool, float: float, big int: long
        field_required -- boolean -- true if this field has to be there to insert
        field_options -- dict -- everything else in key: val notation. Current options:
            size -- int -- the size you want the string to be, or the int to be
            min_size -- int -- the minimum size
            max_size -- int -- if you want a varchar, set this
            unique -- boolean -- True to set a unique index on this field, this is just for convenience and is
                equal to self.set_index(field_name, [field_name], unique=True). this is a convenience option
                to set a unique index on the field without having to add a separate index statement
            ignore_case -- boolean -- True to ignore case if this field is used in indexes
        **field_options_kwargs -- will be combined with field_options
        """
        d = {}
        field_options = utils.make_dict(field_options, field_options_kwargs)

        min_size = field_options.pop("min_size", None)
        max_size = field_options.pop("max_size", None)
        size = field_options.pop("size", None)

        if size > 0:
            d['size'] = size
        else:
            if min_size > 0 and max_size == None:
                raise ValueError("min_size option was set with no corresponding max_size")

            elif min_size == None and max_size > 0:
                d['max_size'] = max_size

            elif min_size >= 0 and max_size >= 0:
                d['min_size'] = min_size
                d['max_size'] = max_size

        field_options.setdefault("unique", False)
        field_options.update(d)

        self.fget = field_options.pop("fget", self.default_fget)
        self.fset = field_options.pop("fset", self.default_fset)
        self.fdel = field_options.pop("fdel", self.default_fdel)

        self.iset = field_options.pop("iset", self.default_iset)
        self.iget = field_options.pop("iget", self.default_iget)

        self.name = field_options.pop("name", "")
        # this creates a numeric dict key that can't be accessed as an attribute
        self.instance_field_name = str(id(self))
        self._type = field_type
        self.required = field_required
        self.options = field_options

    def is_pk(self):
        """return True if this field is a primary key"""
        return self.options.get("pk", False)

    def is_ref(self):
        """return true if this field foreign key references the primary key of another orm"""
        return bool(self.schema)

    def default_fget(self, instance, val):
        return val

    def default_fset(self, instance, val):
        return val

    def default_fdel(self, instance, val):
        return None

    def fgetter(self, fget):
        """decorator for setting field's fget function"""
        self.fget = fget
        return self

    def fsetter(self, fset):
        """decorator for setting field's fset function"""
        self.fset = fset
        return self

    def fdeleter(self, fdel):
        """decorator for setting field's fdel function"""
        self.fdel = fdel
        return self

    def default_iset(self, classtype, val, is_update, is_modified):
        return val

    def isetter(self, iset):
        self.iset = iset
        return self

    def default_iget(self, classtype, val):
        return val

    def igetter(self, iget):
        self.iget = iget
        return self

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

        return self.fget(instance, self.fval(instance))

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

