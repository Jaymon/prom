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

    #debug = False
    """true to turn on debugging for this connection"""

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
    def pk(self):
        """return the primary key of the table schema"""
        ret = None
        for field_name, field_options in self.fields.items():
            if field_options.get('pk', False):
                ret = field_name
                break

        if not ret:
            raise AttributeError("no primary key in schema")

        return ret

    @property
    def normal_fields(self):
        """fields that aren't magic (eg, aren't _id, _created, _updated)"""
        return {f:v for f, v in self.fields.items() if not f.startswith('_')}

    @property
    def required_fields(self):
        """The normal required fields (eg, no magic fields like _id are included)"""
        return {f:v for f, v in self.normal_fields.items() if v['required']}

    @property
    def magic_fields(self):
        """the magic fields for the schema"""
        return {f:v for f, v in self.fields.items() if f.startswith('_')}

    @classmethod
    def get_instance(cls, orm_class):
        table_name = orm_class.table_name
        if table_name not in cls.instances:
            s = cls(table_name)
            #properties = inspect.getmembers(orm_class, lambda x: isinstance(x, (Field, Index)))
            #for k, v in properties:
            for k, v in vars(orm_class).items():
                if isinstance(v, (Field, Index)):
                    s.set(k, v)

            cls.instances[table_name] = s

        return cls.instances[table_name]

    def __init__(self, table, **fields):
        """
        create an instance

        every Orm should have a .schema attribute that is an instance of this class

        example --

            self.schema = Schema(
                "table_name"
                field1=(int, True),
                field2=(str,),
                index_fields=("field1", "field2")
            )

        table -- string -- the table name
        **fields -- a dict of field name or index keys with tuple values, see __getattr__ for more details
        """
        self.fields = {}
        self.indexes = {}
        self.table = str(table)

        self.set_field("_id", Field(long, True, pk=True))
        self.set_field("_created", Field(datetime.datetime, True))
        self.set_field("_updated", Field(datetime.datetime, True))

        self.set_index("updated", Index(self._updated))
        self.set_index("created", Index(self._created))

        for field_name, field_val in fields.items():
            setattr(self, field_name, field_val)

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
        this is mainly here to enable fluid defining of indexes using class attributes

        example -- 
            self.set_field("foo", Field(int, True))
            self.index_foo = s.foo # s.foo seems more fluid to me than "foo" :P

        return -- string -- the string value of the attribute name, eg, self.foo returns "foo"
        """
        if not name in self.fields:
            raise AttributeError("{} is not a valid field name".format(name))

        return self.fields[name]['name']

    def set_field(self, field_name, field):
        if not field_name: raise ValueError("field_name is empty")
        if field_name in self.fields: raise ValueError("{} already exists and cannot be changed".format(field_name))
        if not isinstance(field, Field): raise ValueError("{} is not a Field instance".format(type(field)))

        d = FieldOptions(
            name=field_name,
            type=field.type,
            required=field.required,
            field=field
        )
#        d = {
#            'name': field_name,
#            'type': field[0],
#            'required': field[1]
#        }

        if field.options['unique']:
            self.set_index(field_name, Index(field_name, unique=True))

        d.update(field.options)
        self.fields[field_name] = d

        return self

    def set_index(self, index_name, index):
        """
        add an index to the schema

        for the most part, you will use the __getattr__ method of adding indexes for a more fluid interface,
        but you can use this if you want to get closer to the bare metal

        index_name -- string -- the name of the index
        index_fields -- list -- the string field_names this index will index on, fields have to be already added
            to this schema index
        **options -- dict --
            unique -- boolean -- True if the index should be unique, false otherwise
        """
        if not index_name:
            raise ValueError("index_name must have a value")
        if index_name in self.indexes:
            raise ValueError("index_name {} has already been defined on {}".format(
                index_name, str(self.indexes[index_name]['fields'])
            ))
        if not isinstance(index, Index): raise ValueError("{} is not an Index instance".format(type(index)))

        self.indexes[index_name] = {
            'name': index_name,
            'fields': index.fields,
            'unique': False,
            'index': index
        }
        self.indexes[index_name].update(index.options)

        return self

    def field_name(self, k):
        """
        get the field name of k

        most of the time, the field_name of k will just be k, but this makes special
        allowance for k's like "pk" which will return _id
        """
        if k == u'pk': k = self.pk
        if k not in self.fields:
            raise KeyError(u"key {} is not in the {} schema".format(k, self.table))

        return k


class Index(object):
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

        self.fields = map(str, fields)
        self.options = options


class Field(object):

    @property
    def schema(self):
        """return the schema instance if this is reference to another table"""
        if not hasattr(self, "_schema"):
            ret = None
            o = self.type
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
        if not isinstance(self._type, types.TypeType):
            s = self.schema


            raise ValueError("field_type is not a valid python built-in type: str, int, float, ...")


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
#         if not isinstance(field_type, types.TypeType):
#             raise ValueError("field_type is not a valid python built-in type: str, int, float, ...")

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

        self._type = field_type
        self.required = field_required
        self.options = field_options
        self.normalize = None

    def __get__(self, instance, klass):
        #pout.v("__get__")
        #return klass.__getattr__(self.name)
        return self

    def __set__(self, instance, val):
        pout.v("__set__", self.name, instance, val)
        return instance.__setattr__(self.name, val)

    def __delete__(self, instance):
        #pout.v("__delete__")
        #return self
        return instance.__delattr__(self.name)

    def __call__(self, func):
        self.normalize = func
        #pout.v(self.name, self.normalize)
        return self


# DEPRECATED -- 11-4-2015 -- get rid of this class
class FieldOptions(dict):
    """
    Holds the options dict for the fields

    This came about because of circular dependencies when using string refs, I needed
    a way to delay actually figuring out what schema we were referencing until the
    absolute last possible moment, and this was the simplest way I could do it while
    keeping backwards compatibility high
    """
    @property
    def ref_schema(self):
        return self["field"].schema

