import types
import urlparse
import datetime

import re

from .model import Orm
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
        for field_name, field_options in self.fields.iteritems():
            if field_options.get('pk', False):
                ret = field_name
                break

        if not ret:
            raise AttributeError("no primary key in schema")

        return ret

    @property
    def normal_fields(self):
        """fields that aren't magic (eg, aren't _id, _created, _updated)"""
        return {f:v for f, v in self.fields.iteritems() if not f.startswith('_')}

    @property
    def required_fields(self):
        """The normal required fields (eg, no magic fields like _id are included)"""
        return {f:v for f, v in self.normal_fields.iteritems() if v['required']}

    @property
    def magic_fields(self):
        """the magic fields for the schema"""
        return {f:v for f, v in self.fields.iteritems() if f.startswith('_')}

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

        self._id = Field(long, True, pk=True)
        self._created = Field(datetime.datetime, True)
        self._updated = Field(datetime.datetime, True)

        self.index_updated = self._updated
        self.index_created = self._created

        for field_name, field_val in fields.iteritems():
            setattr(self, field_name, field_val)

    def __str__(self):
        return self.table

    def __setattr__(self, name, val):
        """
        allow schema to magically set fields and indexes by using the method name

        you can either set a field name:

            self.fieldname = <type>, <required>, <option_hash>

        or you can set a normal index:

            self.index_indexname = field1, field2, ...

        or a unique index:

            self.unique_indexname = field1, field2, ...

        example --
            # add foo and bar fields
            self.foo = int, True, dict(min_size=0, max_size=100)
            self.bar = str, False, dict(max_size=32)

            # add a normal index and a unique index
            self.index_foobar = self.foo, self.bar
            self.unique_bar = self.bar
        """
        # canary, ignore already defined attributes
        if name in self.__dict__ or name in self.__class__.__dict__:
            return object.__setattr__(self, name, val)

        # compensate for the special _name fields
        if name[0] != '_':
            name_bits = name.split(u'_', 1)
        else:
            name_bits = [name]

        is_field = True

        index_name = name_bits[1] if len(name_bits) > 1 else u""
        index_types = {
            # index_type : **kwargs options
            'index': {},
            'unique': dict(unique=True)
        }

        if name_bits[0] in index_types:
            is_field = False
            # compensate for passing in one value instead of a tuple
            if isinstance(val, (types.DictType, types.StringType)):
                val = (val,)

            self.set_index(index_name, val, **index_types[name_bits[0]])

        if is_field:
            # compensate for passing in one value, not a tuple
            if isinstance(val, types.TypeType):
                val = Field(val)

            else:
                if not isinstance(val, Field):
                    val = Field(*val)

            self.set_field(name, val)

    def __getattr__(self, name):
        """
        this is mainly here to enable fluid defining of indexes using class attributes

        example -- 
            self.foo = int, True
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
            type=field[0],
            required=field[1]
        )
#        d = {
#            'name': field_name,
#            'type': field[0],
#            'required': field[1]
#        }

        if field[2]['unique']:
            self.set_index(field_name, [field_name], unique=True)

        d.update(field[2])
        self.fields[field_name] = d

        return self

    def set_index(self, index_name, index_fields, **options):
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
        if not index_fields:
            raise ValueError("index_fields list is empty")
        if index_name in self.indexes:
            raise ValueError("index_name has already been defined on {}".format(str(self.indexes[index_name]['fields'])))

        field_names = []
        for field_name in index_fields:
            field_name = str(field_name)
            field_names.append(field_name)

        if not index_name:
            index_name = u"_".join(field_names)

        self.indexes[index_name] = {
            'name': index_name,
            'fields': field_names,
            'unique': False
        }
        self.indexes[index_name].update(options)

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


class Field(tuple):
    def __new__(cls, field_type, field_required=False, field_options=None, **field_options_kwargs):
        """
        create a field tuple

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
        if not isinstance(field_type, types.TypeType):
            raise ValueError("field_type is not a valid python built-in type: str, int, float, ...")

        d = {}
        if not field_options: field_options = {}
        if field_options_kwargs: field_options.update(field_options_kwargs)

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

        instance = super(Field, cls).__new__(cls, [field_type, field_required, field_options])

        # make some of the field options easier to reference
        instance.type = instance[0]
        instance.required = instance[1]
        instance.options = instance[2]

        return instance


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
        if 'ref_schema' in self: return self['ref_schema']
        if 'weak_ref_schema' in self: return self['weak_ref_schema']
        #if 'ref' not in self: return None
        #if 'weak_ref' not in self: return None

        s = None
        for k in ['ref', 'weak_ref']:
            if k in self:
                o = self[k]
                s = self._get_schema(o)
                self['{}_schema'.format(o)] = s
                break

        return s

#    def __getitem__(self, key):
#        pout.v(key)
#        return super(FieldOptions, self).__getitem__(key)

    def _get_schema(self, o):
        ret = None
        if isinstance(o, types.TypeType):
            if issubclass(o, Orm):
                ret = o.schema

        elif isinstance(o, Schema):
            ret = o

        else:
            module, klass = utils.get_objects(o)
            ret = klass.schema

        return ret

