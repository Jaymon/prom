# -*- coding: utf-8 -*-
from __future__ import unicode_literals, division, print_function, absolute_import
import inspect
import sys
import datetime

from datatypes.collections import Pool

# first party
from .query import Query, Iterator
from . import decorators, utils
from .interface import get_interface
from .config import (
    Schema,
    Field,
    AutoDatetime,
    AutoIncrement,
    Index
)
from .compat import *


class OrmPool(Pool):
    """
    Create a pool of Orm instances, which is just a dict of primary_key -> Orm instance
    mappings

    Let's say you are iterating through millions of rows of Foo, and for each Foo
    instance you need to get the Bar instance from the Foo.bar_id field, and lots of
    Foos have the same bar_id, but you only want to pull the Bar instance from
    the db once, this allows you to easily do that

    :Example:
        bar_pool = Bar.pool(500) # keep the pool contained to the last 500 Bar instances
        for f in Foo.query.all():
            b = bar_pool[f.bar_id]
            print "Foo {} loves Bar {}".format(f.pk, b.pk)
    """
    def __init__(self, orm_class, maxsize=0):
        super(OrmPool, self).__init__(maxsize=maxsize)
        self.orm_class = orm_class

    def __missing__(self, pk):
        o = self.orm_class.query.eq_field("pk", pk).one()
        self[pk] = o
        return o


class Orm(object):
    """
    this is the parent class of any Orm child class you want to create that can access the db

    :example:
        from prom import Orm, Field, Index

        # create a simple class using standard fields
        class Foo(Orm):
            table_name = "<TABLE NAME>"

            bar = Field(int, True, max_size=512, default=0, unique=True)
            che = Field(str, True)

            index_barche = Index('bar', 'che')
    """
    connection_name = ""
    """the name of the connection to use to retrieve the interface"""

    query_class = Query
    """the class this Orm will use to create Query instances to query the db"""

    iterator_class = Iterator
    """the class this Orm will use for iterating through results returned from db"""

    #_id = Field(int, True, pk=True)
    _id = AutoIncrement(aliases=["id"])
    _created = AutoDatetime(created=True, updated=False, aliases=["created"])
    _updated = AutoDatetime(created=False, updated=True, aliases=["updated"])

#     class _created(Field):
#         type = datetime.datetime
#         required = True
#         options = {
#             "aliases": ["created"],
#         }
# 
#         def iset(self, orm, val):
#             if not val and orm.is_insert():
#                 val = datetime.datetime.utcnow()
#             return val
# 
#     class _updated(Field):
#         type = datetime.datetime
#         required = True
#         options = {
#             "aliases": ["updated"],
#         }
# 
#         def iset(self, orm, val):
#             if val:
#                 if not self.modified(orm, val):
#                     val = datetime.datetime.utcnow()
# 
#             else:
#                 val = datetime.datetime.utcnow()
# 
#             return val

    @decorators.classproperty
    def table_name(cls):
        return "{}_{}".format(
            cls.__module__.lower().replace(".", "_"),
            cls.__name__.lower()
        )

    @decorators.classproperty
    def schema(cls):
        """the Schema() instance that this class will derive all its db info from"""
        return Schema.get_instance(cls)

    @decorators.classproperty
    def interface(cls):
        """
        return an Interface instance that can be used to access the db

        return -- Interface() -- the interface instance this Orm will use
        """
        return get_interface(cls.connection_name)

    @decorators.classproperty
    def query(cls):
        """return a new Query instance ready to make a db call using the child class

        :example:
            # fluid interface
            results = Orm.query.is_foo('value').desc_bar().get()

        :returns: Query, every time this is called a new query instance is created using cls.query_class
        """
        query_class = cls.query_class
        return query_class(orm_class=cls)

    @property
    def field_names(self):
        """Return all the field names"""
        return [k for k in self.schema.fields]

    @property
    def fields(self):
        """
        return all the fields and their raw values for this Orm instance. This
        property returns a dict with the field names and their current values

        if you want to control the values for outputting to an api, use .jsonable()
        """
        return {k:getattr(self, k, None) for k in self.schema.fields}

    @property
    def modified_field_names(self):
        """Return all the field names that are currently modified"""
        modified_field_names = set()
        for field_name, field in self.schema.fields.items():
            if field.modified(self, getattr(self, field_name)):
                modified_field_names.add(field_name)
        return modified_field_names

    @property
    def modified_fields(self):
        """Return a dict of field_names/field_values for all the currently modified fields"""
        return {k:getattr(self, k) for k in self.modified_field_names}

    @classmethod
    def pool(cls, maxsize=0):
        """
        return a new OrmPool instance

        return -- OrmPool -- the orm pool instance will be tied to this Orm
        """
        return OrmPool(orm_class=cls, maxsize=maxsize)

    @classmethod
    def create(cls, fields=None, **fields_kwargs):
        """
        create an instance of cls with the passed in fields and set it into the db

        fields -- dict -- field_name keys, with their respective values
        **fields_kwargs -- dict -- if you would rather pass in fields as name=val, that works also
        """
        # NOTE -- you cannot use hydrate/populate here because populate alters modified fields
        instance = cls(fields, **fields_kwargs)
        instance.save()
        return instance

    @classmethod
    def hydrate(cls, fields=None, **fields_kwargs):
        """return a populated instance with the present fields

        NOTE -- you probably shouldn't override this method since the Query methods
        rely on this method signature to create each instance of the results

        :param fields: dict, the fields to populate in this instance
        :param **fields_kwargs: dict, the fields in key=val form to populate in this instance
        :returns: an instance of this class with populated fields
        """
        instance = cls()
        fields = cls.make_dict(fields, fields_kwargs)
        instance.from_interface(fields)
        instance._interface_hydrate = True
        return instance

    @classmethod
    def make_dict(cls, fields, fields_kwargs, schema=None):
        """Lots of methods take a dict and key=val for fields, this combines fields
        and fields_kwargs into one master dict, turns out we want to do this more
        than I would've thought to keep api compatibility with prom proper

        :param fields: dict, the fields in a dict
        :param fields_kwargs: dict, if you would like to pass the fields as key=val
            this picks those up and combines them with fields
        :schema: Schema, if passed in then this will normalize field names
        :returns: dict, the combined fields
        """
        fields = utils.make_dict(fields, fields_kwargs)

        if schema:
            # since schema is passed in resolve any aliases
            for field_name in list(fields.keys()):
                if fn := schema.field_name(field_name):
                    fields[fn] = fields.pop(field_name)

        return fields

    def __init__(self, fields=None, **fields_kwargs):
        """Create an Orm object

        :param fields: dict, the fields in a dict
        :param **fields_kwargs: dict, if you would like to pass the fields as key=val
        """
        self._interface_pk = None
        self._interface_hydrate = False

        schema = self.schema
        fields = self.make_dict(fields, fields_kwargs, schema=schema)

        # set defaults
        for field_name, field in schema.fields.items():
            fields[field_name] = field.fdefault(self, fields.get(field_name, None))

        self.modify(fields)

    def fk(self, orm_class):
        """find the field value in self that is the primary key of the passed in orm_class

        :example:
            class Foo(Orm):
                pass

            class Bar(Orm):
                foo_id = Field(Foo)

            b = Bar(foo_id=1)
            print(b.fk(Foo)) # 1

        :param orm_class: Orm, the fields in self will be checked until the field that
            references Orm is found, then the value of that field will be returned
        :returns: the self field value that is a foreign key references to orm_class
        """
        for field_name, field in self.schema.ref_fields.items():
            if field.schema is orm_class.schema:
                return getattr(self, field_name)

        raise ValueError("Did not find a foreign key reference for {} in {}".format(
            orm_class.__name__,
            self.__class__.__name__,
        ))

    def ref(self, orm_classpath):
        """see Query.ref() for an explanation of what this method does

        :param orm_classpath: string|type, a full python class path (eg, foo.bar.Che) or
            an actual model.Orm python class
        :returns: Orm
        """
        return self.query.ref(orm_classpath).orm_class

    def is_hydrated(self):
        """return True if this orm was populated from the interface/db"""
        return self._interface_hydrate

    def is_update(self):
        """Return True if .save() will perform an interface update"""
        return self._interface_pk is not None

    def is_insert(self):
        """Return True if .save() will perform an interface insert"""
        return not self.is_update()

    def from_interface(self, fields):
        """this runs all the fields through their iget methods to mimic them
        freshly coming out of the db, then resets modified

        :param fields: dict, only the fields you want to populate
        """
        schema = self.schema
        for field_name, v in fields.items():
            fields[field_name] = schema.fields[field_name].iget(self, v)
            if field_name in schema.fields:
                fields[field_name] = schema.fields[field_name].iget(self, v)

        self.modify(fields)

        # this marks that this was repopulated from the interface (database)
        self._interface_pk = self.pk

    def to_interface(self):
        """Get all the fields that need to be saved

        :param is_udpate: bool, True if update query, False if insert
        :returns: dict, key is field_name and val is the field value to be saved
        """
        fields = {}
        schema = self.schema
        for k, field in schema.fields.items():
            v = field.iset(self, getattr(self, k))

            is_modified = field.modified(self, v)
            if is_modified:
                fields[k] = v

            if v is None and field.is_required():
                if field.is_pk():
                    if is_modified:
                        raise KeyError("Primary key has been removed and is required")

                else:
                    if self.is_insert() or is_modified:
                        raise KeyError("Missing required field {}".format(k))

        return fields

    def insert(self, **kwargs):
        """persist the field values of this orm"""
        ret = True

        schema = self.schema
        fields = self.to_interface()

        q = self.query
        q.set(fields)
        pk = q.insert(**kwargs)
        if pk:
            fields = q.fields_set.fields
            pk_name = schema.pk_name
            if pk_name:
                fields[pk_name] = pk
                self.from_interface(fields)

        else:
            ret = False

        return ret

    def update(self, **kwargs):
        """re-persist the updated field values of this orm that has a primary key"""
        ret = True
        fields = self.to_interface()

        q = self.query
        q.set(fields)

        pk = self._interface_pk
        if pk:
            q.eq_field(self.schema.pk.name, pk)

        else:
            raise ValueError("Cannot update an unhydrated orm instance")

        if q.update(**kwargs):
            fields = q.fields_set.fields
            self.from_interface(fields)

        else:
            ret = False

        return ret

    def upsert(self, **kwargs):
        """Perform an UPSERT query where we insert the fields if they don't already
        exist on the db or we UPDATE if they do

        We only want to upsert on specific occasions where we know we've set the
        conflict values and will be sending them to the db. UPSERT queries need
        to have a unique index on the table they can use for the conflict fields.

        This method will go through the indexes and try and find a unique index
        that has all fields that are being sent to the interface and it will use
        those fields as the conflict fields, it will raise a ValueError if it can't
        find a valid set of conflict fields

        :param **kwargs: passed through to the interface
        """
        ret = True

        schema = self.schema
        fields = self.to_interface()

        conflict_fields = self.conflict_fields(fields)
        if not conflict_fields:
            raise ValueError(f"Upsert failed to find the conflict field names")

        q = self.query
        q.set(fields)
        pk = q.upsert([t[0] for t in conflict_fields], **kwargs)
        if pk:
            fields = q.fields_set.fields
            pk_name = schema.pk_name
            if pk_name:
                fields[pk_name] = pk
                self.from_interface(fields)

        else:
            ret = False

        return ret

    def save(self, **kwargs):
        """
        persist the fields in this object into the db, this will update if _id is set, otherwise
        it will insert

        see also -- .insert(), .update()
        """
        ret = False

        pk = self._interface_pk
        if pk:
            ret = self.update(**kwargs)
        else:
            ret = self.insert(**kwargs)

        return ret

    def delete(self, **kwargs):
        """delete the object from the db if pk is set"""
        ret = False
        q = self.query
        pk = self._interface_pk
        if pk:
            pk_name = self.schema.pk.name
            self.query.eq_field(pk_name, pk).delete(**kwargs)

            for field_name, field in self.schema.fields.items():
                setattr(self, field_name, field.idel(self, getattr(self, field_name)))

            self._interface_pk = None
            self._interface_hydrate = False

            ret = True

        return ret

    def conflict_fields(self, fields):
        """Internal method. This will find fields that can be used for .upsert/.load

        :param fields: dict, the fields to check for values that would satisfy
            unique indexes or a primary key
        :returns: list<tuple>, a list of (field_name, field_value) tuples
        """
        schema = self.schema

        conflict_fields = []

        # we'll start with checking the primary key
        for field_name in schema.pk_names:
            if field_name in fields:
                conflict_fields.append((field_name, fields[field_name]))

            else:
                conflict_fields = []
                break

        if not conflict_fields:
            # no luck with the primary key, so let's check unique indexes 
            for index in schema.indexes.values():
                if index.unique:
                    for field_name in index.field_names:
                        if field_name in fields:
                            conflict_fields.append((field_name, fields[field_name]))

                        else:
                            conflict_fields = []
                            break

        return conflict_fields

    def load(self):
        """Given a partially populated orm try and load any missing fields from
        the db

        :returns: bool, True if it loaded from the db, False otherwise
        """
        fields = self.modified_fields
        conflict_fields = self.conflict_fields(fields)
        if not conflict_fields:
            raise ValueError(f"Load failed to find suitable fields to query on")

        q = self.query
        for field_name, field_val in conflict_fields:
            q.eq_field(field_name, field_val)

        field_names = []
        for field_name in self.schema.fields.keys():
            if field_name not in fields:
                field_names.append(field_name)

        q.select(*field_names)
        field_values = q.one()
        if field_values:
            ret = True
            fields = dict(zip(field_names, field_values))
            self.from_interface(fields)

            # can't decide if I should actually set this or not
            self._interface_hydrate = True

        else:
            ret = False

        return ret

    def requery(self):
        """Fetch this orm from the db again (ie, re-query the row from the db and
        return a new Orm instance with the columns from that row)"""
        fields = {k:v for k, v in self.fields.items() if v is not None}

        conflict_fields = self.conflict_fields(fields)
        if not conflict_fields:
            raise ValueError("Unable to refetch orm")

        q = self.query
        for field_name, field_val in conflict_fields:
            q.eq_field(field_name, field_val)

        return q.one()

    def is_modified(self, field_name=""):
        """true if a field, or any field, has been changed from its original value, false otherwise

        :param field_name: string, the name of the field you want to check for modification
        :returns: bool
        """
        if field_name:
            ret = field_name in self.modified_field_names
        else:
            ret = len(self.modified_fields_names) > 0
        return ret

    def modify(self, fields=None, **fields_kwargs):
        """update the fields of this instance with the values in dict fields

        this should rarely be messed with, if you would like to manipulate the
        fields you should override modify_fields()

        :param fields: dict, the fields in a dict
        :param **fields_kwargs: dict, if you would like to pass the fields as key=val
            this picks those up and combines them with fields
        """
        fields = self.make_dict(fields, fields_kwargs)
        fields = self.modify_fields(fields)
        schema = self.schema

        for field_name, field_val in fields.items():
            if fn := schema.field_name(field_name, None):
                setattr(self, fn, field_val)

    def modify_fields(self, fields):
        """In child classes you should override this method to do any default 
        customizations on the fields, so if you want to set defaults or anything
        you should do that here

        :param fields: dict, the fields you might want to be modified
        :returns: dict, the fields you want to actually be modified
        """
        return fields

    def __getattr__(self, k):
        ret = None
        try:
            field_name = self.schema.field_name(k)

        except AttributeError:
            # we treat pk (alias for _id) as special because we usually want the
            # pk to just return None, even if it doesn't exist, the reason why is
            # because usually you remove the pk by setting `OrmClass._id = None`
            # and so self._id would return None, so we want self.pk to return
            # None also. This should only ever be checked if the field doesn't
            # exist, and self.pk won't exist if there is no primary key
            # https://github.com/Jaymon/prom/issues/139#issuecomment-944806055
            if k != "pk":
                raise

        else:
            ret = getattr(self, field_name)

        return ret

    def __setattr__(self, k, v):
        try:
            field_name = self.schema.field_name(k)
        except AttributeError:
            field_name = k
        return super(Orm, self).__setattr__(field_name, v)

    def __delattr__(self, k):
        try:
            field_name = self.schema.field_name(k)
        except AttributeError:
            field_name = k
        return super(Orm, self).__delattr__(field_name)

    def __int__(self):
        return int(self.pk)

    def __long__(self):
        return long(self.pk)

    def __str__(self):
        #return self.__unicode__() if is_py2 else self.__bytes__()
        return str(self.pk)

    def __unicode__(self):
        return unicode(self.pk)

    def __bytes__(self):
        return bytes(self.pk)

    def jsonable(self, *args, **options):
        """
        return a public version of this instance that can be jsonified

        Note that this does not return _id, _created, _updated, the reason why is
        because lots of times you have a different name for _id (like if it is a 
        user object, then you might want to call it user_id instead of _id) and I
        didn't want to make assumptions

        note 2, I'm not crazy about the name, but I didn't like to_dict() and pretty
        much any time I need to convert the object to a dict is for json, I kind of
        like dictify() though, but I've already used this method in so many places
        """
        d = {}
        #for field_name, field in self.schema.normal_fields.items():
        for field_name, field in self.schema.fields.items():
            field_val = getattr(self, field_name, None)
            field_val = field.jsonable(self, field_val)
            if field_val is not None:
                d[field_name] = field_val

        return d

    @classmethod
    def install(cls):
        """install the Orm's table using the Orm's schema"""
        return cls.interface.set_table(cls.schema)

