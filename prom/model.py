# -*- coding: utf-8 -*-
from __future__ import unicode_literals, division, print_function, absolute_import
import inspect
import sys
import datetime

# first party
from .query import Query, Iterator
from . import decorators, utils
from .interface import get_interface
from .config import Schema, Field, ObjectField, Index
from .compat import *


class OrmPool(utils.Pool):
    """
    Create a pool of Orm instances, which is just a dict of primary_key -> Orm instance
    mappings

    Let's say you are iterating through millions of rows of Foo, and for each Foo
    instance you need to get the Bar instance from the Foo.bar_id field, and lots of
    Foos have the same bar_id, but you only want to pull the Bar instance from
    the db once, this allows you to easily do that

    example --
        bar_pool = Bar.pool(500) # keep the pool contained to the last 500 Bar instances
        for f in Foo.query.all():
            b = bar_pool[f.bar_id]
            print "Foo {} loves Bar {}".format(f.pk, b.pk)
    """
    def __init__(self, orm_class, size=0):
        super(OrmPool, self).__init__(size=size)
        self.orm_class = orm_class

    def create_value(self, pk):
        return self.orm_class.query.get_pk(pk)


class Orm(object):
    """
    this is the parent class of any model Orm class you want to create that can access the db

    example -- create a user class

        import prom

        class User(prom.Orm):
            table_name = "user_table_name"

            username = prom.Field(str, True, unique=True) # set a unique index on user
            password = prom.Field(str, True)
            email = prom.Field(str, True)

            index_email = prom.Index('email') # set a normal index on email

        # create a user
        u = User(username='foo', password='awesome_and_secure_pw_hash', email='foo@bar.com')
        u.set()

        # query for our new user
        u = User.query.is_username('foo').get_one()
        print u.username # foo
    """

    connection_name = ""
    """the name of the connection to use to retrieve the interface"""

    query_class = Query
    """the class this Orm will use to create Query instances to query the db"""

    iterator_class = Iterator
    """the class this Orm will use for iterating through results returned from db"""

    _id = Field(long, True, pk=True)
    _created = Field(datetime.datetime, True, idefault_insert=lambda *_, **__: datetime.datetime.utcnow())
    _updated = Field(datetime.datetime, True, idefault=lambda *_, **__: datetime.datetime.utcnow())

#     @_created.isetter
#     def _created(self, val, is_update, is_modified):
#         if not is_modified and not is_update:
#             val = datetime.datetime.utcnow()
#         return val
# 
#     @_updated.isetter
#     def _updated(self, val, is_update, is_modified):
#         if not is_modified:
#             val = datetime.datetime.utcnow()
#         return val

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
        """
        return a new Query instance ready to make a db call using the child class

        example -- fluid interface
            results = Orm.query.is_foo('value').desc_bar().get()

        return -- Query() -- every time this is called a new query instance is created using cls.query_class
        """
        query_class = cls.query_class
        return query_class(orm_class=cls)

    @property
    def pk(self):
        """wrapper method to return the primary key, None if the primary key is not set"""
        pk_name = self.schema.pk_name
        return getattr(self, pk_name, None) if pk_name else None

    @property
    def created(self):
        """wrapper property method to return the created timestamp"""
        return getattr(self, self.schema._created.name, None)

    @property
    def updated(self):
        """wrapper property method to return the updated timestamp"""
        return getattr(self, self.schema._updated.name, None)

    @property
    def fields(self):
        """
        return all the fields and their raw values for this Orm instance. This
        property returns a dict with the field names and their current values

        if you want to control the values for outputting to an api, use .jsonable()
        """
        return {k:getattr(self, k, None) for k in self.schema.fields}

    def __init__(self, fields=None, **fields_kwargs):
        """Create an Orm object

        :param fields: dict, the fields in a dict
        :param **fields_kwargs: dict, if you would like to pass the fields as key=val
        """
        self.reset_modified()

        fields = self.make_dict(fields, fields_kwargs)
        if fields:
            self.modify(fields)

    @classmethod
    def pool(cls, size=0):
        """
        return a new OrmPool instance

        return -- OrmPool -- the orm pool instance will be tied to this Orm
        """
        return OrmPool(orm_class=cls, size=size)

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
    def populated(cls, fields=None, **fields_kwargs):
        """return a populated instance with the present fields

        NOTE -- you probably shouldn't override this method since the Query methods
        rely on this method signature to create each instance of the results

        :param fields: dict, the fields to populate in this instance
        :param **fields_kwargs: dict, the fields in key=val form to populate in this instance
        :returns: an instance of this class with populated fields
        """
        instance = cls()
        instance.populate(fields, **fields_kwargs)
        return instance

    @classmethod
    def make_dict(cls, fields, fields_kwargs):
        """Lots of methods take a dict and key=val for fields, this combines fields
        and fields_kwargs into one master dict, turns out we want to do this more
        than I would've thought to keep api compatibility with prom proper

        :param fields: dict, the fields in a dict
        :param fields_kwargs: dict, if you would like to pass the fields as key=val
            this picks those up and combines them with fields
        :returns: dict, the combined fields
        """
        return utils.make_dict(fields, fields_kwargs)

    def fk(self, orm_class):
        """find the field value in self that is the primary key of the passed in orm_class

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

    def populate(self, fields=None, **fields_kwargs):
        """take the passed in fields, combine them with missing fields that should
        be there and then run all those through appropriate methods to hydrate this
        orm.

        The method replaces cls.hydrate() since it was becoming hard to understand
        what was going on with all these methods that did things just a little bit
        different.

        This is used to completely set all the fields of self. If you just want
        to populate certain fields, you can use the submethod populate_fields

        :param fields: dict, the fields in a dict that will be combined with all the
            fields of the Orm
        :param **fields_kwargs: dict, if you would like to pass the fields as key=val
            this picks those up and combines them with fields
        """
        pop_fields = {}
        fields = self.make_dict(fields, fields_kwargs)
        for k in self.schema.fields.keys():
            pop_fields[k] = fields.get(k, None)

        self.populate_fields(pop_fields)

    def populate_fields(self, fields):
        """this runs all the fields through their iget methods to mimic them
        freshly coming out of the db, then resets modified

        This differs from the .populate() method because it applies only to the
        passed in fields, the .populate() method applies to *all supported* fields
        of this orm while .populate_fields() only applies to the passed in fields

        :param fields: dict, only the fields you want to populate
        """
        schema = self.schema
        for k, v in fields.items():
            fields[k] = schema.fields[k].iget(self, v)

        self.modify(fields)

        for field_name in fields.keys():
            self.reset_modified(field_name)

    def depopulate(self, is_update):
        """Get all the fields that need to be saved

        :param is_udpate: bool, True if update query, False if insert
        :returns: dict, key is field_name and val is the field value to be saved
        """
        fields = {}
        schema = self.schema
        for k, field in schema.fields.items():
            is_modified = k in self.modified_fields
            orig_v = getattr(self, k)
            v = field.isave(
                self,
                orig_v,
                is_update=is_update,
                is_modified=is_modified
            )

            # if v has a value or it's been modified (even if it doesn't have a
            # value) then we will probably want to pass the value onto the db
            if is_modified or v is not None:
                if is_update and field.is_pk() and v == orig_v:
                    continue

                else:
                    fields[k] = v

        if not is_update:
            for field_name in schema.required_fields.keys():
                if field_name not in fields:
                    raise KeyError("Missing required field {}".format(field_name))

        return fields

    def insert(self):
        """persist the field values of this orm"""
        ret = True

        schema = self.schema
        fields = self.depopulate(False)


        q = self.query
        q.set_fields(fields)
        pk = q.insert()
        if pk:
            fields = q.fields
            pk_name = schema.pk_name
            if pk_name:
                fields[pk_name] = pk
            self.populate_fields(fields)

        else:
            ret = False

        return ret

    def update(self):
        """re-persist the updated field values of this orm that has a primary key"""
        ret = True
        fields = self.depopulate(True)
        q = self.query
        q.set_fields(fields)

        pk = self.pk
        if pk:
            q.is_field(self.schema.pk.name, pk)

        else:
            raise ValueError("You cannot update without a primary key")

        if q.update():
            fields = q.fields
            self.populate_fields(fields)

        else:
            ret = False

        return ret

    def save(self):
        """
        persist the fields in this object into the db, this will update if _id is set, otherwise
        it will insert

        see also -- .insert(), .update()
        """
        ret = False

        # we will only use the primary key if it hasn't been modified
        pk = None
        pk_name = self.schema.pk_name
        if pk_name not in self.modified_fields:
            pk = self.pk

        if pk:
            ret = self.update()
        else:
            ret = self.insert()

        return ret

    def delete(self):
        """delete the object from the db if pk is set"""
        ret = False
        q = self.query
        pk = self.pk
        if pk:
            pk_name = self.schema.pk.name
            self.query.is_field(pk_name, pk).delete()
            setattr(self, pk_name, None)

            # mark all the fields that still exist as modified
            self.reset_modified()
            for field_name in self.schema.fields:
                if getattr(self, field_name, None) != None:
                    self.modified_fields.add(field_name)

            ret = True

        return ret

    def is_modified(self):
        """true if a field has been changed from its original value, false otherwise"""
        return len(self.modified_fields) > 0

    def reset_modified(self, field_name=""):
        """
        reset field modification tracking

        this is handy for when you are loading a new Orm with the results from a query and
        you don't want set() to do anything, you can Orm(**fields) and then orm.reset_modified() to
        clear all the passed in fields from the modified list

        :param field_name: str, if present then only reset that particular field
        """
        if field_name:
            field = self.schema.fields[field_name]
            if not isinstance(field, ObjectField):
                self.modified_fields.discard(field_name)

        else:
            self.modified_fields = set()

            # compensate for us not having knowledge of certain fields changing
            for field_name, field in self.schema.normal_fields.items():
                if isinstance(field, ObjectField):
                    self.modified_fields.add(field_name)

    def modify(self, fields=None, **fields_kwargs):
        """update the fields of this instance with the values in dict fields

        this should rarely be messed with, if you would like to manipulate the
        fields you should override modify_fields()

        :param fields: dict, the fields in a dict
        :param **fields_kwargs: dict, if you would like to pass the fields as key=val
            this picks those up and combines them with fields
        :returns: set, all the names of the fields that were modified
        """
        modified_fields = set()
        fields = self.make_dict(fields, fields_kwargs)
        fields = self.modify_fields(fields)
        for field_name, field_val in fields.items():
            in_schema = field_name in self.schema.fields
            if in_schema:
                setattr(self, field_name, field_val)
                modified_fields.add(field_name)

        return modified_fields

    def modify_fields(self, fields):
        """In child classes you should override this method to do any default 
        customizations on the fields, so if you want to set defaults or anything
        you should do that here

        :param fields: dict, the fields you might want to be modified
        :returns: dict, the fields you want to actually be modified
        """
        return fields

    def __setattr__(self, field_name, field_val):
        if field_name in self.schema.fields:
            if self.schema.fields[field_name].is_pk():
                # we mark everything as dirty because the primary key has changed
                # and so a new row would be inserted into the db
                self.modified_fields.add(field_name)
                self.modified_fields.update(self.schema.normal_fields.keys())

            else:
                self.modified_fields.add(field_name)

        super(Orm, self).__setattr__(field_name, field_val)

    def __delattr__(self, field_name):
        if field_name in self.schema.fields:
            self.modified_fields.add(field_name)

        super(Orm, self).__delattr__(field_name)

    def __int__(self):
        return int(self.pk)

    def __long__(self):
        return long(self.pk)

    def __str__(self):
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

