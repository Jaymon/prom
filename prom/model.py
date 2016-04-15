# stdlib
import inspect
import sys
import datetime

# first party
from .query import Query, Iterator
from . import decorators, utils
from .interface import get_interface
from .config import Schema, Field, Index


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
        #pout.v("missing {}".format(pk))
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
    _created = Field(datetime.datetime, True)
    _updated = Field(datetime.datetime, True)

    @_created.isetter
    def _created(cls, val, is_update, is_modified):
        if not is_modified and not is_update:
            val = datetime.datetime.utcnow()
        return val

    @_updated.isetter
    def _updated(cls, val, is_update, is_modified):
        if not is_modified:
            val = datetime.datetime.utcnow()
        return val

    index_created = Index("_created")
    index_updated = Index("_updated")

    @decorators.classproperty
    def table_name(cls):
        return u"{}_{}".format(
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
        return query_class(orm=cls)

    @property
    def pk(self):
        """wrapper method to return the primary key, None if the primary key is not set"""
        return getattr(self, self.schema.pk.name, None)

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
        self.reset_modified()
        self.modify(fields, **fields_kwargs)

    @classmethod
    def pool(cls, size=0):
        """
        return a new OrmPool instance

        return -- OrmPool -- the orm pool instance will be tied to this Orm
        """
        return OrmPool(orm=cls, size=size)

    @classmethod
    def create(cls, fields=None, **fields_kwargs):
        """
        create an instance of cls with the passed in fields and set it into the db

        fields -- dict -- field_name keys, with their respective values
        **fields_kwargs -- dict -- if you would rather pass in fields as name=val, that works also
        """
        # NOTE -- you cannot use populate here because populate alters modified fields
        instance = cls(fields, **fields_kwargs)
        instance.save()
        return instance

    @classmethod
    def populate(cls, fields=None, **fields_kwargs):
        """
        create an instance of cls with the passed in fields but don't set it into the db or mark the passed
        in fields as modified, this is used by the Query class to hydrate objects

        fields -- dict -- field_name keys, with their respective values
        **fields_kwargs -- dict -- if you would rather pass in fields as name=val, that works also
        """
        fields = utils.make_dict(fields, fields_kwargs)
        for k, field in cls.schema.fields.items():
            fields[k] = field.iget(
                cls,
                fields.get(k, None)
            )

        instance = cls(fields)
        instance.reset_modified()
        return instance

    @classmethod
    def depart(cls, fields, is_update):
        """
        return a dict of fields that is ready to be persisted into the db

        fields -- dict -- the raw fields that haven't been processed with any
            schema iset functions yet
        is_update -- boolean -- True if getting fields for an update query, False
            if for an insert query

        return -- dict -- the fields all ran through iset functions
        """
        schema = cls.schema
        for k, field in schema.fields.items():
            is_modified = k in fields
            v = field.iset(
                cls,
                fields[k] if is_modified else None,
                is_update=is_update,
                is_modified=is_modified
            )
            if is_modified or (v is not None):
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
        q = self.query
        q.set_fields(self.get_modified())
        pk = q.insert()
        if pk:
            fields = q.fields
            fields[schema.pk.name] = pk
            self.modify(fields)
            self.reset_modified()

        else:
            ret = False

        return ret

    def update(self):
        """re-persist the updated field values of this orm that has a primary key"""
        ret = True
        q = self.query
        q.set_fields(self.get_modified())

        pk = self.pk
        if pk:
            q.is_field(self.schema.pk.name, pk)

        else:
            raise ValueError("You cannot update without a primary key")

        if q.update():
            self.modify(q.fields)
            self.reset_modified()

        else:
            ret = False

        return ret

    def set(self): return self.save()
    def save(self):
        """
        persist the fields in this object into the db, this will update if _id is set, otherwise
        it will insert

        see also -- .insert(), .update()
        """
        ret = False

        # we will only use the primary key if it hasn't been modified
        pk = None
        if self.schema.pk.name not in self.modified_fields:
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

    def modify_fields(self, fields=None, **fields_kwargs):
        return utils.make_dict(fields, fields_kwargs)

    def get_modified(self):
        """return the modified fields and their new values"""
        fields = {}
        for field_name in self.modified_fields:
            fields[field_name] = getattr(self, field_name)

        return fields

    def is_modified(self):
        """true if a field has been changed from its original value, false otherwise"""
        return len(self.modified_fields) > 0

    def reset_modified(self):
        """
        reset field modification tracking

        this is handy for when you are loading a new Orm with the results from a query and
        you don't want set() to do anything, you can Orm(**fields) and then orm.reset_modified() to
        clear all the passed in fields from the modified list
        """
        self.modified_fields = set()

    def modify(self, fields=None, **fields_kwargs):
        """update the fields of this instance with the values in dict fields"""
        modified_fields = set()
        fields = self.modify_fields(fields, **fields_kwargs)
        for field_name, field_val in fields.items():
            in_schema = field_name in self.schema.fields
            if in_schema:
                setattr(self, field_name, field_val)
                modified_fields.add(field_name)

        return modified_fields

    def __setattr__(self, field_name, field_val):
        if field_name in self.schema.fields:
            if field_name == self.schema.pk.name:
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
        def default_field_type(field_type):
            r = ''
            if issubclass(field_type, int):
                r = 0
            elif issubclass(field_type, bool):
                r = False
            elif issubclass(field_type, float):
                r = 0.0

            return r

        for field_name, field in self.schema.normal_fields.items():
            try:
                d[field_name] = getattr(self, field_name, None)
                if d[field_name]:
                    if isinstance(d[field_name], datetime.date):
                        d[field_name] = str(d[field_name])
                    elif isinstance(d[field_name], datetime.datetime):
                        d[field_name] = str(d[field_name])

                else:
                    d[field_name] = default_field_type(field.type)

            except AttributeError:
                d[field_name] = default_field_type(field.type)

        return d

    @classmethod
    def install(cls):
        """install the Orm's table using the Orm's schema"""
        return cls.interface.set_table(cls.schema)

