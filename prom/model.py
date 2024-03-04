# -*- coding: utf-8 -*-

from datatypes import (
    EnglishWord,
    NamingConvention,
    classproperty,
    OrderedSubclasses,
    ReflectModule,
    Environ,
    Dirpath,
    ReflectPath,
)

from .compat import *
from .query import Query, Iterator
from . import utils
from .interface import get_interface
from .config import (
    Schema,
    AutoDatetime,
    AutoIncrement,
)


class Orms(OrderedSubclasses):
    """Holds all the Orms loaded into memory

    See Orm.__init_subclass__, this is a class attribute found Orm.orm_classes.

    See Orm.find_orm_class to see how this can be used exeternally

    This class is a hybrid Mapping and Sequence, if you iterate through it will
    iterate the values like a list, if you use item indexes then it will act
    like a dict
    """
    def default_cutoff(self):
        return (Orm,)

    def __init__(self):
        super().__init__()

        # set to True in .insert_modules
        self.inserted_modules = False

        # holds any loaded model prefixes
        self.model_prefixes = set()

        # model(s)_name is the key, an Orm class child is the value
        self.lookup_table = {}

        # should always be `prom` and is used in ._is_valid_subclass to make
        # sure only true child classes are inserted
        self.module_name = ReflectModule(__name__).modroot

    def _insert(self, orm_class, class_info):
        super()._insert(orm_class, class_info)

        if not class_info["in_info"]:
            # this orm class is the new edge for this model name
            self.lookup_table[orm_class.model_name] = orm_class
            self.lookup_table[orm_class.models_name] = orm_class

    def __getitem__(self, index_or_name):
        """If int then treat it like getting the index of a list, if str then
        treat it like fetching a key on a dictionary

        :param index_or_name: int|str, either the index of the list you want or
            the model name you want
        :returns: type, the requested Orm child class
        """
        if isinstance(index_or_name, int):
            return super().__getitem__(index_or_name)

        else:
            return self.lookup_table[index_or_name]

    def __contains__(self, name_or_class):
        """If str then it checks model_name keys as a dict, if type then it will
        check for the class in the list

        :param name_or_class: str|type
        :returns: bool
        """
        if isinstance(name_or_class, str):
            return name_or_class in self.lookup_table

        else:
            return super().__contains__(name_or_class)

    def insert_modules(self):
        """Goes through the PROM_PREFIX evnironment variables and loads any
        found module classpaths and loads all the Orm classes found in those
        modules
        """
        if not self.inserted_modules:
            environ = Environ("PROM_")
            for modpath in environ.paths("PREFIX"):
                self.model_prefixes.add(modpath)
                super().insert_modules(modpath)

            # if there aren't any defined prefixes let's inspect the current
            # working directory
            if not self.model_prefixes:
                rp = ReflectPath(Dirpath.cwd())
                for mod in rp.find_modules("models"):
                    self.model_prefixes.add(mod.__name__)

            self.inserted_modules = True

    def get(self, model_name):
        """Returns the Orm class found at model_name

        :param model_name: str, the model name you want
        :returns: type, Orm chiild class
        """
        try:
            return self.lookup_table[model_name]

        except KeyError:
            return None

    def _is_valid_subclass(self, orm_class, cutoff_classes):
        ret = super()._is_valid_subclass(orm_class, cutoff_classes)
        if ret:
            # while we check for Orm derived child classes, we also don't want
            # any Orm child classes that are defined in prom since those are
            # also base classes and we're only interested in valid child classes
            # that can access a db
            ret = not orm_class.__module__.startswith(self.module_name)

        return ret


class Orm(object):
    """
    this is the parent class of any Orm child class you want to create that can
    access the db

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
    """the name of the connection to use to retrieve the interface

    In your DSNs you can set a connection name using the fragment, then the
    Interface instance that matches fragment and .connection_name will be used
    by this orm to interact with the db
    """

    query_class = Query
    """the class this Orm will use to create Query instances to query the db

    You can override this in your child class and you create new instances by
    calling .query
    """

    iterator_class = Iterator
    """the class this Orm will use for iterating through results returned from
    db

    This is returned from the Query.get
    """

    orm_classes = Orms()
    """This will hold all other orm classes that have been loaded into memory
    the class path is the key and the class object is the value"""

    _id = AutoIncrement(aliases=["id"])
    """The primary key is an auto-increment integer by default

    You can override this in your child class if you want a different primary
    key, I wouldn't change the name though. The _id name comes from MongoDB
    """

    _created = AutoDatetime(created=True, updated=False, aliases=["created"])
    """Anytime a new row is created this will be populated

    If you don't want this functionality just do `_created = None` in your
    child class
    """

    _updated = AutoDatetime(created=False, updated=True, aliases=["updated"])
    """Anytime a row is written to this will be updated, that means it will have
    roughly the same value as ._created when the row is first inserted

    If you don't want this functionality just do `_updated = None` in your
    child class
    """

    @classproperty
    def table_name(cls):
        """The name of the table this orm is wrapping

        To be more explicit it is nice to override this attribute in your child
        classes, but it's nice to have a default value when rapidly prototyping
        """
        return NamingConvention("{}_{}".format(
            cls.__module__.replace(".", "_"),
            cls.__name__
        )).snakecase()

    @classproperty
    def model_name(cls):
        """Returns the name for this orm

        :Example:
            class Collection(Orm): pass

            print(Collection.model_name) # collection
            print(Collection.models_name) # collections

        :param plural: bool, True if you would like the plural model name
        :returns: str, the model name in either singular or plural form
        """
        return NamingConvention(cls.__name__).snakecase()

    @classproperty
    def models_name(cls):
        """Returns the plural name for this orm

        :see: .model_name
        :returns: str, the model name in plural form
        """
        return EnglishWord(cls.model_name).plural()

    @classproperty
    def schema(cls):
        """the Schema instance that this class will derive all its db info from

        Unless you really know what you are doing you should never have to set
        this value, it will be automatically created using the Field instances
        you define on your child class
        """
        return Schema.get_instance(cls)

    @classproperty
    def interface(cls):
        """
        return an Interface instance that can be used to access the db

        :returns: Interface, the interface instance this Orm will use
        """
        return get_interface(cls.connection_name)

    @classproperty
    def query(cls):
        """return a new Query instance ready to make a db call using the child
        class

        :example:
            # fluid interface
            results = await Orm.query.is_foo('value').desc_bar().get()

        :returns: Query, every time this is called a new query instance is
            created using the .query_class attribute
        """
        query_class = cls.query_class
        return query_class(orm_class=cls)

    @property
    def field_names(self):
        """Return all the field names

        :returns: list[str], the field names
        """
        return [k for k in self.schema.fields]

    @property
    def fields(self):
        """return all the fields and their raw values for this Orm instance.
        This property returns a dict with the field names and their current
        values

        if you want to control the values for outputting to an api, use
        .jsonable() instead

        :returns: dict[str, Any]
        """
        return {k:getattr(self, k, None) for k in self.schema.fields}

    @property
    def modified_field_names(self):
        """Return all the field names that are currently modified

        :returns: set[str], all the fields that are currently considered 
            modified on this instance
        """
        modified_field_names = set()
        for field_name, field in self.schema.fields.items():
            if field.modified(self, getattr(self, field_name)):
                modified_field_names.add(field_name)
        return modified_field_names

    @property
    def modified_fields(self):
        """Return a dict of field_names/field_values for all the currently
        modified fields

        :returns: dict[str, Any]
        """
        return {k:getattr(self, k) for k in self.modified_field_names}

    @classmethod
    async def transaction(cls, **kwargs):
        """Create a transaction for this Orm

        :Example:
            async with FooOrm.transaction() as conn:
                o = FooOrm(foo=1)
                o.save(connection=conn)

        :param **kwargs: passed through to the Interface.transaction
            * prefix: str, the name of the transaction you want to use
            * nest: bool, True if you want nested transactions to be created,
                False to ignore nested transactions
        :returns: Connection, the connection instance with an active tx
        """
        kwargs.setdefault("nest", False)
        kwargs.setdefault("prefix", f"{cls.__name__}_{cls.connection_name}_tx")
        return cls.interface.transaction(**kwargs)

    @classmethod
    async def create(cls, *args, **kwargs):
        """
        create an instance of cls with the passed in fields and set it into the
        db

        this method takes *args, **kwargs because a child class can override
        .__init__ and it's nice to not have to modify this method also

        :param *args: passed directly to .__init__
        :param **kwargs: passed directly to .__init__
        :returns: Orm instance that has been saved into the db
        """
        # NOTE -- you cannot use hydrate/populate here because populate alters
        # modified fields
        connection = kwargs.pop("connection", None)

        instance = cls(*args, **kwargs)

        if connection:
            await instance.save(connection=connection)

        else:
            await instance.save()

        return instance

    @classmethod
    def hydrate(cls, fields=None, **fields_kwargs):
        """return a populated instance with the present fields

        NOTE -- you probably shouldn't override this method since 
        Iterator.hydrate relies on this method signature to create each instance

        :param fields: dict, the fields to populate in this instance
        :param **fields_kwargs: dict, the fields in key=val form to populate in
            this instance
        :returns: an instance of this class with populated fields
        """
        instance = cls()
        fields = cls.make_dict(fields, fields_kwargs)
        instance.from_interface(fields)
        instance._interface_hydrate = True
        return instance

    @classmethod
    def make_dict(cls, fields, fields_kwargs, schema=None):
        """Lots of methods take a dict and key=val for fields, this combines
        fields and fields_kwargs into one master dict, turns out we want to do
        this more than I would've thought to keep api compatibility with prom
        proper

        :param fields: dict, the fields in a dict
        :param fields_kwargs: dict, if you would like to pass the fields as
            key=val this picks those up and combines them with fields
        :schema: Schema, if passed in then this will normalize field names and
            resolve any aliases
        :returns: dict, the combined fields
        """
        fields = utils.make_dict(fields, fields_kwargs)

        if schema:
            # since schema is passed in resolve any aliases
            for field_name in list(fields.keys()):
                if fn := schema.field_name(field_name, ""):
                    fields[fn] = fields.pop(field_name)

        return fields

    @classmethod
    def find_orm_class(cls, model_name):
        """Using the internal Orm class tracker (Orm.orm_classes) return the
        Orm class for model_name

        This is handy for introspection and enables a whole bunch of magic in
        various places

        :param model_name: str, the model name you're looking for
        :returns: type, the Orm class where Orm.model_name matches
        """
        if model_name not in cls.orm_classes:
            cls.orm_classes.insert_modules()

        try:
            return cls.orm_classes[model_name]

        except KeyError as e:
            raise ValueError(
                f"Could not find an orm_class for {model_name}"
            ) from e

    @classmethod
    def add_orm_class(cls, orm_class):
        cls.orm_classes.insert(orm_class)

        #classpath = f"{orm_class.__module__}:{orm_class.__qualname__}"
        #cls.orm_classes[classpath] = orm_class

    def __init__(self, fields=None, **fields_kwargs):
        """Create an Orm object

        While you can override this method to customize the signature, you might
        also need to override .hydrate (but don't change .hydrate's signature)
        since .hydrate creates an instance using no arguments

        NOTE -- Honestly, I've tried it multiple times and it's almost never
            worth overriding this method nor .hydrate. If you ever get tempted
            just say no!

        :param fields: dict, the fields in a dict
        :param **fields_kwargs: if you would like to pass the fields as key=val
        """
        self._interface_pk = None
        self._interface_hydrate = False

        schema = self.schema
        fields = self.make_dict(fields, fields_kwargs, schema=schema)

        # set defaults
        for field_name, field in schema.fields.items():
            fields[field_name] = field.fdefault(
                self,
                fields.get(field_name, None)
            )

        self.modify(fields)

    def __init_subclass__(cls):
        """When a child class is loaded into memory it will be saved into
        .orm_classes, this way every orm class knows about all the other orm
        classes, this is the method that makes that possible magically

        https://peps.python.org/pep-0487/
        """
        super().__init_subclass__()

        cls.add_orm_class(cls)

    def fk(self, orm_class):
        """find the field value in self that is the primary key of the passed in
        orm_class

        :example:
            class Foo(Orm):
                pass

            class Bar(Orm):
                foo_id = Field(Foo)

            b = Bar(foo_id=1)
            print(b.fk(Foo)) # 1

        :param orm_class: Orm, the fields in self will be checked until the
            field that references Orm is found, then the value of that field
            will be returned
        :returns: the self field value that is a foreign key references to
            orm_class
        """
        for field_name, field in self.schema.ref_fields.items():
            if field.schema is orm_class.schema:
                return getattr(self, field_name)

        raise ValueError(
            "Did not find a foreign key reference for {} in {}".format(
                orm_class.__name__,
                self.__class__.__name__,
            )
        )

    def ref(self, orm_classpath):
        """see Query.ref() for an explanation of what this method does

        :param orm_classpath: string|type, a full python class path (eg,
            foo.bar.Che) or an actual model.Orm python class
        :returns: Orm
        """
        return self.query.ref(orm_classpath).orm_class

    def ref_class(self, orm_classpath):
        """Alias for .ref to be more consistent with other *_class attributes"""
        return self.ref(orm_classpath)

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
            if field_name in schema.fields:
                fields[field_name] = schema.fields[field_name].iget(self, v)

        self.modify(fields)

        # this marks that this was repopulated from the interface (database)
        self._interface_pk = self.pk

    def to_interface(self):
        """Get all the fields that need to be persisted into the db

        :returns: dict[str, Any], key is field_name and val is the field value
            to be saved
        """
        fields = {}
        schema = self.schema
        for k, field in schema.fields.items():
            v = field.iset(self, getattr(self, k))

            is_modified = field.modified(self, v)
            if is_modified:
                if not field.options.get("empty", True) and not v:
                    raise ValueError(
                        f"{self.__class__.__name__}.{k} cannot be empty"
                    )

                fields[k] = v

            if v is None and field.is_required():
                if field.is_pk():
                    if is_modified:
                        raise KeyError(
                            "Primary key has been removed and is required"
                        )

                else:
                    if self.is_insert() or is_modified:
                        raise KeyError(
                            "Missing required field {}.{}".format(
                                self.__class__.__name__,
                                k
                            )
                        )

        return fields

    async def insert(self, **kwargs):
        """persist the field values of this orm"""
        ret = True

        schema = self.schema
        fields = self.to_interface()

        q = self.query
        q.set(fields)
        pk = await q.insert(**kwargs)
        if pk:
            fields = q.fields_set.fields
            pk_name = schema.pk_name
            if pk_name:
                fields[pk_name] = pk
                self.from_interface(fields)

        else:
            ret = False

        return ret

    async def update(self, **kwargs):
        """re-persist the updated field values of this orm that has a primary
        key"""
        ret = True
        fields = self.to_interface()

        q = self.query
        q.set(fields)

        pk = self._interface_pk
        if pk:
            q.eq_field(self.schema.pk.name, pk)

        else:
            raise ValueError("Cannot update an unhydrated orm instance")

        if await q.update(**kwargs):
            fields = q.fields_set.fields
            self.from_interface(fields)

        else:
            ret = False

        return ret

    async def upsert(self, **kwargs):
        """Perform an UPSERT query where we insert the fields if they don't
        already exist on the db or we UPDATE if they do

        We only want to upsert on specific occasions where we know we've set the
        conflict values and will be sending them to the db. UPSERT queries need
        to have a unique index on the table they can use for the conflict fields

        This method will go through the indexes and try and find a unique index
        that has all fields that are being sent to the interface and it will use
        those fields as the conflict fields, it will raise a ValueError if it
        can't find a valid set of conflict fields

        :param **kwargs: passed through to the interface
        """
        ret = True

        if pk := self._interface_pk:
            ret = await self.update(**kwargs)

        else:
            schema = self.schema
            fields = self.to_interface()

            conflict_fields = self.conflict_fields(fields)
            if not conflict_fields:
                raise ValueError(
                    "Failed to find conflict field names from: {}".format(
                        list(fields.keys())
                    )
                )

            q = self.query
            q.set(fields)
            pk = await q.upsert([t[0] for t in conflict_fields], **kwargs)
            if pk:
                fields = q.fields_set.fields
                pk_name = schema.pk_name
                if pk_name:
                    fields[pk_name] = pk
                self.from_interface(fields)

            else:
                ret = False

        return ret

    async def save(self, **kwargs):
        """persist the fields in this object into the db, this will update if
        _id is set, otherwise it will insert

        see also -- .insert(), .update()
        """
        ret = False

        pk = self._interface_pk
        if pk:
            ret = await self.update(**kwargs)
        else:
            ret = await self.insert(**kwargs)

        return ret

    async def delete(self, **kwargs):
        """delete the object from the db if pk is set"""
        ret = False
        q = self.query
        pk = self._interface_pk
        if pk:
            pk_name = self.schema.pk.name
            await self.query.eq_field(pk_name, pk).delete(**kwargs)

            for field_name, field in self.schema.fields.items():
                setattr(
                    self,
                    field_name,
                    field.idel(self, getattr(self, field_name))
                )

            self._interface_pk = None
            self._interface_hydrate = False

            ret = True

        return ret

    def conflict_fields(self, fields):
        """Internal method. This will find fields that can be used for
        .upsert/.load

        :param fields: dict, the fields to check for values that would satisfy
            unique indexes or a primary key
        :returns: list<tuple>, a list of (field_name, field_value) tuples
        """
        conflict_fields = []

        schema = self.schema

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
                            conflict_fields.append(
                                (field_name, fields[field_name])
                            )

                        else:
                            conflict_fields = []
                            break

                    if conflict_fields:
                        break

        return conflict_fields

    async def load(self):
        """Given a partially populated orm try and load any missing fields from
        the db

        :returns: bool, True if it loaded from the db, False otherwise
        """
        fields = self.modified_fields
        conflict_fields = self.conflict_fields(fields)
        if not conflict_fields:
            raise ValueError("Load failed to find suitable fields to query on")

        q = self.query
        for field_name, field_val in conflict_fields:
            q.eq_field(field_name, field_val)

        field_names = []
        for field_name in self.schema.fields.keys():
            if field_name not in fields:
                field_names.append(field_name)

        q.select(*field_names)
        field_values = await q.one()
        if field_values:
            ret = True
            fields = dict(zip(field_names, field_values))
            self.from_interface(fields)

            # can't decide if I should actually set this or not
            self._interface_hydrate = True

        else:
            ret = False

        return ret

    async def requery(self):
        """Fetch this orm from the db again (ie, re-query the row from the db
        and return a new Orm instance with the columns from that row)"""
        fields = {k:v for k, v in self.fields.items() if v is not None}

        conflict_fields = self.conflict_fields(fields)
        if not conflict_fields:
            raise ValueError("Unable to refetch orm")

        q = self.query
        for field_name, field_val in conflict_fields:
            q.eq_field(field_name, field_val)

        return await q.one()

    def is_modified(self, field_name=""):
        """true if a field, or any field, has been changed from its original
        value, false otherwise

        :param field_name: string, the name of the field you want to check for
            modification
        :returns: bool
        """
        if field_name:
            ret = field_name in self.modified_field_names

        else:
            ret = len(self.modified_field_names) > 0

        return ret

    def modify(self, fields=None, **fields_kwargs):
        """update the fields of this instance with the passed in values

        this should rarely be messed with, if you would like to manipulate the
        fields you should override .modify_fields

        :param fields: dict, the fields in a dict
        :param **fields_kwargs: dict, if you would like to pass the fields as
            key=val this picks those up and combines them with fields
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
        """
        NOTE -- this is a hybrid method, sometimes it will return coroutines
        """
        ret = None
        try:
            field_name = self.schema.field_name(k)

        except AttributeError:
            # Go through looking for a FK's model name, if a match is found then
            # load that FK's row using this orm's field value and return an
            # orm instance where this row's orm field value is the primary key
            for ref_field_name, ref_field in self.schema.ref_fields.items():
                ref_class = ref_field.ref
                if k == ref_class.model_name:
                    ref_field_value = getattr(self, ref_field_name, None)

                    if ref_field_value:
                        # this is a coroutine
                        ret = ref_class.query.eq_pk(ref_field_value).one()

                    else:
                        # we do this so if there isn't a value it can still be
                        # awaited and return None
                        async def await_none(): return None
                        ret = await_none()

                    return ret

            # Go through all the orm_classes looking for a model_name or
            # models_name match and query that model using that model's FK field
            # name that matches self.pk
            if orm_class := self.orm_classes.get(k):
                ref_items = orm_class.schema.ref_fields.items()
                for ref_field_name, ref_field in ref_items:
                    ref_class = ref_field.ref
                    if ref_class and isinstance(self, ref_class):
                        query = orm_class.query.eq_field(
                            ref_field_name,
                            self.pk
                        )
                        if k == orm_class.models_name:
                            # this is a coroutine
                            return query.get()

                        else:
                            # this is a coroutine
                            return query.one()

            raise

        else:
            ret = getattr(self, field_name)

        return ret

    def __setattr__(self, k, v):
        try:
            field_name = self.schema.field_name(k)

        except AttributeError:
            for ref_field_name, ref_field in self.schema.ref_fields.items():
                ref_class = ref_field.ref
                if k == ref_class.model_name:
                    k = ref_field_name
                    v = v.pk
                    break

            field_name = k
        return super().__setattr__(field_name, v)

    def __delattr__(self, k):
        try:
            field_name = self.schema.field_name(k)

        except AttributeError:
            field_name = k

        return super(Orm, self).__delattr__(field_name)

    def __int__(self):
        """Syntactic sugar to get the primary key as an int"""
        return int(self.pk)

    def __str__(self):
        """Syntactic sugar to get the primary key as a string"""
        return str(self.pk)

    def __bytes__(self):
        """Syntactic sugar to get the primary key as bytes"""
        return bytes(self.pk)

    def jsonable(self, *args, **options):
        """
        return a public version of this instance that can be jsonified

        Note that this does not return _id, _created, _updated, the reason why
        is because lots of times you have a different name for _id (like if it
        is a user object, then you might want to call it user_id instead of _id)
        and I didn't want to make assumptions

        note 2, I'm not crazy about the name, but I didn't like to_dict() and
        pretty much any time I need to convert the object to a dict is for json,
        I kind of like dictify() though, but I've already used this method in so
        many places. Another name I don't mind is .tojson
        """
        d = {}
        for field_name, field in self.schema.fields.items():
            field_name, field_val = field.jsonable(
                self,
                field_name,
                getattr(self, field_name, None)
            )
            if field_val is not None:
                d[field_name] = field_val

        return d

    @classmethod
    async def install(cls):
        """install the Orm's table using the Orm's schema"""
        return await cls.interface.set_table(cls.schema)

