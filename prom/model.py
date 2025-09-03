# -*- coding: utf-8 -*-
from contextlib import asynccontextmanager
import inspect

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
    Field,
    Index,
)


class Orms(OrderedSubclasses):
    """Holds all the Orms loaded into memory

    See Orm.__init_subclass__, this is a class attribute found Orm.orm_classes.

    This class is a hybrid Mapping and Sequence, if you iterate through it will
    iterate the values like a list, if you use item indexes then it will act
    like a dict
    """
    def default_cutoff(self):
        return (Orm,)

    def __init__(self):
        super().__init__(
            insert_cutoff_classes=False
        )

        self.prepare()

        # should always be `prom` and is used in ._is_valid_subclass to make
        # sure only true child classes are inserted
        self.module_name = ReflectModule(__name__).modroot

    def prepare(self):
        """These initializations are broken out from __init__ because .clear
        will also need to use them

        NOTE -- This completely resets the state of the class but that means
        it won't reload any classes previously added so even though 
        .insert_modules will rerun, it might not actually load any classes
        """

        # set to True in .insert_modules
        self.inserted_modules = False

        # holds any loaded model prefixes
        self.model_prefixes = set()

        # model(s)_name is the key, an Orm class child is the value
        self.lookup_orm_table = {}

        # refs lookup table (a ref is a foreign key present on an orm
        self.lookup_ref_table = {}

        # dependency lookup table (a dependency is all the orms that ref
        # an orm
        self.lookup_dep_table = {}

        # lookup table for "lookup table" orms
        self.lookup_rel_table = {}

    def clear(self):
        super().clear()
        self.prepare()

    def _insert(self, orm_class, class_info):
        super()._insert(orm_class, class_info)

        if not class_info["in_info"]:
            # this orm class is the new edge for this model name
            self.lookup_orm_table[orm_class.model_name] = orm_class
            self.lookup_orm_table[orm_class.models_name] = orm_class

        # we reset the dependency tables because we've potentially added new
        # dependencies
        self.lookup_dep_table = {}
        self.lookup_rel_table = {}

    def __getitem__(self, index_or_name_or_class):
        """If int then treat it like getting the index of a list, if str then
        treat it like fetching a key on a dictionary

        :param index_or_name_or_class: int|str|Orm, either the index of the
            list you want or the model name you want
        :returns: type, the requested Orm child class
        """
        if isinstance(index_or_name_or_class, int):
            return super().__getitem__(index_or_name_or_class)

        else:
            if isinstance(index_or_name_or_class, str):
                model_name = index_or_name_or_class

            else:
                model_name = index_or_name_or_class.model_name

            if model_name not in self.lookup_orm_table:
                self.insert_modules()

            return self.lookup_orm_table[model_name]

    def __getattr__(self, model_name):
        try:
            return self.__getitem__(model_name)

        except KeyError as e:
            raise AttributeError(model_name) from e

    def __contains__(self, name_or_class):
        """If str then it checks model_name keys as a dict, if type then it
        will check for the class in the list

        :param name_or_class: str|type
        :returns: bool
        """
        if isinstance(name_or_class, str):
            return name_or_class in self.lookup_orm_table

        else:
            return super().__contains__(name_or_class)

    def insert_modules(self, modpaths=None):
        """Goes through the PROM_PREFIX evnironment variables and loads any
        found module classpaths and loads all the Orm classes found in those
        modules

        :param modpaths: Sequence[str], a list of modpaths (eg ["foo.bar",
            "che"])
        """
        if modpaths:
            for modath in modpaths:
                self.model_prefixes.add(modpath)
                super().insert_modules(modpath)

        else:
            if not self.inserted_modules:
                environ = Environ("PROM_")
                for modpath in environ.paths("PREFIX"):
                    self.model_prefixes.add(modpath)
                    super().insert_modules(modpath)

                # if there aren't any defined prefixes let's inspect the
                # current working directory
                if not self.model_prefixes:
                    rp = ReflectPath(Dirpath.cwd())
                    for mod in rp.find_modules("models"):
                        self.model_prefixes.add(mod.__name__)

                self.inserted_modules = True

    def get(self, model_name, default=None):
        """Returns the Orm class found at model_name

        :param model_name: str, the model name you want
        :param default: Any, only here for full compatibility with dict.get
        :returns: type, Orm child class
        """
        try:
            return self.__getitem__(model_name)

        except KeyError:
            return default

    def get_subclass(
        self,
        child_name_or_class,
        parent_name_or_class,
        default=None
    ):
        """Returns the orm_class of child_name_or_class only if it is a
        child of parent_name_or_class

        :param child_name_or_class: str|Orm, the child class we're looking for
        :param parent_name_or_class: str|Orm, the parent class which child
            must extend
        :param default: Any
        :returns: Orm, the class if it is found
        """
        child_orm_class = self.get(child_name_or_class)
        parent_orm_class = self.get(parent_name_or_class)

        if child_orm_class is not None:
            if not issubclass(child_orm_class, parent_orm_class):
                return default

            return child_orm_class

    def get_ref_classes(self, name_or_class):
        """Get reference classes for the given orm class

        A reference class has a foreign key reference on the given orm class

        A reference class is a class referenced in `name_or_class` fields

        :Example:
            class Foo(Orm):
                pass

            class Bar(Orm):
                foo_id = Field(Foo, True)

            Orm.orm_classes.get_ref_classes("bar") # [Foo]
            Orm.orm_classes.get_ref_classes("foo") # []

        :param name_or_class: str|Orm
        :returns: list[Orm]
        """
        orm_class = self[name_or_class]
        model_name = orm_class.model_name

        if model_name not in self.lookup_ref_table:
            ref_classes = list(
                f.ref_class for f in orm_class.schema.ref_fields.values()
            )
            self.lookup_ref_table[model_name] = ref_classes

        return self.lookup_ref_table[model_name]

    def get_dep_classes(self, name_or_class):
        """Get dependency classes for the given orm

        A dependency class is an orm class that contains a foreign key
        reference for the given orm class (ie, it is dependent on the given
        orm class but the given orm class doesn't have a reference to the
        dependent class)

        A dependency class is a class that references `name_or_class` in
        its fields

        :Example:
            class Foo(Orm):
                pass

            class Bar(Orm):
                foo_id = Field(Foo, True)

            Orm.orm_classes.get_dep_classes("foo") # [Bar]
            Orm.orm_classes.get_dep_classes("bar") # []

        :param name_or_class: str|Orm
        :returns: list[Orm]
        """
        orm_class = self[name_or_class]
        model_name = orm_class.model_name

        if model_name not in self.lookup_dep_table:
            dep_classes = []
            for dep_class in self:
                for ref_class in self.get_ref_classes(dep_class.model_name):
                    if ref_class.model_name == model_name:
                        dep_classes.append(dep_class)

            self.lookup_dep_table[model_name] = dep_classes

        return self.lookup_dep_table[model_name]

    def get_rel_classes(self, name_or_class_1, name_or_class_2):
        """Get lookup table classes for the given orm classes

        a relationship class is an orm class that contains foreign key
        references to both passed in orm classes (ie, the returned orm
        classes are basically lookup tables for the relationship between
        the two passed in orm classes)

        A relatonship class is a class that references both `name_or_class_1`
        and `name_or_class_2` in its fields

        :Example:
            class Foo(Orm):
                pass

            class Bar(Orm):
                pass

            class FooBar(Orm):
                foo_id = Field(Foo, True)
                bar_id = Field(Bar, True)

            Orm.orm_classes.get_rel_classes("foo", "bar") # [FooBar]
            Orm.orm_classes.get_rel_classes("bar", "foo") # [FooBar]

        :param name_or_class_1: str|Orm
        :param name_or_class_2: str|Orm
        :returns: list[Orm]
        """
        orm_class_1 = self[name_or_class_1]
        model_name_1 = orm_class_1.model_name

        orm_class_2 = self[name_or_class_2]
        model_name_2 = orm_class_2.model_name

        key_name = f"{model_name_1}-{model_name_2}"

        if key_name not in self.lookup_rel_table:
            rel_classes = []

            dep_classes_1 = {
                oc.model_name: oc for oc in self.get_dep_classes(model_name_1)
            }

            for oc in self.get_dep_classes(model_name_2):
                if oc.model_name in dep_classes_1:
                    rel_classes.append(oc)

            self.lookup_rel_table[key_name] = rel_classes
            key_name_2 = f"{model_name_2}-{model_name_1}"
            self.lookup_rel_table[key_name_2] = rel_classes

        return self.lookup_rel_table[key_name]

#     def _is_valid_subclass(self, orm_class, cutoff_classes):
#         ret = super()._is_valid_subclass(orm_class, cutoff_classes)
#         if ret:
#             # while we check for Orm derived child classes, we also don't want
#             # any Orm child classes that are defined in prom since those are
#             # also base classes and we're only interested in valid child
#             # classes that can access a db
#             ret = not orm_class.__module__.startswith(self.module_name)
# 
#         return ret


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

    schema_class = Schema
    """The class the orm will use for its schema

    This is created in .create_schema and cached/returned in .schema
    """

    orm_classes = Orms()
    """This will hold all other orm classes that have been loaded into memory
    the class path is the key and the class object is the value"""

    _id = AutoIncrement(aliases=["id"])
    """The primary key is an auto-increment integer by default

    You can override this in your child class if you want a different primary
    key, I wouldn't change the name though. The _id name comes from MongoDB
    """

    _created = AutoDatetime(
        created=True,
        updated=False,
        aliases=["created"],
        jsonable_field=False, # don't include in .jsonable by default
    )
    """Anytime a new row is created this will be populated

    If you don't want this functionality just do `_created = None` in your
    child class
    """

    _updated = AutoDatetime(
        created=False,
        updated=True,
        aliases=["updated"],
        jsonable_field=False, # don't include in .jsonable by default
    )
    """Anytime a row is written to this will be updated, that means it will
    have roughly the same value as ._created when the row is first inserted,
    but will have later values as the row is updated

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
        s = cls.create_schema()
        cls.schema = s # cache the schema so we don't need to create it again
        return s

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
    @asynccontextmanager
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
        kwargs.setdefault("prefix", f"{cls.__name__}_{cls.connection_name}_tx")

        async with cls.interface.transaction(**kwargs) as conn:
            yield conn

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
        Iterator.hydrate relies on this method signature to create each
        instance

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
    def make_dict(cls, *fields, schema=None):
        """Lots of methods take a dict and key=val for fields, this combines
        fields and fields_kwargs into one master dict, turns out we want to do
        this more than I would've thought to keep api compatibility with prom
        proper

        :param *fields: dict, usually a fields dict passed in directly and the
            second index are the kwargs passed in
        :schema: Schema, if passed in then this will normalize field names and
            resolve any aliases
        :returns: dict, the combined fields
        """
        fields = utils.make_dict(*fields)

        if schema:
            # since schema is passed in resolve any aliases
            for field_name in list(fields.keys()):
                if fn := schema.field_name(field_name, ""):
                    fields[fn] = fields.pop(field_name)

        return fields

    @classmethod
    def create_schema(cls):
        """Create the schema instance for this class

        This is the method you will want to override to customize fields in
        parent classes, this is only called once and then cached in the
        instance's .schema property

        NOTE -- This is a class method and works on the class schema, that
        means if you set a value (like in a Field's options) then it will be
        set for all classes that use this schema and if the value is dynamic
        in some way you might end up with unexpected results

        :returns: Schema
        """
        table_name = cls.table_name
        s = cls.schema_class(table_name, cls)

        seen_properties = {}
        for klass in inspect.getmro(cls)[:-1]:
            for k, v in vars(klass).items():
                field = None

                if isinstance(v, Field):
                    field = v

                elif isinstance(v, Index):
                    s.set_index(k, v)
                    seen_properties[k] = v

                elif isinstance(v, type) and issubclass(v, Field):
                    # We've defined a Field class inline of the Orm, so we
                    # want to instantiate it and set it in all the places
                    #field = v.get_instance()
                    field = v(v.type, v.required, v.options)
                    field.__set_name__(cls, k)
                    setattr(cls, k, field)

                else:
                    if v is None:
                        seen_properties[k] = v

                if field:
                    if k in seen_properties:
                        if seen_properties[k] is None:
                            for fn in field.names:
                                s.lookup["field_names_deleted"][fn] = k

                    else:
                        s.set_field(k, field)
                        seen_properties[k] = field

        return s

    @classmethod
    async def install(cls):
        """install the Orm's table using the Orm's schema"""
        return await cls.interface.set_table(cls.schema)

    def __init__(self, fields=None, **fields_kwargs):
        """Create an Orm instance

        While you can override this method to customize the signature, you
        might also need to override .hydrate (but don't change .hydrate's
        signature) since .hydrate creates an instance using no arguments

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
        cls.orm_classes.insert(cls)
        super().__init_subclass__()

    def fk(self, orm_class):
        """find the field value in self that is the primary key of the passed
        in orm_class

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
        """Alias for .ref to be more consistent with other *_class
        attributes"""
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
        q = self.query.set(self.to_interface())

        if fields := await q.insert(**kwargs):
            self.from_interface(self.make_dict(fields))

        else:
            ret = False

        return ret

    async def update(self, **kwargs):
        """re-persist the updated field values of this orm that has a primary
        key"""
        ret = True
        q = self.query.set(self.to_interface())

        if pk := self._interface_pk:
            q.eq_field(self.schema.pk.name, pk)

        else:
            raise ValueError("Cannot update an unhydrated orm instance")

        if rows := await q.update(**kwargs):
            self.from_interface(self.make_dict(rows[0]))

        else:
            ret = False

        return ret

    async def upsert(self, **kwargs):
        """Perform an UPSERT query where we insert the fields if they don't
        already exist on the db or we UPDATE if they do

        We only want to upsert on specific occasions where we know we've set
        the conflict values and will be sending them to the db. UPSERT queries
        need to have a unique index on the table they can use for the conflict
        fields

        This method will go through the indexes and try and find a unique index
        that has all fields that are being sent to the interface and it will
        use those fields as the conflict fields, it will raise a ValueError if
        it can't find a valid set of conflict fields

        :param **kwargs: passed through to the interface
        """
        ret = True

        if pk := self._interface_pk:
            ret = await self.update(**kwargs)

        else:
            fields = self.to_interface()
            q = self.query.set(fields)
            schema = self.schema

            conflict_fields = self.conflict_fields(fields)
            if not conflict_fields:
                raise ValueError(
                    "Failed to find conflict field names from: {}".format(
                        q.fields_set.names()
                    )
                )

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

        :returns: bool, True if the save was successful
        """
        if pk := self._interface_pk:
            return await self.update(**kwargs)

        else:
            return await self.insert(**kwargs)

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
        customizations on the fields, so if you want to set defaults or
        anything you should do that here

        :param fields: dict, the fields you might want to be modified
        :returns: dict, the fields you want to actually be modified
        """
        return fields

    def get_ref_value(self, k):
        """Internal method called in .__getattr__. If k is a model_name for a
        reference class this will return the actual orm instance for the value
        found in the field in self

        Go through looking for a FK's model name, if a match is found
        then load that FK's row using this orm's field value and return
        an orm instance where this row's orm field value is the primary
        key

        :Example:
            f = await Foo.query.one()

            f.bar_id # 100

            b = await f.bar
            b.pk # 100

        :param k: str, the model name of foreign key field on self
        :returns: coroutine[Orm]
        """
        for ref_field_name, ref_field in self.schema.ref_fields.items():
            ref_class = ref_field.ref_class
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

        raise AttributeError(f"No reference for {k}")

    def get_dep_value(self, k):
        """Internal method called in .__getattr__. If k represents an Orm
        instance that a foreign key reference to self with the value .pk

        Go through all the orm_classes looking for a model_name or
        models_name match and query that model using that model's FK field
        name that matches self.pk

        :Example:
            f = await Foo.query.one()
            f.pk # 100

            b = await Bar.query.eq_foo_id(f.pk).one()
            b.foo_id # 100

            b2 = await f.bar
            b2.foo_id # 100

            async for b in await f.bars:
                b.foo_id # 100

        :param k: str, the model(s) name of an orm class that contains a
            foreign key field for self and has values of .pk
        :returns: coroutine[Orm]|coroutine[Iterator]
        """
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

        raise AttributeError(f"No dependency for {k}")

    def get_rel_value(self, k):
        """Internal method called in .__getattr__. This one is a bit harder
        to understand because self doesn't have a direct relationship to k,
        but if there is a third orm_class that has a relationship to both
        self and k then this will use that to lookup values matching .pk

        :param k: str, the model(s) name of an orm class that might have a
            relationship with self
        :returns: coroutine[Orm]|coroutine[Iterator]
        """
        model_name_1 = self.model_name
        for dep_class in self.orm_classes.get_rel_classes(model_name_1, k):
            field_name_1 = dep_class.schema.field_model_name(model_name_1)
            orm_class_1 = dep_class.schema.fields[field_name_1].ref_class

            field_name_2 = dep_class.schema.field_model_name(k)
            orm_class_2 = dep_class.schema.fields[field_name_2].ref_class

            query = orm_class_2.query.in_pk(
                dep_class.query.select(field_name_2).eq_field(
                    field_name_1,
                    self.pk
                )
            )

            if k == orm_class_2.models_name:
                # this is a coroutine
                return query.get()

            else:
                # this is a coroutine
                return query.one()

        raise AttributeError(f"No relationship for {model_name_1} and {k}")

    def get_method_value(self, k):
        """Adds some magic methods to check the value of field

        Adds support for:
            * .is_fieldname() = if fieldname is a boolean then returns
                True/False, if fieldname is another value then you can do
                .is_fieldname(val) to compare val to the fieldname's value

        The magic method is of the form `<COMPARE>_<FIELD_NAME>`, so if you
        had a field `foo`, you could do:

            * eq_foo(<VALUE>) - <VALUE> == foo's value
            * ne_foo(<VALUE>) - <VALUE> != foo's value
            * lt_foo(<VALUE>) - <VALUE> < foo's value
            * lte_foo(<VALUE>) - <VALUE> <= foo's value
            * gt_foo(<VALUE>) - <VALUE> > foo's value
            * gte_foo(<VALUE>) - <VALUE> >= foo's value
            * in_foo(<VALUE>) - <VALUE> in foo's value
            * nin_foo(<VALUE>) - <VALUE> not in foo's value
            * is_foo() - only works if foo's type is bool, checks if foo's
                value is True
            * is_foo(<VALUE>) - equivalent to eq_foo(<VALUE>)

        These roughly match the equivalent magic methods in Query

        :returns: callable -> bool, this returns a callable that returns
            a boolean
        """
        ret = None

        prefix = k[:3]
        field_name = k[3:]
        if field := self.schema.fields.get(field_name, None):
            if prefix.startswith("is_"):
                if issubclass(field.type, bool):
                    ret = lambda: getattr(self, field_name)

                else:
                    ret = lambda x: x == getattr(self, field_name)

            elif prefix.startswith("eq_"):
                ret = lambda x: x == getattr(self, field_name)

            elif prefix.startswith("ne_"):
                ret = lambda x: x != getattr(self, field_name)

            elif prefix.startswith("lt_"):
                ret = lambda x: x < getattr(self, field_name)

            elif prefix.startswith("gt_"):
                ret = lambda x: x > getattr(self, field_name)

            elif prefix.startswith("in_"):
                ret = lambda x: x in getattr(self, field_name)

        if not ret:
            prefix = k[:4]
            field_name = k[4:]
            if field := self.schema.fields.get(field_name, None):
                if prefix.startswith("nin_"):
                    ret = lambda x: x not in getattr(self, field_name)

                elif prefix.startswith("lte_"):
                    ret = lambda x: x <= getattr(self, field_name)

                elif prefix.startswith("gte_"):
                    ret = lambda x: x >= getattr(self, field_name)

        if not ret:
            raise AttributeError(f"No method name for {k}")

        return ret

    def __getattr__(self, k):
        """
        NOTE -- this is a hybrid method, sometimes it will return coroutines
        """
        ret = None
        try:
            field_name = self.schema.field_name(k)

        except AttributeError:
            try:
                return self.get_method_value(k)

            except AttributeError:
                try:
                    return self.get_ref_value(k)

                except AttributeError:
                    try:
                        return self.get_dep_value(k)

                    except AttributeError:
                        try:
                            return self.get_rel_value(k)

                        except (AttributeError, KeyError):
                            pass

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
        is a user object, then you might want to call it user_id instead of
        _id) and I didn't want to make assumptions

        note 2, I'm not crazy about the name, but I didn't like to_dict() and
        pretty much any time I need to convert the object to a dict is for
        json, I kind of like dictify() though, but I've already used this
        method in so many places. Another name I don't mind is .tojson
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

