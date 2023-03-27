# -*- coding: utf-8 -*-
import datetime
import functools
import logging

from testdata.base import TestData
from datatypes import (
    OrderedSubclasses,
    ReflectModule,
)

from ..model import Orm
from ..interface import get_interfaces
from ..exception import UniqueError


logger = logging.getLogger(__name__)


class ModelData(TestData):
    """Provides testdata hooks for the projects models, testdata is a python module
    for generating random data for testing

    https://github.com/Jaymon/testdata

    This uses a lot of magic and I'm not sure the best way to describe it, basically,
    this will create get_*, create_*, and get_*_fields methods for any Orm subclass
    you have loaded into memory. The * will correspond to Orm.model_name and 
    Orm.models_name.

    Because it only creates those methods for Orm classes loaded
    into memory you need to import the modules for the classes you want to test

    Maybe the best way to understand what this does is by example.

    :Example:
        from prom import Orm

        class Foobar(Orm):
            pass

        import testdata
        from prom.extras.testdata import ModelData

        o = testdata.get_foobar()
        print(o.pk) # None

        o = testdata.create_foobar()
        print(o.pk) # pk will be set because .save() was called

        fields = testdata.get_foobar_fields()
        print(fields) # dict that can be used create a Foobar(fields) instance

        os = testdata.get_foobars(foobar_count=2) # will return a list of 2 Foobar instances

        os = testdata.create_foobars(foobar_count=2) # will return a list of 2 created Foobar instances


    To customize a method, you can extend this class and define the method. The
    signature for any overridden methods is (self, orm_class, **kwargs),
    and you will usually just want to override the get_<MODELS-NAME>_fields class
    since that is the one the get_* and create_* methods use to populate the orms

    All the signatures of all the methods need to be the same because they are
    mixed and matched, so if you have a `Foobar` Orm class and override the
    get_foobar_fields testdata method, then get_foobar() would call get_foobar_fields
    and get_orm(Foobar, **kwargs) would also call get_foobar_fields. So all of these
    methods can be mixed and matched

    :Example:
        from prom.extras.testdata import ModelData

        class MyModelData(ModelData):
            def get_foobar_fields(self, orm_class, **kwargs):
                return super().get_orm_fields(orm_class, **kwargs)

    If you'd like to customize the testdata method names, you can set the Orm.model_name
    or the Orm.models_name class properties

    :Example:
        from prom import Orm

        class Foobar(Orm):
            model_name = "foo_bar"
            models_name = "foo_bars"
    """
    method_cache = {}

    model_cache = {}

    def _orm_classes(self):
        """Iterate through a list of orms that should be injected into testdata

        by default, we want to ignore any orm classes that are defined in this
        library since they are by definition base classes

        :returns: generator, each orm class
        """
        orm_classes = OrderedSubclasses(classes=Orm.orm_classes.values())
        module_name = ReflectModule(__name__).modroot

        for orm_class in orm_classes.edges():
            if module_name not in orm_class.__module__:
                yield orm_class

    def _find_method(self, method_name, default_method=None, **kwargs):
        """Find the method, this will return the user defined method or the default
        method if no user defined method exists

        :param method_name: str, the method name
        :param default_method: callable, the fallback method
        :param **kwargs:
        :returns: callable
        """
        method = getattr(self, method_name, None)
        if method:
            return method_name, method

        else:
            return method_name, default_method

    def _get_method(self, orm_class, **kwargs):
        """Return the .get_orm() type method for orm_class"""
        method_name = f"get_{orm_class.model_name}"
        kwargs.setdefault("default_method", self.get_orm)
        return self._find_method(method_name, **kwargs)

    def _get(self, orm_class, **kwargs):
        """Internal dispatcher method for get_orm, this will first try and find
        a get_<ORM-NAME> method and fallback to get_orm"""
        method_name, method = self._get_method(orm_class)
        kwargs.setdefault("orm_class", orm_class)
        logger.debug(f"Running {method_name} for orm_class {orm_class.__name__}")
        return method(**kwargs)

    def _gets_method(self, orm_class, **kwargs):
        """Return the .get_orms() type method for orm_class"""
        method_name = f"get_{orm_class.models_name}"
        kwargs.setdefault("default_method", self.get_orms)
        return self._find_method(method_name, **kwargs)

    def _gets(self, orm_class, **kwargs):
        """Internal dispatcher method for get_orms, this will first try and find
        a get_<ORM-MODELS-NAME> method and fallback to get_orms"""
        method_name, method = self._gets_method(orm_class)
        kwargs.setdefault("orm_class", orm_class)
        logger.debug(f"Running {method_name} for orm_class {orm_class.__name__}")
        return method(**kwargs)

    def _create_method(self, orm_class, **kwargs):
        """Return the .create_orm() type method for orm_class"""
        method_name = f"create_{orm_class.model_name}"
        kwargs.setdefault("default_method", self.create_orm)
        return self._find_method(method_name, **kwargs)

    def _create(self, orm_class, **kwargs):
        """Internal dispatcher method for create_orm, this will first try and find
        a create_<ORM-NAME> method and fallback to create_orm"""
        method_name, method = self._create_method(orm_class)
        kwargs.setdefault("orm_class", orm_class)
        logger.debug(f"Running {method_name} for orm_class {orm_class.__name__}")
        return method(**kwargs)

    def _creates_method(self, orm_class, **kwargs):
        """Return the .create_orms() type method for orm_class"""
        method_name = f"create_{orm_class.models_name}"
        kwargs.setdefault("default_method", self.create_orms)
        return self._find_method(method_name, **kwargs)

    def _creates(self, orm_class, **kwargs):
        """Internal dispatcher method for create_orms, this will first try and find
        a create_<ORM-MODELS-NAME> method and fallback to create_orm"""
        method_name, method = self._creates_method(orm_class)
        kwargs.setdefault("orm_class", orm_class)
        logger.debug(f"Running {method_name} for orm_class {orm_class.__name__}")
        return method(**kwargs)

    def _fields_method(self, orm_class, **kwargs):
        """Return the .get_orm_fields() type method for orm_class"""
        method_name = f"get_{orm_class.model_name}_fields"
        kwargs.setdefault("default_method", self.get_orm_fields)
        return self._find_method(method_name, **kwargs)

    def _fields(self, orm_class, **kwargs):
        """Internal dispatcher method for get_orm_fields, this will first try and find
        a get_<ORM-NAME>_fields method and fallback to get_orm_fields"""
        method_name, method = self._fields_method(orm_class)
        kwargs.setdefault("orm_class", orm_class)
        logger.debug(f"Running {method_name} for orm_class {orm_class.__name__}")
        return method(**kwargs)

    def _find_attr(self, method_name):

        method = None

        if method_name in self.method_cache:
            method = self.method_cache[method_name]

        else:
            try:
                # check for <NAME>_<MODEL_NAME>
                name, model_name = method_name.split("_", 1)

                if model_name.endswith("_fields"):
                    name = "fields"
                    model_name, _ = model_name.rsplit("_", 1)

            except ValueError:
                raise AttributeError("invalid potential method: {}".format(method_name))

            else:
                try:
                    orm_class = self.get_orm_class(model_name)

                except ValueError as e:
                    raise AttributeError() from e

                else:
                    orm_method = None
                    if name == "get":
                        if orm_class.model_name == model_name:
                            orm_method = self.get_orm

                        else:
                            orm_method = self.get_orms

                    elif name == "create":
                        if orm_class.model_name == model_name:
                            orm_method = self.create_orm

                        else:
                            orm_method = self.create_orms

                    elif name == "fields":
                        orm_method = self.get_orm_fields

                    if orm_method:
                        method = functools.partial(orm_method, orm_class=orm_class)
                        self.method_cache[method_name] = method

        if not method:
            raise AttributeError(f"Could not find an orm matching {method_name}")

        return method

    def __getattr__(self, method_name):
        try:
            return self._find_attr(method_name)

        except AttributeError:
            return super().__getattr__(method_name)

    def unsafe_delete_db(self):
        """This will delete all the tables from the db

        It relies on a method .ensure_safe_env being defined in the project code
        """
        if self.ensure_safe_env(): # this method needs to be defined in project code
            for inter in get_interfaces().values():
                inter.unsafe_delete_tables()

    def assure_orm_field_names(self, orm_class, **kwargs):
        """Field instances can have aliases, in order to allow you to pass in aliases,
        this will go through kwargs and normalize the field names

        :Example:
            class Foobar(Orm):
                che = Field(str, aliases=["baz"]

            kwargs = testdata.assure_orm_field_names(Foobar, {"baz": "1"})
            print(kwargs["che"]) # "1"

        :param orm_class: Orm
        :param **kwargs: the fields where keys will be normalized to field names
            in orm_class.schema
        :returns: dict, the normalized kwargs
        """
        schema = orm_class.schema
        # normalize passed in field names to make sure we correctly find the field's
        # value if it exists
        for field_name in list(kwargs.keys()):
            if schema.has_field(field_name):
                kwargs.setdefault(schema.field_name(field_name), kwargs.pop(field_name))

        return kwargs

    def assure_ref_field_names(self, orm_class, ref_class, **kwargs):
        """Make sure the kwargs destined for orm_class don't impact the kwargs
        that will be used to create a ref_class instance

        Sometimes, we have multiple Orm classes that have the same field names and
        when those fields get set for orm_class they can cause ref_class to fail
        so we want to strip those common field names out

        :param orm_class: Orm, the main orm class
        :param ref_class: Orm, the Orm that orm_class references in some way
        :param **kwargs: the fields that were passed in for orm_class, if these
            fields are also in ref_class they will be stripped
        :returns: dict, the kwargs suitable to be used to create an instance of
            ref_class
        """
        ref_kwargs = {}
        orm_fields = orm_class.schema.fields
        ref_fields = ref_class.schema.fields
        for field_name, field_value in kwargs.items():
            if (field_name not in orm_fields) or (field_name not in ref_fields):
                ref_kwargs[field_name] = field_value

        return ref_kwargs

    def assure_orm_refs(self, orm_class, assure_orm_class=None, **kwargs):
        """When creating an orm, they will often need foreign key values, this will
        go through any of the foreign key ref fields and create a foreign key if
        it wasn't included.

        This is a recursive method, when it finds a ref_class it will assure that
        ref's foreign keys before handling ref, this way all references all the
        way down the stack can be generated for the top level so the same refs
        can propogate all the way down the dependency chain

        :param orm_class: Orm
        :param assure_orm_class: Orm|None, the orm that is getting its references
            assured. If this is None then orm_class is considered the orm that is
            being checked
        :param **kwargs: orm_class's actual field name value will be checked and
            the ref's orm_class.model_name will be checked
            * ignore_refs: bool, default False, if True then refs will not be checked,
                passed in refs will still be set
            * require_fields: bool, default True, if True then create missing refs,
                if False, then refs won't be created and so if they are missing
                their fields will not be populated
        :returns: dict, the kwargs with ref field_name and ref orm_class.model_name
            will be included
        """
        logger.debug(f"Assuring orm refs for orm_class {orm_class.__name__}")
        kwargs = self.assure_orm_field_names(orm_class, **kwargs)

        # if assure class isn't passed in then we assume the passed in orm_class
        # is the class to be assured and all recursive calls will now have it
        # set
        if assure_orm_class is None:
            assure_orm_class = orm_class

        ignore_refs = kwargs.get("ignore_refs", False)
        require_fields = kwargs.get("require_fields", True)
        for field_name, field in orm_class.schema.fields.items():
            if ref_class := field.ref:
                ref_field_name = ref_class.model_name

                if field_name in kwargs:
                    if ref_field_name not in kwargs:
                        kwargs[ref_field_name] = ref_class.query.eq_pk(
                            kwargs[field_name]
                        ).one()

                else:
                    if ref_field_name in kwargs:
                        kwargs[field_name] = kwargs[ref_field_name].pk

                    else:
                        if not ignore_refs:
                            if require_fields or field.is_required() or self.yes():
                                # handle all ref_class's refs before we handle
                                # ref_class
                                kwargs.update(self.assure_orm_refs(
                                    ref_class,
                                    assure_orm_class,
                                    **kwargs
                                ))

                                kwargs[ref_field_name] = self._create(
                                    ref_class,
                                    **self.assure_ref_field_names(
                                        assure_orm_class,
                                        ref_class,
                                        **kwargs
                                    )
                                )
                                kwargs[field_name] = kwargs[ref_field_name].pk

        return kwargs

    def create_orm(self, orm_class, **kwargs):
        """create an instance of the orm and save it into the db

        :param orm_class: Orm
        :param **kwargs:
        :returns: Orm, the orm saved into the db
        """
        kwargs.setdefault("ignore_refs", False)
        instance = self._get(orm_class, **kwargs)
        try:
            instance.save()

        except UniqueError as e:
            logger.warning(" ".join([
                f"Creating {orm_class.__name__} failed because it exists.",
                "Fetching the existing instance without updating it",
            ]))
            instance = instance.requery()

        return instance

    def create_orms(self, orm_class, **kwargs):
        """create instances of the orm and save it into the db

        :param orm_class: Orm
        :param **kwargs:
            count: int, how many instances you want
            <MODEL_NAME>_count: int, alias for count
        :returns: list, a list of Orm instances that have all been saved into the db
        """
        kwargs.setdefault("ignore_refs", False)
        instances = self._gets(orm_class, **kwargs)
        for instance in instances:
            instance.save()
        return instances

    def get_orm_class(self, model_name, **kwargs):
        """get the orm class found at model_name

        :param model_name: str, the name of the orm class
        :returns: Orm, the orm_class.model_name that matches model_name
        """
        if model_name in self.model_cache:
            return self.model_cache[model_name]

        elif model_name in Orm.orm_classes:
            orm_class = Orm.orm_classes[model_name]
            self.model_cache[model_name] = orm_class
            return orm_class

        else:
            for oc in self._orm_classes():
                if model_name in set([oc.model_name, oc.models_name]):
                    orm_class = oc
                    self.model_cache[model_name] = orm_class
                    return orm_class

            for orm_classpath, orm_class in Orm.orm_classes.items():
                if orm_classpath.endswith(f":{model_name}"):
                    self.model_cache[model_name] = orm_class
                    return orm_class

        raise ValueError(f"could not find an orm_class for {model_name}")

    def get_orm(self, orm_class, **kwargs):
        """get an instance of the orm but don't save it into the db

        :param orm_class: Orm
        :param **kwargs:
        :returns: Orm
        """
        kwargs.setdefault("ignore_refs", True)
        instance = kwargs.get(orm_class.model_name, None)
        if not instance:
            fields = self._fields(orm_class, **kwargs)
            instance = orm_class(fields)

        return instance

    def get_orms(self, orm_class, **kwargs):
        """get instances of the orm

        :param orm_class: Orm
        :param **kwargs:
            * count: int, how many instances you want
            * <MODEL_NAME>_count: int, alias for count
            * related_refs: bool, default True, all the orms will have the same
                foreign key references
        :returns: list, a list of Orm instances
        """
        ret = []
        orm_field_name = orm_class.model_name
        if kwargs.get("related_refs", True):
            # because we need related refs, we will need to create refs if they
            # don't exist
            kwargs.setdefault("ignore_refs", False)
            kwargs = self.assure_orm_refs(orm_class, **kwargs)

        count = kwargs.get(f"{orm_field_name}_count", kwargs.get("count", 1))
        for _ in range(count):
            ret.append(self._get(orm_class, **kwargs))

        return ret

    def get_orm_fields(self, orm_class, **kwargs):
        """Get the fields of an orm_class

        :param orm_class: Orm
        :param **kwargs: the fields found in orm_class.schema
            * require_fields: bool, default True, this will require that all fields
                have values even if they aren't required, this does not apply to
                foreign key references
            * field_callbacks: dict, the key is the field name and the value is
                a callable that can take self
        :returns: dict
        """
        ret = {}
        schema = orm_class.schema
        kwargs.setdefault("ignore_refs", True)
        kwargs = self.assure_orm_refs(orm_class, **kwargs)
        require_fields = kwargs.get("require_fields", True)
        field_callbacks = kwargs.get("field_callbacks", {})

        for field_name, field in schema.fields.items():
            if field_name in kwargs:
                # this value was passed in so we don't need to do anything
                ret[field_name] = kwargs[field_name]

            elif field.is_auto():
                # db will handle any auto-generating fields
                pass

            elif field.is_pk():
                # primary key isn't auto-generating and wasn't passed in, so
                # we'll cross our fingers and hope it will be taken care of
                # somewhere else
                pass

            elif field.is_ref():
                # foreign keys are handled in .assure_orm_refs
                pass

            else:
                if require_fields or field.is_required() or self.yes():

                    field_type = field.interface_type

                    if field.choices:
                        ret[field_name] = self.choice(field.choices)

                    else:
                        if cb := getattr(field, "testdata", field_callbacks.get(field_name, None)):
                            ret[field_name] = cb(self)

                        else:
                            if issubclass(field_type, bool):
                                ret[field_name] = bool(self.yes())

                            elif issubclass(field_type, int):
                                size_info = field.size_info()
                                ret[field_name] = self.get_posint(size_info["size"])

                            elif issubclass(field_type, str):
                                size_info = field.size_info()
                                if "bounds" in size_info:
                                    ret[field_name] = self.get_words(
                                        min_size=size_info["bounds"][0],
                                        max_size=size_info["bounds"][1],
                                    )

                                else:
                                    ret[field_name] = self.get_words()

                            elif issubclass(field_type, dict):
                                ret[field_name] = self.get_dict()

                            elif issubclass(field_type, float):
                                size_info = field.size_info()
                                ret[field_name] = self.get_posfloat(size_info["size"])

                            elif issubclass(field_type, datetime.datetime):
                                ret[field_name] = self.get_past_datetime()

                            elif issubclass(field_type, datetime.date):
                                ret[field_name] = self.get_past_date()

                            else:
                                raise ValueError(f"Not sure what to do with {field.type}")

        return ret

