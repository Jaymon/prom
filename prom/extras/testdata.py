# -*- coding: utf-8 -*-
import datetime
import functools
import logging
import uuid
import re

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
    """Provides testdata hooks for the projects models, testdata is a python
    module for generating random data for testing

    https://github.com/Jaymon/testdata

    In order for this to be used, it needs to be loaded into memory so testdata
    can discover it:

        from prom.extras.testdata import ModelData

    I usually import this in a common test module that gets imported by all the
    actual tests so this is always loaded.

    This uses a lot of magic and I'm not sure the best way to describe it,
    basically, this will create get_*, create_*, and get_*_fields methods for
    any Orm subclass you have loaded into memory. The * will correspond to
    Orm.model_name and Orm.models_name (get_*_fields only ever corresponds to
    Orm.model_name).

    NOTE -- Because this class only creates those methods for Orm classes
    loaded into memory you need to import the modules for the classes you want
    to test

    Maybe the best way to understand what this does is by example.

    :Example:
        from prom import Orm

        class Foobar(Orm):
            pass

        import testdata
        from prom.extras.testdata import ModelData

        o = await testdata.get_foobar()
        print(o.pk) # None because .save was not called

        o = await testdata.create_foobar()
        print(o.pk) # pk will be set because .save() was called

        fields = await testdata.get_foobar_fields()
        print(fields) # dict that can be used create a Foobar(fields) instance

        # will return a list of 2 Foobar instances
        os = await testdata.get_foobars(foobar_count=2)

        # will return a list of 2 created Foobar instances
        os = await testdata.create_foobars(foobar_count=2)


    To customize a method, you can extend this class and define the method. The
    signature for any overridden methods is (self, orm_class, **kwargs),
    and you will usually just want to override the get_<MODELS-NAME>_fields
    class since that is the one the get_* and create_* methods use to populate
    the orms

    All the signatures of all the methods need to be the same because they are
    mixed and matched, so if you have a `Foobar` Orm class and override the
    get_foobar_fields testdata method, then get_foobar() would call
    get_foobar_fields and get_orm(Foobar, **kwargs) would also call
    get_foobar_fields. So all of these methods can be mixed and matched

    :Example:
        from prom.extras.testdata import ModelData

        class MyModelData(ModelData):
            async def get_foobar_fields(self, orm_class, **kwargs):
                return await super().get_orm_fields(orm_class, **kwargs)

    If you'd like to customize the testdata method names, you can set the
    Orm.model_name or the Orm.models_name class properties

    :Example:
        from prom import Orm

        class Foobar(Orm):
            model_name = "foo_bar"
            models_name = "foo_bars"

    So this is the order of calls:

        * .create_orms
            * .get_orms
                * .get_orm
                    * .get_fields
                    * .create_orm_instance

        * .create_orm
            * .get_orm
                * .get_fields
                * .create_orm_instance

    Any kwargs you pass in any of the methods will be passed to the methods
    below, so if you pass in a value to .create_orm it will be available in
    .get_fields
    """
    def _orm_classes(self):
        """Iterate through a list of orms that should be injected into testdata

        by default, we want to ignore any orm classes that are defined in this
        library since they are by definition base classes

        :returns: generator[Orm], each orm class
        """
        Orm.orm_classes.insert_modules()

        for orm_class in Orm.orm_classes.edges():
            yield orm_class

    def _gets_count(self, orm_class, **kwargs):
        """Find how many orm_class should be created

        :param orm_class: Orm
        :param **kwargs: this will be checked for the correct *_count or count
            value
        :returns: int, how many of orm_class is wanted
        """
        return kwargs.get(
            f"{orm_class.model_name}_count",
            kwargs.get(
                f"{orm_class.models_name}_count",
                kwargs.get(
                    "count",
                    1
                )
            )
        )

#     async def _get(self, orm_class, **kwargs):
#         """Internal dispatcher method for get_orm, this will first try and find
#         a get_<ORM-NAME> method and fallback to get_orm"""
#         method_name = f"get_{orm_class.model_name}"
#         kwargs["default_method"] = self.get_orm
#         method_name, method = self._find_method(method_name, **kwargs)
# 
#         return await self._run_method(method_name, method, orm_class, **kwargs)

#     async def _gets(self, orm_class, **kwargs):
#         """Internal dispatcher method for get_orms, this will first try and find
#         a get_<ORM-MODELS-NAME> method and fallback to get_orms"""
#         method_name = f"get_{orm_class.models_name}"
#         kwargs["default_method"] = self.get_orms
#         method_name, method = self._find_method(method_name, **kwargs)
# 
#         return await self._run_method(method_name, method, orm_class, **kwargs)

#     async def _create(self, orm_class, **kwargs):
#         """Internal dispatcher method for create_orm, this will first try and
#         find a create_<ORM-NAME> method and fallback to create_orm"""
#         method_name = f"create_{orm_class.model_name}"
#         kwargs["default_method"] = self.create_orm
#         method_name, method = self._find_method(method_name, **kwargs)
# 
#         return await self._run_method(method_name, method, orm_class, **kwargs)

#     async def _creates(self, orm_class, **kwargs):
#         """Internal dispatcher method for create_orms, this will first try and
#         find a create_<ORM-MODELS-NAME> method and fallback to create_orm"""
#         method_name = f"create_{orm_class.models_name}"
#         kwargs["default_method"] = self.create_orms
#         method_name, method = self._find_method(method_name, **kwargs)
# 
#         return await self._run_method(method_name, method, orm_class, **kwargs)

#     async def _fields(self, orm_class, **kwargs):
#         """Internal dispatcher method for get_orm_fields, this will first try
#         and find a get_<ORM-NAME>_fields method and fallback to get_orm_fields
#         """
#         method_name = f"get_{orm_class.model_name}_fields"
#         kwargs["default_method"] = self.get_orm_fields
#         method_name, method = self._find_method(method_name, **kwargs)
# 
#         return await self._run_method(method_name, method, orm_class, **kwargs)

#     async def _instance(self, orm_class, **kwargs):
#         """Internal dispatcher method for create_orm_instance, this will first
#         try and find a create_<ORM-NAME>_instance method and fallback to
#         create_orm_instance
#         """
#         method_name = f"create_{orm_class.model_name}_instance"
#         #kwargs.setdefault("default_method", self.create_orm_instance)
#         kwargs["default_method"] = self.create_orm_instance
#         method_name, method = self._find_method(method_name, **kwargs)
# 
#         return await self._run_method(method_name, method, orm_class, **kwargs)

    def _find_method(self, method_name, default_method=None, **kwargs):
        """Find the method, this will return the user defined method or the
        default method if no user defined method exists

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

    def _run_method(self, orm_class, method, method_name, **kwargs):
        """This can run both asyncronous and syncronous methods, it's not async
        but should be awaited when running async methods"""
        kwargs.setdefault("orm_class", orm_class)
        logger.debug(
            "Running {} as {} for orm_class {}".format(
                method.__name__,
                method_name,
                orm_class.__name__,
            )
        )
        return method(**kwargs)

    async def _dispatch_method(self, orm_class, method, **kwargs):
        m = re.match(r"^([^_]+)_(orms?)(?:_(.+))?$", method.__name__)

        parts = [m.group(1)]

        if m.group(2) == "orm":
            parts.append(orm_class.model_name)

        else:
            parts.append(orm_class.models_name)

        if suffix := m.group(3):
            parts.append(suffix)

        method_name = "_".join(parts)
        kwargs["default_method"] = method
        method_name, method = self._find_method(method_name, **kwargs)

        return await self._run_method(orm_class, method, method_name, **kwargs)

    def _parse_method_name(self, method_name):
        """Parses method name and returns the magic method name/type, the
        inferred orm model name, and the found orm_class

        This raises an AttributeError on failure or invalid magic method name

        :param method_name: str, the full method name that will be parsed
        :returns: tuple[str, str, callable], (name, module_name, orm_class)
        """
        parts = method_name.split("_")

        if len(parts) == 1:
            raise AttributeError(
                f"Invalid magic method: {method_name}"
            )

        else:
            name = parts[0]
            if parts[-1] in set(["fields", "instance"]):
                name = parts[-1]
                model_name = "_".join(parts[1:-1])

            else:
                model_name = "_".join(parts[1:])

            try:
                orm_class = self.get_orm_class(model_name)

            except ValueError as e:
                raise AttributeError(
                    f"Could not derive orm class from {method_name}"
                ) from e

        return name, model_name, orm_class

    def _find_attr(self, method_name):
        """Internal method used by self.__getattr__ to find the orm and create a
        wrapper method to call

        NOTE -- this method must raise AttributeError on expected errors

        :param method_name: str, the method name that contains the model class
            we're ultimately looking for
        :returns: callable, the method that should be ran for the passed in
            method_name
        """
        logger.debug(
            f"Finding {self.__class__.__name__}.{method_name} method"
        )

        name, model_name, orm_class = self._parse_method_name(method_name)

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

        elif name == "instance":
            orm_method = self.create_orm_instance

        if orm_method:
            logger.debug(
                f"Found {orm_method.__name__} for {orm_class.__name__}"
            )

            def method(**kwargs):
                # https://github.com/Jaymon/prom/issues/166
                # we want to override the passed in orm_class if it
                # doesn't match our found orm class because this has
                # most likely been called internally by another magic
                # method that just passed kwargs
                kwargs.setdefault("orm_class", orm_class)
                if not isinstance(orm_class, kwargs["orm_class"]):
                    kwargs["orm_class"] = orm_class

                return orm_method(**kwargs)

            # could also use functools.wraps here on method instead of
            # just setting the name, but I like that it explicitely
            # says it is wrapped in the name instead of just
            # transparantly using orm_method's name
            method.__name__ = f"wrapped_getattr_{orm_method.__name__}"
            return method

        raise AttributeError(f"Could not find an orm matching {method_name}")

    def __getattribute__(self, method_name):
        attribute = super().__getattribute__(method_name)

        if not method_name.startswith("_") and callable(attribute):
            if not re.search(r"_orm(?:$|_)", method_name):
                try:
                    name, model_name, orm_class = self._parse_method_name(
                        method_name
                    )

                except AttributeError:
                    pass

                else:
                    attribute = functools.partial(
                        attribute,
                        orm_class=orm_class
                    )

                    attribute.__name__ = f"wrapped_getattribute_{method_name}"

        return attribute

    def __getattr__(self, method_name):
        try:
            return self._find_attr(method_name)

        except AttributeError:
            return super().__getattr__(method_name)

    async def close_orm_interfaces(self):
        """Close down all the globally created interfaces

        This, along with the unsafe_* methods are designed to be used in actual
        project testing, this is most useful in the test's asyncTearDown method
        """
        for inter in get_interfaces().values():
            await inter.close()

    async def unsafe_delete_orm_tables(self):
        """This will delete all the tables from the db

        NOTE -- You'll want to make sure you only call the method in the
        right environments as this really will delete all the tables in
        whatever dbs it has connections for
        """
        for inter in get_interfaces().values():
            await inter.unsafe_delete_tables()

    async def unsafe_install_orms(self, modulepaths=None):
        """Go through and install all the Orm subclasses found in the passed in
        module paths

        :param modulepaths: Sequence[str], a list of modpaths (eg ["foo.bar",
        "che"])
        """
        # import the module paths to load the Orms into memory
        if modulepaths:
            for modulepath in modulepaths:
                rm = ReflectModule(modulepath)
                m = rm.module() 

        # now go through all the orm classes that have been loaded and install
        # them
        seen_table_names = set()
        for orm_class in self._orm_classes():
            for s in orm_class.schema.schemas:
                if s.table_name not in seen_table_names:
                    await s.orm_class.install()
                    seen_table_names.add(s.table_name)

    def assure_orm_field_names(self, orm_class, **kwargs):
        """Field instances can have aliases, in order to allow you to pass in
        aliases, this will go through kwargs and normalize the field names

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
        # normalize passed in field names to make sure we correctly find the
        # field's value if it exists
        for field_name in list(kwargs.keys()):
            if schema.has_field(field_name):
                kwargs.setdefault(
                    schema.field_name(field_name),
                    kwargs.pop(field_name)
                )

        return kwargs

    def assure_ref_field_names(self, orm_class, ref_class, **kwargs):
        """Make sure the kwargs destined for orm_class don't impact the kwargs
        that will be used to create a ref_class instance

        Sometimes, we have multiple Orm classes that have the same field names
        and when those fields get set for orm_class they can cause ref_class to
        fail so we want to strip those common field names out

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

    async def assure_orm_refs(self, orm_class, assure_orm_class=None, **kwargs):
        """When creating an orm, they will often need foreign key values, this
        will go through any of the foreign key ref fields and create a foreign
        key if it wasn't included.

        This is a recursive method, when it finds a ref_class it will assure
        that ref's foreign keys before handling ref, this way all references all
        the way down the stack can be generated for the top level so the same
        refs can propogate all the way down the dependency chain

        :param orm_class: Orm
        :param assure_orm_class: Orm|None, the orm that is getting its
            references assured. If this is None then orm_class is considered the
            orm that is being checked
        :param **kwargs: orm_class's actual field name value will be checked and
            the ref's orm_class.model_name will be checked
            * ignore_refs: bool, default False, if True then refs will not be
                checked, passed in refs will still be set
            * require_fields: bool, default True, if True then create missing
                refs, if False, then refs won't be created and so if they are
                missing their fields will not be populated
            * ignore_field_names: set[str]|list[str], a set of field names that
                should be ignored when creating refs
        :returns: dict, the kwargs with ref field_name and ref
            orm_class.model_name will be included
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
        ignore_field_names = set(kwargs.get("ignore_field_names", []))

        for field_name, field in orm_class.schema.fields.items():
            if ref_class := field.ref:
                ref_field_name = ref_class.model_name

                if field_name in kwargs:
                    if ref_field_name not in kwargs:
                        kwargs[ref_field_name] = await ref_class.query.eq_pk(
                            kwargs[field_name]
                        ).one()

                elif field_name in ignore_field_names:
                    # we were explicitely told to ignore this field
                    continue

                else:
                    if ref_field_name in kwargs:
                        kwargs[field_name] = kwargs[ref_field_name].pk

                    else:
                        if not ignore_refs:
                            if (
                                require_fields
                                or field.is_required()
                                or self.yes()
                            ):
                                # handle all ref_class's refs before we handle
                                # ref_class
                                kwargs.update(await self.assure_orm_refs(
                                    ref_class,
                                    assure_orm_class,
                                    **kwargs
                                ))

                                fields = await self._dispatch_method(
                                    ref_class,
                                    self.create_orm,
                                    **self.assure_ref_field_names(
                                        assure_orm_class,
                                        ref_class,
                                        **kwargs
                                    )
                                )

                                kwargs[ref_field_name] = fields
                                kwargs[field_name] = kwargs[ref_field_name].pk

        return kwargs

    async def create_orm(self, orm_class, **kwargs):
        """create an instance of the orm and save it into the db

        :param orm_class: Orm
        :param **kwargs:
        :returns: Orm, the orm saved into the db
        """
        kwargs.setdefault("ignore_refs", False)
        instance = await self._dispatch_method(
            orm_class,
            self.get_orm,
            **kwargs
        )
        try:
            await instance.save(nest=True)

        except UniqueError as e:
            logger.warning(" ".join([
                f"Creating {orm_class.__name__} failed because it exists.",
                "Fetching the existing instance without updating it",
            ]))
            instance = await instance.requery()

        for k in list(kwargs.keys()):
            # We want to create any orms with FK references to orm_class if
            # counts were passed in
            if k.endswith("_count"):
                k_model_name = k[0:-6]
                k_orm_class = self.get_orm_class(k_model_name)
                if k_orm_class:
                    logger.debug(
                        "Creating {} {} instances tied to {} instance".format(
                            kwargs[k],
                            k_orm_class.__name__,
                            orm_class.__name__,
                        )
                    )
                    kwargs.setdefault(instance.model_name, instance)
                    await self.create_orms(k_orm_class, **kwargs)

        return instance

    async def create_orms(self, orm_class, **kwargs):
        """create instances of the orm and save it into the db

        :param orm_class: Orm
        :param **kwargs:
            count: int, how many instances you want
            <MODEL_NAME>_count: int, alias for count
        :returns: list, a list of Orm instances that have all been saved into
            the db
        """
        kwargs.setdefault("ignore_refs", False)
        instances = await self._dispatch_method(
            orm_class,
            self.get_orms,
            **kwargs
        )
        for instance in instances:
            await instance.save(nest=True)
        return instances

    def get_orm_class(self, model_name, **kwargs):
        """get the orm class found at model_name

        Yes, I'm aware I've basically just changed the name from find_* to
        get_* but, for some reason, it makes sense to me to have get_* here
        because it matches all the other get_orm_* methods while it feels right
        to have Orm.find_*, no idea why but Orm.get_orm_class didn't feel right

        :param model_name: str, the name of the orm class
        :returns: Orm, the orm_class.model_name that matches model_name
        """
        return Orm.find_orm_class(model_name)

    async def get_orm(self, orm_class, **kwargs):
        """get an instance of the orm but don't save it into the db

        :param orm_class: Orm
        :param **kwargs:
        :returns: Orm
        """
        kwargs.setdefault("ignore_refs", True)
        instance = kwargs.get(orm_class.model_name, None)
        if not instance:
            kwargs["fields"] = await self._dispatch_method(
                orm_class,
                self.get_orm_fields,
                **kwargs
            )

            if kwargs["fields"] and "**" in kwargs["fields"]:
                kwargs.update(kwargs["fields"].pop("**"))

            instance = await self._dispatch_method(
                orm_class,
                self.create_orm_instance,
                **kwargs
            )

        return instance

    async def get_orms(self, orm_class, **kwargs):
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
        if kwargs.get("related_refs", True):
            # because we need related refs, we will need to create refs if they
            # don't exist
            kwargs.setdefault("ignore_refs", False)
            kwargs = await self.assure_orm_refs(orm_class, **kwargs)

        count = self._gets_count(orm_class, **kwargs)
        for _ in range(count):
            ret.append(await self._dispatch_method(
                orm_class,
                self.get_orm,
                **kwargs
            ))

        return ret

    async def create_orm_instance(self, orm_class, **kwargs):
        """Semi-internal method to actually create an instance of orm_class

        This is semi-internal because it can be overridden and customized but
        it can't be called externally, it is designed to only be called 
        internally by the other methods but never to be called externally

        https://github.com/Jaymon/prom/issues/170

        :param orm_class: Orm
        :param fields: dict[str, Any], these should ruoughly correspond to the
            Orm's Schema fields 
        :param **kwargs:
            * attributes: dict[str, Any], These will be set onto the instance
                after it is created
            * fields: dict[str, Any], these are the actual fields that will be
                passed to Orm.__init__, that makes this kwargs different than
                other methods because the fields have been separated out.
                Basically, if you did .get_orm_class(FooOrm, bar="...") then
                by the time we got to here you would access bar with
                kwargs["fields"]["bar"] instead of just kwargs["bar"] in all
                the other methods
        :returns: Orm, the actual instance populated with fields 
        """
        instance = orm_class(kwargs["fields"])

        if attributes := kwargs.get("attributes", {}):
            for name, value in attributes.items():
                setattr(instance, name, value)

        return instance

    async def get_orm_fields(self, orm_class, **kwargs):
        """Get the fields of an orm_class

        :param orm_class: Orm
        :param **kwargs: the fields found in orm_class.schema
            * require_fields: bool, default True, this will require that all
                fields have values even if they aren't required, this does not
                apply to foreign key references
            * field_callbacks: dict, the key is the field name and the value is
                a callable that can take self
            * ignore_field_names: set[str]|list[str], a set of field names that
                should be ignored when creating refs
            * fields: dict[str, Any], these will be used to seed the return
                dict, you use this to get non schema fields to be passed to
                orm's __init__ method
        :returns: dict, these are the fields that will be passed to Orm.__init__
            with one exception, if the dict contains a key "**" then that key
            will be popped and it's value (which should be a dict) will update
            the kwargs that are passed to .create_orm_instance to create the
            actual orm instance. The "**" key is just a handy way to do all
            customizing in one overridden method
        """
        ret = kwargs.pop("fields", {})
        schema = orm_class.schema

        kwargs.setdefault("ignore_refs", True)
        kwargs = await self.assure_orm_refs(orm_class, **kwargs)

        ignore_field_names = set(kwargs.get("ignore_field_names", []))

        for field_name, field in schema.fields.items():
            if field_name in kwargs:
                # this value was passed in so we don't need to do anything
                ret[field_name] = kwargs[field_name]

            elif field_name in ignore_field_names:
                # we were explicitely told to ignore this field
                pass

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

            elif not self.get_orm_field_required(field_name, field, **kwargs):
                pass

            else:
                ret[field_name] = self.get_orm_field_value(
                    field_name,
                    field,
                    fields=ret,
                    **kwargs
                )

        return ret

    def get_orm_field_value(self, field_name, field, **kwargs):
        """Returns the generated value for the specific field

        This is a wrapper around all the specific field type generators, see the
        other get_orm_field_* methods

        :param field_name: str
        :param field: Field, the orm's field property
        :param **kwargs: see .get_orm_fields for values this can have
        :returns: Any, the generated value
        """
        if field.choices:
            ret = self.get_orm_field_choice(
                field_name,
                field,
                **kwargs
            )

        else:
            field_callbacks = kwargs.get("field_callbacks", {})
            field_type = field.interface_type

            cb = getattr(
                field,
                "testdata",
                field.options.get(
                    "testdata",
                    field_callbacks.get(field_name, None)
                )
            )
            if cb:
                ret = cb(self)

            else:
                if issubclass(field_type, bool):
                    ret = self.get_orm_field_bool(
                        field_name,
                        field,
                        **kwargs
                    )

                elif issubclass(field_type, int):
                    ret = self.get_orm_field_int(
                        field_name,
                        field,
                        **kwargs
                    )

                elif issubclass(field_type, str):
                    ret = self.get_orm_field_str(
                        field_name,
                        field,
                        **kwargs
                    )

                elif issubclass(field_type, dict):
                    ret = self.get_orm_field_dict(
                        field_name,
                        field,
                        **kwargs
                    )

                elif issubclass(field_type, float):
                    ret = self.get_orm_field_float(
                        field_name,
                        field,
                        **kwargs
                    )

                elif issubclass(field_type, datetime.datetime):
                    ret = self.get_orm_field_datetime(
                        field_name,
                        field,
                        **kwargs
                    )

                elif issubclass(field_type, datetime.date):
                    ret = self.get_orm_field_date(
                        field_name,
                        field,
                        **kwargs
                    )

                elif issubclass(field_type, uuid.UUID):
                    ret = self.get_orm_field_uuid(
                        field_name,
                        field,
                        **kwargs
                    )

                else:
                    ret = self.get_orm_field_any(
                        field_name,
                        field,
                        **kwargs
                    )

        return ret

    def get_orm_field_required(self, field_name, field, **kwargs):
        """Returns True if field is required"""
        require_fields = kwargs.get("require_fields", True)
        return require_fields or field.is_required() or self.yes()

    def get_orm_field_choice(self, field_name, field, **kwargs):
        """Returns one of field's defined choices"""
        return self.choice(field.choices)

    def get_orm_field_bool(self, field_name, field, **kwargs):
        return bool(self.yes())

    def get_orm_field_int(self, field_name, field, **kwargs):
        size_info = field.size_info()
        return self.get_posint(
            size_info["size"]
        )

    def get_orm_field_str(self, field_name, field, **kwargs):
        size_info = field.size_info()
        if "bounds" in size_info:
            ret = self.get_words(
                min_size=size_info["bounds"][0],
                max_size=size_info["bounds"][1],
            )

        else:
            ret = self.get_words()

        return ret

    def get_orm_field_dict(self, field_name, field, **kwargs):
        return self.get_dict()

    def get_orm_field_float(self, field_name, field, **kwargs):
        size_info = field.size_info()
        return self.get_posfloat(
            size_info["size"]
        )

    def get_orm_field_datetime(self, field_name, field, **kwargs):
        return self.get_past_datetime()

    def get_orm_field_date(self, field_name, field, **kwargs):
        return self.get_past_date()

    def get_orm_field_uuid(self, field_name, field, **kwargs):
        return str(uuid.uuid4())

    def get_orm_field_any(self, field_name, field, **kwargs):
        """If one of the other field type generators isn't called then this
        will be called. It's designed for children classes to customize the
        field value generator further and support more types"""
        raise ValueError(
            f"Not sure what to do with {field.type}"
        )

