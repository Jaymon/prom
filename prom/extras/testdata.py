# -*- coding: utf-8 -*-
import string
import random
import datetime
import functools
import logging
import uuid
import re
from collections.abc import Sequence
from typing import Type

from testdata.base import TestData
from datatypes import (
    OrderedSubclasses,
    ReflectModule,
    ReflectClass,
)

from ..model import Orm
from ..config import Schema, Field, Index, AutoIncrement
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

    async def _dispatch_method(self, orm_class, method, **kwargs):
        """Internal dispatch method. This uses the orm_class to first try and
        call a magical wrapper method (eg, if `orm_class` was named `Foo` and
        method was `.get_orm` then this would first try and run `.get_foo`
        and only then would it fallback to method

        :param orm_class: type|str, the Orm child
        :param method: callable, the fallback method
        :param **kwargs: passed through to whatever method is ran
        :returns: Any, whatever the ran method returns
        """
        if isinstance(orm_class, str):
            orm_class = self.get_orm_class(orm_class)

        method_names = set(dir(self))
        m = re.match(r"^([^_]+)_(orms?)(?:_(.+))?$", method.__name__)

        for oc in Orm.orm_classes.getmro(orm_class):
            parts = [m.group(1)]

            if m.group(2) == "orm":
                parts.append(oc.model_name)

            else:
                parts.append(oc.models_name)

            if suffix := m.group(3):
                parts.append(suffix)

            method_name = "_".join(parts)
            if method_name in method_names:
                method = getattr(self, method_name)
                break

        logger.debug(
            "Running {} as {} for orm_class {}".format(
                method.__name__,
                method_name,
                orm_class.__name__,
            )
        )

        kwargs.setdefault("orm_class", orm_class)
        return await method(**kwargs)

    def _parse_dispatch(self, method_name):
        """Parses method name and returns the found orm_class and the
        default/fallback method that can be passed to ._dispatch_method

        .. note:: this is where a lot of the parsing magic happens, for
            adding functionality in the future this is probably the method
            to start with

        :param method_name: str, the full method name that will be parsed
        :returns: tuple[type, callable], (orm_class, method), this will return
            (None, None) if method_name isn't valid
        """
        orm_class = method = None

        parts = method_name.split("_")

        if len(parts) > 1 and parts[1] not in set(["orm", "orms"]):
            prefix = parts[0]
            model_name = suffix = ""
            method_names = set(dir(self))

            method_name = "_".join([prefix, "orm", parts[-1]])
            if method_name in method_names:
                model_name = "_".join(parts[1:-1])
                suffix = parts[-1]

            if not model_name:
                method_name = "_".join([prefix, "orm"])
                if method_name in method_names:
                    model_name = "_".join(parts[1:])
                    suffix = ""

            if model_name:
                if orm_class := self.get_orm_class(model_name, None):
                    if orm_class.model_name == model_name:
                        parts = [prefix, "orm"]

                    else:
                        parts = [prefix, "orms"]

                    if suffix:
                        parts.append(suffix)

                    method = getattr(self, "_".join(parts))

        return orm_class, method

    def __getattribute__(self, method_name):
        """Introspect method_name to see if it is a valid Orm reflection
        request, if it isn't then pass the call on down the line

        To be even more magical this will try and parse method_name and if
        it is valid it will return a partial that has the orm_class property
        set to make it easier to call without having to worry about things like
        having the right orm_class

        :param method_name: str, the method name we're looking for, if this
            isn't actually a method_name then it will pass it on down the line
        :returns: Any, if it successfully identified method_name then this will
            return a callable, otherwise it's whatever on down the line returns
        """
        if method_name.startswith("_"):
            return super().__getattribute__(method_name)

        orm_class = default_method = method = None

        try:
            method = super().__getattribute__(method_name)

        except AttributeError:
            logger.debug(
                f"Finding {self.__class__.__name__}.{method_name} method"
            )

            orm_class, default_method = self._parse_dispatch(method_name)

            if default_method:
                logger.debug(
                    f"Found {default_method.__name__} for {orm_class.__name__}"
                )

            else:
                raise

        if callable(method) or default_method:
            if not orm_class:
                orm_class, _ = self._parse_dispatch(method_name)

            if orm_class:
                def wrapper(**kwargs):
                    # https://github.com/Jaymon/prom/issues/166
                    # we want to override the passed in orm_class if it
                    # doesn't match our found orm class because this has
                    # most likely been called internally by another magic
                    # method that just passed kwargs
                    kwargs.setdefault("orm_class", orm_class)

                    if not isinstance(orm_class, kwargs["orm_class"]):
                        kwargs["orm_class"] = orm_class

                    # if these are always there it's easier to mess with them
                    # in child methods
                    kwargs.setdefault("fields", {})
                    kwargs.setdefault("properties", {})

                    if method:
                        return method(**kwargs)

                    else:
                        return self._dispatch_method(
                            method=default_method,
                            **kwargs
                        )

                wrapper.__name__ = f"wrapped_getattribute_{method_name}"
                return wrapper

        return method

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

    async def unsafe_install_orms(self, modpaths=None):
        """Go through and install all the Orm subclasses found in the passed in
        module paths

        This exists because if you are making use of a lot of transactions
        that don't nest that can cause prom's built-in error handling to
        completely fail so it's easier to create all the tables in the 
        beginning than rely on prom to magically do it

        :param modpaths: Sequence[str], a list of modpaths (eg ["foo.bar",
            "che"])
        """
        # import the module paths to load the Orms into memory
        if modpaths:
            Orm.orm_classes.insert_modules(modpaths)

        # now go through all the orm classes that have been loaded and install
        # them
        seen_table_names = set()
        for orm_class in self._orm_classes():
            for s in orm_class.schema.schemas:
                if s.table_name not in seen_table_names:
                    await s.orm_class.install()
                    seen_table_names.add(s.table_name)

    async def unsafe_reset_orms(self, modpaths=None):
        """Delete all the tables in the db and then load all the Orm child
        classes and make sure they have a table

        NOTE -- this is incredibly unsafe, it deletes and creates tables in
        the configured db

        :param modpaths: passed through to .unsafe_install_orms
        """
        await self.unsafe_delete_orm_tables()
        await self.unsafe_install_orms(modpaths=modpaths)

    def assure_orm_field_names(self, orm_class, **kwargs):
        """Field instances can have aliases, in order to allow you to pass in
        aliases, this will go through kwargs and normalize the field names

        :Example:
            class Foobar(Orm):
                che = Field(str, aliases=["baz"]

            kwargs = testdata.assure_orm_field_names(Foobar, {"baz": "1"})
            print(kwargs["che"]) # "1"

        :param orm_class: Orm
        :param **kwargs: the fields where keys will be normalized to field
            names in orm_class.schema
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
        :keyword ignore_refs: bool, default False, if True then refs will not
            be checked, passed in refs will still be set
        :keyword require_fields: bool, default True, if True then create
            missing refs, if False, then refs won't be created and so if they
            are missing their fields will not be populated
        :keyword ignore_field_names: set[str]|list[str], a set of field names
            that should be ignored when creating refs
        :param **kwargs: orm_class's actual field name value will be checked
            and the ref's orm_class.model_name will be checked
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

        The typical method call hierarchy from here:

            <CREATE-ORM>
                <GET-ORM>
                [<CREATE-ORMS>]

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

    def get_orm_class(self, model_name, *default, **kwargs):
        """get the orm class found at model_name, if model_name doesn't exist
        than return default if it exists

        :param model_name: str, the name of the orm class
        :param *default: Any, if exists then this will be returned instead of
            raising a ValueError if model_name isn't found
        :returns: Orm, the orm_class.model_name that matches model_name
        """
        try:
            return Orm.orm_classes[model_name]

        except KeyError:
            if default:
                return default[0]

            else:
                raise

    async def get_orm(self, orm_class, **kwargs):
        """get an instance of the orm but don't save it into the db

        The method call hierarchy from here:

            <GET-ORM>
                <GET-ORM-FIELDS>
                <CREATE-ORM-INSTANCE>

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

            # since .get_orm_fields is the most common overrided method, this
            # is just a nice message to notify that you most likely forgot to
            # return something
            if kwargs["fields"] is None:
                raise ValueError((
                    "Dispatched {orm_class.model_name}"
                    " get fields method returned None instead of dict"
                ))

            # If the `.get_orm_fields` method returns a properties dict we
            # want to add it to the passed in properties dict, this is just
            # syntactic sugar so child classes only really ever need to
            # override the fields method
            if properties := kwargs["fields"].pop("properties", None):
                kwargs["properties"] = properties

            # the "**" key should be a dict and pulls all the values in the
            # dict back into the top kwargs so they are passed as keywords
            # to `.create_orm_instance`
            if keywords := kwargs["fields"].pop("**", None):
                kwargs.update(keywords)

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
            * related_refs: bool, default True, all the orms will have the
                same foreign key references
        :returns: list, a list of Orm instances
        """
        ret = []
        if kwargs.get("related_refs", True):
            # because we need related refs, we will need to create refs if
            # they don't exist
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
        it shouldn't be called externally, it is designed to only be called 
        internally by the other methods but never to be called externally

        https://github.com/Jaymon/prom/issues/170

        :param orm_class: Orm
        :param fields: dict[str, Any], these should ruoughly correspond to the
            Orm's Schema fields 
        :keyword properties: dict[str, Any], These will be set onto the
            instance after it is created
        :keyword fields: dict[str, Any], these are the actual fields that will
            be passed to Orm.__init__, that makes this kwargs different than
            other methods because the fields have been separated out.
            Basically, if you did .get_orm_class(FooOrm, bar="...") then by the
            time we got to here you would access bar with
            kwargs["fields"]["bar"] instead of just kwargs["bar"] in all the
            other methods
        :returns: Orm, the actual instance populated with fields 
        """
        instance = orm_class(kwargs["fields"])

        if properties := kwargs.get("properties", {}):
            for name, value in properties.items():
                setattr(instance, name, value)

        return instance

    async def get_orm_fields(self, orm_class, **kwargs):
        """Get the fields of an orm_class

        The method call hierarchy from here:

            <GET-ORM-FIELDS>
                <ASSURE-ORM-REFS>
                <GET-ORM-FIELD-VALUE>

        :param orm_class: Orm
        :keyword require_fields: bool, default True, this will require that all
            fields have values even if they aren't required, this does not
            apply to foreign key references
        :keyword field_callbacks: dict, the key is the field name and the
            value is a callable that can take self
        :keyword ignore_field_names: set[str]|list[str], a set of field names
            that should be ignored when creating refs
        :keyword fields: dict[str, Any], these will be used to seed the return
            dict, you use this key to get non schema fields to be passed to
            orm's __init__ method
        :keyword: properties: dict[str, Any], these are set in
            `.create_orm_instance` as properties on the instance, so they
            aren't passed to the init method but set after the instance is
            created
        :returns: dict, these are the fields that will be passed to
            Orm.__init__ with one exception, if the dict contains a key "**"
            then that key will be popped and it's value (which should be a
            dict) will update the kwargs that are passed to
            .create_orm_instance to create the actual orm instance. The "**"
            key is just a handy way to do all customizing in one overridden
            method, certain returned keys do certain things:
                * **: dict, yes, that's right, a double asterisks key will
                    be moved back into the kwargs that will be passed to the
                    .create_orm_instance method, this allows a child method
                    to pass keywords to .get_orm_instance because 
                    .get_orm_instance expects all the orm init fields to be
                    in the fields keyword
                * properties: dict, this dict will be set into the orm
                    instance after it is created
        """
        fields = kwargs.pop("fields", {})

        if properties := kwargs.pop("properties", None):
            fields["properties"] = properties

        if keywords := kwargs.pop("**", None):
            fields["**"] = keywords

        kwargs.setdefault("ignore_refs", True)
        kwargs = await self.assure_orm_refs(orm_class, **kwargs)

        return self.get_schema_fields(
            orm_class.schema,
            fields=fields,
            **kwargs
        )

    def get_schema_fields(self, schema: Schema, **kwargs) -> dict:
        """Semi internal method. Generates random values for all the fields
        in `schema`

        see `.get_orm_fields`, this is separate from that method since it
        is sometimes useful to generate a random schema with `.get_schema`
        and then get a set field values for that schema without having
        an Orm instance.

        Pretty much everything you can pass into `.get_orm_fields` you can
        pass into this method
        """
        fields = kwargs.pop("fields", {})
        ignore_field_names = set(kwargs.get("ignore_field_names", []))

        for field_name, field in schema.fields.items():
            if field_name in kwargs:
                # this value was passed in so we don't need to do anything
                fields[field_name] = kwargs[field_name]

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
                fields[field_name] = self.get_orm_field_value(
                    field_name,
                    field,
                    fields=fields,
                    **kwargs
                )

        return fields

    def get_orm_field_value(self, field_name, field, **kwargs):
        """Returns the generated value for the specific field

        This is a wrapper around all the specific field type generators, see
        the other get_orm_field_* methods

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

                elif issubclass(field_type, (bytes, bytearray)):
                    ret = self.get_orm_field_bytes(
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

    def get_orm_field_bytes(self, field_name, field, **kwargs):
        s = self.get_orm_field_str(field_name, field, **kwargs)
        return s.encode()

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


class ModelData(ModelData):
    """This contains additional functionality to create/generate Orm classes
    if they're not passed into the `*_orm*` methods
    """
    def get_table_name(self, *args, **kwargs):
        """return a random table name

        All values are passed through to `.get_field_name`
        """
        kwargs.setdefault("suffix", "_table")
        return self.get_field_name(*args, **kwargs)

    def get_field_name(
        self,
        name: str = "",
        *,
        prefix: str = "",
        suffix: str = "",
        **kwargs
    ) -> str:
        """return a random name

        :param name: if passed in then this will be returned
        :keyword prefix: if passed in then a name will be randomly
            generated that starts with this value
        :keyword suffix: if present then generate a random name that ends
            with this value
        """
        if name:
            return name

        return "{}{}{}".format(
            prefix,
            "".join(
                random.sample(string.ascii_lowercase, random.randint(5, 15))
            ),
            suffix,
        )

    def get_schema_field(
        self,
        field_name: str = "",
        field_type: type|None = None,
        *,
        field_required: bool|None = None,
        **kwargs
    ) -> Field:
        """Create a Field instance that can be added to a schema

        :param field_name: the name of the field name, `.get_field_name` will
            be called if this is empty
        :param field_type: the type of the field, will be randomly assigned
            if None
        :keyword field_required: this will randomly assigned if None
        """
        if not field_name:
            kwargs.setdefault("suffix", "_field")
            field_name = self.get_field_name(**kwargs)

        if field_type is None:
            field_type = random.choice([
                str,
                bytes,
                bool,
                int,
                datetime.datetime,
                float
            ])

        field_class = kwargs.get("field_class", Field)

        if field_required is None:
            field_required = random.choice([True, False])

        return field_class(field_type, field_required, name=field_name)

    def get_schema(
        self,
        field_count: int = 0,
        *,
        field_names: Sequence[str]|None = None,
        fields: dict[str, Field|Type]|None = None,
        indexes: dict[str, Sequence[str]|Index]|None = None,
        refs: Sequence[Orm]|None = None,
        **kwargs
    ) -> Schema:
        """Get a Schema instance

        :param field_count: how many fields you want in the schema, a
            random value will be used if 0
        :keyword field_names: a list of field names that will be added to
            the schema
        :keyword fields: a mapping of field names to types or Field
            instances that will be added to the schema
        :keyword indexes: a mapping of index names to Index instances
        :keyword refs: a list of Orm classes where a foreign key field
            will be added to the schema
        :keyword table_name: if empty a random name will be used
        """
        fields = fields or {}

        field_names = field_names or []
        for field_name in field_names:
            if field_name not in fields:
                fields[field_name] = self.get_schema_field(field_name)

        refs = refs or []
        for orm_class in refs:
            field_name = f"{orm_class.model_name}_id"
            fields[field_name] = self.get_schema_field(field_name, orm_class)

        if len(fields) == 0:
            if field_count == 0:
                field_count = random.randint(1, 5)

        else:
            field_count -= len(fields)

        if "_id" not in fields:
            fields["_id"] = AutoIncrement()

        if field_count > 0:
            for i in range(field_count):
                field = self.get_schema_field()
                fields[field.name] = field

        for field_name in list(fields.keys()):
            if fields[field_name] is None:
                # remove any None values
                fields.pop(field_name)

            else:
                if not isinstance(fields[field_name], Field):
                    fields[field_name] = self.get_schema_field(
                        field_name,
                        fields[field_name],
                    )

        indexes = indexes or {}
        for index_name in list(indexes.keys()):
            if index_name in fields:
                raise ValueError(f"index {index_name} also in fields")

            if indexes[index_name]:
                if not isinstance(indexes[index_name], Index):
                    indexes[index_name] = Index(*indexes[index_name])

            else:
                # remove any None values
                indexes.pop(index_name)

        s = Schema(
            kwargs.get("table_name", self.get_table_name(**kwargs)),
            **fields,
            **indexes,
        )
        return s

    def _assure_orm_class(
        self,
        orm_class: Type[Orm]|None,
        **kwargs
    ) -> Type[Orm]:
        """Internal method. Makes sure orm_class is a valid Orm class"""
        if orm_class is None:
            # create an orm class if we don't have one
            orm_class = self.get_orm_class(**kwargs)

        return orm_class

    def get_orm_class(
        self,
        model_name: str = "",
        *default,
        **kwargs
    ) -> Type[Orm]:
        """get the orm class found at model_name, if model_name doesn't exist
        than return default if it exists

        :param model_name: str, the name of the orm class, if empty then a
            random orm will be generated
        :param *default: Any, if exists then this will be returned instead of
            raising an exception if model_name isn't found
        :keyword schema: only used if `model_name` is empty, this will be the
            schema for a generated orm
        :keyword parent_class: only used if `model_name` is empty, this will
            be the parent class of the randomly generated Orm subclass
        :keyword interface: only used if `model_name` is empty, this will
            be the interface the generated Orm class uses
        :returns: Orm, the orm_class.model_name that matches model_name
        """
        if model_name:
            return super().get_orm_class(model_name, *default, **kwargs)

        else:
            if default:
                return default[0]

            else:
                interface = kwargs.pop("interface", None)

                # generate a new orm class
                if "schema" in kwargs:
                    schema = kwargs["schema"]

                else:
                    kwargs.setdefault("suffix", "_orm")
                    schema = self.get_schema(**kwargs)

                parent_class = kwargs.get("parent_class", Orm)

                orm_class_properties = {
                    "schema": schema,
                    "table_name": schema.table_name,
                }
                if interface is not None:
                    orm_class_properties["interface"] = interface

                orm_class = type(
                    schema.table_name,
                    (parent_class,),
                    {
                        **orm_class_properties,
                        **schema.fields,
                        **schema.indexes,
                    },
                )

                schema.orm_class = orm_class

                return orm_class

    async def get_orm(self, orm_class: type|None = None, **kwargs) -> Orm:
        """Wraps `.get_orm` but creates an `orm_class` if nothing is passed
        in"""
        orm_class = self._assure_orm_class(orm_class, **kwargs)
        return await super().get_orm(orm_class, **kwargs)

    async def get_orms(
        self,
        orm_class: type|None = None,
        **kwargs
    ) -> Sequence[Orm]:
        """Wraps `.get_orms` but creates an `orm_class` if None"""
        orm_class = self._assure_orm_class(orm_class, **kwargs)
        return await super().get_orms(orm_class, **kwargs)

    async def create_orm(self, orm_class: type|None = None, **kwargs) -> Orm:
        """Wraps `.create_orm` but creates an `orm_class` if None"""
        orm_class = self._assure_orm_class(orm_class, **kwargs)
        return await super().create_orm(orm_class, **kwargs)

    async def create_orms(
        self,
        orm_class: type|None = None,
        **kwargs
    ) -> Sequence[Orm]:
        """Wraps `.create_orms` but creates an `orm_class` if None"""
        orm_class = self._assure_orm_class(orm_class, **kwargs)
        return await super().create_orms(orm_class, **kwargs)

