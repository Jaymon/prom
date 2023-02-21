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


logger = logging.getLogger(__name__)


# class OrmTestDataMixin(object):
#     data_instance = None
# 
#     def __init_subclass__(cls):
#         super().__init_subclass__()
# 
#         # find the OrmData instance
#         for instance in TestData.data_instances.values():
#             if isinstance(instance, OrmData):
#                 cls.data_instance = instance
#                 break
# 
#         testdata = TestData.module()
#         class_name = cls.__name__.lower()
#         create_name = f"create_{class_name}"
#         #create_lambda = lambda **kwargs: cls.data_instance.create_orm(orm_class=cls, **kwargs)
#         setattr(testdata, create_name, cls.testdata_create_method)
#         setattr(cls.data_instance, create_name, cls.testdata_create_method)
# 
#         get_name = f"get_{class_name}"
#         #get_lambda = lambda **kwargs: cls.data_instance.get_orm(orm_class=cls, **kwargs)
#         setattr(testdata, get_name, cls.testdata_get_method)
#         setattr(cls.data_instance, get_name, cls.testdata_get_method)
# 
#         fields_name = f"get_{class_name}_fields"
#         #get_lambda = lambda **kwargs: cls.data_instance.get_orm_fields(schema=cls.schema, **kwargs)
#         if not hasattr(testdata, fields_name):
#             setattr(testdata, fields_name, cls.testdata_fields_method)
#         if not hasattr(testdata, fields_name):
#             setattr(cls.data_instance, fields_name, cls.testdata_fields_method)
# 
#     @classmethod
#     def testdata_fields(cls, data_instance, **kwargs):
#         return data_instance.get_orm_fields(cls.schema, **kwargs)
# 
#     @classmethod
#     def testdata(cls, data_instance, **kwargs):
#         fields = cls.testdata_fields(data_instance, **kwargs)
#         return cls(fields)
# 
#     @classmethod
#     def testdata_fields_method(cls, *args, **kwargs):
#         return cls.testdata_fields(cls.data_instance, **kwargs)
# 
#     @classmethod
#     def testdata_get_method(cls, **kwargs):
#         return cls.testdata(cls.data_instance, **kwargs)
# 
#     @classmethod
#     def testdata_create_method(cls, **kwargs):
#         instance = cls.testdata(cls.data_instance, **kwargs)
#         instance.save()
#         return instance

#     @classmethod
#     def testdata_create(cls, **kwargs):
#         fields = cls.testdata_fields(**kwargs)
#         return cls.create(fields)


class ModelData(TestData):
    injected_orm_classes = set([])

    def _orm_classes(self):
        orm_classes = OrderedSubclasses(classes=Orm.orm_classes.values())
        module_name = ReflectModule(__name__).modroot

        for orm_class in orm_classes.edges():
            # we want to ignore any orm classes that are defined in this library
            # since they are by definition base classes
            if module_name not in orm_class.__module__:
                yield orm_class

    def _find_method(self, method_name, default_method=None, **kwargs):
        method = getattr(self, method_name, None)
        if method:
            return method_name, method

        else:
            return method_name, default_method

    def _get_method(self, orm_class, **kwargs):
        method_name = f"get_{orm_class.model_name}"
        return self._find_method(method_name, **kwargs)

    def _gets_method(self, orm_class, **kwargs):
        method_name = f"get_{orm_class.models_name}"
        return self._find_method(method_name, **kwargs)

    def _create_method(self, orm_class, **kwargs):
        method_name = f"create_{orm_class.model_name}"
        return self._find_method(method_name, **kwargs)

    def _creates_method(self, orm_class, **kwargs):
        method_name = f"create_{orm_class.models_name}"
        return self._find_method(method_name, **kwargs)

    def _fields_method(self, orm_class, **kwargs):
        method_name = f"get_{orm_class.model_name}_fields"
        return self._find_method(method_name, **kwargs)

    def _inject_update(self, testdata):
        for orm_class in self._orm_classes():
            if orm_class not in self.injected_orm_classes:
                logger.debug(f"Injecting {orm_class.__name__} into {self.__class__.__name__}")
                self.injected_orm_classes.add(orm_class)

                methods = [
                    self._get_method(orm_class, default_method=self.get_orm),
                    self._gets_method(orm_class, default_method=self.get_orms),
                    self._create_method(orm_class, default_method=self.create_orm),
                    self._creates_method(orm_class, default_method=self.create_orms),
                    self._fields_method(orm_class, default_method=self.get_orm_fields),
                ]

                for method_name, method in methods:
                    method = functools.partial(method, orm_class=orm_class)
                    setattr(self, method_name, method)
                    setattr(testdata, method_name, method)

#                     if method:
#                         method = functools.partial(method, orm_class=orm_class)
#                         setattr(self, method_name, method)
#                         setattr(testdata, method_name, method)
# 
#                     else:
#                         logger.debug(f"Creating {self.__class__.__name__}.{method_name}")
#                         method = functools.partial(self.get_orm_fields, orm_class=orm_class)
#                         setattr(self, method_name, method)
#                         setattr(testdata, method_name, method)







#                 class_name = orm_class.__name__.lower()
# 
#                 method_name = f"create_{class_name}"
#                 if not hasattr(self, method_name):
#                     logger.debug(f"Creating {self.__class__.__name__}.{method_name}")
#                     method = functools.partial(self.create_orm, orm_class=orm_class)
#                     setattr(self, method_name, method)
#                     setattr(testdata, method_name, method)
# 
#                 method_name = f"get_{class_name}"
#                 if not hasattr(self, method_name):
#                     logger.debug(f"Creating {self.__class__.__name__}.{method_name}")
#                     #method = functools.partialmethod(self.get_orm, orm_class=orm_class)
#                     method = functools.partial(self.get_orm, orm_class=orm_class)
#                     setattr(self, method_name, method)
#                     setattr(testdata, method_name, method)
#                     #setattr(testdata, method_name, getattr(self, method_name))
# 
#                 method_name = f"get_{class_name}_fields"
#                 if method := getattr(self, method_name, None):
#                     method = functools.partial(method, orm_class=orm_class)
#                     setattr(self, method_name, method)
#                     setattr(testdata, method_name, method)
# 
#                 else:
#                     logger.debug(f"Creating {self.__class__.__name__}.{method_name}")
#                     method = functools.partial(self.get_orm_fields, orm_class=orm_class)
#                     setattr(self, method_name, method)
#                     setattr(testdata, method_name, method)


    def _fields(self, orm_class, **kwargs):
        """Internal dispatcher method for get_orm_fields, this will first try and find
        a get_<ORM-NAME>_fields method and fallback to get_orm_fields"""
        method_name, method = self._fields_method(
            orm_class,
            default_method=self.get_orm_fields,
        )
        kwargs.setdefault("orm_class", orm_class)
        return method(**kwargs)

    def _get(self, orm_class, **kwargs):
        """Internal dispatcher method for get_orm, this will first try and find
        a get_<ORM-NAME> method and fallback to get_orm"""
        method_name, method = self._get_method(
            orm_class,
            default_method=self.get_orm,
        )
        kwargs.setdefault("orm_class", orm_class)
        return method(**kwargs)

    def _gets(self, orm_class, **kwargs):
        """Internal dispatcher method for get_orms, this will first try and find
        a get_<ORM-MODELS-NAME> method and fallback to get_orms"""
        method_name, method = self._gets_method(
            orm_class,
            default_method=self.get_orms,
        )
        kwargs.setdefault("orm_class", orm_class)
        return method(**kwargs)

    def _create(self, orm_class, **kwargs):
        """Internal dispatcher method for create_orm, this will first try and find
        a create_<ORM-NAME> method and fallback to create_orm"""
        method_name, method = self._create_method(
            orm_class,
            default_method=self.create_orm,
        )
        kwargs.setdefault("orm_class", orm_class)
        return method(**kwargs)

    def _creates(self, orm_class, **kwargs):
        """Internal dispatcher method for create_orms, this will first try and find
        a create_<ORM-MODELS-NAME> method and fallback to create_orm"""
        method_name, method = self._creates_method(
            orm_class,
            default_method=self.create_orms,
        )
        kwargs.setdefault("orm_class", orm_class)
        return method(**kwargs)

    def unsafe_delete_db(self):
        if self.ensure_safe_env(): # this method needs to be defined in project code
            for inter in get_interfaces().values():
                inter.unsafe_delete_tables()

    def assure_orm_field_names(self, orm_class, **kwargs):
        schema = orm_class.schema
        # normalize passed in field names to make sure we correctly find the field's
        # value if it exists
        for field_name in list(kwargs.keys()):
            if fn := schema.field_name(field_name):
                kwargs[fn] = kwargs.pop(field_name)

        return kwargs

    def assure_orm_refs(self, orm_class, **kwargs):
        kwargs = self.assure_orm_field_names(orm_class, **kwargs)
        require_refs = kwargs.get("require_refs", True)

        for field_name, field in orm_class.schema.fields.items():
            if ref_class := field.ref:
                if require_refs or field.is_required() or self.yes():
                    ref_field_name = ref_class.model_name

                    if field_name in kwargs:
                        if ref_field_name not in kwargs:
                            kwargs[ref_field_name] = ref_class.query.eq_pk(kwargs[field_name]).one()

                    else:
                        if ref_field_name in kwargs:
                            kwargs[field_name] = kwargs[ref_field_name].pk

                        else:
                            kwargs[ref_field_name] = self._create(ref_class)
                            kwargs[field_name] = kwargs[ref_field_name].pk

        return kwargs

#         for field_name, field_value in kwargs.values():
#             if isinstance(, orm_class):
#                 pk = fv.pk
#                 if pk:
#                     ret[field_name] = fv.pk
#                     break

    def create_orm(self, orm_class, **kwargs):
        instance = self._get(orm_class, **kwargs)

#         kwargs.setdefault("orm_class", orm_class)
#         method_name, method = self._get_get_method(
#             orm_class,
#             default_method=self.get_orm,
#         )
#         instance = method(**kwargs)

#         if method:
#             instance = method(**kwargs)
#         else:
#             kwargs.setdefault("orm_class", orm_class)
#             instance = self.create_orm(**kwargs)
        instance.save()
        return instance

    def create_orms(self, orm_class, **kwargs):
        instances = self._gets(orm_class, **kwargs)
        for instance in instances:
            instance.save()
        return instances

    def get_orm(self, orm_class, **kwargs):
#         if cb := getattr(orm_class, "testdata", None):
#             instance = cb(self, **kwargs)
# 
#         else:
        fields = self._fields(orm_class, **kwargs)
#             kwargs.setdefault("orm_class", orm_class)
#             method_name, method = self._get_fields_method(
#                 orm_class,
#                 default_method=self.get_orm_fields,
#             )
#             if method:
#                 fields = method(**kwargs)
#             else:
#                 kwargs.setdefault("orm_class", orm_class)
#                 fields = self.create_orm(**kwargs)
#             fields = method(**kwargs)
        instance = orm_class(fields)

        return instance

    def get_orms(self, orm_class, **kwargs):
        ret = []
        orm_field_name = orm_class.model_name
        if kwargs.get("related_refs", True):
            kwargs = self.assure_orm_refs(orm_class, **kwargs)

        count = kwargs.get(f"{orm_field_name}_count", kwargs.get("count", 1))
        for _ in range(count):
            ret = self._get(orm_class, **kwargs)

        return ret

    def get_orm_fields(self, orm_class, **kwargs):
        ret = {}
        schema = orm_class.schema
        kwargs = self.assure_orm_refs(orm_class, **kwargs)

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

#                 if field_name not in ret:
#                     # check all the kwargs for an instance of the needed orm_class
#                     orm_class = field.ref
#                     for fv in kwargs.values():
#                         if isinstance(fv, orm_class):
#                             pk = fv.pk
#                             if pk:
#                                 ret[field_name] = fv.pk
#                                 break
# 
#                     if field_name not in ret and field.is_required():
#                         ret[field_name] = self._create(orm_class)

            else:
                has_value = field.is_required() or self.yes()
                if has_value:
                    field_type = field.interface_type

                    if field.choices:
                        ret[field_name] = self.choice(field.choices)

                    else:
                        if cb := getattr(field, "testdata", None):
                            ret[field_name] = cb(self)

                        else:
                            if issubclass(field_type, int):
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

                            elif issubclass(field_type, bool):
                                ret[field_name] = self.yes()

                            elif issubclass(field_type, dict):
                                ret[field_name] = self.get_dict()

                            elif issubclass(field_type, float):
                                ret[field_name] = self.get_posfloat(size_info["size"])

                            elif issubclass(field_type, datetime.datetime):
                                ret[field_name] = self.get_past_datetime()

                            elif issubclass(field_type, datetime.date):
                                ret[field_name] = self.get_past_date()

                            else:
                                raise ValueError(f"Not sure what to do with {field.type}")

#         pick up any alias field names in kwargs
#         for field_name, field_value in kwargs.items():
#             if schema.has_field(field_name):
#                 ret[schema.field_name(field_name)] = field_value

        return ret

