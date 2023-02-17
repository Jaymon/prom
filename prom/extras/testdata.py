# -*- coding: utf-8 -*-

from testdata.base import TestData

#from ..model import Orm
from ..interface import get_interfaces


class OrmTestDataMixin(object):
    data_instance = None

    def __init_subclass__(cls):
        super().__init_subclass__()

        # find the OrmData instance
        for instance in TestData.data_instances.values():
            if isinstance(instance, OrmData):
                cls.data_instance = instance
                break

        testdata = TestData.module()
        class_name = cls.__name__.lower()
        create_name = f"create_{class_name}"
        #create_lambda = lambda **kwargs: cls.data_instance.create_orm(orm_class=cls, **kwargs)
        setattr(testdata, create_name, cls.testdata_create_method)
        setattr(cls.data_instance, create_name, cls.testdata_create_method)

        get_name = f"get_{class_name}"
        #get_lambda = lambda **kwargs: cls.data_instance.get_orm(orm_class=cls, **kwargs)
        setattr(testdata, get_name, cls.testdata_get_method)
        setattr(cls.data_instance, get_name, cls.testdata_get_method)

        fields_name = f"get_{class_name}_fields"
        #get_lambda = lambda **kwargs: cls.data_instance.get_orm_fields(schema=cls.schema, **kwargs)
        if not hasattr(testdata, fields_name):
            setattr(testdata, fields_name, cls.testdata_fields_method)
        if not hasattr(testdata, fields_name):
            setattr(cls.data_instance, fields_name, cls.testdata_fields_method)

    @classmethod
    def testdata_fields(cls, data_instance, **kwargs):
        return data_instance.get_orm_fields(cls.schema, **kwargs)

    @classmethod
    def testdata(cls, data_instance, **kwargs):
        fields = cls.testdata_fields(data_instance, **kwargs)
        return cls(fields)

    @classmethod
    def testdata_fields_method(cls, *args, **kwargs):
        return cls.testdata_fields(cls.data_instance, **kwargs)

    @classmethod
    def testdata_get_method(cls, **kwargs):
        return cls.testdata(cls.data_instance, **kwargs)

    @classmethod
    def testdata_create_method(cls, **kwargs):
        instance = cls.testdata(cls.data_instance, **kwargs)
        instance.save()
        return instance

#     @classmethod
#     def testdata_create(cls, **kwargs):
#         fields = cls.testdata_fields(**kwargs)
#         return cls.create(fields)


class OrmData(TestData):
    def unsafe_delete_db(self):
        if self.ensure_safe_env(): # this method needs to be defined in project code
            for inter in get_interfaces().values():
                inter.unsafe_delete_tables()

    def get_orm(self, orm_class, **kwargs):
        if cb := getattr(orm_class, "testdata", None):
            instance = cb(self, **kwargs)

        else:
            fields = self.get_orm_fields(orm_class.schema, **kwargs)
            instance = cls(fields)

        return instance

    def create_orm(self, orm_class, **kwargs):
        instance = self.get_orm(orm_class=orm_class, **kwargs)
        instance.save()
        return instance

    def get_orm_fields(self, schema, **kwargs):
        ret = {}
        for field_name, field in schema.fields.items():
            if field.is_pk():
                # we will assume the primary key is auto-generating unless it's not
                # required, then we will need a value
                if not field.is_required():
                    ret[field_name] = kwargs.pop(field_name)

            elif field.is_ref():
                if field.is_required():
                    ret[field_name] = kwargs.pop(field_name)

                else:
                    if value := kwargs.pop(field_name, None):
                        ret[field_name] = value

            elif field.is_auto():
                # db will handle any auto-generating fields
                pass

            else:
                has_value = field.is_required() or self.yes()
                if has_value:
                    if field.choices:
                        ret[field_name] = self.choice(field.choices)

                    else:
                        if cb := getattr(field, "testdata", None):
                            ret[field_name] = cb(self)

                        else:
                            if issubclass(field.type, int):
                                size_info = field.size_info()
                                ret[field_name] = self.get_posint(size_info["size"])

                            elif issubclass(field.type, str):
                                size_info = field.size_info()
                                if "bounds" in size_info:
                                    ret[field_name] = self.get_words(
                                        min_size=size_info["bounds"][0],
                                        max_size=size_info["bounds"][1],
                                    )
                                else:
                                    ret[field_name] = self.get_words()

                            elif issubclass(field.type, bool):
                                ret[field_name] = self.yes()

                            elif issubclass(field.type, dict):
                                ret[field_name] = self.get_dict()

                            elif issubclass(field.type, float):
                                ret[field_name] = self.get_posfloat(size_info["size"])

                            else:
                                raise ValueError(f"Not sure what to do with {field.type}")

        for field_name, field_value in kwargs.items():
            if schema.has_field(field_name):
                ret[schema.field_name(field_name)] = field_value

        return ret

