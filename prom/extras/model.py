# -*- coding: utf-8 -*-

from datatypes import classproperty

from ..model import Orm


class MagicOrm(Orm):
    """Extends the default Orm with some more syntactic sugar

    https://github.com/Jaymon/prom/issues/78
    """
    def __pout__(self):
        """This just makes the object easier to digest in pout.v() calls

        more information on what pout is: https://github.com/Jaymon/pout
        """
        return self.fields

    @classproperty
    def pk_name(cls):
        """return the preferred primary key name for the Orm

        you can access the primary key on an instance using ._id or .pk but
        usually when we have foreign keys we use modelname_id and then in the
        jsonable dicts that are passed down we include that modelname_id in the
        actual instance instead of an _id field (eg, the jsonable on object Foo
        would have 'foo_id' key) and this makes it easy to get that key name and
        override it if needed

        :returns: string, the preferred primary key name that is usually the
            name that should be used in jsonable and in foreign key fields on
            other models
        """
        return f"{cls.model_name}_id"

    def __getattr__(self, k):
        """Adds some syntactic sugar to the Orm

        adds support for:
            .<MODEL-NAME>_id = alias for self.pk or self._id
            .is_fieldname() = if fieldname is a boolean then returns True/False,
                if fieldname is another value then you can do .is_fieldname(val)
                to compare val to the fieldname's value
        """
        if k.startswith("is_"):
            field_name = k[3:]
            field = self.schema.fields[field_name]
            if issubclass(field.type, bool):
                ret = lambda: getattr(self, field_name)

            else:
                ret = lambda x: x == getattr(self, field_name)

        elif k == self.pk_name:
            ret = self.pk

        else:
            ret = super().__getattr__(k)

        return ret

    def jsonable(self):
        """Switches out _id for pk_name"""
        pk_fields = self.schema.pk_fields
        if len(pk_fields) == 1:
            for pk_field in pk_fields.values():
                pk_field.options.setdefault("jsonable_name", self.pk_name)

        return super().jsonable()

