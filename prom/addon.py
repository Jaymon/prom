# -*- coding: utf-8 -*-
from __future__ import unicode_literals, division, print_function, absolute_import

from . import decorators
from .model import Orm
from .config import (
    Field,
    ObjectField,
    JsonField,
    Index
)


class MagicOrm(Orm):
    """Extends the default Orm with some more syntactic sugar

    https://github.com/Jaymon/prom/issues/78
    """

    # These just make it easier to define fields and indexes by just importing
    # MagicOrm
    Field = Field
    ObjectField = ObjectField
    JsonField = JsonField
    Index = Index

    def __pout__(self):
        """This just makes the object easier to digest in pout.v() calls

        more information on what pout is: https://github.com/Jaymon/pout
        """
        return self.fields

    @decorators.classproperty
    def pk_name(cls):
        """return the preferred primary key name for the Orm

        you can access the primary key on an instance using ._id or .pk but usually
        when we have foreign keys we use modelname_id and then in the jsonable
        dicts that are passed down we include that modelname_id in the actual instance
        instead of an _id field (eg, the jsonable on object Foo would have 'foo_id' key)
        and this makes it easy to get that key name and override it if needed

        :returns: string, the preferred primary key name that is usually the name
            that should be used in jsonable and in foreign key fields on other
            models
        """
        return "{}_id".format(cls.__name__.lower())

    def __getattr__(self, k):
        """Adds some syntactic sugar to the Orm

        adds support for:
            .ormname_id = alias for self.pk or self._id
            .is_fieldname() = if fieldname is a boolean then returns True/False,
                if fieldname is another value then you can do .is_fieldname(val)
                to compare val to the fieldname's value
            .fk = if you have a fieldname like other_id or other_fk that contains
                the pk value for other orm then you can just omit the _id suffix
                and it will fetch the actual instance from the other orm (eg, if
                you have .foo_id and do .foo it will return the Foo instance with
                instance.pk == self.foo_id)
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
            raise_error = True
            field_name = "{}_id".format(k)
            field = self.schema.fields.get(field_name, None)
            if not field:
                field_name = "{}_fk".format(k)
                field = self.schema.fields.get(field_name, None)

            if field:
                # NOTE -- another way to do this might be to just use field._type
                # and pass that into self.query.ref(field._type)
                schema = field.schema
                if schema:
                    orm_class = schema.orm_class
                    if orm_class:
                        raise_error = False
                        ret = orm_class.query.get_pk(getattr(self, field_name))

            if raise_error:
                raise AttributeError(k)

        return ret

    def jsonable(self):
        """Switches out _id for pk_name"""
        d = super(MagicOrm, self).jsonable()
        d[self.pk_name] = self.pk
        d.pop("_id", None)
        return d

