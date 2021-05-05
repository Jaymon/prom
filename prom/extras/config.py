# -*- coding: utf-8 -*-
from __future__ import unicode_literals, division, print_function, absolute_import

from datatypes import Enum

from ..config import (
    Field as BaseField,
)


class Field(BaseField):
    """Adds support for Enum as the field type. This allows you to set an enum string
    value, integer value, or Enum instance to the Orm or the Query and it will 
    be inserted into the db as an integer"""
    def is_enum(self):
        """Return True if the field type is an Enum"""
        try:
            ret = issubclass(self.original_type, Enum)
        except TypeError:
            ret = False
        return ret

    def fset(self, orm, val):
        if val is not None and self.is_enum():
            val = self.original_type.find_value(val)
        else:
            val = super(Field, self).fset(orm, val)
        return val

    def iquery(self, query, val):
        if val is not None and self.is_enum():
            val = self.original_type.find_value(val)
        else:
            val = super(Field, self).iquery(query, val)
        return val

    def set_type(self, field_type):
        super(Field, self).set_type(field_type)
        if self.is_enum():
            self.serializer = ""
            self._interface_type = int

