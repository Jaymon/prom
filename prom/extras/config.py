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
    @property
    def type(self):
        return int if self.is_enum() else super(Field, self).type

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

