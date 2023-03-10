# -*- coding: utf-8 -*-
from __future__ import unicode_literals, division, print_function, absolute_import

class Error(Exception):
    """all prom errors will inherit from this base class"""
    def __init__(self, e, exc_info=None):
        self.e = e
        self.exc_info = exc_info
        super(Error, self).__init__(str(e))


class InterfaceError(Error):
    """specifically for wrapping SQLite and Postgres errors

    see Interface.create_error() for how InterfaceError instances are created
    """
    def unwrapped_e(self):
        """Find the first unwrapped error (ie, unwind to the original error)"""
        e = self
        # unwind to the original error
        while isinstance(e, InterfaceError):
            e = e.e
        return e

    def __str__(self):
        """Postgres returns multi-line errors, this switches to just return the
        first line since the other lines aren't usually helpful, if you want the
        full message just call .unwrapped_e()"""
        s = super().__str__()
        return s.splitlines()[0]


class TableError(InterfaceError):
    pass


class FieldError(InterfaceError):
    pass


class UniqueError(FieldError):
    pass


class CloseError(InterfaceError):
    pass

