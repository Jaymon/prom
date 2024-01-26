# -*- coding: utf-8 -*-


class Error(Exception):
    """all prom errors will inherit from this base class

    :param e: Exception, the exception instance that is getting wrapped
    :param **kwargs:
        - message: by default it will use str(e) but you can customize it by
          passing in "message", you can see in the source code that message is
          usually the first argument, it doesn't have a name, it's just args[0],
          https://github.com/python/cpython/blob/3.11/Objects/exceptions.c
    """
    def __init__(self, e, **kwargs):
        self.e = e
        super(Error, self).__init__(kwargs.get("message", str(e)))

    def unwrapped_e(self):
        """Find the first unwrapped error (ie, unwind to the original error)"""
        e = self
        # unwind to the original error
        while isinstance(e, InterfaceError):
            e = e.e
        return e


class InterfaceError(Error):
    """specifically for wrapping SQLite and Postgres errors

    see Interface.create_error() for how InterfaceError instances are created
    """
    def __str__(self):
        """Postgres returns multi-line errors, this switches to just return the
        first line since the other lines aren't usually helpful, if you want the
        full message just call .unwrapped_e()"""
        s = super().__str__()
        lines = s.splitlines()
        return lines[0] if lines else ""


class TableError(InterfaceError):
    pass


class FieldError(InterfaceError):
    pass


class UniqueError(FieldError):
    pass


class CloseError(InterfaceError):
    pass


class PlaceholderError(InterfaceError):
    """This is raised when there is a raw query mismatch between the
    placeholders and the passed in arguments, see Interface._raw

    https://github.com/Jaymon/prom/issues/74
    """
    pass

