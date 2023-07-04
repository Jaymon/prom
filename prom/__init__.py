# -*- coding: utf-8 -*-
from __future__ import unicode_literals, division, print_function, absolute_import
import os
import logging

from .config import (
    DsnConnection,
    Schema,
    Field,
    Index
)
from .query import Query, Iterator
from .model import Orm
from .interface import (
    get_interface,
    set_interface,
    get_interfaces,
    configure,
    configure_environ
)
from .exception import (
    Error,
    InterfaceError,
    UniqueError,
    TableError,
    FieldError,
    UniqueError,
    CloseError,
)

from . import utils


__version__ = '4.7.0'


def transaction(connection_name="", **kwargs):
    """Create a transaction 

    Sometimes you just need to batch a whole bunch of operation across a whole bunch
    of different models, this allows you to create a transaction outside of any
    of the models so you can do that, it will yield a connection you can then pass
    into the Orm/Query methods

    :Example:
        with prom.transaction(prefix="batch") as conn:
            o = FooOrm(foo=1)
            o.save(connection=conn)

    :param connection_name: str, the connection name corresponding to the anchor
        of the DSN, defaults to the default "" connection
    :param **kwargs: passed through to the Interface.transaction context manager
        * prefix: str, the name of the transaction you want to use
        * nest: bool, True if you want nested transactions to be created, False
            to ignore nested transactions
    :returns: Connection instance
    """
    kwargs.setdefault("nest", False)
    kwargs.setdefault("prefix", f"prom_{connection_name}_tx")
    return get_interface(connection_name).transaction(**kwargs)

