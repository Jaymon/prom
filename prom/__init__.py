# -*- coding: utf-8 -*-
from __future__ import unicode_literals, division, print_function, absolute_import
import os
import logging

import dsnparse

from .config import DsnConnection, \
    Schema, \
    Field, \
    ObjectField, \
    JsonField, \
    Index
from .query import Query, CacheQuery
from . import decorators
from .model import Orm
from .interface import get_interface, set_interface, get_interfaces
from .exception import InterfaceError, Error, UniqueError
from . import utils


__version__ = '0.9.107'


# get rid of "No handler found" warnings (cribbed from requests)
logging.getLogger(__name__).addHandler(logging.NullHandler())


def configure_environ(dsn_env_name='PROM_DSN', connection_class=DsnConnection):
    """
    configure interfaces based on environment variables

    by default, when prom is imported, it will look for PROM_DSN, and PROM_DSN_N (where
    N is 1 through infinity) in the environment, if it finds them, it will assume they
    are dsn urls that prom understands and will configure db connections with them. If you
    don't want this behavior (ie, you want to configure prom manually) then just make sure
    you don't have any environment variables with matching names

    The num checks (eg PROM_DSN_1, PROM_DSN_2) go in order, so you can't do PROM_DSN_1, PROM_DSN_3,
    because it will fail on _2 and move on, so make sure your num dsns are in order (eg, 1, 2, 3, ...)

    example --
        export PROM_DSN_1=some.Interface://host:port/dbname#i1
        export PROM_DSN_2=some.Interface://host2:port/dbname2#i2
        $ python
        >>> import prom
        >>> print prom.interfaces # prints a dict with interfaces i1 and i2 keys

    :param dsn_env_name: string, the name of the environment variables
    """
    inters = []
    cs = dsnparse.parse_environs(dsn_env_name, parse_class=connection_class)
    for c in cs:
        inter = c.interface
        set_interface(inter, c.name)
        inters.append(inter)

    return inters


def configure(dsn, connection_class=DsnConnection):
    """
    configure an interface to be used to query a backend

    you use this function to configure an Interface using a dsn, then you can get
    that interface using the get_interface() method

    dsn -- string -- a properly formatted prom dsn, see DsnConnection for how to format the dsn
    """
    c = dsnparse.parse(dsn, parse_class=connection_class)
    inter = c.interface
    set_interface(inter, c.name)
    return inter


configure_environ()

