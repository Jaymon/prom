# -*- coding: utf-8 -*-
from __future__ import unicode_literals, division, print_function, absolute_import
import os
import logging

from .config import (
    DsnConnection,
    Schema,
    Field,
    ObjectField,
    JsonField,
    Index
)
from .query import Query, CacheQuery
from . import decorators
from .model import Orm
from .interface import (
    get_interface,
    set_interface,
    get_interfaces,
    configure,
    configure_environ
)
from .exception import InterfaceError, Error, UniqueError
from . import utils


__version__ = '1.4.3'


# get rid of "No handler found" warnings (cribbed from requests)
logging.getLogger(__name__).addHandler(logging.NullHandler())

