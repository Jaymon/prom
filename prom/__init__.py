# stdlib
import os
import logging

# first party
from .config import DsnConnection, Schema, Field, Index
from .query import Query, CacheQuery
from . import decorators
from .model import Orm
from .interface import get_interface, set_interface, get_interfaces
from .utils import get_objects
from .exception import InterfaceError, Error


__version__ = '0.9.87'


# get rid of "No handler found" warnings (cribbed from requests)
logging.getLogger(__name__).addHandler(logging.NullHandler())


def configure_environ(dsn_env_name='PROM_DSN'):
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

    dsn_env_name -- string -- the name of the environment variables
    """
    if dsn_env_name in os.environ:
        configure(os.environ[dsn_env_name])

    # now try importing 1 -> N dsns
    increment_name = lambda name, num: '{}_{}'.format(name, num)
    dsn_num = 1
    dsn_env_num_name = increment_name(dsn_env_name, dsn_num)
    if dsn_env_num_name in os.environ:
        try:
            while True:
                configure(os.environ[dsn_env_num_name])
                dsn_num += 1
                dsn_env_num_name = increment_name(dsn_env_name, dsn_num)

        except KeyError:
            pass


def configure(dsn):
    """
    configure an interface to be used to query a backend

    you use this function to configure an Interface using a dsn, then you can get
    that interface using the get_interface() method

    dsn -- string -- a properly formatted prom dsn, see DsnConnection for how to format the dsn
    """
    #global interfaces

    c = DsnConnection(dsn)
    if c.name in get_interfaces():
        raise ValueError('a connection named "{}" has already been configured'.format(c.name))

    interface_module, interface_class = get_objects(c.interface_name)
    i = interface_class(c)
    set_interface(i, c.name)
    return i


configure_environ()

