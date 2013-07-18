# stdlib
import importlib

# first party
from .config import DsnConnection

__version__ = '0.2'

_interfaces = {}
"""holds all the configured interfaces"""

def configure(dsn):
    """
    configure an interface to be used to query a backend

    you use this function to configure an Interface using a dsn, then you can get
    that interface using the get_interface() method

    dsn -- string -- a properly formatted prom dsn, see DsnConnection for how to format the dsn
    """
    c = DsnConnection(dsn)
    if c.name in _interfaces:
        raise ValueError('a connection named "{}" has already been configured'.format(c.name))

    interface_module_name, interface_class_name = c.interface_name.rsplit('.', 1)
    interface_module = importlib.import_module(interface_module_name)
    interface_class = getattr(interface_module, interface_class_name)

    i = interface_class(c)
    set_interface(i, c.name)
    return i

def set_interface(interface, name=''):
    """
    don't want to bother with a dsn? Use this method to make an interface available
    """
    if not interface: raise ValueError('interface is empty')
    _interfaces[name] = interface

def get_interface(name=''):
    """
    get an interface that was created using configure()

    name -- string -- the name of the connection for the interface to return
    """
    return _interfaces[name]


