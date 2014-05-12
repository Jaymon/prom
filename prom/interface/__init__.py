
interfaces = {}
"""holds all the configured interfaces"""


def get_interfaces():
    return interfaces


def set_interface(interface, name=''):
    """
    don't want to bother with a dsn? Use this method to make an interface available
    """
    if not interface: raise ValueError('interface is empty')

    global interfaces
    interfaces[name] = interface


def get_interface(name=''):
    """
    get an interface that was created using configure()

    name -- string -- the name of the connection for the interface to return
    """
    global interfaces
    return interfaces[name]

