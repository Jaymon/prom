from urlparse import urlparse
import re

def get():
    # todo, implement this to return the right connection for the child_class
    # connection = prom.connection.get(child_klass)
    pass

def set():
    # todo, set a connection for a child orm class using interface
#prom.connection.set(
#    orm=klass,
#    interface=Interface,
#    dbname="",
#    username="",
#    password=""
#    host="",
#    port=""
#)
    pass

class Config(object):
    """
    the paramaters to use to connect to an interface

    https://github.com/Jaymon/Mingo/blob/master/Mingo/MingoConfig.php
    """

    database = ""
    """the db name to use, in postgres, this is the database name"""

    port = 0
    """the host port"""

    username = ""
    """the username to use to to connect to the db"""

    password = ""
    """the password for the username"""

    options = None
    """any other db options, these can be interface implementation specific"""

    debug = False
    """true to turn on debugging for this connection"""

    @property
    def host(self):
        """the db host"""
        return self._host

    @host.setter
    def host(self, h):

        # normalize the host so urlparse can parse it correctly
        # http://stackoverflow.com/questions/9530950/parsing-hostname-and-port-from-string-or-url#comment12075005_9531210
        if not re.match(ur'(?:\S+|^)\/\/', h):
            h = "//{}".format(h)

        o = urlparse(h)

        self._host = o.hostname
        if o.port: self.port = o.port

