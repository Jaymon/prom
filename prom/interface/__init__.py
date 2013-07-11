
class Interface(object):

    connected = False
    """true if a connection has been established, false otherwise"""

    connection = None
    """hold the actual raw connection to the db"""

    connection_config = None
    """a connection.Params() instance"""

    def connect(self, connection_config=None, *args, **kwargs):
        """
        connect to the interface

        this will set the raw db connection to self.connection

        *args -- anything you want that will help the db connect
        **kwargs -- anything you want that the backend db connection will need to actually connect
        """

        if self.connected: return self.connected

        if connection_config: self.connection_config = connection_config

        self.connected = False
        self._connect(self.connection_config)
        self.connected = True

        return self.connected

    def _connect(self, connection_config):
        raise NotImplementedError("this needs to be implemented in a child class")

    def query(self, query_str, *query_args, **query_options):
        """
        run a raw query on the db

        query_str -- string -- the query to run
        *query_args -- if the query_str is a formatting string, pass the values in this
        **query_options -- any query options can be passed in by using key=val syntax
        """
        self.connect()
        return self._query(query_str, *query_args, **query_options)

    def _query(self, query_str, *query_args, **query_options):
        raise NotImplementedError("this needs to be implemented in a child class")
