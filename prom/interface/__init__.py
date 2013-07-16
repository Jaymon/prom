
class Interface(object):

    connected = False
    """true if a connection has been established, false otherwise"""

    connection = None
    """hold the actual raw connection to the db"""

    connection_config = None
    """a config.Connection() instance"""

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
        if self.connection:
            self.connected = True
        else:
            raise ValueError("the ._connect() method did not set .connection attribute")

        return self.connected

    def _connect(self, connection_config):
        """this *MUST* set the self.connection attribute"""
        raise NotImplementedError("this needs to be implemented in a child class")

    def close(self):
        """
        close an open connection
        """
        if not self.connected: return True

        self.connection.close()
        self.connection = None
        self.connected = False
        return True

    def assure(self):
        """handle any things that need to be done before a query can be performed"""
        self.connect()

    def query(self, query_str, *query_args, **query_options):
        """
        run a raw query on the db

        query_str -- string -- the query to run
        *query_args -- if the query_str is a formatting string, pass the values in this
        **query_options -- any query options can be passed in by using key=val syntax
        """
        self.assure()
        return self._query(query_str, query_args, **query_options)

    def _query(self, query_str, query_args=None, query_options=None):
        raise NotImplementedError("this needs to be implemented in a child class")

    def set_table(self, schema):
        """
        add the table to the db

        schema -- Schema() -- contains all the information about the table
        """
        self.assure()

        if not self.has_table(schema.table):
            self._set_table(schema)

    def _set_table(self, schema):
        raise NotImplementedError("this needs to be implemented in a child class")

    def has_table(self, table_name):
        """
        check to see if a table is in the db

        table_name -- string -- the table to check
        return -- boolean -- True if the table exists, false otherwise
        """
        self.assure()
        tables = self.get_tables(table_name)
        return len(tables) > 0

    def get_tables(self, table_name=""):
        """
        get all the tables of the currently connected db

        table_name -- string -- if you would like to filter the tables list to only include matches with this name
        return -- list -- a list of table names
        """
        self.assure()
        return self._get_tables(table_name)

    def _get_tables(self, table_name):
        raise NotImplementedError("this needs to be implemented in a child class")


