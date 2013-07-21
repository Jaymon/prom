import calendar
import datetime

# first party
from ..query import Query

class Interface(object):

    connected = False
    """true if a connection has been established, false otherwise"""

    connection = None
    """hold the actual raw connection to the db"""

    connection_config = None
    """a config.Connection() instance"""

    transaction = False
    """true if currently in a transaction, false if not in a transaction, see transaction_start(), transaction_stop()"""

    def __init__(self, connection_config=None):
        self.connection_config = connection_config

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

    def assure(self, schema=None):
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

    def transaction_start(self):
        """
        start a transaction

        this should set self.transaction to true, this takes transaction management into your
        hands, so you will have to call transaction_stop() to actually commit the changes, this
        is indempotent, multiple calls are ignored if already in a transaction
        """
        if self.transaction: return True

        self._transaction_start()
        self.transaction = True
        return True

    def _transaction_start(self):
        raise NotImplementedError("this needs to be implemented in a child class")

    def transaction_stop(self):
        """
        stop/commit a transaction

        this should set self.transaction to False
        """
        if not self.transaction: return True

        self._transaction_stop()
        self.transaction = False
        return True

    def _transaction_stop(self):
        raise NotImplementedError("this needs to be implemented in a child class")

    def transaction_fail(self, e=None):
        """
        rollback a transaction
        """
        if self.transaction: 
            self._transaction_fail(e)
            self.transaction = False

        if not e:
            return True
        else:
            raise e

    def _transaction_fail(self, e=None):
        raise NotImplementedError("this needs to be implemented in a child class")

    def set_table(self, schema):
        """
        add the table to the db

        schema -- Schema() -- contains all the information about the table
        """
        self.assure(schema)
        if self.has_table(schema.table): return True

        try:
            self.transaction_start()

            self._set_table(schema)

            for index_name, index_d in schema.indexes.iteritems():
                self.set_index(schema, **index_d)

            self.transaction_stop()

        except Exception, e:
            self.transaction_fail(e)

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

    def delete_table(self, schema):
        """
        remove a table matching schema from the db

        schema -- Schema()
        """
        self.assure(schema)
        if not self.has_table(schema.table): return True

        try:
            self.transaction_start()
            self._delete_table(schema)
            self.transaction_stop()
        except Exception, e:
            self.transaction_fail(e)

        return True

    def _delete_table(self, schema):
        raise NotImplementedError("this needs to be implemented in a child class")

    def get_indexes(self, schema):
        """
        get all the indexes

        schema -- Schema()

        return -- dict -- the indexes in {indexname: fields} format
        """
        self.assure(schema)

        return self._get_indexes(schema)

    def _get_indexes(self, schema):
        raise NotImplementedError("this needs to be implemented in a child class")

    def set_index(self, schema, name, fields, unique=False):
        """
        add an index to the table

        schema -- Schema()
        name -- string -- the name of the index
        fields -- array -- the fields the index should be on
        unique -- boolean -- true if this is a unique index
        """
        self.assure()
        try:
            self.transaction_start()
            self._set_index(schema, name, fields, unique)
            self.transaction_stop()
        except Exception, e:
            self.transaction_fail(e)

        return True
    
    def _set_index(self, schema, name, fields, unique=False):
        raise NotImplementedError("this needs to be implemented in a child class")

    def prepare_dict(self, schema, d):
        if not d: raise ValueError('no point in preparing an empty dict')

        # update the times
        # http://crazytechthoughts.blogspot.com/2012/02/get-current-utc-timestamp-in-python.html
        now = calendar.timegm(datetime.datetime.utcnow().utctimetuple())
        if schema._created not in d:
            d[schema._created] = now
        d[schema._updated] = now

        return d

    def insert(self, schema, d):
        """
        Persist d into the db

        schema -- Schema()
        d -- dict -- the values to persist

        return -- dict -- the dict that was inserted into the db
        """
        self.assure(schema)
        d = self.prepare_dict(schema, d)

        try:
            self.transaction_start()
            r = self._insert(schema, d)
            d[schema._id] = r
            self.transaction_stop()

        except Exception, e:
            self.transaction_fail(e)

        return d

    def _insert(self, schema, d):
        """
        return -- id -- the _id value
        """
        raise NotImplementedError("this needs to be implemented in a child class")

    def update(self, schema, query):
        """
        Persist the query.fields into the db that match query.fields_where

        schema -- Schema()
        query -- Query() -- will be used to create the where clause

        return -- dict -- the dict that was inserted into the db
        """
        self.assure(schema)
        d = query.fields
        d = self.prepare_dict(schema, d)

        try:
            self.transaction_start()
            r = self._update(schema, query, d)
            self.transaction_stop()

        except Exception, e:
            self.transaction_fail(e)

        return d

    def _update(self, schema, query, d):
        raise NotImplementedError("this needs to be implemented in a child class")

    def set(self, schema, query):
        """
        set d into the db, this is just a convenience method that will call either insert
        or update depending on if query has a where clause

        schema -- Schema()
        query -- Query() -- set a where clause to perform an update, insert otherwise
        return -- dict -- the dict inserted into the db
        """
        try:
            if query.fields_where:
                d = self.update(schema, query)

            else:
                # insert
                d = query.fields
                d = self.insert(schema, d)

        except Exception, e:
            if self.handle_error(schema, e):
                d = self.set(schema, query)
            else:
                raise

        return d

    def _get_query(self, callback, schema, query=None, *args, **kwargs):
        """
        this is just a common wrapper around all the get queries since they are
        all really similar in how they execute
        """
        if not query: query = Query()

        self.assure(schema)
        ret = None

        try:
            ret = callback(schema, query, *args, **kwargs)

        except Exception, e:
            # TODO handle exceptions were we should add the table or field and stuff
            raise

        return ret

    def get_one(self, schema, query=None):
        """
        get one row from the db matching filters set in query

        schema -- Schema()
        query -- Query()

        return -- dict -- the matching row
        """
        ret = self._get_query(self._get_one, schema, query)
        if not ret: ret = {}
        return ret

    def _get_one(self, schema, query):
        raise NotImplementedError("this needs to be implemented in a child class")

    def get(self, schema, query=None):
        """
        get matching rows from the db matching filters set in query

        schema -- Schema()
        query -- Query()

        return -- list -- a list of matching dicts
        """
        ret = self._get_query(self._get, schema, query)
        if not ret: ret = []
        return ret

    def _get(self, schema, query):
        raise NotImplementedError("this needs to be implemented in a child class")

    def count(self, schema, query=None):
        ret = self._get_query(self._count, schema, query)
        return int(ret)

    def _count(self, schema, query):
        raise NotImplementedError("this needs to be implemented in a child class")

    def delete(self, schema, query):
        if not query or not query.fields_where:
            raise ValueError('aborting delete because there is no where clause')

        try:
            self.transaction_start()
            ret = self._get_query(self._delete, schema, query)
            self.transaction_stop()

        except Exception, e:
            self.transaction_fail(e)

        return ret

    def _delete(self, schema, query):
        raise NotImplementedError("this needs to be implemented in a child class")

    def handle_error(self, schema, e):
        """
        try and handle the error, return False if the error can't be handled

        TODO -- this method is really general, maybe change this so there are a couple other methods
        like isTableError() and isFieldError() that the child needs to flesh out, maybe someday

        return -- boolean -- True if the error was handled, False if it wasn't
        """
        return False



