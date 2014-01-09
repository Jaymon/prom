import calendar
import datetime
import logging
from contextlib import contextmanager

# first party
from ..query import Query

logger = logging.getLogger(__name__)

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


class Interface(object):

    connected = False
    """true if a connection has been established, false otherwise"""

    connection = None
    """hold the actual raw connection to the db"""

    connection_config = None
    """a config.Connection() instance"""

    transaction_count = 0
    """
    counting semaphore, greater than 0 if in a transaction, 0 if no current current transaction.

    This will be incremented everytime transaction_start() is called, and decremented
    everytime transaction_stop() is called.

    transaction_fail will set this back to 0 and rollback the transaction
    """

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

        self.log("Connected {}", self.connection_config.interface_name)
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
        self.log("Closed Connection {}", self.connection_config.interface_name)
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

    @contextmanager
    def transaction(self):
        """
        a simple context manager useful for when you want to wrap a bunch of db calls in a transaction

        This is useful for making sure the db is connected before starting a transaction

        http://docs.python.org/2/library/contextlib.html
        http://docs.python.org/release/2.5/whatsnew/pep-343.html

        example --
            with self.transaction()
                # do a bunch of calls
            # those db calls will be committed by this line
        """
        self.assure()
        self.transaction_start()
        try:
            yield self
            self.transaction_stop()
        except Exception, e:
            self.transaction_fail(e)

    def transaction_start(self):
        """
        start a transaction

        this will increment transaction semaphore and pass it to _transaction_start()
        """
        self.transaction_count += 1
        if self.transaction_count == 1:
            self.log("Transaction started")
        else:
            self.log("Transaction incremented {}", self.transaction_count)

        self._transaction_start(self.transaction_count)
        return self.transaction_count

    def _transaction_start(self, count):
        """count = 1 is the first call, count = 2 is the second transaction call"""
        pass

    def transaction_stop(self):
        """stop/commit a transaction if ready"""
        if self.transaction_count > 0:
            if self.transaction_count == 1:
                self.log("Transaction stopped")
            else:
                self.log("Transaction decremented {}", self.transaction_count)

            self._transaction_stop(self.transaction_count)
            self.transaction_count -= 1

        return self.transaction_count

    def _transaction_stop(self, count):
        """count = 1 is the last time this will be called for current set of transactions"""
        pass

    def transaction_fail(self, e=None):
        """
        rollback a transaction if currently in one

        e -- Exception() -- if passed in, bubble up the exception by re-raising it
        """
        if self.transaction_count > 0: 
            self.log("Transaction fail")
            self._transaction_fail(self.transaction_count, e)
            self.transaction_count -= 1

        if not e:
            return True
        else:
            raise e

    def _transaction_fail(self, count, e=None):
        pass

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

    def delete_tables(self, **kwargs):
        """
        removes all the tables from the db

        this is, obviously, very bad if you didn't mean to call this, because of that, you
        have to pass in disable_protection=True, if it doesn't get that passed in, it won't
        run this method
        """
        if not kwargs.get('disable_protection', False):
            raise ValueError('In order to delete all the tables, pass in disable_protection=True')

        self._delete_tables(**kwargs)

    def _delete_tables(self, **kwargs):
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

    def set_index(self, schema, name, fields, **index_options):
        """
        add an index to the table

        schema -- Schema()
        name -- string -- the name of the index
        fields -- array -- the fields the index should be on
        **index_options -- dict -- any index options that might be useful to create the index
        """
        self.assure()
        try:
            self.transaction_start()
            self._set_index(schema, name, fields, **index_options)
            self.transaction_stop()
        except Exception, e:
            self.transaction_fail(e)

        return True
    
    def _set_index(self, schema, name, fields, **index_options):
        raise NotImplementedError("this needs to be implemented in a child class")

    def prepare_dict(self, schema, d, is_insert):
        """
        prepare the dict for insert/update

        is_insert -- boolean -- True if insert, False if update
        return -- dict -- the same dict, but now prepared
        """
        # update the times
        now = datetime.datetime.utcnow()
        field_created = schema._created
        field_updated = schema._updated
        if is_insert:
            if field_created not in d:
                d[field_created] = now

        if field_updated not in d:
            d[field_updated] = now

        return d

    def insert(self, schema, d):
        """
        Persist d into the db

        schema -- Schema()
        d -- dict -- the values to persist

        return -- dict -- the dict that was inserted into the db
        """
        self.assure(schema)
        d = self.prepare_dict(schema, d, is_insert=True)

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
        d = self.prepare_dict(schema, d, is_insert=False)

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
            if self.handle_error(schema, e):
                ret = callback(schema, query, *args, **kwargs)

            else:
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

    def log(self, format_str, *format_args, **log_options):
        """
        wrapper around the module's logger

        format_str -- string -- the message to log
        *format_args -- list -- if format_str is a string containing {}, then format_str.format(*format_args) is ran
        **log_options -- 
            level -- something like logging.DEBUG
        """
        if isinstance(format_str, Exception):
            logger.exception(format_str, *format_args)
        else:
            log_level = log_options.get('level', logging.DEBUG)
            if logger.isEnabledFor(log_level):
                if format_args:
                    logger.log(log_level, format_str.format(*format_args))
                else:
                    logger.log(log_level, format_str)

