import sys
import logging
from contextlib import contextmanager
import exceptions
import uuid as uuidgen

# first party
from ...query import Query
from ...exception import InterfaceError
from ...decorators import reconnecting


logger = logging.getLogger(__name__)


class Clause(object):
    interface = None
    def __init__(self, interface_query, *args, **kwargs):
        self.interface_query = interface_query
        self.interface = interface_query.interface

    def normalize(self):
        raise NotImplementedError("Children classes should add definition")


class FieldClause(Clause):
    def __init__(self, name, val=None, operator="", **options):
        self.name = name
        self.val = val
        self.operator = operator
        self.options = options


class FieldsClause(Clause):
    def __init__(self, interface_query):
        super(FieldsClause, self).__init__(interface_query)
        self.fields = []
        self.field_class = interface_query.field_class

    def append(self, *args, **kwargs):
        field = self.field_class(*args, **kwargs)
        self.fields.append(field)


    def normalize_val(self, schema, field):
        raise NotImplementedError()

    def normalize_date(self, field):
        raise NotImplementedError()




class WhereClause(FieldsClause):
    pass


class SelectClause(FieldsClause):
    def append(self, field_name, *args, **kwargs):
        f = self.field_class(field_name, operator="select")
        self.fields.append(f)


class SortClause(FieldsClause):
    def append(self, field_name, direction, field_vals=None, *args, **kwargs):
        """
        sort this query by field_name in directrion

        field_name -- string -- the field to sort on
        direction -- integer -- negative for DESC, positive for ASC
        field_vals -- list -- the order the rows should be returned in
        """
        if direction > 0:
            direction = 1
        elif direction < 0:
            direction = -1
        else:
            raise ValueError("direction {} is undefined".format(direction))

        f = self.field_class(field_name, field_vals, operator="sort", direction=direction)
        self.fields.append(f)


class LimitClause(Clause):

    @property
    def limit(self):
        return getattr(self, "_limit", 0)

    @limit.setter
    def limit(self, v):
        v = int(v)
        if v < 0:
            raise ValueError("Limit cannot be negative")
        self._limit = v

    @limit.deleter
    def limit(self):
        try:
            del self._limit
        except AttributeError: pass

    @property
    def limit_paginate(self):
        limit = self.limit
        return limit + 1 if limit > 0 else 0

    @property
    def offset(self):
        offset = getattr(self, "_offset", None)
        if offset is None:
            page = self.page
            limit = self.limit
            offset = (page - 1) * limit

        else:
            offset = offset if offset >= 0 else 0

        return offset

    @offset.setter
    def offset(self, v):
        v = int(v)
        if v < 0:
            raise ValueError("Offset cannot be negative")
        del self.page
        self._offset = v

    @offset.deleter
    def offset(self):
        try:
            del self._offset
        except AttributeError: pass

    @property
    def page(self):
        page = getattr(self, "_page", 0)
        return page if page >= 1 else 1

    @page.setter
    def page(self, v):
        v = int(v)
        if v < 0:
            raise ValueError("Page cannot be negative")
        del self.offset
        self._page = int(v)

    @page.deleter
    def page(self):
        try:
            del self._page
        except AttributeError: pass

    def get(self):
        return (self.limit, self.offset, self.limit_paginate)

    def __nonzero__(self):
        return self.limit > 0 or self.offset > 0

    def has(self):
        return bool(self)

    def has_limit(self):
        return self.limit > 0

    def normalize(self):
        raise NotImplementedError("Children classes should add definition")


class QueryClause(Clause):

    field_class = FieldClause

    select_class = SelectClause

    where_class = WhereClause

    sort_class = SortClause

    limit_class = LimitClause

    def __init__(self, interface, schema):
        self.interface = interface
        self.schema = schema
        self.select = self.select_class(self)
        self.where = self.where_class(self)
        self.sort = self.sort_class(self)
        self.limit = self.limit_class(self)







class Connection(object):
    """holds common methods that all raw connections should have"""

    transaction_count = 0
    """
    counting semaphore, greater than 0 if in a transaction, 0 if no current transaction.

    This will be incremented everytime transaction_start() is called, and decremented
    everytime transaction_stop() is called.

    transaction_fail will set this back to 0 and rollback the transaction
    """

    def transaction_name(self):
        """generate a random transaction name for use in start_transaction() and
        fail_transaction()"""
        name = uuidgen.uuid4()
        return "p{}".format(str(name.hex))

    def in_transaction(self):
        """return true if currently in a transaction"""
        return self.transaction_count > 0

    def transaction_start(self, name):
        """
        start a transaction

        this will increment transaction semaphore and pass it to _transaction_start()
        """
        if not name:
            raise ValueError("Transaction name cannot be empty")
            #uid = id(self)

        self.transaction_count += 1
        logger.debug("{}. Start transaction {}".format(self.transaction_count, name))
        if self.transaction_count == 1:
            self._transaction_start()
        else:
            self._transaction_started(name)

        return self.transaction_count

    def _transaction_start(self): pass

    def _transaction_started(self, name): pass

    def transaction_stop(self):
        """stop/commit a transaction if ready"""
        if self.transaction_count > 0:
            logger.debug("{}. Stop transaction".format(self.transaction_count))
            if self.transaction_count == 1:
                self._transaction_stop()

            self.transaction_count -= 1

        return self.transaction_count

    def _transaction_stop(self): pass

    def transaction_fail(self, name):
        """
        rollback a transaction if currently in one

        e -- Exception() -- if passed in, bubble up the exception by re-raising it
        """
        if not name:
            raise ValueError("Transaction name cannot be empty")

        if self.transaction_count > 0:
            logger.debug("{}. Failing transaction {}".format(self.transaction_count, name))
            if self.transaction_count == 1:
                self._transaction_fail()
            else:
                self._transaction_failing(name)

            self.transaction_count -= 1

    def _transaction_fail(self): pass

    def _transaction_failing(self, name): pass

#     def cursor(self, *args, **kwargs):
#         pout.v("in transaction? {}".format(self.in_transaction()))
#         return super(Connection, self).cursor(*args, **kwargs)


class Interface(object):

    connected = False
    """true if a connection has been established, false otherwise"""

    connection_config = None
    """a config.Connection() instance"""

    query_class = QueryClause

    def __init__(self, connection_config=None):
        self.connection_config = connection_config

    def create_query(self, schema):
        return self.query_class(self, schema)

    def connect(self, connection_config=None, *args, **kwargs):
        """
        connect to the interface

        this will set the raw db connection to self.connection

        *args -- anything you want that will help the db connect
        **kwargs -- anything you want that the backend db connection will need to actually connect
        """
        if self.connected: return self.connected

        if connection_config: self.connection_config = connection_config

        self.connected = True
        try:
            self._connect(self.connection_config)

        except Exception as e:
            self.connected = False
            self.raise_error(e)

        self.log("Connected {}", self.connection_config.interface_name)
        return self.connected

    def _connect(self, connection_config): raise NotImplementedError()

    def free_connection(self, connection): pass

    def get_connection(self): raise NotImplementedError()

    @contextmanager
    def connection(self, connection=None, **kwargs):
        try:
            if connection:
                yield connection

            else:
                # note to future self, this is out of try/finally because if
                # connection fails to be created then free_connection() will fail
                # which would then cover up the real error, so don't think to 
                # yourself you can move it back into try/finally
                connection = self.get_connection()
                try:
                    yield connection

                finally:
                    self.free_connection(connection)

        except Exception as e:
            self.raise_error(e)

    def close(self):
        """close an open connection"""
        if not self.connected: return True

        self._close()
        self.connected = False
        self.log("Closed Connection {}", self.connection_config.interface_name)
        return True

    def _close(self): raise NotImplementedError()

    def query(self, query_str, *query_args, **query_options):
        """
        run a raw query on the db

        query_str -- string -- the query to run
        *query_args -- if the query_str is a formatting string, pass the values in this
        **query_options -- any query options can be passed in by using key=val syntax
        """
        with self.connection(**query_options) as connection:
            query_options['connection'] = connection
            return self._query(query_str, query_args, **query_options)

    def _query(self, query_str, query_args=None, **query_options):
        raise NotImplementedError()

    @contextmanager
    def transaction(self, connection=None, **kwargs):
        """
        a simple context manager useful for when you want to wrap a bunch of db calls in a transaction
        http://docs.python.org/2/library/contextlib.html
        http://docs.python.org/release/2.5/whatsnew/pep-343.html

        example --
            with self.transaction()
                # do a bunch of calls
            # those db calls will be committed by this line
        """
        with self.connection(connection) as connection:
            name = connection.transaction_name()
            connection.transaction_start(name)
            try:
                yield connection
                connection.transaction_stop()

            except Exception as e:
                connection.transaction_fail(name)
                self.raise_error(e)

    def set_table(self, schema, **kwargs):
        """
        add the table to the db

        schema -- Schema() -- contains all the information about the table
        """
        with self.connection(**kwargs) as connection:
            kwargs['connection'] = connection
            if self.has_table(schema.table, **kwargs): return True

            try:
                with self.transaction(**kwargs):
                    self._set_table(schema, **kwargs)

                    for index_name, index in schema.indexes.items():
                        self.set_index(
                            schema,
                            name=index.name,
                            fields=index.fields,
                            connection=connection,
                            **index.options
                        )

            except InterfaceError:
                # check to see if this table now exists, it might have been created
                # in another thread
                if not self.has_table(schema, **kwargs):
                    raise

    def _set_table(self, schema, **kwargs): raise NotImplementedError()

    def has_table(self, table_name, **kwargs):
        """
        check to see if a table is in the db

        table_name -- string -- the table to check
        return -- boolean -- True if the table exists, false otherwise
        """
        with self.connection(kwargs.get('connection', None)) as connection:
            kwargs['connection'] = connection
            tables = self.get_tables(table_name, **kwargs)
            return len(tables) > 0

    def get_tables(self, table_name="", **kwargs):
        """
        get all the tables of the currently connected db

        table_name -- string -- if you would like to filter the tables list to only include matches with this name
        return -- list -- a list of table names
        """
        with self.connection(**kwargs) as connection:
            kwargs['connection'] = connection
            return self._get_tables(table_name, **kwargs)

    def _get_tables(self, table_name, **kwargs): raise NotImplementedError()

    def delete_table(self, schema, **kwargs):
        """
        remove a table matching schema from the db

        schema -- Schema()
        """
        with self.connection(**kwargs) as connection:
            kwargs['connection'] = connection
            if not self.has_table(schema.table, **kwargs): return True
            with self.transaction(**kwargs):
                self._delete_table(schema, **kwargs)

        return True

    def _delete_table(self, schema): raise NotImplementedError()

    def delete_tables(self, **kwargs):
        """
        removes all the tables from the db

        this is, obviously, very bad if you didn't mean to call this, because of that, you
        have to pass in disable_protection=True, if it doesn't get that passed in, it won't
        run this method
        """
        if not kwargs.get('disable_protection', False):
            raise ValueError('In order to delete all the tables, pass in disable_protection=True')

        with self.connection(**kwargs) as connection:
            kwargs['connection'] = connection
            self._delete_tables(**kwargs)

    def _delete_tables(self, **kwargs): raise NotImplementedError()

    def get_indexes(self, schema, **kwargs):
        """
        get all the indexes

        schema -- Schema()

        return -- dict -- the indexes in {indexname: fields} format
        """
        with self.connection(**kwargs) as connection:
            kwargs['connection'] = connection
            return self._get_indexes(schema, **kwargs)

    def _get_indexes(self, schema, **kwargs): raise NotImplementedError()

    def set_index(self, schema, name, fields, **index_options):
        """
        add an index to the table

        schema -- Schema()
        name -- string -- the name of the index
        fields -- array -- the fields the index should be on
        **index_options -- dict -- any index options that might be useful to create the index
        """
        with self.transaction(**index_options) as connection:
            index_options['connection'] = connection
            self._set_index(schema, name, fields, **index_options)

        return True

    def _set_index(self, schema, name, fields, **index_options):
        raise NotImplementedError()

    @reconnecting()
    def insert(self, schema, fields, **kwargs):
        """
        Persist d into the db

        schema -- Schema()
        fields -- dict -- the values to persist

        return -- int -- the primary key of the row just inserted
        """
        r = 0

        with self.connection(**kwargs) as connection:
            kwargs['connection'] = connection
            try:
                with self.transaction(**kwargs):
                    r = self._insert(schema, fields, **kwargs)

            except Exception as e:
                exc_info = sys.exc_info()
                if self.handle_error(schema, e, **kwargs):
                    r = self._insert(schema, fields, **kwargs)
                else:
                    self.raise_error(e, exc_info)

        return r

    def _insert(self, schema, fields, **kwargs): raise NotImplementedError()

    @reconnecting()
    def update(self, schema, fields, query, **kwargs):
        """
        Persist the query.fields into the db that match query.fields_where

        schema -- Schema()
        fields -- dict -- the values to persist
        query -- Query() -- will be used to create the where clause

        return -- int -- how many rows where updated
        """
        with self.connection(**kwargs) as connection:
            kwargs['connection'] = connection
            try:
                with self.transaction(**kwargs):
                    r = self._update(schema, fields, query, **kwargs)

            except Exception as e:
                exc_info = sys.exc_info()
                if self.handle_error(schema, e, **kwargs):
                    r = self._update(schema, fields, query, **kwargs)
                else:
                    self.raise_error(e, exc_info)

        return r

    def _update(self, schema, fields, query, **kwargs): raise NotImplementedError()

    @reconnecting()
    def _get_query(self, callback, schema, query=None, *args, **kwargs):
        """this is just a common wrapper around all the get queries since they are
        all really similar in how they execute"""
        if not query: query = Query()

        ret = None
        with self.connection(**kwargs) as connection:
            kwargs['connection'] = connection
            try:
                if connection.in_transaction():
                    # we wrap SELECT queries in a transaction if we are in a transaction because
                    # it could cause data loss if it failed by causing the db to discard
                    # anything in the current transaction if the query isn't wrapped,
                    # go ahead, ask me how I know this
                    with self.transaction(**kwargs):
                        ret = callback(schema, query, *args, **kwargs)

                else:
                    ret = callback(schema, query, *args, **kwargs)

            except Exception as e:
                exc_info = sys.exc_info()
                if self.handle_error(schema, e, **kwargs):
                    ret = callback(schema, query, *args, **kwargs)
                else:
                    self.raise_error(e, exc_info)

        return ret

    def get_one(self, schema, query=None, **kwargs):
        """
        get one row from the db matching filters set in query

        schema -- Schema()
        query -- Query()

        return -- dict -- the matching row
        """
        ret = self._get_query(self._get_one, schema, query, **kwargs)
        if not ret: ret = {}
        return ret

    def _get_one(self, schema, query, **kwargs): raise NotImplementedError()

    def get(self, schema, query=None, **kwargs):
        """
        get matching rows from the db matching filters set in query

        schema -- Schema()
        query -- Query()

        return -- list -- a list of matching dicts
        """
        ret = self._get_query(self._get, schema, query, **kwargs)
        if not ret: ret = []
        return ret

    def _get(self, schema, query, **kwargs): raise NotImplementedError()

    def count(self, schema, query=None, **kwargs):
        ret = self._get_query(self._count, schema, query, **kwargs)
        return int(ret)

    def _count(self, schema, query, **kwargs): raise NotImplementedError()

    def delete(self, schema, query, **kwargs):
        if not query or not query.fields_where:
            raise ValueError('aborting delete because there is no where clause')

        with self.transaction(**kwargs) as connection:
            kwargs['connection'] = connection
            ret = self._get_query(self._delete, schema, query, **kwargs)

        return ret

    def _delete(self, schema, query, **kwargs): raise NotImplementedError()

    def handle_error(self, schema, e, **kwargs):
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

    def raise_error(self, e, exc_info=None):
        """this is just a wrapper to make the passed in exception an InterfaceError"""
        if not exc_info:
            exc_info = sys.exc_info()
        if not isinstance(e, InterfaceError):
            # allow python's built in errors to filter up through
            # https://docs.python.org/2/library/exceptions.html
            if not hasattr(exceptions, e.__class__.__name__):
                e = InterfaceError(e, exc_info)
        raise e.__class__, e, exc_info[2]


