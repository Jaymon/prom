import sys
import os
import datetime
import logging
from contextlib import contextmanager
import exceptions
import uuid as uuidgen

# first party
from ..query import Query
from ..exception import InterfaceError
from ..decorators import reconnecting


logger = logging.getLogger(__name__)


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


class SQLConnection(Connection):
    def _transaction_start(self):
        cur = self.cursor()
        cur.execute("BEGIN")

    def _transaction_started(self, name):
        cur = self.cursor()
        # http://www.postgresql.org/docs/9.2/static/sql-savepoint.html
        cur.execute("SAVEPOINT {}".format(name))

    def _transaction_stop(self):
        """
        http://initd.org/psycopg/docs/usage.html#transactions-control
        https://news.ycombinator.com/item?id=4269241
        """
        cur = self.cursor()
        cur.execute("COMMIT")

    def _transaction_fail(self):
        cur = self.cursor()
        cur.execute("ROLLBACK")

    def _transaction_failing(self, name):
        cur = self.cursor()
        # http://www.postgresql.org/docs/9.2/static/sql-rollback-to.html
        cur.execute("ROLLBACK TO SAVEPOINT {}".format(name))


class Interface(object):

    connected = False
    """true if a connection has been established, false otherwise"""

    connection_config = None
    """a config.Connection() instance"""

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


class SQLInterface(Interface):
    """Generic base class for all SQL derived interfaces"""
    @property
    def val_placeholder(self):
        raise NotImplementedError("this property should be set in any children class")

#     def get_field_SQL(self):
#         raise NotImplementedError()

    def _delete_tables(self, **kwargs):
        for table_name in self.get_tables(**kwargs):
            with self.transaction(**kwargs) as connection:
                kwargs['connection'] = connection
                self._delete_table(table_name, **kwargs)

        return True

    def _delete(self, schema, query, **kwargs):
        where_query_str, query_args = self.get_SQL(schema, query, only_where_clause=True)
        query_str = []
        query_str.append('DELETE FROM')
        query_str.append('  {}'.format(schema))
        query_str.append(where_query_str)
        query_str = os.linesep.join(query_str)
        ret = self.query(query_str, *query_args, count_result=True, **kwargs)
        return ret

    def _query(self, query_str, query_args=None, **query_options):
        """
        **query_options -- dict
            ignore_result -- boolean -- true to not attempt to fetch results
            fetchone -- boolean -- true to only fetch one result
            count_result -- boolean -- true to return the int count of rows affected
        """
        ret = True
        # http://stackoverflow.com/questions/6739355/dictcursor-doesnt-seem-to-work-under-psycopg2
        connection = query_options.get('connection', None)
        with self.connection(connection) as connection:
            cur = connection.cursor()
            ignore_result = query_options.get('ignore_result', False)
            count_result = query_options.get('count_result', False)
            one_result = query_options.get('fetchone', False)
            cur_result = query_options.get('cursor_result', False)

            try:
                if not query_args:
                    self.log(query_str)
                    cur.execute(query_str)

                else:
                    self.log("{}{}{}", query_str, os.linesep, query_args)
                    cur.execute(query_str, query_args)

                if cur_result:
                    ret = cur

                elif not ignore_result:
                        if one_result:
                            ret = cur.fetchone()
                        elif count_result:
                            ret = cur.rowcount
                        else:
                            ret = cur.fetchall()

            except Exception as e:
                self.log(e)
                raise

            return ret

    def _normalize_date_SQL(self, field_name, field_kwargs):
        raise NotImplemented()

    def _normalize_field_SQL(self, schema, field_name):
        return field_name, self.val_placeholder

    def _normalize_val_SQL(self, schema, symbol_map, field_name, field_val, field_kwargs=None):

        format_str = ''
        format_args = []
        symbol = symbol_map['symbol']
        is_list = symbol_map.get('list', False)

        if field_kwargs:
            # kwargs take precedence because None is a perfectly valid field_val
            f = schema.fields[field_name]
            if issubclass(f.type, (datetime.datetime, datetime.date)):
                format_strs = self._normalize_date_SQL(field_name, field_kwargs)
                for fname, fvstr, farg in format_strs:
                    if format_str:
                        format_str += ' AND '


                    if is_list:
                        format_str += '{} {} ({})'.format(fname, symbol, ', '.join([fvstr] * len(farg)))
                        format_args.extend(farg)

                    else:
                        format_str += '{} {} {}'.format(fname, symbol, fvstr)
                        format_args.append(farg)

            else:
                raise ValueError('Field {} does not support extended kwarg values'.format(field_name))

        else:
            if is_list:
                field_name, format_val_str = self._normalize_field_SQL(schema, field_name)
                format_str = '{} {} ({})'.format(field_name, symbol, ', '.join([format_val_str] * len(field_val)))
                format_args.extend(field_val)

            else:
                # special handling for NULL
                if field_val is None:
                    symbol = symbol_map['none_symbol']

                field_name, format_val_str = self._normalize_field_SQL(schema, field_name)
                format_str = '{} {} {}'.format(field_name, symbol, format_val_str)
                format_args.append(field_val)

        return format_str, format_args

    def _normalize_sort_SQL(self, field_name, field_vals, sort_dir_str):
        """normalize the sort string

        return -- tuple -- field_sort_str, field_sort_args"""
        raise NotImplemented()

    def get_SQL(self, schema, query, **sql_options):
        """
        convert the query instance into SQL

        this is the glue method that translates the generic Query() instance to
        the SQL specific query, this is where the magic happens

        **sql_options -- dict
            count_query -- boolean -- true if this is a count query SELECT
            only_where_clause -- boolean -- true to only return after WHERE ...
        """
        only_where_clause = sql_options.get('only_where_clause', False)
        symbol_map = {
            'in': {'symbol': 'IN', 'list': True},
            'nin': {'symbol': 'NOT IN', 'list': True},
            'is': {'symbol': '=', 'none_symbol': 'IS'},
            'not': {'symbol': '!=', 'none_symbol': 'IS NOT'},
            'gt': {'symbol': '>'},
            'gte': {'symbol': '>='},
            'lt': {'symbol': '<'},
            'lte': {'symbol': '<='},
        }

        query_args = []
        query_str = []

        if not only_where_clause:
            query_str.append('SELECT')
            select_fields = query.fields_select
            if select_fields:
                distinct = "DISTINCT " if select_fields.options.get("unique", False) else ""
                select_fields_str = distinct + ',{}'.format(os.linesep).join(select_fields.names())
            else:
                select_fields_str = "*"

            if sql_options.get('count_query', False):
                query_str.append('  count({}) as ct'.format(select_fields_str))

            else:
                query_str.append('  {}'.format(select_fields_str))

            query_str.append('FROM')
            query_str.append('  {}'.format(schema))

        if query.fields_where:
            query_str.append('WHERE')

            for i, field in enumerate(query.fields_where):
                if i > 0: query_str.append('AND')

                field_str = ''
                field_args = []
                sd = symbol_map[field[0]]

                # field[0], field[1], field[2], field[3]
                _, field_name, field_val, field_kwargs = field
                field_str, field_args = self._normalize_val_SQL(
                    schema,
                    sd,
                    field_name,
                    field_val,
                    field_kwargs
                )

                query_str.append('  {}'.format(field_str))
                query_args.extend(field_args)

        if query.fields_sort:
            query_sort_str = []
            query_str.append('ORDER BY')
            for field in query.fields_sort:
                sort_dir_str = 'ASC' if field[0] > 0 else 'DESC'
                if field[2]:
                    field_sort_str, field_sort_args = self._normalize_sort_SQL(field[1], field[2], sort_dir_str)
                    query_sort_str.append(field_sort_str)
                    query_args.extend(field_sort_args)

                else:
                    query_sort_str.append('  {} {}'.format(field[1], sort_dir_str))

            query_str.append(',{}'.format(os.linesep).join(query_sort_str))

        if query.bounds:
            offset = query.bounds.offset
            limit = 1 if sql_options.get('one_query', False) else query.bounds.limit
            query_str.append('LIMIT {} OFFSET {}'.format(
                limit,
                offset
            ))

        query_str = os.linesep.join(query_str)
        return query_str, query_args

    def handle_error(self, schema, e, **kwargs):
        connection = kwargs.get('connection', None)
        if not connection: return False

        ret = False
        if connection.closed == 0: # connection is open
            #connection.transaction_stop()
            if isinstance(e, InterfaceError):
                ret = self._handle_error(schema, e.e, **kwargs)

            else:
                ret = self._handle_error(schema, e, **kwargs)

        else:
            # we are unsure of the state of everything since this connection has
            # closed, go ahead and close out this interface and allow this query
            # to fail, but subsequent queries should succeed
            self.close()
            ret = True

        return ret

    def _handle_error(self, schema, e, **kwargs): raise NotImplemented()

    def _set_all_tables(self, schema, **kwargs):
        """
        You can run into a problem when you are trying to set a table and it has a 
        foreign key to a table that doesn't exist, so this method will go through 
        all fk refs and make sure the tables exist
        """
        with self.transaction(**kwargs) as connection:
            kwargs['connection'] = connection
            # go through and make sure all foreign key referenced tables exist
            for field_name, field_val in schema.fields.items():
                s = field_val.schema
                if s:
                    self._set_all_tables(s, **kwargs)

            # now that we know all fk tables exist, create this table
            self.set_table(schema, **kwargs)

        return True

    def _update(self, schema, fields, query, **kwargs):
        where_query_str, where_query_args = self.get_SQL(schema, query, only_where_clause=True)
        query_str = 'UPDATE {} SET {} {}'
        query_args = []

        field_str = []
        for field_name, field_val in fields.items():
            field_str.append('{} = {}'.format(field_name, self.val_placeholder))
            query_args.append(field_val)

        query_str = query_str.format(
            schema.table,
            ',{}'.format(os.linesep).join(field_str),
            where_query_str
        )
        query_args.extend(where_query_args)

        return self.query(query_str, *query_args, count_result=True, **kwargs)

    def _get_one(self, schema, query, **kwargs):
        query_str, query_args = self.get_SQL(schema, query, one_query=True)
        return self.query(query_str, *query_args, fetchone=True, **kwargs)

    def _get(self, schema, query, **kwargs):
        query_str, query_args = self.get_SQL(schema, query)
        return self.query(query_str, *query_args, **kwargs)

    def _count(self, schema, query, **kwargs):
        query_str, query_args = self.get_SQL(schema, query, count_query=True)
        ret = self.query(query_str, *query_args, **kwargs)
        if ret:
            ret = int(ret[0]['ct'])
        else:
            ret = 0

        return ret

    def _set_all_fields(self, schema, **kwargs):
        """
        this will add fields that don't exist in the table if they can be set to NULL,
        the reason they have to be NULL is adding fields to Postgres that can be NULL
        is really light, but if they have a default value, then it can be costly
        """
        current_fields = self._get_fields(schema, **kwargs)
        for field_name, field in schema.fields.items():
            if field_name not in current_fields:
                if field.required:
                    raise ValueError('Cannot safely add {} on the fly because it is required'.format(field_name))

                else:
                    query_str = []
                    query_str.append('ALTER TABLE')
                    query_str.append('  {}'.format(schema))
                    query_str.append('ADD COLUMN')
                    query_str.append('  {}'.format(self.get_field_SQL(field_name, field)))
                    query_str = os.linesep.join(query_str)
                    self.query(query_str, ignore_result=True, **kwargs)

        return True

