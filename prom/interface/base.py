# -*- coding: utf-8 -*-
from __future__ import unicode_literals, division, print_function, absolute_import
import sys
import os
import datetime
import decimal
import logging
from contextlib import contextmanager
import uuid

from datatypes import LogMixin

# first party
from ..query import Query
from ..exception import (
    InterfaceError,
    UniqueError,
    TableError,
    FieldError,
    UniqueError,
    CloseError,
)

from ..decorators import reconnecting
from ..compat import *
from ..utils import make_list


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

    def transaction_name(self, prefix=""):
        """generate a random transaction name for use in start_transaction() and
        fail_transaction()

        :param prefix: str, to better track transactions in logs you can give a
            prefix name that will be prepended to the auto-generated name
        """
        name = str(uuid.uuid4())[-5:]
        #tcount = self.transaction_count + 1
        prefix = prefix or "p"
        return f"{prefix}_{name}"

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
        logger.info("{}. Start transaction {}".format(self.transaction_count, name))
        if self.transaction_count == 1:
            self._transaction_start()
        else:
            self._transaction_started(name)

        return self.transaction_count

    def _transaction_start(self): pass

    def _transaction_started(self, name): pass

    def transaction_stop(self, name):
        """stop/commit a transaction if ready"""
        if self.transaction_count > 0:
            logger.info("{}. Stop transaction {}".format(self.transaction_count, name))
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
            logger.info("{}. Failing transaction {}".format(self.transaction_count, name))
            if self.transaction_count == 1:
                self._transaction_fail()
            else:
                self._transaction_failing(name)

            self.transaction_count -= 1

    def _transaction_fail(self): pass

    def _transaction_failing(self, name): pass


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


class Interface(LogMixin):

    connected = False
    """true if a connection has been established, false otherwise"""

    connection_config = None
    """a config.Connection() instance"""

    InterfaceError = InterfaceError
    UniqueError = UniqueError

    @classmethod
    def configure(cls, connection_config):
        host = connection_config.host
        if host:
            db = connection_config.database
            connection_config.database = db.strip("/")
        return connection_config

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

    def is_connected(self): return self.connected

    def close(self):
        """close an open connection"""
        if not self.connected: return True

        self._close()
        self.connected = False
        self.log("Closed Connection {}", self.connection_config.interface_name)
        return True

    def _close(self): raise NotImplementedError()

    def readonly(self, readonly=True):
        """Make the connection read only (pass in True) or read/write (pass in False)

        :param readonly: boolean, True if this connection should be readonly, False
            if the connection should be read/write
        """
        self.connection_config.readonly = readonly

        if self.connected:
            self._readonly(readonly)

    def _readonly(self, readonly): raise NotImplementedError()

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
            name = connection.transaction_name(kwargs.get("prefix", ""))
            connection.transaction_start(name)
            try:
                yield connection
                connection.transaction_stop(name)

            except Exception as e:
                connection.transaction_fail(name)
                self.raise_error(e)

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

    def set_table(self, schema, **kwargs):
        """
        add the table to the db

        schema -- Schema() -- contains all the information about the table
        """
        with self.connection(**kwargs) as connection:
            kwargs['connection'] = connection
            kwargs.setdefault("prefix", "set_table")
            if self.has_table(str(schema), **kwargs): return True

            try:
                with self.transaction(**kwargs):
                    self._set_table(schema, **kwargs)

                    for index_name, index in schema.indexes.items():
                        self.set_index(
                            schema,
                            name=index.name,
                            field_names=index.field_names,
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
        with self.connection(**kwargs) as connection:
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
            return self._get_tables(str(table_name), **kwargs)

    def _get_tables(self, table_name, **kwargs): raise NotImplementedError()

    def unsafe_delete_table(self, schema, **kwargs):
        """wrapper around delete_table that matches the *_tables variant and denotes
        that this is a serious operation

        remove a table matching schema from the db

        :param schema: Schema instance, the table to delete
        """
        with self.connection(**kwargs) as connection:
            kwargs['connection'] = connection
            if not self.has_table(str(schema), **kwargs): return True
            with self.transaction(**kwargs):
                self._delete_table(schema, **kwargs)

        return True

    def _delete_table(self, schema): raise NotImplementedError()

    def unsafe_delete_tables(self, **kwargs):
        """Removes all the tables from the db

        https://github.com/Jaymon/prom/issues/75
        """
        with self.connection(**kwargs) as connection:
            kwargs['connection'] = connection
            self._delete_tables(**kwargs)

    def _delete_tables(self, **kwargs): raise NotImplementedError()

    def get_fields(self, table_name, **kwargs):
        with self.connection(**kwargs) as connection:
            kwargs['connection'] = connection
            return self._get_fields(str(table_name), **kwargs)

    def _get_fields(self, table_name, **kwargs): raise NotImplementedError()

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

    def set_index(self, schema, name, field_names, **index_options):
        """
        add an index to the table

        schema -- Schema()
        name -- string -- the name of the index
        field_names -- array -- the fields the index should be on
        **index_options -- dict -- any index options that might be useful to create the index
        """
        with self.transaction(**index_options) as connection:
            index_options['connection'] = connection
            self._set_index(schema, name, field_names, **index_options)

        return True

    def _set_index(self, schema, name, field_names, **index_options):
        raise NotImplementedError()

    @reconnecting()
    def execute_gracefully(self, callback, *args, **kwargs):
        """Internal method. Execute the callback with args and kwargs, retrying
        the query if an error is raised that it thinks it successfully handled

        better names: retry? execute_retry?

        :param callback: callable, this will be run at-most twice
        :param *args: passed directly to callback as *args
        :param **kwargs: passed to callback as **kwargs, can have values added
        :returns: mixed, whatever the callback returns
        """
        r = None

        with self.connection(**kwargs) as connection:
            kwargs['connection'] = connection
            prefix = callback.__name__
            always_transaction = set(["_insert", "_upsert", "_update", "_delete"])

            # we wrap SELECT queries in a transaction if we are in a transaction because
            # it could cause data loss if it failed by causing the db to discard
            # anything in the current transaction if the query isn't wrapped,
            # go ahead, ask me how I know this
            cb_transaction = prefix in always_transaction or connection.in_transaction()

            try:
                if cb_transaction:
                    with self.transaction(prefix=prefix, **kwargs):
                        r = callback(*args, **kwargs)

                else:
                    r = callback(*args, **kwargs)

            except Exception as e:
                self.log(
                    f"{prefix} failed with {e}, attempting to handle the error",
                    level='WARNING',
                )
                if self.handle_error(e=e, **kwargs):
                    self.log(
                        f"{prefix} has handled error: '{e}', re-running original query",
                        level="WARNING",
                    )

                    try:
                        if cb_transaction:
                            with self.transaction(prefix=f"{prefix}_retry", **kwargs):
                                r = callback(*args, **kwargs)

                        else:
                            r = callback(*args, **kwargs)

                    except Exception as e:
                        self.log(
                            f"{prefix} failed again re-running original query",
                            level="WARNING",
                        )
                        self.raise_error(e)

                else:
                    self.log(
                        f"Raising '{e}' because it could not be handled!",
                        level='WARNING',
                    )
                    self.raise_error(e)

        return r

    def insert(self, schema, fields, **kwargs):
        """Persist fields into the db

        :param schema: Schema instance, the table the query will run against
        :param fields: dict, the fields {field_name: field_value} to persist
        :param **kwargs: passed through
        :returns: mixed, will return the primary key values
        """
        return self.execute_gracefully(
            self._insert,
            schema=schema,
            fields=fields,
            **kwargs
        )

    def _insert(self, schema, fields, **kwargs): raise NotImplementedError()

    def update(self, schema, fields, query, **kwargs):
        """Persist the query.fields into the db that match query.fields_where

        :param schema: Schema instance, the table the query will run against
        :param fields: dict, the fields {field_name: field_value} to persist
        :param query: Query instance, will be used to create the where clause
        :param **kwargs: passed through
        :returns: int, how many rows where updated
        """
        return self.execute_gracefully(
            self._update,
            schema=schema,
            fields=fields,
            query=query,
            **kwargs,
        )

    def _update(self, schema, fields, query, **kwargs): raise NotImplementedError()

    def upsert(self, schema, insert_fields, update_fields, conflict_field_names, **kwargs):
        """Perform an upsert (insert or update) on the table

        :param schema: Schema instance, the table the query will run against
        :param insert_fields: dict, these are the fields that will be inserted
        :param update_fields: dict, on a conflict with the insert_fields, these
            fields will instead be used to update the row
        :param conflict_field_names: list, the field names that will decide if
            an insert or update is performed
        :param **kwargs: anything else
        :returns: mixed, the primary key
        """
        return self.execute_gracefully(
            self._upsert,
            schema=schema,
            insert_fields=insert_fields,
            update_fields=update_fields,
            conflict_field_names=conflict_field_names,
            **kwargs,
        )

    def _upsert(self, schema, insert_fields, update_fields, conflict_field_names, **kwargs):
        raise NotImplementedError()

    def delete(self, schema, query, **kwargs):
        """delete matching rows according to query filter criteria

        :param schema: Schema instance, the table the query will run against
        :param query: Query instance, the filter criteria, this will fail if empty
        :returns: int, how many rows were deleted ... I think
        """
        if not query or not query.fields_where:
            raise ValueError('aborting delete because there is no where clause')

        return self.execute_gracefully(
            self._delete,
            schema=schema,
            query=query,
            **kwargs
        )

    def _delete(self, schema, query, **kwargs): raise NotImplementedError()

    def get_one(self, schema, query=None, **kwargs):
        """get one row from the db matching filters set in query

        :param schema: Schema instance, the table the query will run against
        :param query: Query instance, the filter criteria
        :return: dict, the matching row
        """
        ret = self.execute_gracefully(
            self._get_one,
            schema=schema,
            query=query or Query(),
            **kwargs
        )
        return ret or {}

    def _get_one(self, schema, query, **kwargs): raise NotImplementedError()

    def get(self, schema, query=None, **kwargs):
        """get matching rows from the db matching filters set in query

        :param schema: Schema instance, the table the query will run against
        :param query: Query instance, the filter criteria
        :returns: list, a list of matching dicts
        """
        ret = self.execute_gracefully(
            self._get,
            schema=schema,
            query=query or Query(),
            **kwargs
        )
        return ret or []

    def _get(self, schema, query, **kwargs): raise NotImplementedError()

    def count(self, schema, query=None, **kwargs):
        """count matching rows according to query filter criteria

        :param schema: Schema instance, the table the query will run against
        :param query: Query instance, the filter criteria
        :returns: list, a list of matching dicts
        """
        ret = self.execute_gracefully(
            self._count,
            schema=schema,
            query=query or Query(),
            **kwargs
        )
        return int(ret) if ret else 0

    def _count(self, schema, query, **kwargs): raise NotImplementedError()

    def render(self, schema, query, **kwargs):
        """Render the query in a way that the interface can interpret it

        so in a SQL interface, this would render SQL, this is mainly used for
        debugging

        :param query: Query, the Query instance to render
        :param **kwargs: any named arguments
        :returns: mixed
        """
        raise NotImplementedError()

    def spawn(self):
        """Return a new instance of this Interface with the same connection configuration

        :returns: Interface instance
        """
        return type(self)(self.connection_config)

    def handle_error(self, schema, e, **kwargs):
        """
        try and handle the error, return False if the error can't be handled

        TODO -- this method is really general, maybe change this so there are a couple other methods
        like isTableError() and isFieldError() that the child needs to flesh out, maybe someday

        return -- boolean -- True if the error was handled, False if it wasn't
        """
        return False

    def raise_error(self, e, **kwargs):
        """raises e

        :param e: Exception, if a built-in exception then it's raised, if any other
            error then it will be wrapped in an InterfaceError
        """
        e2 = self.create_error(e, **kwargs)
        if e2 is not e:
            raise e2 from e
        else:
            raise e
        #raise self.create_error(e, **kwargs) from e

    def create_error(self, e, **kwargs):
        """create the error that you want to raise, this gives you an opportunity
        to customize the error

        allow python's built in errors to filter up through
        https://docs.python.org/2/library/exceptions.html
        """
        if not isinstance(e, InterfaceError) and not hasattr(builtins, e.__class__.__name__):
            e_class = kwargs.get("e_class", InterfaceError)
            e = e_class(e)
        return e


class SQLInterface(Interface):
    """Generic base class for all SQL derived interfaces"""
    @property
    def val_placeholder(self):
        raise NotImplementedError("this property should be set in any children class")

    def _delete_tables(self, **kwargs):
        with self.transaction(**kwargs) as connection:
            kwargs['connection'] = connection
            for table_name in self.get_tables(**kwargs):
                self._delete_table(table_name, **kwargs)

        return True

    def _delete(self, schema, query, **kwargs):
        where_query_str, query_args = self.get_SQL(schema, query, only_where_clause=True)
        query_str = []
        query_str.append('DELETE FROM')
        query_str.append('  {}'.format(self._normalize_table_name(schema)))
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
            one_result = query_options.get('fetchone', query_options.get('one_result', False))
            cursor_result = query_options.get('cursor_result', False)

            try:
                if query_args:
                    self.log_for(
                        debug=(["{}\n{}", query_str, query_args],),
                        info=([query_str],)
                    )

                    #self.log("{}{}{}", query_str, os.linesep, query_args, level="INFO")
                    cur.execute(query_str, query_args)
                else:
                    self.log(query_str)
                    cur.execute(query_str)

                if cursor_result:
                    ret = cur

                elif not ignore_result:
                    if one_result:
                        # https://www.psycopg.org/docs/cursor.html#cursor.fetchone
                        ret = cur.fetchone()
                    elif count_result:
                        # https://www.psycopg.org/docs/cursor.html#cursor.rowcount
                        ret = cur.rowcount
                    else:
                        # https://www.psycopg.org/docs/cursor.html#cursor.fetchall
                        ret = cur.fetchall()

            except Exception as e:
                self.raise_error(e)

            return ret

    def handle_error(self, schema, e, **kwargs):
        connection = kwargs.get('connection', None)
        if not connection: return False

        ret = False
        if connection.closed:
            # we are unsure of the state of everything since this connection has
            # closed, go ahead and close out this interface and allow this query
            # to fail, but subsequent queries should succeed
            self.close()
            ret = True

        else:
            # connection is open
            e = self.create_error(e)

            query = kwargs.get("query", None)
            schemas = query.schemas if query else []
            if schemas:
                ret = True
                for s in query.schemas:
                    ret = self._handle_error(s, e, **kwargs)
                    if not ret:
                        break

            else:
                ret = self._handle_error(schema, e, **kwargs)

        return ret

    def _handle_error(self, schema, e, **kwargs):
        ret = False
        if isinstance(e, UniqueError):
            ret = False

        elif isinstance(e, FieldError):
            try:
                ret = self._set_all_fields(schema, **kwargs)

            except ValueError:
                ret = False

        elif isinstance(e, TableError):
            ret = self._set_all_tables(schema, **kwargs)

        return ret

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

    def _set_table(self, schema, **kwargs):
        """
        http://sqlite.org/lang_createtable.html
        http://www.postgresql.org/docs/9.1/static/sql-createtable.html
        http://www.postgresql.org/docs/8.1/static/datatype.html
        http://pythonhosted.org/psycopg2/usage.html#adaptation-of-python-values-to-sql-types
        """
        query_str = []
        query_str.append("CREATE TABLE {} (".format(self._normalize_table_name(schema)))

        query_fields = []
        for field_name, field in schema.fields.items():
            query_fields.append('  {}'.format(self.render_datatype_sql(field_name, field)))

        query_str.append(",{}".format(os.linesep).join(query_fields))
        query_str.append(')')
        query_str = os.linesep.join(query_str)
        ret = self._query(query_str, ignore_result=True, **kwargs)

    def _set_all_fields(self, schema, **kwargs):
        """
        this will add fields that don't exist in the table if they can be set to NULL,
        the reason they have to be NULL is adding fields to Postgres that can be NULL
        is really light, but if they have a default value, then it can be costly
        """
        current_fields = self.get_fields(schema, **kwargs)
        for field_name, field in schema.fields.items():
            if field_name not in current_fields:
                if field.required:
                    raise ValueError('Cannot safely add {} on the fly because it is required'.format(field_name))

                else:
                    query_str = []
                    query_str.append('ALTER TABLE')
                    query_str.append('  {}'.format(self._normalize_table_name(schema)))
                    query_str.append('ADD COLUMN')
                    query_str.append('  {}'.format(self.render_datatype_sql(field_name, field)))
                    query_str = os.linesep.join(query_str)
                    self.query(query_str, ignore_result=True, **kwargs)

        return True

    def _insert(self, schema, fields, **kwargs):
        pk_names = schema.pk_names
        kwargs.setdefault("ignore_return_clause", len(pk_names) == 0)
        kwargs.setdefault("ignore_result", len(pk_names) == 0)

        query_str, query_args = self.render_insert_sql(
            schema,
            fields,
            **kwargs,
        )

        r = self.query(query_str, *query_args, **kwargs)
        if r and pk_names:
            if len(pk_names) > 1:
                r = r[0]
            else:
                r = r[0][pk_names[0]]
        return r

    def _update(self, schema, fields, query, **kwargs):
        query_str, query_args = self.render_update_sql(
            schema,
            fields,
            query=query,
            **kwargs,
        )

        return self.query(query_str, *query_args, count_result=True, **kwargs)

    def _upsert(self, schema, insert_fields, update_fields, conflict_field_names, **kwargs):
        """
        https://www.sqlite.org/lang_UPSERT.html
        """
        if not conflict_field_names:
            raise ValueError(f"Upsert is missing conflict fields for {schema}")

        for field_name in conflict_field_names:
            # conflict fields need to be in the insert fields
            if field_name not in insert_fields:
                raise ValueError(f"Upsert insert fields on {schema} missing conflict field {field_name}")

            # conflict fields should not be in the udpate fields (this is more
            # for safety, they should use .update if they want to change them)
            if field_name in update_fields:
                raise ValueError(f"Upsert update fields on {schema} contains conflict field {field_name}")

        insert_sql, insert_args = self.render_insert_sql(
            schema,
            insert_fields,
            ignore_return_clause=True,
            **kwargs,
        )

        update_sql, update_args = self.render_update_sql(
            schema,
            update_fields,
            query=None,
            only_set_clause=True,
            **kwargs,
        )

        query_str = '{} ON CONFLICT({}) DO UPDATE {}'.format(
            insert_sql,
            ', '.join(map(self._normalize_name, conflict_field_names)),
            update_sql,
        )

        returning_field_names = schema.pk_names
        if returning_field_names:
            # https://www.sqlite.org/lang_returning.html
            query_str += ' RETURNING {}'.format(', '.join(map(self._normalize_name, returning_field_names)))
            query_args = insert_args + update_args

        r = self.query(query_str, *query_args, **kwargs)
        if r and returning_field_names:
            if len(returning_field_names) > 1:
                r = r[0]
            else:
                r = r[0][returning_field_names[0]]
        return r

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

    def _normalize_date_SQL(self, field_name, field_kwargs, symbol):
        raise NotImplemented()

    def _normalize_field_SQL(self, schema, field_name, symbol):
        return self._normalize_name(field_name), self.val_placeholder

    def _normalize_val_SQL(self, schema, symbol_map, field):
        format_str = ''
        format_args = []
        symbol = symbol_map['symbol']
        is_list = field.is_list
        field_name = field.name
        field_val = field.value
        field_kwargs = field.kwargs

        if field_kwargs:
            # kwargs take precedence because None is a perfectly valid field_val
            f = schema.fields[field_name]
            if issubclass(f.type, (datetime.datetime, datetime.date)):
                format_strs = self._normalize_date_SQL(field_name, field_kwargs, symbol)
                for fname, fvstr, farg in format_strs:
                    if format_str:
                        format_str += ' AND '

                    if is_list:
                        # you can pass in things like day=..., month=... to
                        # date fields, this converts those values to lists to
                        # make sure we can handle something like in_foo(day=1)
                        # and in_foo(day=[1, 2, 3]) the same way
                        farg = make_list(farg)

                        format_str += '{} {} ({})'.format(
                            fname,
                            symbol,
                            ', '.join([fvstr] * len(farg))
                        )
                        format_args.extend(farg)

                    else:
                        format_str += '{} {} {}'.format(fname, symbol, fvstr)
                        format_args.append(farg)

            else:
                raise ValueError('Field {} does not support extended kwarg values'.format(field_name))

        else:
            if is_list and not isinstance(field_val, Query):
                field_val = make_list(field_val) if field_val else []
                field_name, format_val_str = self._normalize_field_SQL(schema, field_name, symbol)
                if field_val:
                    format_str = '{} {} ({})'.format(
                        field_name,
                        symbol,
                        ', '.join([format_val_str] * len(field_val))
                    )
                    format_args.extend(field_val)

                else:
                    # field value is empty, so we need to customize the SQL to
                    # compensate for the empty set since SQL doesn't like empty
                    # sets
                    #
                    # the idea here is this is a condition that will
                    # automatically cause the query to fail but not necessarily be an error, 
                    # the best example is the IN (...) queries, if you do self.in_foo([]).get()
                    # that will fail because the list was empty, but a value error shouldn't
                    # be raised because a common case is: self.if_foo(Bar.query.is_che(True).pks).get()
                    # which should result in an empty set if there are no rows where che = TRUE
                    #
                    # https://stackoverflow.com/a/58078468/5006
                    if symbol == "IN":
                        format_str = '{} <> {}'.format(field_name, field_name)

                    elif symbol == "NOT IN":
                        format_str = '{} = {}'.format(field_name, field_name)

                    else:
                        raise ValueError("Unsure what to do here")

            else:
                # special handling for NULL
                if field_val is None:
                    symbol = symbol_map['none_symbol']

                field_name, format_val_str = self._normalize_field_SQL(
                    schema,
                    field_name,
                    symbol
                )

                if isinstance(field_val, Query):
                    subquery_schema = field_val.schema
                    if not subquery_schema:
                        raise ValueError("{} subquery has no schema".format(field_name))

                    subquery_sql, subquery_args = self.get_SQL(
                        field_val.schema,
                        field_val
                    )

                    format_str = '{} {} ({})'.format(
                        field_name,
                        symbol,
                        subquery_sql
                    )
                    format_args.extend(subquery_args)

                else:
                    format_str = '{} {} {}'.format(
                        field_name,
                        symbol,
                        format_val_str
                    )
                    format_args.append(field_val)

        return format_str, format_args

    def _normalize_sort_SQL(self, field_name, field_vals, sort_dir_str):
        """normalize the sort string

        return -- tuple -- field_sort_str, field_sort_args"""
        raise NotImplemented()

    def _normalize_table_name(self, schema):
        return self._normalize_name(schema)

    def _normalize_name(self, name):
        """normalize a non value name for the query

        https://blog.christosoft.de/2012/10/sqlite-escaping-table-acolumn-names/

        :param name: str, the name that should be prepared to be queried
        :returns: the modified name ready to be added to a query string
        """
        return '"{}"'.format(name)

    def _normalize_bounds_SQL(self, bounds):
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
            'eq': {'symbol': '=', 'none_symbol': 'IS'},
            'ne': {'symbol': '!=', 'none_symbol': 'IS NOT'},
            'gt': {'symbol': '>'},
            'gte': {'symbol': '>='},
            'lt': {'symbol': '<'},
            'lte': {'symbol': '<='},
            # https://www.tutorialspoint.com/postgresql/postgresql_like_clause.htm
            # https://www.tutorialspoint.com/sqlite/sqlite_like_clause.htm
            'like': {'symbol': 'LIKE'},
            'nlike': {'symbol': 'NOT LIKE'},
        }

        query_args = []
        query_str = []

        if not only_where_clause:
            query_str.append('SELECT')
            is_count_query = sql_options.get('count_query', False)
            select_fields = query.fields_select
            if select_fields:
                distinct_fields = select_fields.options.get(
                    "distinct",
                    select_fields.options.get("unique", False)
                )
                distinct = "DISTINCT " if distinct_fields else ""
                select_fields_str = distinct + ", ".join(
                    (self._normalize_name(f.name) for f in select_fields)
                )
            else:
                if is_count_query or select_fields.options.get("all", False):
                    select_fields_str = "*"
                else:
                    select_fields_str = ", ".join(
                        (self._normalize_name(fname) for fname in schema.fields.keys())
                    )

            if is_count_query:
                query_str.append('  count({}) as ct'.format(select_fields_str))

            else:
                query_str.append('  {}'.format(select_fields_str))

            query_str.append('FROM')
            query_str.append("  {}".format(self._normalize_table_name(schema)))

        if query.fields_where:
            query_str.append('WHERE')

            for i, field in enumerate(query.fields_where):
                if i > 0: query_str.append('AND')

                field_str = ''
                field_args = []
                sd = symbol_map[field.operator]

                field_str, field_args = self._normalize_val_SQL(
                    schema,
                    sd,
                    field,
                )

                query_str.append('  {}'.format(field_str))
                query_args.extend(field_args)

        if query.fields_sort:
            query_sort_str = []
            query_str.append('ORDER BY')
            for field in query.fields_sort:
                sort_dir_str = 'ASC' if field.direction > 0 else 'DESC'
                if field.value:
                    field_sort_str, field_sort_args = self._normalize_sort_SQL(field.name, field.value, sort_dir_str)
                    query_sort_str.append(field_sort_str)
                    query_args.extend(field_sort_args)

                else:
                    query_sort_str.append('  {} {}'.format(field.name, sort_dir_str))

            query_str.append(',{}'.format(os.linesep).join(query_sort_str))

        if query.bounds:
            query_str.append(self._normalize_bounds_SQL(query.bounds, sql_options))

        query_str = "\n".join(query_str)
        return query_str, query_args

    def render_insert_sql(self, schema, fields, **kwargs):
        """
        https://www.sqlite.org/lang_insert.html
        """
        field_formats = []
        field_names = []
        query_vals = []
        for field_name, field_val in fields.items():
            field_names.append(self._normalize_name(field_name))
            field_formats.append(self.val_placeholder)
            query_vals.append(field_val)

        query_str = 'INSERT INTO {} ({}) VALUES ({})'.format(
            self._normalize_table_name(schema),
            ', '.join(field_names),
            ', '.join(field_formats),
        )

        if not kwargs.get("ignore_return_clause", False):
            # https://www.sqlite.org/lang_returning.html
            pk_name = schema.pk_name
            if pk_name:
                query_str += ' RETURNING {}'.format(self._normalize_name(pk_name))

        return query_str, query_vals

    def render_update_sql(self, schema, fields, query, **kwargs):
        query_str = ''
        query_args = []

        if not kwargs.get("only_set_clause", False):
            query_str = 'UPDATE {} '.format(self._normalize_table_name(schema))

        field_str = []
        for field_name, field_val in fields.items():
            field_str.append('{} = {}'.format(self._normalize_name(field_name), self.val_placeholder))
            query_args.append(field_val)

        query_str += 'SET {}'.format(',{}'.format(os.linesep).join(field_str))

        if query:
            where_query_str, where_query_args = self.get_SQL(schema, query, only_where_clause=True)
            query_str += ' {}'.format(where_query_str)
            query_args.extend(where_query_args)

        return query_str, query_args

    def render_datatype_sql(self, field_name, field):
        """Returns the SQL for a given field with full type information

        http://www.sqlite.org/datatype3.html
        https://www.postgresql.org/docs/current/datatype.html

        :param field_name: str, the field's name
        :param field: Field instance, the configuration for the field
        :returns: str, the complete field datatype SQL (eg, foo BOOL NOT NULL)
        """
        field_type = ""
        interface_type = field.interface_type

        if issubclass(interface_type, bool):
            field_type = self.render_datatype_bool_sql(field_name, field)

        elif issubclass(interface_type, int):
            field_type = self.render_datatype_int_sql(field_name, field)

        elif issubclass(interface_type, str):
            field_type = self.render_datatype_str_sql(field_name, field)

        elif issubclass(interface_type, datetime.datetime):
            field_type = self.render_datatype_datetime_sql(field_name, field)

        elif issubclass(interface_type, datetime.date):
            field_type = self.render_datatype_date_sql(field_name, field)

        elif issubclass(interface_type, dict):
            field_type = self.render_datatype_dict_sql(field_name, field)

        elif issubclass(interface_type, (float, decimal.Decimal)):
            field_type = self.render_datatype_float_sql(field_name, field)

        elif issubclass(interface_type, (bytearray, bytes)):
            field_type = self.render_datatype_bytes_sql(field_name, field)

        elif issubclass(interface_type, uuid.UUID):
            field_type = self.render_datatype_uuid_sql(field_name, field)

        else:
            raise ValueError('Unknown python type: {} for field: {}'.format(
                interface_type.__name__,
                field_name,
            ))

        field_type += ' ' + self.render_datatype_required_sql(field_name, field)

        if not field.is_pk():
            if field.is_ref():
                field_type += ' ' + self.render_datatype_ref_sql(field_name, field)

        return '{} {}'.format(self._normalize_name(field_name), field_type)

    def render_datatype_bool_sql(self, field_name, field, **kwargs):
        return 'BOOL'

    def render_datatype_int_sql(self, field_name, field, **kwargs):
        return 'INTEGER'

    def render_datatype_str_sql(self, field_name, field, **kwargs):
        fo = field.interface_options
        field_type = kwargs.get("datatype", 'TEXT')
        size_info = field.size_info()

        # https://www.sqlitetutorial.net/sqlite-check-constraint/
        if 'size' in size_info["original"]:
            field_type += f" CHECK(length({field_name}) = {size_info['size']})"

        elif 'max_size' in size_info["original"]:
            if "min_size" in size_info["original"]:
                field_type += f" CHECK(length({field_name}) >= {size_info['original']['min_size']}"
                field_type += " AND "
                field_type += f"length({field_name}) <= {size_info['original']['max_size']})"

            else:
                field_type += f" CHECK(length({field_name}) <= {size_info['size']})"

        if field.is_pk():
            field_type += ' PRIMARY KEY'

        return field_type

    def render_datatype_datetime_sql(self, field_name, field, **kwargs):
        raise NotImplementedError()

    def render_datatype_date_sql(self, field_name, field):
        return 'DATE'

    def render_datatype_dict_sql(self, field_name, field, **kwargs):
        raise NotImplementedError()

    def render_datatype_float_sql(self, field_name, field, **kwargs):
        return 'REAL'

    def render_datatype_bytes_sql(self, field_name, field, **kwargs):
        return 'BLOB'

    def render_datatype_uuid_sql(self, field_name, field, **kwargs):
        raise NotImplementedError()

    def render_datatype_required_sql(self, field_name, field, **kwargs):
        return 'NOT NULL' if field.required else 'NULL'

    def render_datatype_ref_sql(self, field_name, field, **kwargs):
        ref_s = field.schema
        if field.required: # strong ref, it deletes on fk row removal
            format_str = 'REFERENCES {} ({}) ON UPDATE CASCADE ON DELETE CASCADE'

        else: # weak ref, it sets column to null on fk row removal
            format_str = 'REFERENCES {} ({}) ON UPDATE CASCADE ON DELETE SET NULL'

        ret = format_str.format(
            self._normalize_table_name(ref_s),
            self._normalize_name(ref_s.pk.name)
        )

        return ret

    def render(self, schema, query, **kwargs):
        """Render the query

        :param schema: Schema, the query schema
        :param query: Query, the query to render
        :param **kwargs: named arguments
            placeholders: boolean, True if place holders should remain
        :returns: string if placeholders is False, (string, list) if placeholders is True
        """
        sql, sql_args = self.get_SQL(schema, query)
        placeholders = kwargs.get("placeholders", kwargs.get("placeholder", False))

        if not placeholders:
            for sql_arg in sql_args:
                sa = String(sql_arg)
                if not sa.isnumeric():
                    sa = "'{}'".format(sa)
                sql = sql.replace(self.val_placeholder, sa, 1)

        return (sql, sql_args) if placeholders else sql

