# -*- coding: utf-8 -*-
from __future__ import unicode_literals, division, print_function, absolute_import
import sys
import os
import datetime
import decimal
import logging
from contextlib import contextmanager
import uuid
import weakref

from datatypes import (
    LogMixin,
    Stack,
)

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

from ..compat import *
from ..utils import make_list


logger = logging.getLogger(__name__)


class ConnectionABC(LogMixin):
    """Subclasses should extend Connection and implement the methods in this class"""
    def _transaction_start(self):
        pass

    def _transaction_started(self, name):
        pass

    def _transaction_stop(self):
        pass

    def _transaction_stopping(self):
        pass

    def _transaction_fail(self):
        pass

    def _transaction_failing(self, name):
        pass


class Connection(ConnectionABC):
    """holds common methods that all raw connections should have"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # counting semaphore, greater than 0 if in a transaction, 0 if no current transaction.
        #
        # This will be incremented everytime transaction_start() is called, and decremented
        # everytime transaction_stop() is called.
        #
        # transaction_fail will set this back to 0 and rollback the transaction
        #
        # Holds the active transaction names
        self.transactions = Stack()

    @property
    def transaction_count(self):
        """How many active transactions there currently are"""
        return len(self.transactions)

    def transaction_names(self):
        return " > ".join(reversed(self.transactions))

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

    def transaction_exists(self):
        """return true if currently in a transaction

        this was previously named .in_transaction but it turns out SQLite has a
        property with that name
        """
        return self.transaction_count > 0

    def transaction_start(self, **kwargs):
        """start a transaction

        this will increment transaction semaphore and pass it to _transaction_start()
        """
        if not (name := kwargs.get("name", "")):
            name = self.transaction_name(prefix=kwargs.get("prefix", ""))

        self.transactions.push(name)
        transaction_count = self.transaction_count
        self.log_debug([
            f"{transaction_count}.", 
            f"Start 0x{id(self):02x} transaction {self.transaction_names()}",
        ])
        if transaction_count == 1:
            self._transaction_start()
        else:
            self._transaction_started(name)

        return transaction_count

    def transaction_stop(self):
        """stop/commit a transaction if ready"""
        transaction_count = self.transaction_count
        if transaction_count > 0:

            self.log_debug([
                f"{transaction_count}.", 
                f"Stopping 0x{id(self):02x} transaction {self.transaction_names()}",
            ])

            name = self.transactions.pop()
            if transaction_count == 1:
                self._transaction_stop()
            else:
                self._transaction_stopping(name)

        return self.transaction_count

    def transaction_fail(self):
        """rollback a transaction if currently in one"""
        transaction_count = self.transaction_count
        if transaction_count > 0:

            self.log_debug([
                f"{transaction_count}.", 
                f"Failing 0x{id(self):02x} transaction {self.transaction_names()}",
            ])

            name = self.transactions.pop()
            if transaction_count == 1:
                self._transaction_fail()
            else:
                self._transaction_failing(name)


class InterfaceABC(LogMixin):
    """This is just a convenience abstract base class so child interfaces can easily
    see what methods they need to implement. They should extend Interface and then
    implement the methods in this class"""
    def _connect(self, config):
        raise NotImplementedError()

    def _configure_connection(self, **kwargs):
        """This is ran immediately after a successful .connect()"""
        pass

    def _free_connection(self, connection):
        pass

    def _get_connection(self):
        raise NotImplementedError()

    def _close(self):
        raise NotImplementedError()

    def _readonly(self, readonly, **kwargs):
        raise NotImplementedError()

    def _raw(self, query_str, *query_args, **kwargs):
        raise NotImplementedError()

    def _set_table(self, schema, **kwargs):
        raise NotImplementedError()

    def _get_tables(self, table_name, **kwargs):
        raise NotImplementedError()

    def _delete_table(self, schema):
        raise NotImplementedError()

    def _get_fields(self, table_name, **kwargs):
        raise NotImplementedError()

    def _get_indexes(self, schema, **kwargs):
        raise NotImplementedError()

    def _set_index(self, schema, name, field_names, **kwargs):
        raise NotImplementedError()

    def _insert(self, schema, fields, **kwargs):
        raise NotImplementedError()

    def _update(self, schema, fields, query, **kwargs):
        raise NotImplementedError()

    def _upsert(self, schema, insert_fields, update_fields, conflict_field_names, **kwargs):
        raise NotImplementedError()

    def _delete(self, schema, query, **kwargs):
        raise NotImplementedError()

    def _get(self, schema, query, **kwargs):
        raise NotImplementedError()

    def _count(self, schema, query, **kwargs):
        raise NotImplementedError()

    def render(self, schema, query, **kwargs):
        """Render the query in a way that the interface can interpret it

        so in a SQL interface, this would render SQL, this is mainly used for
        debugging

        :param query: Query, the Query instance to render
        :param **kwargs: any named arguments
        :returns: mixed
        """
        raise NotImplementedError()

    def _handle_unique_error(self, e, **kwargs):
        return False

    def _handle_field_error(self, e, **kwargs):
        return False

    def _handle_table_error(self, e, **kwargs):
        return False

    def _handle_general_error(self, e, **kwargs):
        return False

    def _handle_close_error(self, e, **kwargs):
        self.close()
        return True


class Interface(InterfaceABC):

    connected = False
    """true if a connection has been established, false otherwise"""

    config = None
    """a config.Connection() instance"""

    InterfaceError = InterfaceError
    UniqueError = UniqueError
    TableError = TableError
    FieldError = FieldError
    UniqueError = UniqueError
    CloseError = CloseError

    @classmethod
    def configure(cls, config):
        host = config.host
        if host:
            db = config.database
            config.database = db.strip("/")
        return config

    def __init__(self, config=None):
        self.config = config

        # enables cleanup of open sockets even if the object isn't correctly garbage collected
        weakref.finalize(self, self.__del__)

    def __del__(self):
        """Whenever this gets garbage collected close everything. This is also the
        method for weakref.finalize"""
        self.close()

    def connect(self, config=None, *args, **kwargs):
        """
        connect to the interface

        this will set the raw db connection to self.connection

        *args -- anything you want that will help the db connect
        **kwargs -- anything you want that the backend db connection will need to actually connect
        """
        if self.connected:
            return self.connected

        if config:
            self.config = config

        try:
            self._connect(self.config)
            self.connected = True

        except Exception as e:
            self.connected = False
            self.raise_error(e)

        self.log_debug(f"Connected {self.config.interface_name}")
        self.configure_connection()
        return self.connected

    def reconnect(self):
        self.close()
        self.connect()

    def configure_connection(self, **kwargs):
        kwargs.setdefault("prefix", "configure_connection")
        self.execute(
            self._configure_connection,
            **kwargs
        )

    def get_connection(self):
        """Any time you need a connection it should be retrieved through .connection,
        and that method uses this method

        :returns: Connection instance
        """
        if not self.is_connected():
            self.connect()

        connection = self._get_connection()

        if connection.closed:
            # we've gotten into a bad state so let's try reconnecting
            self.reconnect()
            connection = self._get_connection()

        connection.interface = self

        self.log_debug(f"Getting {self.config.interface_name} connection 0x{id(connection):02x}")
        return connection

    def free_connection(self, connection):
        """When .connection is done with a connection it calls this method"""
        #connection.interface = None
        if self.is_connected():
            self.log_debug(f"Freeing {self.config.interface_name} connection 0x{id(connection):02x}")
            self._free_connection(connection)

    def is_connected(self):
        return self.connected

    def close(self):
        """close an open connection"""
        if not self.connected: return True

        self._close()
        self.connected = False
        self.log_debug(f"Closed Connection {self.config.interface_name}")
        return True

    def readonly(self, readonly=True, **kwargs):
        """Make the connection read only (pass in True) or read/write (pass in False)

        :param readonly: boolean, True if this connection should be readonly, False
            if the connection should be read/write
        """
        self.log_warning([
            f"Setting interface {self.config.interface_name}",
            f"to readonly={readonly}",
        ])
        self.config.readonly = readonly

        if self.connected:
            kwargs.setdefault("prefix", "readonly")
            self.execute(
                self._readonly,
                readonly,
                **kwargs
            )
            #self._readonly(readonly, **kwargs)

    @contextmanager
    def connection(self, connection=None, **kwargs):
        """Any time you need a connection you should use this context manager, this
        is the only place that wraps exceptions in InterfaceError, so all connections
        should go through this method or .transaction if you need to start a transaction

        :Example:
            with self.connection(**kwargs) as connection:
                # do something with connection
        """
        free_connection = False
        try:
            if connection:
                if connection.closed:
                    self.log_warning("Passed in connection is closed and must be refreshed")
                    if connection.transaction_exists():
                        self.log_error("Closed connection had open transactions!")

                    connection = None

                else:
                    self.log_debug(f"Connection call using existing connection 0x{id(connection):02x}")
                    yield connection

            if connection is None:
                free_connection = True
                connection = self.get_connection()
                yield connection

        except Exception as e:
            self.raise_error(e)

        finally:
            if free_connection and connection:
                self.free_connection(connection)

            else:
                self.log_debug(f"Connection call NOT freeing existing connection 0x{id(connection):02x}")

    @contextmanager
    def transaction(self, connection=None, **kwargs):
        """A simple context manager useful for when you want to wrap a bunch of
        db calls in a transaction, this is used internally for any write statements

        :Example:
            with self.transaction() as connection
                # do a bunch of calls
            # those db calls will be committed by this line
        """
        with self.connection(connection) as connection:
            if not kwargs.get("nest", True) and connection.transaction_exists():
                # internal write transactions don't nest
                self.log_debug("Transaction call IS NOT creating a new transaction")
                yield connection

            else:
                self.log_debug("Transaction call IS creating a new transaction")
                connection.transaction_start(**kwargs)
                try:
                    yield connection

                except Exception:
                    connection.transaction_fail()
                    raise

                else:
                    connection.transaction_stop()

    def execute_write(self, callback, *args, **kwargs):
        """Any write statements will use this method

        CREATE, DELETE, DROP, INSERT, or UPDATE (collectively "write statements")
        """
        kwargs.setdefault("nest", True)
        kwargs.setdefault("execute_in_transaction", True)

        return self.execute(callback, *args, **kwargs)

    def execute_read(self, callback, *args, **kwargs):
        """Any write statements will use this method

        SELECT (collectively "read statements")
        """
        with self.connection(**kwargs) as connection:
            kwargs["connection"] = connection

            in_transaction = connection.transaction_exists()
            kwargs.setdefault("nest", in_transaction)
            kwargs.setdefault("execute_in_transaction", in_transaction)

            return self.execute(callback, *args, **kwargs)

    def execute(self, callback, *args, **kwargs):
        """Internal method. Execute the callback with args and kwargs, retrying
        the query if an error is raised that it thinks it successfully handled

        This is called by .execute_write, .execute_read, and .query

        :param callback: callable, this will be run at-most twice
        :param *args: passed directly to callback as *args
        :param **kwargs: passed to callback as **kwargs, can have values added
        :returns: mixed, whatever the callback returns
        """
        prefix = kwargs.pop("prefix", callback.__name__)

        try:
            return self._execute(callback, *args, prefix=prefix, **kwargs)

        except Exception as e:
            if self.handle_error(e=e, prefix=prefix, **kwargs):
                return self._execute(
                    callback,
                    *args,
                    prefix=f"{prefix}_retry",
                    **kwargs
                )

            else:
                self.raise_error(e)

    def _execute(self, callback, *args, **kwargs):
        """Internal method. Called by .execute"""
        in_transaction = kwargs.get("execute_in_transaction", False)

        if in_transaction:
            with self.transaction(**kwargs) as connection:
                kwargs["connection"] = connection
                return callback(*args, **kwargs)

        else:
            with self.connection(**kwargs) as connection:
                kwargs["connection"] = connection
                return callback(*args, **kwargs)

    def has_table(self, table_name, **kwargs):
        """
        check to see if a table is in the db

        table_name -- string -- the table to check
        return -- boolean -- True if the table exists, false otherwise
        """
        kwargs.setdefault("prefix", "has_table")
        tables = self.execute_read(
            self.get_tables,
            table_name,
            **kwargs
        )
        return len(tables) > 0

    def get_tables(self, table_name="", **kwargs):
        """
        get all the tables of the currently connected db

        table_name -- string -- if you would like to filter the tables list to only include matches with this name
        return -- list -- a list of table names
        """
        kwargs.setdefault("prefix", "get_tables")
        return self.execute_read(
            self._get_tables,
            str(table_name),
            **kwargs
        )

    def set_table(self, schema, **kwargs):
        """
        add the table to the db

        :param schema: Schema instance, contains all the information about the table
        """
        kwargs.setdefault("prefix", "set_table")
        with self.transaction(**kwargs) as connection:
            kwargs['connection'] = connection

            self._set_table(schema=schema, **kwargs)

            for index_name, index in schema.indexes.items():
                self._set_index(
                    schema=schema,
                    name=index.name,
                    field_names=index.field_names,
                    connection=connection,
                    **index.options,
                )

    def unsafe_delete_table(self, schema, **kwargs):
        """wrapper around delete_table that matches the *_tables variant and denotes
        that this is a serious operation

        remove a table matching schema from the db

        :param schema: Schema instance, the table to delete
        """
        kwargs.setdefault("prefix", "unsafe_delete_table")
        self.execute_write(
            self._delete_table,
            schema=schema,
            **kwargs
        )
        return True

    def unsafe_delete_tables(self, **kwargs):
        """Removes all the tables from the db

        https://github.com/Jaymon/prom/issues/75
        """
        kwargs.setdefault("prefix", "unsafe_delete_tables")
        with self.transaction(**kwargs) as connection:
            kwargs['connection'] = connection
            kwargs.setdefault('nest', False)
            for table_name in self.get_tables(**kwargs):
                self._delete_table(table_name, **kwargs)
        return True

    def get_indexes(self, schema, **kwargs):
        """
        get all the indexes

        schema -- Schema()

        return -- dict -- the indexes in {indexname: fields} format
        """
        kwargs.setdefault("prefix", "get_indexes")
        return self.execute_read(
            self._get_indexes,
            schema=schema,
            **kwargs
        )

    def set_index(self, schema, name, field_names, **kwargs):
        """
        add an index to the table

        schema -- Schema()
        name -- string -- the name of the index
        field_names -- array -- the fields the index should be on
        **index_options -- dict -- any index options that might be useful to create the index
        """
        kwargs.setdefault("prefix", "set_index")
        self.execute_write(
            self._set_index,
            schema=schema,
            name=name,
            field_names=field_names,
            **kwargs,
        )
        return True

    def insert(self, schema, fields, **kwargs):
        """Persist fields into the db

        :param schema: Schema instance, the table the query will run against
        :param fields: dict, the fields {field_name: field_value} to persist
        :param **kwargs: passed through
        :returns: mixed, will return the primary key values
        """
        kwargs.setdefault("prefix", "insert")
        return self.execute_write(
            self._insert,
            schema=schema,
            fields=fields,
            **kwargs
        )

    def update(self, schema, fields, query, **kwargs):
        """Persist the query.fields into the db that match query.fields_where

        :param schema: Schema instance, the table the query will run against
        :param fields: dict, the fields {field_name: field_value} to persist
        :param query: Query instance, will be used to create the where clause
        :param **kwargs: passed through
        :returns: int, how many rows where updated
        """
        kwargs.setdefault("prefix", "update")
        return self.execute_write(
            self._update,
            schema=schema,
            fields=fields,
            query=query,
            **kwargs,
        )

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
        kwargs.setdefault("prefix", "upsert")
        return self.execute_write(
            self._upsert,
            schema=schema,
            insert_fields=insert_fields,
            update_fields=update_fields,
            conflict_field_names=conflict_field_names,
            **kwargs,
        )

    def delete(self, schema, query, **kwargs):
        """delete matching rows according to query filter criteria

        :param schema: Schema instance, the table the query will run against
        :param query: Query instance, the filter criteria, this will fail if empty
        :returns: int, how many rows were deleted ... I think
        """
        if not query or not query.fields_where:
            raise ValueError('aborting delete because there is no where clause')

        kwargs.setdefault("prefix", "delete")
        return self.execute_write(
            self._delete,
            schema=schema,
            query=query,
            **kwargs
        )

    def raw(self, query_str, *query_args, **kwargs):
        """
        run a raw query on the db

        query_str -- string -- the query to run
        *query_args -- if the query_str is a formatting string, pass the values in this
        **kwargs -- any query options can be passed in by using key=val syntax
        """
        kwargs.setdefault("prefix", "raw")
        return self.execute(
            self._raw,
            query_str,
            *query_args,
            **kwargs
        )

    def get_fields(self, table_name, **kwargs):
        kwargs.setdefault("prefix", "get_fields")
        return self.execute_read(
            self._get_fields,
            str(table_name),
            **kwargs
        )

    def get_one(self, schema, query=None, **kwargs):
        """get one row from the db matching filters set in query

        :param schema: Schema instance, the table the query will run against
        :param query: Query instance, the filter criteria
        :return: dict, the matching row
        """
        kwargs.setdefault("prefix", "get_one")
        return self.get(schema, query, fetchone=True, **kwargs) or {}

    def get(self, schema, query=None, **kwargs):
        """get matching rows from the db matching filters set in query

        :param schema: Schema instance, the table the query will run against
        :param query: Query instance, the filter criteria
        :returns: list, a list of matching dicts
        """
        kwargs.setdefault("prefix", "get")
        ret = self.execute_read(
            self._get,
            schema=schema,
            query=query or Query(),
            **kwargs
        )
        return ret or []

    def count(self, schema, query=None, **kwargs):
        """count matching rows according to query filter criteria

        :param schema: Schema instance, the table the query will run against
        :param query: Query instance, the filter criteria
        :returns: list, a list of matching dicts
        """
        kwargs.setdefault("prefix", "count")
        ret = self.execute_read(
            self._count,
            schema=schema,
            query=query or Query(),
            **kwargs
        )
        return int(ret) if ret else 0

    def handle_error(self, e, **kwargs):
        """Try and handle the error, return False if the error can't be handled

        :param e: Exception, the caught exception
        :param **kwargs:
            - schema: Schema, this does not have to be there, but usually is
        :returns: bool, True if the error was handled, False if it wasn't
        """
        ret = False

        prefix = kwargs.get("prefix", "")
        self.log_warning(["Handling", prefix, f"error: {e}"])

        e = self.create_error(e)

        if isinstance(e, CloseError):
            self.log_debug("Handling a close error")
            ret = self._handle_close_error(e=e, **kwargs)

        else:
            with self.transaction(**kwargs) as connection:
                kwargs["connection"] = connection

                if isinstance(e, UniqueError):
                    self.log_debug("Handling a unique error")
                    ret = self._handle_unique_error(e=e, **kwargs)

                elif isinstance(e, FieldError):
                    self.log_debug("Handling a field error")
                    ret = self._handle_field_error(e=e, **kwargs)

                elif isinstance(e, TableError):
                    self.log_debug("Handling a table error")
                    ret = self._handle_table_error(e=e, **kwargs)

                else:
                    self.log_debug("Handling a general error")
                    ret = self._handle_general_error(e=e, **kwargs)

        if ret:
            self.log_info(["Successfully handled", prefix, "error"])

        else:
            self.log_warning(["Failed to handle", prefix, "error"])

        return ret

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

    def create_error(self, e, **kwargs):
        """create the error that you want to raise, this gives you an opportunity
        to customize the error

        allow python's built in errors to filter up through
        https://docs.python.org/2/library/exceptions.html

        :param e: Exception, this error will be wrapped in an InterfaceError (or
            whatever is in kwargs["error_class"] if it isn't already an instance
            of that class
        :param **kwargs:
            - error_class: InterfaceError
            - error_module: module, the dbapi module
        """
        error_class = kwargs.get("error_class", InterfaceError)
        if not isinstance(e, error_class):
            if not hasattr(builtins, e.__class__.__name__):
                if "error_module" in kwargs:
                    if kwargs["error_module"].__name__ in e.__class__.__module__:
                        e = error_class(e)

                else:
                    e = error_class(e)

        return e

