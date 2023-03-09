# -*- coding: utf-8 -*-
from __future__ import unicode_literals, division, print_function, absolute_import
import sys
import os
import datetime
import decimal
import logging
from contextlib import contextmanager
import uuid

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

from ..decorators import reconnecting
from ..compat import *
from ..utils import make_list


logger = logging.getLogger(__name__)


class ConnectionABC(LogMixin):
    def _transaction_start(self): pass

    def _transaction_started(self, name): pass

    def _transaction_stop(self): pass

    def _transaction_fail(self): pass

    def _transaction_failing(self, name): pass


class Connection(ConnectionABC):
    """holds common methods that all raw connections should have"""

    transactions = Stack()

#     transaction_count = 0
    """
    counting semaphore, greater than 0 if in a transaction, 0 if no current transaction.

    This will be incremented everytime transaction_start() is called, and decremented
    everytime transaction_stop() is called.

    transaction_fail will set this back to 0 and rollback the transaction
    """
    @property
    def transaction_count(self):
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

    def in_transaction(self):
        """return true if currently in a transaction"""
        return self.transaction_count > 0

    def transaction_start(self, **kwargs):
        """
        start a transaction

        this will increment transaction semaphore and pass it to _transaction_start()
        """
        if not (name := kwargs.get("name", "")):
            name = self.transaction_name(prefix=kwargs.get("prefix", ""))

        self.transactions.push(name)
        transaction_count = self.transaction_count
        #self.transaction_count += 1
        #logger.info("{}. Start transaction {}".format(transaction_count, name))
        self.log_info([
            f"{transaction_count}.", 
            f"Start transaction {self.transaction_names()}",
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

            self.log_info([
                f"{transaction_count}.", 
                f"Stopping transaction {self.transaction_names()}",
            ])

            #logger.info(f"Stopping transaction {self.transaction_names()}")
            #logger.info("{}. Stopping transaction {}".format(transaction_count, name))
            if transaction_count == 1:
                self._transaction_stop()

            self.transactions.pop()

            #self.transaction_count -= 1

        return self.transaction_count

    def transaction_fail(self):
        """
        rollback a transaction if currently in one
        """
        if self.transactions:
            transaction_count = self.transaction_count

            self.log_info([
                f"{transaction_count}.", 
                f"Failing transaction {self.transaction_names()}",
            ])

            name = self.transactions.pop()
            #logger.info(f"Failing transaction {self.transaction_names()}")
            #logger.info("{}. Failing transaction {}".format(transaction_count, name))
            if transaction_count == 1:
                self._transaction_fail()
            else:
                self._transaction_failing(name)


class InterfaceABC(LogMixin):
    def _connect(self, connection_config):
        raise NotImplementedError()

    def free_connection(self, connection):
        pass

    def get_connection(self):
        raise NotImplementedError()

    def _close(self):
        raise NotImplementedError()

    def _readonly(self, readonly):
        raise NotImplementedError()

    def _query(self, query_str, *query_args, **kwargs):
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

    def _get_one(self, schema, query, **kwargs):
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

    connection_config = None
    """a config.Connection() instance"""

    InterfaceError = InterfaceError
    UniqueError = UniqueError
    TableError = TableError
    FieldError = FieldError
    UniqueError = UniqueError
    CloseError = CloseError

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

    def is_connected(self): return self.connected

    def close(self):
        """close an open connection"""
        if not self.connected: return True

        self._close()
        self.connected = False
        self.log("Closed Connection {}", self.connection_config.interface_name)
        return True

    def readonly(self, readonly=True):
        """Make the connection read only (pass in True) or read/write (pass in False)

        :param readonly: boolean, True if this connection should be readonly, False
            if the connection should be read/write
        """
        if readonly:
            self.log_warning([
                f"Setting interface {self.connection_config.interface_name}",
                f"to readonly={readonly}",
            ])
        self.connection_config.readonly = readonly

        if self.connected:
            self._readonly(readonly)

    @contextmanager
    def connection(self, connection=None, **kwargs):
        try:
            if connection:
                if connection.closed:
                    self.log_warning("Passed in connection is closed and must be refreshed")
                    if connection.in_transaction():
                        self.log_error("Closed connection had open transactions!")

                    connection = None

                else:
                    yield connection

            if connection is None:
                try:
                    connection = self.get_connection()
                    yield connection

                finally:
                    if connection:
                        self.free_connection(connection)

        except Exception as e:
            self.raise_error(e)


# 
#     @contextmanager
#     def connection(self, connection=None, **kwargs):
#         try:
#             if connection and not connection.closed:
#                 yield connection
# 
#             else:
#                 try:
#                     connection = self.get_connection()
#                     yield connection
# 
#                 finally:
#                     if connection:
#                         self.free_connection(connection)
# 
#         except Exception as e:
#             self.raise_error(e)

#     @contextmanager
#     def connection(self, connection=None, **kwargs):
#         try:
#             if connection:
#                 yield connection
# 
#             else:
#                 # note to future self, this is out of try/finally because if
#                 # connection fails to be created then free_connection() will fail
#                 # which would then cover up the real error, so don't think to 
#                 # yourself you can move it back into try/finally
#                 connection = self.get_connection()
#                 try:
#                     yield connection
# 
#                 finally:
#                     self.free_connection(connection)
# 
#         except Exception as e:
#             self.raise_error(e)

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
#         with self.connection(connection) as connection:
#             try:
#                 do_transaction = kwargs.get("nest", True) or connection.in_transaction()
# 
#                 if do_transaction:
#                     connection.transaction_start(**kwargs)
#                 yield connection
#                 connection.transaction_stop()
# 
#             except Exception as e:
#                 connection.transaction_fail()
#                 self.raise_error(e)

        with self.connection(connection) as connection:
            if not kwargs.get("nest", True) and connection.in_transaction():
                # internal write transactions don't nest
                self.log_debug("Transaction call IS NOT creating a new transaction")
                yield connection

            else:
                self.log_debug("Transaction call IS creating a new transaction")
                connection.transaction_start(**kwargs)
                try:
                    yield connection
                    connection.transaction_stop()

                except Exception as e:
                    connection.transaction_fail()
                    self.raise_error(e)



#             if kwargs.get("write", False):
#                 if connection.in_transaction():
#                     yield connection
# 
#                 else:
#                     connection.transaction_start(**kwargs)
#                     try:
#                         yield connection
#                         connection.transaction_stop()
# 
#                     except Exception as e:
#                         connection.transaction_fail()
#                         self.raise_error(e)
# 
#             else:
#                 yield connection

#             connection.transaction_start(**kwargs)
#             try:
#                 yield connection
#                 connection.transaction_stop()
# 
#             except Exception as e:
#                 connection.transaction_fail()
#                 self.raise_error(e)

    def execute_write(self, callback, *args, **kwargs):
        """
        CREATE, DELETE, DROP, INSERT, or UPDATE (collectively "write statements")
        """
        kwargs.setdefault("nest", True)
        kwargs.setdefault("execute_in_transaction", True)
        return self.execute(callback, *args, **kwargs)

#     def execute_write(self, callback, *args, **kwargs):
#         """
#         CREATE, DELETE, DROP, INSERT, or UPDATE (collectively "write statements")
#         """
#         kwargs.setdefault("prefix", callback.__name__)
#         nest = kwargs.pop("nest", False)
#         with self.transaction(nest=nest, **kwargs) as connection:
#             kwargs["connection"] = connection
#             return self.execute(callback, *args, **kwargs)

    def execute_read(self, callback, *args, **kwargs):
        with self.connection(**kwargs) as connection:
            kwargs["connection"] = connection

            in_transaction = connection.in_transaction()
            kwargs.setdefault("nest", in_transaction)
            kwargs.setdefault("execute_in_transaction", in_transaction)

            return self.execute(callback, *args, **kwargs)

#     def execute_read(self, callback, *args, **kwargs):
#         with self.connection(**kwargs) as connection:
#             kwargs["connection"] = connection
#             if in_transaction := connection.in_transaction():
#                 nest = kwargs.pop("nest", in_transaction)
#                 with self.transaction(nest=nest, **kwargs) as connection:
#                     kwargs["connection"] = connection
#                     return self.execute(callback, *args, **kwargs)
# 
#             else:
#                 return self.execute(callback, *args, **kwargs)

    def execute(self, callback, *args, **kwargs):
        """Internal method. Execute the callback with args and kwargs, retrying
        the query if an error is raised that it thinks it successfully handled

        better names: retry? execute_retry?

        :param callback: callable, this will be run at-most twice
        :param *args: passed directly to callback as *args
        :param **kwargs: passed to callback as **kwargs, can have values added
        :returns: mixed, whatever the callback returns
        """
        prefix = kwargs.pop("prefix", callback.__name__)
        #kwargs.setdefault("prefix", callback.__name__)

        try:
            return self._execute(callback, *args, prefix=prefix, **kwargs)

        except Exception as e:
            if self.handle_error(e=e, **kwargs):
                return self._execute(
                    callback,
                    *args,
                    prefix=f"{prefix}_retry",
                    **kwargs
                )

            else:
                self.raise_error(e)

    def _execute(self, callback, *args, **kwargs):
        in_transaction = kwargs.get("execute_in_transaction", False)

        if in_transaction:
            with self.transaction(**kwargs) as connection:
                kwargs["connection"] = connection
                return callback(*args, **kwargs)

        else:
            with self.connection(**kwargs) as connection:
                kwargs["connection"] = connection
                return callback(*args, **kwargs)


#     def execute(self, callback, *args, **kwargs):
#         """Internal method. Execute the callback with args and kwargs, retrying
#         the query if an error is raised that it thinks it successfully handled
# 
#         better names: retry? execute_retry?
# 
#         :param callback: callable, this will be run at-most twice
#         :param *args: passed directly to callback as *args
#         :param **kwargs: passed to callback as **kwargs, can have values added
#         :returns: mixed, whatever the callback returns
#         """
#         with self.connection(**kwargs) as connection:
#             kwargs["connection"] = connection
#             try:
#                 return callback(*args, **kwargs)
# 
#             except Exception as e:
#                 if self.handle_error(e=e, **kwargs):
#                     # refresh the connection just in case
#                     with self.connection(**kwargs) as connection:
#                         kwargs["connection"] = connection
#                         return callback(*args, **kwargs)
# 
#                 else:
#                     self.raise_error(e)

#     @reconnecting()
#     def execute_gracefully(self, callback, *args, **kwargs):
#         """Internal method. Execute the callback with args and kwargs, retrying
#         the query if an error is raised that it thinks it successfully handled
# 
#         better names: retry? execute_retry?
# 
#         :param callback: callable, this will be run at-most twice
#         :param *args: passed directly to callback as *args
#         :param **kwargs: passed to callback as **kwargs, can have values added
#         :returns: mixed, whatever the callback returns
#         """
#         r = None
# 
#         with self.connection(**kwargs) as connection:
#             kwargs['connection'] = connection
#             prefix = callback.__name__
#             always_transaction = set(["_insert", "_upsert", "_update", "_delete"])
# 
#             # we wrap SELECT queries in a transaction if we are in a transaction because
#             # it could cause data loss if it failed by causing the db to discard
#             # anything in the current transaction if the query isn't wrapped,
#             # go ahead, ask me how I know this
#             cb_transaction = prefix in always_transaction or connection.in_transaction()
# 
#             try:
#                 if cb_transaction:
#                     with self.transaction(prefix=prefix, **kwargs):
#                         r = callback(*args, **kwargs)
# 
#                 else:
#                     r = callback(*args, **kwargs)
# 
#             except Exception as e:
#                 self.log(
#                     f"{prefix} failed with {e}, attempting to handle the error",
#                     level='WARNING',
#                 )
#                 if self.handle_error(e=e, **kwargs):
#                     self.log(
#                         f"{prefix} has handled error: '{e}', re-running original query",
#                         level="WARNING",
#                     )
# 
#                     try:
#                         if cb_transaction:
#                             with self.transaction(prefix=f"{prefix}_retry", **kwargs):
#                                 r = callback(*args, **kwargs)
# 
#                         else:
#                             r = callback(*args, **kwargs)
# 
#                     except Exception as e:
#                         self.log(
#                             f"{prefix} failed again re-running original query",
#                             level="WARNING",
#                         )
#                         self.raise_error(e)
# 
#                 else:
#                     self.log(
#                         f"Raising '{e}' because it could not be handled!",
#                         level='WARNING',
#                     )
#                     self.raise_error(e)
# 
#         return r

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

#         with self.connection(**kwargs) as connection:
#             kwargs['connection'] = connection
#             tables = self.get_tables(table_name, **kwargs)
#             return len(tables) > 0

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

#         with self.connection(**kwargs) as connection:
#             kwargs['connection'] = connection
#             return self._get_tables(str(table_name), **kwargs)

    def set_table(self, schema, **kwargs):
        """
        add the table to the db

        schema -- Schema() -- contains all the information about the table
        """
        kwargs.setdefault("prefix", "set_table")
        try:
            with self.transaction(write=True, **kwargs) as connection:
                kwargs['connection'] = connection

                self.execute_write(self._set_table, schema, **kwargs)

                for index_name, index in schema.indexes.items():
                    self.execute_write(
                        self.set_index,
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

#         with self.connection(**kwargs) as connection:
#             kwargs['connection'] = connection
#             kwargs.setdefault("prefix", "set_table")
#             if self.has_table(str(schema), **kwargs): return True
# 
#             try:
#                 with self.transaction(**kwargs):
#                     self._set_table(schema, **kwargs)
# 
#                     for index_name, index in schema.indexes.items():
#                         self.set_index(
#                             schema,
#                             name=index.name,
#                             field_names=index.field_names,
#                             connection=connection,
#                             **index.options
#                         )
# 
#             except InterfaceError:
#                 # check to see if this table now exists, it might have been created
#                 # in another thread
#                 if not self.has_table(schema, **kwargs):
#                     raise

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

#         with self.connection(**kwargs) as connection:
#             kwargs['connection'] = connection
#             if not self.has_table(str(schema), **kwargs): return True
#             with self.transaction(**kwargs):
#                 self._delete_table(schema, **kwargs)
# 
#         return True

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

#         self.execute_write(
#             self._delete_tables,
#             **kwargs,
#         )

#         with self.connection(**kwargs) as connection:
#             kwargs['connection'] = connection
#             self._delete_tables(**kwargs)

#     def _delete_tables(self, **kwargs):
#         with self.transaction(**kwargs) as connection:
#             kwargs['connection'] = connection
#             for table_name in self.get_tables(**kwargs):
#                 self._delete_table(table_name, **kwargs)
# 
#         return True

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

#         with self.connection(**kwargs) as connection:
#             kwargs['connection'] = connection
#             return self._get_indexes(schema, **kwargs)

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

#         with self.transaction(**index_options) as connection:
#             index_options['connection'] = connection
#             self._set_index(schema, name, field_names, **index_options)
# 
#         return True

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

    def query(self, query_str, *query_args, **kwargs):
        """
        run a raw query on the db

        query_str -- string -- the query to run
        *query_args -- if the query_str is a formatting string, pass the values in this
        **kwargs -- any query options can be passed in by using key=val syntax
        """
        kwargs.setdefault("prefix", "query")
        return self.execute(
            self._query,
            query_str,
            *query_args,
            **kwargs
        )

#         with self.connection(**query_options) as connection:
#             query_options['connection'] = connection
#             return self._query(query_str, query_args, **query_options)

    def get_fields(self, table_name, **kwargs):
        kwargs.setdefault("prefix", "get_fields")
        return self.execute_read(
            self._get_fields,
            str(table_name),
            **kwargs
        )

#         with self.connection(**kwargs) as connection:
#             kwargs['connection'] = connection
#             return self._get_fields(str(table_name), **kwargs)

    def get_one(self, schema, query=None, **kwargs):
        """get one row from the db matching filters set in query

        :param schema: Schema instance, the table the query will run against
        :param query: Query instance, the filter criteria
        :return: dict, the matching row
        """
        kwargs.setdefault("prefix", "get_one")
        ret = self.execute_read(
            self._get_one,
            schema=schema,
            query=query or Query(),
            **kwargs
        )
        return ret or {}

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

        # if we have a connection in a transaction we should fail that transaction
#         if connection := kwargs.get("connection", None):
#             if connection.in_transaction():
#                 connection.transaction_fail()

        e = self.create_error(e)

        if isinstance(e, CloseError):
            ret = self._handle_close_error(e=e, **kwargs)

        else:
            with self.transaction(**kwargs) as connection:
                kwargs["connection"] = connection

                if isinstance(e, UniqueError):
                    ret = self._handle_unique_error(e=e, **kwargs)

                elif isinstance(e, FieldError):
                    ret = self._handle_field_error(e=e, **kwargs)

                elif isinstance(e, TableError):
                    ret = self._handle_table_error(e=e, **kwargs)

                else:
                    ret = self._handle_general_error(e=e, **kwargs)

        if ret:
            self.log_warning(["Successfully handled", prefix, "error"])
        else:
            self.log_warning(["Failed to handle", prefix, "error"])

        return ret


#     def handle_error(self, e, **kwargs):
#         """Try and handle the error, return False if the error can't be handled
# 
#         :param e: Exception, the caught exception
#         :param **kwargs:
#             - schema: Schema, this does not have to be there, but usually is
#         :returns: bool, True if the error was handled, False if it wasn't
#         """
#         ret = False
#         connection = kwargs.pop('connection', None)
# 
#         prefix = kwargs.get("prefix", "")
#         self.log_warning(["Handling", prefix, f"error: {e}"])
# 
#         if connection:
#             if connection.closed:
#                 # we are unsure of the state of everything since this connection has
#                 # closed, go ahead and close out this interface and allow this query
#                 # to fail, but subsequent queries should succeed
#                 ret = self._handle_close_error(e=e, **kwargs)
# 
#             else:
#                 # connection is open
#                 e = self.create_error(e)
# 
#                 if isinstance(e, CloseError):
#                     ret = self._handle_close_error(e=e, **kwargs)
# 
#                 else:
#                     with self.transaction(**kwargs) as connection:
#                         kwargs["connection"] = connection
# 
#                         if isinstance(e, UniqueError):
#                             ret = self._handle_unique_error(e=e, **kwargs)
# 
#                         elif isinstance(e, FieldError):
#                             ret = self._handle_field_error(e=e, **kwargs)
# 
#                         elif isinstance(e, TableError):
#                             ret = self._handle_table_error(e=e, **kwargs)
# 
#         else:
#             ret = self._handle_general_error(e=e, **kwargs)
# 
#         if ret:
#             self.log_warning(["Successfully handled", prefix, "error"])
#         else:
#             self.log_warning(["Failed to handle", prefix, "error"])
# 
#         return ret

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

