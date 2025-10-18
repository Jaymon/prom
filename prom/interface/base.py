# -*- coding: utf-8 -*-
from contextlib import asynccontextmanager, AbstractAsyncContextManager
import uuid
from collections import Counter, defaultdict

from datatypes import (
    LogMixin,
    Stack,
)

from ..compat import *
from ..exception import (
    InterfaceError,
    UniqueError,
    TableError,
    FieldError,
    CloseError,
)


class InterfaceABC(LogMixin):
    """This is just a convenience abstract base class so child interfaces can
    easily see what methods they might need to implement. They should extend
    Interface and then implement the methods in this class
    """
    @classmethod
    async def configure(cls, config):
        """This is called by Config whenever it parses a DSN, it is meant to
        customize any configuration for the specific child interface
        """
        return config

    async def _connect(self, config):
        """See the docblock for Interface.connect to understand how the
        connection interface is used by Interface to make and manage connections
        """
        raise NotImplementedError()

    async def _configure_connection(self, **kwargs):
        """The wrapper method for this (Interface.configure_connection) is
        never called directly by Interface. It is up to the child interfaces
        to decide when a connection is ready and to call this method"""
        pass

    async def _free_connection(self, connection):
        """The wrapper method for this method is called at the end of the
        Interface.connection context manager"""
        pass

    async def _get_connection(self):
        """The wrapper method for this method is called at the beginning of the
        Interface.connection context manager"""
        raise NotImplementedError()

    async def _close(self):
        raise NotImplementedError()

    async def _readonly(self, readonly, **kwargs):
        raise NotImplementedError()

    async def _raw(self, query_str, *query_args, **kwargs):
        raise NotImplementedError()

    async def _set_table(self, schema, **kwargs):
        raise NotImplementedError()

    async def _get_tables(self, table_name, **kwargs):
        raise NotImplementedError()

    async def _delete_table(self, schema):
        raise NotImplementedError()

    async def _get_fields(self, table_name, **kwargs):
        raise NotImplementedError()

    async def _get_indexes(self, schema, **kwargs):
        raise NotImplementedError()

    async def _set_index(self, schema, name, field_names, **kwargs):
        raise NotImplementedError()

    async def _insert(self, schema, fields, **kwargs):
        raise NotImplementedError()

    async def _inserts(self, schema, field_names, field_values, **kwargs):
        raise NotImplementedError()

    async def _update(self, schema, fields, query, **kwargs):
        raise NotImplementedError()

    async def _upsert(
        self,
        schema,
        insert_fields,
        update_fields,
        conflict_field_names,
        **kwargs
    ):
        raise NotImplementedError()

    async def _delete(self, schema, query, **kwargs):
        raise NotImplementedError()

    async def _get(self, schema, query, **kwargs):
        raise NotImplementedError()

    async def _count(self, schema, query, **kwargs):
        raise NotImplementedError()

    async def _handle_unique_error(self, e, **kwargs):
        return False

    async def _handle_field_error(self, e, **kwargs):
        return False

    async def _handle_table_error(self, e, **kwargs):
        return False

    async def _handle_general_error(self, e, **kwargs):
        return False

    async def _handle_close_error(self, e, **kwargs):
        await self.close()
        return True

    def render(self, schema, query, **kwargs):
        """Render the query in a way that the interface can interpret it

        so in a SQL interface, this would render SQL, this is mainly used for
        debugging

        :param query: Query, the Query instance to render
        :param **kwargs: any named arguments
        :returns: Any
        """
        raise NotImplementedError()

    async def _start_transaction(self, connection, tx, **kwargs):
        """Called when the first transaction is started"""
        pass

    async def _ignore_transaction(self, connection, tx, **kwargs):
        """Called when nested transaction is ignored instead of started"""
        pass

    async def _stop_transaction(self, connection, tx, **kwargs):
        """Called when the last transaction is stopped"""
        pass

    async def _fail_transaction(self, connection, tx, **kwargs):
        """Called when the last transaction has failed"""
        pass


class Interface(InterfaceABC):

    connected = False
    """true if a connection has been established, false otherwise"""

    config = None
    """a config.Connection() instance"""

    def __init__(self, config=None):
        self.config = config

        # Holds the active transactions
        #
        # This is managed in the `.transaction` context manager
        #
        # counting-ish semaphore, length will be greater than 0 if in a
        # transaction, and 0 if there are no current transactions.
        #
        # This will push every time `.start_transaction_start` is called, and
        # pop every time `.stop_transaction` is called.
        #
        # `.fail_transaction` will clear this and rollback the transaction
        self._transactions = defaultdict(list)

        # holds the active connections
        #
        # This is managed in the `.connection` context manager
        #
        # Holds the active connection in a stack, this could just be a counter
        # but I've done it this way, the amount of entries (which should all
        # be the same connection instance) is the depth of the
        # `.connection` context manager, when the list is empty then the
        # connection can be freed
        self._connections = []

    async def connect(self, config=None, *args, **kwargs):
        """connect to the interface

        The parent Interface (this class) never interacts with a connection
        directly, it uses the connection interface for all use/manipulation
        of the connection:

            * connect: This handles making the actual connection
            * configure_connection: This is called immediately after a 
                connection is ready and can be used to further configure the
                connection, if needed. This is *NEVER* called by this class, so
                it is up to the child interface to decide when a connection is
                ready and to call this method
            * close: This handles closing the actual connection
            * get_connection: Whenever a connection is needed this class will
                use the .connection context manager which calls this method to
                get an actual connection, this should return a connection
                instance
            * free_connection: Whenever this class is done with a connection
                this method will be called, usually at the end of the
                .connection context manager call
            * readonly: Responsible for setting a connection to readonly, this
                is called in .configure_connection but can be called separately
                also
            * is_connected: Returns True if an active connection has been made
            * connection: The connection context manager that calls
                .get_connection and .free_connection, all interactions with
                a connection should use this
            * transaction: wraps the .connection context manager and starts a
                transaction, all transactions should use this

        The reason why this class uses the connection interface and that no
        connection anything is fleshed out is because that allows the child
        interfaces a lot of flexibility in how they handle the connection, they
        can have a single connection or a pool of connections, etc.

        The amount of methods that will need to be actually implemented by the
        child interfaces is up to the child interfaces themselves

        :param config: Config, this doesn't need to be passed in because it can
            also be passed into the __init__ method, a valid Config object
            needs to be present before a connection can be made though
        :param *args: anything you want that will help the db connect
        :param **kwargs: anything you want that the backend db connection will
            need to actually connect
        """
        if self.connected:
            return self.connected

        if config:
            self.config = config

        await self.configure(self.config)

        try:
            await self._connect(self.config)
            self.connected = True

        except Exception as e:
            self.connected = False
            await self.raise_error(e)

        self.log_debug("Connected {}", self.config.interface_name)
        return self.connected

    async def configure_connection(self, **kwargs):
        """Configure a ready connection

        It is up to the child interfaces to decide when to call this method

        :param **kwargs:
            - connection: Any, the connection to configure
        """
        await self.execute(
            self._configure_connection,
            **kwargs
        )

        self.config.readonly = kwargs.get("readonly", self.config.readonly)

        # by default we can read/write, so only bother to run this if we
        # need to actually make the connection readonly
        if self.config.readonly:
            await self.readonly(
                self.config.readonly,
                **kwargs
            )

    async def get_connection(self):
        """Any time you need a connection it should be retrieved through
        .connection, and that method uses this method

        :returns: Connection instance
        """
        if not self.is_connected():
            await self.connect()

        return await self._get_connection()

    async def free_connection(self, connection):
        """When .connection is done with a connection it calls this method"""
        if self.is_connected():
            await self._free_connection(connection)

    def is_connected(self, connection=None):
        """Returns True if this Interface has been connected

        :param connection: Optional[object], an interface connection, if it
            is passed in then it will check that connection instead of the
            interface
        """
        if connection is None:
            connected = self.connected

        else:
            connected = True
            if connection.closed:
                self.log_warning(
                    f"{self.connection_name(connection)} Connection is closed"
                )
                connected = False

        return connected

    async def close(self):
        """close an open connection"""
        if not self.connected:
            return True

        await self._close()
        self.connected = False
        self.log_debug("Closed Connection {}", self.config.interface_name)
        return True

    def get_connection_name(self, prefix: str, **kwargs) -> str:
        """get the currention connection name at `prefix`

        If you need a new connection name use `.create_connection_name`
        """
        suffix = len(self._connections) + 1
        name = f"{prefix}_{suffix}"
        return name

    def connection_name(self, connection) -> str:
        """This is used in logging to get an identifiable connection string"""
        return f"0x{id(connection):02x}"

    @asynccontextmanager
    async def connection(
        self,
        prefix: str = "",
        connection = None,
        **kwargs,
    ) -> AbstractAsyncContextManager:
        """Any time you need a connection you should use this context manager,
        this is the only place that wraps exceptions in InterfaceError, so all
        connections should go through this method or .transaction if you need
        to start a transaction

        :example:
            with self.connection(**kwargs) as connection:
                # do something with connection
        """
        kwargs["prefix"] = prefix if prefix else "conn"
        conn_name = self.get_connection_name(**kwargs)

        try:
            if connection:
                if not self.is_connected(connection):
                    connection = None

                else:
                    self.log_debug(
                        f"{self.connection_name(connection)} Connection"
                        f" ({conn_name})"
                        " provided"
                        )

            if connection is None:
                if self._connections:
                    connection = self._connections[-1]
                    self.log_debug(
                        f"{self.connection_name(connection)} Connection"
                        f" ({conn_name})"
                        " used previous connection"
                    )

                    if not self.is_connected(connection):
                        connection = None

            if connection is None:
                connection = await self.get_connection()
                self.log_debug(
                    f"{self.connection_name(connection)} Connection"
                    f" ({conn_name})"
                    " used get_connection"
                )

            self._connections.append(connection)
            yield connection

        except Exception as e:
            await self.raise_error(e)

        finally:
            if connection:
                self._connections.pop(-1)

                if self._connections:
                    s = "NOT freed"

                else:
                    s = "freed"
                    await self.free_connection(connection)

                self.log_debug(
                    f"{self.connection_name(connection)} Connection"
                    f" ({conn_name})"
                    f" {s}"
                )

    @asynccontextmanager
    async def transaction(
        self,
        prefix: str = "",
        connection = None,
        **kwargs,
    ) -> AbstractAsyncContextManager:
        """A simple context manager useful for when you want to wrap a bunch of
        db calls in a transaction, this is used internally for any write
        statements

        .. note:: psycopg3 now has a transaction context manager:
            https://www.psycopg.org/psycopg3/docs/api/connections.html#psycopg.Connection.transaction

            It might be worth switching over to it sometime in the future? I'm
            not sure how that would work with sqlite though

        :example:
            with self.transaction() as connection
                # do a bunch of calls
            # those db calls will be committed by this line
        """
        kwargs["prefix"] = prefix if prefix else "tx"
        kwargs["connection"] = connection

        async with self.connection(**kwargs) as connection:
            kwargs["name"] = self.get_connection_name(**kwargs)
            kwargs["connection"] = connection

            await self.start_transaction(**kwargs)
            try:
                yield connection

            except Exception:
                await self.fail_transaction(**kwargs)
                raise

            else:
                await self.stop_transaction(**kwargs)

    def transaction_names(self, connection):
        """Get all the transaction names for logging

        :param tx: dict, sometimes you pop the transaction before you get the
            names, passing that popped tx will allow it to be placed at the
            end
        :returns: str, all the names nested
        """
        transactions = self._transactions.get(connection, [])
        names = " > ".join((r["name"] for r in transactions))
        return names

    def in_transaction(self, connection):
        """return true if currently in a transaction"""
        transactions = self._transactions.get(connection, [])
        return len(transactions) > 0

    async def start_transaction(self, connection, name, **kwargs):
        """Create a new transaction dict that will be placed on the
        .transactions stack

        :param name: str, the transaction name
        :keyword nest: bool, True if this (and children unless passed in) will
            run nested transactions. If this is False then subsequent calls to
            .transaction_start will be ignored unless nest=True is passed in
            again
        :returns: dict[str], the created transaction with keys:
            - nest: bool, the value of kwargs["nest"] or of
              .transaction_current()["nest"]
            - ignored: bool, True if this tx is going to be ignored
            - index: int, the depth of the transaction
        """
        """start a transaction

        this will increment transaction semaphore and pass it to
        _transaction_start()
        """
        transactions = self._transactions.get(connection, [])
        current_tx = transactions[-1] if transactions else {}
        nest = kwargs.get("nest", current_tx.get("nest", True))

        ignored = False
        if current_tx:
            ignored = not nest

        else:
            ignored = False

        tx = {
            "nest": nest,
            "name": name,
            "ignored": ignored,
            "index": len(transactions)
        }
        self._transactions[connection].append(tx)

        if tx["ignored"]:
            self.log_debug(
                f"{self.connection_name(connection)} Transaction"
                f" ({self.transaction_names(connection)})"
                " ignored"
            )
            await self._ignore_transaction(connection, tx, **kwargs)

        else:
            self.log_debug(
                f"{self.connection_name(connection)} Transaction"
                f" ({self.transaction_names(connection)})"
                " started"
            )
            await self._start_transaction(connection, tx, **kwargs)

    async def stop_transaction(self, connection, **kwargs):
        """stop/commit a transaction if ready"""
        transactions = self._transactions.get(connection, [])
        if transactions:
            tx_names = self.transaction_names(connection)
            tx = transactions.pop(-1)
            if not tx["ignored"]:
                self.log_debug(
                    f"{self.connection_name(connection)} Transaction"
                    f" ({tx_names})"
                    f" stopped"
                )
                await self._stop_transaction(connection, tx, **kwargs)

    async def fail_transaction(self, connection, **kwargs):
        """rollback a transaction if currently in one"""
        transactions = self._transactions.get(connection, [])
        if transactions:
            tx_names = self.transaction_names(connection)
            tx = transactions.pop(-1)
            if not tx["ignored"]:
                self.log_debug(
                    f"{self.connection_name(connection)} Transaction"
                    f" ({tx_names})"
                    f" failed"
                )
                await self._fail_transaction(connection, tx, **kwargs)

    async def readonly(self, readonly=True, **kwargs):
        """Make the connection read only (pass in True) or read/write (pass in
        False)

        :param readonly: boolean, True if this connection should be readonly,
            False if the connection should be read/write
        """
        self.log_warning([
            f"Setting interface {self.config.interface_name}",
            f"to readonly={readonly}",
        ])
        self.config.readonly = readonly

        await self.execute(
            self._readonly,
            readonly,
            **kwargs
        )

    async def execute_write(self, callback, *args, **kwargs):
        """Any write statements will use this method

        collectively "write statements":
            CREATE, DELETE, DROP, INSERT, or UPDATE
        """
        kwargs.setdefault("execute_in_transaction", True)
        return await self.execute(callback, *args, **kwargs)

    async def execute_read(self, callback, *args, **kwargs):
        """Any read statements will use this method

        collectively "read statements":
            SELECT
        """
        kwargs["prefix"] = "execute_read"
        # we need the connection so we can decide if we need to run the query
        # in a tx or not
        async with self.connection(**kwargs) as connection:
            kwargs["connection"] = connection

            kwargs.setdefault(
                "execute_in_transaction",
                self.in_transaction(connection)
            )

            return await self.execute(callback, *args, **kwargs)

    async def execute(self, callback, *args, **kwargs):
        """Internal method. Execute the callback with args and kwargs, retrying
        the query if an error is raised that it thinks it successfully handled

        This is called by .execute_write, .execute_read, and .raw

        :param callback: callable, this will be run at-most twice
        :param *args: passed directly to callback as *args
        :param **kwargs: passed to callback as **kwargs, can have values added
        :returns: Any, whatever the callback returns
        """
        # we want to override the prefix at this point
        kwargs.pop("prefix", None)
        prefix = callback.__name__

        try:
            return await self._execute(
                callback,
                *args,
                prefix=prefix,
                **kwargs
            )

        except Exception as e:
            if await self.handle_error(e=e, prefix=prefix, **kwargs):
                return await self._execute(
                    callback,
                    *args,
                    prefix=f"{prefix}_retry",
                    **kwargs
                )

            else:
                await self.raise_error(e)

    async def _execute(self, callback, *args, **kwargs):
        """Internal method for .execute, this should never be called directly,
        it's broken out from .execute so .execute can re-run it if an error is
        encountered. Because the transaction is started in this method then
        it will be rolledback before .execute starts error handling, which is
        what we want.

        :param callback: callable, this will be run at-most twice
        :param *args: passed directly to callback as *args
        :param **kwargs: passed to callback as **kwargs, can have values added
            - execute_only: bool, don't wrap the callback call in a transaction
                or connection context manager
            - execute_in_transaction: bool, True if callback should be wrapped
                in a tx, only applies if execute_only=False
        :returns: Any, whatever the callback returns
        """
        execute_only = kwargs.get("execute_only", False)

        if execute_only:
            return await callback(*args, **kwargs)

        else:
            in_transaction = kwargs.get("execute_in_transaction", False)

            if in_transaction:
                async with self.transaction(**kwargs) as connection:
                    kwargs["connection"] = connection
                    return await callback(*args, **kwargs)

            else:
                async with self.connection(**kwargs) as connection:
                    kwargs["connection"] = connection
                    return await callback(*args, **kwargs)

    async def has_table(self, table_name, **kwargs):
        """Check to see if a table is in the db

        :param table_name: str, the table to check
        :returns: bool, True if the table exists, false otherwise
        """
        tables = await self.execute_read(
            self._get_tables,
            table_name,
            **kwargs
        )
        return len(tables) > 0

    async def get_tables(self, table_name="", **kwargs):
        """Get all the tables of the currently connected db

        :param table_name: str, if you would like to filter the tables list to
            only include matches with this name
        :returns: list, a list of table names
        """
        return await self.execute_read(
            self._get_tables,
            str(table_name),
            **kwargs
        )

    async def set_table(self, schema, **kwargs):
        """
        add the table to the db

        :param schema: Schema instance, contains all the information about the
            table
        """
        kwargs["prefix"] = "set_table"
        async with self.transaction(**kwargs) as connection:
            kwargs["connection"] = connection

            await self.execute_write(
                self._set_table,
                schema=schema,
                #connection=connection,
                #execute_only=True,
                **kwargs
            )

            for index_name, index in schema.indexes.items():
                await self.execute_write(
                    self._set_index,
                    schema=schema,
                    name=index.name,
                    field_names=index.field_names,
                    #connection=connection,
                    #execute_only=True
                    **index.options,
                )

    async def unsafe_delete_table(self, schema, **kwargs):
        """wrapper around delete_table that matches the *_tables variant and
        denotes that this is a serious operation

        remove a table matching schema from the db

        :param schema: Schema instance, the table to delete
        """
        await self.execute_write(
            self._delete_table,
            schema=schema,
            **kwargs,
        )
        return True

    async def unsafe_delete_tables(self, **kwargs):
        """Removes all the tables from the db

        https://github.com/Jaymon/prom/issues/75
        """
        kwargs["prefix"] = "unsafe_delete_tables"
        async with self.transaction(**kwargs) as connection:
            kwargs['connection'] = connection
            kwargs.setdefault('nest', False)
            for table_name in await self.get_tables(**kwargs):
                # we don't wrap this in an .execute_write because there isn't
                # anything to recover from if it fails
                await self._delete_table(table_name, **kwargs)
        return True

    async def get_indexes(self, schema, **kwargs):
        """
        get all the indexes

        schema -- Schema()

        return -- dict -- the indexes in {indexname: fields} format
        """
        return await self.execute_read(
            self._get_indexes,
            schema=schema,
            **kwargs
        )

    async def set_index(self, schema, name, field_names, **kwargs):
        """
        add an index to the table

        :param schema: Schema
        :param name: str, the name of the index
        :param field_names: list, the fields the index should be on
        :param **index_options: any index options that might be useful to
            create the index
        """
        await self.execute_write(
            self._set_index,
            schema=schema,
            name=name,
            field_names=field_names,
            **kwargs,
        )
        return True

    async def insert(self, schema, fields, **kwargs):
        """Persist fields into the db

        :param schema: Schema instance, the table the query will run against
        :param fields: dict, the fields {field_name: field_value} to persist
        :param **kwargs: passed through
        :returns: mixed, will return the primary key values
        """
        return await self.execute_write(
            self._insert,
            schema=schema,
            fields=fields,
            **kwargs
        )

    async def inserts(self, schema, field_names, field_rows, **kwargs):
        """Persist the field names found in all the field rows

        :param schema: Schema
        :param field_names: list, the field names that will be checked for each
            row in field_rows and used to turn each dict in field_rows to a
            tuple
        :param field_rows: Sequence, if an iterator of dict instances with keys
            found in field_names, if a key is missing it will have None set as
            the value, if it's an iterator of values then the tuple value
            ordering should match with field_names
        :param **kwargs: passed through
        :returns: bool, True if the query executed successfully
        """
        def field_values(field_names, field_rows):
            for fields in field_rows:
                if isinstance(fields, Mapping):
                    row = []
                    for field_name in field_names:
                        row.append(fields.get(field_name, None))
                    yield row

                else:
                    yield fields

        await self.execute_write(
            self._inserts,
            schema=schema,
            field_names=field_names,
            field_values=field_values(field_names, field_rows),
            **kwargs,
        )
        return True

    async def update(self, schema, fields, query, **kwargs):
        """Persist the query.fields into the db that match query.fields_where

        :param schema: Schema instance, the table the query will run against
        :param fields: dict, the fields {field_name: field_value} to persist
        :param query: Query instance, will be used to create the where clause
        :param **kwargs:
            * count_result: bool, True if you want to return how many rows
                where touched by the update query
            * ignore_result: bool, True if you don't care about returning
                any result
            * ignore_return_clause: bool, True if you don't want to return
                the updated values of the rows that were touched by the query
        :returns: list[dict]|int|bool, by default this will return the touched
            rows that were updated by the query. If return value is an int then
            it will be how many rows were updated. If return value is a boolean
            then it will be True showing that the query succeeded
        """
        return await self.execute_write(
            self._update,
            schema=schema,
            fields=fields,
            query=query,
            **kwargs,
        )

    async def upsert(
        self,
        schema,
        insert_fields,
        update_fields,
        conflict_field_names,
        **kwargs
    ):
        """Perform an upsert (insert or update) on the table

        :param schema: Schema instance, the table the query will run against
        :param insert_fields: dict, these are the fields that will be inserted
        :param update_fields: dict, on a conflict with the insert_fields, these
            fields will instead be used to update the row
        :param conflict_field_names: list, the field names that will decide if
            an insert or update is performed
        :param **kwargs: anything else
        :returns: str|int|None, the primary key
        """
        return await self.execute_write(
            self._upsert,
            schema=schema,
            insert_fields=insert_fields,
            update_fields=update_fields,
            conflict_field_names=conflict_field_names,
            **kwargs,
        )

    async def delete(self, schema, query, **kwargs):
        """delete matching rows according to query filter criteria

        :param schema: Schema instance, the table the query will run against
        :param query: Query instance, the filter criteria, this will fail if
            empty
        :returns: int, how many rows were deleted ... I think
        """
        if not query or not query.fields_where:
            raise ValueError(
                "Aborting delete because there is no where clause"
            )

        return await self.unsafe_delete(schema, query, **kwargs)

    async def unsafe_delete(self, schema, query, **kwargs):
        """delete matching rows

        WARNING -- this can clear the whole table, you should mainly use
            .delete and always include filtering criteria in the query

        :param schema: Schema instance, the table the query will run against
        :param query: Query instance, the filter criteria, this will fail if
            empty
        :returns: int, how many rows were deleted ... I think
        """
        return await self.execute_write(
            self._delete,
            schema=schema,
            query=query,
            **kwargs
        )

    async def raw(self, query_str, *query_args, **kwargs):
        """
        run a raw query on the db

        :param query_str: str, the query to run
        :param *query_args: if the query_str is a formatting string, pass the
            values in this
        :param **kwargs: any query options can be passed in by using key=val
            syntax
        """
        return await self.execute(
            self._raw,
            query_str,
            *query_args,
            **kwargs
        )

    async def get_fields(self, table_name, **kwargs):
        return await self.execute_read(
            self._get_fields,
            str(table_name),
            **kwargs
        )

    async def one(self, schema, query, **kwargs):
        """get one row from the db matching filters set in query

        :param schema: Schema instance, the table the query will run against
        :param query: Query instance, the filter criteria
        :return: dict, the matching row
        """
        return await self.get(schema, query, fetchone=True, **kwargs) or {}

    async def get(self, schema, query, **kwargs):
        """get matching rows from the db matching filters set in query

        :param schema: Schema instance, the table the query will run against
        :param query: Query instance, the filter criteria
        :returns: list, a list of matching dicts
        """
        ret = await self.execute_read(
            self._get,
            schema=schema,
            query=query,
            **kwargs
        )
        return ret or []

    async def count(self, schema, query, **kwargs):
        """count matching rows according to query filter criteria

        :param schema: Schema instance, the table the query will run against
        :param query: Query instance, the filter criteria
        :returns: list, a list of matching dicts
        """
        ret = await self.execute_read(
            self._count,
            schema=schema,
            query=query,
            **kwargs
        )
        return int(ret) if ret else 0

    async def handle_error(self, e, **kwargs):
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
            ret = await self._handle_close_error(e=e, **kwargs)

        else:
            kwargs["prefix"] = f"{prefix}_handle_error"
            async with self.transaction(**kwargs) as connection:
                kwargs["connection"] = connection

                if isinstance(e, UniqueError):
                    self.log_debug("Handling a unique error")
                    ret = await self._handle_unique_error(e=e, **kwargs)

                elif isinstance(e, FieldError):
                    self.log_debug("Handling a field error")
                    ret = await self._handle_field_error(e=e, **kwargs)

                elif isinstance(e, TableError):
                    self.log_debug("Handling a table error")
                    ret = await self._handle_table_error(e=e, **kwargs)

                else:
                    self.log_debug("Handling a general error")
                    ret = await self._handle_general_error(e=e, **kwargs)

        if ret:
            self.log_info(["Successfully handled", prefix, "error"])

        else:
            self.log_warning(["Failed to handle", prefix, "error"])

        return ret

    def raise_error(self, e, **kwargs):
        """raises e

        :param e: Exception, if a built-in exception then it's raised, if any
            other error then it will be wrapped in an InterfaceError
        """
        e2 = self.create_error(e, **kwargs)
        if e2 is not e:
            raise e2 from e
        else:
            raise e

    def create_error(self, e, **kwargs):
        """create the error that you want to raise, this gives you an
        opportunity to customize the error

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
                    errmod_name = kwargs["error_module"].__name__
                    if errmod_name in e.__class__.__module__:
                        e = error_class(e)

                else:
                    e = error_class(e)

        return e

