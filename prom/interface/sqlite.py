# -*- coding: utf-8 -*-
"""
Bindings for SQLite

https://docs.python.org/3/library/sqlite3.html
https://github.com/omnilib/aiosqlite

Notes, certain SQLite versions might have a problem with long integers
http://jakegoulding.com/blog/2011/02/06/sqlite-64-bit-integers/

Looking at the docs, it says it will set an integer value to 1-4, 6, or 8 bytes
depending on the size, but I couldn't get it to accept anything over the 32-bit
signed integer value of around 2billion

savepoints and transactions are similar to Postgres
https://www.sqlite.org/lang_savepoint.html
http://sqlite.org/lang_transaction.html

alter table is similar to Postgres
https://www.sqlite.org/lang_altertable.html

other links that were helpful
http://www.numericalexpert.com/blog/sqlite_blob_time/
"""
import os
import decimal
import datetime
from distutils import dir_util
import re
import sqlite3
import json
import uuid

from datatypes import Datetime
import aiosqlite

# first party
from ..exception import (
    InterfaceError,
    UniqueError,
    TableError,
    FieldError,
    UniqueError,
    CloseError,
    PlaceholderError,
)

from ..compat import *
from .sql import SQLInterface, SQLConnection


class AsyncSQLiteConnection(SQLConnection, aiosqlite.Connection):
    """Thin wrapper around the default connection to make sure it has a similar
    interface to Postgres' connection instance so the common code can all be the
    same in the Interface class

    https://docs.python.org/3.11/library/sqlite3.html#sqlite3.Connection
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.closed = 0

    async def close(self, *args, **kwargs):
        try:
            return await super().close(*args, **kwargs)

        except ValueError:
            # aiosqlite: ValueError: no active connection
            pass

        finally:
            self.closed = 1


class BooleanType(object):
    FIELD_TYPE = 'BOOL'

    @classmethod
    def adapt(cls, val):
        """From python you get False and True, convert those to 1/0"""
        return 1 if val else 0

    @classmethod
    def convert(cls, val):
        """from the db you get values like b'0' and b'1', convert those to
        True/False"""
        return bool(int(val))


class StringType(object):
    """this just makes sure 8-bit bytestrings get converted ok"""
    FIELD_TYPE = 'TEXT'

    @classmethod
    def adapt(cls, val):
        return String(val)


class DatetimeType(StringType):
    """External sqlite3 databases can store the TIMESTAMP type as unix
    timestamps, this caused parsing problems when pulling the values out of the
    db because the default adapter expected TIMESTAMP to be in the form of
    YYYY-MM-DD HH:MM:SS.SSSSSS and so it would fail to convert the DDDDDD.DDD
    values, this handles that conversion

    https://www.sqlite.org/lang_datefunc.html
    the "unixepoch" modifier only works for dates between 0000-01-01 00:00:00
    and 5352-11-01 10:52:47 (unix times of -62167219200 through 106751991167)

    uses the name TIMESTAMP over DATETIME to be consistent with Postgres
    """
    FIELD_TYPE = 'TIMESTAMP'

    @classmethod
    def adapt(cls, val):
        return Datetime(val).isoformat()

    @classmethod
    def convert(cls, val):
        return Datetime(super().adapt(val)).datetime()


class NumericType(object):
    """numbers bigger than 64bit integers can be stored as this

    This is named to be as consistent with Postgres's NUMERIC(<precision>, 0)
    type

    This has TEXT in the name so it is treated as text according to SQLite's
    order of affinity rule 2:

        If the declared type of the column contains any of the strings "CHAR",
        "CLOB", or "TEXT" then that column has TEXT affinity.

        https://www.sqlite.org/datatype3.html
    """
    FIELD_TYPE = 'NUMERICTEXT'

    @classmethod
    def adapt(cls, val):
        if val < 9223372036854775807:
            return val
        else:
            v = str(val)
            return v

    @classmethod
    def convert(cls, val):
        """This should only be called when the column type is actually
        FIELD_TYPE"""
        return int(val)


class DecimalType(object):
    FIELD_TYPE = 'DECIMALTEXT'

    @staticmethod
    def adapt(val):
        return str(val)

    @staticmethod
    def convert(val):
        ret = decimal.Decimal(val)
        return ret


class DictType(object):
    """Converts a dict to json text and back again

    Uses JSONBTEXT to be as close to Postgres while still triggering SQLite's
    affinity rule 2
    """
    FIELD_TYPE = 'JSONBTEXT'

    @classmethod
    def adapt(cls, val):
        return json.dumps(val)

    @classmethod
    def convert(cls, val):
        """This should only be called when the column type is actually
        FIELD_TYPE"""
        return json.loads(val)


class SQLite(SQLInterface):
    """
    https://docs.python.org/3/library/sqlite3.html
    """
    LIMIT_NONE = -1

    _connection = None

    @classmethod
    async def configure(cls, config):
        if not config.get("path"):
            dsn = config.get("dsn", "")
            if dsn:
                host = config.host
                db = config.database
                if not host:
                    path = db

                elif not db:
                    path = host

                else:
                    path = os.sep.join([host, db])

            else:
                path = config.database

            if not path:
                raise ValueError("no sqlite db path found in config")

            config.path = path

        return config

    def get_paramstyle(self):
        """
        https://docs.python.org/3/library/sqlite3.html#sqlite3.paramstyle
        """
        return sqlite3.paramstyle

    def _connector(self):
        """
        https://docs.python.org/3.11/library/sqlite3.html#sqlite3.connect
        """
        config = self.config
        path = config.path

        # https://docs.python.org/2/library/sqlite3.html#default-adapters-and-converters
        options = {
            'isolation_level': None,
            #'isolation_level': "IMMEDIATE",
            #'isolation_level': "EXCLUSIVE",
            'detect_types': sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES,
            #'factory': SQLiteConnection,
            'check_same_thread': True, # https://stackoverflow.com/a/2578401
            #'timeout': 100,
        }
        option_keys = list(options.keys()) + ['timeout', 'cached_statements']
        for k in option_keys:
            if k in config.options:
                options[k] = config.options[k]

        try:
            connection = sqlite3.connect(path, **options)

        except sqlite3.DatabaseError as e:
            path_d = os.path.dirname(path)
            if os.path.isdir(path_d):
                raise

            else:
                # let's try and make the directory path and connect again
                dir_util.mkpath(path_d)
                connection = sqlite3.connect(path, **options)

        # https://docs.python.org/2/library/sqlite3.html#row-objects
        connection.row_factory = sqlite3.Row
        # https://docs.python.org/2/library/sqlite3.html#sqlite3.Connection.text_factory
        connection.text_factory = StringType.adapt

        # NOTE -- it's bad encapsulation that these are saved on the module,
        # Psycopg3 allows these adapters to be placed on the connection instead
        # of the moduel and I like that a lot better since once prom is
        # connected the first time then raw sqlite3 is basically borked for the
        # life of the script

        # for some reason this is needed in python 3.6 in order for saved bytes
        # to be ran through the converter, not sure why
        sqlite3.register_converter(StringType.FIELD_TYPE, StringType.adapt)

        sqlite3.register_adapter(bool, BooleanType.adapt)
        sqlite3.register_converter(BooleanType.FIELD_TYPE, BooleanType.convert)

        # sadly, it doesn't look like these work for child classes so each class
        # has to be adapted even if its parent is already registered
        sqlite3.register_adapter(datetime.datetime, DatetimeType.adapt)
        sqlite3.register_adapter(Datetime, DatetimeType.adapt)
        sqlite3.register_converter(
            DatetimeType.FIELD_TYPE,
            DatetimeType.convert
        )

        sqlite3.register_adapter(int, NumericType.adapt)
        sqlite3.register_converter(NumericType.FIELD_TYPE, NumericType.convert)

        sqlite3.register_adapter(decimal.Decimal, DecimalType.adapt)
        sqlite3.register_converter(DecimalType.FIELD_TYPE, DecimalType.convert)

        sqlite3.register_adapter(dict, DictType.adapt)
        sqlite3.register_converter(DictType.FIELD_TYPE, DictType.convert)

        return connection

    async def _connect(self, config):
        self._connection = AsyncSQLiteConnection(
            self._connector,
            iter_chunk_size=config.options.get("iter_chunk_size", 64)
        )
        self._connection.start()
        await self._connection._connect()

        self.log_debug("Connected to connection {}", self._connection)

        await self.configure_connection(connection=self._connection)

    async def _configure_connection(self, **kwargs):
        # turn on foreign keys
        # http://www.sqlite.org/foreignkeys.html
        await self._raw(
            "PRAGMA foreign_keys = ON",
            ignore_result=True,
            **kwargs
        )

    async def _get_connection(self):
        return self._connection

    async def _close(self):
        await self._connection.close()
        self._connection = None

    async def _readonly(self, readonly, **kwargs):
        await self._raw(
            # https://stackoverflow.com/a/49630725/5006
            'PRAGMA query_only = {}'.format("ON" if readonly else "OFF"),
            ignore_result=True,
            **kwargs
        )

    async def _get_tables(self, table_name, **kwargs):
        query_str = 'SELECT tbl_name FROM sqlite_master WHERE type = ?'
        query_args = ['table']

        if table_name:
            query_str += ' AND name = ?'
            query_args.append(str(table_name))

        ret = await self._raw(query_str, *query_args, **kwargs)
        return [r['tbl_name'] for r in ret]

    async def _get_indexes(self, schema, **kwargs):
        """return all the indexes for the given schema"""
        # http://www.sqlite.org/pragma.html#schema
        # http://www.mail-archive.com/sqlite-users@sqlite.org/msg22055.html
        # http://stackoverflow.com/questions/604939/
        ret = {}
        rs = await self._raw(
            'PRAGMA index_list({})'.format(self.render_table_name_sql(schema)),
            **kwargs
        )
        if rs:
            for r in rs:
                iname = r['name']
                ret.setdefault(iname, [])
                indexes = await self._raw(
                    'PRAGMA index_info({})'.format(r['name']),
                    **kwargs
                )
                for idict in indexes:
                    ret[iname].append(idict['name'])

        return ret

    async def _delete_table(self, schema, **kwargs):
        """
        https://www.sqlite.org/lang_droptable.html
        """
        query_str = "DROP TABLE IF EXISTS {}".format(
            self.render_table_name_sql(schema)
        )
        await self._raw(query_str, ignore_result=True, **kwargs)

    def create_error(self, e, **kwargs):
        kwargs.setdefault("error_module", sqlite3)
        if isinstance(e, sqlite3.OperationalError):
            e_msg = str(e)
            if "no such column" in e_msg or "has no column" in e_msg:
                #INSERT: "table yscrmiklbgdtx has no column named che"
                #SELECT: "no such column: che"
                e = FieldError(e)

            elif "no such table" in e_msg:
                e = TableError(e)

            elif "UNIQUE" in e_msg:
                e = UniqueError(e)

            elif "database is locked":
                e = CloseError(e)

            else:
                e = super().create_error(e, **kwargs)

        elif isinstance(e, sqlite3.IntegrityError):
            e = UniqueError(e)

        elif isinstance(e, sqlite3.ProgrammingError):
            e_msg = str(e)
            if "closed database" in e_msg:
                e = CloseError(e)

            elif "Incorrect number of bindings supplied" in e_msg:
                e = PlaceholderError(e)

            else:
                e = super().create_error(e, **kwargs)

        elif isinstance(e, sqlite3.InterfaceError):
            e_msg = str(e)
            if "Error binding parameter" in e_msg:
                if error_args := kwargs.get("error_args", []):
                    ms = re.search(r"parameter\s(\d+)", e_msg)
                    index = int(ms.group(1))
                    value = error_args[1][index]
                    msg = "Query Placeholder {} has unexpected type {}".format(
                        index,
                        type(value)
                    )

                    e = PlaceholderError(e, message=msg)

                else:
                    e = PlaceholderError(e)

            else:
                e = super().create_error(e, **kwargs)

        elif isinstance(e, ValueError):
            e_msg = str(e)
            if "no active connection" in e_msg: # aiosqlite specific
                e = CloseError(e)

            else:
                e = super().create_error(e, **kwargs)

        else:
            e = super().create_error(e, **kwargs)

        return e

    async def _get_fields(self, table_name, **kwargs):
        """return all the fields for the given table"""
        ret = {}
        query_str = 'PRAGMA table_info({})'.format(
            self.render_table_name_sql(table_name)
        )
        fields = await self._raw(query_str, **kwargs)

        query_str = 'PRAGMA foreign_key_list({})'.format(
            self.render_table_name_sql(table_name)
        )
        fks = {f["from"]: f for f in await self._raw(query_str, **kwargs)}

        pg_types = {
            "INTEGER": int,
            "BIGINT": long,
            "DOUBLE PRECISION": float,
            "FLOAT": float,
            "REAL": float,
            "NUMERIC": decimal.Decimal,
            "BOOLEAN": bool,
            "DATE": datetime.date,
            "TIMESTAMP": datetime.datetime,
            "CHARACTER": str,
            "VARCHAR": str,
            "TEXT": str,
            "BLOB": bytearray,
        }

        for field_type, adapter in sqlite3.adapters.items():
            if adapter_class := getattr(adapter, "__self__", None):
                if adapter_class.FIELD_TYPE not in pg_types:
                    pg_types[adapter_class.FIELD_TYPE] = field_type[0]

        # the rows we can set:
        #   field_type, name, field_required, min_size, max_size, size, unique,
        #   pk, <foreign key info>
        for row in fields:
            field = {
                "name": row["name"],
                "field_required": bool(row["notnull"]) or bool(row["pk"]),
                "pk": bool(row["pk"]),
            }

            for tname, ty in pg_types.items():
                if row["type"].startswith(tname):
                    field["field_type"] = ty
                    break

            if row["name"] in fks:
                field["schema_table_name"] = fks[row["name"]]["table"]
                field["ref_table_name"] = fks[row["name"]]["table"]

            ret[field["name"]] = field

        return ret

    def render_date_field_sql(self, field_name, field_kwargs, symbol):
        """
        allow extracting information from date

        http://www.sqlite.org/lang_datefunc.html
        """
        fstrs = []
        k_opts = {
            'day': "CAST(strftime('%d', {}) AS integer)",
            'hour': "CAST(strftime('%H', {}) AS integer)",
            'doy': "CAST(strftime('%j', {}) AS integer)", # day of year
            'julian_day': "strftime('%J', {})", # YYYY-MM-DD
            'month': "CAST(strftime('%m', {}) AS integer)",
            'minute': "CAST(strftime('%M', {}) AS integer)",
            'dow': "CAST(strftime('%w', {}) AS integer)", # day of week 0=sunday
            'week': "CAST(strftime('%W', {}) AS integer)",
            'year': "CAST(strftime('%Y', {}) AS integer)"
        }

        for k, v in field_kwargs.items():
            fstrs.append([k_opts[k].format(
                self.render_field_name_sql(field_name)),
                self.PLACEHOLDER,
                v
            ])

        return fstrs

    def render_sort_field_sql(self, field_name, field_vals, sort_dir_str):
        """
        allow sorting by a set of values

        http://stackoverflow.com/questions/3303851/sqlite-and-custom-order-by
        """
        fvi = None
        if sort_dir_str == 'ASC':
            fvi = (t for t in enumerate(field_vals)) 

        else:
            fvi = (t for t in enumerate(reversed(field_vals))) 

        query_sort_str = ['  CASE {}'.format(
            self.render_field_name_sql(field_name)
        )]
        query_args = []
        for i, v in fvi:
            query_sort_str.append('    WHEN {} THEN {}'.format(
                self.PLACEHOLDER,
                i
            ))
            query_args.append(v)

        query_sort_str.append('  END')
        query_sort_str = "\n".join(query_sort_str)
        return query_sort_str, query_args

    def render_datatype_int_sql(self, field_name, field, **kwargs):
        if field.is_auto():
            field_type = 'INTEGER'

        else:
            # we could break these up into tiny, small, and big but it
            # doesn't really matter so we're not bothering
            # https://www.sqlite.org/datatype3.html
            size = field.size_info()["size"]

            if size <= 9223372036854775807:
                field_type = 'INTEGER'

            else:
                field_type = NumericType.FIELD_TYPE

        return field_type

    def render_datatype_str_sql(self, field_name, field, **kwargs):
            field_type = super().render_datatype_str_sql(
                field_name,
                field,
                **kwargs
            )

            fo = field.interface_options
            if fo.get('ignore_case', False):
                field_type += ' COLLATE NOCASE'

            return field_type

    def render_datatype_datetime_sql(self, field_name, field, **kwargs):
        return DatetimeType.FIELD_TYPE

    def render_datatype_dict_sql(self, field_name, field, **kwargs):
        return DictType.FIELD_TYPE

    def render_datatype_uuid_sql(self, field_name, field, **kwargs):
        return self.render_datatype_str_sql(field_name, field)

