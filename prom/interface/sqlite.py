# -*- coding: utf-8 -*-
"""
Bindings for SQLite

https://docs.python.org/2/library/sqlite3.html

Notes, certain SQLite versions might have a problem with long integers
http://jakegoulding.com/blog/2011/02/06/sqlite-64-bit-integers/

Looking at the docs, it says it will set an integer value to 1-4, 6, or 8 bytes
depending on the size, but I couldn't get it to accept anything over the 32-bit signed
integer value of around 2billion

savepoints and transactions are similar to Postgres
https://www.sqlite.org/lang_savepoint.html
http://sqlite.org/lang_transaction.html

alter table is similar to Postgres
https://www.sqlite.org/lang_altertable.html

other links that were helpful
http://www.numericalexpert.com/blog/sqlite_blob_time/
"""
from __future__ import unicode_literals, division, print_function, absolute_import
import os
import decimal
import datetime
from distutils import dir_util
import re
import sqlite3
import json
import uuid

from datatypes import Datetime

# first party
from ..exception import UniqueError
from ..compat import *
from .base import SQLInterface, SQLConnection


class SQLiteRowDict(sqlite3.Row):
    def get(self, k, default_val=None):
        try:
            r = self[k]
        except KeyError:
            r = default_val
        return r


# TODO -- I think we can get rid of this by just reconnecting in the error
# handler
class SQLiteConnection(SQLConnection, sqlite3.Connection):
    """
    Thin wrapper around the default connection to make sure it has a similar interface
    to Postgres' connection instance so the common code can all be the same in the
    parent class
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.closed = 0

    def close(self, *args, **kwargs):
        r = super().close(*args, **kwargs)
        self.closed = 1
        return r


class BooleanType(object):
    @classmethod
    def adapt(cls, val):
        """From python you get False and True, convert those to 1/0"""
        return 1 if val else 0

    @classmethod
    def convert(cls, val):
        """from the db you get values like b'0' and b'1', convert those to True/False"""
        return bool(int(val))


# class NumericType(object):
#     @staticmethod
#     def adapt(val):
#         return float(str(val))
# 
#     @staticmethod
#     def convert(val):
#         val = StringType.adapt(val)
#         ret = decimal.Decimal(val)
#         return ret


class StringType(object):
    """this just makes sure 8-bit bytestrings get converted ok"""
    @classmethod
    def adapt(cls, val):
        return String(val)


class DatetimeType(StringType):
    """External sqlite3 databases can store the TIMESTAMP type as unix timestamps,
    this caused parsing problems when pulling the values out of the db because the
    default adapter expected TIMESTAMP to be in the form of YYYY-MM-DD HH:MM:SS.SSSSSS
    and so it would fail to convert the DDDDDD.DDD values, this handles that conversion

    https://www.sqlite.org/lang_datefunc.html
    the "unixepoch" modifier only works for dates between 0000-01-01 00:00:00 and
    5352-11-01 10:52:47 (unix times of -62167219200 through 106751991167)

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

    This is named to be as consistent with Postgres's NUMERIC(<precision>, 0) type

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
        """This should only be called when the column type is actually FIELD_TYPE"""
        return int(val)


class DictType(object):
    """Converts a dict to json text and back again

    Uses JSONBTEXT to be as close to Postgres while still triggering SQLite's affinity rule 2
    """
    FIELD_TYPE = 'JSONBTEXT'

    @classmethod
    def adapt(cls, val):
        return json.dumps(val)

    @classmethod
    def convert(cls, val):
        """This should only be called when the column type is actually FIELD_TYPE"""
        return json.loads(val)


class SQLite(SQLInterface):

    val_placeholder = '?'

    _connection = None

    @classmethod
    def configure(cls, connection_config):
        dsn = getattr(connection_config, 'dsn', '')
        if dsn:
            host = connection_config.host
            db = connection_config.database
            if not host:
                path = db

            elif not db:
                path = host

            else:
                path = os.sep.join([host, db])

        else:
            path = connection_config.database

        if not path:
            raise ValueError("no sqlite db path found in connection_config")

        connection_config.path = path
        return connection_config

    def _connect(self, connection_config):
        path = connection_config.path

        # https://docs.python.org/2/library/sqlite3.html#default-adapters-and-converters
        options = {
            'isolation_level': None,
            'detect_types': sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES,
            'factory': SQLiteConnection,
            'check_same_thread': True, # https://stackoverflow.com/a/2578401/5006
        }
        option_keys = list(options.keys()) + ['timeout', 'cached_statements']
        for k in option_keys:
            if k in connection_config.options:
                options[k] = connection_config.options[k]

        try:
            self._connection = sqlite3.connect(path, **options)

        except sqlite3.DatabaseError as e:
            path_d = os.path.dirname(path)
            if os.path.isdir(path_d):
                raise

            else:
                # let's try and make the directory path and connect again
                dir_util.mkpath(path_d)
                self._connection = sqlite3.connect(path, **options)

        # https://docs.python.org/2/library/sqlite3.html#row-objects
        self._connection.row_factory = SQLiteRowDict
        # https://docs.python.org/2/library/sqlite3.html#sqlite3.Connection.text_factory
        self._connection.text_factory = StringType.adapt

        # for some reason this is needed in python 3.6 in order for saved bytes
        # to be ran through the converter, not sure why
        sqlite3.register_converter('TEXT', StringType.adapt)

        #sqlite3.register_adapter(decimal.Decimal, NumericType.adapt)
        #sqlite3.register_converter('NUMERIC', NumericType.convert)

        sqlite3.register_adapter(bool, BooleanType.adapt)
        sqlite3.register_converter('BOOLEAN', BooleanType.convert)

        # sadly, it doesn't look like these work for child classes so each class
        # has to be adapted even if its parent is already registered
        sqlite3.register_adapter(datetime.datetime, DatetimeType.adapt)
        sqlite3.register_adapter(Datetime, DatetimeType.adapt)
        sqlite3.register_converter(DatetimeType.FIELD_TYPE, DatetimeType.convert)

        sqlite3.register_adapter(int, NumericType.adapt)
        sqlite3.register_converter(NumericType.FIELD_TYPE, NumericType.convert)

        sqlite3.register_adapter(dict, DictType.adapt)
        sqlite3.register_converter(DictType.FIELD_TYPE, DictType.convert)

        # turn on foreign keys
        # http://www.sqlite.org/foreignkeys.html
        self._query('PRAGMA foreign_keys = ON', ignore_result=True);
        self.readonly(self.connection_config.readonly)

    def get_connection(self):
        if not self.connected: self.connect()
        return self._connection

    def _close(self):
        self._connection.close()
        self._connection = None

    def _readonly(self, readonly):
        self._query(
            # https://stackoverflow.com/a/49630725/5006
            'PRAGMA query_only = {}'.format("ON" if readonly else "OFF"),
            ignore_result=True
        )

    def _get_tables(self, table_name, **kwargs):
        query_str = 'SELECT tbl_name FROM sqlite_master WHERE type = ?'
        query_args = ['table']

        if table_name:
            query_str += ' AND name = ?'
            query_args.append(str(table_name))

        ret = self._query(query_str, query_args, **kwargs)
        return [r['tbl_name'] for r in ret]

    def get_field_SQL(self, field_name, field):
        """
        returns the SQL for a given field with full type information

        http://www.sqlite.org/datatype3.html

        field_name -- string -- the field's name
        field -- Field() -- the set options for the field

        return -- string -- the field type (eg, foo BOOL NOT NULL)
        """
        field_type = ""
        is_pk = field.options.get('pk', False)
        interface_type = field.interface_type

        if issubclass(interface_type, bool):
            field_type = 'BOOLEAN'

        elif issubclass(interface_type, int):
            if is_pk:
                field_type += 'INTEGER PRIMARY KEY'

            else:
                # we could break these up into tiny, small, and big but it
                # doesn't really matter so we're not bothering
                # https://www.sqlite.org/datatype3.html
                size = field.options.get('size', field.options.get('max_size', 0))

                if size < 9223372036854775807:
                    field_type = 'INTEGER'

                else:
                    field_type = NumericType.FIELD_TYPE

        elif issubclass(interface_type, str):
            if field.is_ref():
                fo = field.schema.pk.options
                fo.update(field.options)
            else:
                fo = field.options

            field_type = 'TEXT'

            if 'size' in fo:
                field_type += f" CHECK(length({field_name}) == {fo['size']})"
            elif 'max_size' in fo:
                field_type += f" CHECK(length({field_name}) <= {fo['max_size']})"

            if fo.get('ignore_case', False):
                field_type += ' COLLATE NOCASE'

            if is_pk:
                field_type += ' PRIMARY KEY'

        elif issubclass(interface_type, datetime.datetime):
            field_type = DatetimeType.FIELD_TYPE

        elif issubclass(interface_type, datetime.date):
            field_type = 'DATE'

        elif issubclass(interface_type, dict):
            field_type = DictType.FIELD_TYPE

        elif issubclass(interface_type, (float, decimal.Decimal)):
            field_type = 'REAL'

        elif issubclass(interface_type, (bytearray, bytes)):
            field_type = 'BLOB'

        elif issubclass(interface_type, uuid.UUID):
            field_type = 'CHARACTER(36)'
            if is_pk:
                field_type += ' PRIMARY KEY'

        else:
            raise ValueError('Unknown python type: {}'.format(interface_type.__name__))

        if field.required:
            field_type += ' NOT NULL'
        else:
            field_type += ' NULL'

        if not is_pk and field.is_ref():
            ref_s = field.schema
            if field.required: # strong ref, it deletes on fk row removal
                field_type += ' REFERENCES {} ({}) ON UPDATE CASCADE ON DELETE CASCADE'.format(
                    ref_s,
                    ref_s.pk.name
                )

            else: # weak ref, it sets column to null on fk row removal
                field_type += ' REFERENCES {} ({}) ON UPDATE CASCADE ON DELETE SET NULL'.format(
                    ref_s,
                    ref_s.pk.name
                )

        return '{} {}'.format(self._normalize_name(field_name), field_type)

    def _set_table(self, schema, **kwargs):
        """
        http://sqlite.org/lang_createtable.html
        """
        query_str = []
        query_str.append("CREATE TABLE {} (".format(self._normalize_table_name(schema)))

        query_fields = []
        for field_name, field in schema.fields.items():
            query_fields.append('  {}'.format(self.get_field_SQL(field_name, field)))

        query_str.append(",{}".format(os.linesep).join(query_fields))
        query_str.append(')')
        query_str = os.linesep.join(query_str)
        ret = self._query(query_str, ignore_result=True, **kwargs)

    def _set_index(self, schema, name, field_names, **index_options):
        """
        https://www.sqlite.org/lang_createindex.html
        """
        query_str = "CREATE {}INDEX IF NOT EXISTS '{}_{}' ON {} ({})".format(
            'UNIQUE ' if index_options.get('unique', False) else '',
            schema,
            name,
            self._normalize_table_name(schema),
            ', '.join(map(self._normalize_name, field_names))
        )

        return self._query(query_str, ignore_result=True, **index_options)

    def _get_indexes(self, schema, **kwargs):
        """return all the indexes for the given schema"""
        # http://www.sqlite.org/pragma.html#schema
        # http://www.mail-archive.com/sqlite-users@sqlite.org/msg22055.html
        # http://stackoverflow.com/questions/604939/
        ret = {}
        rs = self._query('PRAGMA index_list({})'.format(self._normalize_table_name(schema)), **kwargs)
        if rs:
            for r in rs:
                iname = r['name']
                ret.setdefault(iname, [])
                indexes = self._query('PRAGMA index_info({})'.format(r['name']), **kwargs)
                for idict in indexes:
                    ret[iname].append(idict['name'])

        return ret

    def _insert(self, schema, fields, **kwargs):
        """
        http://www.sqlite.org/lang_insert.html
        """
        query_str, query_args = self.render_insert_sql(
            schema,
            fields,
            ignore_return_clause=True,
            **kwargs,
        )
        ret = self._query(query_str, query_args, cursor_result=True, **kwargs)

        pk_name = schema.pk_name
        # http://stackoverflow.com/questions/6242756/
        # could also do _query('SELECT last_insert_rowid()')
        return ret.lastrowid if pk_name not in fields else fields[pk_name]

    def _delete_tables(self, **kwargs):
        self._query('PRAGMA foreign_keys = OFF', ignore_result=True, **kwargs);
        ret = super(SQLite, self)._delete_tables(**kwargs)
        self._query('PRAGMA foreign_keys = ON', ignore_result=True, **kwargs);
        return ret

    def _delete_table(self, schema, **kwargs):
        #query_str = 'DROP TABLE IF EXISTS {}'.format(str(schema))
        query_str = "DROP TABLE IF EXISTS {}".format(self._normalize_table_name(schema))
        ret = self._query(query_str, ignore_result=True, **kwargs)

    def _handle_error(self, schema, e, **kwargs):
        ret = False
        if isinstance(e, sqlite3.OperationalError):
            e_msg = str(e)
            if "no such column" in e_msg or "has no column" in e_msg:
                #INSERT: "table yscrmiklbgdtx has no column named che"
                #SELECT: "no such column: che"
                try:
                    ret = self._set_all_fields(schema, **kwargs)

                except ValueError:
                    ret = False

            elif "no such table" in e_msg:
                ret = self._set_all_tables(schema, **kwargs)

            elif "UNIQUE" in e_msg:
                self.raise_error(e, e_class=UniqueError)

        return ret

    def _create_error(self, e, exc_info):
        if isinstance(e, sqlite3.IntegrityError):
            er = UniqueError(e, exc_info)
        else:
            er = super(SQLite, self)._create_error(e, exc_info)
        return er

    def _get_fields(self, table_name, **kwargs):
        """return all the fields for the given table"""
        ret = {}
        query_str = 'PRAGMA table_info({})'.format(self._normalize_table_name(table_name))
        fields = self._query(query_str, **kwargs)
        #pout.v([dict(d) for d in fields])

        query_str = 'PRAGMA foreign_key_list({})'.format(self._normalize_table_name(table_name))
        fks = {f["from"]: f for f in self._query(query_str, **kwargs)}
        #pout.v([dict(d) for d in fks.values()])

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

        # the rows we can set: field_type, name, field_required, min_size, max_size,
        #   size, unique, pk, <foreign key info>
        # These keys will roughly correspond with schema.Field
        # TODO -- we could actually use "type" to get the size because SQLite returns
        # a value like VARCHAR[32]
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

            if field["pk"] and field["field_type"] is int:
                # we compensate for SQLite internally setting pk to int
                field["field_type"] = long

            if row["name"] in fks:
                field["schema_table_name"] = fks[row["name"]]["table"]
                field["ref_table_name"] = fks[row["name"]]["table"]

            ret[field["name"]] = field

        return ret

    def _normalize_date_SQL(self, field_name, field_kwargs, symbol):
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
            'dow': "CAST(strftime('%w', {}) AS integer)", # day of week 0 = sunday
            'week': "CAST(strftime('%W', {}) AS integer)",
            'year': "CAST(strftime('%Y', {}) AS integer)"
        }

        for k, v in field_kwargs.items():
            fstrs.append([k_opts[k].format(self._normalize_name(field_name)), self.val_placeholder, v])

        return fstrs

    def _normalize_sort_SQL(self, field_name, field_vals, sort_dir_str):
        """
        allow sorting by a set of values

        http://stackoverflow.com/questions/3303851/sqlite-and-custom-order-by
        """
        fvi = None
        if sort_dir_str == 'ASC':
            fvi = (t for t in enumerate(field_vals)) 

        else:
            fvi = (t for t in enumerate(reversed(field_vals))) 

        query_sort_str = ['  CASE {}'.format(self._normalize_name(field_name))]
        query_args = []
        for i, v in fvi:
            query_sort_str.append('    WHEN {} THEN {}'.format(self.val_placeholder, i))
            query_args.append(v)

        query_sort_str.append('  END')
        query_sort_str = "\n".join(query_sort_str)
        return query_sort_str, query_args

    def _normalize_bounds_SQL(self, bounds, sql_options):
        offset = bounds.offset
        if sql_options.get('one_query', False):
            limit = 1

        else:
            limit, offset = bounds.get()
            if not bounds.has_limit():
                limit = -1

        return 'LIMIT {} OFFSET {}'.format(
            limit,
            offset
        )

