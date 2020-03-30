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
try:
    import thread
except ImportError:
    thread = None

# first party
from ..exception import UniqueError
from ..compat import *
from .base import SQLInterface, SQLConnection


class SQLiteRowDict(sqlite3.Row):
    def __getitem__(self, k):
        if is_py2:
            return super(SQLiteRowDict, self).__getitem__(b"{}".format(k))
        else:
            return super(SQLiteRowDict, self).__getitem__(k)

    def get(self, k, default_val=None):
        r = default_val
        r = self[k]
        return r


class SQLiteConnection(SQLConnection, sqlite3.Connection):
    """
    Thin wrapper around the default connection to make sure it has a similar interface
    to Postgres' connection instance so the common code can all be the same in the
    parent class
    """
    def __init__(self, *args, **kwargs):
        super(SQLiteConnection, self).__init__(*args, **kwargs)
        self.closed = 0

    def close(self, *args, **kwargs):
        r = super(SQLiteConnection, self).close(*args, **kwargs)
        self.closed = 1
        return r


class TimestampType(object):
    """External sqlite3 databases can store the TIMESTAMP type as unix timestamps,
    this caused parsing problems when pulling the values out of the db because the
    default adapter expected TIMESTAMP to be in the form of YYYY-MM-DD HH:MM:SS.SSSSSS
    and so it would fail to convert the DDDDDD.DDD values, this handles that conversion

    https://www.sqlite.org/lang_datefunc.html
    the "unixepoch" modifier only works for dates between 0000-01-01 00:00:00 and
    5352-11-01 10:52:47 (unix times of -62167219200 through 106751991167)
    """
    @staticmethod
    def adapt(val):
        return val.isoformat(b" ") if is_py2 else val.isoformat(" ")

    @staticmethod
    def convert(val):
        val = StringType.adapt(val)
        if re.match(r"^\-?\d+\.\d+$", val):
            # account for unix timestamps with microseconds
            val = datetime.datetime.fromtimestamp(float(val))

        elif re.match(r"^\-?\d+$", val):
            # account for unix timestamps without microseconds
            val = int(val)

            try:
                val = datetime.datetime.fromtimestamp(val)

            except ValueError:
                # we're hosed with this unix timestamp, but rather than error
                # out let's go ahead and return the closest approximation we
                # can get to the correct timestamp
                if val > 0:
                    val = datetime.datetime.max
                else:
                    val = datetime.datetime.min

        else:
            # ISO 8601 is not very strict with the date format and this tries to
            # capture most of that leniency, with the one exception that the
            # date must be in UTC
            # https://en.wikipedia.org/wiki/ISO_8601
            m = re.match(
                r"^(\d{4}).?(\d{2}).?(\d{2}).(\d{2}):?(\d{2}):?(\d{2})(?:\.(\d+))?Z?$",
                val
            )

            if m:
                parsed_dateparts = m.groups()
                dateparts = list(map(lambda x: int(x) if x else 0, parsed_dateparts[:6]))
                val = datetime.datetime(*dateparts)

                # account for ms with leading zeros
                if parsed_dateparts[6]:
                    ms_len = len(parsed_dateparts[6])
                    if ms_len >= 3:
                        millis = parsed_dateparts[6][:3]
                        micros = parsed_dateparts[6][3:] or 0

                    else:
                        millis = parsed_dateparts[6] or 0
                        micros = 0

                    # make sure each part is 3 digits by zero padding on the right
                    if millis:
                        millis = "{:0<3.3}".format(millis)
                    if micros:
                        micros = "{:0<3.3}".format(micros)

                    val += datetime.timedelta(
                        milliseconds=int(millis),
                        microseconds=int(micros)
                    )

            else:
                raise ValueError("Cannot infer UTC datetime value from {}".format(val))

        return val


class BooleanType(object):
    @staticmethod
    def adapt(val):
        """From python you get False and True, convert those to 1/0"""
        return 1 if val else 0

    @staticmethod
    def convert(val):
        """from the db you get values like b'0' and b'1', convert those to True/False"""
        return bool(int(val))


class NumericType(object):
    @staticmethod
    def adapt(val):
        return float(str(val))

    @staticmethod
    def convert(val):
        if is_py2:
            ret = decimal.Decimal(str(val))
        else:
            val = StringType.adapt(val)
            ret = decimal.Decimal(val)
        return ret


class StringType(object):
    """this just makes sure 8-bit bytestrings get converted ok"""
    @staticmethod
    def adapt(val):
        #if isinstance(val, str):
        if isinstance(val, bytes):
            val = val.decode('utf-8')

        return val


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
        sqlite3.register_converter('TEXT' if not is_py2 else b'TEXT', StringType.adapt)

        sqlite3.register_adapter(decimal.Decimal, NumericType.adapt)
        sqlite3.register_converter('NUMERIC' if not is_py2 else b'NUMERIC', NumericType.convert)

        sqlite3.register_adapter(bool, BooleanType.adapt)
        sqlite3.register_converter('BOOLEAN' if not is_py2 else b'BOOLEAN', BooleanType.convert)

        sqlite3.register_adapter(datetime.datetime, TimestampType.adapt)
        sqlite3.register_converter('TIMESTAMP' if not is_py2 else b'TIMESTAMP', TimestampType.convert)

        # turn on foreign keys
        # http://www.sqlite.org/foreignkeys.html
        self._query('PRAGMA foreign_keys = ON', ignore_result=True);
        self.readonly(self.connection_config.readonly)

    def get_connection(self):
        if not self.connected: self.connect()
        return self._connection

    def _get_thread(self):
        if thread:
            ret = str(thread.get_ident())
        else:
            ret = ""
        return ret

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

        if issubclass(field.type, bool):
            field_type = 'BOOLEAN'

        elif issubclass(field.type, long):
            if is_pk:
                field_type = 'INTEGER PRIMARY KEY'
            else:
                field_type = 'BIGINT'

        elif issubclass(field.type, int):
            field_type = 'INTEGER'
            if is_pk:
                field_type += ' PRIMARY KEY'

        elif issubclass(field.type, basestring):
            fo = field.options
            if field.is_ref():
                # TODO -- 7-8-17 - this isn't a great way to do this, ideally the Field instance
                # would combine all the options of both the current field and the
                # foreign key field and return all those when Field.options is called
                # (with the current field's options taking precedence) but there are
                # lots of circular dependency things that happen when one field is
                # trying to get the schema of another field and I don't have time
                # to sort it all out right now
                ref_s = field.schema
                fo = ref_s.pk.options

            if 'size' in fo:
                field_type = 'CHARACTER({})'.format(fo['size'])
            elif 'max_size' in fo:
                field_type = 'VARCHAR({})'.format(fo['max_size'])
            else:
                field_type = 'TEXT'

            if fo.get('ignore_case', False):
                field_type += ' COLLATE NOCASE'

            if is_pk:
                field_type += ' PRIMARY KEY'

        elif issubclass(field.type, datetime.datetime):
            #field_type = 'DATETIME'
            field_type = 'TIMESTAMP'

        elif issubclass(field.type, datetime.date):
            field_type = 'DATE'

        elif issubclass(field.type, float):
            field_type = 'REAL'
            size = field.options.get('size', field.options.get('max_size', 0))
            if size > 6:
                field_type = 'DOUBLE PRECISION'

        elif issubclass(field.type, decimal.Decimal):
            field_type = 'NUMERIC'

        elif issubclass(field.type, bytearray):
            field_type = 'BLOB'

        else:
            raise ValueError('unknown python type: {}'.format(field.type.__name__))

        if field.required:
            field_type += ' NOT NULL'
        else:
            field_type += ' NULL'

        if not is_pk:
            if field.is_ref():
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

    def _set_index(self, schema, name, fields, **index_options):
        """
        https://www.sqlite.org/lang_createindex.html
        """
        query_str = "CREATE {}INDEX IF NOT EXISTS '{}_{}' ON {} ({})".format(
            'UNIQUE ' if index_options.get('unique', False) else '',
            schema,
            name,
            self._normalize_table_name(schema),
            ', '.join((self._normalize_name(f) for f in fields))
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
        field_formats = []
        field_names = []
        query_vals = []
        for field_name, field_val in fields.items():
            field_names.append(self._normalize_name(field_name))
            field_formats.append(self.val_placeholder)
            query_vals.append(field_val)

        query_str = "INSERT INTO {} ({}) VALUES ({})".format(
            self._normalize_table_name(schema),
            ', '.join(field_names),
            ', '.join(field_formats)
        )

        ret = self._query(query_str, query_vals, cursor_result=True, **kwargs)

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
                except ValueError as e:
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

