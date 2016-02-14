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
import os
import types
import decimal
import datetime

# third party
import sqlite3

# first party
from .base import SQLInterface, SQLConnection


class SQLiteRowDict(sqlite3.Row):
    def get(self, k, default_val=None):
        r = default_val
        r = self[str(k)]
        return r


# class LoggingCursor(sqlite3.Cursor):
#     def execute(self, sql, args=None):
#         #logger.debug(self.mogrify(sql, args))
#         super(LoggingCursor, self).execute(sql, args)


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

#     def cursor(self, cursor_class=None):
#         if not cursor_class:
#             cursor_class = LoggingCursor
#         return super(SQLiteConnection, self).cursor(cursor_class)


class BooleanType(object):
    @staticmethod
    def adapt(val):
        return int(str(val))

    @staticmethod
    def convert(val):
        return bool(str(val))


class NumericType(object):
    @staticmethod
    def adapt(val):
        return float(str(val))

    @staticmethod
    def convert(val):
        return decimal.Decimal(str(val))


class StringType(object):
    """this just makes sure 8-bit bytestrings get converted ok"""
    @staticmethod
    def adapt(val):
        if isinstance(val, str):
            val = val.decode('utf-8')

        return val


class SQLite(SQLInterface):

    val_placeholder = '?'

    _connection = None

    def _connect(self, connection_config):
        path = ''
        dsn = getattr(connection_config, 'dsn', '')
        if dsn:
            host = connection_config.host
            db = connection_config.database
            if not host:
                path = os.sep + db

            elif not db:
                path = host

            else:
                path = os.sep.join([host, db])

        else:
            path = connection_config.database

        if not path:
            raise ValueError("no sqlite db path found in connection_config")

        # https://docs.python.org/2/library/sqlite3.html#default-adapters-and-converters
        options = {
            'isolation_level': None,
            'detect_types': sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES,
            'factory': SQLiteConnection
        }
        for k in ['timeout', 'detect_types', 'isolation_level', 'check_same_thread', 'factory', 'cached_statements']:
            if k in connection_config.options:
                options[k] = connection_config.options[k]

        self._connection = sqlite3.connect(path, **options)
        # https://docs.python.org/2/library/sqlite3.html#row-objects
        self._connection.row_factory = SQLiteRowDict
        # https://docs.python.org/2/library/sqlite3.html#sqlite3.Connection.text_factory
        self._connection.text_factory = StringType.adapt

        sqlite3.register_adapter(decimal.Decimal, NumericType.adapt)
        sqlite3.register_converter('NUMERIC', NumericType.convert)

        sqlite3.register_adapter(bool, BooleanType.adapt)
        sqlite3.register_converter('BOOLEAN', BooleanType.convert)

        # turn on foreign keys
        # http://www.sqlite.org/foreignkeys.html
        self._query('PRAGMA foreign_keys = ON', ignore_result=True);

    def get_connection(self):
        if not self.connected: self.connect()
        return self._connection

    def _close(self):
        self._connection.close()
        self._connection = None

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

        if field.options.get('pk', False):
            field_type = 'INTEGER PRIMARY KEY'

        else:
            if issubclass(field.type, bool):
                field_type = 'BOOLEAN'

            elif issubclass(field.type, int):
                field_type = 'INTEGER'

            elif issubclass(field.type, long):
                field_type = 'BIGINT'

            elif issubclass(field.type, types.StringTypes):
                if 'size' in field.options:
                    field_type = 'CHARACTER({})'.format(field.options['size'])
                elif 'max_size' in field.options:
                    field_type = 'VARCHAR({})'.format(field.options['max_size'])
                else:
                    field_type = 'TEXT'

                if field.options.get('ignore_case', False):
                    field_type += ' COLLATE NOCASE'

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

            else:
                raise ValueError('unknown python type: {}'.format(field.type.__name__))

            if field.required:
                field_type += ' NOT NULL'
            else:
                field_type += ' NULL'

            if field.is_ref():
                if field.required: # strong ref, it deletes on fk row removal
                    ref_s = field.schema
                    field_type += ' REFERENCES {} ({}) ON UPDATE CASCADE ON DELETE CASCADE'.format(ref_s.table, ref_s.pk.name)

                else: # weak ref, it sets column to null on fk row removal
                    ref_s = field.schema
                    field_type += ' REFERENCES {} ({}) ON UPDATE CASCADE ON DELETE SET NULL'.format(ref_s.table, ref_s.pk.name)

        return '{} {}'.format(field_name, field_type)

    def _set_table(self, schema, **kwargs):
        """
        http://sqlite.org/lang_createtable.html
        """
        query_str = []
        query_str.append("CREATE TABLE {} (".format(schema.table))

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
        query_str = 'CREATE {}INDEX IF NOT EXISTS {}_{} ON {} ({})'.format(
            'UNIQUE ' if index_options.get('unique', False) else '',
            schema,
            name,
            schema,
            ', '.join(fields)
        )

        return self._query(query_str, ignore_result=True, **index_options)

    def _get_indexes(self, schema, **kwargs):
        """return all the indexes for the given schema"""
        # http://www.sqlite.org/pragma.html#schema
        # http://www.mail-archive.com/sqlite-users@sqlite.org/msg22055.html
        # http://stackoverflow.com/questions/604939/
        ret = {}
        rs = self._query('PRAGMA index_list({})'.format(schema), **kwargs)
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

        # get the primary key
        field_formats = []
        field_names = []
        query_vals = []
        for field_name, field_val in fields.items():
            field_names.append(field_name)
            field_formats.append(self.val_placeholder)
            query_vals.append(field_val)

        query_str = 'INSERT INTO {} ({}) VALUES ({})'.format(
            schema,
            ', '.join(field_names),
            ', '.join(field_formats)
        )

        ret = self._query(query_str, query_vals, cursor_result=True, **kwargs)
        # http://stackoverflow.com/questions/6242756/
        # could also do _query('SELECT last_insert_rowid()')
        return ret.lastrowid

    def _delete_table(self, schema, **kwargs):
        query_str = 'DROP TABLE IF EXISTS {}'.format(str(schema))
        ret = self._query(query_str, ignore_result=True, **kwargs)

    def _handle_error(self, schema, e, **kwargs):
        ret = False
        if isinstance(e, sqlite3.OperationalError):
            e_msg = str(e)
            if schema.table in e_msg:
                if "no such table" in e_msg:
                    ret = self._set_all_tables(schema, **kwargs)

                elif "column" in e_msg:
                    # "table yscrmiklbgdtx has no column named che"
                    try:
                        ret = self._set_all_fields(schema, **kwargs)
                    except ValueError, e:
                        ret = False

        return ret

    def _get_fields(self, schema, **kwargs):
        """return all the fields for the given schema"""
        ret = []
        query_str = 'PRAGMA table_info({})'.format(schema)
        fields = self._query(query_str, **kwargs)
        return set((d['name'] for d in fields))

    def _normalize_date_SQL(self, field_name, field_kwargs):
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

        for k, v in field_kwargs.iteritems():
            fstrs.append([k_opts[k].format(field_name), self.val_placeholder, v])

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

        query_sort_str = ['  CASE {}'.format(field_name)]
        query_args = []
        for i, v in fvi:
            query_sort_str.append('    WHEN {} THEN {}'.format(self.val_placeholder, i))
            query_args.append(v)

        query_sort_str.append('  END'.format(field_name))
        query_sort_str = "\n".join(query_sort_str)
        return query_sort_str, query_args


