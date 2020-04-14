# -*- coding: utf-8 -*-
"""
http://pythonhosted.org/psycopg2/module.html

http://zetcode.com/db/postgresqlpythontutorial/
http://wiki.postgresql.org/wiki/Using_psycopg2_with_PostgreSQL
http://pythonhosted.org/psycopg2/
"""
from __future__ import unicode_literals, division, print_function, absolute_import
import os
import sys
import decimal
import datetime

# third party
import psycopg2
import psycopg2.extras
import psycopg2.extensions

# first party
from ..base import SQLInterface, SQLConnection
from ...compat import *
from ...utils import get_objects
from ...exception import UniqueError


# class LoggingCursor(psycopg2.extras.RealDictCursor):
#     def execute(self, sql, args=None):
#         logger.debug(self.mogrify(sql, args))
#         super(LoggingCursor, self).execute(sql, args)
#         #psycopg2.extensions.cursor.execute(self, sql, args)


#class Connection(psycopg2.extensions.connection, SQLConnection):
class Connection(SQLConnection, psycopg2.extensions.connection):
#class Connection(SQLConnection, psycopg2.extras.LoggingConnection):
    """
    http://initd.org/psycopg/docs/advanced.html
    http://initd.org/psycopg/docs/extensions.html#psycopg2.extensions.connection
    """
    def __init__(self, *args, **kwargs):
        super(Connection, self).__init__(*args, **kwargs)

        # http://initd.org/psycopg/docs/connection.html#connection.autocommit
        self.autocommit = True

        if is_py2:
            # unicode harden for python 2
            # http://initd.org/psycopg/docs/usage.html#unicode-handling
            psycopg2.extensions.register_type(psycopg2.extensions.UNICODE, self)
            psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY, self)

            # return ints for any long values, this normalizes with SQLite and 
            # python 2.7+ transparently handles really large integer values
            # with no long needed (long doesn't even exist in python 3+)
            def normalize_long(v, cur):
                return v if v is None else int(v)
            psycopg2.extensions.register_type(
                psycopg2.extensions.new_type(
                    psycopg2.extensions.LONGINTEGER.values,
                    b"LONGINTEGER",
                    normalize_long
                )
            )

        else:
            def normalize_str(v, cur):
                if isinstance(v, str) and v.startswith("\\x"):
                    buf = psycopg2.BINARY(v, cur)
                    v = bytes(buf).decode(cur.connection.encoding)

                    #import binascii
                    #pout.v(binascii.unhexlify(v[2:]))
                    #v = v.encode(cur.connection.encoding)
                    #v = bytes(v, encoding=cur.connection.encoding)
                return v
            psycopg2.extensions.register_type(
                psycopg2.extensions.new_type(psycopg2.STRING.values, "STRING", normalize_str)
            )


        # http://initd.org/psycopg/docs/connection.html#connection.set_client_encoding
        # https://www.postgresql.org/docs/current/static/multibyte.html
        # > The default is the encoding defined by the database
        # Not sure we want to override db encoding which is why I'm sure why I didn't
        # set this previously
        #self.set_client_encoding("UTF8")

        #self.initialize(logger)


class PostgreSQL(SQLInterface):

    val_placeholder = '%s'

    connection_pool = None

    _connection = None

    def _connect(self, connection_config):
        database = connection_config.database
        username = connection_config.username
        password = connection_config.password
        host = connection_config.host
        port = connection_config.port
        if not port: port = 5432

        minconn = int(connection_config.options.get('pool_minconn', 1))
        maxconn = int(connection_config.options.get('pool_maxconn', 1))
        pool_class_name = connection_config.options.get(
            'pool_class',
            'psycopg2.pool.SimpleConnectionPool'
        )
        async_conn = int(connection_config.options.get('async', 0))

        _, pool_class = get_objects(pool_class_name)

        self.log("connecting using pool class {}".format(pool_class_name))

        # http://initd.org/psycopg/docs/module.html#psycopg2.connect
        self.connection_pool = pool_class(
            minconn,
            maxconn,
            dbname=database,
            user=username,
            password=password,
            host=host,
            port=port,
            cursor_factory=psycopg2.extras.RealDictCursor,
            #cursor_factory=LoggingCursor,
            connection_factory=Connection,
        )

        # hack for sync backwards compatibility with transactions
        if not async_conn:
            self._connection = self.connection_pool.getconn()

    def free_connection(self, connection):
        if not self.connected: return
        if self._connection:
            self.log("freeing sync connection")
            return
        self.log("freeing async connection {}", id(connection))
        self.connection_pool.putconn(connection)

    def get_connection(self):
        if not self.connected: self.connect()

        connection = None
        if self._connection:
            self.log("getting sync connection")
            connection = self._connection

        else:
            connection = self.connection_pool.getconn()
            self.log("getting async connection {}", id(connection))

        # change the connection readonly status if they don't match
        if connection.readonly != self.connection_config.readonly:
            # https://www.psycopg.org/docs/connection.html#connection.readonly
            connection.readonly = self.connection_config.readonly
        return connection

    def _close(self):
        self.connection_pool.closeall()
        if self._connection:
            self._connection.close()
            self._connection = None
            self.connection_pool = None

    def _readonly(self, readonly):
        """readonly setting is handled when you grab the connection from get_connection()
        so this method does nothing"""
        pass

    def _get_tables(self, table_name, **kwargs):
        query_str = 'SELECT tablename FROM pg_tables WHERE tableowner = %s'
        query_args = [self.connection_config.username]

        if table_name:
            query_str += ' AND tablename = %s'
            query_args.append(str(table_name))

        ret = self.query(query_str, *query_args, **kwargs)
        # http://www.postgresql.org/message-id/CA+mi_8Y6UXtAmYKKBZAHBoY7F6giuT5WfE0wi3hR44XXYDsXzg@mail.gmail.com
        return [r['tablename'] for r in ret]

    def _set_table(self, schema, **kwargs):
        """
        http://www.postgresql.org/docs/9.1/static/sql-createtable.html
        http://www.postgresql.org/docs/8.1/static/datatype.html
        http://pythonhosted.org/psycopg2/usage.html#adaptation-of-python-values-to-sql-types
        """
        query_str = []
        query_str.append("CREATE TABLE {} (".format(self._normalize_table_name(schema)))

        query_fields = []
        for field_name, field in schema.fields.items():
            query_fields.append('  {}'.format(self.get_field_SQL(field_name, field)))

        query_str.append(",{}".format(os.linesep).join(query_fields))
        query_str.append(')')
        query_str = os.linesep.join(query_str)
        ret = self.query(query_str, ignore_result=True, **kwargs)

    def _delete_table(self, schema, **kwargs):
        query_str = 'DROP TABLE IF EXISTS {} CASCADE'.format(self._normalize_table_name(schema))
        ret = self.query(query_str, ignore_result=True, **kwargs)

    def _get_fields(self, table_name, **kwargs):
        """return all the fields for the given schema"""
        ret = {}
        query_args = ['f', table_name]

        # I had to brush up on my join knowledge while writing this query
        # https://en.wikipedia.org/wiki/Join_(SQL)
        #
        # other helpful links
        # https://wiki.postgresql.org/wiki/Retrieve_primary_key_columns
        # https://www.postgresql.org/docs/9.4/static/catalog-pg-attribute.html
        # https://www.postgresql.org/docs/9.3/static/catalog-pg-type.html
        # 
        # another approach
        # http://dba.stackexchange.com/questions/22362/how-do-i-list-all-columns-for-a-specified-table
        # http://gis.stackexchange.com/questions/94049/how-to-get-the-data-type-of-each-column-from-a-postgis-table
        query_str = [
            'SELECT',
            ',  '.join([
                'a.attnum',
                'a.attname',
                'a.attnotnull',
                't.typname',
                'i.indisprimary',
                #'s.conname',
                #'pg_get_constraintdef(s.oid, true) as condef',
                'c.relname AS confrelname',
            ]),
            'FROM',
            '  pg_attribute a',
            'JOIN pg_type t ON a.atttypid = t.oid',
            'LEFT JOIN pg_index i ON a.attrelid = i.indrelid',
            '  AND a.attnum = any(i.indkey)',
            'LEFT JOIN pg_constraint s ON a.attrelid = s.conrelid',
            '  AND s.contype = {} AND a.attnum = any(s.conkey)'.format(self.val_placeholder),
            'LEFT JOIN pg_class c ON s.confrelid = c.oid',
            'WHERE',
            '  a.attrelid = {}::regclass'.format(self.val_placeholder),
            '  AND a.attisdropped = False',
            '  AND a.attnum > 0',
            'ORDER BY a.attnum ASC',
        ]
        query_str = os.linesep.join(query_str)
        fields = self.query(query_str, *query_args, **kwargs)

        pg_types = {
            "float8": float,
            "timestamp": datetime.datetime,
            "int2": int,
            "int4": int,
            "int8": long,
            "numeric": decimal.Decimal,
            "text": str,
            "bpchar": str,
            "varchar": str,
            "bool": bool,
            "date": datetime.date,
            "blob": bytearray,
        }

        # the rows we can set: field_type, name, field_required, min_size, max_size,
        #   size, unique, pk, <foreign key info>
        # These keys will roughly correspond with schema.Field
        for row in fields:
            field = {
                "name": row["attname"],
                "field_type": pg_types[row["typname"]],
                "field_required": row["attnotnull"],
                "pk": bool(row["indisprimary"]),
            }

            if row["confrelname"]:
                # TODO -- I can't decide which name I like
                field["schema_table_name"] = row["confrelname"]
                field["ref_table_name"] = row["confrelname"]

            ret[field["name"]] = field

        return ret

    def _get_indexes(self, schema, **kwargs):
        """return all the indexes for the given schema"""
        ret = {}
        query_str = [
            'SELECT',
            '  tbl.relname AS table_name, i.relname AS index_name, a.attname AS field_name,',
            '  ix.indkey AS index_order, a.attnum AS field_num',
            'FROM',
            '  pg_class tbl, pg_class i, pg_index ix, pg_attribute a',
            'WHERE',
            '  tbl.oid = ix.indrelid AND i.oid = ix.indexrelid AND a.attrelid = tbl.oid',
            '  AND a.attnum = ANY(ix.indkey) AND tbl.relkind = %s AND tbl.relname = %s',
            'ORDER BY',
            '  tbl.relname, i.relname',
        ]
        query_str = os.linesep.join(query_str)

        indexes = self.query(query_str, 'r', str(schema), **kwargs)

        # massage the data into more readable {index_name: fields} format
        for idict in indexes:
            if idict['index_name'] not in ret:
                ret[idict['index_name']] = list(map(int, idict['index_order'].split(' ')))

            i = ret[idict['index_name']].index(idict['field_num'])
            ret[idict['index_name']][i] = idict['field_name']

        return ret

    def _set_index(self, schema, name, fields, **index_options):
        """
        NOTE -- we set the index name using <table_name>_<name> format since indexes have to have
        a globally unique name in postgres

        http://www.postgresql.org/docs/9.1/static/sql-createindex.html
        """
        index_fields = []
        for field_name in fields:
            field = schema.fields[field_name]
            if issubclass(field.type, basestring):
                if field.options.get('ignore_case', False):
                    field_name = 'UPPER({})'.format(self._normalize_name(field_name))
            index_fields.append(field_name)

        query_str = 'CREATE {}INDEX {} ON {} USING BTREE ({})'.format(
            'UNIQUE ' if index_options.get('unique', False) else '',
            self._normalize_name("{}_{}".format(schema, name)),
            self._normalize_table_name(schema),
            ', '.join(index_fields)
        )

        return self.query(query_str, ignore_result=True, **index_options)

    def _insert(self, schema, fields, **kwargs):
        field_formats = []
        field_names = []
        query_vals = []
        for field_name, field_val in fields.items():
            field_names.append(self._normalize_name(field_name))
            field_formats.append(self.val_placeholder)
            query_vals.append(field_val)

        pk_name = schema.pk_name
        if pk_name:
            query_str = 'INSERT INTO {} ({}) VALUES ({}) RETURNING {}'.format(
                self._normalize_table_name(schema),
                ', '.join(field_names),
                ', '.join(field_formats),
                self._normalize_name(pk_name),
            )
            ret = self.query(query_str, *query_vals, **kwargs)
            ret = ret[0][pk_name]

        else:
            query_str = 'INSERT INTO {} ({}) VALUES ({})'.format(
                self._normalize_table_name(schema),
                ', '.join(field_names),
                ', '.join(field_formats),
            )
            ret = self.query(query_str, *query_vals, ignore_result=True, **kwargs)

        return ret

    def _normalize_field_SQL(self, schema, field_name, symbol):
        format_field_name = self._normalize_name(field_name)
        format_val_str = self.val_placeholder

        if 'LIKE' in symbol:
            format_field_name += '::text'

        # postgres specific for getting around case sensitivity:
        if schema.fields[field_name].options.get('ignore_case', False):
            format_field_name = 'UPPER({})'.format(field_name)
            format_val_str = 'UPPER({})'.format(self.val_placeholder)

        return format_field_name, format_val_str

    def _normalize_sort_SQL(self, field_name, field_vals, sort_dir_str):
        # this solution is based off:
        # http://postgresql.1045698.n5.nabble.com/ORDER-BY-FIELD-feature-td1901324.html
        # see also: https://gist.github.com/cpjolicoeur/3590737
        query_sort_str = []
        query_args = []
        for v in reversed(field_vals):
            query_sort_str.append('  {} = {} {}'.format(
                self._normalize_name(field_name), self.val_placeholder, sort_dir_str))
            query_args.append(v)

        return ',\n'.join(query_sort_str), query_args

    def _normalize_date_SQL(self, field_name, field_kwargs, symbol):
        """
        allow extracting information from date

        http://www.postgresql.org/docs/8.3/static/functions-datetime.html#FUNCTIONS-DATETIME-EXTRACT
        """
        fstrs = []
        k_opts = {
            'century': 'EXTRACT(CENTURY FROM {})',
            'day': 'EXTRACT(DAY FROM {})',
            'decade': 'EXTRACT(DECADE FROM {})',
            'weekday': 'EXTRACT(DOW FROM {})',
            'dow': 'EXTRACT(DOW FROM {})',
            'isodow': 'EXTRACT(ISODOW FROM {})',
            'epoch': 'EXTRACT(EPOCH FROM {})',
            'hour': 'EXTRACT(HOUR FROM {})',
            'year': 'EXTRACT(YEAR FROM {})',
            'isoyear': 'EXTRACT(ISOYEAR FROM {})',
            'minute': 'EXTRACT(MINUTE FROM {})',
            'month': 'EXTRACT(MONTH FROM {})',
            'quarter': 'EXTRACT(QUARTER FROM {})',
            'week': 'EXTRACT(WEEK FROM {})',
        }

        for k, v in field_kwargs.items():
            fstrs.append([k_opts[k].format(self._normalize_name(field_name)), self.val_placeholder, v])

        return fstrs

    def get_field_SQL(self, field_name, field):
        """
        returns the SQL for a given field with full type information

        field_name -- string -- the field's name
        fields -- Field() -- the info for the field

        return -- string -- the field type (eg, foo BOOL NOT NULL)
        """
        field_type = ""
        is_pk = field.options.get('pk', False)

        if issubclass(field.type, bool):
            field_type = 'BOOL'

        elif issubclass(field.type, long):
            if is_pk:
                field_type = 'BIGSERIAL PRIMARY KEY'
            else:
                field_type = 'BIGINT'

        elif issubclass(field.type, int):
            #size = 2147483647
            if is_pk:
                field_type = 'BIGSERIAL PRIMARY KEY'

            else:
                size = sys.maxsize # http://stackoverflow.com/questions/7604966
                if 'size' in field.options:
                    size = field.options['size']
                elif 'max_size' in field.options:
                    size = field.options['max_size']

                if size < 32767:
                    field_type = 'SMALLINT'
                else:
                    if field.is_ref():
                        field_type = 'BIGINT'
                    else:
                        field_type = 'INTEGER'

        elif issubclass(field.type, basestring):
            fo = field.options
            if field.is_ref():
                ref_s = field.schema
                fo = ref_s.pk.options

            if 'size' in fo:
                field_type = 'CHAR({})'.format(fo['size'])
            elif 'max_size' in fo:
                field_type = 'VARCHAR({})'.format(fo['max_size'])
            else:
                field_type = 'TEXT'

            if is_pk:
                field_type += ' PRIMARY KEY'

        elif issubclass(field.type, datetime.datetime):
            # http://www.postgresql.org/docs/9.0/interactive/datatype-datetime.html
            field_type = 'TIMESTAMP WITHOUT TIME ZONE'

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

        if not is_pk:
            if field.required:
                field_type += ' NOT NULL'
            else:
                field_type += ' NULL'

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

    def _handle_error(self, schema, e, **kwargs):
        ret = False
        if isinstance(e, psycopg2.ProgrammingError):
            e_msg = str(e)
            if "does not exist" in e_msg:
                if "column" in e_msg:
                    #INSERT: 'column "cancelled_date" of relation "chat_followup" does not exist'
                    #SELECT: 'column "cancelled_date" does not exist'
                    try:
                        ret = self._set_all_fields(schema, **kwargs)
                    except ValueError as e:
                        ret = False

                else:
                    #'relation "table_name" does not exit'
                    ret = self._set_all_tables(schema, **kwargs)

        return ret

    def _create_error(self, e, exc_info):
        if isinstance(e, psycopg2.IntegrityError):
            er = UniqueError(e, exc_info)
        else:
            er = super(PostgreSQL, self)._create_error(e, exc_info)
        return er

