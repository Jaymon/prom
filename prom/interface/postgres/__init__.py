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
import uuid
import json

# third party
import psycopg2
import psycopg2.extras
import psycopg2.extensions

# first party
from ..sql import SQLInterface, SQLConnection
from ...compat import *
from ...utils import get_objects
from ...exception import (
    InterfaceError,
    UniqueError,
    TableError,
    FieldError,
    UniqueError,
    CloseError,
)


# class LoggingCursor(psycopg2.extras.RealDictCursor):
#     def execute(self, sql, args=None):
#         logger.debug(self.mogrify(sql, args))
#         super(LoggingCursor, self).execute(sql, args)
#         #psycopg2.extensions.cursor.execute(self, sql, args)


class DictType(psycopg2.extensions.QuotedString):
    """Converts from python dict to JSONB to be saved into the db

    https://www.psycopg.org/docs/extensions.html#psycopg2.extensions.QuotedString
    https://www.psycopg.org/docs/extensions.html#psycopg2.extensions.register_adapter

    Surprisingly, Postgres converts the json value in the db back to dict just fine
    but does not adapt a dict, I'm not sure why
    """
    @classmethod
    def adapt(cls, val):
        """adapter should be a function taking a single argument (the object to adapt)
        and returning an object conforming to the ISQLQuote protocol (e.g. exposing
        a getquoted() method). Once an object is registered, it can be safely used
        in SQL queries and by the adapt() function

        :param val: dict, the value coming from Python destined for Postgres
        :returns: str
        """
        return cls(json.dumps(val))


class StringType(object):
    @classmethod
    def convert(cls, val, cur):
        """Convert a db string to a python str type

        https://www.psycopg.org/docs/extensions.html#psycopg2.extensions.new_type

            These functions are used to manipulate type casters to convert from
            PostgreSQL types to Python objects.

            adapter should have signature fun(value, cur) where value is the string
            representation returned by PostgreSQL and cur is the cursor from which
            data are read. In case of NULL, value will be None. The adapter should
            return the converted object.

        :param val: str, the value coming from Postgres and destined for Python
        :param cur: cursor
        :returns: str
        """
        if isinstance(val, str) and val.startswith("\\x"):
            buf = psycopg2.BINARY(val, cur)
            val = bytes(buf).decode(cur.connection.encoding)

        return val


class PostgreSQLConnection(SQLConnection, psycopg2.extensions.connection):
#class Connection(SQLConnection, psycopg2.extras.LoggingConnection):
    """
    http://initd.org/psycopg/docs/advanced.html
    http://initd.org/psycopg/docs/extensions.html#psycopg2.extensions.connection
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # http://initd.org/psycopg/docs/connection.html#connection.autocommit
        self.autocommit = True

        psycopg2.extensions.register_type(
            # https://www.psycopg.org/docs/extensions.html#psycopg2.extensions.new_type
            psycopg2.extensions.new_type(psycopg2.STRING.values, "STRING", StringType.convert)
        )

        # https://www.psycopg.org/docs/extensions.html#psycopg2.extensions.register_adapter
        psycopg2.extensions.register_adapter(dict, DictType.adapt)


        # http://initd.org/psycopg/docs/connection.html#connection.set_client_encoding
        # https://www.postgresql.org/docs/current/static/multibyte.html
        # > The default is the encoding defined by the database
        # Not sure we want to override db encoding which is probably why I didn't
        # set this previously
        #self.set_client_encoding("UTF8")

        #self.initialize(logger)


class PostgreSQL(SQLInterface):

    val_placeholder = '%s'

    _connection_pool = None

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

        _, pool_class = get_objects(pool_class_name)

        self.log("connecting using pool class {}".format(pool_class_name))

        # http://initd.org/psycopg/docs/module.html#psycopg2.connect
        self._connection_pool = pool_class(
            minconn,
            maxconn,
            dbname=database,
            user=username,
            password=password,
            host=host,
            port=port,
            cursor_factory=psycopg2.extras.RealDictCursor,
            #cursor_factory=LoggingCursor,
            connection_factory=PostgreSQLConnection,
        )

    def _get_connection(self):
        connection = self._connection_pool.getconn()

        connection_id = id(connection)
        self.log(f"getting pool connection {connection_id}", )

        # change the connection readonly status if they don't match
        if bool(connection.readonly) != bool(self.connection_config.readonly):
            # https://www.psycopg.org/docs/connection.html#connection.readonly
            self.log_warning([
                f"Changing connection {connection_id}",
                f"to readonly={self.connection_config.readonly}",
                f"from readonly={connection.readonly}",
            ])
            connection.readonly = self.connection_config.readonly

        return connection

    def _free_connection(self, connection):
        # if an error was handled there is a chance that the connection was reset
        # and we don't want to put a dead connection back into the pool
        if connection.closed:
            self.log_warning(f"discarding pool connection {id(connection)} because it is closed")

        else:
            self.log(f"freeing pool connection {id(connection)}")
            self._connection_pool.putconn(connection)

    def _close(self):
        self._connection_pool.closeall()
        self._connection_pool = None

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

        ret = self._query(query_str, *query_args, **kwargs)
        # http://www.postgresql.org/message-id/CA+mi_8Y6UXtAmYKKBZAHBoY7F6giuT5WfE0wi3hR44XXYDsXzg@mail.gmail.com
        return [r['tablename'] for r in ret]

    def _delete_table(self, schema, **kwargs):
        """
        https://www.postgresql.org/docs/current/sql-droptable.html
        """
        query_str = 'DROP TABLE IF EXISTS {} CASCADE'.format(self._normalize_table_name(schema))
        ret = self._query(query_str, ignore_result=True, **kwargs)

    def _get_fields(self, table_name, **kwargs):
        """return all the fields for the given schema"""
        ret = {}
        query_args = ['f', self._normalize_table_name(table_name)]

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
        query_str = "\n".join(query_str)
        fields = self._query(query_str, *query_args, **kwargs)

        pg_types = {
            "float4": float,
            "float8": float,
            "timestamp": datetime.datetime,
            "timestamptz": datetime.datetime,
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
            "jsonb": bytearray,
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
        query_str = "\n".join(query_str)

        indexes = self._query(query_str, 'r', str(schema), **kwargs)

        # massage the data into more readable {index_name: fields} format
        for idict in indexes:
            if idict['index_name'] not in ret:
                ret[idict['index_name']] = list(map(int, idict['index_order'].split(' ')))

            i = ret[idict['index_name']].index(idict['field_num'])
            ret[idict['index_name']][i] = idict['field_name']

        return ret

    def _set_index(self, schema, name, field_names, **kwargs):
        """
        NOTE -- we set the index name using <table_name>_<name> format since indexes have to have
        a globally unique name in postgres

        http://www.postgresql.org/docs/9.1/static/sql-createindex.html
        """
        query_str = 'CREATE {}INDEX {} ON {} USING BTREE ({})'.format(
            'UNIQUE ' if kwargs.get('unique', False) else '',
            self._normalize_name("{}_{}".format(schema, name)),
            self._normalize_table_name(schema),
            ', '.join(map(self._normalize_name, field_names))
        )

        return self._query(query_str, ignore_result=True, **kwargs)

    def _normalize_field_SQL(self, schema, field_name, symbol):
        format_field_name = self._normalize_name(field_name)
        format_val_str = self.val_placeholder

        if 'LIKE' in symbol:
            format_field_name += '::text'

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

    def _normalize_bounds_SQL(self, bounds, sql_options):
        offset = bounds.offset
        if sql_options.get('one_query', False):
            limit = 1

        else:
            limit, offset = bounds.get()
            if not bounds.has_limit():
                limit = "ALL"

        return 'LIMIT {} OFFSET {}'.format(
            limit,
            offset
        )

    def render_datatype_int_sql(self, field_name, field, **kwargs):
        if field.is_pk():
            field_type = 'BIGSERIAL PRIMARY KEY' # INT8

        else:
            if field.is_ref():
                field_type = 'BIGINT' # INT8

            else:
                # https://www.postgresql.org/docs/current/datatype-numeric.html
                size_info = field.size_info()
                size = size_info["size"]

                if size == 0:
                    field_type = 'INTEGER' # INT4

                elif size < 32767:
                    field_type = 'SMALLINT' # INT2

                elif size < 2147483647:
                    field_type = 'INTEGER' # INT4

                elif size < 9223372036854775807:
                    field_type = 'BIGINT' # INT8

                else:
                    precision = size_info["precision"]
                    field_type = f'NUMERIC({precision}, 0)'

        return field_type

    def render_datatype_str_sql(self, field_name, field, **kwargs):
        if field.interface_options.get('ignore_case', False):
            kwargs.setdefault("datatype", "CITEXT")

        return super().render_datatype_str_sql(field_name, field, **kwargs)

    def render_datatype_datetime_sql(self, field_name, field, **kwargs):
        # http://www.postgresql.org/docs/9.0/interactive/datatype-datetime.html
        #field_type = 'TIMESTAMP WITHOUT TIME ZONE'

        # https://wiki.postgresql.org/wiki/Don't_Do_This#Don.27t_use_timestamp_.28without_time_zone.29
        return 'TIMESTAMPTZ'

    def render_datatype_dict_sql(self, field_name, field, **kwargs):
        # https://www.postgresql.org/docs/current/datatype-json.html
        # In general, most applications should prefer to store JSON data as
        # jsonb, unless there are quite specialized needs
        return 'JSONB'

    def render_datatype_float_sql(self, field_name, field, **kwargs):
        """
        https://www.postgresql.org/docs/current/datatype-numeric.html#DATATYPE-NUMERIC-DECIMAL
        """
        size_info = field.size_info()
        if size_info["has_precision"]:
            precision = size_info["precision"]
            scale = size_info["scale"]
            field_type = f'NUMERIC({precision}, {scale})'

        else:
            size = size_info["size"]

            # https://learn.microsoft.com/en-us/cpp/c-language/type-float
            if size < 3.402823466e+38:
                #field_type = 'REAL'
                # REAL only has 6 digits after the period, double precision has
                # 15 digits after the period
                field_type = 'DOUBLE PRECISION'

            elif size < 1.7976931348623158e+308:
                field_type = 'DOUBLE PRECISION'

            else:
                precision = size_info["precision"]
                field_type = f'NUMERIC({precision})'

        return field_type

    def render_datatype_uuid_sql(self, field_name, field, **kwargs):
        # https://www.postgresql.org/docs/current/datatype-uuid.html
        # https://www.postgresql.org/docs/current/functions-uuid.html
        field_type = 'UUID'
        if field.is_pk():
            field_type += ' DEFAULT gen_random_uuid() PRIMARY KEY'
        return field_type

    def create_error(self, e, **kwargs):
        if isinstance(e, psycopg2.ProgrammingError):
            e_msg = String(e)
            if "does not exist" in e_msg:
                if "column" in e_msg:
                    #INSERT: column "foo" of relation "<TABLE-NAME>" does not exist
                    #SELECT: column "foo" does not exist
                    e = FieldError(e)

                else:
                    #'relation "<TABLE-NAME>" does not exit'
                    e = TableError(e)

        elif isinstance(e, psycopg2.errors.AdminShutdown):
            e = CloseError(e)

        elif isinstance(e, psycopg2.errors.InFailedSqlTransaction):
            e = CloseError(e)

        elif isinstance(e, psycopg2.IntegrityError):
            e = UniqueError(e)

        else:
            e = super().create_error(e, **kwargs)

        return e

