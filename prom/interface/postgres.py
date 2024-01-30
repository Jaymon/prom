# -*- coding: utf-8 -*-
import re
import os
import sys
import decimal
import datetime
import uuid
import json
from collections import Counter

# third party
import psycopg
from psycopg.adapt import Dumper

# first party
from .sql import SQLInterface, SQLConnection
from ..compat import *
from ..utils import get_objects
from ..exception import (
    InterfaceError,
    UniqueError,
    TableError,
    FieldError,
    UniqueError,
    CloseError,
    PlaceholderError,
)


class DictDumper(Dumper):
    """Converts from python dict to JSONB to be saved into the db

    https://www.psycopg.org/psycopg3/docs/basic/adapt.html
    https://www.psycopg.org/psycopg3/docs/advanced/adapt.html
    https://www.psycopg.org/psycopg3/docs/api/adapt.html#psycopg.adapt.Dumper

    Surprisingly, Postgres converts the json value in the db back to dict just
    fine but does not adapt a dict, I'm not sure why
    """
    TYPE = dict

    def dump(self, val):
        """adapter should be a function taking a single argument (the object to
        adapt) and returning an object conforming to the ISQLQuote protocol
        (e.g. exposing a getquoted() method). Once an object is registered, it
        can be safely used in SQL queries and by the adapt() function

        :param val: dict, the value coming from Python destined for Postgres
        :returns: bytes
        """
        return ByteString(json.dumps(val))


class AsyncPostgreSQLConnection(SQLConnection, psycopg.AsyncConnection):
    """
    https://www.psycopg.org/docs/connection.html
    """
    pass


class PostgreSQL(SQLInterface):
    """
    https://www.psycopg.org/psycopg3/docs/advanced/async.html
    https://www.psycopg.org/psycopg3/docs/basic/from_pg2.html
    https://github.com/psycopg/psycopg

    https://www.psycopg.org/docs/
    https://www.psycopg.org/docs/usage.html
    """
    LIMIT_NONE = "ALL"

    _connection = None

    @classmethod
    async def configure(cls, config):
        port = config.port
        if not port:
            config.port = 5432
        return config

    def get_paramstyle(self):
        """
        https://www.psycopg.org/docs/module.html#psycopg2.paramstyle
        """
        return psycopg.paramstyle

    async def _connect(self, config):
        """
        https://www.psycopg.org/psycopg3/docs/api/connections.html

        If I ever wanted to add pool support back:
            https://www.psycopg.org/psycopg3/docs/advanced/pool.html
        """
        self._connection = await AsyncPostgreSQLConnection.connect(
            dbname=config.database,
            user=config.username,
            password=config.password,
            host=config.host,
            port=config.port,
            row_factory=psycopg.rows.dict_row,
            autocommit=True, # if False there will be random hangs
        )

        self.log_debug("Connected to connection {}", self._connection)

        await self.configure_connection(connection=self._connection)

    async def _configure_connection(self, **kwargs):
        kwargs["prefix"] = "_configure_connection"
        async with self.connection(**kwargs) as connection:
            connection.adapters.register_dumper(DictDumper.TYPE, DictDumper)

    async def _get_connection(self):
        return self._connection

    async def _close(self):
        await self._connection.close()
        self._connection = None

    async def _readonly(self, readonly, **kwargs):
        """
        https://www.psycopg.org/psycopg3/docs/api/connections.html#psycopg.Connection.set_read_only
        https://www.psycopg.org/psycopg3/docs/api/connections.html#psycopg.AsyncConnection.set_read_only
        """
        async with self.connection(**kwargs) as connection:
            kwargs["connection"] = connection

            await connection.set_read_only(readonly)

            # https://www.psycopg.org/psycopg3/docs/basic/from_pg2.html - search
            # for "read_only"
            # https://www.postgresql.org/docs/current/runtime-config-client.html#GUC-DEFAULT-TRANSACTION-READ-ONLY
            await self._raw(
                'SET default_transaction_read_only TO {}'.format(
                    "true" if readonly else "false"),
                ignore_result=True,
                **kwargs
            )

    async def _get_tables(self, table_name, **kwargs):
        query_str = "\n".join([
            "SELECT",
            "  tablename",
            "FROM",
            "  pg_tables",
            "WHERE",
            "  tableowner = {}".format(self.PLACEHOLDER)
        ])
        query_args = [self.config.username]

        if table_name:
            query_str += " AND tablename = {}".format(self.PLACEHOLDER)
            query_args.append(str(table_name))

        schema_name = kwargs.get("schema_name", "public")
        if schema_name:
            query_str += " AND schemaname = {}".format(self.PLACEHOLDER)
            query_args.append(schema_name)

        ret = await self._raw(query_str, *query_args, **kwargs)
        # http://www.postgresql.org/message-id/CA+mi_8Y6UXtAmYKKBZAHBoY7F6giuT5WfE0wi3hR44XXYDsXzg@mail.gmail.com
        return [r['tablename'] for r in ret]

    async def _delete_table(self, schema, **kwargs):
        """
        https://www.postgresql.org/docs/current/sql-droptable.html
        """
        query_str = "DROP TABLE IF EXISTS {} CASCADE".format(
            self.render_table_name_sql(schema)
        )
        await self._raw(query_str, ignore_result=True, **kwargs)

    async def _get_indexes(self, schema, **kwargs):
        """return all the indexes for the given schema"""
        ret = {}
        query_str = [
            "SELECT",
            "  tbl.relname AS table_name,",
            "  i.relname AS index_name,",
            "  a.attname AS field_name,",
            "  ix.indkey AS index_order,",
            "  a.attnum AS field_num",
            "FROM",
            "  pg_class tbl, pg_class i, pg_index ix, pg_attribute a",
            "WHERE",
            "  tbl.oid = ix.indrelid",
            "  AND i.oid = ix.indexrelid",
            "  AND a.attrelid = tbl.oid",
            "  AND a.attnum = ANY(ix.indkey)",
            "  AND tbl.relkind = {}".format(self.PLACEHOLDER),
            "  AND tbl.relname = {}".format(self.PLACEHOLDER),
            "ORDER BY",
            "  tbl.relname, i.relname",
        ]
        query_str = "\n".join(query_str)

        kwargs["cursor_result"] = True
        indexes = await self._raw(query_str, "r", str(schema), **kwargs)

        # massage the data into more readable {index_name: fields} format
        async for idict in indexes:
            if idict['index_name'] not in ret:
                ret[idict['index_name']] = list(
                    map(int, idict['index_order'].split(' '))
                )

            i = ret[idict['index_name']].index(idict['field_num'])
            ret[idict['index_name']][i] = idict['field_name']

        return ret

    async def _get_fields(self, table_name, **kwargs):
        """return all the fields for the given schema"""
        ret = {}

        # I had to brush up on my join knowledge while writing this query
        # https://en.wikipedia.org/wiki/Join_(SQL)
        #
        # other helpful links
        # * https://wiki.postgresql.org/wiki/Retrieve_primary_key_columns
        # * https://www.postgresql.org/docs/9.4/static/catalog-pg-attribute.html
        # * https://www.postgresql.org/docs/9.3/static/catalog-pg-type.html
        # 
        # another approach
        # * http://dba.stackexchange.com/questions/22362/
        # * http://gis.stackexchange.com/questions/94049/
        query_str = "\n".join([
            "SELECT",
            "  a.attnum,",
            "  a.attname,",
            "  a.attnotnull,",
            "  t.typname,",
            "  i.indisprimary,",
            "  c.relname AS confrelname",
            "FROM",
            "  pg_attribute a",
            "JOIN pg_type t ON a.atttypid = t.oid",
            "LEFT JOIN",
            "  pg_index i ON a.attrelid = i.indrelid",
            "  AND a.attnum = any(i.indkey)",
            "LEFT JOIN",
            "  pg_constraint s ON a.attrelid = s.conrelid",
            "  AND s.contype = {}".format(self.PLACEHOLDER),
            "  AND a.attnum = any(s.conkey)",
            "LEFT JOIN",
            "  pg_class c ON s.confrelid = c.oid",
            "WHERE",
            "  a.attrelid = {}::regclass".format(self.PLACEHOLDER),
            "  AND a.attisdropped = False",
            "  AND a.attnum > 0",
            "ORDER BY a.attnum ASC",
        ])
        query_args = ["f", self.render_table_name_sql(table_name)]
        kwargs["cursor_result"] = True
        fields = await self._raw(query_str, *query_args, **kwargs)

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
            "citext": str,
            "bpchar": str,
            "varchar": str,
            "bool": bool,
            "date": datetime.date,
            "blob": bytes,
            "bytea": bytes,
            "jsonb": bytes,
            "uuid": uuid.UUID,
        }

        # the rows we can set:
        #   field_type, name, field_required, min_size, max_size, size, unique,
        #   pk, <foreign key info>
        # These keys will roughly correspond with schema.Field
        async for row in fields:
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

            if row["typname"] == "citext":
                field["ignore_case"] = True

            ret[field["name"]] = field

        return ret

    def render_sort_field_sql(self, field_name, field_vals, sort_dir_str):
        # this solution is based off:
        # http://postgresql.1045698.n5.nabble.com/ORDER-BY-FIELD-feature-td1901324.html
        # see also: https://gist.github.com/cpjolicoeur/3590737
        query_sort_str = []
        query_args = []
        for v in reversed(field_vals):
            query_sort_str.append('  {} = {} {}'.format(
                self.render_field_name_sql(field_name),
                self.PLACEHOLDER,
                sort_dir_str
            ))
            query_args.append(v)

        return ',\n'.join(query_sort_str), query_args

    def render_date_field_sql(self, field_name, field_kwargs, symbol):
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
            fstrs.append([
                k_opts[k].format(self.render_field_name_sql(field_name)),
                self.PLACEHOLDER,
                v
            ])

        return fstrs

    def render_datatype_int_sql(self, field_name, field, **kwargs):
        if field.is_ref():
            field_type = 'BIGINT' # INT8

        else:
            # https://www.postgresql.org/docs/current/datatype-numeric.html
            size_info = field.size_info()
            size = size_info["size"]

            if size == 0:
                field_type = 'INTEGER' # INT4

            elif size <= 32767:
                field_type = 'SMALLINT' # INT2

            elif size <= 2147483647:
                # INT4
                field_type = 'SERIAL' if field.is_auto() else 'INTEGER'

            elif size <= 9223372036854775807:
                # INT8
                field_type = 'BIGSERIAL' if field.is_auto() else 'BIGINT'

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

    def render_datatype_bytes_sql(self, field_name, field, **kwargs):
        """Why Postgres? Why?

        https://www.postgresql.org/docs/current/datatype-binary.html
            The SQL standard defines a different binary string type, called BLOB
            or BINARY LARGE OBJECT. The input format is different from bytea,
            but the provided functions and operators are mostly the same.
        """
        return 'BYTEA'

    def render_datatype_uuid_sql(self, field_name, field, **kwargs):
        # https://www.postgresql.org/docs/current/datatype-uuid.html
        # https://www.postgresql.org/docs/current/functions-uuid.html
        field_type = 'UUID'
        if field.is_auto():
            field_type += ' DEFAULT gen_random_uuid()'

        return field_type

    def create_error(self, e, **kwargs):
        """
        https://www.psycopg.org/docs/module.html#exceptions
        """
        kwargs.setdefault("error_module", psycopg)
        if isinstance(e, psycopg.ProgrammingError):
            e_msg = String(e)
            if "does not exist" in e_msg:
                if "column" in e_msg:
                    # INSERT: column "foo" of relation "<TABLE-NAME>" does not
                    #   exist
                    # SELECT: column "foo" does not exist
                    e = FieldError(e)

                else:
                    #'relation "<TABLE-NAME>" does not exit'
                    e = TableError(e)

            elif "cannot adapt type" in e_msg:
                ms = re.search(r"type\s'(\S+)'", e_msg)
                type_name = ms.group(1)
                if error_args := kwargs.get("error_args", []):
                    for index, value in enumerate(error_args[1]):
                        if type_name in str(type(value)):
                            e = PlaceholderError(
                                e,
                                message=" ".join([
                                    f"Placeholder {index} of query",
                                    f"has unexpected type {type(value)}"
                                ])
                            )
                            break

                else:
                    e = PlaceholderError(e)

            elif (
                "the query has" in e_msg
                and "placeholders" in e_msg
                and "parameters were passed" in e_msg
            ):
                e = PlaceholderError(e)

            else:
                e = super().create_error(e, **kwargs)

        elif isinstance(e, psycopg.errors.AdminShutdown):
            e = CloseError(e)

        elif isinstance(e, psycopg.errors.InFailedSqlTransaction):
            e = CloseError(e)

        elif isinstance(e, psycopg.errors.IntegrityError):
            e = UniqueError(e)

        elif isinstance(e, IndexError):
            e = PlaceholderError(e)

        else:
            e = super().create_error(e, **kwargs)

        return e

