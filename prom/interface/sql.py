# -*- coding: utf-8 -*-
from __future__ import unicode_literals, division, print_function, absolute_import
import os
import datetime
import decimal
import logging
import uuid

from ..query import Query
from ..exception import (
    TableError,
    FieldError,
    UniqueError,
)

from ..compat import *
from ..utils import make_list
from .base import Connection, Interface


logger = logging.getLogger(__name__)


class SQLConnection(Connection):
    """
    https://docs.python.org/3.9/library/sqlite3.html#sqlite3-controlling-transactions
    """
    def _transaction_start(self):
        self._execute("BEGIN")

    def _transaction_started(self, name):
        # http://www.postgresql.org/docs/9.2/static/sql-savepoint.html
        self._execute("SAVEPOINT {}".format(self.interface._normalize_name(name)))

    def _transaction_stop(self):
        """
        http://initd.org/psycopg/docs/usage.html#transactions-control
        https://news.ycombinator.com/item?id=4269241
        """
        self._execute("COMMIT")

    def _transaction_stopping(self, name):
        self._execute("RELEASE {}".format(self.interface._normalize_name(name)))

    def _transaction_fail(self):
        self._execute("ROLLBACK")

    def _transaction_failing(self, name):
        # http://www.postgresql.org/docs/9.2/static/sql-rollback-to.html
        self._execute("ROLLBACK TO SAVEPOINT {}".format(self.interface._normalize_name(name)))

    def _execute(self, query_str):
        self.log_info(f"0x{id(self):02x} - {query_str}")
        cur = self.cursor()
        cur.execute(query_str)
        cur.close()


class SQLInterfaceABC(Interface):
    """SQL database interfaces should extend SQLInterface and implement all these
    methods in this class and all the methods in InterfaceABC"""
    @property
    def val_placeholder(self):
        raise NotImplementedError("this property should be set in any children class")

    def _normalize_date_SQL(self, field_name, field_kwargs, symbol):
        raise NotImplemented()

    def _normalize_field_SQL(self, schema, field_name, symbol):
        return self._normalize_name(field_name), self.val_placeholder

    def _normalize_sort_SQL(self, field_name, field_vals, sort_dir_str):
        """normalize the sort string

        return -- tuple -- field_sort_str, field_sort_args"""
        raise NotImplemented()

    def _normalize_bounds_SQL(self, bounds):
        raise NotImplemented()

    def render_datatype_datetime_sql(self, field_name, field, **kwargs):
        raise NotImplementedError()

    def render_datatype_dict_sql(self, field_name, field, **kwargs):
        raise NotImplementedError()

    def render_datatype_uuid_sql(self, field_name, field, **kwargs):
        raise NotImplementedError()


class SQLInterface(SQLInterfaceABC):
    """Generic base class for all SQL derived interfaces"""
    def _set_table(self, schema, **kwargs):
        """
        http://sqlite.org/lang_createtable.html
        http://www.postgresql.org/docs/9.1/static/sql-createtable.html
        http://www.postgresql.org/docs/8.1/static/datatype.html
        http://pythonhosted.org/psycopg2/usage.html#adaptation-of-python-values-to-sql-types
        """
        query_str = []
        query_str.append("CREATE TABLE IF NOT EXISTS {} (".format(self._normalize_table_name(schema)))

        query_fields = []
        for field_name, field in schema.fields.items():
            query_fields.append('  {}'.format(self.render_datatype_sql(field_name, field)))

        query_str.append(",\n".join(query_fields))
        query_str.append(')')
        query_str = "\n".join(query_str)
        self._query(query_str, ignore_result=True, **kwargs)

    def _set_index(self, schema, name, field_names, **kwargs):
        """
        NOTE -- we set the index name using <table_name>_<name> format since indexes have to have
        a globally unique name in postgres

        * https://www.sqlite.org/lang_createindex.html
        * https://www.postgresql.org/docs/14/sql-createindex.html - "IF NOT EXISTS support
        was added around 9.5
        """
        query_str = 'CREATE {}INDEX IF NOT EXISTS {} ON {} ({})'.format(
            'UNIQUE ' if kwargs.get('unique', False) else '',
            self._normalize_name(f"{schema}_{name}"),
            self._normalize_table_name(schema),
            ', '.join(map(self._normalize_name, field_names))
        )

        return self._query(query_str, ignore_result=True, **kwargs)

    def _insert(self, schema, fields, **kwargs):
        pk_names = schema.pk_names
        kwargs.setdefault("ignore_return_clause", len(pk_names) == 0)
        kwargs.setdefault("ignore_result", len(pk_names) == 0)

        query_str, query_args = self.render_insert_sql(
            schema,
            fields,
            **kwargs,
        )

        r = self._query(query_str, *query_args, **kwargs)
        if r and pk_names:
            if len(pk_names) > 1:
                r = r[0]
            else:
                r = r[0][pk_names[0]]
        return r

    def _update(self, schema, fields, query, **kwargs):
        query_str, query_args = self.render_update_sql(
            schema,
            fields,
            query=query,
            **kwargs,
        )

        return self._query(query_str, *query_args, count_result=True, **kwargs)

    def _upsert(self, schema, insert_fields, update_fields, conflict_field_names, **kwargs):
        """
        https://www.sqlite.org/lang_UPSERT.html
        """
        if not conflict_field_names:
            raise ValueError(f"Upsert is missing conflict fields for {schema}")

        for field_name in conflict_field_names:
            # conflict fields need to be in the insert fields
            if field_name not in insert_fields:
                raise ValueError(f"Upsert insert fields on {schema} missing conflict field {field_name}")

            # conflict fields should not be in the udpate fields (this is more
            # for safety, they should use .update if they want to change them)
            if field_name in update_fields:
                raise ValueError(f"Upsert update fields on {schema} contains conflict field {field_name}")

        insert_sql, insert_args = self.render_insert_sql(
            schema,
            insert_fields,
            ignore_return_clause=True,
            **kwargs,
        )

        update_sql, update_args = self.render_update_sql(
            schema,
            update_fields,
            query=None,
            only_set_clause=True,
            **kwargs,
        )

        query_str = '{} ON CONFLICT({}) DO UPDATE {}'.format(
            insert_sql,
            ', '.join(map(self._normalize_name, conflict_field_names)),
            update_sql,
        )

        returning_field_names = schema.pk_names
        if returning_field_names:
            # https://www.sqlite.org/lang_returning.html
            query_str += ' RETURNING {}'.format(', '.join(map(self._normalize_name, returning_field_names)))
            query_args = insert_args + update_args

        r = self._query(query_str, *query_args, **kwargs)
        if r and returning_field_names:
            if len(returning_field_names) > 1:
                r = r[0]
            else:
                r = r[0][returning_field_names[0]]
        return r

    def _delete(self, schema, query, **kwargs):
        where_query_str, query_args = self.get_SQL(schema, query, only_where_clause=True)
        query_str = []
        query_str.append('DELETE FROM')
        query_str.append('  {}'.format(self._normalize_table_name(schema)))
        query_str.append(where_query_str)
        query_str = "\n".join(query_str)
        ret = self._query(query_str, *query_args, count_result=True, **kwargs)
        return ret

    def _query(self, query_str, *query_args, **kwargs):
        """
        **kwargs -- dict
            ignore_result -- boolean -- true to not attempt to fetch results
            fetchone -- boolean -- true to only fetch one result
            count_result -- boolean -- true to return the int count of rows affected
        """
        ret = True
        # http://stackoverflow.com/questions/6739355/dictcursor-doesnt-seem-to-work-under-psycopg2
        #connection = kwargs.get('connection', None)
        with self.connection(**kwargs) as connection:
            cur = connection.cursor()
            ignore_result = kwargs.get('ignore_result', False)
            count_result = kwargs.get('count_result', False)
            one_result = kwargs.get('fetchone', kwargs.get('one_result', False))
            cursor_result = kwargs.get('cursor_result', False)

            if query_args:
                self.log_for(
                    debug=(["0x{:02x} - {}\n{}", id(connection), query_str, query_args],),
                    info=([f"0x{id(connection):02x} - {query_str}"],)
                )
                cur.execute(query_str, query_args)
            else:
                self.log_info(f"0x{id(connection):02x} - {query_str}")
                cur.execute(query_str)

            if cursor_result:
                ret = cur

            elif ignore_result:
                cur.close()

            else:
                if one_result:
                    # https://www.psycopg.org/docs/cursor.html#cursor.fetchone
                    ret = cur.fetchone()

                elif count_result:
                    # https://www.psycopg.org/docs/cursor.html#cursor.rowcount
                    ret = cur.rowcount

                else:
                    # https://www.psycopg.org/docs/cursor.html#cursor.fetchall
                    ret = cur.fetchall()

                cur.close()

            return ret

    def _get_one(self, schema, query, **kwargs):
        query_str, query_args = self.get_SQL(schema, query, one_query=True)
        return self._query(query_str, *query_args, fetchone=True, **kwargs)

    def _get(self, schema, query, **kwargs):
        query_str, query_args = self.get_SQL(schema, query)
        return self._query(query_str, *query_args, **kwargs)

    def _count(self, schema, query, **kwargs):
        query_str, query_args = self.get_SQL(schema, query, count_query=True)
        ret = self._query(query_str, *query_args, **kwargs)
        if ret:
            ret = int(ret[0]['ct'])
        else:
            ret = 0

        return ret

    def _handle_field_error(self, schema, e, **kwargs):
        """
        this will add fields that don't exist in the table if they can be set to NULL,
        the reason they have to be NULL is adding fields to Postgres that can be NULL
        is really light, but if they have a default value, then it can be costly
        """
        current_fields = self._get_fields(schema, **kwargs)
        for field_name, field in schema.fields.items():
            if field_name not in current_fields:
                if field.required:
                    self.log_error(f"Cannot safely add {field_name} on the fly because it is required")
                    return False

                else:
                    query_str = []
                    query_str.append('ALTER TABLE')
                    query_str.append('  {}'.format(self._normalize_table_name(schema)))
                    query_str.append('ADD COLUMN')
                    query_str.append('  {}'.format(self.render_datatype_sql(field_name, field)))
                    query_str = "\n".join(query_str)
                    self._query(query_str, ignore_result=True, **kwargs)

        return True

    def _handle_table_error(self, schema, e, **kwargs):
        """
        You can run into a problem when you are trying to set a table and it has a 
        foreign key to a table that doesn't exist, so this method will go through 
        all fk refs and make sure all the tables exist
        """
        if query := kwargs.pop("query", None):
            if schemas := query.schemas:
                for s in schemas:
                    self.log_warning(f"Verifying foreign key table: {s}")
                    if not self._handle_table_error(s, e=e, **kwargs):
                        return False

        for field_name, field_val in schema.fields.items():
            s = field_val.schema
            if s:
                self.log_warning(f"Verifying foreign key table: {s}")
                if not self._handle_table_error(schema=s, e=e, **kwargs):
                    return False

        # now that we know all fk tables exist, create this table
        # !!! This uses the external .set_table so it will run through all the 
        # indexes also
        self.set_table(schema, **kwargs)
        return True

    def _normalize_val_SQL(self, schema, symbol_map, field):
        format_str = ''
        format_args = []
        symbol = symbol_map['symbol']
        is_list = field.is_list
        field_name = field.name
        field_val = field.value
        field_kwargs = field.kwargs

        if field_kwargs:
            # kwargs take precedence because None is a perfectly valid field_val
            f = schema.fields[field_name]
            if issubclass(f.type, (datetime.datetime, datetime.date)):
                format_strs = self._normalize_date_SQL(field_name, field_kwargs, symbol)
                for fname, fvstr, farg in format_strs:
                    if format_str:
                        format_str += ' AND '

                    if is_list:
                        # you can pass in things like day=..., month=... to
                        # date fields, this converts those values to lists to
                        # make sure we can handle something like in_foo(day=1)
                        # and in_foo(day=[1, 2, 3]) the same way
                        farg = make_list(farg)

                        format_str += '{} {} ({})'.format(
                            fname,
                            symbol,
                            ', '.join([fvstr] * len(farg))
                        )
                        format_args.extend(farg)

                    else:
                        format_str += '{} {} {}'.format(fname, symbol, fvstr)
                        format_args.append(farg)

            else:
                raise ValueError('Field {} does not support extended kwarg values'.format(field_name))

        else:
            if is_list and not isinstance(field_val, Query):
                field_val = make_list(field_val) if field_val else []
                field_name, format_val_str = self._normalize_field_SQL(schema, field_name, symbol)
                if field_val:
                    format_str = '{} {} ({})'.format(
                        field_name,
                        symbol,
                        ', '.join([format_val_str] * len(field_val))
                    )
                    format_args.extend(field_val)

                else:
                    # field value is empty, so we need to customize the SQL to
                    # compensate for the empty set since SQL doesn't like empty
                    # sets
                    #
                    # the idea here is this is a condition that will
                    # automatically cause the query to fail but not necessarily be an error, 
                    # the best example is the IN (...) queries, if you do self.in_foo([]).get()
                    # that will fail because the list was empty, but a value error shouldn't
                    # be raised because a common case is: self.if_foo(Bar.query.is_che(True).pks).get()
                    # which should result in an empty set if there are no rows where che = TRUE
                    #
                    # https://stackoverflow.com/a/58078468/5006
                    if symbol == "IN":
                        format_str = '{} <> {}'.format(field_name, field_name)

                    elif symbol == "NOT IN":
                        format_str = '{} = {}'.format(field_name, field_name)

                    else:
                        raise ValueError("Unsure what to do here")

            else:
                # special handling for NULL
                if field_val is None:
                    symbol = symbol_map['none_symbol']

                field_name, format_val_str = self._normalize_field_SQL(
                    schema,
                    field_name,
                    symbol
                )

                if isinstance(field_val, Query):
                    subquery_schema = field_val.schema
                    if not subquery_schema:
                        raise ValueError("{} subquery has no schema".format(field_name))

                    subquery_sql, subquery_args = self.get_SQL(
                        field_val.schema,
                        field_val
                    )

                    format_str = '{} {} ({})'.format(
                        field_name,
                        symbol,
                        subquery_sql
                    )
                    format_args.extend(subquery_args)

                else:
                    format_str = '{} {} {}'.format(
                        field_name,
                        symbol,
                        format_val_str
                    )
                    format_args.append(field_val)

        return format_str, format_args

    def _normalize_table_name(self, schema):
        return self._normalize_name(schema)

    def _normalize_name(self, name):
        """normalize a non value name for the query

        https://blog.christosoft.de/2012/10/sqlite-escaping-table-acolumn-names/

        :param name: str, the name that should be prepared to be queried
        :returns: the modified name ready to be added to a query string
        """
        return '"{}"'.format(name)

    def get_SQL(self, schema, query, **sql_options):
        """
        convert the query instance into SQL

        this is the glue method that translates the generic Query() instance to
        the SQL specific query, this is where the magic happens

        **sql_options -- dict
            count_query -- boolean -- true if this is a count query SELECT
            only_where_clause -- boolean -- true to only return after WHERE ...
        """
        only_where_clause = sql_options.get('only_where_clause', False)
        symbol_map = {
            'in': {'symbol': 'IN', 'list': True},
            'nin': {'symbol': 'NOT IN', 'list': True},
            'eq': {'symbol': '=', 'none_symbol': 'IS'},
            'ne': {'symbol': '!=', 'none_symbol': 'IS NOT'},
            'gt': {'symbol': '>'},
            'gte': {'symbol': '>='},
            'lt': {'symbol': '<'},
            'lte': {'symbol': '<='},
            # https://www.tutorialspoint.com/postgresql/postgresql_like_clause.htm
            # https://www.tutorialspoint.com/sqlite/sqlite_like_clause.htm
            'like': {'symbol': 'LIKE'},
            'nlike': {'symbol': 'NOT LIKE'},
        }

        query_args = []
        query_str = []

        if not only_where_clause:
            query_str.append('SELECT')
            is_count_query = sql_options.get('count_query', False)
            select_fields = query.fields_select
            if select_fields:
                distinct_fields = select_fields.options.get(
                    "distinct",
                    select_fields.options.get("unique", False)
                )
                distinct = "DISTINCT " if distinct_fields else ""
                select_fields_str = distinct + ", ".join(
                    (self._normalize_name(f.name) for f in select_fields)
                )
            else:
                if is_count_query or select_fields.options.get("all", False):
                    select_fields_str = "*"
                else:
                    select_fields_str = ", ".join(
                        (self._normalize_name(fname) for fname in schema.fields.keys())
                    )

            if is_count_query:
                query_str.append('  count({}) as ct'.format(select_fields_str))

            else:
                query_str.append('  {}'.format(select_fields_str))

            query_str.append('FROM')
            query_str.append("  {}".format(self._normalize_table_name(schema)))

        if query.fields_where:
            query_str.append('WHERE')

            for i, field in enumerate(query.fields_where):
                if i > 0: query_str.append('AND')

                field_str = ''
                field_args = []
                sd = symbol_map[field.operator]

                field_str, field_args = self._normalize_val_SQL(
                    schema,
                    sd,
                    field,
                )

                query_str.append('  {}'.format(field_str))
                query_args.extend(field_args)

        if query.fields_sort:
            query_sort_str = []
            query_str.append('ORDER BY')
            for field in query.fields_sort:
                sort_dir_str = 'ASC' if field.direction > 0 else 'DESC'
                if field.value:
                    field_sort_str, field_sort_args = self._normalize_sort_SQL(field.name, field.value, sort_dir_str)
                    query_sort_str.append(field_sort_str)
                    query_args.extend(field_sort_args)

                else:
                    query_sort_str.append('  {} {}'.format(field.name, sort_dir_str))

            query_str.append(',{}'.format(os.linesep).join(query_sort_str))

        if query.bounds:
            query_str.append(self._normalize_bounds_SQL(query.bounds, sql_options))

        query_str = "\n".join(query_str)
        return query_str, query_args

    def render_insert_sql(self, schema, fields, **kwargs):
        """
        https://www.sqlite.org/lang_insert.html
        """
        field_formats = []
        field_names = []
        query_vals = []
        for field_name, field_val in fields.items():
            field_names.append(self._normalize_name(field_name))
            field_formats.append(self.val_placeholder)
            query_vals.append(field_val)

        query_str = 'INSERT INTO {} ({}) VALUES ({})'.format(
            self._normalize_table_name(schema),
            ', '.join(field_names),
            ', '.join(field_formats),
        )

        if not kwargs.get("ignore_return_clause", False):
            # https://www.sqlite.org/lang_returning.html
            pk_name = schema.pk_name
            if pk_name:
                query_str += ' RETURNING {}'.format(self._normalize_name(pk_name))

        return query_str, query_vals

    def render_update_sql(self, schema, fields, query, **kwargs):
        query_str = ''
        query_args = []

        if not kwargs.get("only_set_clause", False):
            query_str = 'UPDATE {} '.format(self._normalize_table_name(schema))

        field_str = []
        for field_name, field_val in fields.items():
            field_str.append('{} = {}'.format(self._normalize_name(field_name), self.val_placeholder))
            query_args.append(field_val)

        query_str += 'SET {}'.format(',\n'.join(field_str))

        if query:
            where_query_str, where_query_args = self.get_SQL(schema, query, only_where_clause=True)
            query_str += ' {}'.format(where_query_str)
            query_args.extend(where_query_args)

        return query_str, query_args

    def render_datatype_sql(self, field_name, field):
        """Returns the SQL for a given field with full type information

        http://www.sqlite.org/datatype3.html
        https://www.postgresql.org/docs/current/datatype.html

        :param field_name: str, the field's name
        :param field: Field instance, the configuration for the field
        :returns: str, the complete field datatype SQL (eg, foo BOOL NOT NULL)
        """
        field_type = ""
        interface_type = field.interface_type

        if issubclass(interface_type, bool):
            field_type = self.render_datatype_bool_sql(field_name, field)

        elif issubclass(interface_type, int):
            field_type = self.render_datatype_int_sql(field_name, field)

        elif issubclass(interface_type, str):
            field_type = self.render_datatype_str_sql(field_name, field)

        elif issubclass(interface_type, datetime.datetime):
            field_type = self.render_datatype_datetime_sql(field_name, field)

        elif issubclass(interface_type, datetime.date):
            field_type = self.render_datatype_date_sql(field_name, field)

        elif issubclass(interface_type, dict):
            field_type = self.render_datatype_dict_sql(field_name, field)

        elif issubclass(interface_type, (float, decimal.Decimal)):
            field_type = self.render_datatype_float_sql(field_name, field)

        elif issubclass(interface_type, (bytearray, bytes)):
            field_type = self.render_datatype_bytes_sql(field_name, field)

        elif issubclass(interface_type, uuid.UUID):
            field_type = self.render_datatype_uuid_sql(field_name, field)

        else:
            raise ValueError('Unknown python type: {} for field: {}'.format(
                interface_type.__name__,
                field_name,
            ))

        field_type += ' ' + self.render_datatype_required_sql(field_name, field)

        if not field.is_pk():
            if field.is_ref():
                field_type += ' ' + self.render_datatype_ref_sql(field_name, field)

        return '{} {}'.format(self._normalize_name(field_name), field_type)

    def render_datatype_bool_sql(self, field_name, field, **kwargs):
        return 'BOOL'

    def render_datatype_int_sql(self, field_name, field, **kwargs):
        return 'INTEGER'

    def render_datatype_str_sql(self, field_name, field, **kwargs):
        fo = field.interface_options
        field_type = kwargs.get("datatype", 'TEXT')
        size_info = field.size_info()

        # https://www.sqlitetutorial.net/sqlite-check-constraint/
        if 'size' in size_info["original"]:
            field_type += f" CHECK(length({field_name}) = {size_info['size']})"

        elif 'max_size' in size_info["original"]:
            if "min_size" in size_info["original"]:
                field_type += f" CHECK(length({field_name}) >= {size_info['original']['min_size']}"
                field_type += " AND "
                field_type += f"length({field_name}) <= {size_info['original']['max_size']})"

            else:
                field_type += f" CHECK(length({field_name}) <= {size_info['size']})"

        if field.is_pk():
            field_type += ' PRIMARY KEY'

        return field_type

    def render_datatype_date_sql(self, field_name, field):
        return 'DATE'

    def render_datatype_float_sql(self, field_name, field, **kwargs):
        return 'REAL'

    def render_datatype_bytes_sql(self, field_name, field, **kwargs):
        return 'BLOB'

    def render_datatype_required_sql(self, field_name, field, **kwargs):
        return 'NOT NULL' if field.required else 'NULL'

    def render_datatype_ref_sql(self, field_name, field, **kwargs):
        ref_s = field.schema
        if field.required: # strong ref, it deletes on fk row removal
            format_str = 'REFERENCES {} ({}) ON UPDATE CASCADE ON DELETE CASCADE'

        else: # weak ref, it sets column to null on fk row removal
            format_str = 'REFERENCES {} ({}) ON UPDATE CASCADE ON DELETE SET NULL'

        ret = format_str.format(
            self._normalize_table_name(ref_s),
            self._normalize_name(ref_s.pk.name)
        )

        return ret

    def render(self, schema, query, **kwargs):
        """Render the query

        :param schema: Schema, the query schema
        :param query: Query, the query to render
        :param **kwargs: named arguments
            placeholders: boolean, True if place holders should remain
        :returns: string if placeholders is False, (string, list) if placeholders is True
        """
        sql, sql_args = self.get_SQL(schema, query)
        placeholders = kwargs.get("placeholders", kwargs.get("placeholder", False))

        if not placeholders:
            for sql_arg in sql_args:
                sa = String(sql_arg)
                if not sa.isnumeric():
                    sa = "'{}'".format(sa)
                sql = sql.replace(self.val_placeholder, sa, 1)

        return (sql, sql_args) if placeholders else sql

