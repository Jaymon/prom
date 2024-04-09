# -*- coding: utf-8 -*-
import os
import datetime
import decimal
import logging
import uuid
from functools import cached_property
import inspect

from ..query import Query, QueryField
from ..exception import (
    TableError,
    FieldError,
    UniqueError,
    PlaceholderError,
)

from ..compat import *
from ..utils import make_list
from .base import Connection, Interface


logger = logging.getLogger(__name__)


class SQLConnection(Connection):
    """
    https://docs.python.org/3.9/library/sqlite3.html#sqlite3-controlling-transactions
    https://www.sqlite.org/lockingv3.html
    https://www.sqlite.org/lang_transaction.html
    """
    async def _transaction_start(self):
        await self.execute("BEGIN")

    async def _transaction_starting(self, tx):
        # http://www.postgresql.org/docs/9.2/static/sql-savepoint.html
        await self.execute("SAVEPOINT {}".format(
            self.interface.render_field_name_sql(tx["name"])
        ))

    async def _transaction_stop(self):
        """
        http://initd.org/psycopg/docs/usage.html#transactions-control
        https://news.ycombinator.com/item?id=4269241
        """
        await self.execute("COMMIT")

    async def _transaction_stopping(self, tx):
        await self.execute("RELEASE {}".format(
            self.interface.render_field_name_sql(tx["name"])
        ))

    async def _transaction_fail(self):
        await self.execute("ROLLBACK")

    async def _transaction_failing(self, tx):
        # http://www.postgresql.org/docs/9.2/static/sql-rollback-to.html
        await self.execute("ROLLBACK TO SAVEPOINT {}".format(
            self.interface.render_field_name_sql(tx["name"])
        ))


class SQLInterfaceABC(Interface):
    """SQL database interfaces should extend SQLInterface and implement all
    these methods in this class and all the methods in InterfaceABC"""
    @property
    def LIMIT_NONE(self):
        """When an offset is set but not a limit, this is the value that will be
        put into the LIMIT part of the query

        :returns: str|int|None
        """
        raise NotImplementedError(
            "this property should be set in a child class"
        )

    def get_paramstyle(self):
        """Returns the paramstyle that is used by self.PLACEHOLDER to decide
        what val placeholder to use when building queries. This would also be
        the value to use when calling .raw().

        The dbapi 2.0 spec requires this to be set

        https://peps.python.org/pep-0249/#paramstyle

        :returns: str, something like "qmark" (for ? placeholders) or "pyformat"
            (for %s placeholders)
        """
        raise NotImplemented()

    async def render_date_field_sql(self, field_name, field_kwargs, symbol):
        raise NotImplemented()

    async def render_sort_field_sql(self, field_name, field_vals, sort_dir_str):
        """normalize the sort string

        return -- tuple -- field_sort_str, field_sort_args"""
        raise NotImplemented()

    async def render_nolimit_sql(limit, **kwargs):
        raise NotImplemented()

    async def render_datatype_datetime_sql(self, field_name, field, **kwargs):
        raise NotImplementedError()

    async def render_datatype_dict_sql(self, field_name, field, **kwargs):
        raise NotImplementedError()

    async def render_datatype_uuid_sql(self, field_name, field, **kwargs):
        raise NotImplementedError()


class SQLInterface(SQLInterfaceABC):
    """Generic base class for all SQL derived interfaces"""
    @cached_property
    def PLACEHOLDER(self):
        """What placeholder value this interface uses when building queries.

        This uses .get_paramstyle() to decide what placeholder value to use

        https://www.psycopg.org/docs/usage.html#passing-parameters-to-sql-queries

        NOTE -- It looks like both SQLite and Postgres support "named" so if
        you want to use .raw() queries that will work in both interface I would
        use named parameters

        :returns: str, usually something like "?" or "%s"
        """
        paramstyle = self.get_paramstyle()

        if paramstyle == "qmark":
            return "?"

        elif paramstyle == "format":
            return "%s"

        elif paramstyle == "pyformat":
            return "%s"

        elif paramstyle == "numeric":
            raise NotImplementedError("These are :1 :2 :3")

        elif paramstyle == "named":
            raise NotImplementedError("These are :name")

        else:
            raise NotImplementedError(f"Unknown paramstyle {paramstyle}")

    async def _set_table(self, schema, **kwargs):
        """
        http://sqlite.org/lang_createtable.html
        http://www.postgresql.org/docs/9.1/static/sql-createtable.html
        http://www.postgresql.org/docs/8.1/static/datatype.html
        http://pythonhosted.org/psycopg2/usage.html#adaptation-of-python-values-to-sql-types
        """
        query_str = [
            "CREATE TABLE IF NOT EXISTS {} (".format(
                self.render_table_name_sql(schema)
            )
        ]

        query_fields = []
        for field_name, field in schema.fields.items():
            query_fields.append("  {}".format(
                self.render_datatype_sql(field_name, field)
            ))
        query_str.append(",\n".join(query_fields))

        query_str.append(")")
        query_str = "\n".join(query_str)
        await self._raw(query_str, ignore_result=True, **kwargs)

    def _set_index(self, schema, name, field_names, **kwargs):
        """
        NOTE -- we set the index name using <table_name>_<name> format since
        indexes have to have a globally unique name in postgres

        * https://www.sqlite.org/lang_createindex.html
        * https://www.postgresql.org/docs/14/sql-createindex.html - "IF NOT
            EXISTS support was added around 9.5
        """
        query_str = 'CREATE {}INDEX IF NOT EXISTS {} ON {} ({})'.format(
            'UNIQUE ' if kwargs.get('unique', False) else '',
            self.render_field_name_sql(f"{schema}_{name}"),
            self.render_table_name_sql(schema),
            ', '.join(map(self.render_field_name_sql, field_names))
        )

        return self._raw(query_str, ignore_result=True, **kwargs)

    async def _insert(self, schema, fields, **kwargs):
        query_str, query_args = self.render_insert_sql(
            schema,
            fields,
            **kwargs,
        )

        r = await self._raw(query_str, *query_args, **kwargs)
        return r[0] if r else {}

    async def _inserts(self, schema, field_names, field_values, **kwargs):
        """Do a mutli insert

        The query this will run is roughly:

            INSERT INTO
                (<field_names>)
            VALUES
                (tuple for tuple in <field_values>)

        :param schema: Schema, the table
        :param field_names: list[str], the field names
        :param field_values: Iterable[Iterable[Any]], each row in the iterable
            is a tuple of values that correspond to field_names
        :param **kwargs: passed through
        """
        query_str = self.render_inserts_sql(schema, field_names, **kwargs)
        await self._raw(
            query_str,
            *field_values,
            ignore_result=True,
            execute_many=True,
            **kwargs
        )

    async def _update(self, schema, fields, query, **kwargs):
        query_str, query_args = self.render_update_sql(
            schema,
            fields,
            query=query,
            **kwargs,
        )

        return await self._raw(
            query_str,
            *query_args,
            **kwargs
        )

    async def _upsert(
        self,
        schema,
        insert_fields,
        update_fields,
        conflict_field_names,
        **kwargs
    ):
        """
        https://www.sqlite.org/lang_UPSERT.html
        """
        if not conflict_field_names:
            raise ValueError(f"Upsert is missing conflict fields for {schema}")

        for field_name in conflict_field_names:
            # conflict fields need to be in the insert fields
            if field_name not in insert_fields:
                errmsg = "Upsert insert fields on {} missing conflict field {}"
                raise ValueError(
                    errmsg.format(
                        schema,
                        field_name,
                    )
                )

            # conflict fields should not be in the update fields (this is more
            # for safety, they should use .update if they want to change them)
            if field_name in update_fields:
                errmsg = "Upsert update fields on {} contain conflict field {}"
                raise ValueError(
                    errmsg.format(
                        schema,
                        field_name,
                    )
                )

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
            ', '.join(map(self.render_field_name_sql, conflict_field_names)),
            update_sql,
        )

        returning_field_names = schema.pk_names
        if returning_field_names:
            # https://www.sqlite.org/lang_returning.html
            query_str += ' RETURNING {}'.format(', '.join(
                map(self.render_field_name_sql, returning_field_names))
            )
            query_args = insert_args + update_args

        r = await self._raw(query_str, *query_args, **kwargs)
        if r and returning_field_names:
            if len(returning_field_names) > 1:
                r = r[0]
            else:
                r = r[0][returning_field_names[0]]
        return r

    async def _delete(self, schema, query, **kwargs):
        where_query_str, query_args = self.render_sql(
            schema,
            query,
            only_where_clause=True
        )
        query_str = []
        query_str.append('DELETE FROM')
        query_str.append('  {}'.format(self.render_table_name_sql(schema)))
        query_str.append(where_query_str)
        query_str = "\n".join(query_str)
        return await self._raw(
            query_str,
            *query_args,
            count_result=True,
            **kwargs
        )

    async def _raw(self, query_str, *query_args, **kwargs):
        """
        :param **kwargs:
            - ignore_result: bool, true to not attempt to fetch results
            - fetchone: bool, true to only fetch one result
            - count_result: bool, true to return the int count of rows affected
            - execute_many: bool, True to call cursor.executemany instead of
                cursor.execute
        """
        ret = True
        # http://stackoverflow.com/questions/6739355/dictcursor-doesnt-seem-to-work-under-psycopg2
        #connection = kwargs.get('connection', None)
        async with self.connection(**kwargs) as connection:
            cur = connection.cursor()
            # depending on the api, cursor() could either return a coroutine
            # or something different like a cursor class instance
            if inspect.isawaitable(cur):
                cur = await cur

            ignore_result = kwargs.get("ignore_result", False)
            count_result = kwargs.get("count_result", False)
            one_result = kwargs.get(
                "fetchone",
                kwargs.get("one_result", False)
            )
            cursor_result = kwargs.get("cursor_result", False)
            execute_many = kwargs.get(
                "execute_many",
                kwargs.get("executemany", False)
            )

            if cursor_result:
                # https://stackoverflow.com/a/125140
                cur.arraysize = kwargs.get("arraysize", 500)

            if query_args:
                self.log_for(
                    debug=(
                        ["{} - {}\n{}", connection, query_str, query_args],
                    ),
                    info=(["{} - {}", connection, query_str],)
                )

                execute_args = [query_str, query_args]

            else:
                self.log_info("{} - {}", connection, query_str)

                execute_args = [query_str]

            try:
                if execute_many:
                    # https://docs.python.org/3/library/sqlite3.html#sqlite3.Cursor.executemany
                    # https://www.psycopg.org/psycopg3/docs/api/cursors.html#psycopg.AsyncCursor.executemany
                    # https://www.psycopg.org/docs/cursor.html#cursor.executemany
                    await cur.executemany(*execute_args)

                else:
                    await cur.execute(*execute_args)

            except Exception as e:
                await self.raise_error(
                    e,
                    error_args=[
                        query_str,
                        query_args,
                        self.PLACEHOLDER,
                    ]
                )

            if cursor_result:
                ret = cur

            elif ignore_result:
                await cur.close()

            else:
                if one_result:
                    # https://www.psycopg.org/docs/cursor.html#cursor.fetchone
                    ret = await cur.fetchone()

                elif count_result:
                    # https://www.psycopg.org/docs/cursor.html#cursor.rowcount
                    ret = cur.rowcount

                else:
                    # https://www.psycopg.org/docs/cursor.html#cursor.fetchall
                    ret = await cur.fetchall()

                await cur.close()

            return ret

    async def _get(self, schema, query, **kwargs):
        """
        https://www.sqlite.org/lang_select.html
        """
        query_str, query_args = self.render_sql(schema, query)
        return await self._raw(query_str, *query_args, **kwargs)

    async def _count(self, schema, query, **kwargs):
        query_str, query_args = self.render_sql(
            schema,
            query,
            count_query=True
        )
        ret = await self._raw(query_str, *query_args, **kwargs)
        if ret:
            ret = int(ret[0]['ct'])
        else:
            ret = 0

        return ret

    async def _handle_field_error(self, schema, e, **kwargs):
        """This will add fields that don't exist in the table if they can be
        set to NULL, the reason they have to be NULL is adding fields to
        Postgres that can be NULL is really light, but if they have a default
        value, then it can be costly
        """
        current_fields = await self._get_fields(schema, **kwargs)
        for field_name, field in schema.fields.items():
            if field_name not in current_fields:
                if field.required:
                    self.log_error(
                        "Required field {} cannot be safely add on the fly",
                        field_name
                    )
                    return False

                else:
                    query_str = []
                    query_str.append('ALTER TABLE')
                    query_str.append('  {}'.format(
                        self.render_table_name_sql(schema)
                    ))
                    query_str.append('ADD COLUMN')
                    query_str.append('  {}'.format(
                        self.render_datatype_sql(field_name, field)
                    ))
                    query_str = "\n".join(query_str)
                    await self._raw(query_str, ignore_result=True, **kwargs)

        return True

    async def _handle_table_error(self, schema, e, **kwargs):
        """
        You can run into a problem when you are trying to set a table and it
        has a foreign key to a table that doesn't exist, so this method will go
        through all fk refs and make sure all the tables exist
        """
        if query := kwargs.pop("query", None):
            if schemas := query.schemas:
                for s in schemas:
                    self.log_warning(
                        f"Verifying {schema} query foreign key table: {s}"
                    )
                    if not await self._handle_table_error(s, e=e, **kwargs):
                        return False

        for field_name, field_val in schema.fields.items():
            s = field_val.schema
            if s:
                self.log_warning(f"Verifying {schema} foreign key table: {s}")
                if not await self._handle_table_error(schema=s, e=e, **kwargs):
                    return False

        # now that we know all fk tables exist, create this table
        # !!! This uses the external .set_table so it will run through all the 
        # indexes also
        await self.set_table(schema, **kwargs)
        return True

    def render_table_name_sql(self, schema):
        return self.render_field_name_sql(schema)

    def render_field_name_sql(self, name):
        """normalize a non value name for the query

        https://blog.christosoft.de/2012/10/sqlite-escaping-table-acolumn-names/

        :param name: str|QueryField|Schema, the name that should be prepared to
            be queried
        :returns: the modified name ready to be added to a query string
        """
        if isinstance(name, QueryField):
            if name.function_name:
                if name.alias:
                    return "{}(\"{}\") AS \"{}\"".format(
                        name.function_name,
                        name.name,
                        name.alias
                    )

                else:
                    return "{}(\"{}\")".format(
                        name.function_name,
                        name.name,
                    )

            else:
                return "\"{}\"".format(name.name)

        else:
            return "\"{}\"".format(name)

    def render_select_sql(self, schema, query, **kwargs):
        query_str = []
        query_args = []

        only_where_clause = kwargs.get('only_where_clause', False)
        if not only_where_clause:
            query_str.append('SELECT')
            is_count_query = kwargs.get('count_query', False)

            if query.compounds:
                select_fields_str = "*"

            else:
                select_fields = query.fields_select
                if select_fields:
                    distinct_fields = select_fields.options.get(
                        "distinct",
                        False
                    )
                    distinct = "DISTINCT " if distinct_fields else ""
                    select_fields_str = distinct + ", ".join(
                        (self.render_field_name_sql(f) for f in select_fields)
                    )

                else:
                    if is_count_query or select_fields.options.get("all", False):
                        select_fields_str = "*"

                    else:
                        select_fields_str = ", ".join(
                            (
                                self.render_field_name_sql(fname)
                                for fname in schema.fields.keys()
                            )
                        )

            if is_count_query:
                query_str.append('  count({}) as ct'.format(select_fields_str))

            else:
                query_str.append('  {}'.format(select_fields_str))

            query_str.append('FROM')

            if not query.compounds:
                query_str.append("  {}".format(
                    self.render_table_name_sql(schema)
                ))

        return query_str, query_args

    def render_compound_sql(self, schema, query, **kwargs):
        query_str = []
        query_args = []

        if query.compounds:
            query_str.append("(")
            for operator, queries in query.compounds:
                for i, query in enumerate(queries):
                    if i > 0:
                        query_str.append(operator.upper())

                    subquery_str, subquery_args = self.render_sql(
                        query.schema,
                        query,
                    )

                    if query.bounds:
                        query_str.append("  (")
                        query_str.append(subquery_str)
                        query_str.append("  )")

                    else:
                        query_str.append(subquery_str)

                    query_args.extend(subquery_args)

            query_str.append(") I")
        return query_str, query_args

    def render_where_field_raw_sql(self, schema, field):
        return field.name, field.value

    def render_where_field_sql(self, schema, field):
        format_str = ''
        format_args = []
        is_list = field.is_list
        field_name = field.name
        field_val = field.value

        symbol_map = {
            'in': {'symbol': 'IN', 'list': True},
            'nin': {'symbol': 'NOT IN', 'list': True},
            # Previously we used "IS" and "IS NOT" but psycopg3 changed behavior
            # https://stackoverflow.com/a/76396765
            # https://www.psycopg.org/psycopg3/docs/basic/from_pg2.html#you-cannot-use-is-s
            'eq': {'symbol': '=', 'none_symbol': 'IS NOT DISTINCT FROM'},
            'ne': {'symbol': '!=', 'none_symbol': 'IS DISTINCT FROM'},
            'gt': {'symbol': '>'},
            'gte': {'symbol': '>='},
            'lt': {'symbol': '<'},
            'lte': {'symbol': '<='},
            # https://www.tutorialspoint.com/postgresql/postgresql_like_clause.htm
            # https://www.tutorialspoint.com/sqlite/sqlite_like_clause.htm
            'like': {'symbol': 'LIKE'},
            'nlike': {'symbol': 'NOT LIKE'},
        }
        sd = symbol_map[field.operator]
        symbol = sd['symbol']

        field_kwargs = field.kwargs
        if field_kwargs:
            # kwargs take precedence because None is a perfectly valid field_val
            f = schema.fields[field_name]
            if issubclass(f.type, (datetime.datetime, datetime.date)):
                format_strs = self.render_date_field_sql(
                    field_name,
                    field_kwargs,
                    symbol
                )
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
                raise ValueError(
                    'Field {} does not support extended kwarg values'.format(
                        field_name
                    )
                )

        else:
            if is_list and not isinstance(field_val, Query):
                field_val = make_list(field_val) if field_val else []
                field_name = self.render_field_name_sql(field_name)
                format_val_str = self.PLACEHOLDER

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
                    # automatically cause the query to fail but not necessarily 
                    # be an error, the best example is the IN (...) queries, if
                    # you do self.in_foo([]).get() that will fail because the
                    # list was empty, but a value error shouldn't be raised
                    # because a common case is:
                    #   self.if_foo(Bar.query.is_che(True).pks).get()
                    # which should result in an empty set if there are no rows
                    # where che = TRUE
                    #
                    # https://stackoverflow.com/a/58078468/5006
                    if symbol == "IN":
                        format_str = '{} <> {}'.format(field_name, field_name)

                    elif symbol == "NOT IN":
                        format_str = '{} = {}'.format(field_name, field_name)

                    else:
                        raise ValueError("Unsure what to do here")

            else:
                field_name = self.render_field_name_sql(field_name)
                format_val_str = self.PLACEHOLDER

                # special handling for NULL
                if field_val is None:
                    symbol = sd['none_symbol']

                if isinstance(field_val, Query):
                    subquery_schema = field_val.schema
                    if not subquery_schema:
                        raise ValueError(
                            f"{field_name} subquery has no schema"
                        )

                    subquery_sql, subquery_args = self.render_sql(
                        subquery_schema,
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
                        format_val_str,
                    )
                    format_args.append(field_val)

        return format_str, format_args

    def render_where_sql(self, schema, query, **kwargs):
        query_str = []
        query_args = []

        if query.fields_where:
            query_str.append('WHERE')
            or_clause = False

            for i, field in enumerate(query.fields_where):
                if i > 0:
                    query_str.append('OR' if or_clause else 'AND')

                field_str = ''
                field_args = []

                if field.raw:
                    field_str, field_args = self.render_where_field_raw_sql(
                        schema,
                        field,
                    )

                else:
                    field_str, field_args = self.render_where_field_sql(
                        schema,
                        field,
                    )

                if field.or_clause:
                    if not or_clause:
                        query_str.append("(")
                        or_clause = True

                query_str.append('  {}'.format(field_str))
                query_args.extend(field_args)

                if or_clause:
                    if not field.or_clause:
                        query_str.append(")")
                        or_clause = False

        return query_str, query_args

    def render_sort_sql(self, schema, query, **kwargs):
        query_str = []
        query_args = []

        if query.fields_sort:
            query_str.append('ORDER BY')

            query_sort_str = []
            for field in query.fields_sort:
                sort_dir_str = 'ASC' if field.direction > 0 else 'DESC'
                if field.value:
                    field_sort_str, field_sort_args = self.render_sort_field_sql(
                        field.name,
                        field.value,
                        sort_dir_str
                    )
                    query_sort_str.append(field_sort_str)
                    query_args.extend(field_sort_args)

                else:
                    query_sort_str.append('  {} {}'.format(
                        field.name,
                        sort_dir_str
                    ))

            query_str.append(',\n'.join(query_sort_str))

        return query_str, query_args

    def render_bounds_sql(self, schema, query, **kwargs):
        """
        https://www.postgresql.org/docs/current/queries-limit.html
        https://www.sqlite.org/lang_select.html#the_limit_clause
        """
        query_str = []
        query_args = []

        bounds = query.bounds
        fetchone = kwargs.get("fetchone", False)
        if bounds or fetchone:
            if fetchone:
                limit = 1
                offset = bounds.offset

            else:
                if bounds.has_limit():
                    limit, offset = bounds.get()

                else:
                    limit = self.LIMIT_NONE
                    offset = bounds.offset

            query_str.append(f'LIMIT {limit} OFFSET {offset}')

        return query_str, query_args

    def render_sql(self, schema, query, **kwargs):
        """
        convert the query instance into SQL

        this is the glue method that translates the generic Query() instance to
        the SQL specific query, this is where the magic happens

        https://www.sqlite.org/lang_select.html

        :param **kwargs:
            - count_query: bool, True if this is a count query SELECT
            - only_where_clause, bool, True to only return after WHERE ...
        :returns: tuple[str, list[Any]], (query_str, query_args)
        """
        query_str = []
        query_args = []

        select_str, select_args = self.render_select_sql(
            schema,
            query,
            **kwargs
        )
        query_str.extend(select_str)
        query_args.extend(select_args)

        compound_str, compound_args = self.render_compound_sql(
            schema,
            query,
            **kwargs
        )
        query_str.extend(compound_str)
        query_args.extend(compound_args)

        where_str, where_args = self.render_where_sql(
            schema,
            query,
            **kwargs
        )
        query_str.extend(where_str)
        query_args.extend(where_args)

        sort_str, sort_args = self.render_sort_sql(
            schema,
            query,
            **kwargs
        )
        query_str.extend(sort_str)
        query_args.extend(sort_args)

        limit_str, limit_args = self.render_bounds_sql(
            schema,
            query,
            **kwargs
        )
        query_str.extend(limit_str)
        query_args.extend(limit_args)

        query_str = "\n".join(query_str)
        return query_str, query_args

    def render_subquery_sql(self, subquery):
        subquery_schema = subquery.schema
        if not subquery_schema:
            raise ValueError(f"Subquery has no schema")

        return self.render_sql(subquery_schema, subquery)

    def render_set_value_sql(self, field_val):
        if isinstance(field_val, QueryField):
            if field_val.increment:
                if field_val.value:
                    query_str, query_vals = self.render_set_value_sql(
                        field_val.value
                    )

                    # We use COALESCE here to make sure there is a value to
                    # start with if there isn't a starting value
                    # Given a list of values, the COALESCE function returns
                    # the first non-null value
                    # https://www.postgresql.org/docs/current/functions-conditional.html#FUNCTIONS-COALESCE-NVL-IFNULL
                    query_str = "COALESCE({}, 0) + {}".format(
                        query_str,
                        self.PLACEHOLDER
                    )
                    query_vals.append(field_val.increment)

                else:
                    query_str = "{} + {}".format(
                        self.render_field_name_sql(field_val.name),
                        self.PLACEHOLDER
                    )
                    query_vals = [field_val.increment]

            else:
                query_str, query_vals = self.render_set_value_sql(
                    field_val.value
                )

        elif isinstance(field_val, Query):
            subquery_sql, query_vals = self.render_subquery_sql(field_val)
            query_str = "({})".format(subquery_sql)

        else:
            query_str = self.PLACEHOLDER
            query_vals = [field_val]

        return query_str, query_vals

    def render_insert_sql(self, schema, fields, **kwargs):
        """
        https://www.sqlite.org/lang_insert.html
        """
        field_names = []
        field_values = []
        query_vals = []

        ignore_result = kwargs.get("ignore_result", False)
        ignore_return_clause = kwargs.get(
            "ignore_return_clause",
            ignore_result
        )

        for field_name, field_val in fields.items():
            field_names.append(self.render_field_name_sql(field_name))

            field_value, field_query_vals = self.render_set_value_sql(
                field_val
            )
            field_values.append(field_value)
            query_vals.extend(field_query_vals)

        query_str = "INSERT INTO {} ({}) VALUES ({})".format(
            self.render_table_name_sql(schema),
            ", ".join(field_names),
            ", ".join(field_values),
        )

        if not ignore_return_clause:
            # https://www.sqlite.org/lang_returning.html
            if pk_name := schema.pk_name:
                field_names.append(self.render_field_name_sql(pk_name))

            query_str += " RETURNING {}".format(
                ", ".join(field_names)
            )

        return query_str, query_vals

    def render_inserts_sql(self, schema, field_names, **kwargs):
        """
        https://www.sqlite.org/lang_insert.html
        """
        sql_names = []
        sql_formats = []
        for field_name in field_names:
            sql_names.append(self.render_field_name_sql(field_name))
            sql_formats.append(self.PLACEHOLDER)

        return "INSERT INTO {} ({}) VALUES ({})".format(
            self.render_table_name_sql(schema),
            ", ".join(sql_names),
            ", ".join(sql_formats),
        )

    def render_update_sql(self, schema, fields, query, **kwargs):
        """
        https://www.sqlite.org/lang_update.html
        https://www.postgresql.org/docs/current/sql-update.html
        """
        query_str = ''
        query_args = []
        returning_names = []

        count_result = kwargs.get("count_result", False)
        ignore_result = kwargs.get("ignore_result", False)
        only_set_clause = kwargs.get("only_set_clause", False)
        ignore_return_clause = kwargs.get(
            "ignore_return_clause",
            only_set_clause or ignore_result or count_result
        )

        if not only_set_clause:
            query_str = "UPDATE {} ".format(self.render_table_name_sql(schema))

        field_str = []
        for field_name, field_val in fields.items():
            field_name_query_str = self.render_field_name_sql(field_name)
            field_query_str, field_query_vals = self.render_set_value_sql(
                field_val
            )
            field_str.append("{} = {}".format(
                field_name_query_str,
                field_query_str,
            ))
            query_args.extend(field_query_vals)
            returning_names.append(field_name_query_str)

        query_str += "SET {}".format(",\n".join(field_str))

        if query:
            where_query_str, where_query_args = self.render_sql(
                schema,
                query,
                only_where_clause=True
            )
            query_str += " {}".format(where_query_str)
            query_args.extend(where_query_args)

        if not ignore_return_clause:
            if returning_names:
                query_str += " RETURNING {}".format(
                    ",\n".join(returning_names)
                )

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

        if field.is_pk():
            field_type += ' PRIMARY KEY'

        else:
            if field.is_ref():
                field_type += ' ' + self.render_datatype_ref_sql(
                    field_name,
                    field
                )

        return '{} {}'.format(
            self.render_field_name_sql(field_name),
            field_type
        )

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
            self.render_table_name_sql(ref_s),
            self.render_field_name_sql(ref_s.pk.name)
        )

        return ret

    def render(self, schema, query, **kwargs):
        """Render the query

        :param schema: Schema, the query schema
        :param query: Query, the query to render
        :param **kwargs: named arguments
            placeholders: boolean, True if place holders should remain
        :returns: str if placeholders is False, tuple[str, list] if
            placeholders is True
        """
        sql, sql_args = self.render_sql(schema, query)
        placeholders = kwargs.get(
            "placeholders",
            kwargs.get("placeholder", False)
        )

        if not placeholders:
            for sql_arg in sql_args:
                if sql_arg is None:
                    sa = 'NULL'

                else:
                    sa = String(sql_arg)
                    if not sa.isnumeric():
                        sa = "'{}'".format(sa)

                sql = sql.replace(self.PLACEHOLDER, sa, 1)

        return (sql, sql_args) if placeholders else sql

