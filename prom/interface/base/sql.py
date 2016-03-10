import os
import datetime
import textwrap

# first party
from ...exception import InterfaceError
#from .generic import Connection, Interface
from . import generic


class SQLConnection(generic.Connection):
    def _transaction_start(self):
        cur = self.cursor()
        cur.execute("BEGIN")

    def _transaction_started(self, name):
        cur = self.cursor()
        # http://www.postgresql.org/docs/9.2/static/sql-savepoint.html
        cur.execute("SAVEPOINT {}".format(name))

    def _transaction_stop(self):
        """
        http://initd.org/psycopg/docs/usage.html#transactions-control
        https://news.ycombinator.com/item?id=4269241
        """
        cur = self.cursor()
        cur.execute("COMMIT")

    def _transaction_fail(self):
        cur = self.cursor()
        cur.execute("ROLLBACK")

    def _transaction_failing(self, name):
        cur = self.cursor()
        # http://www.postgresql.org/docs/9.2/static/sql-rollback-to.html
        cur.execute("ROLLBACK TO SAVEPOINT {}".format(name))


class FieldClause(generic.FieldClause):
    placeholder = '?'

    def __init__(self, *args, **kwargs):
        super(FieldClause, self).__init__(*args, **kwargs)
        self.placeholder_vals = []

    def normalize_vals(self):
        return self.placeholder_vals

    def normalize_where(self, schema):
        format_str = ''
        format_args = []
        operator_maps = {
            'in': {'symbol': 'IN', 'list': True},
            'nin': {'symbol': 'NOT IN', 'list': True},
            'is': {'symbol': '=', 'none_symbol': 'IS'},
            'not': {'symbol': '!=', 'none_symbol': 'IS NOT'},
            'gt': {'symbol': '>'},
            'gte': {'symbol': '>='},
            'lt': {'symbol': '<'},
            'lte': {'symbol': '<='},
        }
        operator_map = operator_maps[self.operator]
        symbol = operator_map['symbol']
        is_list = operator_map.get('list', False)
        placeholder = self.placeholder

        if self.options:
            # options take precedence because None is a perfectly valid field_val
            f = schema.fields[self.name]
            if issubclass(f.type, (datetime.datetime, datetime.date)):
                format_strs = self.normalize_where_date()
                for fname, fvstr, farg in format_strs:
                    if format_str:
                        format_str += ' AND '

                    if is_list:
                        format_str += '{} {} ({})'.format(fname, symbol, ', '.join([fvstr] * len(farg)))
                        format_args.extend(farg)

                    else:
                        format_str += '{} {} {}'.format(fname, symbol, fvstr)
                        format_args.append(farg)

            else:
                raise ValueError('Field {} does not support extended values'.format(self.name))

        else:
            if is_list:
                format_str = '{} {} ({})'.format(self.name, symbol, ', '.join([placeholder] * len(self.val)))
                format_args.extend(self.val)

            else:
                # special handling for NULL
                if self.val is None:
                    symbol = operator_map['none_symbol']

                format_str = '{} {} {}'.format(self.name, symbol, placeholder)
                format_args.append(self.val)

        return format_str, format_args

    def normalize_where_date(self):
        raise NotImplementedError()

    def normalize_sort_val(self, sort_dir_str):
        raise NotImplementedError()

    def normalize_sort(self):
        s = ""
        args = []

        sort_dir_str = 'ASC' if self.options["direction"] > 0 else 'DESC'
        if self.val:
            s, args = self.normalize_sort_val(sort_dir_str)

        else:
            s = '{} {}'.format(self.name, sort_dir_str)

        return s, args

    def normalize(self):
        schema = self.interface_query.schema
        operator = self.operator

        if operator == "select":
            s = self.normalize_select()

        elif operator == "sort":
            s, a = self.normalize_sort()
            self.placeholder_vals = a

        else:
            s, a = self.normalize_where(schema)
            self.placeholder_vals = a

        return s


class SelectClause(generic.SelectClause):
    def normalize(self):
        query_str = []
        query_str.append('SELECT')
        is_count = getattr(self, "is_count", False)

        if is_count:
            query_str.append('  count(*) as ct')

        else:
            select_fields = self.fields
            if select_fields:
                query_str.append(",\n".join(
                    ("  {}".format(f.normalize()) for f in select_fields)
                ))

            else:
                query_str.append('  *')

        return "\n".join(query_str)


class WhereClause(generic.WhereClause):

    def normalize_vals(self):
        return self.placeholder_vals

    def normalize(self):
        if not self.fields: return ""

        query_str = []
        query_args = []

        query_str.append('WHERE')
        for i, field in enumerate(self.fields):
            if i > 0: query_str.append('AND')

            query_str.append('  {}'.format(field.normalize()))
            query_args.extend(field.placeholder_vals)

        self.placeholder_vals = query_args
        return "\n".join(query_str)


class SortClause(generic.SortClause):

    def normalize_vals(self):
        return self.placeholder_vals

    def normalize(self):
        if not self.fields: return ""

        query_str = []
        query_sort_str = []
        query_args = []

        query_str.append('ORDER BY')
        for field in self.fields:
            query_sort_str.append(field.normalize())
            query_args.extend(field.placeholder_vals)

        self.placeholder_vals = query_args
        query_str.append(",\n  ".join(query_sort_str))
        return "\n  ".join(query_str)


class LimitClause(generic.LimitClause):
    def normalize(self):
        if not self: return ""
        return "LIMIT {} OFFSET {}".format(self.limit, self.offset)


class SetClause(generic.FieldsClause):
    def normalize_update(self):
        query_str = 'SET {} {}'
        query_args = []

        field_str = []
        for field in self.fields:
            field_str.append('{} = {}'.format(field_name, self.val_placeholder))
            query_args.append(field_val)

        query_str = query_str.format(
            schema.table,
            ',{}'.format(os.linesep).join(field_str),
            where_query_str
        )
        query_args.extend(where_query_args)


class QueryClause(generic.QueryClause):

    placeholder = '?'

    field_class = FieldClause

    select_class = SelectClause

    where_class = WhereClause

    sort_class = SortClause

    limit_class = LimitClause

    set_class = SetClause


class SQLInterface(generic.Interface):
    """Generic base class for all SQL derived interfaces"""

    query_class = QueryClause

    @property
    def val_placeholder(self):
        raise NotImplementedError("this property should be set in any children class")

#     def get_field_SQL(self):
#         raise NotImplementedError()

    def _delete_tables(self, **kwargs):
        for table_name in self.get_tables(**kwargs):
            with self.transaction(**kwargs) as connection:
                kwargs['connection'] = connection
                self._delete_table(table_name, **kwargs)

        return True

    def _delete(self, schema, query, **kwargs):
        where_query_str, query_args = self.get_SQL(schema, query, only_where_clause=True)
        query_str = []
        query_str.append('DELETE FROM')
        query_str.append('  {}'.format(schema))
        query_str.append(where_query_str)
        query_str = os.linesep.join(query_str)
        ret = self.query(query_str, *query_args, count_result=True, **kwargs)
        return ret

    def _query(self, query_str, query_args=None, **query_options):
        """
        **query_options -- dict
            ignore_result -- boolean -- true to not attempt to fetch results
            fetchone -- boolean -- true to only fetch one result
            count_result -- boolean -- true to return the int count of rows affected
        """
        ret = True
        # http://stackoverflow.com/questions/6739355/dictcursor-doesnt-seem-to-work-under-psycopg2
        connection = query_options.get('connection', None)
        with self.connection(connection) as connection:
            cur = connection.cursor()
            ignore_result = query_options.get('ignore_result', False)
            count_result = query_options.get('count_result', False)
            one_result = query_options.get('fetchone', False)
            cur_result = query_options.get('cursor_result', False)

            try:
                if not query_args:
                    self.log(query_str)
                    cur.execute(query_str)

                else:
                    self.log("{}{}{}", query_str, os.linesep, query_args)
                    cur.execute(query_str, query_args)

                if cur_result:
                    ret = cur

                elif not ignore_result:
                        if one_result:
                            ret = cur.fetchone()
                        elif count_result:
                            ret = cur.rowcount
                        else:
                            ret = cur.fetchall()

            except Exception as e:
                self.log(e)
                raise

            return ret

    def _normalize_date_SQL(self, field_name, field_kwargs):
        raise NotImplemented()

    def _normalize_field_SQL(self, schema, field_name):
        return field_name, self.val_placeholder

    def _normalize_list_SQL(self, schema, symbol_map, field_name, field_vals, field_kwargs=None):

        format_str = ''
        format_args = []
        symbol = symbol_map['symbol']

        if field_kwargs:
            f = schema.fields[field_name]
            if issubclass(f.type, (datetime.datetime, datetime.date)):
                format_strs = self._normalize_date_SQL(field_name, field_kwargs)
                for fname, fvstr, fargs in format_strs:
                    if format_str:
                        format_str += ' AND '

                    format_str += '{} {} ({})'.format(fname, symbol, ', '.join([fvstr] * len(fargs)))
                    format_args.extend(fargs)

            else:
                raise ValueError('Field {} does not support extended kwarg values'.format(field_name))

        else:
            field_name, format_val_str = self._normalize_field_SQL(schema, field_name)
            format_str = '{} {} ({})'.format(field_name, symbol, ', '.join([format_val_str] * len(field_vals)))
            format_args.extend(field_vals)

        return format_str, format_args

    def _normalize_val_SQL(self, schema, symbol_map, field_name, field_val, field_kwargs=None):

        format_str = ''
        format_args = []

        if field_kwargs:
            symbol = symbol_map['symbol']
            # kwargs take precedence because None is a perfectly valid field_val
            f = schema.fields[field_name]
            if issubclass(f.type, (datetime.datetime, datetime.date)):
                format_strs = self._normalize_date_SQL(field_name, field_kwargs)
                for fname, fvstr, farg in format_strs:
                    if format_str:
                        format_str += ' AND '

                    format_str += '{} {} {}'.format(fname, symbol, fvstr)
                    format_args.append(farg)

            else:
                raise ValueError('Field {} does not support extended kwarg values'.format(field_name))

        else:
            # special handling for NULL
            symbol = symbol_map['none_symbol'] if field_val is None else symbol_map['symbol']
            field_name, format_val_str = self._normalize_field_SQL(schema, field_name)
            format_str = '{} {} {}'.format(field_name, symbol, format_val_str)
            format_args.append(field_val)

        return format_str, format_args

    def _normalize_sort_SQL(self, field_name, field_vals, sort_dir_str):
        """normalize the sort string

        return -- tuple -- field_sort_str, field_sort_args"""
        raise NotImplemented()

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
            'in': {'args': self._normalize_list_SQL, 'symbol': 'IN'},
            'nin': {'args': self._normalize_list_SQL, 'symbol': 'NOT IN'},
            'is': {'arg': self._normalize_val_SQL, 'symbol': '=', 'none_symbol': 'IS'},
            'not': {'arg': self._normalize_val_SQL, 'symbol': '!=', 'none_symbol': 'IS NOT'},
            'gt': {'arg': self._normalize_val_SQL, 'symbol': '>'},
            'gte': {'arg': self._normalize_val_SQL, 'symbol': '>='},
            'lt': {'arg': self._normalize_val_SQL, 'symbol': '<'},
            'lte': {'arg': self._normalize_val_SQL, 'symbol': '<='},
        }

        query_args = []
        query_str = []

        if not only_where_clause:
            query_str.append('SELECT')

            if sql_options.get('count_query', False):
                query_str.append('  count(*) as ct')
            else:
                select_fields = query.fields_select
                if select_fields:
                    query_str.append('  ' + ',{}'.format(os.linesep).join(select_fields))
                else:
                    query_str.append('  *')

            query_str.append('FROM')
            query_str.append('  {}'.format(schema))

        if query.fields_where:
            query_str.append('WHERE')

            for i, field in enumerate(query.fields_where):
                if i > 0: query_str.append('AND')

                field_str = ''
                field_args = []
                sd = symbol_map[field[0]]

                # field[0], field[1], field[2], field[3]
                _, field_name, field_val, field_kwargs = field

                if 'args' in sd:
                    field_str, field_args = sd['args'](schema, sd, field_name, field_val, field_kwargs)

                elif 'arg' in sd:
                    field_str, field_args = sd['arg'](schema, sd, field_name, field_val, field_kwargs)

                query_str.append('  {}'.format(field_str))
                query_args.extend(field_args)

        if query.fields_sort:
            query_sort_str = []
            query_str.append('ORDER BY')
            for field in query.fields_sort:
                sort_dir_str = 'ASC' if field[0] > 0 else 'DESC'
                if field[2]:
                    field_sort_str, field_sort_args = self._normalize_sort_SQL(field[1], field[2], sort_dir_str)
                    query_sort_str.append(field_sort_str)
                    query_args.extend(field_sort_args)

                else:
                    query_sort_str.append('  {} {}'.format(field[1], sort_dir_str))

            query_str.append(',{}'.format(os.linesep).join(query_sort_str))

        if query.bounds:
            limit, offset, _ = query.get_bounds()
            if limit > 0:
                query_str.append('LIMIT {} OFFSET {}'.format(limit, offset))

        query_str = os.linesep.join(query_str)
        return query_str, query_args

    def handle_error(self, schema, e, **kwargs):
        connection = kwargs.get('connection', None)
        if not connection: return False

        ret = False
        if connection.closed == 0: # connection is open
            #connection.transaction_stop()
            if isinstance(e, InterfaceError):
                ret = self._handle_error(schema, e.e, **kwargs)

            else:
                ret = self._handle_error(schema, e, **kwargs)

        else:
            # we are unsure of the state of everything since this connection has
            # closed, go ahead and close out this interface and allow this query
            # to fail, but subsequent queries should succeed
            self.close()
            ret = True

        return ret

    def _handle_error(self, schema, e, **kwargs): raise NotImplemented()

    def _set_all_tables(self, schema, **kwargs):
        """
        You can run into a problem when you are trying to set a table and it has a 
        foreign key to a table that doesn't exist, so this method will go through 
        all fk refs and make sure the tables exist
        """
        with self.transaction(**kwargs) as connection:
            kwargs['connection'] = connection
            # go through and make sure all foreign key referenced tables exist
            for field_name, field_val in schema.fields.items():
                s = field_val.schema
                if s:
                    self._set_all_tables(s, **kwargs)

            # now that we know all fk tables exist, create this table
            self.set_table(schema, **kwargs)

        return True

    def _update(self, schema, fields, query, **kwargs):
        where_query_str, where_query_args = self.get_SQL(schema, query, only_where_clause=True)
        query_str = 'UPDATE {} SET {} {}'
        query_args = []

        field_str = []
        for field_name, field_val in fields.items():
            field_str.append('{} = {}'.format(field_name, self.val_placeholder))
            query_args.append(field_val)

        query_str = query_str.format(
            schema.table,
            ',{}'.format(os.linesep).join(field_str),
            where_query_str
        )
        query_args.extend(where_query_args)

        return self.query(query_str, *query_args, count_result=True, **kwargs)

    def _get_one(self, schema, query, **kwargs):
        # compensate for getting one with an offset
        if query.has_bounds() and not query.has_limit():
            query.set_limit(1)
        query_str, query_args = self.get_SQL(schema, query)
        return self.query(query_str, *query_args, fetchone=True, **kwargs)

    def _get(self, schema, query, **kwargs):
        query_str, query_args = self.get_SQL(schema, query)
        return self.query(query_str, *query_args, **kwargs)

    def _count(self, schema, query, **kwargs):
        query_str, query_args = self.get_SQL(schema, query, count_query=True)
        ret = self.query(query_str, *query_args, **kwargs)
        if ret:
            ret = int(ret[0]['ct'])
        else:
            ret = 0

        return ret

    def _set_all_fields(self, schema, **kwargs):
        """
        this will add fields that don't exist in the table if they can be set to NULL,
        the reason they have to be NULL is adding fields to Postgres that can be NULL
        is really light, but if they have a default value, then it can be costly
        """
        current_fields = self._get_fields(schema, **kwargs)
        for field_name, field in schema.fields.items():
            if field_name not in current_fields:
                if field.required:
                    raise ValueError('Cannot safely add {} on the fly because it is required'.format(field_name))

                else:
                    query_str = []
                    query_str.append('ALTER TABLE')
                    query_str.append('  {}'.format(schema))
                    query_str.append('ADD COLUMN')
                    query_str.append('  {}'.format(self.get_field_SQL(field_name, field)))
                    query_str = os.linesep.join(query_str)
                    self.query(query_str, ignore_result=True, **kwargs)

        return True

