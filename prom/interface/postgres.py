"""
http://pythonhosted.org/psycopg2/module.html

http://zetcode.com/db/postgresqlpythontutorial/
http://wiki.postgresql.org/wiki/Using_psycopg2_with_PostgreSQL
http://pythonhosted.org/psycopg2/
"""
import os

# third party
import psycopg2
import psycopg2.extras

# first party
from ..interface import Interface as BaseInterface

class Interface(BaseInterface):

    def _connect(self, connection_config):
        database = connection_config.database
        username = connection_config.username
        password = connection_config.password
        host = connection_config.host
        port = connection_config.port
        if not port: port = 5432

        # http://pythonhosted.org/psycopg2/module.html
        self.connection = psycopg2.connect(
            database=database,
            user=username,
            password=password,
            host=host,
            port=port,
            cursor_factory=psycopg2.extras.RealDictCursor
        )

    def _query(self, query_str, query_args=None, **query_options):
        """
        **query_options -- dict
            ignore_result -- boolean -- true to not attempt to fetch results
            fetchone -- boolean -- true to only fetch one result
        """
        ret = True
        # http://stackoverflow.com/questions/6739355/dictcursor-doesnt-seem-to-work-under-psycopg2
        cur = self.connection.cursor()
        ignore_result = query_options.get('ignore_result', False)
        one_result = query_options.get('fetchone', False)

        if not query_args:
            cur.execute(query_str)
        else:
            cur.execute(query_str, query_args)

        if not ignore_result:
            if one_result:
                ret = cur.fetchone()
            else:
                ret = cur.fetchall()

        return ret

    def _transaction_start(self):
        """
        http://initd.org/psycopg/docs/usage.html#transactions-control
        """
        pass

    def _transaction_stop(self):
        self.connection.commit()

    def _transaction_fail(self, e=None):
        self.connection.rollback()

    def _get_tables(self, table_name):
        query_str = 'SELECT tablename FROM pg_tables WHERE tableowner = %s'
        query_args = [self.connection_config.database]

        if table_name:
            query_str += ' AND tablename = %s'
            query_args.append(table_name)

        ret = self._query(query_str, query_args)
        # http://www.postgresql.org/message-id/CA+mi_8Y6UXtAmYKKBZAHBoY7F6giuT5WfE0wi3hR44XXYDsXzg@mail.gmail.com
        return [r['tablename'] for r in ret]

    def _set_table(self, schema):

        query_str = []
        query_str.append("CREATE TABLE {} (".format(schema.table))
        query_fields = []
        
        for field_name, field_options in schema.fields.iteritems():

            field_type = ""

            if field_options.get('primary_key', False):
                field_type = 'SERIAL PRIMARY KEY'

            else:
                if issubclass(field_options['type'], int):
                    field_type = 'INTEGER'
                    # TODO, decide on SMALLINT if size is set
                elif issubclass(field_options['type'], long):
                    field_type = 'BIGINT'
                elif issubclass(field_options['type'], (str, unicode)):
                    if 'size' in field_options:
                        field_type = 'CHAR({})'.format(field_options['size'])
                    elif 'max_size' in field_options:
                        field_type = 'VARCHAR({})'.format(field_options['size'])
                    else:
                        field_type = 'TEXT'

                elif issubclass(field_options['type'], bool):
                    field_type = 'BOOL'
                elif issubclass(field_options['type'], float):
                    field_type = 'REAL'
                else:
                    # http://pythonhosted.org/psycopg2/usage.html#adaptation-of-python-values-to-sql-types
                    raise ValueError('unknown python type: {}'.format(field_options['type'].__name__))

                if 'required' in field_options:
                    field_type += ' NOT NULL'

            query_fields.append('  {} {}'.format(field_name, field_type))

        query_str.append(",{}".format(os.linesep).join(query_fields))
        query_str.append(')')
        query_str = os.linesep.join(query_str)
        ret = self._query(query_str, ignore_result=True)

    def _delete_table(self, schema):
        query_str = 'DROP TABLE IF EXISTS {} CASCADE'.format(schema.table)
        ret = self._query(query_str, ignore_result=True)

    def _get_indexes(self, schema):
        ret = {}
        query_str = []
        query_str.append('SELECT')
        query_str.append('  tbl.relname AS table_name, i.relname AS index_name, a.attname AS field_name,')
        query_str.append('  ix.indkey AS index_order, a.attnum AS field_num')
        query_str.append('FROM')
        query_str.append('  pg_class tbl, pg_class i, pg_index ix, pg_attribute a')
        query_str.append('WHERE')
        query_str.append('  tbl.oid = ix.indrelid AND i.oid = ix.indexrelid AND a.attrelid = tbl.oid')
        query_str.append('  AND a.attnum = ANY(ix.indkey) AND tbl.relkind = %s AND tbl.relname = %s')
        query_str.append('ORDER BY')
        query_str.append('  tbl.relname, i.relname')
        query_str = os.linesep.join(query_str)

        indexes = self._query(query_str, ['r', schema.table])

        # massage the data into more readable {index_name: fields} format
        for idict in indexes:
            if idict['index_name'] not in ret:
                ret[idict['index_name']] = map(int, idict['index_order'].split(' '))

            i = ret[idict['index_name']].index(idict['field_num'])
            ret[idict['index_name']][i] = idict['field_name']

        return ret

    def _set_index(self, table_name, name, fields, unique=False):
        """
        NOTE -- we set the index name using <table_name>_<name> format since indexes have to have
        a globally unique name in postgres
        """
        query_str = 'CREATE {}INDEX {}_{} ON {} USING BTREE ({})'.format(
            'UNIQUE' if unique else '',
            table_name,
            name,
            table_name,
            ', '.join(fields)
        )

        return self._query(query_str, ignore_result=True)

    def _insert(self, schema, d):

        # get the primary key
        pk_name = schema.pk

        field_formats = []
        field_names = []
        query_vals = []
        for field_name, field_val in d.iteritems():
            field_names.append(field_name)
            field_formats.append('%s')
            query_vals.append(field_val)

        query_str = 'INSERT INTO {} ({}) VALUES ({}) RETURNING {}'.format(
            schema.table,
            ', '.join(field_names),
            ', '.join(field_formats),
            pk_name
        )

        ret = self._query(query_str, query_vals)
        return ret[0][pk_name]

    def _update(self, schema, query, d):

        where_query_str, where_query_args = self.get_SQL(schema, query, only_where_clause=True)
        pk_name = schema.pk

        query_str = 'UPDATE {} SET {} {}'
        query_args = []

        field_str = []
        for field_name, field_val in d.iteritems():
            field_str.append('{} = %s'.format(field_name))
            query_args.append(field_val)

        query_str = query_str.format(
            schema.table,
            ',{}'.format(os.linesep).join(field_str),
            where_query_str
        )
        query_args.extend(where_query_args)

        return self._query(query_str, query_args, ignore_result=True)

    def _get_one(self, schema, query):
        query_str, query_args = self.get_SQL(schema, query)
        return self._query(query_str, query_args, fetchone=True)

    def _get(self, schema, query):
        query_str, query_args = self.get_SQL(schema, query)
        return self._query(query_str, query_args)

    def _count(self, schema, query):
        query_str, query_args = self.get_SQL(schema, query, count_query=True)
        ret = self._query(query_str, query_args)
        if ret:
            ret = int(ret[0]['ct'])
        else:
            ret = 0

        return ret

    def _delete(self, schema, query):
        where_query_str, query_args = self.get_SQL(schema, query, only_where_clause=True)
        query_str = []
        query_str.append('DELETE FROM')
        query_str.append('  {}'.format(schema))
        query_str.append(where_query_str)
        query_str = os.linesep.join(query_str)
        ret = self._query(query_str, query_args, ignore_result=True)

    def handle_error(self, schema, e):
        ret = False
        if isinstance(e, psycopg2.ProgrammingError):
            if schema.table in e.message and "does not exist" in e.message:
                self.set_table(schema)
                ret = True

        return ret

    def get_SQL(self, schema, query, **sql_options):
        """
        convert the query instance into SQL

        this is the glue method that translates the generic Query() instance to the postgres
        specific SQL query, this is where the magic happens

        **sql_options -- dict
            count_query -- boolean -- true if this is a count query SELECT
            only_where_clause -- boolean -- true to only return after WHERE ...
        """
        only_where_clause = sql_options.get('only_where_clause', False)

        normalize_list = lambda symbol, field_name, args: '{} {} ({})'.format(field_name, symbol, ', '.join(['%s'] * len(args)))
        normalize_val = lambda symbol, field_name, arg: '{} {} %s'.format(field_name, symbol)

        symbol_map = {
            'in': {'args': normalize_list, 'symbol': 'IN'},
            'nin': {'args': normalize_list, 'symbol': 'NOT IN'},
            'is': {'arg': normalize_val, 'symbol': '='},
            'not': {'arg': normalize_val, 'symbol': '!='},
            'gt': {'arg': normalize_val, 'symbol': '>'},
            'gte': {'arg': normalize_val, 'symbol': '>='},
            'lt': {'arg': normalize_val, 'symbol': '<'},
            'lte': {'arg': normalize_val, 'symbol': '<='},
        }

        query_args = []
        query_str = []

        if not only_where_clause:
            query_str.append('SELECT')

            if sql_options.get('count_query', False):
                query_str.append('  count(*) as ct')
            else:
                select_fields = [select_field for select_field, _ in query.fields]
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

                sd = symbol_map[field[0]]
                if 'args' in sd:
                    query_str.append('  {}'.format(sd['args'](sd['symbol'], field[1], field[2])))
                    query_args.extend(field[2])
                elif 'arg' in sd:
                    query_str.append('  {}'.format(sd['arg'](sd['symbol'], field[1], field[2])))
                    query_args.append(field[2])

        if query.fields_sort:
            query_sort_str = []
            query_str.append('ORDER BY')
            for field in query.fields_sort:
                query_sort_str.append('  {} {}'.format(field[1], 'ASC' if field[0] > 0 else 'DESC'))

            query_str.append(',{}'.format(os.linesep).join(query_sort_str))

        if query.bounds:
            limit, offset, _ = query.get_bounds()
            if limit > 0:
                query_str.append('LIMIT {} OFFSET {}'.format(limit, offset))

        query_str = os.linesep.join(query_str)
        return query_str, query_args



