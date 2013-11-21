"""
http://pythonhosted.org/psycopg2/module.html

http://zetcode.com/db/postgresqlpythontutorial/
http://wiki.postgresql.org/wiki/Using_psycopg2_with_PostgreSQL
http://pythonhosted.org/psycopg2/
"""
import os
import types
import decimal
import datetime

# third party
import psycopg2
import psycopg2.extras
import psycopg2.extensions

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
        # http://initd.org/psycopg/docs/connection.html#connection.autocommit
        self.connection.autocommit = True
        # unicode harden for python 2
        # http://initd.org/psycopg/docs/usage.html#unicode-handling
        psycopg2.extensions.register_type(psycopg2.extensions.UNICODE, self.connection)
        psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY, self.connection)

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

        try:
            if not query_args:
                self.log(query_str)
                cur.execute(query_str)
            else:
                self.log("{}{}{}", query_str, os.linesep, query_args)
                cur.execute(query_str, query_args)

            if not ignore_result:
                if one_result:
                    ret = cur.fetchone()
                else:
                    ret = cur.fetchall()
        except Exception, e:
            self.log(e)
            raise

        return ret

    def _transaction_start(self, count):
        if count == 1:
            self._query("BEGIN", ignore_result=True)
        else:
            # http://www.postgresql.org/docs/9.2/static/sql-savepoint.html
            self._query("SAVEPOINT prom", ignore_result=True)

    def _transaction_stop(self, count):
        """
        http://initd.org/psycopg/docs/usage.html#transactions-control
        https://news.ycombinator.com/item?id=4269241
        """
        if count == 1:
            #self.connection.commit()
            self._query("COMMIT", ignore_result=True)

    def _transaction_fail(self, count, e=None):
        if count == 1:
            #self.connection.rollback()
            self._query("ROLLBACK", ignore_result=True)
        else:
            # http://www.postgresql.org/docs/9.2/static/sql-rollback-to.html
            self._query("ROLLBACK TO SAVEPOINT prom", ignore_result=True)

    def _get_tables(self, table_name):
        query_str = 'SELECT tablename FROM pg_tables WHERE tableowner = %s'
        query_args = [self.connection_config.username]

        if table_name:
            query_str += ' AND tablename = %s'
            query_args.append(str(table_name))

        ret = self._query(query_str, query_args)
        # http://www.postgresql.org/message-id/CA+mi_8Y6UXtAmYKKBZAHBoY7F6giuT5WfE0wi3hR44XXYDsXzg@mail.gmail.com
        return [r['tablename'] for r in ret]

    def _set_table(self, schema):
        """
        http://www.postgresql.org/docs/9.1/static/sql-createtable.html
        http://www.postgresql.org/docs/8.1/static/datatype.html
        http://pythonhosted.org/psycopg2/usage.html#adaptation-of-python-values-to-sql-types
        """
        query_str = []
        query_str.append("CREATE TABLE {} (".format(schema.table))

        query_fields = []
        for field_name, field_options in schema.fields.iteritems():
            query_fields.append('  {}'.format(self.get_field_SQL(field_name, field_options)))

        query_str.append(",{}".format(os.linesep).join(query_fields))
        query_str.append(')')
        query_str = os.linesep.join(query_str)
        ret = self._query(query_str, ignore_result=True)

    def _delete_table(self, schema):
        query_str = 'DROP TABLE IF EXISTS {} CASCADE'.format(str(schema))
        ret = self._query(query_str, ignore_result=True)

    def _delete_tables(self, **kwargs):
        """
        http://stackoverflow.com/questions/3327312/drop-all-tables-in-postgresql
        """

        # get all the tables owned by the connection owner
        for table_name in self.get_tables():
            self.transaction_start()
            self._delete_table(table_name)
            self.transaction_stop()

        return True
        #return self._query("DROP SCHEMA public CASCADE")

    def _get_fields(self, schema):
        """return all the fields for the given schema"""
        ret = []
        query_str = []
        query_str.append('SELECT')
        query_str.append('  attname')
        query_str.append('FROM')
        query_str.append('  pg_class, pg_attribute')
        query_str.append('WHERE')
        query_str.append('  pg_class.relname = %s')
        query_str.append('  AND pg_class.oid = pg_attribute.attrelid')
        query_str.append('  AND pg_attribute.attnum > 0')
        #query_str.append('ORDER BY')
        #query_str.append('  attname')
        query_str = os.linesep.join(query_str)
        fields = self._query(query_str, [schema.table])
        return set((d['attname'] for d in fields))

    def _get_indexes(self, schema):
        """return all the indexes for the given schema"""
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

    def _set_index(self, schema, name, fields, **index_options):
        """
        NOTE -- we set the index name using <table_name>_<name> format since indexes have to have
        a globally unique name in postgres

        http://www.postgresql.org/docs/9.1/static/sql-createindex.html
        """
        index_fields = []
        for field_name in fields:
            field_options = schema.fields[field_name]
            if issubclass(field_options['type'], types.StringTypes):
                if field_options.get('ignore_case', False):
                    field_name = 'UPPER({})'.format(field_name)
            index_fields.append(field_name)

        query_str = 'CREATE {}INDEX {}_{} ON {} USING BTREE ({})'.format(
            'UNIQUE ' if index_options.get('unique', False) else '',
            schema,
            name,
            schema,
            ', '.join(index_fields)
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
        if not self.connection: return False

        ret = False
        # http://initd.org/psycopg/docs/connection.html#connection.closed
        if self.connection.closed == 0:
            self.transaction_stop()
            #self.connection.rollback() # the last query failed, so let's rollback

            # http://initd.org/psycopg/docs/module.html#psycopg2.ProgrammingError
            if isinstance(e, psycopg2.ProgrammingError):
                e_msg = str(e)
                if schema.table in e_msg and "does not exist" in e_msg:
                    # psycopg2.ProgrammingError - column "name" of relation "table_name" does not exist
                    if "column" in e_msg:
                        try:
                            ret = self._set_all_fields(schema)
                        except ValueError, e:
                            ret = False

                    else:
                        ret = self._set_all_tables(schema)

        else:
            self.close()
            ret = self.connect()

        return ret

    def _set_all_fields(self, schema):
        """
        this will add fields that don't exist in the table if they can be set to NULL,
        the reason they have to be NULL is adding fields to Postgres that can be NULL
        is really light, but if they have a default value, then it can be costly
        """
        current_fields = self._get_fields(schema)
        for field_name, field_options in schema.fields.iteritems():
            if field_name not in current_fields:
                if field_options.get('required', False):
                    raise ValueError('Cannot safely add {} on the fly because it is required'.format(field_name))

                else:
                    query_str = []
                    query_str.append('ALTER TABLE')
                    query_str.append('  {}'.format(schema))
                    query_str.append('ADD COLUMN')
                    query_str.append('  {}'.format(self.get_field_SQL(field_name, field_options)))
                    query_str = os.linesep.join(query_str)
                    self._query(query_str, [], ignore_result=True)

        return True

    def _set_all_tables(self, schema):
        """
        You can run into a problem when you are trying to set a table and it has a 
        foreign key to a table that doesn't exist, so this method will go through 
        all fk refs and make sure the tables exist
        """
        self.transaction_start()
        # go through and make sure all foreign key referenced tables exist
        for field_name, field_val in schema.fields.iteritems():
            for fn in ['ref', 'weak_ref']:
                if fn in field_val:
                    self._set_all_tables(field_val[fn])

        # now that we know all fk tables exist, create this table
        self.set_table(schema)
        self.transaction_stop()
        return True

    def _normalize_list_SQL(self, schema, symbol, field_name, field_vals):

        format_str = '%s'

        # postgres specific for getting around case sensitivity:
        if schema.fields[field_name].get('ignore_case', False):
            field_name = 'UPPER({})'.format(field_name)
            format_str = 'UPPER(%s)'

        return '{} {} ({})'.format(field_name, symbol, ', '.join([format_str] * len(field_vals)))

    def _normalize_val_SQL(self, schema, symbol, field_name, field_val):

        format_str = '%s'

        # postgres specific for getting around case sensitivity:
        if schema.fields[field_name].get('ignore_case', False):
            field_name = 'UPPER({})'.format(field_name)
            format_str = 'UPPER(%s)'

        return '{} {} {}'.format(field_name, symbol, format_str)

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

        #normalize_list = lambda symbol, field_name, args: '{} {} ({})'.format(field_name, symbol, ', '.join(['%s'] * len(args)))
        #normalize_val = lambda symbol, field_name, arg: '{} {} %s'.format(field_name, symbol)

        symbol_map = {
            'in': {'args': self._normalize_list_SQL, 'symbol': 'IN'},
            'nin': {'args': self._normalize_list_SQL, 'symbol': 'NOT IN'},
            'is': {'arg': self._normalize_val_SQL, 'symbol': '='},
            'not': {'arg': self._normalize_val_SQL, 'symbol': '!='},
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

                sd = symbol_map[field[0]]
                if 'args' in sd:
                    query_str.append('  {}'.format(sd['args'](schema, sd['symbol'], field[1], field[2])))
                    query_args.extend(field[2])
                elif 'arg' in sd:
                    query_str.append('  {}'.format(sd['arg'](schema, sd['symbol'], field[1], field[2])))
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


    def get_field_SQL(self, field_name, field_options):
        """
        returns the SQL for a given field with full type information

        field_name -- string -- the field's name
        field_options -- dict -- the set options for the field

        return -- string -- the field type (eg, foo BOOL NOT NULL)
        """
        field_type = ""

        if field_options.get('pk', False):
            field_type = 'BIGSERIAL PRIMARY KEY'

        else:
            if issubclass(field_options['type'], bool):
                field_type = 'BOOL'

            elif issubclass(field_options['type'], int):
                size = 2147483647
                if 'size' in field_options:
                    size = field_options['size']
                elif 'max_size' in field_options:
                    size = field_options['max_size']

                if size < 32767:
                    field_type = 'SMALLINT'
                else:
                    if 'ref' in field_options or 'weak_ref' in field_options:
                        field_type = 'BIGINT'
                    else:
                        field_type = 'INTEGER'

            elif issubclass(field_options['type'], long):
                field_type = 'BIGINT'

            elif issubclass(field_options['type'], types.StringTypes):
                if 'size' in field_options:
                    field_type = 'CHAR({})'.format(field_options['size'])
                elif 'max_size' in field_options:
                    field_type = 'VARCHAR({})'.format(field_options['max_size'])
                else:
                    field_type = 'TEXT'

            elif issubclass(field_options['type'], datetime.datetime):
                # http://www.postgresql.org/docs/9.0/interactive/datatype-datetime.html
                field_type = 'TIMESTAMP WITHOUT TIME ZONE'

            elif issubclass(field_options['type'], datetime.date):
                field_type = 'DATE'

            elif issubclass(field_options['type'], float):
                field_type = 'REAL'
                size = field_options.get('size', field_options.get('max_size', 0))
                if size > 6:
                    field_type = 'DOUBLE PRECISION'

            elif issubclass(field_options['type'], decimal.Decimal):
                field_type = 'NUMERIC'

            else:
                raise ValueError('unknown python type: {}'.format(field_options['type'].__name__))

            if field_options.get('required', False):
                field_type += ' NOT NULL'
            else:
                field_type += ' NULL'

            if 'ref' in field_options: # strong ref, it deletes on fk row removal
                ref_s = field_options['ref']
                field_type += ' REFERENCES {} ({}) ON UPDATE CASCADE ON DELETE CASCADE'.format(ref_s.table, ref_s.pk)

            elif 'weak_ref' in field_options: # weak ref, it sets column to null on fk row removal
                ref_s = field_options['weak_ref']
                field_type += ' REFERENCES {} ({}) ON UPDATE CASCADE ON DELETE SET NULL'.format(ref_s.table, ref_s.pk)

        return '{} {}'.format(field_name, field_type)

