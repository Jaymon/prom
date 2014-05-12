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
from .base import SQLInterface


class PostgreSQL(SQLInterface):

    val_placeholder = '%s'

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

    def _normalize_field_SQL(self, schema, field_name):
        format_field_name = field_name
        format_val_str = self.val_placeholder

        # postgres specific for getting around case sensitivity:
        if schema.fields[field_name].get('ignore_case', False):
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
            query_sort_str.append('  {} = {} {}'.format(field_name, self.val_placeholder, sort_dir_str))
            query_args.append(v)

        return ',\n'.join(query_sort_str), query_args

    def _normalize_date_SQL(self, field_name, field_kwargs):
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

        for k, v in field_kwargs.iteritems():
            fstrs.append([k_opts[k].format(field_name), self.val_placeholder, v])

        return fstrs

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

    def _handle_error(self, schema, e):
        ret = False
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

        return ret

