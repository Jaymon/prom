"""
http://pythonhosted.org/psycopg2/module.html
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
            port=port
        )

    def _query(self, query_str, query_args=None, **query_options):
        ret = True
        cur = self.connection.cursor(cursor_factory=psycopg2.extras.DictCursor)

        if not query_args:
            cur.execute(query_str)
        else:
            cur.execute(query_str, query_args)

        if query_options.get('get_result', True):
            ret = cur.fetchall()

        return ret

    def _get_tables(self, table_name):
        query_str = ''
        query_args = []

        if table_name:
            query_str = 'SELECT tablename FROM pg_tables WHERE tablename = %s'
            query_args.append(table_name)
        else:
            query_str = 'SELECT tablename FROM pg_tables'

        ret = self._query(query_str, query_args)
        if ret:
            ret = ret[0]

        return ret

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
        ret = self._query(query_str, get_result=False)
        self.connection.commit() # this has to be here otherwise table is only created for this session and doesn't persist

