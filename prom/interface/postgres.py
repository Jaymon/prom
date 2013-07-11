"""
http://pythonhosted.org/psycopg2/module.html
"""

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

        self.connection = psycopg2.connect(
            database=database,
            user=username,
            password=password,
            host=host,
            port=port
        )

    def _query(self, query_str, *query_args, **query_options):
        cur = self.connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute(query_str, query_args)
        rows = cur.fetchall()
        return rows
