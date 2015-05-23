from __future__ import absolute_import

import psycogreen.gevent
from gevent.queue import Queue
import psycopg2
from psycopg2 import extensions, OperationalError, connect
from psycopg2.pool import AbstractConnectionPool, PoolError

from .interface import get_interfaces
from .interface.postgres import PostgreSQL

def patch_all(maxconn=10, **kwargs):
    psycogreen.gevent.patch_psycopg()

    kwargs.setdefault('pool_maxconn', maxconn)
    kwargs.setdefault('pool_class', 'prom.gevent.ConnectionPool')
    kwargs.setdefault('async', True)
    for name, interface in get_interfaces().items():
        if isinstance(interface, PostgreSQL):
            interface.close()
            interface.connection_config.options.update(kwargs)


class ConnectionPool(AbstractConnectionPool):
    """a gevent green thread safe connection pool
    this is based off an example found here
    https://github.com/surfly/gevent/blob/master/examples/psycopg2_pool.py
    """
    def __init__(self, minconn, maxconn, *args, **kwargs):
        self.size = 0
        self.minconn = minconn
        self.maxconn = maxconn
        self.closed = False

        self._args = args
        self._kwargs = kwargs
        self._pool = Queue()

        for i in range(self.minconn):
            connection = self._connect()
            self.putconn(connection)

    def _connect(self, key=None):
        self.size += 1
        try:
            # I think connect() causes a WAIT, so we need to increment size before
            # we call connect() in order to actually make it increment
            conn = psycopg2.connect(*self._args, **self._kwargs)

        except:
            self.size -= 1
            raise

        return conn

    def getconn(self, key=None):
        if self.closed: raise PoolError("connection pool is closed")
        pool = self._pool
        if self.size >= self.maxconn or pool.qsize():
            conn = pool.get()

        else:
            try:
                conn = self._connect()

            except:
                raise

        return conn

    def putconn(self, conn=None, key=None, close=False):
        if self.closed: raise PoolError("connection pool is closed")
        if close:
            conn.close()
        else:
            self._pool.put(conn)

    def closeall(self):
        if self.closed: raise PoolError("connection pool is closed")
        # TODO -- might be better to do this decrementing self.size?
        while not self._pool.empty():
            conn = self._pool.get_nowait()
            try:
                conn.close()
            except Exception:
                pass

        self.closed = True
