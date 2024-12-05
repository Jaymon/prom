# -*- coding: utf-8 -*-
from __future__ import unicode_literals, division, print_function, absolute_import
import logging

import psycogreen.gevent
import gevent.monkey
from gevent.queue import Queue
from gevent.lock import Semaphore
import psycopg2
from psycopg2 import extensions, OperationalError, connect
from psycopg2.pool import AbstractConnectionPool, PoolError

from .. import get_interfaces
from . import PostgreSQL


logger = logging.getLogger(__name__)


def patch_all(maxconn=10, **kwargs):
    if not gevent.monkey.saved:
        # https://github.com/gevent/gevent/blob/master/src/gevent/monkey.py
        logger.warning("Running gevent.monkey.patch_all() since not run previously")
        gevent.monkey.patch_all()

    psycogreen.gevent.patch_psycopg()

    kwargs.setdefault('pool_maxconn', maxconn)
    kwargs.setdefault('pool_class', '{}.ConnectionPool'.format(__name__))
    kwargs.setdefault('async', True)
    for name, interface in get_interfaces().items():
        if isinstance(interface, PostgreSQL):
            interface.close()
            interface.connection_config.options.update(kwargs)


class ConnectionPool(AbstractConnectionPool):
    """a gevent green thread safe connection pool
    this is based off an example found here
    https://github.com/surfly/gevent/blob/master/examples/psycopg2_pool.py

    https://github.com/psycopg/psycopg2/blob/master/lib/pool.py
    """
    def __init__(self, minconn, maxconn, *args, **kwargs):
        self.size = 0
        self.minconn = minconn
        self.maxconn = maxconn
        self.closed = False

        self._args = args
        self._kwargs = kwargs
        self._pool = Queue()
        self._used = {} # required for interface compatibility
        self._locks = {}

        for i in range(self.minconn):
            connection = self._connect()
            self.putconn(connection)

    def _connect(self, key=None):
        self.size += 1
        try:
            conn = psycopg2.connect(*self._args, **self._kwargs)
            logger.info("Connection created. Current pool size: %d", self.size)
        except:
            self.size -= 1
            raise

        self._locks[conn] = Semaphore()  # Create a lock for the new connection
        return conn

    def getconn(self, key=None, timeout=10):
        if self.closed:
            raise PoolError("connection pool is closed")

        pool = self._pool
        if not pool.empty():
            conn = pool.get()
            try:
                if conn.closed or conn.status != psycopg2.extensions.STATUS_READY:
                    conn.close()
                    self.size -= 1
                    conn = self._connect()
            except:
                conn = self._connect()
        else:
            conn = self._connect()

        # Acquire lock before returning the connection
        if conn not in self._locks:
            self._locks[conn] = Semaphore()

        lock = self._locks[conn]
        if not lock.acquire(blocking=False):  # Verifica se está bloqueado
            self.putconn(conn)
            conn = self._connect()  # Always open a new connection if busy

        return conn

    def putconn(self, conn=None, key=None, close=False):
        if self.closed:
            raise PoolError("connection pool is closed")

        lock = self._locks.get(conn)
        if lock:
            lock.release()  # Release the lock when the connection is returned

        if close or conn.closed or conn.status != psycopg2.extensions.STATUS_READY:
            conn.close()
            self.size -= 1
        else:
            self._pool.put(conn)

    def closeall(self):
        if self.closed:
            raise PoolError("connection pool is closed")
        self.closed = True

        while not self._pool.empty():
            conn = self._pool.get_nowait()
            try:
                conn.close()
            except:
                pass
        self.size = 0

        # Clear all locks
        self._locks.clear()
