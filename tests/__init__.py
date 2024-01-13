# -*- coding: utf-8 -*-
import logging

from prom.compat import *
from . import testdata
from .testdata import TestCase, SkipTest


testdata.basic_logging(
    levels={
        "prom": "DEBUG",
        #"prom": "ERROR",
        #"prom": "INFO",
        "datatypes": "WARNING",
        "dsnparse": "WARNING",
        "psycopg": "WARNING",
        "aiosqlite": "WARNING",
        "asyncio": "WARNING",
    },
#     format="|".join(['[%(levelname).1s',
#         '%(asctime)s',
#         '%(process)d.%(thread)d',
#         '%(name)s',
#         '%(filename)s:%(lineno)s] %(message)s',
#     ])
)


logger = logging.getLogger(__name__)


class BaseTestCase(TestCase):
    @classmethod
    def setUpClass(cls):
        """make sure there is a default interface for any class"""
        for inter in cls.create_environ_interfaces():
            try:
                cls.mock_async(inter.unsafe_delete_tables())

            except inter.InterfaceError as e:
                logger.exception(e)

            finally:
                cls.mock_async(inter.close())

    def tearDown(self):
        self.tearDownClass()

    @classmethod
    def tearDownClass(cls):
        for inter in cls.get_interfaces():
            cls.mock_async(inter.close())
        cls.interfaces = set()


class EnvironTestCase(BaseTestCase):
    """This will run all the tests with multple environments (eg, run the test
        with both SQLite and Postgres interfaces)"""
    interface = None

    @classmethod
    def create_interface(cls):
        return cls.create_dsn_interface(cls.interface.config.dsn)

    def run(self, *args, **kwargs):
        for inter in self.create_environ_interfaces():
            type(self).interface = inter
            super().run(*args, **kwargs)

    def countTestCases(self):
        ret = super().countTestCases()

        # get the number of interfaces
        multiplier = len(list(self.create_environ_connections()))

        return ret * multiplier

