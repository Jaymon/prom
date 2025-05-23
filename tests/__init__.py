# -*- coding: utf-8 -*-
import logging

from prom.compat import *
from prom.exception import InterfaceError
from prom.extras.testdata import ModelData
import prom
from .testdata import (
    TestData,
    TestCase,
    IsolatedAsyncioTestCase,
    SkipTest,
    basic_logging,
)

basic_logging(
    levels={
        "prom": "DEBUG",
        #"prom": "ERROR",
        #"prom": "INFO",
        #"prom.extras.testdata": "DEBUG",
        "datatypes": "WARNING",
        "dsnparse": "WARNING",
        "psycopg": "WARNING",
        "aiosqlite": "WARNING",
        "asyncio": "WARNING",
        "testdata": "WARNING",
    },
)


logger = logging.getLogger(__name__)


class IsolatedAsyncioTestCase(IsolatedAsyncioTestCase):
    interface_class = None
    """Set this to an Interface class and only that class will be used to
    create interfaces

    Holds the interface class that should be used. This is set on the actual
    TestCase and then passed to this class in .setUpClass and removed
    in .tearDownClass
    """
    @classmethod
    def setUpClass(cls):
        # we get rid of this because it has .get_orm_class and .get_orm
        # methods that will stomp the normal ones
        cls.data.delete_class(ModelData)

    @classmethod
    def tearDownClass(cls):
        # we remove ModelData here so it doesn't poison other tests just in
        # case it got re-added
        cls.data.delete_class(ModelData)

    def setUp(self):
        self.data.interface_class = self.interface_class

        # clear caches since I have a tendency to use the same names over
        # and over again when testing
        prom.Orm.orm_classes.clear()

    def tearDown(self):
        self.data.interface_class = None

    async def asyncSetUp(self):
        # close any global connections
        for name, inter in prom.interface.interfaces.items():
            await inter.close()

        # we need to delete all the tables
        inter = self.get_interface()
        try:
            await inter.unsafe_delete_tables()

        except InterfaceError as e:
            logger.exception(e)

        finally:
            await inter.close()

        # close any test class connections
        for inter in self.get_interfaces():
            await inter.close()

        # discard all the old connections
        self.interfaces = set()
        prom.interface.interfaces = {}

    async def asyncTearDown(self):
        await self.asyncSetUp()


class EnvironTestCase(IsolatedAsyncioTestCase):
    """This will run all the tests with multple environments (eg, run the test
        with both SQLite and Postgres interfaces)"""
    def run(self, *args, **kwargs):
        for interface_class in self.get_environ_interface_classes():
            type(self).interface_class = interface_class
            super().run(*args, **kwargs)

    def countTestCases(self):
        ret = super().countTestCases() # this is always 1

        # get the number of interfaces
        multiplier = len(list(self.create_environ_connections()))

        return ret * multiplier

