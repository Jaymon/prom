# -*- coding: utf-8 -*-
import logging

from prom.compat import *
import prom
from .testdata import (
    TestData,
    TestCase,
    IsolatedAsyncioTestCase as _IsolatedAsyncioTestCase,
    SkipTest,
    InterfaceData, # importing hooks into testdata
)


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
        "testdata": "WARNING",
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
#     @classmethod
#     def setUpClass(cls):
#         """make sure there is a default interface for any class"""
#         for inter in cls.create_environ_interfaces():
#             try:
#                 cls.mock_async(inter.unsafe_delete_tables())
# 
#             except inter.InterfaceError as e:
#                 logger.exception(e)
# 
#             finally:
#                 cls.mock_async(inter.close())
# 
#     def tearDown(self):
#         self.tearDownClass()
# 
#     @classmethod
#     def tearDownClass(cls):
#         for inter in cls.get_interfaces():
#             cls.mock_async(inter.close())
#         cls.interfaces = set()
    pass


class IsolatedAsyncioTestCase(_IsolatedAsyncioTestCase):
    interface_class = None
    """Set this to an Interface class and only that class will be used to create
    interfaces

    Holds the interface class that should be used. This is set on the actual
    TestCase and then passed to this class in .setUpClass and removed
    in .tearDownClass
    """
    def setUp(self):
        self.data.interface_class = self.interface_class

    def tearDown(self):
        self.data.interface_class = None

#     @classmethod
#     def setUpClass(cls):
#         cls.data.interface_class = cls.interface_class
# 
#     @classmethod
#     def tearDownClass(cls):
#         cls.data.interface_class = None

    async def asyncSetUp(self):
        # close any global connections
        for name, inter in prom.interface.interfaces.items():
            await inter.close()

        # we need to delete all the tables
        inter = self.get_interface()
        try:
            await inter.unsafe_delete_tables()

        except inter.InterfaceError as e:
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

#     def get_interface(self, interface=None):
#         """We have to override certain testdata methods to make sure interface
#         handling works as expected for each interface"""
#         return interface or self.create_interface()
# 
#     def create_interface(self):
#         # interface_class needs to be set on each child class
#         return self.find_interface(self.interface_class)
# 
#     def get_orm_class(self, **kwargs):
#         if "interface" not in kwargs:
#             kwargs["interface"] = self.get_interface(
#                 kwargs.get("interface", None)
#             )
#         return self.td.get_orm_class(**kwargs)
# 
#     def get_table(self, **kwargs):
#         if "interface" not in kwargs:
#             kwargs["interface"] = self.get_interface(
#                 kwargs.get("interface", None)
#             )
#         return self.td.get_table(**kwargs)
# 
#     async def create_table(self, *args, **kwargs):
#         interface, schema = self.get_table(*args, **kwargs)
#         await interface.set_table(schema)
#         return interface, schema
# 
#     async def insert(self, *args, **kwargs):
#         pks = []
#         interface, schema, orm_class, fields = self.get_insert_fields(
#             *args,
#             **kwargs
#         )
#         for fs in fields:
#             pks.append(await interface.insert(schema, fs))
# 
#         return pks


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

