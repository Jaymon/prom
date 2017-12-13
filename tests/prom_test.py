# -*- coding: utf-8 -*-
from __future__ import unicode_literals, division, print_function, absolute_import
from prom import query
from prom.model import Orm
import prom
import prom.interface

from . import TestCase


class PromTest(TestCase):

    def setUp(self):
        prom.interface.interfaces = {}

    def tearDown(self):
        prom.interface.interfaces = {}

    def test_configure(self):
        dsn = 'prom.interface.sqlite.SQLite:///path/to/db'
        i = prom.configure(dsn)
        self.assertTrue(i.connection_config.path)

        dsn = 'prom.interface.postgres.PostgreSQL://username:password@localhost/db'
        prom.configure(dsn)
        i = prom.get_interface()
        self.assertTrue(i is not None)

        dsn += '#postgres'
        prom.configure(dsn)
        i = prom.get_interface('postgres')
        self.assertTrue(i is not None)

        dsn = 'bogus.earaskdfaksfk.Interface://host/dbname#postgres'
        with self.assertRaises(ImportError):
            prom.configure(dsn)

        dsn = 'bogus.earaskdfaksfk.Interface://host/dbname'
        with self.assertRaises(ImportError):
            prom.configure(dsn)

        dsn = 'bogus.earaskdfaksfk.Interface://host/dbname#bogus1'
        with self.assertRaises(ImportError):
            prom.configure(dsn)

        dsn = 'prom.interface.postgres.BogusSdjaksdfInterface://host/dbname#bogus2'
        with self.assertRaises(AttributeError):
            prom.configure(dsn)

