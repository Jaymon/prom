
from prom import query
from prom.model import Orm
import prom
import prom.interface

from . import BaseTestCase


class PromTest(BaseTestCase):

    def setUp(self):
        prom.interface.interfaces = {}

    def test_configure(self):
        dsn = 'prom.interface.postgres.PostgreSQL://username:password@localhost/db'
        prom.configure(dsn)
        i = prom.get_interface()
        self.assertTrue(i is not None)

        dsn += '#postgres'
        prom.configure(dsn)
        i = prom.get_interface('postgres')
        self.assertTrue(i is not None)

        dsn = 'bogus.earaskdfaksfk.Interface://host/dbname#postgres'
        with self.assertRaises(ValueError):
            prom.configure(dsn)

        dsn = 'bogus.earaskdfaksfk.Interface://host/dbname'
        with self.assertRaises(ValueError):
            prom.configure(dsn)

        dsn = 'bogus.earaskdfaksfk.Interface://host/dbname#bogus1'
        with self.assertRaises(ImportError):
            prom.configure(dsn)

        dsn = 'prom.interface.postgres.BogusSdjaksdfInterface://host/dbname#bogus2'
        with self.assertRaises(AttributeError):
            prom.configure(dsn)

    def test_failure_set(self):
        """test to make sure setting on a table that doesn't exist doesn't actually fail"""
        class FailureSetTorm(Orm):
            interface = self.get_interface()
            schema = self.get_schema()

        f = FailureSetTorm(foo=1, bar="value 1")
        f.set()
        self.assertTrue(f.pk)

    def test_failure_get(self):
        """test to make sure getting on a table that doesn't exist works without raising
        an error
        """
        class FailureGetTorm(Orm):
            interface = self.get_interface()
            schema = self.get_schema()

        f = FailureGetTorm(foo=1, bar="value 1")
        f.query.get_one()
        # we succeeded if no error was raised

