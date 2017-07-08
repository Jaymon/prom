import os

import testdata

from prom import query
from prom.interface.sqlite import SQLite

from . import BaseTestInterface


class InterfaceSQLiteTest(BaseTestInterface):
    @classmethod
    def create_interface(cls):
        return cls.create_sqlite_interface()

    def test_create_path(self):
        i = self.create_interface()
        config = i.connection_config

        d = testdata.create_dir()
        config.host = os.path.join(d, "create_path", "db.sqlite")

        i.connect(config)
        self.assertTrue(i.connected)

    def test_db_disconnect(self):
        """make sure interface can recover if the db disconnects mid script execution,
        SQLite is a bit different than postgres which is why this method is completely
        original"""
        i, s = self.get_table()
        _id = self.insert(i, s, 1)[0]
        q = query.Query()
        q.is__id(_id)
        d = i.get_one(s, q)
        self.assertGreater(len(d), 0)

        i._connection.close()

        _id = self.insert(i, s, 1)[0]
        q = query.Query()
        q.is__id(_id)
        d = i.get_one(s, q)
        self.assertGreater(len(d), 0)

    def test_no_connection(self):
        """noop, this doesn't really apply to SQLite"""
        pass

    def test_delete_nonexistent_table(self):
        """this was to fix https://github.com/firstopinion/prom/issues/47 but I
        can't seem to reproduce the problem"""
        i = self.create_interface()
        i.delete_table(testdata.get_ascii(32))
        i._delete_table(testdata.get_ascii(32))
        i.delete_tables(disable_protection=True)
        i.delete_tables(disable_protection=True)

# not sure I'm a huge fan of this solution to remove common parent from testing queue
# http://stackoverflow.com/questions/1323455/python-unit-test-with-base-and-sub-class
del(BaseTestInterface)

