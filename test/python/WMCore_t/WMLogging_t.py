#!/usr/bin/env python
# encoding: utf-8
from builtins import range
import logging
import unittest
import os
from WMCore.WMLogging import CouchHandler
from WMCore.Database.CMSCouch import CouchServer

class WMLoggingTest(unittest.TestCase):
    def setUp(self):
        # Make an instance of the server
        self.server = CouchServer(os.getenv("COUCHURL", 'http://admin:password@localhost:5984'))
        testname = self.id().split('.')[-1]
        # Create a database, drop an existing one first
        self.dbname = 'cmscouch_unittest_%s' % testname.lower()

        if self.dbname in self.server.listDatabases():
            self.server.deleteDatabase(self.dbname)

        self.server.createDatabase(self.dbname)
        self.db = self.server.connectDatabase(self.dbname)

    def tearDown(self):
        # This used to test self._exc_info to only run on success. Broke in 2.7. Removed.
        self.server.deleteDatabase(self.dbname)

    def testLog(self):
        """
        Write ten log messages to the database at three different levels
        """
        my_logger = logging.getLogger('MyLogger')
        my_logger.setLevel(logging.DEBUG)
        handler = CouchHandler(self.server.url, self.dbname)
        formatter = logging.Formatter('%(message)s')
        handler.setFormatter(formatter)
        my_logger.addHandler(handler)

        for _ in range(10):
            my_logger.debug('This is probably all noise.')
            my_logger.info('Jackdaws love my big sphinx of quartz.')
            my_logger.error('HOLLY CRAP!')
        logs = self.db.allDocs()['rows']
        self.assertEqual(30, len(logs))

if __name__ == "__main__":
    unittest.main()
