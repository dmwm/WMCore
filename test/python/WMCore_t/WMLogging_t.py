#!/usr/bin/env python
# encoding: utf-8
"""
Test case for WMLogging module
"""
from builtins import range
from datetime import date
import logging
import unittest
import os

from WMCore.WMLogging import CouchHandler, MyTimedRotatingFileHandler
from WMCore.Database.CMSCouch import CouchServer


class WMLoggingTest(unittest.TestCase):
    """
    Unit tests for WMLogging module.
    """
    def setUp(self):
        # Make an instance of the server
        self.server = CouchServer(os.getenv("COUCHURL", 'http://admin:password@localhost:5984'))
        testname = self.id().split('.')[-1]
        # Create a database, drop an existing one first
        self.dbname = f'cmscouch_unittest_{testname.lower()}'

        if self.dbname in self.server.listDatabases():
            self.server.deleteDatabase(self.dbname)

        self.server.createDatabase(self.dbname)
        self.db = self.server.connectDatabase(self.dbname)

    def tearDown(self):
        # This used to test self._exc_info to only run on success. Broke in 2.7. Removed.
        self.server.deleteDatabase(self.dbname)

    def testRotatingLogHandler(self):
        """
        Test to make sure a date is in or will be added to the logName
        """
        todayStr = date.today().strftime("%Y%m%d")
        logName = "mylog.log"
        handler = MyTimedRotatingFileHandler(logName, 'midnight', backupCount=10)
        self.assertIn(todayStr, handler.baseFilename)

        logName = f"mylog-{todayStr}.log"
        handler = MyTimedRotatingFileHandler(logName, 'midnight', backupCount=10)
        self.assertIn(todayStr, handler.baseFilename)

    def testLog(self):
        """
        Write ten log messages to the database at three different levels
        """
        myLogger = logging.getLogger('MyLogger')
        myLogger.setLevel(logging.DEBUG)
        handler = CouchHandler(self.server.url, self.dbname)
        formatter = logging.Formatter('%(message)s')
        handler.setFormatter(formatter)
        myLogger.addHandler(handler)

        for _ in range(10):
            myLogger.debug('This is probably all noise.')
            myLogger.info('Jackdaws love my big sphinx of quartz.')
            myLogger.error('HOLLY CRAP!')
        logs = self.db.allDocs()['rows']
        self.assertEqual(30, len(logs))


if __name__ == "__main__":
    unittest.main()
