#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
_DBFormatterTest_

Unit tests for the DBFormatter class

"""
from __future__ import print_function

import threading
import unittest

from builtins import str

from WMCore.Database.DBFormatter import DBFormatter
from WMQuality.TestInit import TestInit


class DBFormatterTest(unittest.TestCase):
    """
    _DBFormatterTest_

    Unit tests for the DBFormatter class

    """

    def setUp(self):
        "make a logger instance and create tables"

        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection(destroyAllDatabase=True)
        self.testInit.setSchema(customModules=["WMQuality.TestDB"],
                                useDefault=False)
        self.selectSQL = "SELECT * FROM test_tableb"

    def tearDown(self):
        """
        Delete the databases
        """
        self.testInit.clearDatabase()

    def stuffDB(self):
        """Populate one of the test tables"""
        insertSQL = "INSERT INTO test_tableb (column1, column2, column3) values (:bind1, :bind2, :bind3)"
        insertBinds = [{'bind1': u'value1a', 'bind2': 1, 'bind3': u'value2a'},
                       {'bind1': 'value1b', 'bind2': 2, 'bind3': 'value2b'},
                       {'bind1': b'value1c', 'bind2': 3, 'bind3': b'value2d'}]

        myThread = threading.currentThread()
        myThread.dbi.processData(insertSQL, insertBinds)

    def testBFormatting(self):
        """
        Test various formats
        """
        # fill the database with some initial data
        self.stuffDB()

        myThread = threading.currentThread()
        dbformatter = DBFormatter(myThread.logger, myThread.dbi)

        result = myThread.dbi.processData(self.selectSQL)
        output = dbformatter.format(result)
        self.assertEqual(output, [['value1a', 1, 'value2a'],
                                  ['value1b', 2, 'value2b'],
                                  ['value1c', 3, 'value2d']])

        result = myThread.dbi.processData(self.selectSQL)
        output = dbformatter.formatOne(result)
        print('test1 ' + str(output))
        self.assertEqual(output, ['value1a', 1, 'value2a'])

        result = myThread.dbi.processData(self.selectSQL)
        output = dbformatter.formatDict(result)
        self.assertEqual(output, [{'column3': 'value2a', 'column2': 1, 'column1': 'value1a'},
                                  {'column3': 'value2b', 'column2': 2, 'column1': 'value1b'},
                                  {'column3': 'value2d', 'column2': 3, 'column1': 'value1c'}])

        result = myThread.dbi.processData(self.selectSQL)
        output = dbformatter.formatOneDict(result)
        self.assertEqual(output, {'column3': 'value2a', 'column2': 1, 'column1': 'value1a'})


if __name__ == "__main__":
    unittest.main()
