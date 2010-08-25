#!/usr/bin/env python
"""
_DBFormatterTest_

Unit tests for the DBFormatter class

"""

__revision__ = "$Id: DBFormatter_t.py,v 1.8 2009/10/13 23:00:08 meloam Exp $"
__version__ = "$Revision: 1.8 $"

import commands
import logging
import unittest
import os
import threading

from WMCore.Database.DBFactory import DBFactory
from WMCore.Database.DBFormatter import DBFormatter
from WMCore.Database.Transaction import Transaction
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
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema()

        myThread = threading.currentThread()
        if os.getenv("DIALECT") == 'MySQL':
            myThread.create = """
create table test (bind1 varchar(20), bind2 varchar(20)) ENGINE=InnoDB """
        if os.getenv("DIALECT") == 'SQLite':
            myThread.create = """
                create table test (bind1 varchar(20), bind2 varchar(20))"""
            
        myThread.insert = """
insert into test (bind1, bind2) values (:bind1, :bind2) """
        myThread.insert_binds = \
          [ {'bind1':'value1a', 'bind2': 'value2a'},\
            {'bind1':'value1b', 'bind2': 'value2b'},\
            {'bind1':'value1c', 'bind2': 'value2d'} ]
        myThread.select = "select * from test"
            
    def tearDown(self):
        """
        Delete the databases
        """
        self.testInit.clearDatabase()


    def testAPrepare(self):
        """
        Prepare database by inserting schema and values

        """

        myThread = threading.currentThread()
        myThread.transaction = Transaction(myThread.dbi)
        myThread.transaction.processData(myThread.create)
        myThread.transaction.processData(myThread.insert, myThread.insert_binds)
        myThread.transaction.commit()

    def testBFormatting(self):
        """
        Test various formats
        """

        myThread = threading.currentThread()
        dbformatter = DBFormatter(myThread.logger, myThread.dbi)
        myThread.transaction.begin()

        result = myThread.transaction.processData(myThread.select)
        output = dbformatter.format(result)
        assert output ==  [['value1a', 'value2a'], \
            ['value1b', 'value2b'], ['value1c', 'value2d']]
        result = myThread.transaction.processData(myThread.select)
        output = dbformatter.formatOne(result)
        print('test1 '+str(output))
        assert output == ['value1a', 'value2a']
        result = myThread.transaction.processData(myThread.select)
        output = dbformatter.formatDict(result)
        assert output == [{'bind2': 'value2a', 'bind1': 'value1a'}, \
            {'bind2': 'value2b', 'bind1': 'value1b'},\
            {'bind2': 'value2d', 'bind1': 'value1c'}]
        result = myThread.transaction.processData(myThread.select)
        output = dbformatter.formatOneDict(result)
        assert output ==  {'bind2': 'value2a', 'bind1': 'value1a'}
        DBFormatterTest.__teardown = True

            
if __name__ == "__main__":
    unittest.main()     
             
