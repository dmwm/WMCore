#!/usr/bin/env python
"""
_DBFormatterTest_

Unit tests for the DBFormatter class

"""




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
        if myThread.dialect == 'MySQL':
            myThread.create = """
create table test (bind1 varchar(20), bind2 varchar(20)) ENGINE=InnoDB """
        if myThread.dialect == 'SQLite':
            myThread.create = """
                create table test (bind1 varchar(20), bind2 varchar(20))"""
            
        myThread.insert = """
insert into test (bind1, bind2) values (:bind1, :bind2) """
        myThread.insert_binds = \
          [ {'bind1':'value1a', 'bind2': 'value2a'},\
            {'bind1':'value1b', 'bind2': 'value2b'},\
            {'bind1':'value1c', 'bind2': 'value2d'} ]
        myThread.select = "select * from test"

        myThread = threading.currentThread()
        myThread.transaction = Transaction(myThread.dbi)
        myThread.transaction.processData(myThread.create)
        myThread.transaction.processData(myThread.insert, myThread.insert_binds)
        myThread.transaction.commit()

        return
            
    def tearDown(self):
        """
        Delete the databases
        """
        myThread = threading.currentThread()
        myThread.transaction = Transaction(myThread.dbi)
        myThread.transaction.processData("drop table test")
        myThread.transaction.commit()        
        self.testInit.clearDatabase()

    def testBFormatting(self):
        """
        Test various formats
        """

        myThread = threading.currentThread()
        dbformatter = DBFormatter(myThread.logger, myThread.dbi)
        myThread.transaction.begin()

        result = myThread.transaction.processData(myThread.select)
        output = dbformatter.format(result)
        self.assertEqual(output ,  [['value1a', 'value2a'], \
            ['value1b', 'value2b'], ['value1c', 'value2d']])
        result = myThread.transaction.processData(myThread.select)
        output = dbformatter.formatOne(result)
        print('test1 '+str(output))
        self.assertEqual( output , ['value1a', 'value2a'] )
        result = myThread.transaction.processData(myThread.select)
        output = dbformatter.formatDict(result)
        self.assertEqual( output , [{'bind2': 'value2a', 'bind1': 'value1a'}, \
            {'bind2': 'value2b', 'bind1': 'value1b'},\
            {'bind2': 'value2d', 'bind1': 'value1c'}] )
        result = myThread.transaction.processData(myThread.select)
        output = dbformatter.formatOneDict(result)
        self.assertEqual( output,  {'bind2': 'value2a', 'bind1': 'value1a'} )

            
if __name__ == "__main__":
    unittest.main()     
             
