#!/usr/bin/env python
"""
_Transaction_t_

Unit tests for the Transaction class

"""

__revision__ = "$Id: Transaction_t.py,v 1.3 2008/11/13 16:05:39 fvlingen Exp $"
__version__ = "$Revision: 1.3 $"

import commands
import logging
import os
import threading
import unittest

from WMCore.Database.DBFactory import DBFactory
from WMCore.Database.Transaction import Transaction

from WMQuality.TestInit import TestInit

class TransactionTest(unittest.TestCase):

    _setup = False
    _teardown = False

    def setUp(self):
        if not TransactionTest._setup:
            self.tearDown()
            self.testInit = TestInit(__file__)
            self.testInit.setLogging()
            self.testInit.setDatabaseConnection()
            TransactionTest._setup = True
        
            #add in Oracle
            self.create = {}
            self.create['MySQL'] = "create table test (bind1 varchar(20), bind2 varchar(20)) ENGINE=InnoDB;"
            self.create['SQLite'] = "create table test (bind1 varchar(20), bind2 varchar(20))"
        
            self.insert = "insert into test (bind1, bind2) values (:bind1, :bind2)"
            self.insert_binds = [ {'bind1':'value1a', 'bind2': 'value2a'},
                  {'bind1':'value1b', 'bind2': 'value2b'},
                  {'bind1':'value1c', 'bind2': 'value2d'} ]
            self.select = "select * from test"
            
    def tearDown(self):
        """
        Delete the databases
        """
        myThread = threading.currentThread()
        if TransactionTest._teardown :
            # call the script we use for cleaning:
            print('Clear database')
            self.testInit.clearDatabase()
        TransactionTest._teardown = False


    def testGoodTransaction(self):
        print('testGoodTransaction')
        TransactionTest._teardown = True
        myThread = threading.currentThread()
        myThread.transaction.begin()
        myThread.transaction.processData(self.create[myThread.dialect])
        myThread.transaction.processData(self.insert, self.insert_binds)
        result1 = myThread.transaction.processData(self.select)
            
        assert len(result1) == 1
        assert len(result1[0].fetchall()) == 3
            
        myThread.transaction.commit()
        myThread.transaction.begin()
        result2 = myThread.transaction.processData(self.select)
            
        assert len(result2[0].fetchall()) == 3, "commit failed"
            
    def testBadTransaction(self):
        print('testBadTransaction')
        TransactionTest._teardown = True
        myThread = threading.currentThread()
        myThread.transaction.begin()
        myThread.transaction.processData(self.create[myThread.dialect])
        myThread.transaction.processData(self.insert, self.insert_binds)
        result1 = myThread.transaction.processData(self.select)
            
        assert len(result1) == 1
        assert len(result1[0].fetchall()) == 3
            
        myThread.transaction.rollback()
        myThread.transaction.begin()

        result2 = myThread.transaction.processData(self.select)
            
        assert len(result2) == 1
        l = len(result2[0].fetchall())
        assert l == 0, "roll back failed, %s records" % l

    def testLostConnection(self):
        """
        Make some transactions and lose the connection 
        before committing.
        """
        print('testLostTransaction')
        TransactionTest._teardown = True
        myThread = threading.currentThread()
        myThread.transaction.begin()
        myThread.transaction.processData(self.create[myThread.dialect])
        myThread.transaction.commit()
        assert len(myThread.transaction.sqlBuffer) == 0
        myThread.transaction.begin()
        # create some inserts (in batches of three)
        for i in xrange(0, 10):
            myThread.transaction.processData(self.insert, self.insert_binds)
        # lose connection by closing on purpose:
        myThread.transaction.conn.close()
        # try to submit something. 
        myThread.transaction.processData(self.insert, self.insert_binds)
        myThread.transaction.commit()
        assert len(myThread.transaction.sqlBuffer) == 0
        myThread.transaction.begin()
       
        result1 = myThread.transaction.processData(self.select)

        # check if the right amount of entries have been made. 
        assert len(result1) == 1
        assert len(result1[0].fetchall()) == 33
        myThread.transaction.commit()
        # check if buffer is empty
        assert len(myThread.transaction.sqlBuffer) == 0
    
           
    def runTest(self): 
        self.testGoodTransaction()
        self.tearDown()
        self.testBadTransaction()
        self.tearDown()
        self.testLostConnection()

if __name__ == "__main__":
    unittest.main()     
             
