#!/usr/bin/env python
"""
_Transaction_t_

Unit tests for the Transaction class

"""

__revision__ = "$Id: Transaction_t.py,v 1.5 2009/10/13 22:42:59 meloam Exp $"
__version__ = "$Revision: 1.5 $"

import commands
import logging
import os
import threading
import unittest

from WMCore.Database.DBFactory import DBFactory
from WMCore.Database.Transaction import Transaction
from WMCore.WMFactory import WMFactory

from WMQuality.TestInit import TestInit

class TransactionTest(unittest.TestCase):

    _setup = False
    _teardown = False

    def setUp(self):
        if not self._setup:
            #self.tearDown()
            self.testInit = TestInit(__file__)
            self.testInit.setLogging()
            self.testInit.setDatabaseConnection()
            self._setup = True
        
            #add in Oracle
            self.create = {}
            self.create['MySQL'] = "create table test (bind1 varchar(20), bind2 varchar(20)) ENGINE=InnoDB;"
            self.create['SQLite'] = "create table test (bind1 varchar(20), bind2 varchar(20))"
            self.create['Oracle'] = "create table test (bind1 varchar(20), bind2 varchar(20))"

            self.destroy = {}
            self.destroy['MySQL']  = "drop table test ENGINE=InnoDB"
            self.destroy['Oracle'] = "drop table test"
            self.destroy['SQLite'] = "drop table test"

            myThread = threading.currentThread()
            myThread.dialect = os.getenv('DIALECT')
        
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

        if self._teardown:
            return


    
        # call the script we use for cleaning:
        print('Clear database')
        self.testInit.clearDatabase()
        myThread.transaction.begin()
        myThread.transaction.processData(self.destroy[myThread.dialect])
        myThread.transaction.commit()
            
        self._teardown = False


    def testGoodTransaction(self):
        print('testGoodTransaction')
        #self._teardown = True
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
        #self._teardown = True
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
        self.assertEqual(l,0)

        return
        #"roll back failed, %s records" % l

    def testLostConnection(self):
        """
        Make some transactions and lose the connection 
        before committing.
        """
        print('testLostTransaction')
        #self._teardown = True
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
        #print result1[0].fetchall()

        # check if the right amount of entries have been made. 
        self.assertEqual(len(result1), 1)
        self.assertEqual(len(result1[0].fetchall()), 33)

        myThread.transaction.commit()
        # check if buffer is empty
        assert len(myThread.transaction.sqlBuffer) == 0
    
           

if __name__ == "__main__":
    unittest.main()     
             
