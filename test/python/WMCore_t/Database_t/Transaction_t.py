#!/usr/bin/env python
"""
_Transaction_t_

Unit tests for the Transaction class

"""




import commands
import logging
import os
import threading
import unittest

from WMCore.Database.DBFactory   import DBFactory
from WMCore.Database.Transaction import Transaction
from WMCore.WMFactory            import WMFactory
from WMQuality.TestInit          import TestInit

class TransactionTest(unittest.TestCase):


    def setUp(self):

        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
    
        self.dialect = os.environ.get('DIALECT', 'MySQL')

        if self.dialect.lower() == 'mysql':
            self.create = "CREATE TABLE test (bind1 varchar(20), bind2 varchar(20)) ENGINE=InnoDB;"
        elif self.dialect.lower() == 'oracle':
            self.create = 'CREATE TABLE test (bind1 varchar(20), bind2 varchar(20))'

        return

            
    def tearDown(self):
        """
        Delete the databases
        """
        # call the script we use for cleaning: Clean the whole DB
        self.testInit.clearDatabase()
        return

    def testA_Insert(self):
        """
        _Insert_

        See if we can insert data into a table
        See if, having inserted data into a table
        """

        myThread = threading.currentThread()
        trans = myThread.transaction
        trans.begin()
        trans.commit()



        trans.begin()
        trans.processData(self.create)
        trans.commit()

        try:
            trans.processData("DESCRIBE test", redo = False)
            flag = True
        except:
            flag = False
        self.assertTrue(flag)

        try:
            trans.processData("DESCRIBE test2", redo = False)
            flag = True
        except:
            flag = False
        self.assertFalse(flag)

        # Make sure we can write a table
        trans.begin()
        trans.processData("INSERT INTO test (bind1, bind2) VALUES (:bind1, :bind2)",
                                         [{'bind1': 'one', 'bind2': 'two'},
                                          {'bind1': 'three', 'bind2': 'four'}], redo = False)
        result = trans.processData("SELECT * FROM test", redo = False)[0].fetchall()
        self.assertEqual(result, [('one', 'two'), ('three', 'four')])
        trans.commit()

        trans.begin()
        result = trans.processData("SELECT * FROM test", redo = False)[0].fetchall()
        self.assertEqual(result, [('one', 'two'), ('three', 'four')])
        trans.commit()


        # Make sure we can drop a table
        trans.begin()
        trans.processData("DROP TABLE test", redo = False)
        trans.commit()

        try:
            trans.processData("DESCRIBE test", redo = False)
            flag = True
        except:
            flag = False
        self.assertFalse(flag)


        # Recreate table.  Try rollback
        trans.begin()
        trans.processData(self.create)
        trans.commit()

        trans.begin()
        trans.processData("INSERT INTO test (bind1, bind2) VALUES (:bind1, :bind2)",
                                         [{'bind1': 'one', 'bind2': 'two'},
                                          {'bind1': 'three', 'bind2': 'four'}], redo = False)
        result = trans.processData("SELECT * FROM test", redo = False)[0].fetchall()
        self.assertEqual(result, [('one', 'two'), ('three', 'four')])
        trans.rollback()

        trans.begin()
        result = trans.processData("SELECT * FROM test", redo = False)[0].fetchall()
        self.assertEqual(result, [])
        trans.commit()

        return



#     def testLostConnection(self):
#         """
#         Make some transactions and lose the connection 
#         before committing.
#         """
#         print('testLostTransaction')
#         raise RuntimeError, "This test seems to run for a really really long time even though it shouldnt"

#         #self._teardown = True
#         myThread = threading.currentThread()
#         myThread.transaction.begin()
#         myThread.transaction.processData(self.create[myThread.dialect])
#         myThread.transaction.commit()
#         self.assertEqual( len(myThread.transaction.sqlBuffer) ,  0 )
#         myThread.transaction.begin()
#         # create some inserts (in batches of three)
#         for i in xrange(0, 10):
#             myThread.transaction.processData(self.insert, self.insert_binds)
#         # lose connection by closing on purpose:
#         myThread.transaction.conn.close()
#         # try to submit something. 
#         myThread.transaction.processData(self.insert, self.insert_binds)
#         myThread.transaction.commit()
#         self.assertEqual( len(myThread.transaction.sqlBuffer) ,  0 )
#         myThread.transaction.begin()
       
#         result1 = myThread.transaction.processData(self.select)
#         #print result1[0].fetchall()

#         # check if the right amount of entries have been made. 
#         self.assertEqual(len(result1), 1)
#         self.assertEqual(len(result1[0].fetchall()), 33)

#         myThread.transaction.commit()
#         # check if buffer is empty
#         self.assertEqual( len(myThread.transaction.sqlBuffer) ,  0 )
    
           

if __name__ == "__main__":
    unittest.main()     
             
