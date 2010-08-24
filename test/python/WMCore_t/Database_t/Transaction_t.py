#!/usr/bin/env python
"""
_Transaction_t_

Unit tests for the Transaction class

"""

__revision__ = "$Id: Transaction_t.py,v 1.2 2008/11/12 16:15:03 fvlingen Exp $"
__version__ = "$Revision: 1.2 $"

import commands
import unittest
import logging
import os

from sqlalchemy import create_engine

from WMCore.Database.DBFactory import DBFactory
from WMCore.Database.Transaction import Transaction

class TransactionTest(unittest.TestCase):
    def setUp(self):
        "make a logger instance and create tables"
        
        logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M',
                    filename='%s.log' % __file__.replace('.py',''),
                    filemode='w')
        
        self.logger = logging.getLogger('DBCoreTest')
        
        self.sqlitedb = 'transaction_test.lite'
        self.mysqldb = 'transaction_test'
        
        self.tearDown()
        
        self.db = {}
        self.db['mysql'] = DBFactory(self.logger,
                         'mysql://metson@localhost/%s' % self.mysqldb).connect()
        self.db['sqlite'] = DBFactory(self.logger,
                         'sqlite:///%s' % self.sqlitedb).connect()
        #add in Oracle
        self.create = {}
        self.create['mysql'] = "create table test (bind1 varchar(20), bind2 varchar(20)) ENGINE=InnoDB;"
        self.create['sqlite'] = "create table test (bind1 varchar(20), bind2 varchar(20))"
        
        self.insert = "insert into test (bind1, bind2) values (:bind1, :bind2)"
        self.insert_binds = [ {'bind1':'value1a', 'bind2': 'value2a'},
              {'bind1':'value1b', 'bind2': 'value2b'},
              {'bind1':'value1c', 'bind2': 'value2d'} ]
        self.select = "select * from test"
            
    def tearDown(self):
        """
        Delete the databases
        """
        self.logger.debug(commands.getstatusoutput('echo yes | mysqladmin -u root drop %s' % self.mysqldb))
        self.logger.debug(commands.getstatusoutput('mysqladmin -u root create %s' % self.mysqldb))
        self.logger.debug("mysql database deleted")
        try:
            self.logger.debug(os.remove(self.sqlitedb))
        except OSError:
            #Don't care if the file doesn't exist
            pass
    
    def testGoodTransaction(self):
        for name, dbi in self.db.items():
            trans = Transaction(dbi)
            trans.processData(self.create[name])
            trans.processData(self.insert, self.insert_binds)
            result1 = trans.processData(self.select)
            
            assert len(result1) == 1
            assert len(result1[0].fetchall()) == 3
            
            trans.commit()
            trans = Transaction(dbi)
            result2 = trans.processData(self.select)
            
            assert len(result2[0].fetchall()) == 3, "commit failed"
            
    def testBadTransaction(self):
        for name, dbi in self.db.items():
            trans = Transaction(dbi)
            trans.processData(self.create[name])
            
            trans = Transaction(dbi)
            trans.processData(self.insert, self.insert_binds)
            result1 = trans.processData(self.select)
            
            assert len(result1) == 1
            assert len(result1[0].fetchall()) == 3
            
            trans.rollback()
            trans = Transaction(dbi)
            result2 = trans.processData(self.select)
            
            assert len(result2) == 1
            l = len(result2[0].fetchall())
            assert l == 0, "roll back failed, %s records" % l
           
    def runTest(self): 
        self.testGoodTransaction()
        self.testBadTransaction()

if __name__ == "__main__":
    unittest.main()     
             
