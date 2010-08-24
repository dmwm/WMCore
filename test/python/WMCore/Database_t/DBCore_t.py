#!/usr/bin/env python
"""
_DBCore_t_

Unit tests for the DBInterface class

"""

__revision__ = "$Id: DBCore_t.py,v 1.2 2008/08/21 17:30:31 metson Exp $"
__version__ = "$Revision: 1.2 $"

import commands
import unittest
import logging
import os

from sqlalchemy import create_engine
from sqlalchemy.exceptions import OperationalError
from WMCore.Database.DBFactory import DBFactory

class DBCoreTest(unittest.TestCase):
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
        
    def createTable(self):
        try:
            sql = "create table test (bind1 varchar(20), bind2 varchar(20))"
            self.runTest(sql)
        except OperationalError, oe:
            print oe
        
        
    def runTest(self, sql, binds={}):
        result = {}
        for name, dbi in self.db.items():
            result[name] = dbi.processData(sql, binds)
        return result
    
    def fillTable(self):
        sql = "insert into test (bind1, bind2) values (:bind1, :bind2)"
        b = [ {'bind1':'value1a', 'bind2': 'value2a'},
              {'bind1':'value1b', 'bind2': 'value2b'},
              {'bind1':'value1c', 'bind2': 'value2d'} ]
        self.runTest(sql, b)
        
    def testCreate(self):
        self.createTable()
        
    def testInsert(self):
        self.createTable()
        sql = "insert into test (bind1, bind2) values (:bind1, :bind2)"
        b = {'bind1':'value1a', 'bind2': 'value2a'}
        self.runTest(sql, b)
        
    def testInsertMany(self):
        self.createTable()
        self.fillTable()

    def testSelect(self):
        self.createTable()
        self.fillTable()
        sql = "select * from test"
        result = self.runTest(sql)
        
        for name in self.db.keys():
            assert len(result[name]) == 1
            assert len(result[name][0].fetchall()) == 3
        
    def testSelectMany(self):
        self.createTable()
        self.fillTable()
        sql = "select * from test where bind1 = :value"
        b = [ {'value':'value1a'},
              {'value':'value1b'} ]
        result = self.runTest(sql, b)
        for name in self.db.keys():
            "should have two results, one for each set of binds"
            assert len(result[name]) == 2
            "should have one record per result"
            for i in result[name]:
                assert len(i.fetchall()) == 1
        
      
            
if __name__ == "__main__":
    unittest.main()     
                   
    