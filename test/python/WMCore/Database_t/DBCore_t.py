#!/usr/bin/env python
"""
_DBCore_t_

Unit tests for the

"""

__revision__ = "$Id: DBCore_t.py,v 1.1 2008/08/21 07:22:51 metson Exp $"
__version__ = "$Revision: 1.1 $"

import commands
import unittest
import logging
import os

from sqlalchemy import create_engine

from WMCore.Database.DBCore import DBInterface

class DBCoreTest(unittest.TestCase):
    def setUp(self):
        "make a logger instance and create tables"
        
        logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M',
                    filename='%s.log' % __file__.replace('.py',''),
                    filemode='w')
        
        self.logger = logging.getLogger('DBCoreTest')
        
        self.tearDown()
        
        self.db = []
        self.db.append(DBInterface(logger, 
                              create_engine('mysql://metson@localhost/test')))
        self.db.append(DBFactory(logger, 
                              create_engine('sqlite:///dbcoretest.lite')))
        #add in Oracle
            
    def tearDown(self):
        """
        Delete the databases
        """
        self.logger.debug(commands.getstatusoutput('echo yes | mysqladmin -u root drop test'))
        self.logger.debug(commands.getstatusoutput('mysqladmin -u root create test'))
        self.logger.debug("database deleted")
        
    def createTable(self):
        sql = "create table test (bind1 varchar(20), bind2 varchar(20))"
        self.runTest(sql)
        
    def runTest(self, sql, binds=None):
        for dbi in self.db:
            dbi.processData(sql, binds)
    
    def fillTable(self):
        sql = "insert into thing (bind1, bind2) values (:bind1, :bind2)"
        b = [ {'bind1':'value1a', 'bind2': 'value2a'},
              {'bind1':'value1b', 'bind2': 'value2b'},
              {'bind1':'value1c', 'bind2': 'value2d'} ]
        self.runTest(sql, b)
        
    def testCreate(self):
        self.createTable()
        
    def testInsert(self):
        self.createTable()
        sql = "insert into thing (bind1, bind2) values (:bind1, :bind2)"
        b = {'bind1':'value1a', 'bind2': 'value2a'}
        self.runTest(sql, b)
        
        
    def testInsertMany(self):
        self.createTable()
        self.fillTable()

    def testSelect(self):
        self.createTable()
        sql = "select * from thing"
        result = self.runTest(sql)
        print result
        
    def testSelectMany(self):
        self.createTable()
        sql = "select * from thing where bind1 = :value"
        b = [ {'value':'value1a'},
              {'value':'value1b'} ]
        result = self.runTest(sql, b)
        print result
        
            
    