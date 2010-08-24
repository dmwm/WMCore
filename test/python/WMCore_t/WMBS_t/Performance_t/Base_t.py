#!/usr/bin/env python

import os, unittest, logging, commands, time


from unittest import TestCase
from WMCore.Database.DBCore import DBInterface
from WMCore.Database.DBFactory import DBFactory
from WMCore.DAOFactory import DAOFactory

class Base_t():
    """
    __Base_t__

    Base class for DB Performance at WMBS


    """
    def setUp(self):
        """
        Common setUp for all Performance tests

        """
        #Place common execute method arguments here        
        self.baseexec=' '
        
        self.sename='localhost'        
        logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M',
                    filename='%s.log' % __file__.replace('.py',''),
                    filemode='w')
        
        self.logger = logging.getLogger('DBPerformanceTest')
        
        self.DBList = ['MySQL','SQLite']
        mysqlURI = 'mysql://jcg@localhost/wmbs'
        sqliteURI = 'sqlite:///dbperftest.lite'

        self.mysqldbf = DBFactory(self.logger, mysqlURI)
        self.sqlitedbf = DBFactory(self.logger, sqliteURI)        

        self.mysqldao = DAOFactory(package='WMCore.WMBS', logger=daologger, 
                        dbinterface=mysqldbf.connect())
        self.sqlitedao = DAOFactory(package='WMCore.WMBS', logger=daologger, 
                        dbinterface=sqlitedbf.connect())        
        
    def tearDown(self)
        #Base tearDown method for the DB Performance test
        #TODO - Database deleting,etc,etc...
        pass

    def getClassNames(self, dirname='.'):
        """
        Method that gets the DAO classnames from the directory tree
        Still needs testing, only present here to illustrate the idea
        of make these tests automatic.

        """
        files = os.listdir(dirname)
        list = []
        for x in files:
            #Hack - Only get relevant .py files (no __init__.py, CVS stuff, etc...)
            if (x[-3:] == '.py') and (x[0] != '_'):
                parts = x.split('.')
                list.append(parts[0])

        return list
    
    def perfTest(self, dao, execinput=''):
        """
        Method that executes a dao class operation and measures its
        execution time.
        
        """
        #POTENTIAL PITFALL:
        #dao object must be given as an argument ALREADY WITH A CLASSNAME LOADED ON IT
        #TODO - Exception that deals with uninitialized DAOs given as an argument

        #Test each of the DAO classes of the specific WMBS class directory        
        startTime = time.time()               
        #Place execute method of the specific classname here            
        dao.execute(execinput)
        endTime = time.time()
        diffTime = endTime - startTime
        
        return diffTime
