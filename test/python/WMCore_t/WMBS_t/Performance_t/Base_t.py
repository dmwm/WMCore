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

        self.mysqldao = DAOFactory(package='WMCore.WMBS', logger=self.logger, 
                        dbinterface=self.mysqldbf.connect())
        self.sqlitedao = DAOFactory(package='WMCore.WMBS', logger=self.logger, 
                        dbinterface=self.sqlitedbf.connect())        
        
    def tearDown(self):
        #Base tearDown method for the DB Performance test
        self.logger.debug(commands.getstatusoutput('echo yes | mysqladmin -u root drop wmbs'))
        self.logger.debug(commands.getstatusoutput('mysqladmin -u root create wmbs'))
        self.logger.debug("WMBS MySQL database deleted")
        try:
            self.logger.debug(os.remove('filesettest.lite'))
        except OSError:
            #Don't care if the file doesn't exist
            pass
        self.logger.debug("WMBS SQLite database deleted")

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
    
    def perfTest(self, dao, action, execinput=''):
        """
        Method that executes a dao class operation and measures its
        execution time.
        
        """
        
        #Test each of the DAO classes of the specific WMBS class directory        
        startTime = time.time()               
        #Place execute method of the specific classname here            
        dao(classname=action).execute(execinput)
        endTime = time.time()
        diffTime = endTime - startTime
        
        return diffTime
