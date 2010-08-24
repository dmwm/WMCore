#!/usr/bin/env python
"""
_FilesTestCase_

Unit tests for File creation, location and exists, including checks to see that calls 
are database dialect neutral.

"""

__revision__ = "$Id: subscription_DAOFactory_unit.py,v 1.1 2008/06/24 17:01:16 metson Exp $"
__version__ = "$Revision: 1.1 $"

import unittest, logging, os, commands

from WMCore.Database.DBCore import DBInterface
from WMCore.Database.DBFactory import DBFactory
from WMCore.DAOFactory import DAOFactory
from WMCore.WMBS.File import File
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Workflow import Workflow
from WMCore.WMBS.Subscription import Subscription
#pylint --rcfile=../../../../standards/.pylintrc  ../../../../src/python/WMCore/WMBS/Fileset.py

class BaseFilesTestCase(unittest.TestCase):
    def setUp(self):
        "make a logger instance"
        #level=logging.ERROR
        logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M',
                    filename='%s.log' % __file__,
                    filemode='w')
        
        self.mysqllogger = logging.getLogger('wmbs_mysql')
        self.sqlitelogger = logging.getLogger('wmbs_sqlite')
        self.testlogger = logging.getLogger('unit_test')
        
        self.tearDown()
        
        self.dbf1 = DBFactory(self.mysqllogger, 'mysql://metson@localhost/wmbs')
        self.dbf2 = DBFactory(self.sqlitelogger, 'sqlite:///filesettest.lite')
        
        self.daofactory1 = DAOFactory(package='WMCore.WMBS', logger=self.mysqllogger, dbinterface=self.dbf1.connect())
        self.daofactory2 = DAOFactory(package='WMCore.WMBS', logger=self.sqlitelogger, dbinterface=self.dbf2.connect())
        
        theMySQLCreator = self.daofactory1(classname='CreateWMBS')
        createworked = theMySQLCreator.execute()
        if createworked:
            self.testlogger.debug("WMBS MySQL database created")
        else:
            self.testlogger.debug("WMBS MySQL database could not be created, already exists?")
            
        theSQLiteCreator = self.daofactory2(classname='CreateWMBS')
        createworked = theSQLiteCreator.execute()
        if createworked:
            self.testlogger.debug("WMBS SQLite database created")
        else:
            self.testlogger.debug("WMBS SQLite database could not be created, already exists?")
        
        self.selist = ['lcgse01.phy.bris.ac.uk', 'lcgse02.phy.bris.ac.uk', 'se01.fnal.gov', 'se02.fnal.gov']
        
        for se in self.selist:
            self.daofactory1(classname='Locations.New').execute(sename=se)
            self.daofactory2(classname='Locations.New').execute(sename=se)
        
    def tearDown(self):
        """
        Delete the databases
        """
        self.testlogger.debug(commands.getstatusoutput('echo yes | mysqladmin -u root drop wmbs'))
        self.testlogger.debug(commands.getstatusoutput('mysqladmin -u root create wmbs'))
        self.testlogger.debug("WMBS MySQL database deleted")
        try:
            self.testlogger.debug(os.remove('filesettest.lite'))
        except OSError:
            #Don't care if the file doesn't exist
            pass
        self.testlogger.debug("WMBS SQLite database deleted")
        
class SubscriptionDAOObjectTestCase(BaseFilesTestCase):
    def setUp(self):
        BaseFilesTestCase.setUp(self)
        
        
        
    
class SubscriptionBusinessObjectTestCase(BaseFilesTestCase):
    def setUp(self):
        BaseFilesTestCase.setUp(self)
        c = 0
        self.workflow = []
        self.fileset = []
        for dbi in self.dbf1, self.dbf2:
            self.workflow.append(Workflow(spec='/home/metson/workflow.xml', 
                                 owner='metson', 
                                 name='My Analysis', 
                                 logger=self.testlogger, 
                                 dbfactory=dbi))
            self.workflow[c].create()
            
            self.fileset.append(Fileset(name='MyCoolFiles', logger=self.testlogger, 
                                 dbfactory=dbi))
            self.fileset[c].create()
            c = c + 1 
    
    def createSubs(self, testlogger):
        subscriptions = []
        c = 0
        for dbi in [self.dbf1]:#, self.dbf2:
            subscriptions.append(Subscription(fileset = self.fileset[c], 
                                            workflow = self.workflow[c], 
                                            logger=testlogger, 
                                            dbfactory = dbi))
            subscriptions[c].create()
            c = c + 1
        return subscriptions
            
    def testCreate(self):
        testlogger = logging.getLogger('testCreate')
        subscriptions = self.createSubs(testlogger)
         
        for i in subscriptions:
            assert i.exists(), "Subscription does not exist"
            
    def testLoad(self):
        testlogger = logging.getLogger('testLoad')
        self.createSubs(testlogger) #Put some subscriptions into the database
        subscriptions = []
        c = 0
        for dbi in [self.dbf1]:#, self.dbf2:
            subscriptions.append(Subscription(fileset = self.fileset[c], 
                                            workflow = self.workflow[c], 
                                            logger=testlogger, 
                                            dbfactory = dbi))
            subscriptions[c].load()
            c = c + 1
    
    def testFileCycle(self):
        pass
             
        
if __name__ == "__main__":
    unittest.main()