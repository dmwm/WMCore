#!/usr/bin/env python
"""
_FilesTestCase_

Unit tests for File creation, location and exists, including checks to see that calls 
are database dialect neutral.

"""

__revision__ = "$Id: subscription_DAOFactory_unit.py,v 1.6 2008/07/21 15:21:35 metson Exp $"
__version__ = "$Revision: 1.6 $"

import unittest, logging, os, commands, random, datetime
import sys, traceback

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
        
        createworked = False
        try:
            theMySQLCreator = self.daofactory1(classname='CreateWMBS')
            createworked = theMySQLCreator.execute()
        except:
            pass
        if createworked:
            self.testlogger.debug("WMBS MySQL database created")
        else:
            self.testlogger.debug("WMBS MySQL database could not be created, already exists?")
        createworked = False
        try:   
            theSQLiteCreator = self.daofactory2(classname='CreateWMBS')
            createworked = theSQLiteCreator.execute()
        except:
            pass
        if createworked:
            self.testlogger.debug("WMBS SQLite database created")
        else:
            self.testlogger.debug("WMBS SQLite database could not be created, already exists?")
        
        self.selist = ['lcgse01.phy.bris.ac.uk', 'lcgse02.phy.bris.ac.uk', 'se01.fnal.gov', 'se02.fnal.gov']
        try:
            for se in self.selist:
                self.daofactory1(classname='Locations.New').execute(sename=se)
                self.daofactory2(classname='Locations.New').execute(sename=se)
        except:
            pass
                
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
    ran = False
    def setUp(self):
        if not self.ran:
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
            self.ran = True
        
    def tearDown(self):
        if not self.ran:
            BaseFilesTestCase.tearDown(self)
            
    def createSubs(self, testlogger):
        subscriptions = []
        c = 0
        for dbi in [self.dbf1, self.dbf2]:
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
        for dbi in [self.dbf1, self.dbf2]:
            subscriptions.append(Subscription(fileset = self.fileset[c], 
                                            workflow = self.workflow[c], 
                                            logger=testlogger, 
                                            dbfactory = dbi))
            subscriptions[c].load()
            c = c + 1
    
    def testFileCycle(self):
        testlogger = logging.getLogger('testFileCycle')
        self.createSubs(testlogger)
        filelist = []
        num_files = 1000
        for i in range(0,num_files):
            filelist.append(("/store/data/Electrons/1234/5678/hkh123ghk12khj3hk123ljhkj1232%s.root" % i, 
                             1000, 2000, 10 + i, 12312))
        for dao in self.daofactory1, self.daofactory2:
            dao(classname='Files.Add').execute(files=filelist)
            dao(classname='Files.AddRunLumi').execute(files=filelist)
        
        def strim(tuple): return tuple[0]
        filelist = map(strim, filelist)
        
        for dao in self.daofactory1, self.daofactory2:     
            dao(classname='Files.AddToFileset').execute(file=filelist, fileset='MyCoolFiles')
        
        subscriptions = []
        c = 0
        for dbi in [self.dbf1, self.dbf2]:
            subscriptions.append(Subscription(fileset = self.fileset[c], 
                                            workflow = self.workflow[c], 
                                            logger=testlogger, 
                                            dbfactory = dbi))
            subscriptions[c].load()
            for i in range(0,15):
                testlogger.debug("Timing stats - start %s" % datetime.datetime.now())
                avail = subscriptions[c].availableFiles()
                testlogger.debug("Timing stats - avail done %s" % datetime.datetime.now())
                acquired = subscriptions[c].acquiredFiles()
                testlogger.debug("Timing stats - acquired done %s" % datetime.datetime.now())
                complete = subscriptions[c].completedFiles()
                testlogger.debug("Timing stats - complete done %s" % datetime.datetime.now())
                failed = subscriptions[c].failedFiles()
                testlogger.debug("Timing stats - failed done %s" % datetime.datetime.now())
                testlogger.debug("\niteration: %i" % i)
                testlogger.debug("\tavail: %i" % len(avail))
                testlogger.debug("\tacquired %i" % len(acquired))
                testlogger.debug("\tcomplete %i" % len(complete))
                testlogger.debug("\tfailed %i" % len(failed))
                assert len(avail) + len(acquired) + len(complete) + len(failed) == num_files, \
                    "iteration %s - number of files not consistent: avail:%s acqu:%s comp:%s fail:%s total:%s" % \
                    (i, len(avail), len(acquired), len(complete), len(failed), num_files)
                testlogger.debug(subscriptions[c].acquireFiles(size=10))
                fail_prob = i / 4
                complete_prob = i / 1.5
                
                #Pretend to run jobs
                for f in subscriptions[c].acquiredFiles():
                    if random.randint(0 , 15) < fail_prob:
                        subscriptions[c].failFiles(f)
                    elif random.randint(0 , 15) < complete_prob:
                        subscriptions[c].completeFiles(f)
                complete = subscriptions[c].completedFiles()
                failed = subscriptions[c].failedFiles()
                if len(complete) > 0:
                    id = complete.pop()
                    testlogger.debug(id)
                    completedfile = File(id = id, dbfactory = dbi, logger = testlogger)
                    completedfile.load()
                    testlogger.debug(completedfile.getInfo())
                    assert completedfile.dict['id'] == id, "File did not load correctly - wrong ID!"
                if len(failed) > 0:
                    id = failed.pop()
                    testlogger.debug(id)
                    failedfile = File(id = id, dbfactory = dbi, logger = testlogger)
                    failedfile.load()
                    testlogger.debug(failedfile.getInfo())
                    assert failedfile.dict['id'] == id, "File did not load correctly - wrong ID!"
            c = c + 1
        
if __name__ == "__main__":
    unittest.main()