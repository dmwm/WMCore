#!/usr/bin/env python

import unittest, os, logging, commands, random, threading
from sets import Set

from WMCore.Database.DBCore import DBInterface
from WMCore.Database.DBFactory import DBFactory
from WMCore.DAOFactory import DAOFactory
from WMCore.WMBS.File import File
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Workflow import Workflow
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMFactory import WMFactory
from WMQuality.TestInit import TestInit

class Subscription_t(unittest.TestCase):
    _setup = False
    _teardown = False
    
    def setUp(self):
        """
        _setUp_

        Setup the database and logging connection.  Try to create all of the
        WMBS tables.
        """
        if self._setup:
            return
        
        self.testInit = TestInit(__file__, os.getenv("DIALECT"))
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules = ["WMCore.WMBS"],
                                useDefault = False)

        myThread = threading.currentThread()
        daofactory = DAOFactory(package = "WMCore.WMBS",
                                logger = myThread.logger,
                                dbinterface = myThread.dbi)
        
        locationAction = daofactory(classname = "Locations.New")
        locationAction.execute(sename = "goodse.cern.ch")
        locationAction.execute(sename = "badse.cern.ch")
        
        self._setup = True
        return
                                                                
    def tearDown(self):
        """
        _tearDown_
        
        Drop all the WMBS tables.
        """
        myThread = threading.currentThread()
        
        if self._teardown:
            return
        
        factory = WMFactory("WMBS", "WMCore.WMBS")
        destroy = factory.loadObject(myThread.dialect + ".Destroy")
        myThread.transaction.begin()
        destroyworked = destroy.execute(conn = myThread.transaction.conn)
        if not destroyworked:
            raise Exception("Could not complete WMBS tear down.")
        myThread.transaction.commit()
        
        self._teardown = True

    def testCreateDeleteExists(self):
        """
        _testCreateDeleteExists_

        """
        testWorkflow = Workflow(spec = "spec.xml", owner = "Simon",
                                name = "wf001")
        testWorkflow.create()

        testFileA = File(lfn = "/this/is/a/lfnA", size = 1024,
                         events = 20, run = 1, lumi = 45)
        testFileB = File(lfn = "/this/is/a/lfnB", size = 1024,
                         events = 20, run = 1, lumi = 45)
        testFileC = File(lfn = "/this/is/a/lfnC", size = 1024,
                         events = 20, run = 1, lumi = 45)
        testFileA.create()
        testFileB.create()
        testFileC.create()
        
        testFilesetA = Fileset(name = "TestFileset")
        testFilesetA.create()
        
        testFilesetA.addFile(testFileA)
        testFilesetA.addFile(testFileB)
        testFilesetA.addFile(testFileC)
        testFilesetA.commit()

        testSubscription = Subscription(fileset = testFilesetA,
                                        workflow = testWorkflow)

        assert testSubscription.exists() == False, \
               "ERROR: Subscription exists before it was created"

        testSubscription.create()

        assert testSubscription.exists() >= 0, \
               "ERROR: Subscription does not exist after it was created"

        testSubscription.delete()

        assert testSubscription.exists() == False, \
               "ERROR: Subscription exists after it was deleted"

        testFilesetA.delete()
        testFileA.delete()
        testFileB.delete()
        testFileC.delete()
        testWorkflow.delete()        
        return
    
    def testFailFiles(self):
        """
        _testFailFiles_

        """
        testWorkflow = Workflow(spec = "spec.xml", owner = "Simon",
                                name = "wf001")
        testWorkflow.create()

        testFileA = File(lfn = "/this/is/a/lfnA", size = 1024,
                         events = 20, run = 1, lumi = 45)
        testFileB = File(lfn = "/this/is/a/lfnB", size = 1024,
                         events = 20, run = 1, lumi = 45)
        testFileC = File(lfn = "/this/is/a/lfnC", size = 1024,
                         events = 20, run = 1, lumi = 45)
        testFileA.create()
        testFileB.create()
        testFileC.create()
        
        testFileset = Fileset(name = "TestFileset")
        testFileset.create()
        
        testFileset.addFile(testFileA)
        testFileset.addFile(testFileB)
        testFileset.addFile(testFileC)        
        testFileset.commit()

        testSubscription = Subscription(fileset = testFileset,
                                        workflow = testWorkflow)
        testSubscription.create()

        testSubscription.failFiles([testFileA, testFileC])
        failedFiles = testSubscription.filesOfStatus(status = "FailedFiles")

        goldenFiles = [testFileA, testFileC]
        for failedFile in failedFiles:
            assert failedFile in goldenFiles, \
                   "ERROR: Unknown failed files"
            goldenFiles.remove(failedFile)

        assert len(goldenFiles) == 0, \
               "ERROR: Missing failed files"

        testSubscription.delete()
        testWorkflow.delete()
        testFileset.delete()
        testFileA.delete()
        testFileB.delete()
        testFileC.delete()
        return
    
    def testCompleteFiles(self):
        """
        _testCompleteFiles_

        """
        testWorkflow = Workflow(spec = "spec.xml", owner = "Simon",
                                name = "wf001")
        testWorkflow.create()

        testFileA = File(lfn = "/this/is/a/lfnA", size = 1024,
                         events = 20, run = 1, lumi = 45)
        testFileB = File(lfn = "/this/is/a/lfnB", size = 1024,
                         events = 20, run = 1, lumi = 45)
        testFileC = File(lfn = "/this/is/a/lfnC", size = 1024,
                         events = 20, run = 1, lumi = 45)
        testFileA.create()
        testFileB.create()
        testFileC.create()
        
        testFileset = Fileset(name = "TestFileset")
        testFileset.create()
        
        testFileset.addFile(testFileA)
        testFileset.addFile(testFileB)
        testFileset.addFile(testFileC)        
        testFileset.commit()

        testSubscription = Subscription(fileset = testFileset,
                                        workflow = testWorkflow)
        testSubscription.create()

        testSubscription.completeFiles([testFileA, testFileC])
        completedFiles = testSubscription.filesOfStatus(status = "CompletedFiles")

        goldenFiles = [testFileA, testFileC]
        for completedFile in completedFiles:
            assert completedFile in goldenFiles, \
                   "ERROR: Unknown completed file"
            goldenFiles.remove(completedFile)

        assert len(goldenFiles) == 0, \
               "ERROR: Missing completed files"

        testSubscription.delete()
        testWorkflow.delete()
        testFileset.delete()
        testFileA.delete()
        testFileB.delete()
        testFileC.delete()        
        return
    
    def testAcquireFiles(self):
        """
        _testAcquireFiles_

        """
        testWorkflow = Workflow(spec = "spec.xml", owner = "Simon",
                                name = "wf001")
        testWorkflow.create()

        testFileA = File(lfn = "/this/is/a/lfnA", size = 1024,
                         events = 20, run = 1, lumi = 45)
        testFileB = File(lfn = "/this/is/a/lfnB", size = 1024,
                         events = 20, run = 1, lumi = 45)
        testFileC = File(lfn = "/this/is/a/lfnC", size = 1024,
                         events = 20, run = 1, lumi = 45)
        testFileA.create()
        testFileB.create()
        testFileC.create()
        
        testFileset = Fileset(name = "TestFileset")
        testFileset.create()
        
        testFileset.addFile(testFileA)
        testFileset.addFile(testFileB)
        testFileset.addFile(testFileC)        
        testFileset.commit()

        testSubscription = Subscription(fileset = testFileset,
                                        workflow = testWorkflow)
        testSubscription.create()

        testSubscription.acquireFiles([testFileA, testFileC])
        acquiredFiles = testSubscription.filesOfStatus(status = "AcquiredFiles")

        goldenFiles = [testFileA, testFileC]
        for acquiredFile in acquiredFiles:
            assert acquiredFile in goldenFiles, \
                   "ERROR: Unknown acquired file"
            goldenFiles.remove(acquiredFile)

        assert len(goldenFiles) == 0, \
               "ERROR: Missing acquired files"

        testSubscription.delete()
        testWorkflow.delete()
        testFileset.delete()
        testFileA.delete()
        testFileB.delete()
        testFileC.delete()
        return
    
    def testAvailableFilesWhiteList(self):
        """
        _testAvailableFilesWhiteList_
        
        Testcase for the availableFiles method of the Subscription Class when a 
        white list is present in the subscription.
        """
        testWorkflow = Workflow(spec = "spec.xml", owner = "Simon",
                                name = "wf001")
        testWorkflow.create()

        testFileset = Fileset(name = "TestFileset")
        testFileset.create()
        
        testSubscription = Subscription(fileset = testFileset,
                                        workflow = testWorkflow)
        testSubscription.create()
        
        count = 0
        for i in range(0, 100):
            lfn = "/store/data/%s/%s/file.root" % (random.randint(1000, 9999),
                                                   random.randint(1000, 9999))
            size = random.randint(1000, 2000)
            events = 1000
            run = random.randint(0, 2000)
            lumi = random.randint(0, 8)
    
            testFile = File(lfn=lfn, size=size, events=events, run=run, lumi=lumi)
            testFile.create()
            
            if random.randint(1, 2) > 1:
                testFile.setLocation("goodse.cern.ch")
                count += 1
            else:
                testFile.setLocation("badse.cern.ch")

            testFileset.addFile(testFile)
            
        testFileset.commit()
        testSubscription.markLocation("goodse.cern.ch")
        
        assert count == len(testSubscription.availableFiles()), \
        "Subscription has %s files available, should have %s" % \
        (len(testSubscription.availableFiles()), count)
        
    def testAvailableFilesBlackList(self):
        """
        _testAvailableFilesBlackList_
        
        Testcase for the availableFiles method of the Subscription Class
        """
        testWorkflow = Workflow(spec = "spec.xml", owner = "Simon",
                                name = "wf001")
        testWorkflow.create()

        testFileset = Fileset(name = "TestFileset")
        testFileset.create()
        
        testSubscription = Subscription(fileset = testFileset,
                                        workflow = testWorkflow)
        testSubscription.create()
        
        count = 0
        for i in range(0, 100):
            lfn = "/blacklist/%s/%s/file.root" % (random.randint(1000, 9999),
                                                  random.randint(1000, 9999))
            size = random.randint(1000, 2000)
            events = 1000
            run = random.randint(0, 2000)
            lumi = random.randint(0, 8)
    
            testFile = File(lfn=lfn, size=size, events=events, run=run, lumi=lumi)
            testFile.create()
            
            if random.randint(1, 2) > 1:
                testFile.setLocation("goodse.cern.ch")
            else:
                testFile.setLocation("badse.cern.ch")
                count += 1
                
            testFileset.addFile(testFile)
        testFileset.commit()
        
        testSubscription.markLocation("badse.cern.ch", whitelist = False)
        assert 100 - count == len(testSubscription.availableFiles()), \
        "Subscription has %s files available, should have %s" %\
        (len(testSubscription.availableFiles()), 100 - count) 
               
    def testAvailableFilesBlackWhiteList(self):
        """
        _testAvailableFilesBlackWhiteList_
        
        Testcase for the availableFiles method of the Subscription Class when 
        both a white and black list are provided
        """
        testWorkflow = Workflow(spec = "spec.xml", owner = "Simon",
                                name = "wf001")
        testWorkflow.create()

        testFileset = Fileset(name = "TestFileset")
        testFileset.create()
        
        testSubscription = Subscription(fileset = testFileset,
                                        workflow = testWorkflow)
        testSubscription.create()
        
        count = 0
        for i in range(0, 10):
            lfn = "/store/data/%s/%s/file.root" % (random.randint(1000, 9999),
                                                   random.randint(1000, 9999))
            size = random.randint(1000, 2000)
            events = 1000
            run = random.randint(0, 2000)
            lumi = random.randint(0, 8)
    
            testFile = File(lfn=lfn, size=size, events=events, run=run, lumi=lumi)
            testFile.create()
            
            if random.randint(1, 2) > 1:
                testFile.setLocation("goodse.cern.ch")
                count += 1
            else:
                testFile.setLocation("badse.cern.ch")

            testFileset.addFile(testFile)
            
        testFileset.commit()   
        testSubscription.markLocation("badse.cern.ch", whitelist = False)
        testSubscription.markLocation("goodse.cern.ch")
        
        assert count == len(testSubscription.availableFiles()), \
        "Subscription has %s files available, should have %s" %\
        (len(testSubscription.availableFiles()), count)   

    def testLoad(self):
        """
        _testLoad_

        """
        testWorkflow = Workflow(spec = "spec.xml", owner = "Simon",
                                name = "wf001")
        testWorkflow.create()

        testFileA = File(lfn = "/this/is/a/lfnA", size = 1024,
                         events = 20, run = 1, lumi = 45)
        testFileB = File(lfn = "/this/is/a/lfnB", size = 1024,
                         events = 20, run = 1, lumi = 45)
        testFileC = File(lfn = "/this/is/a/lfnC", size = 1024,
                         events = 20, run = 1, lumi = 45)
        testFileA.create()
        testFileB.create()
        testFileC.create()
        
        testFileset = Fileset(name = "TestFileset")
        testFileset.create()
        
        testFileset.addFile(testFileA)
        testFileset.addFile(testFileB)
        testFileset.addFile(testFileC)        
        testFileset.commit()

        testSubscriptionA = Subscription(fileset = testFileset,
                                         workflow = testWorkflow)
        testSubscriptionA.create()

        testSubscriptionB = Subscription(id = testSubscriptionA["id"])
        testSubscriptionC = Subscription(workflow = testSubscriptionA["workflow"],
                                         fileset = testSubscriptionA["fileset"],
                                         type = testSubscriptionA["type"])
        testSubscriptionB.load()
        testSubscriptionC.load()

        assert (testWorkflow.id == testSubscriptionB["workflow"].id) and \
               (testWorkflow.name == testSubscriptionB["workflow"].name) and \
               (testWorkflow.spec == testSubscriptionB["workflow"].spec) and \
               (testWorkflow.owner == testSubscriptionB["workflow"].owner), \
               "ERROR: Subscription load by ID didn't load workflow correctly"

        # Need to complete...
        return
       
if __name__ == "__main__":
    unittest.main()
