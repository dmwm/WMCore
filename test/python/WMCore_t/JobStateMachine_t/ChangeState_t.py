#!/usr/bin/python

import unittest
import sys
import os
import logging
import threading

from WMQuality.TestInit import TestInit
from WMCore.DAOFactory import DAOFactory
from WMCore.WMFactory import WMFactory
from WMCore.JobStateMachine.ChangeState import ChangeState, Transitions
from WMCore.JobStateMachine import DefaultConfig
from WMCore.WMBS.Job import Job
from WMCore.WMBS.File import File
from WMCore.WMBS.Workflow import Workflow
import WMCore.Database.CMSCouch as CMSCouch
from WMCore.DataStructs.Run import Run
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.JobGroup import JobGroup
from sets import Set
import time
# Framework for this code written automatically by Inspect.py


class TestChangeState(unittest.TestCase):

    transitions = None
    change = None
    def setUp(self):
        """
        _setUp_
        """

        self.transitions = Transitions()
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
        locationAction.execute(siteName = "goodse.cern.ch")
        locationAction.execute(siteName = "badse.cern.ch")
                                
        # if you want to keep from colliding with other people
        #self.uniqueCouchDbName = 'jsm_test-%i' % time.time()
        # otherwise
        self.uniqueCouchDbName = 'jsm_test'
        self.change = ChangeState(DefaultConfig.config, \
                                  couchDbName=self.uniqueCouchDbName)


    def tearDown(self):
        """
        _tearDown_
        """

        myThread = threading.currentThread()
        factory = WMFactory("WMBS", "WMCore.WMBS")
        destroy = factory.loadObject(myThread.dialect + ".Destroy")
        myThread.transaction.begin()
        destroyworked = destroy.execute(conn = myThread.transaction.conn)
        server = CMSCouch.CouchServer(self.change.config.JobStateMachine.couchurl)
        server.deleteDatabase(self.uniqueCouchDbName)
        if not destroyworked:
            raise Exception("Could not complete WMBS tear down.")
        myThread.transaction.commit()

        


    def testCheck(self):
    	"""
    	This is the test class for function Check from module ChangeState
    	"""
        # Run through all good state transitions and assert that they work
        for state in self.transitions.keys():
            for dest in self.transitions[state]:
                self.change.check(dest, state)
        dummystates = ['dummy1', 'dummy2', 'dummy3', 'dummy4']

        # Then run through some bad state transistions and assertRaises(AssertionError)
        for state in self.transitions.keys():
            for dest in dummystates:
                self.assertRaises(AssertionError, self.change.check, dest, state)
    	return

    def testPersist(self):
    	"""
    	This is the test class for function Persist from module ChangeState
    	"""
        (testSubscription, testFileset, testWorkflow, testFileA,\
            testFileB, testFileC) = self.createSubscriptionWithFileABC()
        

        self.assertFalse(testSubscription.exists() , \
               "ERROR: Subscription exists before it was created")

        testSubscription.create()
        
        assert testSubscription.exists() >= 0, \
               "ERROR: Subscription does not exist after it was created"
               
        testJobGroupA = JobGroup(subscription = testSubscription)
        testJobGroupA.create()

        testJobA = Job(name = "TestJobA")
        testJobA.create(testJobGroupA)
        testJobA.addFile(testFileA)
        
        testJobB = Job(name = "TestJobB")
        testJobB.create(testJobGroupA)
        testJobB.addFile(testFileB)
        
        
        
        testJobGroupA.add(testJobA)
        testJobGroupA.add(testJobB)
        
        testJobGroupA.commit()
    
        self.change.persist([testJobA, testJobB], 'none', 'new')


    def testPropagate(self):
        """
    	This is the test class for function Propagate from module ChangeState
    	"""
        (testSubscription, testFileset, testWorkflow, testFileA,\
            testFileB, testFileC) = self.createSubscriptionWithFileABC()
        
        self.assertFalse(testSubscription.exists() , \
               "ERROR: Subscription exists before it was created")

        testSubscription.create()
        
        assert testSubscription.exists() >= 0, \
               "ERROR: Subscription does not exist after it was created"
            
        testJobGroupA = JobGroup(subscription = testSubscription)
        testJobGroupA.create()
        testJobA = Job(name = "TestJobA")
        testJobA.create(testJobGroupA)
        testJobB = Job(name = "TestJobB")
        testJobB.create(testJobGroupA)
        testJobC = Job(name = "TestJobC")
        testJobC.create(testJobGroupA)
        testJobD = Job(name = "TestJobD")
        testJobD.create(testJobGroupA)
        self.change.propagate([testJobA,testJobB,testJobC,testJobD], 'created', 'new')
        return


    def createSubscriptionWithFileABC(self):
        """
        _createSubscriptionWithFileABC_

        Create a subscription where the input fileset has three files.  Also
        create a second subscription that has acquired two of the files.
        """
        testWorkflow = Workflow(spec = "spec.xml", owner = "Simon",
                                name = "wf001", task = "Test")
        testWorkflow.create()
        testWorkflow2 = Workflow(spec = "specBOGUS.xml", owner = "Simon",
                                name = "wfBOGUS", task = "Test")
        testWorkflow2.create()        

        testFileA = File(lfn = "/this/is/a/lfnA", size = 1024, events = 20,
                         locations = Set(["goodse.cern.ch"]))
        testFileA.addRun(Run(1, *[45]))
                                 
        testFileB = File(lfn = "/this/is/a/lfnB", size = 1024, events = 20,
                         locations = Set(["goodse.cern.ch"]))                         
        testFileB.addRun(Run(1, *[45]))
        
        testFileC = File(lfn = "/this/is/a/lfnC", size = 1024, events = 20,
                         locations = Set(["goodse.cern.ch"]))
        testFileC.addRun(Run(2, *[48]))
         
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
#        testSubscription2 = Subscription(fileset = testFileset,
#                                         workflow = testWorkflow2)
#        testSubscription2.create()
#        testSubscription2.acquireFiles([testFileA, testFileB])
        
        return (testSubscription, testFileset, testWorkflow, testFileA,
                testFileB, testFileC)

    def testRecordOneInCouch(self):
        """
        	This is the test class for function RecordInCouch from module ChangeState
        	"""
        jsm = self.change.recordInCouch( [{ "dumb_value": "is_dumb", "id":1 }], "new", "none")
        jsm = self.change.recordInCouch( jsm , "created", "new")
        jsm = self.change.recordInCouch( jsm , "executing", "created")
        jsm = self.change.recordInCouch( jsm , "complete", "executing")
        jsm = self.change.recordInCouch( jsm , "success", "complete")
        jsm = self.change.recordInCouch( jsm , "closeout", "success")
        jsm1 = self.change.recordInCouch( jsm , "cleanout", "closeout")
        
        jsm = self.change.recordInCouch( [{ "dumb_value": "is_dumb", "id":2 }], "new", "none")
        jsm = self.change.recordInCouch( jsm , "created", "new")
        jsm = self.change.recordInCouch( jsm , "executing", "created")
        jsm = self.change.recordInCouch( jsm , "complete", "executing")
        jsm = self.change.recordInCouch( jsm , "success", "complete")
        jsm = self.change.recordInCouch( jsm , "closeout", "success")
        jsm2 = self.change.recordInCouch( jsm , "cleanout", "closeout")
        
        our_records1 = self.change.getCouchByParentID(jsm1[0]['couch_record'])
        self.assertEquals(len(our_records1['rows']), 7)
        our_records2 = self.change.getCouchByParentID(jsm2[0]['couch_record'])
        self.assertEquals(len(our_records2['rows']), 7)
        our_records3 = self.change.getCouchByJobID(1)
        self.assertEquals(len(our_records3['rows']), 7)
        our_records4 = self.change.getCouchByJobID(2)
        self.assertEquals(len(our_records4['rows']), 7)
        return




    def testStates(self):
    	"""
    	This is the test class for function States from module ChangeState
    	"""
        return




if __name__ == "__main__":
    unittest.main()
