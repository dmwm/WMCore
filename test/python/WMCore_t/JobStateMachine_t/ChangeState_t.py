#!/usr/bin/python

import unittest
import sys
import os
import logging
import threading
import time
import urllib

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
from WMCore.FwkJobReport.Report import Report

class TestChangeState(unittest.TestCase):

    transitions = None
    change = None
    def setUp(self):
        """
        _setUp_

        """
        self.transitions = Transitions()
        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()

        self.testInit.setSchema(customModules = ["WMCore.WMBS"],
                                useDefault = False)

        myThread = threading.currentThread()
        self.daoFactory = DAOFactory(package = "WMCore.WMBS",
                                     logger = myThread.logger,
                                     dbinterface = myThread.dbi)

        DefaultConfig.config.JobStateMachine.couchURL = os.getenv("COUCHURL")
        self.change = ChangeState(DefaultConfig.config, "changestate_t")
        return

    def tearDown(self):
        """
        _tearDown_

        """
        myThread = threading.currentThread()
        factory = WMFactory("WMBS", "WMCore.WMBS")
        destroy = factory.loadObject(myThread.dialect + ".Destroy")
        myThread.transaction.begin()
        destroyworked = destroy.execute(conn = myThread.transaction.conn)
        if not destroyworked:
            raise Exception("Could not complete WMBS tear down.")
        myThread.transaction.commit()
        return

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

    def testRecordInCouch(self):
    	"""
        _testRecordInCouch_
        
        Verify that state changes are recorded correctly into a single couch
        document.
    	"""
        testWorkflow = Workflow(spec = "spec.xml", owner = "Steve",
                                name = "wf001", task = "Test")
        testWorkflow.create()
        testFileset = Fileset(name = "TestFileset")
        testFileset.create()
        testSubscription = Subscription(fileset = testFileset,
                                        workflow = testWorkflow)
        testSubscription.create()
        
        testJobGroupA = JobGroup(subscription = testSubscription)
        testJobGroupA.create()

        testJobA = Job(name = "TestJobA")
        testJobB = Job(name = "TestJobB")
        testJobA.create(testJobGroupA)
        testJobB.create(testJobGroupA)
        
        self.change.recordInCouch([testJobA, testJobB], "new", "none")
        self.change.recordInCouch([testJobA, testJobB], "created", "new")
        self.change.recordInCouch([testJobA, testJobB], "executing", "created")

        testJobADoc = self.change.database.document(testJobA["couch_record"])
        testJobBDoc = self.change.database.document(testJobB["couch_record"])        
        return

    def testPersist(self):
        """
        _testPersist_
        
        This is the test class for function Propagate from module ChangeState
        """
        testWorkflow = Workflow(spec = "spec.xml", owner = "Steve",
                                name = "wf001", task = "Test")
        testWorkflow.create()
        testFileset = Fileset(name = "TestFileset")
        testFileset.create()
        testSubscription = Subscription(fileset = testFileset,
                                        workflow = testWorkflow)
        testSubscription.create()
        
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

        self.change.persist([testJobA, testJobB], "created", "new")
        self.change.persist([testJobC, testJobD], "new", "none")        

        stateDAO = self.daoFactory(classname = "Jobs.GetState")

        jobAState = stateDAO.execute(id = testJobA["id"])
        jobBState = stateDAO.execute(id = testJobB["id"])
        jobCState = stateDAO.execute(id = testJobC["id"])
        jobDState = stateDAO.execute(id = testJobD["id"])        

        assert jobAState == "created" and jobBState =="created" and \
               jobCState == "new" and jobDState == "new", \
               "Error: Jobs didn't change state correctly."
        
        return

    def testJobSerialization(self):
        """
        _testJobSerialization_

        Verify that serialization of a job works when adding a FWJR.
        """
        testWorkflow = Workflow(spec = "spec.xml", owner = "Steve",
                                name = "wf001", task = "Test")
        testWorkflow.create()
        testFileset = Fileset(name = "TestFileset")
        testFileset.create()
        testSubscription = Subscription(fileset = testFileset,
                                        workflow = testWorkflow)
        testSubscription.create()

        testJobGroupA = JobGroup(subscription = testSubscription)
        testJobGroupA.create()

        testJobA = Job(name = "TestJobA")
        testJobA.create(testJobGroupA)

        self.change.propagate([testJobA], 'created', 'new')
        myReport = Report()
        myReport.unpersist("Report.pkl")
        testJobA["fwjr"] = myReport

        self.change.propagate([testJobA], 'executing', 'created')        
        return

if __name__ == "__main__":
    unittest.main()
