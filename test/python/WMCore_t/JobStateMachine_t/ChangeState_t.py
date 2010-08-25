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
from WMCore.Database.CMSCouch import CouchServer
from WMCore.DataStructs.Run import Run
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.JobGroup import JobGroup
from WMCore.FwkJobReport.Report import Report

class TestChangeState(unittest.TestCase):
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

        self.couchServer = CouchServer(dburl = os.getenv("COUCHURL"))
        return

    def tearDown(self):
        """
        _tearDown_

        """
        self.testInit.clearDatabase()
        self.couchServer.deleteDatabase("changestate_t")
        return

    def testCheck(self):
    	"""
    	This is the test class for function Check from module ChangeState
    	"""
        DefaultConfig.config.JobStateMachine.couchURL = os.getenv("COUCHURL")
        change = ChangeState(DefaultConfig.config, "changestate_t")

        # Run through all good state transitions and assert that they work
        for state in self.transitions.keys():
            for dest in self.transitions[state]:
                change.check(dest, state)
        dummystates = ['dummy1', 'dummy2', 'dummy3', 'dummy4']

        # Then run through some bad state transistions and assertRaises(AssertionError)
        for state in self.transitions.keys():
            for dest in dummystates:
                self.assertRaises(AssertionError, change.check, dest, state)
    	return

    def testRecordInCouch(self):
    	"""
        _testRecordInCouch_
        
        Verify that jobs, state transitions and fwjrs are recorded into seperate
        couch documents correctly.
    	"""
        DefaultConfig.config.JobStateMachine.couchURL = os.getenv("COUCHURL")
        change = ChangeState(DefaultConfig.config, "changestate_t")
        
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

        testFileA = File(lfn = "SomeLFNA", events = 1024, size = 2048)
        testFileA.create()
        testJobA.addFile(testFileA)

        testFileB = File(lfn = "SomeLFNB", events = 1025, size = 2049)
        testFileB.create()
        testJobB.addFile(testFileB)        

        timestamp = int(time.time())

        change.propagate([testJobA, testJobB], "new", "none")
        change.propagate([testJobA, testJobB], "created", "new")
        change.propagate([testJobA, testJobB], "executing", "created")

        testJobADoc = change.database.document(testJobA["couch_record"])

        assert testJobADoc["jobid"] == testJobA["id"], \
               "Error: ID parameter is incorrect."
        assert testJobADoc["name"] == testJobA["name"], \
               "Error: Name parameter is incorrect."
        assert testJobADoc["jobgroup"] == testJobA["jobgroup"], \
               "Error: Jobgroup parameter is incorrect."
        assert testJobADoc["mask"] == testJobA["mask"], \
               "Error: Mask parameter is incorrect."
        assert testJobADoc["input_files"] == testJobA["input_files"], \
               "Error: Input files parameter is incorrect."
        
        testJobBDoc = change.database.document(testJobB["couch_record"])

        assert testJobBDoc["jobid"] == testJobB["id"], \
               "Error: ID parameter is incorrect."
        assert testJobBDoc["name"] == testJobB["name"], \
               "Error: Name parameter is incorrect."
        assert testJobBDoc["jobgroup"] == testJobB["jobgroup"], \
               "Error: Jobgroup parameter is incorrect."
        assert testJobBDoc["mask"] == testJobB["mask"], \
               "Error: Mask parameter is incorrect."
        assert testJobBDoc["input_files"] == testJobB["input_files"], \
               "Error: Input files parameter is incorrect."

        changeStateDB = self.couchServer.connectDatabase(dbname = "changestate_t")
        options = {"startkey": testJobA["id"], "endkey": testJobA["id"]}
        results = changeStateDB.loadView("JobDump", "stateTransitionsByJobID",
                                         options)

        assert len(results["rows"]) == 3, \
               "Error: Wrong number of state transitions."

        goldenNewStates = ["new", "created", "executing"]
        for result in results["rows"]:
            if result["value"]["oldstate"] == "none":
                assert result["value"]["newstate"] == "new", \
                       "Error: Wrong newstate."
                goldenNewStates.remove("new")
            elif result["value"]["oldstate"] == "new":
                assert result["value"]["newstate"] == "created", \
                       "Error: Wrong newstate."
                goldenNewStates.remove("created")
            elif result["value"]["oldstate"] == "created":
                assert result["value"]["newstate"] == "executing", \
                       "Error: Wrong newstate."
                goldenNewStates.remove("executing")

        assert len(goldenNewStates) == 0, \
               "Error: Missing state transitions."
                    
        return

    def testPersist(self):
        """
        _testPersist_
        
        This is the test class for function Propagate from module ChangeState
        """
        DefaultConfig.config.JobStateMachine.couchURL = os.getenv("COUCHURL")
        change = ChangeState(DefaultConfig.config, "changestate_t")
        
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

        change.persist([testJobA, testJobB], "created", "new")
        change.persist([testJobC, testJobD], "new", "none")        

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
        DefaultConfig.config.JobStateMachine.couchURL = os.getenv("COUCHURL")
        change = ChangeState(DefaultConfig.config, "changestate_t")
        
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

        change.propagate([testJobA], 'created', 'new')
        myReport = Report()
        myReport.unpersist("Report.pkl")
        testJobA["fwjr"] = myReport

        change.propagate([testJobA], 'executing', 'created')        
        return

if __name__ == "__main__":
    unittest.main()
