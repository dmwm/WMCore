#/usr/bin/env python
"""
_ChangeState_t_

"""

import unittest
import sys
import os
import logging
import threading
import time
import urllib
import types

from WMQuality.TestInitCouchApp import TestInitCouchApp

from WMCore.DAOFactory import DAOFactory
from WMCore.WMFactory import WMFactory
from WMCore.Database.CMSCouch import CouchServer

from WMCore.JobStateMachine.ChangeState import ChangeState, Transitions
from WMCore.JobStateMachine import DefaultConfig

from WMCore.WMBS.Job import Job
from WMCore.WMBS.File import File
from WMCore.WMBS.Workflow import Workflow
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.JobGroup import JobGroup

from WMCore.DataStructs.Run import Run

from WMCore.FwkJobReport.Report import Report
from WMCore.JobSplitting.SplitterFactory import SplitterFactory
from WMCore.WMInit import getWMBASE

class TestChangeState(unittest.TestCase):
    def setUp(self):
        """
        _setUp_

        """
        self.transitions = Transitions()
        self.testInit = TestInitCouchApp(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setupCouch("changestate_t", "JobDump")

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
        self.testInit.tearDownCouch()
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

        locationAction = self.daoFactory(classname = "Locations.New")
        locationAction.execute("site1", seName = "somese.cern.ch")
        
        testWorkflow = Workflow(spec = "spec.xml", owner = "Steve",
                                name = "wf001", task = "Test")
        testWorkflow.create()
        testFileset = Fileset(name = "TestFileset")
        testFileset.create()
        testSubscription = Subscription(fileset = testFileset,
                                        workflow = testWorkflow,
                                        split_algo = "FileBased")
        testSubscription.create()
        
        testFileA = File(lfn = "SomeLFNA", events = 1024, size = 2048,
                         locations = set(["somese.cern.ch"]))
        testFileB = File(lfn = "SomeLFNB", events = 1025, size = 2049,
                         locations = set(["somese.cern.ch"]))
        testFileA.create()
        testFileB.create()

        testFileset.addFile(testFileA)
        testFileset.addFile(testFileB)
        testFileset.commit()

        splitter = SplitterFactory()
        jobFactory = splitter(package = "WMCore.WMBS",
                              subscription = testSubscription)
        jobGroup = jobFactory(files_per_job = 1)[0]

        assert len(jobGroup.jobs) == 2, \
               "Error: Splitting should have created two jobs."

        testJobA = jobGroup.jobs[0]
        testJobB = jobGroup.jobs[1]

        change.propagate([testJobA, testJobB], "new", "none")
        change.propagate([testJobA, testJobB], "created", "new")
        change.propagate([testJobA, testJobB], "executing", "created")

        testJobADoc = change.database.document(testJobA["couch_record"])

        for transition in testJobADoc["states"]:
            self.assertTrue(type(transition["timestamp"] in (types.IntType,
                                                             types.LongType)))

        assert testJobADoc["jobid"] == testJobA["id"], \
               "Error: ID parameter is incorrect."
        assert testJobADoc["name"] == testJobA["name"], \
               "Error: Name parameter is incorrect."
        assert testJobADoc["jobgroup"] == testJobA["jobgroup"], \
               "Error: Jobgroup parameter is incorrect."
        assert testJobADoc["workflow"] == testJobA["workflow"], \
               "Error: Workflow parameter is incorrect."
        assert testJobADoc["task"] == testJobA["task"], \
               "Error: Task parameter is incorrect."
        assert testJobADoc["owner"] == testJobA["owner"], \
               "Error: Owner parameter is incorrect."

        assert testJobADoc["mask"]["firstevent"] == testJobA["mask"]["FirstEvent"], \
               "Error: First event in mask is incorrect."
        assert testJobADoc["mask"]["lastevent"] == testJobA["mask"]["LastEvent"], \
               "Error: Last event in mask is incorrect."
        assert testJobADoc["mask"]["firstlumi"] == testJobA["mask"]["FirstLumi"], \
               "Error: First lumi in mask is incorrect."
        assert testJobADoc["mask"]["lastlumi"] == testJobA["mask"]["LastLumi"], \
               "Error: First lumi in mask is incorrect."
        assert testJobADoc["mask"]["firstrun"] == testJobA["mask"]["FirstRun"], \
               "Error: First run in mask is incorrect."
        assert testJobADoc["mask"]["lastevent"] == testJobA["mask"]["LastRun"], \
               "Error: First event in mask is incorrect."        

        assert len(testJobADoc["inputfiles"]) == 1, \
               "Error: Input files parameter is incorrect."
        
        testJobBDoc = change.database.document(testJobB["couch_record"])

        assert testJobBDoc["jobid"] == testJobB["id"], \
               "Error: ID parameter is incorrect."
        assert testJobBDoc["name"] == testJobB["name"], \
               "Error: Name parameter is incorrect."
        assert testJobBDoc["jobgroup"] == testJobB["jobgroup"], \
               "Error: Jobgroup parameter is incorrect."

        assert testJobBDoc["mask"]["firstevent"] == testJobB["mask"]["FirstEvent"], \
               "Error: First event in mask is incorrect."
        assert testJobBDoc["mask"]["lastevent"] == testJobB["mask"]["LastEvent"], \
               "Error: Last event in mask is incorrect."
        assert testJobBDoc["mask"]["firstlumi"] == testJobB["mask"]["FirstLumi"], \
               "Error: First lumi in mask is incorrect."
        assert testJobBDoc["mask"]["lastlumi"] == testJobB["mask"]["LastLumi"], \
               "Error: First lumi in mask is incorrect."
        assert testJobBDoc["mask"]["firstrun"] == testJobB["mask"]["FirstRun"], \
               "Error: First run in mask is incorrect."
        assert testJobBDoc["mask"]["lastevent"] == testJobB["mask"]["LastRun"], \
               "Error: First event in mask is incorrect."
        
        assert len(testJobBDoc["inputfiles"]) == 1, \
               "Error: Input files parameter is incorrect."

        changeStateDB = self.couchServer.connectDatabase(dbname = "changestate_t")
        options = {"startkey": testJobA["id"], "endkey": testJobA["id"],
                   "include_docs": True}
        results = changeStateDB.loadView("JobDump", "jobsByJobID", options)

        assert len(results["rows"]) == 1, \
               "Error: More than one job returned."

        couchJobDoc = results["rows"][0]["doc"]

        assert couchJobDoc["name"] == testJobA["name"], \
               "Error: Name is wrong"
        assert len(couchJobDoc["inputfiles"]) == 1, \
               "Error: Wrong number of input files."
                    
        return

    def testPersist(self):
        """
        _testPersist_
        
        This is the test class for function Propagate from module ChangeState
        """
        DefaultConfig.config.JobStateMachine.couchURL = os.getenv("COUCHURL")
        change = ChangeState(DefaultConfig.config, "changestate_t")

        locationAction = self.daoFactory(classname = "Locations.New")
        locationAction.execute("site1", seName = "somese.cern.ch")
        
        testWorkflow = Workflow(spec = "spec.xml", owner = "Steve",
                                name = "wf001", task = "Test")
        testWorkflow.create()
        testFileset = Fileset(name = "TestFileset")
        testFileset.create()

        for i in range(4):
            newFile = File(lfn = "File%s" % i, locations = set(["somese.cern.ch"]))
            newFile.create()
            testFileset.addFile(newFile)

        testFileset.commit()
        testSubscription = Subscription(fileset = testFileset,
                                        workflow = testWorkflow,
                                        split_algo = "FileBased")
        testSubscription.create()

        splitter = SplitterFactory()
        jobFactory = splitter(package = "WMCore.WMBS",
                              subscription = testSubscription)
        jobGroup = jobFactory(files_per_job = 1)[0]

        assert len(jobGroup.jobs) == 4, \
               "Error: Splitting should have created four jobs."

        testJobA = jobGroup.jobs[0]
        testJobB = jobGroup.jobs[1]
        testJobC = jobGroup.jobs[2]
        testJobD = jobGroup.jobs[3]

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

    def testRetryCount(self):
        """
        _testRetryCount_
        
        Verify that the retry count is incremented when we move out of the
        submitcooloff or jobcooloff state.
        """
        DefaultConfig.config.JobStateMachine.couchURL = os.getenv("COUCHURL")
        change = ChangeState(DefaultConfig.config, "changestate_t")

        locationAction = self.daoFactory(classname = "Locations.New")
        locationAction.execute("site1", seName = "somese.cern.ch")
        
        testWorkflow = Workflow(spec = "spec.xml", owner = "Steve",
                                name = "wf001", task = "Test")
        testWorkflow.create()
        testFileset = Fileset(name = "TestFileset")
        testFileset.create()

        for i in range(4):
            newFile = File(lfn = "File%s" % i, locations = set(["somese.cern.ch"]))
            newFile.create()
            testFileset.addFile(newFile)

        testFileset.commit()
        testSubscription = Subscription(fileset = testFileset,
                                        workflow = testWorkflow,
                                        split_algo = "FileBased")
        testSubscription.create()

        splitter = SplitterFactory()
        jobFactory = splitter(package = "WMCore.WMBS",
                              subscription = testSubscription)
        jobGroup = jobFactory(files_per_job = 1)[0]

        assert len(jobGroup.jobs) == 4, \
               "Error: Splitting should have created four jobs."

        testJobA = jobGroup.jobs[0]
        testJobB = jobGroup.jobs[1]
        testJobC = jobGroup.jobs[2]
        testJobD = jobGroup.jobs[3]

        change.persist([testJobA], "created", "submitcooloff")
        change.persist([testJobB], "created", "jobcooloff")
        change.persist([testJobC, testJobD], "new", "none")        

        testJobA.load()
        testJobB.load()
        testJobC.load()
        testJobD.load()

        assert testJobA["retry_count"] == 1, \
               "Error: Retry count is wrong."
        assert testJobB["retry_count"] == 1, \
               "Error: Retry count is wrong."
        assert testJobC["retry_count"] == 0, \
               "Error: Retry count is wrong."
        assert testJobD["retry_count"] == 0, \
               "Error: Retry count is wrong."        

        return    

    def testJobSerialization(self):
        """
        _testJobSerialization_

        Verify that serialization of a job works when adding a FWJR.
        """
        DefaultConfig.config.JobStateMachine.couchURL = os.getenv("COUCHURL")
        change = ChangeState(DefaultConfig.config, "changestate_t")

        locationAction = self.daoFactory(classname = "Locations.New")
        locationAction.execute("site1", seName = "somese.cern.ch")
        
        testWorkflow = Workflow(spec = "spec.xml", owner = "Steve",
                                name = "wf001", task = "Test")
        testWorkflow.create()
        testFileset = Fileset(name = "TestFileset")
        testFileset.create()

        testFile = File(lfn = "SomeLFNC", locations = set(["somese.cern.ch"]))
        testFile.create()
        testFileset.addFile(testFile)
        testFileset.commit()
        
        testSubscription = Subscription(fileset = testFileset,
                                        workflow = testWorkflow)
        testSubscription.create()

        splitter = SplitterFactory()
        jobFactory = splitter(package = "WMCore.WMBS",
                              subscription = testSubscription)
        jobGroup = jobFactory(files_per_job = 1)[0]

        assert len(jobGroup.jobs) == 1, \
               "Error: Splitting should have created one job."

        testJobA = jobGroup.jobs[0]        

        change.propagate([testJobA], 'created', 'new')
        myReport = Report()
        reportPath = os.path.join(getWMBASE(),
                                  "test/python/WMCore_t/JobStateMachine_t/Report.pkl")
        myReport.unpersist(reportPath)
        testJobA["fwjr"] = myReport

        change.propagate([testJobA], 'executing', 'created')

        options = {"startkey": testJobA["id"], "endkey": testJobA["id"],
                   "include_docs": True}
        changeStateDB = self.couchServer.connectDatabase(dbname = "changestate_t")        
        results = changeStateDB.loadView("JobDump", "fwjrsByJobID", options)

        assert len(results["rows"]) == 1, \
               "Error: Wrong number of FWJRs returned."

        fwjrDoc = results["rows"][0]["doc"]

        assert fwjrDoc["retrycount"] == 0, \
               "Error: Retry count is wrong."

        assert len(fwjrDoc["fwjr"]["steps"].keys()) == 2, \
               "Error: Wrong number of steps in FWJR."
        assert "cmsRun1" in fwjrDoc["fwjr"]["steps"].keys(), \
               "Error: cmsRun1 step is missing from FWJR."
        assert "stageOut1" in fwjrDoc["fwjr"]["steps"].keys(), \
               "Error: stageOut1 step is missing from FWJR."        

        return

    def testDashboardTransitions(self):
    	"""
        _testDashboardTransitions_

        Verify that the dashboard transitions code works correctly.
    	"""
        DefaultConfig.config.JobStateMachine.couchURL = os.getenv("COUCHURL")
        change = ChangeState(DefaultConfig.config, "changestate_t")

        locationAction = self.daoFactory(classname = "Locations.New")
        locationAction.execute("site1", seName = "somese.cern.ch")
        
        testWorkflow = Workflow(spec = "spec.xml", owner = "Steve",
                                name = "wf001", task = "Test")
        testWorkflow.create()
        testFileset = Fileset(name = "TestFileset")
        testFileset.create()
        testSubscription = Subscription(fileset = testFileset,
                                        workflow = testWorkflow,
                                        split_algo = "FileBased")
        testSubscription.create()
        
        testFileA = File(lfn = "SomeLFNA", events = 1024, size = 2048,
                         locations = set(["somese.cern.ch"]))
        testFileB = File(lfn = "SomeLFNB", events = 1025, size = 2049,
                         locations = set(["somese.cern.ch"]))
        testFileA.create()
        testFileB.create()

        testFileset.addFile(testFileA)
        testFileset.addFile(testFileB)
        testFileset.commit()

        splitter = SplitterFactory()
        jobFactory = splitter(package = "WMCore.WMBS",
                              subscription = testSubscription)
        jobGroup = jobFactory(files_per_job = 1)[0]

        assert len(jobGroup.jobs) == 2, \
               "Error: Splitting should have created two jobs."

        testJobA = jobGroup.jobs[0]
        testJobB = jobGroup.jobs[1]

        change.propagate([testJobA, testJobB], "new", "none")
        change.propagate([testJobA, testJobB], "created", "new")
        change.propagate([testJobA], "executing", "created")
        change.propagate([testJobB], "submitfailed", "created")
        change.propagate([testJobB], "submitcooloff", "submitfailed")

        transitions = change.listTransitionsForDashboard()

        self.assertEqual(len(transitions), 1,
                         "Error: Wrong number of transitions")
        self.assertEqual(transitions[0]["name"], testJobA["name"],
                         "Error: Wrong job name.")
        self.assertEqual(transitions[0]["retryCount"], 0,
                         "Error: Wrong retry count.")
        self.assertEqual(transitions[0]["newState"], "executing",
                         "Error: Wrong new state.")
        self.assertEqual(transitions[0]["oldState"], "created",
                         "Error: Wrong old state.")
        self.assertEqual(transitions[0]["requestName"], "wf001",
                         "Error: Wrong request name.")
        
        transitions = change.listTransitionsForDashboard()

        self.assertEqual(len(transitions), 0,
                         "Error: Wrong number of transitions")        

        change.propagate([testJobB], "created", "submitcooloff")
        change.propagate([testJobB], "executing", "created")
        change.propagate([testJobA, testJobB], "complete", "executing")        
        change.propagate([testJobB], "success", "complete")
        change.propagate([testJobA], "jobfailed", "complete")        

        transitions = change.listTransitionsForDashboard()
        goldenTransitions = [{"name": testJobA["name"], "retryCount": 0, "newState": "jobfailed", "oldState": "complete", "requestName": "wf001"},
                             {"name": testJobB["name"], "retryCount": 1, "newState": "executing", "oldState": "created", "requestName": "wf001"},
                             {"name": testJobB["name"], "retryCount": 1, "newState": "success", "oldState": "complete", "requestName": "wf001"}]
        self.assertEqual(transitions, goldenTransitions,
                         "Error: Wrong transitions.")
        
        return

if __name__ == "__main__":
    unittest.main()
