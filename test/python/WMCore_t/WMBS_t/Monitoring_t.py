#!/usr/bin/env python
""" 
_Monitoring_t_

Unit tests for the WMBS Monitoring DAO objects.
"""

__revision__ = "$Id: Monitoring_t.py,v 1.3 2010/01/26 21:38:34 sfoulkes Exp $"
__version__ = "$Revision: 1.3 $"

import os
import unittest
import threading

from WMCore.WMBS.File import File
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Job import Job
from WMCore.WMBS.JobGroup import JobGroup
from WMCore.WMBS.Workflow import Workflow
from WMCore.WMBS.Subscription import Subscription

from WMCore.JobStateMachine.Transitions import Transitions
from WMCore.WMBS.CreateWMBSBase import CreateWMBSBase

from WMCore.DAOFactory import DAOFactory
from WMCore.WMFactory import WMFactory
from WMQuality.TestInit import TestInit
from WMCore.Services.UUID import makeUUID

class MonitoringTest(unittest.TestCase):
    def setUp(self):
        """
        _setUp_
        
        Setup the database and logging connection.  Try to create all of the
        WMBS tables.
        """
        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules = ["WMCore.WMBS"],
                                useDefault = False)

        myThread = threading.currentThread()        
        self.daoFactory = DAOFactory(package="WMCore.WMBS",
                                     logger = myThread.logger,
                                     dbinterface = myThread.dbi)        
        return
                                                                
    def tearDown(self):
        """
        _tearDown_
        
        Drop all the WMBS tables.
        """
        self.testInit.clearDatabase()
        return

    def testListJobStates(self):
        """
        _testListJobStates_

        Verify that the ListJobStates DAO works correctly.
        """
        listJobStates = self.daoFactory(classname = "Monitoring.ListJobStates")
        jobStates = listJobStates.execute()

        transitionStates = Transitions().states()
        assert len(jobStates) == len(transitionStates), \
               "Error: Number of states don't match."

        for jobState in jobStates:
            assert jobState in transitionStates, \
                   "Error: Missing job state %s" % jobState

        return

    def testListSubTypes(self):
        """
        _testSubTypes_

        Verify that the ListSubTypes DAO works correctly.
        """
        listSubTypes = self.daoFactory(classname = "Monitoring.ListSubTypes")
        subTypes = listSubTypes.execute()

        schemaTypes = CreateWMBSBase().subTypes
        assert len(subTypes) == len(schemaTypes), \
               "Error: Number of subscription types don't match."

        for subType in subTypes:
            assert subType in schemaTypes, \
                   "Error: Missing subscription type: %s" % subType

        return    

    def testListRunningJobs(self):
        """
        _testListRunningJobs_

        Test the ListRunningJobs DAO.
        """
        testWorkflow = Workflow(spec = makeUUID(), owner = "Steve",
                                name = makeUUID(), task="Test")
        testWorkflow.create()

        testFileset = Fileset(name = "TestFileset")
        testFileset.create()

        testSubscription = Subscription(fileset = testFileset,
                                        workflow = testWorkflow,
                                        type = "Processing")
        testSubscription.create()

        testJobGroup = JobGroup(subscription = testSubscription)
        testJobGroup.create()

        testJobA = Job(name = makeUUID(), files = [])
        testJobA["couch_record"] = makeUUID()
        testJobA.create(group = testJobGroup)
        testJobA["state"] = "executing"

        testJobB = Job(name = makeUUID(), files = [])
        testJobB["couch_record"] = makeUUID()
        testJobB.create(group = testJobGroup)
        testJobB["state"] = "complete"

        testJobC = Job(name = makeUUID(), files = [])
        testJobC["couch_record"] = makeUUID()
        testJobC.create(group = testJobGroup)        
        testJobC["state"] = "new"

        changeStateAction = self.daoFactory(classname = "Jobs.ChangeState")
        changeStateAction.execute(jobs = [testJobA, testJobB, testJobC])

        runningJobsAction = self.daoFactory(classname = "Monitoring.ListRunningJobs")
        runningJobs = runningJobsAction.execute()

        assert len(runningJobs) == 2, \
               "Error: Wrong number of running jobs returned."

        for runningJob in runningJobs:
            if runningJob["job_name"] == testJobA["name"]:
                assert runningJob["state"] == testJobA["state"], \
                       "Error: Running job has wrong state."
                assert runningJob["couch_record"] == testJobA["couch_record"], \
                       "Error: Running job has wrong couch record."
            else:
                assert runningJob["job_name"] == testJobC["name"], \
                       "Error: Running job has wrong name."
                assert runningJob["state"] == testJobC["state"], \
                       "Error: Running job has wrong state."
                assert runningJob["couch_record"] == testJobC["couch_record"], \
                       "Error: Running job has wrong couch record."                

        return

    def testListWorkflowEfficiency(self):
        """
        _testListWorkflowEfficiency_

        """
        testWorkflow = Workflow(spec = makeUUID(), owner = "Steve",
                                name = makeUUID(), task="Test")
        testWorkflow.create()

        testInputFileset = Fileset(name = "TestInputFileset")
        testInputFileset.create()

        inputFileA = File(lfn = "/this/is/a/input/lfnA", size = 1024, events = 100)
        inputFileB = File(lfn = "/this/is/a/input/lfnB", size = 1024, events = 100)
        inputFileC = File(lfn = "/this/is/a/input/lfnC", size = 1024, events = 100)        
        
        testInputFileset.addFile(inputFileA)
        testInputFileset.addFile(inputFileB)
        testInputFileset.addFile(inputFileC)        
        testInputFileset.commit()

        testOutputFileset = Fileset(name = "TestOutputFileset")
        testOutputFileset.create()        

        outputFile = File(lfn = "/this/is/a/output/lfn", size = 1024, events = 50)
        testOutputFileset.addFile(outputFile)
        testOutputFileset.commit()

        testWorkflow.addOutput("output", testOutputFileset)

        testSubscription = Subscription(fileset = testInputFileset,
                                        workflow = testWorkflow,
                                        type = "Processing")
        testSubscription.create()

        efficiencyAction = self.daoFactory(classname = "Monitoring.ListWorkflowEfficiency")
        wfEfficiency = efficiencyAction.execute(subscriptionId = testSubscription["id"])

        assert len(wfEfficiency) == 1, \
               "Error: Only one output module should be returned."

        assert wfEfficiency[0]["output_files"] == 1, \
               "Error: Wrong number of output files returned."
        assert wfEfficiency[0]["output_events"] == 50, \
               "Error: Wrong number of output events returned."
        assert wfEfficiency[0]["efficiency"] == "16.67%", \
               "Error: Wrong workflow efficiency calculated."
        assert wfEfficiency[0]["input_events"] == 300, \
               "Error: Wrong number of input events returned."
        assert wfEfficiency[0]["output_module"] == "output", \
               "Error: Wrong output module returned."
        assert wfEfficiency[0]["input_files"] == 3, \
               "Error: Wrong number of input files returned."

        return
        
if __name__ == "__main__":
        unittest.main()
