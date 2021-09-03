#!/usr/bin/env python
"""
Unittests for the Resubmission spec factory
"""
from __future__ import print_function

import os
import unittest

import threading

from Utils.PythonVersion import PY3
from WMCore.DAOFactory import DAOFactory
from WMCore.Database.CMSCouch import CouchServer
from WMCore.WMSpec.StdSpecs.Resubmission import ResubmissionWorkloadFactory
from WMCore.WMSpec.StdSpecs.TaskChain import TaskChainWorkloadFactory
from WMQuality.Emulators.EmulatedUnitTestCase import EmulatedUnitTestCase
from WMQuality.TestInitCouchApp import TestInitCouchApp


class ResubmissionTests(EmulatedUnitTestCase):

    def setUp(self):
        """
        _setUp_

        Initialize the database and couch.
        """
        super(ResubmissionTests, self).setUp()
        self.testInit = TestInitCouchApp(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection(destroyAllDatabase=True)
        self.testInit.setupCouch("taskchain_t", "ConfigCache")
        self.testInit.setupCouch("taskchain_t", "ReqMgr")
        self.testInit.setupCouch("resubmission_t", "ConfigCache")
        self.testInit.setSchema(customModules=["WMCore.WMBS"],
                                useDefault=False)

        couchServer = CouchServer(os.environ["COUCHURL"])
        self.configDatabase = couchServer.connectDatabase("taskchain_t")
        self.testInit.generateWorkDir()

        myThread = threading.currentThread()
        self.daoFactory = DAOFactory(package="WMCore.WMBS",
                                     logger=myThread.logger,
                                     dbinterface=myThread.dbi)
        self.listTasksByWorkflow = self.daoFactory(classname="Workflow.LoadFromName")
        self.listFilesets = self.daoFactory(classname="Fileset.List")
        self.listSubsMapping = self.daoFactory(classname="Subscriptions.ListSubsAndFilesetsFromWorkflow")
        if PY3:
            self.assertItemsEqual = self.assertCountEqual
        return

    def tearDown(self):
        """
        _tearDown_

        Clear out the database.
        """
        self.testInit.clearDatabase()
        self.testInit.delWorkDir()
        super(ResubmissionTests, self).tearDown()

    def testStandardACDC(self):
        """
        Creates a standard ACDC workflow with no parameter updates
        """
        testArguments = TaskChainWorkloadFactory.getTestArguments()
        # use a workflow existent in cmsweb-testbed, not good, but better than nothing
        acdcArgs = {"ACDCDatabase": "acdcserver",
                    "ACDCServer": self.testInit.couchUrl,
                    "CouchURL": "https://cmsweb-testbed.cern.ch/couchdb",
                    "InitialTaskPath": "/amaltaro_TaskChain_LumiMask_multiRun_HG2108_Val_210731_182010_1852/HLTD",
                    "OriginalRequestName": "amaltaro_TaskChain_LumiMask_multiRun_HG2108_Val_210731_182010_1852",
                    "RequestString": "ACDC_UnitTest_TaskChain_LumiMask_multiRun",
                    "RequestType": "Resubmission",
                    "OriginalRequestType": "TaskChain"
                    }
        testArguments.update(acdcArgs)
        factory = ResubmissionWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("TaskChain_ACDC",
                                                           testArguments, acdcArgs)

        # now we can validate the new workload object
        for topTask in testWorkload.taskIterator():
            for taskObj in topTask.taskIterator():
                perfParams = taskObj.jobSplittingParameters()['performance']
                print("Task: {}, type: {}, perf: {}".format(taskObj.name(), taskObj.taskType(), perfParams))
                if taskObj.taskType() in ('Production', 'Processing'):
                    for step in ('cmsRun1', 'stageOut1', 'logArch1'):
                        stepHelper = taskObj.getStepHelper(step)
                        self.assertEqual(stepHelper.getNumberOfCores(), 1)
                        self.assertEqual(stepHelper.getNumberOfStreams(), 0)
                    self.assertEqual(perfParams['memoryRequirement'], 2400.0)
                    self.assertEqual(perfParams['timePerEvent'], 0.8)
                elif taskObj.taskType() == 'LogCollect':
                    stepHelper = taskObj.getStepHelper('logCollect1')
                    self.assertEqual(stepHelper.getNumberOfCores(), 1)
                    self.assertEqual(stepHelper.getNumberOfStreams(), 0)
                    self.assertEqual(perfParams, {})
                elif taskObj.taskType() == 'Cleanup':
                    for step in taskObj.listAllStepNames():
                        stepHelper = taskObj.getStepHelper(step)
                        self.assertEqual(stepHelper.getNumberOfCores(), 1)
                        self.assertEqual(stepHelper.getNumberOfStreams(), 0)
                    self.assertEqual(perfParams, {})

    def testCustomACDC(self):
        """
        Creates a custom ACDC workflow overwriting some parameters
        """
        testArguments = TaskChainWorkloadFactory.getTestArguments()
        # use a workflow existent in cmsweb-testbed, not good, but better than nothing
        acdcArgs = {"ACDCDatabase": "acdcserver",
                    "ACDCServer": self.testInit.couchUrl,
                    "CouchURL": "https://cmsweb-testbed.cern.ch/couchdb",
                    "InitialTaskPath": "/amaltaro_TaskChain_LumiMask_multiRun_HG2108_Val_210731_182010_1852/HLTD",
                    "Memory": {"HLTD": 1950, "RECODreHLT": 2950.0},
                    "Multicore": {"HLTD": 11, "RECODreHLT": 12},
                    "EventStreams": {"HLTD": 21, "RECODreHLT": 22},
                    "TimePerEvent": {"HLTD": 1.5, "RECODreHLT": 2.5},
                    "OriginalRequestName": "amaltaro_TaskChain_LumiMask_multiRun_HG2108_Val_210731_182010_1852",
                    "RequestString": "ACDC_UnitTest_TaskChain_LumiMask_multiRun",
                    "RequestType": "Resubmission",
                    "OriginalRequestType": "TaskChain"
                    }
        testArguments.update(acdcArgs)
        factory = ResubmissionWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("TaskChain_ACDC",
                                                           testArguments, acdcArgs)

        # now we can validate the new workload object
        for topTask in testWorkload.taskIterator():
            for taskObj in topTask.taskIterator():
                taskName = taskObj.name()
                perfParams = taskObj.jobSplittingParameters()['performance']
                print("Task: {}, type: {}, perf: {}".format(taskName, taskObj.taskType(), perfParams))
                if taskObj.taskType() in ('Production', 'Processing'):
                    stepHelper = taskObj.getStepHelper('cmsRun1')
                    self.assertEqual(stepHelper.getNumberOfCores(), acdcArgs['Multicore'][taskName])
                    self.assertEqual(stepHelper.getNumberOfStreams(), acdcArgs['EventStreams'][taskName])
                    for step in ('stageOut1', 'logArch1'):
                        stepHelper = taskObj.getStepHelper(step)
                        self.assertEqual(stepHelper.getNumberOfCores(), 1)
                        self.assertEqual(stepHelper.getNumberOfStreams(), 0)
                    self.assertEqual(perfParams['memoryRequirement'], acdcArgs['Memory'][taskName])
                    self.assertEqual(perfParams['timePerEvent'], acdcArgs['TimePerEvent'][taskName])
                elif taskObj.taskType() == 'LogCollect':
                    stepHelper = taskObj.getStepHelper('logCollect1')
                    self.assertEqual(stepHelper.getNumberOfCores(), 1)
                    self.assertEqual(stepHelper.getNumberOfStreams(), 0)
                    self.assertEqual(perfParams, {})
                elif taskObj.taskType() == 'Cleanup':
                    for step in taskObj.listAllStepNames():
                        stepHelper = taskObj.getStepHelper(step)
                        self.assertEqual(stepHelper.getNumberOfCores(), 1)
                        self.assertEqual(stepHelper.getNumberOfStreams(), 0)
                    self.assertEqual(perfParams, {})

    def testCustomSimpleACDC(self):
        """
        Creates a custom ACDC workflow overwriting some parameters
        """
        testArguments = TaskChainWorkloadFactory.getTestArguments()
        # use a workflow existent in cmsweb-testbed, not good, but better than nothing
        acdcArgs = {"ACDCDatabase": "acdcserver",
                    "ACDCServer": self.testInit.couchUrl,
                    "CouchURL": "https://cmsweb-testbed.cern.ch/couchdb",
                    "InitialTaskPath": "/amaltaro_TaskChain_LumiMask_multiRun_HG2108_Val_210731_182010_1852/HLTD",
                    "Memory": 9999,
                    "Multicore": 22,
                    "EventStreams": 11,
                    "TimePerEvent": 2.5,
                    "OriginalRequestName": "amaltaro_TaskChain_LumiMask_multiRun_HG2108_Val_210731_182010_1852",
                    "RequestString": "ACDC_UnitTest_TaskChain_LumiMask_multiRun",
                    "RequestType": "Resubmission",
                    "OriginalRequestType": "TaskChain"
                    }
        testArguments.update(acdcArgs)
        factory = ResubmissionWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("TaskChain_ACDC",
                                                           testArguments, acdcArgs)

        # now we can validate the new workload object
        for topTask in testWorkload.taskIterator():
            for taskObj in topTask.taskIterator():
                taskName = taskObj.name()
                perfParams = taskObj.jobSplittingParameters()['performance']
                print("Task: {}, type: {}, perf: {}".format(taskName, taskObj.taskType(), perfParams))
                if taskObj.taskType() in ('Production', 'Processing'):
                    stepHelper = taskObj.getStepHelper('cmsRun1')
                    self.assertEqual(stepHelper.getNumberOfCores(), acdcArgs['Multicore'])
                    self.assertEqual(stepHelper.getNumberOfStreams(), acdcArgs['EventStreams'])
                    for step in ('stageOut1', 'logArch1'):
                        stepHelper = taskObj.getStepHelper(step)
                        self.assertEqual(stepHelper.getNumberOfCores(), 1)
                        self.assertEqual(stepHelper.getNumberOfStreams(), 0)
                    self.assertEqual(perfParams['memoryRequirement'], acdcArgs['Memory'])
                    self.assertEqual(perfParams['timePerEvent'], acdcArgs['TimePerEvent'])
                elif taskObj.taskType() == 'LogCollect':
                    stepHelper = taskObj.getStepHelper('logCollect1')
                    self.assertEqual(stepHelper.getNumberOfCores(), 1)
                    self.assertEqual(stepHelper.getNumberOfStreams(), 0)
                    self.assertEqual(perfParams, {})
                elif taskObj.taskType() == 'Cleanup':
                    for step in taskObj.listAllStepNames():
                        stepHelper = taskObj.getStepHelper(step)
                        self.assertEqual(stepHelper.getNumberOfCores(), 1)
                        self.assertEqual(stepHelper.getNumberOfStreams(), 0)
                    self.assertEqual(perfParams, {})


if __name__ == '__main__':
    unittest.main()
