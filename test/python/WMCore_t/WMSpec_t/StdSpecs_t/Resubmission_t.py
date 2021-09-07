#!/usr/bin/env python
"""
Unittests for the Resubmission spec factory
"""
from __future__ import print_function

import os
import unittest
from copy import deepcopy

import threading

from Utils.PythonVersion import PY3
from WMCore.DAOFactory import DAOFactory
from WMCore.Database.CMSCouch import CouchServer, Document
from WMCore.WMSpec.StdSpecs.ReReco import ReRecoWorkloadFactory
from WMCore.WMSpec.StdSpecs.Resubmission import ResubmissionWorkloadFactory
from WMCore.WMSpec.StdSpecs.StepChain import StepChainWorkloadFactory
from WMCore.WMSpec.StdSpecs.TaskChain import TaskChainWorkloadFactory
from WMCore.WMSpec.WMSpecErrors import WMSpecFactoryException
from WMQuality.Emulators.EmulatedUnitTestCase import EmulatedUnitTestCase
from WMQuality.TestInitCouchApp import TestInitCouchApp


TASK2 = {'ConfigCacheID': 'FAKE',
         'GlobalTag': 'GT_DP_V1',
         'TaskName': 'RECODreHLT',
         'TimePerEvent': 123,
         'Memory': 2200,
         'Multicore': 22,
         'EventStreams': 2}

STEP2 = {'ConfigCacheID': 'FAKE',
         'GlobalTag': 'GT_DP_V1',
         'StepName': 'RECODreHLT',
         'Multicore': 22,
         'EventStreams': 2}


class ResubmissionTests(EmulatedUnitTestCase):
    """
    Unit tests for the Resubmission spec

    NOTE: test performance/resource values are actually overwritten by the
    original/parent workflow which we are creating an ACDC for
    """

    def setUp(self):
        """
        Initialize the database and couch.
        """
        super(ResubmissionTests, self).setUp()
        self.testInit = TestInitCouchApp(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection(destroyAllDatabase=True)
        self.testInit.setupCouch("resubmission_t", "ReqMgr")
        self.testInit.setupCouch("resubmission_t", "ConfigCache")
        self.testInit.setSchema(customModules=["WMCore.WMBS"],
                                useDefault=False)

        couchServer = CouchServer(os.environ["COUCHURL"])
        self.configDatabase = couchServer.connectDatabase("resubmission_t")
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
        Clear out the database.
        """
        self.testInit.clearDatabase()
        self.testInit.delWorkDir()
        super(ResubmissionTests, self).tearDown()

    def getDefaultACDCParams(self, origType):
        """
        Returns a default dictionary to be used in the Resubmission
        workflow creation.
        :param origType: spec type for the parent workflow (one being ACDC'ed)
        :return: a dictionary
        """
        defaultArgs = {"ACDCDatabase": "acdcserver",
                       "ACDCServer": self.testInit.couchUrl,
                       "CouchURL": "https://cmsweb-testbed.cern.ch/couchdb",
                       "RequestType": "Resubmission",
                       "OriginalRequestType": origType}
        return defaultArgs

    def testStandardTaskChainACDC(self):
        """
        Creates a standard ACDC for a TaskChain workflow with no parameter updates
        """
        testArguments = TaskChainWorkloadFactory.getTestArguments()
        # use a workflow existent in cmsweb-testbed, not good, but better than nothing
        acdcArgs = self.getDefaultACDCParams("TaskChain")
        acdcArgs.update({"InitialTaskPath": "/amaltaro_TaskChain_LumiMask_multiRun_HG2108_Val_210731_182010_1852/HLTD",
                         "OriginalRequestName": "amaltaro_TaskChain_LumiMask_multiRun_HG2108_Val_210731_182010_1852",
                         "RequestString": "ACDC_UnitTest_TaskChain_LumiMask_multiRun"})
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
                else:
                    for step in taskObj.listAllStepNames():
                        stepHelper = taskObj.getStepHelper(step)
                        self.assertEqual(stepHelper.getNumberOfCores(), 1)
                        self.assertEqual(stepHelper.getNumberOfStreams(), 0)
                    self.assertEqual(perfParams, {})

        # and now we validate the workflow high level description (JSON)
        # top level must have the default value for Resubmission creation (None)
        for param in ("TimePerEvent", "Memory", "Multicore", "EventStreams"):
            self.assertIsNone(testArguments[param])

    def testCustomTaskChainACDC(self):
        """
        Creates a custom ACDC for a TaskChain workflow overwriting some parameters
        """
        testArguments = TaskChainWorkloadFactory.getTestArguments()
        # use a workflow existent in cmsweb-testbed, not good, but better than nothing
        acdcArgs = self.getDefaultACDCParams("TaskChain")
        acdcArgs.update({"InitialTaskPath": "/amaltaro_TaskChain_LumiMask_multiRun_HG2108_Val_210731_182010_1852/HLTD",
                         "OriginalRequestName": "amaltaro_TaskChain_LumiMask_multiRun_HG2108_Val_210731_182010_1852",
                         "RequestString": "ACDC_UnitTest_TaskChain_LumiMask_multiRun",
                         "Memory": {"HLTD": 1950, "RECODreHLT": 2950.0},
                         "Multicore": {"HLTD": 11, "RECODreHLT": 12},
                         "EventStreams": {"HLTD": 21, "RECODreHLT": 22},
                         "TimePerEvent": {"HLTD": 1.5, "RECODreHLT": 2.5}})
        testArguments["Task2"] = deepcopy(TASK2)
        testArguments["Task1"]["TaskName"] = "HLTD"
        testArguments["TaskChain"] = 2
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

        # and now we validate the workflow high level description (JSON)
        # top level must be equal to what is provided by the client
        for param in ("TimePerEvent", "Memory", "Multicore", "EventStreams"):
            self.assertItemsEqual(testArguments[param], acdcArgs[param])
            # and the task needs to be properly updated
            for taskNum in ("Task1", "Task2"):
                taskName = testArguments[taskNum]["TaskName"]
                self.assertEqual(testArguments[taskNum][param], acdcArgs[param][taskName])

    def testCustomSimpleTaskChainACDC(self):
        """
        Creates a custom ACDC for a TaskChain workflow overwriting some parameters
        """
        testArguments = TaskChainWorkloadFactory.getTestArguments()
        # use a workflow existent in cmsweb-testbed, not good, but better than nothing
        acdcArgs = self.getDefaultACDCParams("TaskChain")
        acdcArgs.update({"InitialTaskPath": "/amaltaro_TaskChain_LumiMask_multiRun_HG2108_Val_210731_182010_1852/HLTD",
                         "OriginalRequestName": "amaltaro_TaskChain_LumiMask_multiRun_HG2108_Val_210731_182010_1852",
                         "RequestString": "ACDC_UnitTest_TaskChain_LumiMask_multiRun",
                         "Memory": 9999,
                         "Multicore": 22,
                         "EventStreams": 11,
                         "TimePerEvent": 2.5})
        testArguments["Task2"] = deepcopy(TASK2)
        testArguments["Task1"]["TaskName"] = "HLTD"
        testArguments["TaskChain"] = 2
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

        # and now we validate the workflow high level description (JSON)
        # top level must be equal to what is provided by the client
        for param in ("TimePerEvent", "Memory", "Multicore", "EventStreams"):
            self.assertEqual(testArguments[param], acdcArgs[param])
            # and the task needs to be properly updated
            for taskNum in ("Task1", "Task2"):
                self.assertEqual(testArguments[taskNum][param], acdcArgs[param])

    def testStandardStepChainACDC(self):
        """
        Creates a standard ACDC for a StepChain workflow
        """
        testArguments = StepChainWorkloadFactory.getTestArguments()
        # use a workflow existent in cmsweb-testbed, not good, but better than nothing
        acdcArgs = self.getDefaultACDCParams("StepChain")
        acdcArgs.update({"InitialTaskPath": "/amaltaro_SC_PY3_PURecyc_HG2109_Val_210903_160613_9070/RecoPU_2021PU",
                         "OriginalRequestName": "amaltaro_SC_PY3_PURecyc_HG2109_Val_210903_160613_9070",
                         "RequestString": "ACDC_UnitTest_SC_PY3_PURecyc"})
        testArguments["Step2"] = deepcopy(STEP2)
        testArguments["Step2"]["StepName"] = "Nano_2021PU"
        testArguments["StepChain"] = 2
        testArguments["Multicore"] = 8
        testArguments["EventStreams"] = 2
        testArguments["Memory"] = 10000
        testArguments["TimePerEvent"] = 120
        testArguments.update(acdcArgs)
        factory = ResubmissionWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("StepChain_ACDC",
                                                           testArguments, acdcArgs)
        # now we can validate the new workload object
        for topTask in testWorkload.taskIterator():
            for taskObj in topTask.taskIterator():
                perfParams = taskObj.jobSplittingParameters()['performance']
                print("Task: {}, type: {}, perf: {}".format(taskObj.name(), taskObj.taskType(), perfParams))
                if taskObj.taskType() in ('Production', 'Processing'):
                    for step in ('cmsRun1', 'cmsRun2'):
                        stepHelper = taskObj.getStepHelper(step)
                        self.assertEqual(stepHelper.getNumberOfCores(), 8)
                        self.assertEqual(stepHelper.getNumberOfStreams(), 2)
                    for step in ('stageOut1', 'logArch1'):
                        stepHelper = taskObj.getStepHelper(step)
                        self.assertEqual(stepHelper.getNumberOfCores(), 1)
                        self.assertEqual(stepHelper.getNumberOfStreams(), 0)
                    self.assertEqual(perfParams['memoryRequirement'], 10000)
                    self.assertEqual(perfParams['timePerEvent'], 120)
                else:
                    for step in taskObj.listAllStepNames():
                        stepHelper = taskObj.getStepHelper(step)
                        self.assertEqual(stepHelper.getNumberOfCores(), 1)
                        self.assertEqual(stepHelper.getNumberOfStreams(), 0)
                    self.assertEqual(perfParams, {})

        # and now we validate the workflow high level description (JSON)
        # first, parameters only supported at the top level dictionary
        self.assertEqual(testArguments["Memory"], 10000)
        self.assertEqual(testArguments["TimePerEvent"], 120)
        for stepNum in ("Step1", "Step2"):
            self.assertTrue("Memory" not in testArguments[stepNum])
            self.assertTrue("TimePerEvent" not in testArguments[stepNum])
        # and these are set to their default values (None), if not provided by the user
        self.assertIsNone(testArguments["Multicore"])
        self.assertIsNone(testArguments["EventStreams"])
        self.assertTrue("Multicore" not in testArguments["Step1"])
        self.assertTrue("EventStreams" not in testArguments["Step1"])
        self.assertEqual(testArguments["Step2"]["Multicore"], 22)
        self.assertEqual(testArguments["Step2"]["EventStreams"], 2)

    def testFailStepChainACDC(self):
        """
        Creates an ACDC for a StepChain workflow with disallowed parameters
        """
        testArguments = StepChainWorkloadFactory.getTestArguments()
        # use a workflow existent in cmsweb-testbed, not good, but better than nothing
        acdcArgs = self.getDefaultACDCParams("StepChain")
        acdcArgs.update({"InitialTaskPath": "/amaltaro_SC_PY3_PURecyc_HG2109_Val_210903_160613_9070/RecoPU_2021PU",
                         "OriginalRequestName": "amaltaro_SC_PY3_PURecyc_HG2109_Val_210903_160613_9070",
                         "RequestString": "ACDC_UnitTest_SC_PY3_PURecyc"})
        testArguments["Step2"] = deepcopy(STEP2)
        testArguments["Step1"]["StepName"] = "RecoPU_2021PU"
        testArguments["Step2"]["StepName"] = "Nano_2021PU"
        testArguments["StepChain"] = 2

        testArguments.update(acdcArgs)
        factory = ResubmissionWorkloadFactory()
        # Memory cannot be of dictionary type for ACDCs of StepChain
        testArguments["Memory"] = {"RecoPU_2021PU": 1950, "Nano_2021PU": 2950.0}
        with self.assertRaises(WMSpecFactoryException):
            factory.factoryWorkloadConstruction("StepChain_ACDC", testArguments, acdcArgs)

        testArguments["Memory"] = 2
        testArguments["TimePerEvent"] = {"RecoPU_2021PU": 1.5, "Nano_2021PU": 2.5}
        # Neither TimePerEvent can, for ACDCs of StepChain
        with self.assertRaises(WMSpecFactoryException):
            factory.factoryWorkloadConstruction("StepChain_ACDC", testArguments, acdcArgs)

        testArguments["TimePerEvent"] = 1.5
        # And now it should go through
        factory.factoryWorkloadConstruction("StepChain_ACDC", testArguments, acdcArgs)

    def testCustomStepChainACDC(self):
        """
        Creates a custom ACDC for a StepChain workflow overwriting some parameters
        """
        testArguments = StepChainWorkloadFactory.getTestArguments()
        # use a workflow existent in cmsweb-testbed, not good, but better than nothing
        acdcArgs = self.getDefaultACDCParams("StepChain")
        acdcArgs.update({"InitialTaskPath": "/amaltaro_SC_PY3_PURecyc_HG2109_Val_210903_160613_9070/RecoPU_2021PU",
                         "OriginalRequestName": "amaltaro_SC_PY3_PURecyc_HG2109_Val_210903_160613_9070",
                         "RequestString": "ACDC_UnitTest_SC_PY3_PURecyc",
                         "Memory": 1950,
                         "Multicore": {"RecoPU_2021PU": 11, "Nano_2021PU": 12},
                         "EventStreams": {"RecoPU_2021PU": 21, "Nano_2021PU": 22},
                         "TimePerEvent": 19.5})
        testArguments["Step2"] = deepcopy(STEP2)
        testArguments["Step1"]["StepName"] = "RecoPU_2021PU"
        testArguments["Step2"]["StepName"] = "Nano_2021PU"
        testArguments["StepChain"] = 2
        testArguments.update(acdcArgs)
        factory = ResubmissionWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("StepChain_ACDC",
                                                           testArguments, acdcArgs)
        # now we can validate the new workload object
        for topTask in testWorkload.taskIterator():
            for taskObj in topTask.taskIterator():
                perfParams = taskObj.jobSplittingParameters()['performance']
                print("Task: {}, type: {}, perf: {}".format(taskObj.name(), taskObj.taskType(), perfParams))
                if taskObj.taskType() in ('Production', 'Processing'):
                    stepHelper = taskObj.getStepHelper("cmsRun1")
                    self.assertEqual(stepHelper.getNumberOfCores(), acdcArgs["Multicore"]["RecoPU_2021PU"])
                    self.assertEqual(stepHelper.getNumberOfStreams(), acdcArgs["EventStreams"]["RecoPU_2021PU"])
                    stepHelper = taskObj.getStepHelper("cmsRun2")
                    self.assertEqual(stepHelper.getNumberOfCores(), acdcArgs["Multicore"]["Nano_2021PU"])
                    self.assertEqual(stepHelper.getNumberOfStreams(), acdcArgs["EventStreams"]["Nano_2021PU"])
                    for step in ('stageOut1', 'logArch1'):
                        stepHelper = taskObj.getStepHelper(step)
                        self.assertEqual(stepHelper.getNumberOfCores(), 1)
                        self.assertEqual(stepHelper.getNumberOfStreams(), 0)
                    self.assertEqual(perfParams['memoryRequirement'], acdcArgs["Memory"])
                    self.assertEqual(perfParams['timePerEvent'], acdcArgs["TimePerEvent"])
                elif taskObj.taskType() == "Harvesting":
                    # Harvesting task is allowed to get memory/TpE update
                    self.assertEqual(perfParams['memoryRequirement'], acdcArgs["Memory"])
                    self.assertEqual(perfParams['timePerEvent'], acdcArgs["TimePerEvent"])
                else:
                    for step in taskObj.listAllStepNames():
                        stepHelper = taskObj.getStepHelper(step)
                        self.assertEqual(stepHelper.getNumberOfCores(), 1)
                        self.assertEqual(stepHelper.getNumberOfStreams(), 0)
                    self.assertEqual(perfParams, {})

        # and now we validate the workflow high level description (JSON)
        # first, parameters only supported at the top level dictionary
        self.assertEqual(testArguments["Memory"], acdcArgs["Memory"])
        self.assertEqual(testArguments["TimePerEvent"], acdcArgs["TimePerEvent"])
        for stepNum in ("Step1", "Step2"):
            self.assertTrue("Memory" not in testArguments[stepNum])
            self.assertTrue("TimePerEvent" not in testArguments[stepNum])
            stepName = testArguments[stepNum]["StepName"]
            self.assertEqual(testArguments[stepNum]["Multicore"], acdcArgs["Multicore"][stepName])
            self.assertEqual(testArguments[stepNum]["EventStreams"], acdcArgs["EventStreams"][stepName])
        # and these are set to their default values (None), if not provided by the user
        self.assertItemsEqual(testArguments["Multicore"], acdcArgs["Multicore"])
        self.assertItemsEqual(testArguments["EventStreams"], acdcArgs["EventStreams"])

    def testCustomSimpleStepChainACDC(self):
        """
        Creates a custom ACDC for a StepChain workflow overwriting some parameters
        """
        testArguments = StepChainWorkloadFactory.getTestArguments()
        # use a workflow existent in cmsweb-testbed, not good, but better than nothing
        acdcArgs = self.getDefaultACDCParams("StepChain")
        acdcArgs.update({"InitialTaskPath": "/amaltaro_SC_PY3_PURecyc_HG2109_Val_210903_160613_9070/RecoPU_2021PU",
                         "OriginalRequestName": "amaltaro_SC_PY3_PURecyc_HG2109_Val_210903_160613_9070",
                         "RequestString": "ACDC_UnitTest_SC_PY3_PURecyc",
                         "Memory": 1950,
                         "Multicore": 11,
                         "EventStreams": 21,
                         "TimePerEvent": 19.5})
        testArguments["Step2"] = deepcopy(STEP2)
        testArguments["Step1"]["StepName"] = "RecoPU_2021PU"
        testArguments["Step2"]["StepName"] = "Nano_2021PU"
        testArguments["StepChain"] = 2
        testArguments.update(acdcArgs)
        factory = ResubmissionWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("StepChain_ACDC",
                                                           testArguments, acdcArgs)
        # now we can validate the new workload object
        for topTask in testWorkload.taskIterator():
            for taskObj in topTask.taskIterator():
                perfParams = taskObj.jobSplittingParameters()['performance']
                print("Task: {}, type: {}, perf: {}".format(taskObj.name(), taskObj.taskType(), perfParams))
                if taskObj.taskType() in ('Production', 'Processing'):
                    for step in ('cmsRun1', 'cmsRun2'):
                        stepHelper = taskObj.getStepHelper(step)
                        self.assertEqual(stepHelper.getNumberOfCores(), acdcArgs["Multicore"])
                        self.assertEqual(stepHelper.getNumberOfStreams(), acdcArgs["EventStreams"])
                    for step in ('stageOut1', 'logArch1'):
                        stepHelper = taskObj.getStepHelper(step)
                        self.assertEqual(stepHelper.getNumberOfCores(), 1)
                        self.assertEqual(stepHelper.getNumberOfStreams(), 0)
                    self.assertEqual(perfParams['memoryRequirement'], acdcArgs["Memory"])
                    self.assertEqual(perfParams['timePerEvent'], acdcArgs["TimePerEvent"])
                elif taskObj.taskType() == "Harvesting":
                    # Harvesting task is allowed to get memory/TpE update
                    self.assertEqual(perfParams['memoryRequirement'], acdcArgs["Memory"])
                    self.assertEqual(perfParams['timePerEvent'], acdcArgs["TimePerEvent"])
                else:
                    for step in taskObj.listAllStepNames():
                        stepHelper = taskObj.getStepHelper(step)
                        self.assertEqual(stepHelper.getNumberOfCores(), 1)
                        self.assertEqual(stepHelper.getNumberOfStreams(), 0)
                    self.assertEqual(perfParams, {})

        # and now we validate the workflow high level description (JSON)
        # first, parameters only supported at the top level dictionary
        self.assertEqual(testArguments["Memory"], acdcArgs["Memory"])
        self.assertEqual(testArguments["TimePerEvent"], acdcArgs["TimePerEvent"])
        for stepNum in ("Step1", "Step2"):
            self.assertTrue("Memory" not in testArguments[stepNum])
            self.assertTrue("TimePerEvent" not in testArguments[stepNum])
            self.assertEqual(testArguments[stepNum]["Multicore"], acdcArgs["Multicore"])
            self.assertEqual(testArguments[stepNum]["EventStreams"], acdcArgs["EventStreams"])
        # and these are set to their default values (None), if not provided by the user
        self.assertEqual(testArguments["Multicore"], acdcArgs["Multicore"])
        self.assertEqual(testArguments["EventStreams"], acdcArgs["EventStreams"])

    def testStandardReRecoACDC(self):
        """
        Creates a standard ACDC for a ReReco workflow
        """
        testArguments = ReRecoWorkloadFactory.getTestArguments()
        # use a workflow existent in cmsweb-testbed, not good, but better than nothing
        acdcArgs = self.getDefaultACDCParams("ReReco")
        acdcArgs.update({"InitialTaskPath": "/amaltaro_ReReco_RunBlockWhite_HG2108_Val_210731_181900_4258/DataProcessing",
                         "OriginalRequestName": "amaltaro_ReReco_RunBlockWhite_HG2108_Val_210731_181900_4258",
                         "RequestString": "ACDC_UnitTest_ReReco_RunBlockWhite"})
        testArguments["Multicore"] = 8
        testArguments["EventStreams"] = 0
        testArguments["Memory"] = 12000
        testArguments["TimePerEvent"] = 73.85
        testArguments.update(acdcArgs)
        factory = ResubmissionWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("ReReco_ACDC",
                                                           testArguments, acdcArgs)
        # now we can validate the new workload object
        for topTask in testWorkload.taskIterator():
            for taskObj in topTask.taskIterator():
                perfParams = taskObj.jobSplittingParameters()['performance']
                print("Task: {}, type: {}, perf: {}".format(taskObj.name(), taskObj.taskType(), perfParams))
                if taskObj.taskType() in ('Production', 'Processing'):
                    stepHelper = taskObj.getStepHelper("cmsRun1")
                    self.assertEqual(stepHelper.getNumberOfCores(), testArguments["Multicore"])
                    self.assertEqual(stepHelper.getNumberOfStreams(), testArguments["EventStreams"])
                    for step in ('stageOut1', 'logArch1'):
                        stepHelper = taskObj.getStepHelper(step)
                        self.assertEqual(stepHelper.getNumberOfCores(), 1)
                        self.assertEqual(stepHelper.getNumberOfStreams(), 0)
                    self.assertEqual(perfParams['memoryRequirement'], testArguments["Memory"])
                    self.assertEqual(perfParams['timePerEvent'], testArguments["TimePerEvent"])
                else:
                    for step in taskObj.listAllStepNames():
                        stepHelper = taskObj.getStepHelper(step)
                        self.assertEqual(stepHelper.getNumberOfCores(), 1)
                        self.assertEqual(stepHelper.getNumberOfStreams(), 0)
                    self.assertEqual(perfParams, {})

        # and now we validate the workflow high level description (JSON)
        # only top level parameters are allowed
        self.assertEqual(testArguments["Memory"], 12000)
        self.assertEqual(testArguments["TimePerEvent"], 73.85)
        self.assertEqual(testArguments["Multicore"], 8)
        self.assertEqual(testArguments["EventStreams"], 0)

    def testFailReRecoACDC(self):
        """
        Creates an ACDC for a ReReco workflow with disallowed parameters
        """
        testArguments = ReRecoWorkloadFactory.getTestArguments()
        # use a workflow existent in cmsweb-testbed, not good, but better than nothing
        acdcArgs = self.getDefaultACDCParams("ReReco")
        acdcArgs.update({"InitialTaskPath": "/amaltaro_ReReco_RunBlockWhite_HG2108_Val_210731_181900_4258/DataProcessing",
                         "OriginalRequestName": "amaltaro_ReReco_RunBlockWhite_HG2108_Val_210731_181900_4258",
                         "RequestString": "ACDC_UnitTest_ReReco_RunBlockWhite"})
        testArguments.update(acdcArgs)
        factory = ResubmissionWorkloadFactory()

        # Memory cannot be of dictionary type for ACDCs of StepChain
        testArguments["Memory"] = {"RecoPU_2021PU": 1950, "Nano_2021PU": 2950.0}
        with self.assertRaises(WMSpecFactoryException):
            factory.factoryWorkloadConstruction("ReReco_ACDC", testArguments, acdcArgs)

        testArguments["Memory"] = 2
        testArguments["TimePerEvent"] = {"RecoPU_2021PU": 1.5, "Nano_2021PU": 2.5}
        # Neither TimePerEvent can, for ACDCs of StepChain
        with self.assertRaises(WMSpecFactoryException):
            factory.factoryWorkloadConstruction("ReReco_ACDC", testArguments, acdcArgs)

        testArguments["TimePerEvent"] = 1.5
        # And now it should go through
        factory.factoryWorkloadConstruction("ReReco_ACDC", testArguments, acdcArgs)


if __name__ == '__main__':
    unittest.main()
