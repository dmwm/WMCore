#!/usr/bin/env python
"""
_MonteCarloFromGEN_t_

Unit tests for the MonteCarloFromGEN workflow.
"""
from __future__ import print_function

import os
import threading
import unittest

from WMCore.DAOFactory import DAOFactory
from WMCore.Database.CMSCouch import CouchServer, Document
from WMCore.WMSpec.StdSpecs.MonteCarloFromGEN import MonteCarloFromGENWorkloadFactory
from WMCore.WorkQueue.WMBSHelper import WMBSHelper
from WMQuality.Emulators.EmulatedUnitTestCase import EmulatedUnitTestCase
from WMQuality.TestInitCouchApp import TestInitCouchApp


class MonteCarloFromGENTest(EmulatedUnitTestCase):
    def setUp(self):
        """
        _setUp_

        Initialize the database.
        """
        super(MonteCarloFromGENTest, self).setUp()
        self.testInit = TestInitCouchApp(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setupCouch("mclhe_t", "ConfigCache")
        self.testInit.setSchema(customModules=["WMCore.WMBS"],
                                useDefault=False)

        couchServer = CouchServer(os.environ["COUCHURL"])
        self.configDatabase = couchServer.connectDatabase("mclhe_t")
        self.testDir = self.testInit.generateWorkDir()

        myThread = threading.currentThread()
        self.daoFactory = DAOFactory(package="WMCore.WMBS",
                                     logger=myThread.logger,
                                     dbinterface=myThread.dbi)
        self.listTasksByWorkflow = self.daoFactory(classname="Workflow.LoadFromName")
        self.listFilesets = self.daoFactory(classname="Fileset.List")
        self.listSubsMapping = self.daoFactory(classname="Subscriptions.ListSubsAndFilesetsFromWorkflow")

        return

    def tearDown(self):
        """
        _tearDown_

        Clear out the database.
        """
        self.testInit.clearDatabase()
        self.testInit.tearDownCouch()
        self.testInit.delWorkDir()
        super(MonteCarloFromGENTest, self).tearDown()
        return

    def injectConfig(self):
        """
        _injectConfig_

        Create a bogus config cache document and inject it into couch.  Return
        the ID of the document.
        """
        newConfig = Document()
        newConfig["info"] = None
        newConfig["config"] = None
        newConfig["md5hash"] = "eb1c38cf50e14cf9fc31278a5c8e580f"
        newConfig["pset_hash"] = "7c856ad35f9f544839d8525ca10259a7"
        newConfig["owner"] = {"group": "cmsdataops", "user": "sfoulkes"}
        newConfig["pset_tweak_details"] = {"process": {"outputModules_": ["RECOoutput", "ALCARECOoutput"],
                                                       "RECOoutput": {"dataset": {"filterName": "FilterRECO",
                                                                                  "dataTier": "RECO"}},
                                                       "ALCARECOoutput": {
                                                           "dataset": {"filterName": "FilterALCARECO",
                                                                       "dataTier": "ALCARECO"}}}}
        result = self.configDatabase.commitOne(newConfig)
        return result[0]["id"]

    def testMCFromGENWithPileup(self):
        """
        _testMonteCarloFromGEN_

        Create a MonteCarloFromGEN workflow and verify it installs into WMBS
        correctly.
        """
        arguments = MonteCarloFromGENWorkloadFactory.getTestArguments()
        arguments["ConfigCacheID"] = self.injectConfig()
        arguments["CouchDBName"] = "mclhe_t"
        arguments["PrimaryDataset"] = "WaitThisIsNotMinimumBias"

        # Add pileup inputs
        arguments["MCPileup"] = "/HighPileUp/Run2011A-v1/RAW"
        arguments["DataPileup"] = "/Cosmics/ComissioningHI-v1/RAW"
        arguments["DeterministicPileup"] = True

        factory = MonteCarloFromGENWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", arguments)

        productionTask = testWorkload.getTaskByPath('/TestWorkload/MonteCarloFromGEN')
        cmsRunStep = productionTask.getStep("cmsRun1").getTypeHelper()
        pileupData = cmsRunStep.getPileup()
        self.assertEqual(pileupData.mc.dataset, [arguments["MCPileup"]])
        self.assertEqual(pileupData.data.dataset, [arguments["DataPileup"]])

        splitting = productionTask.jobSplittingParameters()
        self.assertTrue(splitting["deterministicPileup"])

        outputDatasets = testWorkload.listOutputDatasets()
        self.assertEqual(len(outputDatasets), 2)
        self.assertTrue("/WaitThisIsNotMinimumBias/FAKE-FilterRECO-FAKE-v1/RECO" in outputDatasets)
        self.assertTrue("/WaitThisIsNotMinimumBias/FAKE-FilterALCARECO-FAKE-v1/ALCARECO" in outputDatasets)

    def testMemCoresSettings(self):
        """
        _testMemCoresSettings_

        Make sure the multicore and memory setings are properly propagated to
        all tasks and steps.
        """
        defaultArguments = MonteCarloFromGENWorkloadFactory.getTestArguments()
        defaultArguments["ConfigCacheID"] = self.injectConfig()
        defaultArguments["CouchDBName"] = "mclhe_t"
        defaultArguments["PrimaryDataset"] = "WaitThisIsNotMinimumBias"

        factory = MonteCarloFromGENWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", defaultArguments)

        # test default values
        taskObj = testWorkload.getTask('MonteCarloFromGEN')
        for step in ('cmsRun1', 'stageOut1', 'logArch1'):
            stepHelper = taskObj.getStepHelper(step)
            self.assertEqual(stepHelper.getNumberOfCores(), 1)
            self.assertEqual(stepHelper.getNumberOfStreams(), 0)
        # then test Memory requirements
        perfParams = taskObj.jobSplittingParameters()['performance']
        self.assertEqual(perfParams['memoryRequirement'], 2300.0)

        # now test case where args are provided
        defaultArguments["Multicore"] = 6
        defaultArguments["Memory"] = 4600.0
        defaultArguments["EventStreams"] = 3
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", defaultArguments)
        taskObj = testWorkload.getTask('MonteCarloFromGEN')
        for step in ('cmsRun1', 'stageOut1', 'logArch1'):
            stepHelper = taskObj.getStepHelper(step)
            if step == 'cmsRun1':
                self.assertEqual(stepHelper.getNumberOfCores(), defaultArguments["Multicore"])
                self.assertEqual(stepHelper.getNumberOfStreams(), defaultArguments["EventStreams"])
            else:
                self.assertEqual(stepHelper.getNumberOfCores(), 1)
                self.assertEqual(stepHelper.getNumberOfStreams(), 0)
        # then test Memory requirements
        perfParams = taskObj.jobSplittingParameters()['performance']
        self.assertEqual(perfParams['memoryRequirement'], defaultArguments["Memory"])

        return

    def testFilesets(self):
        """
        Test workflow tasks, filesets and subscriptions creation
        """
        # expected tasks, filesets, subscriptions, etc
        expOutTasks = ['/TestWorkload/MonteCarloFromGEN',
                       '/TestWorkload/MonteCarloFromGEN/MonteCarloFromGENMergeRECOoutput',
                       '/TestWorkload/MonteCarloFromGEN/MonteCarloFromGENMergeALCARECOoutput']
        expWfTasks = ['/TestWorkload/MonteCarloFromGEN',
                      '/TestWorkload/MonteCarloFromGEN/LogCollect',
                      '/TestWorkload/MonteCarloFromGEN/MonteCarloFromGENCleanupUnmergedALCARECOoutput',
                      '/TestWorkload/MonteCarloFromGEN/MonteCarloFromGENCleanupUnmergedRECOoutput',
                      '/TestWorkload/MonteCarloFromGEN/MonteCarloFromGENMergeALCARECOoutput',
                      '/TestWorkload/MonteCarloFromGEN/MonteCarloFromGENMergeALCARECOoutput/MonteCarloFromGENALCARECOoutputMergeLogCollect',
                      '/TestWorkload/MonteCarloFromGEN/MonteCarloFromGENMergeRECOoutput',
                      '/TestWorkload/MonteCarloFromGEN/MonteCarloFromGENMergeRECOoutput/MonteCarloFromGENRECOoutputMergeLogCollect']
        expFsets = ['TestWorkload-MonteCarloFromGEN-/MinimumBias/ComissioningHI-v1/RAW',
                    '/TestWorkload/MonteCarloFromGEN/MonteCarloFromGENMergeALCARECOoutput/merged-logArchive',
                    '/TestWorkload/MonteCarloFromGEN/MonteCarloFromGENMergeALCARECOoutput/merged-MergedALCARECO',
                    '/TestWorkload/MonteCarloFromGEN/MonteCarloFromGENMergeRECOoutput/merged-logArchive',
                    '/TestWorkload/MonteCarloFromGEN/MonteCarloFromGENMergeRECOoutput/merged-MergedRECO',
                    '/TestWorkload/MonteCarloFromGEN/unmerged-ALCARECOoutputALCARECO',
                    '/TestWorkload/MonteCarloFromGEN/unmerged-RECOoutputRECO',
                    '/TestWorkload/MonteCarloFromGEN/unmerged-logArchive']
        subMaps = [(6,
                    '/TestWorkload/MonteCarloFromGEN/MonteCarloFromGENMergeALCARECOoutput/merged-logArchive',
                    '/TestWorkload/MonteCarloFromGEN/MonteCarloFromGENMergeALCARECOoutput/MonteCarloFromGENALCARECOoutputMergeLogCollect',
                    'MinFileBased',
                    'LogCollect'),
                   (3,
                    '/TestWorkload/MonteCarloFromGEN/MonteCarloFromGENMergeRECOoutput/merged-logArchive',
                    '/TestWorkload/MonteCarloFromGEN/MonteCarloFromGENMergeRECOoutput/MonteCarloFromGENRECOoutputMergeLogCollect',
                    'MinFileBased',
                    'LogCollect'),
                   (7,
                    '/TestWorkload/MonteCarloFromGEN/unmerged-ALCARECOoutputALCARECO',
                    '/TestWorkload/MonteCarloFromGEN/MonteCarloFromGENCleanupUnmergedALCARECOoutput',
                    'SiblingProcessingBased',
                    'Cleanup'),
                   (5,
                    '/TestWorkload/MonteCarloFromGEN/unmerged-ALCARECOoutputALCARECO',
                    '/TestWorkload/MonteCarloFromGEN/MonteCarloFromGENMergeALCARECOoutput',
                    'ParentlessMergeBySize',
                    'Merge'),
                   (8,
                    '/TestWorkload/MonteCarloFromGEN/unmerged-logArchive',
                    '/TestWorkload/MonteCarloFromGEN/LogCollect',
                    'MinFileBased',
                    'LogCollect'),
                   (4,
                    '/TestWorkload/MonteCarloFromGEN/unmerged-RECOoutputRECO',
                    '/TestWorkload/MonteCarloFromGEN/MonteCarloFromGENCleanupUnmergedRECOoutput',
                    'SiblingProcessingBased',
                    'Cleanup'),
                   (2,
                    '/TestWorkload/MonteCarloFromGEN/unmerged-RECOoutputRECO',
                    '/TestWorkload/MonteCarloFromGEN/MonteCarloFromGENMergeRECOoutput',
                    'ParentlessMergeBySize',
                    'Merge'),
                   (1,
                    'TestWorkload-MonteCarloFromGEN-/MinimumBias/ComissioningHI-v1/RAW',
                    '/TestWorkload/MonteCarloFromGEN',
                    'EventAwareLumiBased',
                    'Production')]

        testArguments = MonteCarloFromGENWorkloadFactory.getTestArguments()
        testArguments["ConfigCacheID"] = self.injectConfig()
        testArguments["CouchDBName"] = "mclhe_t"
        testArguments["PrimaryDataset"] = "WaitThisIsNotMinimumBias"

        factory = MonteCarloFromGENWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", testArguments)

        testWMBSHelper = WMBSHelper(testWorkload, "MonteCarloFromGEN", blockName=testArguments['InputDataset'],
                                    cachepath=self.testInit.testDir)
        testWMBSHelper.createTopLevelFileset()
        testWMBSHelper._createSubscriptionsInWMBS(testWMBSHelper.topLevelTask, testWMBSHelper.topLevelFileset)

        self.assertItemsEqual(testWorkload.listOutputProducingTasks(), expOutTasks)

        workflows = self.listTasksByWorkflow.execute(workflow="TestWorkload")
        self.assertItemsEqual([item['task'] for item in workflows], expWfTasks)

        # returns a tuple of id, name, open and last_update
        filesets = self.listFilesets.execute()
        self.assertItemsEqual([item[1] for item in filesets], expFsets)

        subscriptions = self.listSubsMapping.execute(workflow="TestWorkload", returnTuple=True)
        self.assertItemsEqual(subscriptions, subMaps)


if __name__ == '__main__':
    unittest.main()
