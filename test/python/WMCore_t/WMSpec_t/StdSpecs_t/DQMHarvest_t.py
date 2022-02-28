#!/usr/bin/env python

"""
_DQMHarvest_t_
"""
from __future__ import print_function

import os
import threading
import unittest

from Utils.PythonVersion import PY3

from WMCore.DAOFactory import DAOFactory
from WMCore.Database.CMSCouch import CouchServer, Document
from WMCore.WMSpec.StdSpecs.DQMHarvest import DQMHarvestWorkloadFactory
from WMCore.WMSpec.WMSpecErrors import WMSpecFactoryException
from WMCore.WorkQueue.WMBSHelper import WMBSHelper
from WMQuality.Emulators.EmulatedUnitTestCase import EmulatedUnitTestCase
from WMQuality.TestInitCouchApp import TestInitCouchApp

REQUEST = {
    "AcquisitionEra": "Run2016F",
    "CMSSWVersion": "CMSSW_8_0_20",
    "Campaign": "Campaign-OVERRIDE-ME",
    "Comments": "Harvest all 37 runs in byRun mode (separate jobs)",
    "CouchURL": os.environ["COUCHURL"],
    "ConfigCacheUrl": os.environ["COUCHURL"],
    "CouchDBName": "dqmharvest_t",
    "DQMConfigCacheID": "253c586d672c6c7a88c048d8c7b62135",
    "DQMHarvestUnit": "byRun",
    "DQMUploadUrl": "https://cmsweb-testbed.cern.ch/dqm/dev",
    "DbsUrl": "https://cmsweb-prod.cern.ch/dbs/prod/global/DBSReader",
    "GlobalTag": "80X_dataRun2_2016SeptRepro_v3",
    "InputDataset": "/NoBPTX/Run2016F-23Sep2016-v1/DQMIO",
    "Memory": 1000,
    "Multicore": 1,
    "PrepID": "TEST-Harvest-ReReco-Run2016F-v1-NoBPTX-23Sep2016-0001",
    "ProcessingString": "23Sep2016",
    "ProcessingVersion": 1,
    "RequestPriority": 999999,
    "RequestString": "RequestString-OVERRIDE-ME",
    "RequestType": "DQMHarvest",
    "Requestor": "amaltaro",
    "ScramArch": "slc6_amd64_gcc530",
    "SizePerEvent": 1600,
    "TimePerEvent": 1
}


class DQMHarvestTests(EmulatedUnitTestCase):
    """
    _DQMHarvestTests_

    Tests the DQMHarvest spec file
    """

    def setUp(self):
        """
        _setUp_

        Initialize the database and couch.
        """
        super(DQMHarvestTests, self).setUp()
        self.testInit = TestInitCouchApp(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setupCouch("dqmharvest_t", "ConfigCache")
        self.testInit.setSchema(customModules=["WMCore.WMBS"], useDefault=False)

        couchServer = CouchServer(os.environ["COUCHURL"])
        self.configDatabase = couchServer.connectDatabase("dqmharvest_t")
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
        self.testInit.tearDownCouch()
        self.testInit.clearDatabase()
        self.testInit.delWorkDir()
        super(DQMHarvestTests, self).tearDown()
        return

    def injectDQMHarvestConfig(self):
        """
        _injectDQMHarvest_

        Create a bogus config cache document for DQMHarvest and
        inject it into couch.  Return the ID of the document.
        """
        newConfig = Document()
        newConfig["info"] = None
        newConfig["config"] = None
        newConfig["md5hash"] = "eb1c38cf50e14cf9fc31278a5c8e234f"
        newConfig["pset_hash"] = "7c856ad35f9f544839d8525ca10876a7"
        newConfig["owner"] = {"group": "DATAOPS", "user": "amaltaro"}
        newConfig["pset_tweak_details"] = {"process": {"outputModules_": []}}
        result = self.configDatabase.commitOne(newConfig)
        return result[0]["id"]

    def testDQMHarvest(self):
        """
        Build a DQMHarvest workload
        """
        testArguments = DQMHarvestWorkloadFactory.getTestArguments()
        testArguments.update(REQUEST)
        testArguments.update({
            "DQMConfigCacheID": self.injectDQMHarvestConfig(),
            "LumiList": {"251643": [[1, 15], [50, 70]], "251721": [[50, 100], [110, 120]]}
        })
        testArguments.pop("ConfigCacheID", None)

        factory = DQMHarvestWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", testArguments)

        # test workload properties
        self.assertEqual(testWorkload.getDashboardActivity(), "harvesting")
        self.assertEqual(testWorkload.getCampaign(), "Campaign-OVERRIDE-ME")
        self.assertEqual(testWorkload.getAcquisitionEra(), "Run2016F")
        self.assertEqual(testWorkload.getProcessingString(), "23Sep2016")
        self.assertEqual(testWorkload.getProcessingVersion(), 1)
        self.assertEqual(testWorkload.getPrepID(), "TEST-Harvest-ReReco-Run2016F-v1-NoBPTX-23Sep2016-0001")
        self.assertEqual(testWorkload.getCMSSWVersions(), ['CMSSW_8_0_20'])
        self.assertEqual(sorted(testWorkload.getLumiList().keys()), ['251643', '251721'])
        self.assertEqual(sorted(testWorkload.getLumiList().values()), [[[1, 15], [50, 70]], [[50, 100], [110, 120]]])
        self.assertEqual(testWorkload.data.policies.start.policyName, "Dataset")

        # test workload tasks and steps
        tasks = testWorkload.listAllTaskNames()
        self.assertEqual(len(tasks), 2)
        self.assertEqual(sorted(tasks), ['EndOfRunDQMHarvest', 'EndOfRunDQMHarvestLogCollect'])

        task = testWorkload.getTask(tasks[0])
        self.assertEqual(task.name(), "EndOfRunDQMHarvest")
        self.assertEqual(task.getPathName(), "/TestWorkload/EndOfRunDQMHarvest")
        self.assertEqual(task.taskType(), "Harvesting", "Wrong task type")
        self.assertEqual(task.jobSplittingAlgorithm(), "Harvest", "Wrong job splitting algo")
        self.assertFalse(task.getTrustSitelists().get('trustlists'), "Wrong input location flag")
        self.assertFalse(task.inputRunWhitelist())

        self.assertEqual(sorted(task.listAllStepNames()), ['cmsRun1', 'logArch1', 'upload1'])
        self.assertEqual(task.getStep("cmsRun1").stepType(), "CMSSW")
        self.assertEqual(task.getStep("logArch1").stepType(), "LogArchive")
        self.assertEqual(task.getStep("upload1").stepType(), "DQMUpload")

        return

    def testDQMHarvestFailed(self):
        """
        Build a DQMHarvest workload without a DQM config doc
        """
        testArguments = DQMHarvestWorkloadFactory.getTestArguments()
        testArguments.update(REQUEST)
        testArguments.update({
            "ConfigCacheID": self.injectDQMHarvestConfig()
        })
        testArguments.pop("DQMConfigCacheID", None)

        factory = DQMHarvestWorkloadFactory()
        with self.assertRaises(WMSpecFactoryException):
            factory.factoryWorkloadConstruction("TestBadWorkload", testArguments)
        return

    def testFilesets(self):
        """
        Test workflow tasks, filesets and subscriptions creation
        """
        # expected tasks, filesets, subscriptions, etc
        expOutTasks = []
        expWfTasks = ['/TestWorkload/EndOfRunDQMHarvest',
                      '/TestWorkload/EndOfRunDQMHarvest/EndOfRunDQMHarvestLogCollect']
        expFsets = ['TestWorkload-EndOfRunDQMHarvest-/NoBPTX/Run2016F-23Sep2016-v1/DQMIO',
                    '/TestWorkload/EndOfRunDQMHarvest/unmerged-logArchive']

        subMaps = [(2, '/TestWorkload/EndOfRunDQMHarvest/unmerged-logArchive',
                    '/TestWorkload/EndOfRunDQMHarvest/EndOfRunDQMHarvestLogCollect', 'MinFileBased', 'LogCollect'),
                   (1, 'TestWorkload-EndOfRunDQMHarvest-/NoBPTX/Run2016F-23Sep2016-v1/DQMIO',
                    '/TestWorkload/EndOfRunDQMHarvest', 'Harvest', 'Harvesting')]

        testArguments = DQMHarvestWorkloadFactory.getTestArguments()
        testArguments.update(REQUEST)
        testArguments['DQMConfigCacheID'] = self.injectDQMHarvestConfig()
        testArguments.pop("ConfigCacheID", None)

        factory = DQMHarvestWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", testArguments)

        testWMBSHelper = WMBSHelper(testWorkload, "EndOfRunDQMHarvest",
                                    blockName=testArguments['InputDataset'],
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
