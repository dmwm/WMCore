#!/usr/bin/env python

"""
_DQMHarvest_t_
"""

import json
import os
import unittest

from WMCore.WMSpec.StdSpecs.DQMHarvest import DQMHarvestWorkloadFactory
from WMQuality.TestInitCouchApp import TestInitCouchApp
from WMCore.Database.CMSCouch import CouchServer, Document
from WMCore.WMSpec.WMSpecErrors import WMSpecFactoryException


def getTestFile(partialPath):
    """
    Returns the absolute path for the test json file
    """
    normPath = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', '..', '..', '..'))
    return os.path.join(normPath, partialPath)


class DQMHarvestTests(unittest.TestCase):
    """
    _DQMHarvestTests_

    Tests the DQMHarvest spec file
    """

    def setUp(self):
        """
        _setUp_

        Initialize the database and couch.
        """
        self.testInit = TestInitCouchApp(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setupCouch("dqmharvest_t", "ConfigCache")
        self.testInit.setSchema(customModules=["WMCore.WMBS"], useDefault=False)

        couchServer = CouchServer(os.environ["COUCHURL"])
        self.configDatabase = couchServer.connectDatabase("dqmharvest_t")
        self.testInit.generateWorkDir()
        self.workload = None
        self.jsonTemplate = getTestFile('data/ReqMgr/requests/DMWM/DQMHarvesting.json')

        return

    def tearDown(self):
        """
        _tearDown_

        Clear out the database.
        """
        self.testInit.tearDownCouch()
        self.testInit.clearDatabase()
        self.testInit.delWorkDir()
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
        # Read in the request
        request = json.load(open(self.jsonTemplate))
        testArguments.update(request['createRequest'])
        testArguments.update({
            "CouchURL": os.environ["COUCHURL"],
            "ConfigCacheUrl": os.environ["COUCHURL"],
            "CouchDBName": "dqmharvest_t",
            "DQMConfigCacheID": self.injectDQMHarvestConfig()
        })
        testArguments.pop("ConfigCacheID", None)

        factory = DQMHarvestWorkloadFactory()
        testWorkload = factory.factoryWorkloadConstruction("TestWorkload", testArguments)

        # test workload properties
        self.assertEqual(testWorkload.getDashboardActivity(), "harvesting")
        self.assertEqual(testWorkload.getCampaign(), "Campaign-OVERRIDE-ME")
        self.assertEqual(testWorkload.getAcquisitionEra(), "CMSSW_7_3_1_patch1")
        self.assertEqual(testWorkload.getProcessingString(), "GR_R_73_V0A_TEST_RelVal_jetHT2012c")
        self.assertEqual(testWorkload.getProcessingVersion(), 1)
        self.assertFalse(testWorkload.getPrepID(), "PrepId does not match")
        self.assertEqual(testWorkload.getCMSSWVersions(), ['CMSSW_7_3_1_patch1'])

        # test workload attributes
        self.assertEqual(testWorkload.processingString, "GR_R_73_V0A_TEST_RelVal_jetHT2012c")
        self.assertEqual(testWorkload.acquisitionEra, "CMSSW_7_3_1_patch1")
        self.assertEqual(testWorkload.processingVersion, 1)
        self.assertEqual(sorted(testWorkload.lumiList.keys()), ['139788', '139790', '144011'])
        self.assertEqual(sorted(testWorkload.lumiList.values()),
                         [[[5, 10], [15, 20], [25, 30]], [[25, 75],
                                                          [125, 175], [275, 325]], [[50, 100], [110, 125]]])
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
        self.assertEqual(sorted(task.inputRunWhitelist()),
                         [138923, 138924, 138934, 138937, 139788, 139789,
                          139790, 144011, 144083, 144084, 144086])

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
        # Read in the request
        request = json.load(open(self.jsonTemplate))
        testArguments.update(request['createRequest'])
        testArguments.update({
            "CouchURL": os.environ["COUCHURL"],
            "ConfigCacheUrl": os.environ["COUCHURL"],
            "CouchDBName": "dqmharvest_t",
            "ConfigCacheID": self.injectDQMHarvestConfig()
        })
        testArguments.pop("DQMConfigCacheID", None)

        factory = DQMHarvestWorkloadFactory()
        self.assertRaises(WMSpecFactoryException, factory.validateSchema, testArguments)
        return


if __name__ == '__main__':
    unittest.main()
