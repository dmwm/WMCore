#!/usr/bin/env python
"""
Created on Oct 12, 2012

@author: dballest
"""

import os
import threading
import unittest

from WMComponent.DBS3Buffer.DBSBufferDataset import DBSBufferDataset
from WMComponent.DBS3Buffer.DBSBufferFile import DBSBufferFile
from WMComponent.DBS3Buffer.DBSBufferBlock import DBSBlock

from WMComponent.PhEDExInjector.PhEDExInjectorSubscriber import PhEDExInjectorSubscriber

from WMCore.DAOFactory import DAOFactory
from WMCore.DataStructs.Run import Run
from WMCore.Services.EmulatorSwitch import EmulatorHelper
from WMCore.Services.UUID import makeUUID
from WMCore.WMBase import getTestBase
from WMCore.WMBS.File import File
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Workflow import Workflow
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper

from WMQuality.TestInit import TestInit


class PhEDExInjectorSubscriberTest(unittest.TestCase):
    """
    _PhEDExInjectorSubscriberTest_

    Unit tests for the PhEDExInjectorSubscriber.
    Create some database inside DBSBuffer, run the subscriber algorithm
    using a PhEDEx emulator and verify that it works both in unsafe and safe mode.
    For unsafe mode there a WMBS database is also created
    """

    def setUp(self):
        """
        _setUp_

        Install the DBSBuffer schema into the database and connect to PhEDEx.
        """

        self.phedexURL = "https://bogus.cern.ch/bogus"
        self.dbsURL = "https://bogus.cern.ch/bogus"
        EmulatorHelper.setEmulators(phedex = True, dbs = True, siteDB = True)

        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()

        self.testInit.setSchema(customModules = ["WMComponent.DBS3Buffer",
                                                 "WMCore.WMBS"],
                                useDefault = False)

        self.testFilesA = []
        self.testFilesB = []
        self.testDatasetA = "/BogusPrimary/Run2012Z-PromptReco-v1/RECO"
        self.testDatasetB = "/BogusPrimary/CRUZET11-v1/RAW"

        return

    def tearDown(self):
        """
        _tearDown_

        Delete the database.
        """
        self.testInit.clearDatabase()
        EmulatorHelper.resetEmulators()

    def createConfig(self):
        """
        _createConfig_

        Create a config for the PhEDExInjector, paths to DBS and PhEDEx are dummies because
        we are using Emulators
        """
        config = self.testInit.getConfiguration()
        config.component_("DBSInterface")
        config.DBSInterface.globalDBSUrl = self.dbsURL

        config.component_("PhEDExInjector")
        config.PhEDExInjector.phedexurl = self.phedexURL
        config.PhEDExInjector.subscribeDatasets = True
        config.PhEDExInjector.group = "Saturn"
        config.PhEDExInjector.pollInterval = 30
        config.PhEDExInjector.subscribeInterval = 60

        return config

    def stuffDatabase(self):
        """
        _stuffDatabase_

        Fill the dbsbuffer with some files and blocks.  We'll insert a total
        of 5 files spanning two blocks.  There will be a total of two datasets
        inserted into the database.

        All files will be already in GLOBAL and in_phedex
        """
        myThread = threading.currentThread()

        buffer3Factory = DAOFactory(package = "WMComponent.DBS3Buffer",
                                   logger = myThread.logger,
                                   dbinterface = myThread.dbi)
        insertWorkflow = buffer3Factory(classname = "InsertWorkflow")
        insertWorkflow.execute("BogusRequestA", "BogusTask",
                               0, 0, 0, 0)
        insertWorkflow.execute("BogusRequestB", "BogusTask",
                               0, 0, 0, 0)

        checksums = {"adler32": "1234", "cksum": "5678"}
        testFileA = DBSBufferFile(lfn = makeUUID(), size = 1024, events = 10,
                                  checksums = checksums,
                                  locations = set(["srm-cms.cern.ch"]))
        testFileA.setAlgorithm(appName = "cmsRun", appVer = "CMSSW_2_1_8",
                               appFam = "RECO", psetHash = "GIBBERISH",
                               configContent = "MOREGIBBERISH")
        testFileA.setDatasetPath(self.testDatasetA)
        testFileA.addRun(Run(2, *[45]))
        testFileA.create()

        testFileB = DBSBufferFile(lfn = makeUUID(), size = 1024, events = 10,
                                  checksums = checksums,
                                  locations = set(["srm-cms.cern.ch"]))
        testFileB.setAlgorithm(appName = "cmsRun", appVer = "CMSSW_2_1_8",
                               appFam = "RECO", psetHash = "GIBBERISH",
                               configContent = "MOREGIBBERISH")
        testFileB.setDatasetPath(self.testDatasetA)
        testFileB.addRun(Run(2, *[45]))
        testFileB.create()

        testFileC = DBSBufferFile(lfn = makeUUID(), size = 1024, events = 10,
                                  checksums = checksums,
                                  locations = set(["srm-cms.cern.ch"]))
        testFileC.setAlgorithm(appName = "cmsRun", appVer = "CMSSW_2_1_8",
                               appFam = "RECO", psetHash = "GIBBERISH",
                               configContent = "MOREGIBBERISH")
        testFileC.setDatasetPath(self.testDatasetA)
        testFileC.addRun(Run(2, *[45]))
        testFileC.create()

        self.testFilesA.append(testFileA)
        self.testFilesA.append(testFileB)
        self.testFilesA.append(testFileC)

        testFileD = DBSBufferFile(lfn = makeUUID(), size = 1024, events = 10,
                                  checksums = checksums,
                                  locations = set(["srm-cms.cern.ch"]))
        testFileD.setAlgorithm(appName = "cmsRun", appVer = "CMSSW_2_1_8",
                               appFam = "RECO", psetHash = "GIBBERISH",
                               configContent = "MOREGIBBERISH")
        testFileD.setDatasetPath(self.testDatasetB)
        testFileD.addRun(Run(2, *[45]))
        testFileD.create()

        testFileE = DBSBufferFile(lfn = makeUUID(), size = 1024, events = 10,
                                  checksums = checksums,
                                  locations = set(["srm-cms.cern.ch"]))
        testFileE.setAlgorithm(appName = "cmsRun", appVer = "CMSSW_2_1_8",
                               appFam = "RECO", psetHash = "GIBBERISH",
                               configContent = "MOREGIBBERISH")
        testFileE.setDatasetPath(self.testDatasetB)
        testFileE.addRun(Run(2, *[45]))
        testFileE.create()

        self.testFilesB.append(testFileD)
        self.testFilesB.append(testFileE)

        uploadFactory = DAOFactory(package = "WMComponent.DBS3Buffer",
                                   logger = myThread.logger,
                                   dbinterface = myThread.dbi)
        datasetAction = uploadFactory(classname = "NewDataset")
        createAction = uploadFactory(classname = "CreateBlocks")

        datasetAction.execute(datasetPath = self.testDatasetA)
        datasetAction.execute(datasetPath = self.testDatasetB)

        self.blockAName = self.testDatasetA + "#" + makeUUID()
        self.blockBName = self.testDatasetB + "#" + makeUUID()

        newBlockA = DBSBlock(name = self.blockAName,
                             location = "srm-cms.cern.ch",
                             das = None, workflow = None)
        newBlockA.setDataset(self.testDatasetA, 'data', 'VALID')
        newBlockA.status = 'Closed'

        newBlockB = DBSBlock(name = self.blockBName,
                             location = "srm-cms.cern.ch",
                             das = None, workflow = None)
        newBlockB.setDataset(self.testDatasetB, 'data', 'VALID')
        newBlockB.status = 'Closed'

        createAction.execute(blocks = [newBlockA, newBlockB])

        bufferFactory = DAOFactory(package = "WMComponent.DBS3Buffer",
                                   logger = myThread.logger,
                                   dbinterface = myThread.dbi)

        setBlock = bufferFactory(classname = "DBSBufferFiles.SetBlock")
        setBlock.execute(testFileA["lfn"], self.blockAName)
        setBlock.execute(testFileB["lfn"], self.blockAName)
        setBlock.execute(testFileC["lfn"], self.blockAName)
        setBlock.execute(testFileD["lfn"], self.blockBName)
        setBlock.execute(testFileE["lfn"], self.blockBName)

        fileStatus = bufferFactory(classname = "DBSBufferFiles.SetStatus")
        fileStatus.execute(testFileA["lfn"], "GLOBAL")
        fileStatus.execute(testFileB["lfn"], "GLOBAL")
        fileStatus.execute(testFileC["lfn"], "GLOBAL")
        fileStatus.execute(testFileD["lfn"], "GLOBAL")
        fileStatus.execute(testFileE["lfn"], "GLOBAL")

        phedexStatus = bufferFactory(classname = "DBSBufferFiles.SetPhEDExStatus")
        phedexStatus.execute(testFileA["lfn"], 1)
        phedexStatus.execute(testFileB["lfn"], 1)
        phedexStatus.execute(testFileC["lfn"], 1)
        phedexStatus.execute(testFileD["lfn"], 1)
        phedexStatus.execute(testFileE["lfn"], 1)

        associateWorkflow = buffer3Factory(classname = "DBSBufferFiles.AssociateWorkflowToFile")
        associateWorkflow.execute(testFileA["lfn"], "BogusRequestA", "BogusTask")
        associateWorkflow.execute(testFileB["lfn"], "BogusRequestA", "BogusTask")
        associateWorkflow.execute(testFileC["lfn"], "BogusRequestA", "BogusTask")
        associateWorkflow.execute(testFileD["lfn"], "BogusRequestB", "BogusTask")
        associateWorkflow.execute(testFileE["lfn"], "BogusRequestB", "BogusTask")

        # Make the desired subscriptions
        insertSubAction = buffer3Factory(classname = "NewSubscription")
        datasetA = DBSBufferDataset(path = self.testDatasetA)
        datasetB = DBSBufferDataset(path = self.testDatasetB)
        workload = WMWorkloadHelper()
        workload.load(os.path.join(getTestBase(), 'WMComponent_t/PhEDExInjector_t/specs/TestWorkload.pkl'))
        insertSubAction.execute(datasetA.exists(), workload.getSubscriptionInformation()[self.testDatasetA])
        insertSubAction.execute(datasetB.exists(), workload.getSubscriptionInformation()[self.testDatasetB])

        return

    def testNormalModeSubscriptions(self):
        """
        _testNormalModeSubscriptions_

        Tests that we can make custodial/non-custodial subscriptions on
        normal operation mode, this time we don't need WMBS for anything.
        All is subscribed in one go.

        Check that the requests are correct.
        """

        self.stuffDatabase()
        config = self.createConfig()
        subscriber = PhEDExInjectorSubscriber(config)
        subscriber.setup({})
        subscriber.algorithm({})

        phedexInstance = subscriber.phedex
        subscriptions = phedexInstance.subRequests

        # Let's check /BogusPrimary/Run2012Z-PromptReco-v1/RECO
        # According to the spec, this should be custodial at T1_US_FNAL
        # Non-custodial at T1_UK_RAL and T3_CO_Uniandes
        # Autoapproved in all sites
        # Priority is normal
        self.assertTrue(self.testDatasetA in subscriptions, "Dataset A was not subscribed")
        subInfoA = subscriptions[self.testDatasetA]
        self.assertEqual(len(subInfoA), 3, "Dataset A was not subscribed to all sites")
        for subInfo in subInfoA:
            site = subInfo["node"]
            self.assertEqual(subInfo["priority"], "normal", "Wrong priority for subscription")
            if site == "T1_UK_RAL_MSS" or site == "T3_CO_Uniandes":
                self.assertEqual(subInfo["custodial"], "n", "Wrong custodiality for dataset A at %s" % subInfo["node"])
                self.assertEqual(subInfo["request_only"], "n", "Wrong requestOnly for dataset A at %s" % subInfo["node"])
                self.assertEqual(subInfo["move"], "n", "Wrong subscription type for dataset A at %s" % subInfo["node"])
            elif site == "T1_US_FNAL_MSS":
                self.assertEqual(subInfo["custodial"], "y", "Wrong custodiality for dataset A at %s" % subInfo["node"])
                self.assertEqual(subInfo["request_only"], "n", "Wrong requestOnly for dataset A at %s" % subInfo["node"])
                self.assertEqual(subInfo["move"], "y", "Wrong subscription type for dataset A at %s" % subInfo["node"])
            else:
                self.fail("Dataset A was subscribed  to a wrong site %s" % site)

        # Now check /BogusPrimary/CRUZET11-v1/RAW
        # According to the spec, this is not custodial anywhere
        # Non-custodial at T1_UK_RAL and T2_CH_CERN
        # Request only at both sites and with high priority
        self.assertTrue(self.testDatasetB in subscriptions, "Dataset B was not subscribed")
        subInfoB = subscriptions[self.testDatasetB]
        self.assertEqual(len(subInfoB), 2, "Dataset B was not subscribed to all sites")
        for subInfo in subInfoB:
            site = subInfo["node"]
            self.assertEqual(subInfo["priority"], "high", "Wrong priority for subscription")
            if site == "T1_UK_RAL_MSS" or site == "T2_CH_CERN":
                self.assertEqual(subInfo["custodial"], "n", "Wrong custodiality for dataset B at %s" % subInfo["node"])
                self.assertEqual(subInfo["request_only"], "y", "Wrong requestOnly for dataset B at %s" % subInfo["node"])
                self.assertEqual(subInfo["move"], "n", "Wrong subscription type for dataset B at %s" % subInfo["node"])
            else:
                self.fail("Dataset B was subscribed to a wrong site %s" % site)

        myThread = threading.currentThread()
        result = myThread.dbi.processData("SELECT COUNT(*) FROM dbsbuffer_dataset_subscription where subscribed = 1")[0].fetchall()
        self.assertEqual(result[0][0], 5, "Not all datasets were marked as subscribed")
        result = myThread.dbi.processData("SELECT site FROM dbsbuffer_dataset_subscription where subscribed = 0")[0].fetchall()
        self.assertEqual(result[0][0], "T1_IT_CNAF", "A non-valid CMS site was subscribed")

        # Reset and run again and make sure that no duplicate subscriptions are created
        myThread.dbi.processData("UPDATE dbsbuffer_dataset_subscription SET subscribed = 0")
        subscriber.algorithm({})
        self.assertEqual(len(subscriptions[self.testDatasetA]), 3)
        self.assertEqual(len(subscriptions[self.testDatasetB]), 2)

        return

if __name__ == '__main__':
    unittest.main()
