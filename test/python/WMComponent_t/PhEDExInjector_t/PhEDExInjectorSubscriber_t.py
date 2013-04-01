#!/usr/bin/env python
"""
Created on Oct 12, 2012

@author: dballest
"""

import os
import threading
import unittest

from WMComponent.DBS3Buffer.DBSBufferFile import DBSBufferFile
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

from WMQuality.TestInit import TestInit

from nose.plugins.attrib import attr
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper

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

    def createConfig(self, safeMode):
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
        config.PhEDExInjector.safeOperationMode = safeMode

        return config

    def stuffDatabase(self):
        """
        _stuffDatabase_

        Fill the dbsbuffer with some files and blocks.  We'll insert a total
        of 5 files spanning two blocks.  There will be a total of two datasets
        inserted into the database, both from the same workflow.

        All files will be already in GLOBAL and in_phedex
        """

        myThread = threading.currentThread()
        buffer3Factory = DAOFactory(package = "WMComponent.DBS3Buffer",
                                   logger = myThread.logger,
                                   dbinterface = myThread.dbi)
        insertWorkflow = buffer3Factory(classname = "InsertWorkflow")
        insertWorkflow.execute("BogusRequest", "BogusTask",
                               0,0,0,0,
                               os.path.join(getTestBase(),
                                            "WMComponent_t/PhEDExInjector_t/specs/TestWorkload.pkl"))

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

        uploadFactory = DAOFactory(package = "WMComponent.DBSUpload.Database",
                                   logger = myThread.logger,
                                   dbinterface = myThread.dbi)
        createBlock = uploadFactory(classname = "SetBlockStatus")

        self.blockAName = self.testDatasetA + "#" + makeUUID()
        self.blockBName = self.testDatasetB + "#" + makeUUID()
        createBlock.execute(block = self.blockAName, locations = ["srm-cms.cern.ch"], open_status = 0)
        createBlock.execute(block = self.blockBName, locations = ["srm-cms.cern.ch"], open_status = 0)

        bufferFactory = DAOFactory(package = "WMComponent.DBSBuffer.Database",
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
        associateWorkflow.execute(testFileA["lfn"], "BogusRequest", "BogusTask")
        associateWorkflow.execute(testFileB["lfn"], "BogusRequest", "BogusTask")
        associateWorkflow.execute(testFileC["lfn"], "BogusRequest", "BogusTask")
        associateWorkflow.execute(testFileD["lfn"], "BogusRequest", "BogusTask")
        associateWorkflow.execute(testFileE["lfn"], "BogusRequest", "BogusTask")

        return

    def stuffWMBS(self):
        """
        _stuffWMBS_

        Inject the workflow in WMBS and add the subscriptions
        """

        testWorkflow = Workflow(spec = os.path.join(getTestBase(),
                                                    "WMComponent_t/PhEDExInjector_t/specs/TestWorkload.pkl"),
                                owner = "/CN=OU/DN=SomeoneWithPermissions",
                                name = "BogusRequest", task = "BogusTask", owner_vogroup = "", owner_vorole = "")
        testWorkflow.create()

        testMergeWorkflow = Workflow(spec = os.path.join(getTestBase(),
                                                    "WMComponent_t/PhEDExInjector_t/specs/TestWorkload.pkl"),
                                     owner = "/CN=OU/DN=SomeoneWithPermissions",
                                     name = "BogusRequest", task = "BogusTask/Merge", owner_vogroup = "", owner_vorole = "")
        testMergeWorkflow.create()

        testWMBSFileset = Fileset(name = "TopFileset")
        testWMBSFileset.create()
        testWMBSFilesetUnmerged = Fileset(name = "UnmergedFileset")
        testWMBSFilesetUnmerged.create()

        testFileA = File(lfn = "/this/is/a/lfnA" , size = 1024, events = 10)
        testFileA.addRun(Run(10, *[12312]))
        testFileA.setLocation('malpaquet')

        testFileB = File(lfn = "/this/is/a/lfnB", size = 1024, events = 10)
        testFileB.addRun(Run(10, *[12314]))
        testFileB.setLocation('malpaquet')

        testFileA.create()
        testFileB.create()

        testWMBSFileset.addFile(testFileA)
        testWMBSFilesetUnmerged.addFile(testFileB)
        testWMBSFileset.commit()
        testWMBSFilesetUnmerged.commit()

        testSubscription = Subscription(fileset = testWMBSFileset,
                                        workflow = testWorkflow)
        testSubscription.create()

        testSubscriptionMerge = Subscription(fileset = testWMBSFilesetUnmerged,
                                             workflow = testMergeWorkflow,
                                             type = "Merge")
        testSubscriptionMerge.create()

        return (testSubscription, testSubscriptionMerge)

    def testUnsafeModeSubscriptions(self):
        """
        _testUnsafeModeSubscriptions_

        Tests that we can make custodial/non-custodial subscriptions on
        unsafe operation mode, this time we don't need WMBS for anything.
        All is subscribed in one go.

        Check that the requests are correct.
        """

        self.stuffDatabase()
        config = self.createConfig(safeMode = False)

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
            site = subInfo["node"][0]
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
            site = subInfo["node"][0]
            self.assertEqual(subInfo["priority"], "high", "Wrong priority for subscription")
            if site == "T1_UK_RAL_MSS" or site == "T2_CH_CERN":
                self.assertEqual(subInfo["custodial"], "n", "Wrong custodiality for dataset B at %s" % subInfo["node"])
                self.assertEqual(subInfo["request_only"], "y", "Wrong requestOnly for dataset B at %s" % subInfo["node"])
                self.assertEqual(subInfo["move"], "n", "Wrong subscription type for dataset B at %s" % subInfo["node"])
            else:
                self.fail("Dataset B was subscribed to a wrong site %s" % site)

        myThread = threading.currentThread()
        result = myThread.dbi.processData("SELECT COUNT(*) FROM dbsbuffer_dataset where subscribed = 1")[0].fetchall()
        self.assertEqual(result[0][0], 2, "Not all datasets were marked as subscribed")

        return

    def testSafeModeSubscriptions(self):
        """
        _testSafeModeSubscriptions_

        Tests that we can make custodial/non-custodial subscriptions on
        safe operation mode, make sure that the flow of subscriptions
        obeys the rule laid in the subscriber documentation.

        Check that the requests are correct.
        """
        config = self.createConfig(safeMode = True)
        self.stuffDatabase()
        topSubscription, mergeSubscription = self.stuffWMBS()

        # Start the subscriber
        subscriber = PhEDExInjectorSubscriber(config)
        subscriber.setup({})

        # Run once, this means that all custodial and non-custodial subscriptions
        # will be made but none will be Move
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
            site = subInfo["node"][0]
            self.assertEqual(subInfo["priority"], "normal", "Wrong priority for subscription")
            if site == "T1_UK_RAL_MSS" or site == "T3_CO_Uniandes":
                self.assertEqual(subInfo["custodial"], "n", "Wrong custodiality for dataset A at %s" % subInfo["node"])
                self.assertEqual(subInfo["request_only"], "n", "Wrong requestOnly for dataset A at %s" % subInfo["node"])
                self.assertEqual(subInfo["move"], "n", "Wrong subscription type for dataset A at %s" % subInfo["node"])
            elif site == "T1_US_FNAL_MSS":
                self.assertEqual(subInfo["custodial"], "y", "Wrong custodiality for dataset A at %s" % subInfo["node"])
                self.assertEqual(subInfo["request_only"], "n", "Wrong requestOnly for dataset A at %s" % subInfo["node"])
                self.assertEqual(subInfo["move"], "n", "Wrong subscription type for dataset A at %s" % subInfo["node"])
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
            site = subInfo["node"][0]
            self.assertEqual(subInfo["priority"], "high", "Wrong priority for subscription")
            if site == "T1_UK_RAL_MSS" or site == "T2_CH_CERN":
                self.assertEqual(subInfo["custodial"], "n", "Wrong custodiality for dataset B at %s" % subInfo["node"])
                self.assertEqual(subInfo["request_only"], "y", "Wrong requestOnly for dataset B at %s" % subInfo["node"])
                self.assertEqual(subInfo["move"], "n", "Wrong subscription type for dataset B at %s" % subInfo["node"])
            else:
                self.fail("Dataset B was subscribed to a wrong site %s" % site)

        myThread = threading.currentThread()
        result = myThread.dbi.processData("SELECT COUNT(*) FROM dbsbuffer_dataset where subscribed = 1")[0].fetchall()
        self.assertEqual(result[0][0], 2, "Not all datasets were marked as partially subscribed")

        # Now finish the Processing subscription and run the algorithm again
        topSubscription.markFinished()
        subscriber.algorithm({})

        self.assertTrue(self.testDatasetA in subscriptions, "Dataset A was not subscribed")
        subInfoA = subscriptions[self.testDatasetA]
        self.assertEqual(len(subInfoA), 4, "Dataset A was not subscribed again to custodial site")
        moveCount = 0
        for subInfo in subInfoA:
            site = subInfo["node"][0]
            self.assertEqual(subInfo["priority"], "normal", "Wrong priority for subscription")
            if site == "T1_UK_RAL_MSS" or site == "T3_CO_Uniandes":
                self.assertEqual(subInfo["custodial"], "n", "Wrong custodiality for dataset A at %s" % subInfo["node"])
                self.assertEqual(subInfo["request_only"], "n", "Wrong requestOnly for dataset A at %s" % subInfo["node"])
                self.assertEqual(subInfo["move"], "n", "Wrong subscription type for dataset A at %s" % subInfo["node"])
            elif site == "T1_US_FNAL_MSS":
                self.assertEqual(subInfo["custodial"], "y", "Wrong custodiality for dataset A at %s" % subInfo["node"])
                self.assertEqual(subInfo["request_only"], "n", "Wrong requestOnly for dataset A at %s" % subInfo["node"])
                if subInfo["move"] == "y":
                    moveCount += 1
            else:
                self.fail("Dataset A was subscribed  to a wrong site %s" % site)
        self.assertEqual(moveCount, 1, "Move subscription was not made")

        self.assertTrue(self.testDatasetB in subscriptions, "Dataset B was not subscribed")
        subInfoB = subscriptions[self.testDatasetB]
        self.assertEqual(len(subInfoB), 2, "Dataset B was susbcribed again")

        result = myThread.dbi.processData("SELECT COUNT(*) FROM dbsbuffer_dataset where subscribed = 2")[0].fetchall()
        self.assertEqual(result[0][0], 2, "Not all datasets were marked as subscribed")

        return

if __name__ == '__main__':
    unittest.main()
