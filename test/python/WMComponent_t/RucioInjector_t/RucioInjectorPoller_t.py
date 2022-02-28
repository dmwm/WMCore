#!/usr/bin/env python
"""
Unit tests for the RucioInjectorPoller module
"""

from __future__ import division

import threading
import unittest

from Utils.PythonVersion import PY3

from WMComponent.DBS3Buffer.DBSBufferBlock import DBSBufferBlock
from WMComponent.DBS3Buffer.DBSBufferFile import DBSBufferFile
from WMComponent.RucioInjector.RucioInjectorPoller import RucioInjectorPoller, RucioInjectorException
from WMCore.DAOFactory import DAOFactory
from WMCore.DataStructs.Run import Run
from WMCore.Services.Rucio.Rucio import WMRucioException
from WMCore.Services.UUIDLib import makeUUID
from WMQuality.Emulators.EmulatedUnitTestCase import EmulatedUnitTestCase
from WMQuality.TestInit import TestInit


class RucioInjectorPollerTest(EmulatedUnitTestCase):
    """
    Tests for the RucioInjectorPoller component
    """

    def setUp(self):
        """
        Install the DBSBuffer schema into the database and connect to Rucio.
        """
        super(RucioInjectorPollerTest, self).setUp()
        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection(destroyAllDatabase=True)

        self.testInit.setSchema(customModules=["WMComponent.DBS3Buffer"],
                                useDefault=False)

        myThread = threading.currentThread()
        daofactory = DAOFactory(package="WMComponent.DBS3Buffer",
                                logger=myThread.logger,
                                dbinterface=myThread.dbi)

        locationAction = daofactory(classname="DBSBufferFiles.AddLocation")
        self.locations = ["T2_CH_CERN", "T1_US_FNAL_Disk"]
        for rse in self.locations:
            locationAction.execute(siteName=rse)

        self.testFilesA = []
        self.testFilesB = []
        self.testDatasetA = "/SampleA/PromptReco-v1/RECO"
        self.testDatasetB = "/SampleB/CRUZET11-v1/RAW"

        if PY3:
            self.assertItemsEqual = self.assertCountEqual

        return

    def tearDown(self):
        """
        Delete the database.
        """
        self.testInit.clearDatabase()

    def createConfig(self):
        """
        Create a basic configuration for the component
        """
        config = self.testInit.getConfiguration()
        config.component_("RucioInjector")
        config.RucioInjector.pollInterval = 300
        config.RucioInjector.pollIntervalRules = 43200
        config.RucioInjector.cacheExpiration = 2 * 24 * 60 * 60  # two days
        config.RucioInjector.createBlockRules = True
        config.RucioInjector.RSEPostfix = False  # enable it to append _Test to the RSE names
        config.RucioInjector.metaDIDProject = "Production"
        config.RucioInjector.containerDiskRuleParams = {"weight": "ddm_quota", "copies": 2, "grouping": "DATASET"}
        config.RucioInjector.blockRuleParams = {}
        config.RucioInjector.containerDiskRuleRSEExpr = "(tier=2|tier=1)&cms_type=real&rse_type=DISK"
        config.RucioInjector.rucioAccount = "wma_test"
        config.RucioInjector.rucioUrl = "http://cms-rucio-int.cern.ch"
        config.RucioInjector.rucioAuthUrl = "https://cms-rucio-auth-int.cern.ch"
        return config

    def stuffDatabase(self):
        """
        Fill the dbsbuffer tables with some files and blocks.  We'll insert a total
        of 5 files spanning two blocks.  There will be a total of two datasets
        inserted into the database.
        We'll inject files with the location set as an SE name as well as a
        PhEDEx node name as well.
        """
        myThread = threading.currentThread()

        # Create the DAOs factory and the relevant instances
        buffer3Factory = DAOFactory(package="WMComponent.DBS3Buffer",
                                    logger=myThread.logger,
                                    dbinterface=myThread.dbi)
        setBlock = buffer3Factory(classname="DBSBufferFiles.SetBlock")
        fileStatus = buffer3Factory(classname="DBSBufferFiles.SetStatus")
        associateWorkflow = buffer3Factory(classname="DBSBufferFiles.AssociateWorkflowToFile")
        insertWorkflow = buffer3Factory(classname="InsertWorkflow")
        datasetAction = buffer3Factory(classname="NewDataset")
        createAction = buffer3Factory(classname="CreateBlocks")

        # Create workflow in the database
        insertWorkflow.execute("BogusRequest", "BogusTask", 0, 0, 0, 0)

        # First file on first block
        checksums = {"adler32": "1234", "cksum": "5678"}
        testFileA = DBSBufferFile(lfn=makeUUID(), size=1024, events=10,
                                  checksums=checksums,
                                  locations=set(["T2_CH_CERN"]))
        testFileA.setAlgorithm(appName="cmsRun", appVer="CMSSW_2_1_8",
                               appFam="RECO", psetHash="GIBBERISH",
                               configContent="MOREGIBBERISH")
        testFileA.setDatasetPath(self.testDatasetA)
        testFileA.addRun(Run(2, *[45]))
        testFileA.create()

        # Second file on first block
        testFileB = DBSBufferFile(lfn=makeUUID(), size=1024, events=10,
                                  checksums=checksums,
                                  locations=set(["T2_CH_CERN"]))
        testFileB.setAlgorithm(appName="cmsRun", appVer="CMSSW_2_1_8",
                               appFam="RECO", psetHash="GIBBERISH",
                               configContent="MOREGIBBERISH")
        testFileB.setDatasetPath(self.testDatasetA)
        testFileB.addRun(Run(2, *[45]))
        testFileB.create()

        # Third file on first block
        testFileC = DBSBufferFile(lfn=makeUUID(), size=1024, events=10,
                                  checksums=checksums,
                                  locations=set(["T2_CH_CERN"]))
        testFileC.setAlgorithm(appName="cmsRun", appVer="CMSSW_2_1_8",
                               appFam="RECO", psetHash="GIBBERISH",
                               configContent="MOREGIBBERISH")
        testFileC.setDatasetPath(self.testDatasetA)
        testFileC.addRun(Run(2, *[45]))
        testFileC.create()

        self.testFilesA.append(testFileA)
        self.testFilesA.append(testFileB)
        self.testFilesA.append(testFileC)

        # First file on second block
        testFileD = DBSBufferFile(lfn=makeUUID(), size=1024, events=10,
                                  checksums=checksums,
                                  locations=set(["T1_US_FNAL_Disk"]))
        testFileD.setAlgorithm(appName="cmsRun", appVer="CMSSW_2_1_8",
                               appFam="RECO", psetHash="GIBBERISH",
                               configContent="MOREGIBBERISH")
        testFileD.setDatasetPath(self.testDatasetB)
        testFileD.addRun(Run(2, *[45]))
        testFileD.create()

        # Second file on second block
        testFileE = DBSBufferFile(lfn=makeUUID(), size=1024, events=10,
                                  checksums=checksums,
                                  locations=set(["T1_US_FNAL_Disk"]))
        testFileE.setAlgorithm(appName="cmsRun", appVer="CMSSW_2_1_8",
                               appFam="RECO", psetHash="GIBBERISH",
                               configContent="MOREGIBBERISH")
        testFileE.setDatasetPath(self.testDatasetB)
        testFileE.addRun(Run(2, *[45]))
        testFileE.create()

        self.testFilesB.append(testFileD)
        self.testFilesB.append(testFileE)

        # insert datasets in the dbsbuffer table
        datasetAction.execute(datasetPath=self.testDatasetA)
        datasetAction.execute(datasetPath=self.testDatasetB)

        self.blockAName = self.testDatasetA + "#" + makeUUID()
        self.blockBName = self.testDatasetB + "#" + makeUUID()

        # create and insert blocks into dbsbuffer table
        newBlockA = DBSBufferBlock(name=self.blockAName,
                                   location="T2_CH_CERN",
                                   datasetpath=None)
        newBlockA.setDataset(self.testDatasetA, 'data', 'VALID')
        newBlockA.status = 'Closed'

        newBlockB = DBSBufferBlock(name=self.blockBName,
                                   location="T1_US_FNAL_Disk",
                                   datasetpath=None)
        newBlockB.setDataset(self.testDatasetB, 'data', 'VALID')
        newBlockB.status = 'Closed'

        createAction.execute(blocks=[newBlockA, newBlockB])

        # associate files to their correspondent block id
        setBlock.execute(testFileA["lfn"], self.blockAName)
        setBlock.execute(testFileB["lfn"], self.blockAName)
        setBlock.execute(testFileC["lfn"], self.blockAName)
        setBlock.execute(testFileD["lfn"], self.blockBName)
        setBlock.execute(testFileE["lfn"], self.blockBName)

        # set file status to LOCAL
        fileStatus.execute(testFileA["lfn"], "LOCAL")
        fileStatus.execute(testFileB["lfn"], "LOCAL")
        fileStatus.execute(testFileC["lfn"], "LOCAL")
        fileStatus.execute(testFileD["lfn"], "LOCAL")
        fileStatus.execute(testFileE["lfn"], "LOCAL")

        # associate files to a given workflow
        associateWorkflow.execute(testFileA["lfn"], "BogusRequest", "BogusTask")
        associateWorkflow.execute(testFileB["lfn"], "BogusRequest", "BogusTask")
        associateWorkflow.execute(testFileC["lfn"], "BogusRequest", "BogusTask")
        associateWorkflow.execute(testFileD["lfn"], "BogusRequest", "BogusTask")
        associateWorkflow.execute(testFileE["lfn"], "BogusRequest", "BogusTask")

        return

    def testBadConfig(self):
        """
        Test wrong component configuration
        """
        config = self.createConfig()
        config.RucioInjector.metaDIDProject = "Very invalid project name"

        with self.assertRaises(RucioInjectorException):
            RucioInjectorPoller(config)

    def testActivityMap(self):
        """
        Initialize a RucioInjectorPoller object and test `_activityMap` method
        """
        poller = RucioInjectorPoller(self.createConfig())
        # test production agent and non-Tape endpoint
        activity = poller._activityMap("T1_US_FNAL_Disk")
        self.assertEquals(activity, "Production Output")
        activity = poller._activityMap("T1_US_FNAL_Test")
        self.assertEquals(activity, "Production Output")
        # test production agent and Tape endpoint (which is forbidden at the moment)
        with self.assertRaises(WMRucioException):
            poller._activityMap("T1_US_FNAL_Tape")

        # now pretend it to be a T0 agent/component
        poller.isT0agent = True
        # test T0 agent and non-Tape endpoint
        activity = poller._activityMap("T1_US_FNAL_Disk")
        self.assertEquals(activity, "T0 Export")
        activity = poller._activityMap("T1_US_FNAL_Test")
        self.assertEquals(activity, "T0 Export")
        # test T0 agent and Tape endpoint
        activity = poller._activityMap("T1_US_FNAL_Tape")
        self.assertEquals(activity, "T0 Tape")

    def testLoadingFiles(self):
        """
        Initialize a RucioInjectorPoller object and load uninjected files
        """
        self.stuffDatabase()
        poller = RucioInjectorPoller(self.createConfig())
        poller.setup(parameters=None)
        uninjectedFiles = poller.getUninjected.execute()
        self.assertItemsEqual(list(uninjectedFiles), self.locations)
        self.assertEquals(list(uninjectedFiles["T2_CH_CERN"]), [self.testDatasetA])
        self.assertEquals(list(uninjectedFiles["T1_US_FNAL_Disk"]), [self.testDatasetB])


if __name__ == '__main__':
    unittest.main()
