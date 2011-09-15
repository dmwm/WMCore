#!/usr/bin/env python
"""
PhEDExInjectorPoller_t

Unit tests for the PhEDExInjector.  Create some database inside DBSBuffer
and then have the PhEDExInjector upload the data to PhEDEx.  Pull the data
back down and verify that everything is complete.
"""

import threading
import time
import os
import unittest
import logging

from WMComponent.PhEDExInjector.PhEDExInjectorPoller import PhEDExInjectorPoller
from WMComponent.PhEDExInjector.PhEDExInjectorSubscriber import PhEDExInjectorSubscriber
from WMComponent.DBSBuffer.Database.Interface.DBSBufferFile import DBSBufferFile

from WMCore.Services.PhEDEx.PhEDEx import PhEDEx
from WMCore.Services.UUID import makeUUID

from WMCore.Agent.Configuration import Configuration
from WMCore.WMFactory import WMFactory
from WMCore.DAOFactory import DAOFactory
from WMCore.DataStructs.Run import Run
from WMQuality.TestInit import TestInit

class PhEDExInjectorPollerTest(unittest.TestCase):
    """
    _PhEDExInjectorPollerTest_

    Unit tests for the PhEDExInjector.  Create some database inside DBSBuffer
    and then have the PhEDExInjector upload the data to PhEDEx.  Pull the data
    back down and verify that everything is complete.    
    """

    def setUp(self):
        """
        _setUp_

        Install the DBSBuffer schema into the database and connect to PhEDEx.
        """
        self.phedexURL = "https://cmsweb.cern.ch/phedex/datasvc/json/test"
        self.dbsURL = "http://vocms09.cern.ch:8880/cms_dbs_int_local_yy_writer/servlet/DBSServlet"
        
        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()

        self.testInit.setSchema(customModules = ["WMComponent.DBS3Buffer"],
                                useDefault = False)

        myThread = threading.currentThread()
        daofactory = DAOFactory(package = "WMComponent.DBSBuffer.Database",
                                logger = myThread.logger,
                                dbinterface = myThread.dbi)

        locationAction = daofactory(classname = "DBSBufferFiles.AddLocation")
        locationAction.execute(siteName = "srm-cms.cern.ch")

        self.testFilesA = []
        self.testFilesB = []
        self.testDatasetA = "/%s/PromptReco-v1/RECO" % makeUUID()
        self.testDatasetB = "/%s/CRUZET11-v1/RAW" % makeUUID()
        self.phedex = PhEDEx({"endpoint": self.phedexURL}, "json")

        return

    def tearDown(self):
        """
        _tearDown_

        Delete the database.
        """
        self.testInit.clearDatabase()
        
    def stuffDatabase(self):
        """
        _stuffDatabase_

        Fill the dbsbuffer with some files and blocks.  We'll insert a total
        of 5 files spanning two blocks.  There will be a total of two datasets
        inserted into the datbase.

        We'll inject files with the location set as an SE name as well as a
        PhEDEx node name as well.
        """
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

        myThread = threading.currentThread()
        uploadFactory = DAOFactory(package = "WMComponent.DBSUpload.Database",
                                   logger = myThread.logger,
                                   dbinterface = myThread.dbi)
        createBlock = uploadFactory(classname = "SetBlockStatus")

        self.blockAName = self.testDatasetA + "#" + makeUUID()
        self.blockBName = self.testDatasetB + "#" + makeUUID()
        createBlock.execute(block = self.blockAName, locations = ["srm-cms.cern.ch"], open_status = 1)
        createBlock.execute(block = self.blockBName, locations = ["srm-cms.cern.ch"], open_status = 1)

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
        fileStatus.execute(testFileA["lfn"], "LOCAL")
        fileStatus.execute(testFileB["lfn"], "LOCAL")
        fileStatus.execute(testFileC["lfn"], "LOCAL")
        fileStatus.execute(testFileD["lfn"], "LOCAL")
        fileStatus.execute(testFileE["lfn"], "LOCAL")        
        return

    def createConfig(self):
        """
        _createConfig_

        Create a config for the PhEDExInjector with paths to the test DBS and
        PhEDEx instances.
        """
        config = self.testInit.getConfiguration()
        config.component_("DBSInterface")
        config.DBSInterface.globalDBSUrl = self.dbsURL

        config.component_("PhEDExInjector")
        config.PhEDExInjector.phedexurl = self.phedexURL
        config.PhEDExInjector.subscribeMSS = True
        config.PhEDExInjector.group = "Saturn"
        config.PhEDExInjector.pollInterval = 30
        config.PhEDExInjector.subscribeInterval = 60

        return config

    def retrieveReplicaInfoForBlock(self, blockName):
        """
        _retrieveReplicaInfoForBlock_

        Retrieve the replica information for a block.  It takes several minutes
        after a block is injected for the statistics to be calculated, so this
        will block until that information is available.
        """
        attempts = 0

        while attempts < 15:
            result = self.phedex.getReplicaInfoForFiles(block = blockName)

            if result.has_key("phedex"):
                if result["phedex"].has_key("block"):
                    if len(result["phedex"]["block"]) != 0:
                        return result["phedex"]["block"][0]
            
            attempts += 1
            time.sleep(20)

        logging.info("Could not retrieve replica info for block: %s" % blockName)
        return None

    def DISABLEDtestPoller(self):
        """
        _testPoller_

        Stuff the database and have the poller upload files to PhEDEx.  Retrieve
        replica information for the uploaded blocks and verify that all files
        have been injected.  Also verify that files have been subscribed to MSS.
        """
        self.stuffDatabase()

        poller = PhEDExInjectorPoller(self.createConfig())
        poller.setup(parameters = None)
        poller.algorithm(parameters = None)

        replicaInfo = self.retrieveReplicaInfoForBlock(self.blockAName)
        goldenLFNs = []
        for file in self.testFilesA:
            goldenLFNs.append(file["lfn"])

        for replicaFile in replicaInfo["file"]:
            assert replicaFile["name"] in goldenLFNs, \
                   "Error: Extra file in replica block: %s" % replicaFile["name"]
            goldenLFNs.remove(replicaFile["name"])

        assert len(goldenLFNs) == 0, \
               "Error: Files missing from PhEDEx replica: %s" % goldenLFNs

        replicaInfo = self.retrieveReplicaInfoForBlock(self.blockBName)
        goldenLFNs = []
        for file in self.testFilesB:
            goldenLFNs.append(file["lfn"])

        for replicaFile in replicaInfo["file"]:
            assert replicaFile["name"] in goldenLFNs, \
                   "Error: Extra file in replica block: %s" % replicaFile["name"]
            goldenLFNs.remove(replicaFile["name"])

        assert len(goldenLFNs) == 0, \
               "Error: Files missing from PhEDEx replica: %s" % goldenLFNs        

        myThread = threading.currentThread()
        daofactory = DAOFactory(package = "WMComponent.DBSUpload.Database",
                                logger = myThread.logger,
                                dbinterface = myThread.dbi)
        setBlock = daofactory(classname = "SetBlockStatus")
        setBlock.execute(self.blockAName, locations = None,
                         open_status = "InGlobalDBS")

        poller.algorithm(parameters = None)
        replicaInfo = self.retrieveReplicaInfoForBlock(self.blockAName)
        assert replicaInfo["is_open"] == "n", \
               "Error: block should be closed."

        replicaInfo = self.retrieveReplicaInfoForBlock(self.blockBName)
        assert replicaInfo["is_open"] == "y", \
               "Error: block should be open."

        subscriber = PhEDExInjectorSubscriber(self.createConfig())
        subscriber.setup(parameters = None)
        subscriber.algorithm(parameters = None)        

        subAResult = self.phedex.subscriptions(dataset = self.testDatasetA)
        self.assertEqual(len(subAResult["phedex"]["dataset"]), 1,
                         "Error: Subscription was not made.")
        datasetASub = subAResult["phedex"]["dataset"][0]
        self.assertTrue(datasetASub["files"] == "3" and datasetASub["name"] == self.testDatasetA,
                        "Error: Metadata is incorrect for sub.")
        self.assertEqual(datasetASub["subscription"][0]["node"], "T1_CH_CERN_MSS",
                         "Error: Node is wrong.")
        return

if __name__ == '__main__':
    unittest.main()
