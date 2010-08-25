#!/usr/bin/env python
"""
PhEDExInjectorPoller_t

Unit tests for the PhEDExInjector.  Create some database inside DBSBuffer
and then have the PhEDExInjector upload the data to PhEDEx.  Pull the data
back down and verify that everything is complete.
"""

__revision__ = "$Id: PhEDExInjectorPoller_t.py,v 1.2 2009/09/24 20:19:48 sfoulkes Exp $"
__version__ = "$Revision: 1.2 $"

import logging
import threading
import time
import unittest

from sets import Set

from WMComponent.PhEDExInjector.PhEDExInjectorPoller import PhEDExInjectorPoller
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
        self.phedexURL = "https://cmsweb.cern.ch/phedex/datasvc/json/tbedi/"
        self.dbsURL = "http://cmssrv49.fnal.gov:8989/DBS/servlet/DBSServlet"
        
        self.testInit = TestInit(__file__, os.getenv("DIALECT"))
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()

        self.testInit.setSchema(customModules = ["WMComponent.DBSBuffer.Database"],
                                useDefault = False)

        myThread = threading.currentThread()
        daofactory = DAOFactory(package = "WMComponent.DBSBuffer.Database",
                                logger = myThread.logger,
                                dbinterface = myThread.dbi)

        locationAction = daofactory(classname = "DBSBufferFiles.AddLocation")
        locationAction.execute(siteName = "srm.cern.ch")

        self.testFilesA = []
        self.testFilesB = []
        self.phedex = PhEDEx({"endpoint": self.phedexURL}, "json")

        return

    def tearDown(self):
        """
        _tearDown_

        Delete the database.
        """
        myThread = threading.currentThread()
        
        factory = WMFactory("DBSBuffer", "WMComponent.DBSBuffer.Database")
        destroy = factory.loadObject(myThread.dialect + ".Destroy")
        myThread.transaction.begin()
        destroyworked = destroy.execute(conn = myThread.transaction.conn)
        if not destroyworked:
            raise Exception("Could not complete DBSBuffer tear down.")
        myThread.transaction.commit()
        
        return

    def stuffDatabase(self):
        """
        _stuffDatabase_

        Fill the dbsbuffer with some files and blocks.  We'll insert a total
        of 5 files spanning two blocks.  There will be a total of two datasets
        inserted into the datbase.

        We'll inject files with the location set as an SE name as well as a
        PhEDEx node name as well.
        """
        testFileA = DBSBufferFile(lfn = makeUUID(), size = 1024, events = 10,
                                  cksum = 1234, locations = Set(["srm.cern.ch"]))
        testFileA.setAlgorithm(appName = "cmsRun", appVer = "CMSSW_2_1_8",
                               appFam = "RECO", psetHash = "GIBBERISH",
                               configContent = "MOREGIBBERISH")
        testFileA.setDatasetPath("/Cosmics/CRUZET09-PromptReco-v1/RECO")
        testFileA.addRun(Run(2, *[45]))
        testFileA.create()

        testFileB = DBSBufferFile(lfn = makeUUID(), size = 1024, events = 10,
                                  cksum = 1234, locations = Set(["srm.cern.ch"]))
        testFileB.setAlgorithm(appName = "cmsRun", appVer = "CMSSW_2_1_8",
                               appFam = "RECO", psetHash = "GIBBERISH",
                               configContent = "MOREGIBBERISH")
        testFileB.setDatasetPath("/Cosmics/CRUZET09-PromptReco-v1/RECO")
        testFileB.addRun(Run(2, *[45]))
        testFileB.create()

        testFileC = DBSBufferFile(lfn = makeUUID(), size = 1024, events = 10,
                                  cksum = 1234, locations = Set(["srm.cern.ch"]))
        testFileC.setAlgorithm(appName = "cmsRun", appVer = "CMSSW_2_1_8",
                               appFam = "RECO", psetHash = "GIBBERISH",
                               configContent = "MOREGIBBERISH")
        testFileC.setDatasetPath("/Cosmics/CRUZET09-PromptReco-v1/RECO")
        testFileC.addRun(Run(2, *[45]))
        testFileC.create()        
                                        
        self.testFilesA.append(testFileA)
        self.testFilesA.append(testFileB)
        self.testFilesA.append(testFileC)        

        testFileD = DBSBufferFile(lfn = makeUUID(), size = 1024, events = 10,
                                  cksum = 1234, locations = Set(["srm.cern.ch"]))
        testFileD.setAlgorithm(appName = "cmsRun", appVer = "CMSSW_2_1_8",
                               appFam = "RECO", psetHash = "GIBBERISH",
                               configContent = "MOREGIBBERISH")
        testFileD.setDatasetPath("/Cosmics/CRUZET09-PromptReco-v1/RAW")
        testFileD.addRun(Run(2, *[45]))
        testFileD.create()

        testFileE = DBSBufferFile(lfn = makeUUID(), size = 1024, events = 10,
                                  cksum = 1234, locations = Set(["srm.cern.ch"]))
        testFileE.setAlgorithm(appName = "cmsRun", appVer = "CMSSW_2_1_8",
                               appFam = "RECO", psetHash = "GIBBERISH",
                               configContent = "MOREGIBBERISH")
        testFileE.setDatasetPath("/Cosmics/CRUZET09-PromptReco-v1/RAW")
        testFileE.addRun(Run(2, *[45]))
        testFileE.create()        

        self.testFilesB.append(testFileD)
        self.testFilesB.append(testFileE)

        myThread = threading.currentThread()
        uploadFactory = DAOFactory(package = "WMComponent.DBSUpload.Database",
                                   logger = myThread.logger,
                                   dbinterface = myThread.dbi)
        createBlock = uploadFactory(classname = "SetBlockStatus")

        self.blockAName = "/Cosmics/CRUZET09-PromptReco-v1/RAW#" + makeUUID()
        self.blockBName = "/Cosmics/CRUZET09-PromptReco-v1/RECO#" + makeUUID()
        createBlock.execute(block = self.blockAName, locations = ["srm.cern.ch"])
        createBlock.execute(block = self.blockBName, locations = ["srm.cern.ch"])

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
        fileStatus.execute(testFileA["lfn"], "InDBS")
        fileStatus.execute(testFileB["lfn"], "InDBS")
        fileStatus.execute(testFileC["lfn"], "InDBS")
        fileStatus.execute(testFileD["lfn"], "InDBS")
        fileStatus.execute(testFileE["lfn"], "InDBS")        
        return

    def createConfig(self):
        """
        _createConfig_

        Create a config for the PhEDExInjector with paths to the test DBS and
        PhEDEx instances.
        """
        config = Configuration()
        config.component_("DBSUpload")
        config.DBSUpload.dbsurl = self.dbsURL

        config.component_("PhEDExInjector")
        config.PhEDExInjector.phedexurl = self.phedexURL

        return config

    def retrieveReplicaInfoForBlock(self, blockName):
        """
        _retrieveReplicaInfoForBlock_

        Retrieve the replica information for a block.  It takes several minutes
        after a block is injected for the statistics to be calculated, so this
        will block until that information is available.
        """
        attempts = 0

        while attempts < 5:
            result = self.phedex.getReplicaInfoForBlock(blockName)

            if result.has_key("phedex"):
                if result["phedex"].has_key("block"):
                    if len(result["phedex"]["block"]) != 0:
                        return result["phedex"]["block"][0]
            
            attempts += 1
            time.sleep(60)

        print "Could not retrieve replica info for block: %s" % blockName
        return None

    def testPoller(self):
        """
        _testPoller_

        Stuff the database and have the poller upload files to PhEDEx.  Retrieve
        replica information for the uploaded blocks and verify that all files
        have been injected.
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
        
        return

if __name__ == '__main__':
    unittest.main()
