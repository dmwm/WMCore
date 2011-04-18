#!/usr/bin/env python
# encoding: utf-8
"""
DataCollectionService_t.py

Created by Dave Evans on 2010-10-05.
Copyright (c) 2010 Fermilab. All rights reserved.
"""

import unittest
import os
import random


from WMQuality.TestInitCouchApp import TestInitCouchApp
from WMCore.ACDC.DataCollectionService import DataCollectionService
from WMCore.WMSpec.WMWorkload import newWorkload, WMWorkloadHelper
from WMCore.WMBS.Job import Job
from WMCore.DataStructs.File import File
from WMCore.DataStructs.Run import Run
from WMCore.Services.UUID import makeUUID

from WMCore.WMSpec.StdSpecs.ReDigi import getTestArguments, reDigiWorkload
from WMCore.Database.CMSCouch import CouchServer, Document

class DataCollectionService_t(unittest.TestCase):
    def setUp(self):
        """bootstrap tests"""
        self.testInit = TestInitCouchApp(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules = ["WMCore.WMBS"],
                                useDefault = False)
        self.testInit.setupCouch("wmcore-acdc-datacollectionsvc", "GroupUser", "ACDC")
        self.testInit.setupCouch("datacollectionsvc_t_cc", "ConfigCache")

        couchServer = CouchServer(os.environ["COUCHURL"])
        self.configDatabase = couchServer.connectDatabase("datacollectionsvc_t_cc")
        return
        
    def tearDown(self):
        self.testInit.tearDownCouch()
        self.testInit.clearDatabase()
        return

    def createTestWorkload(self):
        """
        _createTestWorkload_

        Create a bogus test workload with two tasks.
        """
        workload = newWorkload("ACDCTest")        
        reco = workload.newTask("reco")
        skim1 = reco.addTask("skim1")
        workload.setOwnerDetails("evansde77", "DMWM")

        # first task uses the input dataset
        reco.addInputDataset(primary = "PRIMARY", processed = "processed-v1", tier = "TIER1")
        cmsRunReco = reco.makeStep("cmsRun1")
        cmsRunReco.setStepType("CMSSW")
        reco.applyTemplates()
        cmsRunRecoHelper = cmsRunReco.getTypeHelper()
        cmsRunRecoHelper.addOutputModule("outputRECO",
                                        primaryDataset = "PRIMARY",
                                        processedDataset = "processed-v2",
                                        dataTier = "TIER2",
                                        lfnBase = "/store/dunkindonuts",
                                        mergedLFNBase = "/store/kfc")
        # second step uses an input reference
        cmsRunSkim = skim1.makeStep("cmsRun2")
        cmsRunSkim.setStepType("CMSSW")
        skim1.applyTemplates()        
        skim1.setInputReference(cmsRunReco, outputModule = "outputRECO")

        return workload

    def injectReDigiConfigs(self):
        """
        _injectReDigiConfigs_

        Create bogus config cache documents for the various steps of the
        ReDigi workflow.  Return the IDs of the documents.
        """
        stepOneConfig = Document()
        stepOneConfig["info"] = None
        stepOneConfig["config"] = None
        stepOneConfig["md5hash"] = "eb1c38cf50e14cf9fc31278a5c8e580f"
        stepOneConfig["pset_hash"] = "7c856ad35f9f544839d8525ca10259a7"
        stepOneConfig["owner"] = {"group": "cmsdataops", "user": "sfoulkes"}
        stepOneConfig["pset_tweak_details"] ={"process": {"outputModules_": ["RAWDEBUGoutput"],
                                                          "RAWDEBUGoutput": {"dataset": {"filterName": "",
                                                                                         "dataTier": "RAW-DEBUG-OUTPUT"}}}}

        stepTwoConfig = Document()
        stepTwoConfig["info"] = None
        stepTwoConfig["config"] = None
        stepTwoConfig["md5hash"] = "eb1c38cf50e14cf9fc31278a5c8e580f"
        stepTwoConfig["pset_hash"] = "7c856ad35f9f544839d8525ca10259a7"
        stepTwoConfig["owner"] = {"group": "cmsdataops", "user": "sfoulkes"}
        stepTwoConfig["pset_tweak_details"] ={"process": {"outputModules_": ["RECODEBUGoutput", "DQMoutput"],
                                                          "RECODEBUGoutput": {"dataset": {"filterName": "",
                                                                                          "dataTier": "RECO-DEBUG-OUTPUT"}},
                                                          "DQMoutput": {"dataset": {"filterName": "",
                                                                                    "dataTier": "DQM"}}}}

        stepThreeConfig = Document()
        stepThreeConfig["info"] = None
        stepThreeConfig["config"] = None
        stepThreeConfig["md5hash"] = "eb1c38cf50e14cf9fc31278a5c8e580f"
        stepThreeConfig["pset_hash"] = "7c856ad35f9f544839d8525ca10259a7"
        stepThreeConfig["owner"] = {"group": "cmsdataops", "user": "sfoulkes"}
        stepThreeConfig["pset_tweak_details"] ={"process": {"outputModules_": ["aodOutputModule"],
                                                            "aodOutputModule": {"dataset": {"filterName": "",
                                                                                            "dataTier": "AODSIM"}}}}        
        stepOne = self.configDatabase.commitOne(stepOneConfig)[0]["id"]
        stepTwo = self.configDatabase.commitOne(stepTwoConfig)[0]["id"]
        stepThree = self.configDatabase.commitOne(stepThreeConfig)[0]["id"]        
        return (stepOne, stepTwo, stepThree)

    def testReDigiInsertion(self):
        """
        _testReDigiInsertion_

        Verify that the ReDigi workflow is correctly inserted into ACDC.
        """
        defaultArguments = getTestArguments()
        defaultArguments["CouchURL"] = os.environ["COUCHURL"]
        defaultArguments["CouchDBName"] = "datacollectionsvc_t_cc"
        configs = self.injectReDigiConfigs()
        defaultArguments["StepOneConfigCacheID"] = configs[0]
        defaultArguments["StepTwoConfigCacheID"] = configs[1]
        defaultArguments["StepThreeConfigCacheID"] = configs[2]

        testWorkload = reDigiWorkload("TestWorkload", defaultArguments)
        testWorkload.setSpecUrl("somespec")
        testWorkload.setOwnerDetails("sfoulkes@fnal.gov", "DWMWM")        

        dcs = DataCollectionService(url = self.testInit.couchUrl, database = "wmcore-acdc-datacollectionsvc")
        dcs.createCollection(testWorkload)

        dataCollections = dcs.listDataCollections()
        self.assertEqual(len(dataCollections), 1,
                         "Error: There should only be one data collection.")

        for taskName in testWorkload.listAllTaskPathNames():
            if taskName.find("Cleanup") != -1 or taskName.find("LogCollect") != -1:
                # We don't insert cleanup and logcollect tasks into ACDC.
                continue

            taskFilesets = [ x for x in dcs.filesetsByTask(dataCollections[0], taskName)]
            self.assertEqual(len(taskFilesets), 1,
                             "Error: Fileset is missing.")

        testWorkload.truncate("ACDC_Round_1", "/TestWorkload/ReDigi", os.environ["COUCHURL"],
                              "wmcore-acdc-datacollectionsvc")
        dcs.createCollection(testWorkload)

        dataCollections = dcs.listDataCollections()
        self.assertEqual(len(dataCollections), 2,
                         "Error: There should only be two data collections.")
        for dataCollection in dataCollections:
            if dataCollection["name"] == "ACDC_Round_1":
                break

        for taskName in testWorkload.listAllTaskPathNames():
            if taskName.find("Cleanup") != -1 or taskName.find("LogCollect") != -1:
                # We don't insert cleanup and logcollect tasks into ACDC.
                continue

            taskFilesets = [ x for x in dcs.filesetsByTask(dataCollection, taskName)]
            self.assertEqual(len(taskFilesets), 1,
                             "Error: Fileset is missing.")

        testWorkload.truncate("ACDC_Round_2", "/ACDC_Round_1/ReDigi/ReDigiMergeRAWDEBUGoutput/ReDigiReReco", os.environ["COUCHURL"],
                              "wmcore-acdc-datacollectionsvc")
        dcs.createCollection(testWorkload)

        dataCollections = dcs.listDataCollections()
        self.assertEqual(len(dataCollections), 3,
                         "Error: There should only be two data collections.")
        for dataCollection in dataCollections:
            if dataCollection["name"] == "ACDC_Round_2":
                break

        for taskName in testWorkload.listAllTaskPathNames():
            if taskName.find("Cleanup") != -1 or taskName.find("LogCollect") != -1:
                # We don't insert cleanup and logcollect tasks into ACDC.
                continue

            taskFilesets = [ x for x in dcs.filesetsByTask(dataCollection, taskName)]
            self.assertEqual(len(taskFilesets), 1,
                             "Error: Fileset is missing.")
        
        return

    def testA(self):
        """
        test creating collections and filesets based off a workload.
        """
        workload = self.createTestWorkload()
        
        dcs = DataCollectionService(url = self.testInit.couchUrl, database = "wmcore-acdc-datacollectionsvc")
        dcs.createCollection(workload)
        
        colls = [c for c in dcs.listDataCollections()]
        self.assertEqual(len(colls), 1)
        coll = colls[0]
        
        recofs = [ x for x in dcs.filesetsByTask(coll, "/ACDCTest/reco")]
        skimfs = [ x for x in dcs.filesetsByTask(coll, "/ACDCTest/reco/skim1")]
        self.assertEqual(len(recofs), 1)
        self.assertEqual(len(skimfs), 1)
        
        
        
        job = Job('eb9b6afc-a175-11df-9ef3-00221959e7c0')
        job['task'] = workload.getTask("reco").getPathName()
        job['workflow'] = workload.name()
        job['location'] = "T1_US_FNAL"
        job['input_files'] 

        numberOfFiles = 10
        run = Run(10000000, 1,2,3,4,5,6,7,8,9,10)
        for i in range(0, numberOfFiles):
            f = File(lfn = "/store/test/some/dataset/%s.root" % makeUUID(), size = random.randint(100000000,50000000000), 
                     events = random.randint(1000, 5000))
            f.setLocation("cmssrm.fnal.gov")
            f.addRun(run)
            job.addFile(f)
        
        dcs.failedJobs([job])
        
        res = dcs.filesetsByTask(coll, "/ACDCTest/reco")
        nFiles = 0
        for x in res:
            for f in x.files():
                nFiles += 1

        self.assertEqual(nFiles, numberOfFiles)

    def testChunking(self):
        """
        _testChunking_

        Insert a workload and files that have several distinct sets of
        locations.  Verify that the chunks are created correctly and that they
        only groups files that have the same set of locations.  Also verify that
        the chunks are pulled out of ACDC correctly.
        """
        workload = self.createTestWorkload()
        dcs = DataCollectionService(url = self.testInit.couchUrl, database = "wmcore-acdc-datacollectionsvc")
        dcs.createCollection(workload)        

        def getJob(workload):
            job = Job()
            job["task"] = workload.getTask("reco").getPathName()
            job["workflow"] = workload.name()
            job["location"] = "cmssrm.fnal.gov"
            return job

        testFileA = File(lfn = makeUUID(), size = 1024, events = 1024)
        testFileA.setLocation(["cmssrm.fnal.gov", "castor.cern.ch"])
        testFileA.addRun(Run(1, 1, 2))
        testFileB = File(lfn = makeUUID(), size = 1024, events = 1024)
        testFileB.setLocation(["cmssrm.fnal.gov", "castor.cern.ch"])
        testFileB.addRun(Run(1, 3, 4))
        testFileC = File(lfn = makeUUID(), size = 1024, events = 1024)
        testFileC.setLocation(["cmssrm.fnal.gov", "castor.cern.ch"])
        testFileC.addRun(Run(1, 5, 6))
        testJobA = getJob(workload)
        testJobA.addFile(testFileA)
        testJobA.addFile(testFileB)
        testJobA.addFile(testFileC)

        testFileD = File(lfn = makeUUID(), size = 1024, events = 1024)
        testFileD.setLocation(["cmssrm.fnal.gov"])
        testFileD.addRun(Run(2, 1, 2))
        testFileE = File(lfn = makeUUID(), size = 1024, events = 1024)
        testFileE.setLocation(["cmssrm.fnal.gov"])
        testFileE.addRun(Run(2, 3, 4))
        testJobB = getJob(workload)
        testJobB.addFile(testFileD)
        testJobB.addFile(testFileE)

        testFileF = File(lfn = makeUUID(), size = 1024, events = 1024,
                         parents = set(["/some/parent/F"]))
        testFileF.setLocation(["cmssrm.fnal.gov", "castor.cern.ch", "srm.ral.uk"])
        testFileF.addRun(Run(3, 1, 2))
        testFileG = File(lfn = makeUUID(), size = 1024, events = 1024,
                         parents = set(["/some/parent/G"]))
        testFileG.setLocation(["cmssrm.fnal.gov", "castor.cern.ch", "srm.ral.uk"] )
        testFileG.addRun(Run(3, 3, 4))
        testFileH = File(lfn = makeUUID(), size = 1024, events = 1024,
                         parents = set(["/some/parent/H"]))
        testFileH.setLocation(["cmssrm.fnal.gov", "castor.cern.ch", "srm.ral.uk"])
        testFileH.addRun(Run(3, 5, 6))
        testJobC = getJob(workload)
        testJobC.addFile(testFileF)
        testJobC.addFile(testFileG)
        testJobC.addFile(testFileH)

        testFileI = File(lfn = makeUUID(), size = 1024, events = 1024, merged = True)
        testFileI.setLocation(["cmssrm.fnal.gov", "castor.cern.ch"])
        testFileI.addRun(Run(4, 1, 2))
        testFileJ = File(lfn = makeUUID(), size = 1024, events = 1024, merged = True)
        testFileJ.setLocation(["cmssrm.fnal.gov", "castor.cern.ch"] )
        testFileJ.addRun(Run(4, 3, 4))
        testFileK = File(lfn = makeUUID(), size = 1024, events = 1024, merged = True)
        testFileK.setLocation(["cmssrm.fnal.gov", "castor.cern.ch"])
        testFileK.addRun(Run(4, 5, 6))
        testJobD = getJob(workload)
        testJobD.addFile(testFileI)
        testJobD.addFile(testFileJ)
        testJobD.addFile(testFileK)

        dcs.failedJobs([testJobA, testJobB, testJobC, testJobD])
        dataCollection = dcs.getDataCollection(workload.name())
        chunks = dcs.chunkFileset(dataCollection, "/ACDCTest/reco",
                                  chunkSize = 5)

        self.assertEqual(len(chunks), 4, "Error: There should be four chunks.")
        
        goldenMetaData = {1: {"lumis": 2, "locations": ["castor.cern.ch", "cmssrm.fnal.gov"], "events": 1024},
                          2: {"lumis": 4, "locations": ["cmssrm.fnal.gov"], "events": 2048},
                          3: {"lumis": 6, "locations": ["castor.cern.ch", "cmssrm.fnal.gov", "srm.ral.uk"], "events": 3072},
                          5: {"lumis": 10, "locations": ["castor.cern.ch", "cmssrm.fnal.gov"], "events": 5120}}

        testFiles =[testFileA, testFileB, testFileC, testFileI, testFileJ, testFileK]
        lastFile = testFileA
        for testFile in testFiles:
            if lastFile["lfn"] < testFile["lfn"]:
                lastFile = testFile

        testFiles.remove(lastFile)
            
        goldenFiles = {1: [lastFile],
                       2: [testFileD, testFileE],
                       3: [testFileF, testFileG, testFileH],
                       5: testFiles}

        for chunk in chunks:
            chunkMetaData = dcs.getChunkInfo(dataCollection, "/ACDCTest/reco",
                                             chunk["offset"], chunk["files"])

            self.assertEqual(chunkMetaData["files"], chunk["files"],
                             "Error: Metadata doesn't match.")
            self.assertEqual(chunkMetaData["lumis"], chunk["lumis"],
                             "Error: Metadata doesn't match.")            
            self.assertEqual(chunkMetaData["events"], chunk["events"],
                             "Error: Metadata doesn't match.")
            self.assertEqual(chunkMetaData["locations"], chunk["locations"],
                             "Error: Metadata doesn't match.")            
            
            self.assertTrue(chunk["files"] in goldenMetaData.keys(),
                            "Error: Extra chunk found.")
            self.assertEqual(chunk["lumis"], goldenMetaData[chunk["files"]]["lumis"],
                             "Error: Lumis in chunk is wrong.")
            self.assertEqual(chunk["locations"], goldenMetaData[chunk["files"]]["locations"],
                             "Error: Locations in chunk is wrong.")
            self.assertEqual(chunk["events"], goldenMetaData[chunk["files"]]["events"],
                             "Error: Events in chunk is wrong.")
            del goldenMetaData[chunk["files"]]
            
            chunkFiles = dcs.getChunkFiles(dataCollection, "/ACDCTest/reco",
                                           chunk["offset"], chunk["files"])

            self.assertTrue(chunk["files"] in goldenFiles.keys(),
                            "Error: Extra chunk found.")
            goldenChunkFiles = goldenFiles[chunk["files"]]            
            self.assertEqual(len(chunkFiles), len(goldenChunkFiles))

            for chunkFile in chunkFiles:
                foundFile = None
                for goldenChunkFile in goldenChunkFiles:
                    if chunkFile["lfn"] == goldenChunkFile["lfn"]:
                        foundFile = goldenChunkFile
                        break
                        
                self.assertTrue(foundFile != None,
                                "Error: Missing chunk file: %s, %s" % (chunkFiles, goldenChunkFiles))
                self.assertEqual(foundFile["parents"], chunkFile["parents"],
                                 "Error: File parents should match.")
                self.assertEqual(foundFile["merged"], chunkFile["merged"],
                                 "Error: File merged status should match.")
                self.assertEqual(foundFile["locations"], chunkFile["locations"],
                                 "Error: File locations should match.")
                self.assertEqual(foundFile["events"], chunkFile["events"],
                                 "Error: File locations should match: %s" % chunk["files"])
                self.assertEqual(foundFile["size"], chunkFile["size"],
                                 "Error: File locations should match.")
                self.assertEqual(len(foundFile["runs"]), len(chunkFile["runs"]),
                                 "Error: Wrong number of runs.")
                for run in foundFile["runs"]:
                    runMatch = False
                    for chunkRun in chunkFile["runs"]:
                        if chunkRun.run == run.run and chunkRun.lumis == run.lumis:
                            runMatch = True
                            break

                    self.assertTrue(runMatch, "Error: Run information is wrong.")
                    
            del goldenFiles[chunk["files"]]
        
        return
        
    def testGetLumiWhitelist(self):
        """
        _testGetLumiWhitelist_

        Verify that the ACDC whitelist generation code works correctly.  We'll
        add jobs with the following lumi info:
          # Run 1, lumis [1, 2, 3], [4, 6], [7], [9], [11, 12]
          # Run 2, lumis [5, 6, 7], [10, 11, 12], [15]
          # Run 3, lumis [20]

        And should get out a whitelist that looks like this:
          {"1": [[1, 4], [6, 7], [9, 9], [11, 12]],
           "2": [[5, 7], [10, 12], [15, 15]],
           "3": [[20, 20]]}
        """
        dcs = DataCollectionService(url = self.testInit.couchUrl, database = "wmcore-acdc-datacollectionsvc")

        workload = self.createTestWorkload()
        collection = dcs.createCollection(workload)

        def getJob(workload):
            job = Job()
            job["task"] = workload.getTask("reco").getPathName()
            job["workflow"] = workload.name()
            job["location"] = "cmssrm.fnal.gov"
            return job

        testFileA = File(lfn = makeUUID(), size = 1024, events = 1024)
        testFileA.addRun(Run(1, 1, 2))
        testFileB = File(lfn = makeUUID(), size = 1024, events = 1024)
        testFileB.addRun(Run(1, 3))
        testJobA = getJob(workload)
        testJobA.addFile(testFileA)
        testJobA.addFile(testFileB)

        testFileC = File(lfn = makeUUID(), size = 1024, events = 1024)
        testFileC.addRun(Run(1, 4, 6))
        testJobB = getJob(workload)
        testJobB.addFile(testFileC)
        
        testFileD = File(lfn = makeUUID(), size = 1024, events = 1024)
        testFileD.addRun(Run(1, 7))
        testJobC = getJob(workload)
        testJobC.addFile(testFileD)
                         
        testFileE = File(lfn = makeUUID(), size = 1024, events = 1024)
        testFileE.addRun(Run(1, 11, 12))
        testJobD = getJob(workload)
        testJobD.addFile(testFileE)

        testFileF = File(lfn = makeUUID(), size = 1024, events = 1024)
        testFileF.addRun(Run(2, 5, 6, 7))
        testJobE = getJob(workload)
        testJobE.addFile(testFileF)

        testFileG = File(lfn = makeUUID(), size = 1024, events = 1024)
        testFileG.addRun(Run(2, 10, 11, 12))
        testJobF = getJob(workload)
        testJobF.addFile(testFileG)

        testFileH = File(lfn = makeUUID(), size = 1024, events = 1024)
        testFileH.addRun(Run(2, 15))
        testJobG = getJob(workload)
        testJobG.addFile(testFileH)

        testFileI = File(lfn = makeUUID(), size = 1024, events = 1024)
        testFileI.addRun(Run(3, 20))
        testJobH = getJob(workload)
        testJobH.addFile(testFileI)

        testFileJ = File(lfn = makeUUID(), size = 1024, events = 1024)
        testFileJ.addRun(Run(1, 9))
        testJobI = getJob(workload)
        testJobI.addFile(testFileJ)
        
        dcs.failedJobs([testJobA, testJobB, testJobC, testJobD, testJobE,
                        testJobF, testJobG, testJobH, testJobI])
        whiteList = dcs.getLumiWhitelist(collection["collection_id"],
                                         workload.getTask("reco").getPathName())

        self.assertEqual(len(whiteList.keys()), 3,
                         "Error: There should be 3 runs.")
        self.assertEqual(whiteList["1"], [[1, 4], [6, 7], [9, 9], [11, 12]],
                         "Error: Whitelist for run 1 is wrong.")
        self.assertEqual(whiteList["2"], [[5, 7], [10, 12], [15, 15]],
                         "Error: Whitelist for run 2 is wrong.")
        self.assertEqual(whiteList["3"], [[20, 20]],
                         "Error: Whitelist for run 3 is wrong.")
        return

if __name__ == '__main__':
    unittest.main()
