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
from WMCore.WMBS.File import File
from WMCore.DataStructs.Run import Run
from WMCore.Services.UUID import makeUUID

class DataCollectionService_t(unittest.TestCase):
    def setUp(self):
        """bootstrap tests"""
        self.testInit = TestInitCouchApp(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema(customModules = ["WMCore.WMBS"],
                                useDefault = False)
        self.testInit.setupCouch("wmcore-acdc-datacollectionsvc", "GroupUser", "ACDC")
        return
        
    def tearDown(self):
        self.testInit.tearDownCouch()
        self.testInit.clearDatabase()
        return

    def createTestWorkload(self):
        """
        _createTestWorkload_

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

    def testA(self):
        """
        test creating collections and filesets based off a workload.
        """
        workload = self.createTestWorkload()
        
        try:                                
            dcs = DataCollectionService(url = self.testInit.couchUrl, database = self.testInit.couchDbName)
        except Exception, ex:
            msg = 'Failed to instantiate DataCollectionService: %s' % str(ex)
            self.fail(msg)
        try:
            dcs.createCollection(workload)
        except Exception, ex:
            msg = "Failed to create Data Collection from Workload:%s" % str(ex)
            self.fail(msg)
        
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
            f = File( lfn = "/store/test/some/dataset/%s.root" % makeUUID(), size = random.randint(100000000,50000000000), 
                      events = random.randint(1000, 5000))
            f.addRun(run)
            job.addFile(f)
        
        dcs.failedJobs([job])

        res = dcs.filesetsByTask(coll, "/ACDCTest/reco")
        nFiles = 0
        for x in res:
            for f in x.files():
                nFiles += 1

        self.assertEqual(nFiles, numberOfFiles)
        
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
        dcs = DataCollectionService(url = self.testInit.couchUrl, database = self.testInit.couchDbName)        

        workload = self.createTestWorkload()
        collection = dcs.createCollection(workload)

        def getJob(workload):
            job = Job()
            job["task"] = workload.getTask("reco").getPathName()
            job["workflow"] = workload.name()
            job["location"] = "T1_US_FNAL"
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
