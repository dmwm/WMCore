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
        
        self.workload = newWorkload("ACDCTest")

        
    def tearDown(self):
        self.testInit.tearDownCouch()
        self.testInit.clearDatabase()
        


    def testA(self):
        """
        test creating collections and filesets based off a workload.
        """
        reco = self.workload.newTask("reco")
        skim1 = reco.addTask("skim1")
        self.workload.setOwnerDetails("evansde77", "DMWM")

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
                                        
        
        try:                                
            dcs = DataCollectionService(url = self.testInit.couchUrl, database = self.testInit.couchDbName)
        except Exception, ex:
            msg = 'Failed to instantiate DataCollectionService: %s' % str(ex)
            self.fail(msg)
        try:
            dcs.createCollection(self.workload)
        except Exception, ex:
            msg = "Failed to create Data Collection from Workload:%s" % str(ex)
            self.fail(msg)
        
        colls = [c for c in dcs.listDataCollections()]
        self.assertEqual(len(colls), 1)
        coll = colls[0]
        
        recofs = [ x for x in dcs.filesetsByTask(coll, reco.getPathName())]
        skimfs = [ x for x in dcs.filesetsByTask(coll, skim1.getPathName())]
        self.assertEqual(len(recofs), 1)
        self.assertEqual(len(skimfs), 1)
        
        
        
        job = Job('eb9b6afc-a175-11df-9ef3-00221959e7c0')
        job['task'] = reco.getPathName()
        job['workflow'] = self.workload.name()
        job['location'] = "T1_US_FNAL"
        job['input_files'] 

        numberOfFiles = 10
        run = Run(10000000, 1,2,3,4,5,6,7,8,9,10)
        for i in range(0, numberOfFiles):
            f = File( lfn = "/store/test/some/dataset/%s.root" % makeUUID(), size = random.randint(100000000,50000000000), 
                      events = random.randint(1000, 5000))
            f.addRun(run)
            job.addFile(f)
        
        try:
            dcs.failedJobs([job])
        except Exception, ex:
            msg = "Error calling failedJobs method in DataCollectionService: %s" % str(ex)
            self.fail(msg)
        
        
        

        
        

if __name__ == '__main__':
    unittest.main()