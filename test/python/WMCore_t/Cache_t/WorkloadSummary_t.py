#!/usr/bin/env python
# encoding: utf-8
"""
WorkloadSummary_t.py

Created by Dave Evans on 2011-01-07.
Copyright (c) 2011 Fermilab. All rights reserved.

Test WorkloadSummary couchapp and related bits of workload

"""

import unittest
import random
from WMQuality.TestInitCouchApp import TestInitCouchApp
from WMCore.WMSpec.WMWorkload import newWorkload, WMWorkloadHelper
from WMCore.Cache.WorkloadSummary import WorkloadSummary
from WMCore.Services.UUID import makeUUID

def populateWorkload(workload, owner):
    """
    add some structure to the test workload
    """
    reco = workload.newTask("reco")
    workload.setOwnerDetails(owner, "DMWM")

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
    for n in range(0, random.randint(1, 8)):
        skimN = reco.addTask("skim%s" % n)
        cmsRunSkim = skimN.makeStep("cmsRun%s" % n)
        cmsRunSkim.setStepType("CMSSW")
        skimN.applyTemplates()        
        skimN.setInputReference(cmsRunReco, outputModule = "outputRECO")
        skimNHelper = cmsRunSkim.getTypeHelper()
        skimNHelper.addOutputModule("outputSkim%s" % n,
            primaryDataset = "PRIMARY",
            processedDataset = "processed-skim%s-v1" % n,
            dataTier = "TIER3",
            lfnBase = "/store/dunkindonuts",
            mergedLFNBase = "/store/kfc"   
        )

class WorkloadSummary_t(unittest.TestCase):
    def setUp(self):
        """bootstrap tests"""
        self.testInit = TestInitCouchApp(__file__)
        self.testInit.setLogging()
        #self.testInit.setDatabaseConnection()
        #self.testInit.setSchema(customModules = ["WMCore.WMBS"],
        #                        useDefault = False)
        self.testInit.setupCouch("wmcore-workloadsummary", "WorkloadSummary")
        
        self.workload1 = newWorkload("WorkloadSummaryTest1")
        self.workload2 = newWorkload("WorkloadSummaryTest2")

        
    def tearDown(self):
        self.testInit.tearDownCouch()
        self.testInit.clearDatabase()
        pass    

    def testA(self):
        """
        register workloads in the couchapp
        and pulling back information from the views
        """
        populateWorkload(self.workload1, "evansde77")
        populateWorkload(self.workload2, "drsm79")
        
        summary1 = self.workload1.generateWorkloadSummary()
        summary2 = self.workload2.generateWorkloadSummary()
        
        summ1 = WorkloadSummary(self.workload1.name(), self.testInit.couchUrl, self.testInit.couchDbName, self.workload1)
        summ2 = WorkloadSummary(self.workload2.name(), self.testInit.couchUrl, self.testInit.couchDbName, self.workload2)
        summ1.create()
        summ2.create()
        
        summ1.addACDCCollection(makeUUID())
        for t in self.workload1.listAllTaskPathNames():
            fakeDoc = makeUUID()
            summ1.addACDCFileset(t, fakeDoc)
            
    
if __name__ == '__main__':
    unittest.main()