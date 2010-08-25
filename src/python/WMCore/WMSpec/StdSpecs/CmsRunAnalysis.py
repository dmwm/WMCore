#!/usr/bin/env python
# encoding: utf-8
"""CmsRunAnalysis
CmsRunAnalysis.py

Created by Dave Evans on 2010-03-25.
Copyright (c) 2010 Fermilab. All rights reserved.
"""

import sys
import os
import unittest

from WMCore.WMSpec.WMWorkload import newWorkload
from WMCore.WMSpec.WMStep import makeWMStep
from WMCore.WMSpec.Steps.StepFactory import getStepTypeHelper

parseDataset = lambda x : { "Primary" : x.split("/")[1], 
                            "Processed": x.split("/")[2],
                            "Tier" : x.split("/")[3]}


class CmsRunAnalysis:
    """
    _CmsRunAnalysis_
    
    Util object to build a "normal" CMSSW/cmsRun based analysis
    """
    def __init__(self):
        pass

    def __call__(self, workloadName, arguments):
        """
        _operator()_

        Standard way of building a cmsRun based analysis workload.
        Inspiration from: http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/COMP/CRAB/python/full_crab.cfg?view=markup

        """
        lfnCategory = "/store/user/%s" % arguments["Username"]
        lfnBase = "%s/%s" % (lfnCategory, workloadName)
        
        
        cmsswVersion = arguments.get("CMSSWVersion", "CMSSW_3_3_5_patch3")
        scramArchitecture = arguments.get("ScramArch", "slc5_ia32_gcc434")
        jobSplittingAlgo = arguments.get("JobSplittingAlgorithm", "EventBased")
        jobSplittingParams = arguments.get("JobSplittingArgs", {"events_per_job": 1000})
        
        # ConfigCache specified by URL of the ConfigCache service, and ID of the document
        # with the configuration
        configCacheHost = arguments.get("ConfigCacheURL")
        configCacheDoc  = arguments.get("ConfigCacheDoc")


        #    //
        #  //  Map of Output module names to output dataset name to be published
        #//
        outputModules = arguments.get("OutputModules", {})
        
        #    //
        #  // todo: User controls to handle Analysis File output?
        #//

        #  //
        # // Input Data selection
        #//
        datasetElements = parseDataset(arguments['InputDataset'])
        inputPrimaryDataset = datasetElements['Primary']
        inputProcessedDataset = datasetElements['Processed']
        inputDataTier = datasetElements['Tier']

        siteWhitelist = arguments.get("SiteWhitelist", [])
        siteBlacklist = arguments.get("SiteBlacklist", [])
        blockBlacklist = arguments.get("BlockBlacklist", [])
        blockWhitelist = arguments.get("BlockWhitelist", [])    
        runWhitelist = arguments.get("RunWhitelist", [])
        runBlacklist = arguments.get("RunBlacklist", [])    

        #  //
        # // Enabling Emulation from the Request allows some nice diagnostic tests
        #//
        emulationMode = arguments.get("Emulate", False)

        #  //
        # // likely to be ~stable
        #//
        dbsUrl = arguments.get("DBSURL","http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet" )
        softwareInitCommand = arguments.get("SoftwareInitCommand", " . /uscmst1/prod/sw/cms/shrc prod")
        
        workload = newWorkload(workloadName)
    
        
        analysis = workload.newTask("Analysis")
        analysisCmssw = analysis.makeStep("cmsRun1")
        analysisCmssw.setStepType("CMSSW")
        analysisStageOut = analysisCmssw.addStep("stageOut1")
        analysisStageOut.setStepType("StageOut")
        analysisLogArch = analysisCmssw.addStep("logArch1")
        analysisLogArch.setStepType("LogArchive")
        analysis.applyTemplates()
        analysis.setSplittingAlgorithm(jobSplittingAlgo, **jobSplittingParams)
        analysis.addGenerator("BasicNaming")
        analysis.addGenerator("BasicCounter")
        analysis.setTaskType("Analysis")




        #  //
        # // cmssw step
        #//
        #
        # TODO: Anywhere helper.data is accessed means we need a method added to the
        # type based helper class to provide a clear API.
        analysisCmsswHelper = analysisCmssw.getTypeHelper()
        analysis.addInputDataset(
            primary = inputPrimaryDataset,
            processed = inputProcessedDataset,
            tier = inputDataTier,
            dbsurl = dbsUrl,
            block_blacklist = blockBlacklist,
            block_whitelist = blockWhitelist,
            run_blacklist = runBlacklist,
            run_whitelist = runWhitelist
            )
        analysis.data.constraints.sites.whitelist = siteWhitelist
        analysis.data.constraints.sites.blacklist = siteBlacklist


        analysisCmsswHelper.cmsswSetup(
            cmsswVersion,
            softwareEnvironment = softwareInitCommand ,
            scramArch = scramArchitecture
            )

        analysisCmsswHelper.setConfigCache(configCacheHost, configCacheDoc)
        
        #  //
        # // Add output module records
        #//
        for outModName, datasetName in outputModules.items():
            datasetElems = parseDataset(datasetName)
            inputPrimaryDataset = datasetElements['Primary']
            inputProcessedDataset = datasetElements['Processed']
            inputDataTier = datasetElements['Tier']
            analysisCmsswHelper.addOutputModule(
                outModName, primaryDataset = datasetElems['Primary'],
                processedDataset = datasetElems['Processed'],
                dataTier = datasetElems['Tier'],
                lfnBase = "%s%s" % ( lfnBase, datasetName)
            )   
        
        
        return workload



class CmsRunAnalysisTests(unittest.TestCase):
    def setUp(self):
        self.arguments = {
            'Username' : "evansde77",
            "CMSSWVersion" : "CMSSW_3_3_5_patch3",
            "ScramArch" : "slc5_ia32_gcc434",
            "InputDataset" : "/MinimumBias/BeamCommissioning09-v1/RAW",
            "Emulate" : True,
            "OutputModules" : { 
                "writeData1": "/%sUserOutput/SampleAnalysisOutput1/USER" % "evansde77",
                "writeData2": "/%sUserOutput/SampleAnalysisOutput2/USER" % "evansde77",
                "writeData3": "/%sUserOutput/SampleAnalysisOutput3/USER" % "evansde77",
            },
            #  //
            # // These probably come from the client uploading the config to the agent
            #//  rather than the user themselves
            #  //
            # //  For testing purposes 
            #//
            "ConfigCacheURL" : "127.0.0.1:5984",
            "ConfigCacheDoc" : "2589b86441f8a1a9eed24ff36b63722b",
        }



    def testA(self):
        """create a CmsRunAnalysis workload"""
        
        cmsRunAna = CmsRunAnalysis()
        workload = cmsRunAna("SampleAnalysis", self.arguments)

        print workload.data
        

if __name__ == '__main__':
    unittest.main()