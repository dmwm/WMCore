#!/usr/bin/env python
"""
_MonteCarlo_

Created by Dave Evans on 2010-08-17.
Copyright (c) 2010 Fermilab. All rights reserved.
"""

import os

from WMCore.WMSpec.StdSpecs.StdBase import StdBase

def getTestArguments():
    """
    _getTestArguments_

    """
    args = {}
    args["AcquisitionEra"] = "CSA2010"
    args["Requestor"] = "sfoulkes@fnal.gov"
    args["ScramArch"] =  "slc5_ia32_gcc434"
    args["PrimaryDataset"] = "MonteCarloData"    
    args["ProcessingVersion"] = "v2scf"
    args["GlobalTag"] = None
    args["RequestSizeEvents"] = 10
    
    args["CouchURL"] = os.environ.get("COUCHURL", None)
    args["CouchDBName"] = "scf_wmagent_configcache"

    args["CMSSWVersion"] = "CMSSW_3_8_1"
    args["ProdConfigCacheID"] = "f90fc973b731a37c531f6e60e6c57955"
    return args

class MonteCarloWorkloadFactory(StdBase):
    """
    _MonteCarloWorkloadFactory_

    Stamp out Monte Carlo workflows.
    """
    def __init__(self):
        StdBase.__init__(self)

    def buildWorkload(self):
        """
        _buildWorkload_

        Build a workflow for a MonteCarlo request.  This means a production
        config and merge tasks for each output module.
        """
        workload = self.createWorkload()
        workload.setWorkQueueSplitPolicy("MonteCarlo", self.prodJobSplitAlgo, self.prodJobSplitArgs)
        prodTask = workload.newTask("Production")

        outputMods = self.setupProcessingTask(prodTask, "Production", None,
                                              couchURL = self.couchURL, couchDBName = self.couchDBName,
                                              configDoc = self.prodConfigCacheID, splitAlgo = self.prodJobSplitAlgo,
                                              splitArgs = self.prodJobSplitArgs,
                                              seeding = self.seeding, totalEvents = self.totalEvents) 
        self.addLogCollectTask(prodTask)

        prodMergeTasks = {}
        for outputModuleName in outputMods.keys():
            outputModuleInfo = outputMods[outputModuleName]
            self.addMergeTask(prodTask, self.prodJobSplitAlgo,
                              outputModuleName, outputModuleInfo["dataTier"],
                              outputModuleInfo["filterName"],
                              outputModuleInfo["processedDataset"])

        return workload
        
    def __call__(self, workloadName, arguments):
        """
        Create a workload instance for a MonteCarlo request
        
        """
        StdBase.__call__(self, workloadName, arguments)

        # Required parameters that must be specified by the Requestor.
        self.inputPrimaryDataset = arguments["PrimaryDataset"]
        self.frameworkVersion = arguments["CMSSWVersion"]
        self.globalTag = arguments["GlobalTag"]
        self.totalEvents = arguments["RequestSizeEvents"]
        self.seeding = arguments.get("Seeding", "AutomaticSeeding")
        self.prodConfigCacheID = arguments["ProdConfigCacheID"]

        # The CouchURL and name of the ConfigCache database must be passed in
        # by the ReqMgr or whatever is creating this workflow.
        self.couchURL = arguments["CouchURL"]
        self.couchDBName = arguments["CouchDBName"]        

        # Optional arguments that default to something reasonable.
        self.dbsUrl = arguments.get("DbsUrl", "http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet")
        self.emulation = arguments.get("Emulation", False)

        # These are mostly place holders because the job splitting algo and
        # parameters will be updated after the workflow has been created.
        self.prodJobSplitAlgo  = arguments.get("ProdJobSplitAlgo", "EventBased")
        self.prodJobSplitArgs  = arguments.get("ProdJobSplitArgs",
                                               {"events_per_job": 1000})
        
        return self.buildWorkload()
