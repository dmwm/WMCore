#!/usr/bin/env python
# encoding: utf-8
# pylint: disable-msg=C0301,W0142


import time

##from WMCore.Configuration import ConfigSection
from WMCore.WMSpec.StdSpecs.StdBase import StdBase


def getTestArguments():
    """generate some test data"""
    args = {}
    args['Requestor'] = "mmascher"
    args['Username'] = "mmascher"
    args['ProcessingVersion'] = ""
    args['RequestorDN'] = "/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=mmascher/CN=720897/CN=Marco Mascheroni"
    args['InputDataset'] = "/RelValProdTTbar/JobRobot-MC_3XY_V24_JobRobot-v1/GEN-SIM-DIGI-RECO"
    args["ScramArch"] =  "slc5_ia32_gcc434"
    args['CMSSWVersion'] = "CMSSW_4_2_0"
    
    return args


class AnalysisWorkloadFactory(StdBase):
    """
    Analysis workload.
    
    """
    def __init__(self):
        StdBase.__init__(self)

    def buildWorkload(self):
        """
        _buildWorkload_

        Build a workflow for Analysis  requests.
        
        """
        workload = self.createWorkload()
        analysisTask = workload.newTask("Analysis")

        outputMods = self.setupProcessingTask(analysisTask, "Analysis", inputDataset=self.inputDataset,
                                              couchURL = self.couchURL, couchDBName = self.couchDBName,
                                              configDoc = self.analysisConfigCacheID, splitAlgo = self.analysisJobSplitAlgo,
                                              splitArgs = self.analysisJobSplitArgs, \
                                              userDN = self.owner_dn, asyncDest = self.asyncDest, publishName = self.publishName,
                                              userSandbox = self.userSandbox, userFiles = self.userFiles)
        self.addLogCollectTask(analysisTask)

        workload.setWorkQueueSplitPolicy("DatasetBlock", self.analysisJobSplitAlgo, self.analysisJobSplitArgs)

        return workload



    def __call__(self, workloadName, arguments):
        """
        Create a workload instance for an Analysis request
        
        """
        StdBase.__call__(self, workloadName, arguments)

        #TODO modify accordingly with policy for user data handlying to be defined
        lfnCategory = "/store/user/%s" % arguments["Username"]
        self.userUnmergedLFN = "%s/%s" % (lfnCategory, arguments['ProcessingVersion'])
        self.globalTag = arguments.get("GlobalTag", None)
        self.inputPrimaryDataset = None

        # Required parameters.
        self.owner = arguments["Requestor"]
        self.owner_dn = arguments["RequestorDN"]
        self.frameworkVersion = arguments["CMSSWVersion"]
        self.scramArch = arguments["ScramArch"]
        self.inputDataset = arguments['InputDataset']
        self.blockBlacklist = arguments.get("BlockBlacklist", [])
        self.blockWhitelist = arguments.get("BlockWhitelist", [])
        self.runWhitelist = arguments.get("RunWhitelist", [])
        self.runBlacklist = arguments.get("RunBlacklist", [])

        self.couchURL = arguments.get("CouchUrl", "http://derpderp:derpityderp@cmssrv52.derp.gov:5984")
        self.couchDBName = arguments.get("CouchDBName", "wmagent_config_cache")        
        self.analysisConfigCacheID = arguments.get("AnalysisConfigCacheDoc", None)
        
        # for publication
        self.dbsUrl = arguments.get("DbsUrl", "http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet")

        self.emulation = arguments.get("Emulation", False)
        
        # These are mostly place holders because the job splitting algo and
        # parameters will be updated after the workflow has been created.
        self.analysisJobSplitAlgo  = arguments.get("JobSplitAlgo", "EventBased")
        self.analysisJobSplitArgs  = arguments.get("JobSplitArgs",
                                               {"events_per_job": 1000})
        self.asyncDest = arguments.get("asyncDest", "T2_IT_Pisa")
        self.publishName = arguments.get("PublishDataName", str(int(time.time())))
        self.userSandbox = arguments.get("userSandbox", None)
        self.userFiles   = arguments.get("userFiles", [])

        return self.buildWorkload()

