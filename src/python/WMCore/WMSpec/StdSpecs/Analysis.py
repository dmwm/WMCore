#!/usr/bin/env python
# encoding: utf-8
# pylint: disable-msg=C0301,W0142

import logging
import os
import time

from WMCore.WMSpec.StdSpecs.StdBase import StdBase


def getCommonTestArgs():
    """
    _getCommonTestArgs_

    Arguments in common between Analysis and PrivateMC
    """
    args = {}
    args["AcquisitionEra"] = "CSA2010"
    args['Requestor'] = "mmascher"
    args["CouchURL"] = os.environ.get("COUCHURL", None)
    args["CouchDBName"] = "test_wmagent_configcache"
    # or alternatively CouchURL part can be replaced by ConfigCacheUrl,
    # then ConfigCacheUrl + CouchDBName + ConfigCacheID
    args["ConfigCacheUrl"] = None    
    args["ScramArch"] =  "slc5_ia32_gcc434"
    args['CMSSWVersion'] = "CMSSW_4_2_0"
    args["ProcessingVersion"] = 2
    args['DashboardHost'] = "127.0.0.1"
    args['DashboardPort'] = 8884
    return args

def getTestArguments():
    """
    _getTestArguments_

    """
    args = getCommonTestArgs()

    args['Username'] = "mmascher"
    args['RequestorDN'] = "/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=mmascher/CN=720897/CN=Marco Mascheroni"
    args['InputDataset'] = "/RelValProdTTbar/JobRobot-MC_3XY_V24_JobRobot-v1/GEN-SIM-DIGI-RECO"
    args["userSandbox"] = 'http://home.fnal.gov/~ewv/agent.tgz'

    return args

def remoteLFNPrefix(site, lfn=''):
    """
    Convert a site name to the relevant remote LFN prefix
    """
    from WMCore.Services.PhEDEx.PhEDEx import PhEDEx
    phedexJSON = PhEDEx(responseType='json')

    seName = phedexJSON.getNodeSE(site)
    uri = phedexJSON.getPFN(nodes=[site], lfns=[lfn])[(site,lfn)]

    return uri.replace(lfn, ''), seName # Don't want the actual LFN, just prefix

class AnalysisWorkloadFactory(StdBase):
    """
    Analysis workload.

    """

    def __init__(self):
        StdBase.__init__(self)
        self.requiredFields = ["CMSSWVersion", "ScramArch", "InputDataset",
                               "Requestor", "RequestorDN" , "RequestName"]

    def commonWorkload(self):
        """
        _commonWorkload_

        Settings in common between Analysis and PrivateMC
        """
        self.workload = self.createWorkload()
        self.workload.setDashboardActivity("analysis")
        self.reportWorkflowToDashboard(self.workload.getDashboardActivity())

        lfnBase = '/store/temp/user/%s' % self.userName
        self.unmergedLFNBase = lfnBase
        self.userOutBase = '%s/CRAB-Out' % lfnBase
        self.logBase = '/store/temp/user/%s/logs' % self.userName
        self.logCollBase = '/store/user/%s/CRAB-Out' % self.userName
        self.lcPrefix, self.seName = remoteLFNPrefix(site=self.asyncDest, lfn=self.logCollBase)

    def setUserOutput(self, mainTask):
        """
        _setUserOutput_

        Setup all the bits for user output, log collect, etc.
        This differs consideralbly from production

        """

        # Send everything straight to merge
        stageOut = mainTask.getStep('stageOut1')
        stageOutHelper = stageOut.getTypeHelper()
        stageOutHelper.setMinMergeSize(1, 1)

        # Put temporary log files in /store/temp/user/USERNAME/
        logArchiveStep = mainTask.getStep('logArch1')
        logArchiveStep.addOverride('altLFN',  self.logBase)

        # Set up log collecting the same as Analysis
        if self.saveLogs:
            logCollectTask = self.addLogCollectTask(mainTask)
            logCollectStep = logCollectTask.getStep('logCollect1')
            logCollectStep.addOverride('userLogs',  True)
            logCollectStep.addOverride('seName', self.seName)
            logCollectStep.addOverride('lfnBase', self.logCollBase)
            logCollectStep.addOverride('lfnPrefix', self.lcPrefix)
            logCollectStep.addOverride('dontStage', False)

        # Get the user output files we need
        cmsswStep = mainTask.getStep('cmsRun1')
        cmsswHelper = cmsswStep.getTypeHelper()
        cmsswHelper.setUserLFNBase(self.userOutBase)
        for outputFile in self.outputFiles:
            cmsswHelper.addAnalysisFile(outputFile, fileName=outputFile, lfnBase=self.userOutBase)

        return

    def buildWorkload(self):
        """
        _buildWorkload_

        Build a workflow for Analysis requests.

        """

        self.commonWorkload()
        analysisTask = self.workload.newTask("Analysis")

        (self.inputPrimaryDataset, self.inputProcessedDataset, self.inputDataTier) = self.inputDataset[1:].split("/")

        # Force ACDC input if present
        self.inputStep = None
        if self.Submission > 1: #resubmit
            analysisTask.addInputACDC(self.ACDCURL, self.ACDCDBName, self.origRequest, None)
            self.inputDataset = None
            self.workload.setWorkQueueSplitPolicy("ResubmitBlock", self.analysisJobSplitAlgo, self.analysisJobSplitArgs)
        else:
            self.workload.setWorkQueueSplitPolicy("Block", self.analysisJobSplitAlgo, self.analysisJobSplitArgs)

        outputMods = self.setupProcessingTask(analysisTask, "Analysis", inputDataset=self.inputDataset,
                                              inputStep=self.inputStep,
                                              couchURL = self.couchURL, couchDBName = self.couchDBName,
                                              configCacheUrl = self.configCacheUrl,
                                              configDoc = self.configCacheID, splitAlgo = self.analysisJobSplitAlgo,
                                              splitArgs = self.analysisJobSplitArgs, \
                                              userDN = self.owner_dn, asyncDest = self.asyncDest,
                                              owner_vogroup = self.owner_vogroup, owner_vorole = self.owner_vorole,
                                              userSandbox = self.userSandbox, userFiles = self.userFiles)

        self.setUserOutput(analysisTask)
        return self.workload

    def __call__(self, workloadName, arguments):
        """
        Create a workload instance for an Analysis request

        """

        StdBase.__call__(self, workloadName, arguments)

        # Parameters for users
        self.owner_vogroup = arguments.get("VoGroup", '')
        self.owner_vorole = arguments.get("VoRole", '')
        self.userSandbox = arguments.get("userSandbox", None)
        self.userFiles = arguments.get("userFiles", [])
        self.outputFiles = arguments.get("OutputFiles", [])
        self.userName = arguments.get("Username",'jblow')
        self.saveLogs = arguments.get("SaveLogs", True)
        self.emulation = arguments.get("Emulation", False)

        # Workflow creation
        self.couchURL = arguments.get("CouchURL")
        self.couchDBName = arguments.get("CouchDBName", "wmagent_configcache")
        self.configCacheID = arguments.get("AnalysisConfigCacheDoc", None)
        self.configCacheUrl = arguments.get("ConfigCacheUrl", None)
        
        self.minMergeSize = 1
        
        self.frameworkVersion = arguments["CMSSWVersion"]
        self.acquisitionEra = arguments.get("PublishDataName", str(int(time.time())))
        self.globalTag = arguments.get("GlobalTag", None)

        self.inputDataset = arguments.get('InputDataset', None)
        self.processingVersion = arguments.get('ProcessingVersion', 1)
        self.origRequest = arguments.get('OriginalRequestName', '')

        # Sites
        self.blockBlacklist = arguments.get("BlockBlacklist", [])
        self.blockWhitelist = arguments.get("BlockWhitelist", [])
        self.runWhitelist = arguments.get("RunWhitelist", [])
        self.runBlacklist = arguments.get("RunBlacklist", [])
        self.asyncDest = arguments.get("asyncDest", "T1_US_FNAL_Buffer")

        # ACDC and job splitting
        self.ACDCURL = arguments.get("ACDCUrl", "")
        self.ACDCDBName = arguments.get("ACDCDBName", "wmagent_acdc")
        self.Runs  = arguments.get("Runs" , None)
        self.Lumis = arguments.get("Lumis", None)
        self.Submission = arguments.get("Submission", 1)
        self.analysisJobSplitAlgo  = arguments.get("JobSplitAlgo", "EventBased")

        if self.Lumis and self.analysisJobSplitAlgo not in ['LumiBased']:
            raise RuntimeError('Running on selected lumis only supported in split mode(s) %s' %
                               'LumiBased')

        if self.analysisJobSplitAlgo == 'EventBased':
            self.analysisJobSplitArgs = arguments.get('JobSplitArgs', {'events_per_job' : 1000})
        elif self.analysisJobSplitAlgo == 'LumiBased':
            self.analysisJobSplitArgs = arguments.get('JobSplitArgs', {'lumis_per_job' : 15})
            if self.Lumis:
                self.analysisJobSplitArgs.update({'lumis' : self.Lumis})
                self.analysisJobSplitArgs.update({'runs'  : self.Runs})
            self.analysisJobSplitArgs.update(
                           {'halt_job_on_file_boundaries' : False,
                            'splitOnRun' : False,
                           })
        else:
            self.analysisJobSplitArgs = arguments.get('JobSplitArgs', {})

        return self.buildWorkload()

    def validateSchema(self, schema):
        """
        _validateSchema_

        Check for required fields, and some skim facts
        """
        def _checkSiteList(list):
            if self.has_key(list) and hasattr(self,'allCMSNames'):
                for site in self[list]:
                    if not site in self.allCMSNames: #self.allCMSNames needs to be initialized to allow sitelisk check
                        raise RuntimeError("The site " + site + " provided in the " + list + " param has not been found. Check https://cmsweb.cern.ch/sitedb/json/index/SEtoCMSName?name= for a list of known sites")

        self.requireValidateFields(fields=self.requiredFields, schema=schema, validate=False)

        _checkSiteList("SiteWhitelist")
        _checkSiteList("SiteBlacklist")

        #Control if the request name contain spaces
        if schema.get("RequestName").count(' ') > 0:
            msg = "RequestName cannot contain spaces"
            self.raiseValidationException(msg = msg)

        return



def analysisWorkload(workloadName, arguments):
    """
    _analysisWorkload_

    Instantiate the AnalysisWorkflowFactory and have it generate a workload for
    the given parameters.
    """
    myAnalysisFactory = AnalysisWorkloadFactory()
    return myAnalysisFactory(workloadName, arguments)