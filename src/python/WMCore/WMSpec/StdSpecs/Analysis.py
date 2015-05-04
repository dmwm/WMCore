#!/usr/bin/env python
# encoding: utf-8
# pylint: disable=C0301,W0142

from WMCore.Lexicon import couchurl, identifier, block
from WMCore.WMSpec.StdSpecs.StdBase import StdBase
from WMCore.WMSpec.WMWorkloadTools import makeList, strToBool

def remoteLFNPrefix(site, lfn = ''):
    """
    Convert a site name to the relevant remote LFN prefix
    """
    from WMCore.Services.PhEDEx.PhEDEx import PhEDEx
    phedexJSON = PhEDEx(responseType = 'json')

    seName = phedexJSON.getNodeSE(site)
    uri = phedexJSON.getPFN(nodes = [site], lfns = [lfn])[(site, lfn)]

    return uri.replace(lfn, ''), seName # Don't want the actual LFN, just prefix

class AnalysisWorkloadFactory(StdBase):
    """
    Analysis workload.
    """

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
        self.lcPrefix, self.seName = remoteLFNPrefix(site = self.asyncDest, lfn = self.logCollBase)

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
        logArchiveStep.addOverride('altLFN', self.logBase)

        # Set up log collecting the same as Analysis
        if self.saveLogs:
            logCollectTask = self.addLogCollectTask(mainTask)
            logCollectStep = logCollectTask.getStep('logCollect1')
            logCollectStep.addOverride('userLogs', True)
            logCollectStep.addOverride('seName', self.seName)
            logCollectStep.addOverride('lfnBase', self.logCollBase)
            logCollectStep.addOverride('lfnPrefix', self.lcPrefix)
            logCollectStep.addOverride('dontStage', False)

        # Get the user output files we need
        cmsswStep = mainTask.getStep('cmsRun1')
        cmsswHelper = cmsswStep.getTypeHelper()
        cmsswHelper.setUserLFNBase(self.userOutBase)
        for outputFile in self.outputFiles:
            cmsswHelper.addAnalysisFile(outputFile, fileName = outputFile, lfnBase = self.userOutBase)

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
        if self.Submission > 1: # resubmit
            analysisTask.addInputACDC(self.ACDCURL, self.ACDCDBName, self.origRequest, None)
            self.inputDataset = None
            self.workload.setWorkQueueSplitPolicy("ResubmitBlock", self.analysisJobSplitAlgo, self.analysisJobSplitArgs)
        else:
            self.workload.setWorkQueueSplitPolicy("Block", self.analysisJobSplitAlgo, self.analysisJobSplitArgs)

        outputMods = self.setupProcessingTask(analysisTask, "Analysis", inputDataset = self.inputDataset,
                                              inputStep = self.inputStep,
                                              couchURL = self.couchURL, couchDBName = self.couchDBName,
                                              configCacheUrl = self.configCacheUrl,
                                              configDoc = self.configCacheID, splitAlgo = self.analysisJobSplitAlgo,
                                              splitArgs = self.analysisJobSplitArgs, \
                                              userDN = self.owner_dn, asyncDest = self.asyncDest,
                                              owner_vogroup = self.owner_vogroup, owner_vorole = self.owner_vorole,
                                              userSandbox = self.userSandbox, userFiles = self.userFiles)

        self.setUserOutput(analysisTask)
        # setting the parameters which need to be set for all the tasks
        # sets acquisitionEra, processingVersion, processingString
        self.workload.setTaskPropertiesFromWorkload()

        # set the LFN bases (normally done by request manager)
        # also pass runNumber (workload evaluates it)
        self.workload.setLFNBase(self.mergedLFNBase, self.unmergedLFNBase,
                            runNumber = self.runNumber)

        return self.workload

    def __call__(self, workloadName, arguments):
        """
        Create a workload instance for an Analysis request

        """
        StdBase.__call__(self, workloadName, arguments)

        self.minMergeSize = 1

        if self.Lumis and self.analysisJobSplitAlgo not in ['LumiBased']:
            raise RuntimeError('Running on selected lumis only supported in split mode(s) %s' %
                               'LumiBased')

        if self.analysisJobSplitAlgo == 'EventBased':
            self.analysisJobSplitArgs = {'events_per_job' : self.eventsPerJob}
        elif self.analysisJobSplitAlgo == 'LumiBased':
            self.analysisJobSplitArgs = {'lumis_per_job' : self.lumisPerJob}
            if self.Lumis:
                self.analysisJobSplitArgs.update({'lumis' : self.Lumis})
                self.analysisJobSplitArgs.update({'runs'  : self.Runs})
            self.analysisJobSplitArgs.update(
                           {'halt_job_on_file_boundaries' : False,
                            'splitOnRun' : False,
                           })

        return self.buildWorkload()

    @staticmethod
    def getWorkloadArguments():
        baseArgs = StdBase.getWorkloadArguments()
        del baseArgs["AcquisitionEra"]
        reqMgrArgs = StdBase.getWorkloadArgumentsWithReqMgr()
        baseArgs.update(reqMgrArgs)
        specArgs = {"RequestType" : {"default" : "Analysis", "optional" : True,
                                      "attr" : "requestType"},
                    "userSandbox" : {"default" : "http://home.fnal.gov/~ewv/agent.tgz",
                                     "type" : str,
                                     "optional" : True, "validate" : None,
                                     "attr" : "userSandbox", "null" : True},
                    "userFiles" : {"default" : [], "type" : makeList,
                                   "optional" : True, "validate" : None,
                                   "attr" : "userFiles", "null" : False},
                    "OutputFiles" : {"default" : [], "type" : makeList,
                                   "optional" : True, "validate" : None,
                                   "attr" : "outputFiles", "null" : False},
                    "Username" : {"default" : "mmascher", "type" : str,
                                  "optional" : True, "validate" : None,
                                  "attr" : "userName", "null" : False},
                    "SaveLogs" : {"default" : True, "type" : strToBool,
                                  "optional" : True, "validate" : None,
                                  "attr" : "saveLogs", "null" : False},
                    "CouchURL" : {"default" : "http://localhost:5984", "type" : str,
                                  "optional" : False, "validate" : couchurl,
                                  "attr" : "couchURL", "null" : False},
                    "CouchDBName" : {"default" : "analysis_reqmgr_config_cache", "type" : str,
                                     "optional" : False, "validate" : identifier,
                                     "attr" : "couchDBName", "null" : False},
                    "AnalysisConfigCacheDoc" : {"default" : None, "type" : str,
                                                "optional" : False, "validate" : None,
                                                "attr" : "configCacheID", "null" : False},
                    "ConfigCacheUrl" : {"default" : None, "type" : str,
                                        "optional" : True, "validate" : None,
                                        "attr" : "configCacheUrl", "null" : False},
                    "PublishDataName" : {"default" : None, "type" : str,
                                         "optional" : True, "validate" : None,
                                         "attr" : "acquisitionEra", "null" : False},
                    "GlobalTag" : {"default" : "GT_AN_V1:All", "type" : str,
                                   "optional" : False, "validate" : None,
                                   "attr" : "globalTag", "null" : False},
                    "InputDataset" : {"default" : "/RelValProdTTbar/JobRobot-MC_3XY_V24_JobRobot-v1/GEN-SIM-DIGI-RECO",
                                      "type" : str, "optional" : False,
                                      "validate" : None, "attr" : "inputDataset",
                                      "null" : False},
                    "OriginalRequestName" : {"default" : None, "type" : str,
                                             "optional" : True, "validate" : None,
                                             "attr" : "origRequest", "null" : False},
                    "BlockBlacklist" : {"default" : [], "type" : makeList,
                                        "optional" : True, "validate" : lambda x: all([block(y) for y in x]),
                                        "attr" : "blockBlacklist", "null" : False},
                    "BlockWhitelist" : {"default" : [], "type" : makeList,
                                        "optional" : True, "validate" : lambda x: all([block(y) for y in x]),
                                        "attr" : "blockWhitelist", "null" : False},
                    "RunBlacklist" : {"default" : [], "type" : makeList,
                                      "optional" : True, "validate" : lambda x: all([int(y) > 0 for y in x]),
                                      "attr" : "runBlacklist", "null" : False},
                    "RunWhitelist" : {"default" : [], "type" : makeList,
                                      "optional" : True, "validate" : lambda x: all([int(y) > 0 for y in x]),
                                      "attr" : "runWhitelist", "null" : False},
                    "asyncDest" : {"default" : "T1_US_FNAL_Buffer", "type" : str,
                                   "optional" : True, "validate" : None,
                                   "attr" : "asyncDest", "null" : False},
                    "ACDCUrl" : {"default" : "http://cmsweb-testbed.cern.ch/couchdb",
                                 "type" : str,
                                 "optional" : True, "validate" : couchurl,
                                 "attr" : "ACDCURL", "null" : False},
                    "ACDCDBName" : {"default" : "acdcserver", "type" : str,
                                    "optional" : True, "validate" : identifier,
                                    "attr" : "ACDCDBName", "null" : False},
                    "Runs" : {"default" : None, "type" : makeList,
                              "optional" : True, "validate" : None,
                              "attr" : "Runs", "null" : False},
                    "Lumis" : {"default" : None, "type" : makeList,
                              "optional" : True, "validate" : None,
                              "attr" : "Lumis", "null" : False},
                    "Submission" : {"default" : 1, "type" : int,
                                    "optional" : True, "validate" : None,
                                    "attr" : "Submission", "null" : False},
                    "JobSplitAlgo" : {"default" : "EventBased", "type" : str,
                                      "optional" : True, "validate" : None,
                                      "attr" : "analysisJobSplitAlgo", "null" : False},
                    "EventsPerJob" : {"default" : 1000, "type" : int,
                                      "optional" : True, "validate" : lambda x : x > 0,
                                      "attr" : "eventsPerJob", "null" : False},
                    "LumisPerJob" : {"default" : 8, "type" : int,
                                     "optional" : True, "validate" : lambda x : x > 0,
                                     "attr" : "lumisPerJob", "null" : False}, }
        baseArgs.update(specArgs)
        StdBase.setDefaultArgumentsProperty(baseArgs)
        return baseArgs

    def validateSchema(self, schema):
        """
        _validateSchema_

        Check for required fields, and some skim facts
        """

        # Control if the request name contain spaces
        if schema.get("RequestName", "").count(' ') > 0:
            msg = "RequestName cannot contain spaces"
            self.raiseValidationException(msg = msg)

        return