#!/usr/bin/env python
"""
_PromptReco_

Standard PromptReco workflow.
"""

import logging
import os
import re
import shutil
import tempfile
import urllib

from WMCore.Cache.WMConfigCache import ConfigCache
from WMCore.Lexicon import dataset, couchurl, identifier, block
from WMCore.WMInit import getWMBASE
from WMCore.WMRuntime.Tools.Scram import Scram
from WMCore.WMSpec.StdSpecs.StdBase import StdBase
from WMCore.WMSpec.WMWorkloadTools import makeList, strToBool

def injectIntoConfigCache(frameworkVersion, scramArch, initCommand,
                          configUrl, configLabel, couchUrl, couchDBName,
                          envPath = None, binPath = None):
    """
    _injectIntoConfigCache_
    """
    logging.info("Injecting to config cache.\n")
    configTempDir = tempfile.mkdtemp()
    configPath = os.path.join(configTempDir, "cmsswConfig.py")
    configString = urllib.urlopen(fixCVSUrl(configUrl)).read(-1)
    configFile = open(configPath, "w")
    configFile.write(configString)
    configFile.close()

    scramTempDir = tempfile.mkdtemp()
    wmcoreBase = getWMBASE()
    if not envPath:
        envPath = os.path.normpath(os.path.join(wmcoreBase, "../../../../../../../../apps/wmagent/etc/profile.d/init.sh"))
    scram = Scram(version = frameworkVersion, architecture = scramArch,
                  directory = scramTempDir, initialise = initCommand,
                  envCmd = "source %s" % envPath)
    scram.project()
    scram.runtime()

    if not binPath:
        scram("python2.6 %s/../../../bin/inject-to-config-cache %s %s PromptSkimmer cmsdataops %s %s None" % (wmcoreBase,
                                                                                                              couchUrl,
                                                                                                              couchDBName,
                                                                                                              configPath,
                                                                                                              configLabel))
    else:
        scram("python2.6 %s/inject-to-config-cache %s %s PromptSkimmer cmsdataops %s %s None" % (binPath,
                                                                                                 couchUrl,
                                                                                                 couchDBName,
                                                                                                 configPath,
                                                                                                 configLabel))

    shutil.rmtree(configTempDir)
    shutil.rmtree(scramTempDir)
    return

def parseT0ProcVer(procVer, procString = None):
    compoundProcVer = r"^(((?P<ProcString>[a-zA-Z0-9_]+)-)?v)?(?P<ProcVer>[0-9]+)$"
    match = re.match(compoundProcVer, procVer)
    if match:
        return {'ProcString' : match.group('ProcString') or procString,
                'ProcVer' : int(match.group('ProcVer'))}
    logging.error('Processing version %s is not compatible'
                                % procVer)
    raise Exception

def fixCVSUrl(url):
    """
    _fixCVSUrl_

    Checks the url, if it looks like a cvs url then make sure it has no
    view option in it, so it can be downloaded correctly
    """
    cvsPatt = '(http://cmssw\.cvs\.cern\.ch.*\?).*(revision=[0-9]*\.[0-9]*).*'
    cvsMatch = re.match(cvsPatt, url)
    if cvsMatch:
        url = cvsMatch.groups()[0] + cvsMatch.groups()[1]
    return url

class PromptRecoWorkloadFactory(StdBase):
    """
    _PromptRecoWorkloadFactory_

    Stamp out PromptReco workflows.
    """

    def buildWorkload(self):
        """
        _buildWorkload_

        Build the workload given all of the input parameters.

        Not that there will be LogCollect tasks created for each processing
        task and Cleanup tasks created for each merge task.

        """
        (self.inputPrimaryDataset, self.inputProcessedDataset,
         self.inputDataTier) = self.inputDataset[1:].split("/")

        workload = self.createWorkload()
        workload.setDashboardActivity("tier0")
        self.reportWorkflowToDashboard(workload.getDashboardActivity())
        workload.setWorkQueueSplitPolicy("Block", self.procJobSplitAlgo,
                                         self.procJobSplitArgs)

        cmsswStepType = "CMSSW"
        taskType = "Processing"
        if self.multicore:
            taskType = "MultiProcessing"

        recoOutputs = []
        for dataTier in self.writeTiers:
            recoOutputs.append( { 'dataTier' : dataTier,
                                  'eventContent' : dataTier,
                                  'moduleLabel' : "write_%s" % dataTier } )

        recoTask = workload.newTask("Reco")
        recoOutMods = self.setupProcessingTask(recoTask, taskType, self.inputDataset,
                                               scenarioName = self.procScenario,
                                               scenarioFunc = "promptReco",
                                               scenarioArgs = { 'globalTag' : self.globalTag,
                                                                'skims' : self.alcaSkims,
                                                                'dqmSeq' : self.dqmSequences,
                                                                'outputs' : recoOutputs },
                                               splitAlgo = self.procJobSplitAlgo,
                                               splitArgs = self.procJobSplitArgs,
                                               stepType = cmsswStepType,
                                               forceUnmerged = True)
        if self.doLogCollect:
            self.addLogCollectTask(recoTask)

        recoMergeTasks = {}
        for recoOutLabel, recoOutInfo in recoOutMods.items():
            if recoOutInfo['dataTier'] != "ALCARECO":
                mergeTask = self.addMergeTask(recoTask, self.procJobSplitAlgo, recoOutLabel,
                                              doLogCollect = self.doLogCollect)
                recoMergeTasks[recoOutInfo['dataTier']] = mergeTask

            else:
                alcaTask = recoTask.addTask("AlcaSkim")
                alcaOutMods = self.setupProcessingTask(alcaTask, taskType,
                                                       inputStep = recoTask.getStep("cmsRun1"),
                                                       inputModule = recoOutLabel,
                                                       scenarioName = self.procScenario,
                                                       scenarioFunc = "alcaSkim",
                                                       scenarioArgs = { 'globalTag' : self.globalTag,
                                                                        'skims' : self.alcaSkims,
                                                                        'primaryDataset' : self.inputPrimaryDataset },
                                                       splitAlgo = "WMBSMergeBySize",
                                                       splitArgs = {"max_merge_size": self.maxMergeSize,
                                                                    "min_merge_size": self.minMergeSize,
                                                                    "max_merge_events": self.maxMergeEvents},
                                                       stepType = cmsswStepType)
                if self.doLogCollect:
                    self.addLogCollectTask(alcaTask, taskName = "AlcaSkimLogCollect")
                self.addCleanupTask(recoTask, recoOutLabel)

                for alcaOutLabel, alcaOutInfo in alcaOutMods.items():
                    self.addMergeTask(alcaTask, self.procJobSplitAlgo, alcaOutLabel,
                                      doLogCollect = self.doLogCollect)

        for promptSkim in self.promptSkims:
            if not promptSkim.DataTier in recoMergeTasks:
                error = 'PromptReco output does not have the following output data tier: %s.' % promptSkim.DataTier
                error += 'Please change the skim input to be one of the following: %s' % recoMergeTasks.keys()
                error += 'That should be in the relevant skimConfig in T0AST'
                logging.error(error)
                raise Exception

            mergeTask = recoMergeTasks[promptSkim.DataTier]
            skimTask = mergeTask.addTask(promptSkim.SkimName)
            parentCmsswStep = mergeTask.getStep('cmsRun1')

            parsedProcVer = parseT0ProcVer(promptSkim.ProcessingVersion,
                                           'PromptSkim')
            self.processingString = parsedProcVer["ProcString"]
            self.processingVersion = parsedProcVer["ProcVer"]

            if promptSkim.TwoFileRead:
                self.skimJobSplitArgs['include_parents'] = True
            else:
                self.skimJobSplitArgs['include_parents'] = False

            configLabel = '%s-%s' % (self.workloadName, promptSkim.SkimName)
            configCacheUrl = self.configCacheUrl or self.couchURL
            injectIntoConfigCache(self.frameworkVersion, self.scramArch,
                                       self.initCommand, promptSkim.ConfigURL, configLabel,
                                       configCacheUrl, self.couchDBName,
                                       self.envPath, self.binPath)
            try:
                configCache = ConfigCache(configCacheUrl, self.couchDBName)
                configCacheID = configCache.getIDFromLabel(configLabel)
                if configCacheID:
                    logging.error("The configuration was not uploaded to couch")
                    raise Exception
            except Exception:
                logging.error("There was an exception loading the config out of the")
                logging.error("ConfigCache.  Check the scramOutput.log file in the")
                logging.error("PromptSkimScheduler directory to find out what went")
                logging.error("wrong.")
                raise

            outputMods = self.setupProcessingTask(skimTask, "Skim", inputStep = parentCmsswStep, inputModule = "Merged",
                                                  couchURL = self.couchURL, couchDBName = self.couchDBName,
                                                  configCacheUrl = self.configCacheUrl,
                                                  configDoc = configCacheID, splitAlgo = self.skimJobSplitAlgo,
                                                  splitArgs = self.skimJobSplitArgs)
            if self.doLogCollect:
                self.addLogCollectTask(skimTask, taskName = "%sLogCollect" % promptSkim.SkimName)

            for outputModuleName in outputMods.keys():
                self.addMergeTask(skimTask, self.skimJobSplitAlgo, outputModuleName,
                                  doLogCollect = self.doLogCollect)

        return workload

    def __call__(self, workloadName, arguments):
        """
        _call_

        Create a ReReco workload with the given parameters.
        """
        StdBase.__call__(self, workloadName, arguments)

        # These are mostly place holders because the job splitting algo and
        # parameters will be updated after the workflow has been created.
        self.procJobSplitArgs = {}
        if self.procJobSplitAlgo == "EventBased" or self.procJobSplitAlgo == "EventAwareLumiBased":
            if self.eventsPerJob is None:
                self.eventsPerJob = int((8.0 * 3600.0) / self.timePerEvent)
            self.procJobSplitArgs["events_per_job"] = self.eventsPerJob
            if self.procJobSplitAlgo == "EventAwareLumiBased":
                self.procJobSplitArgs["max_events_per_lumi"] = 20000
        elif self.procJobSplitAlgo == "LumiBased":
            self.procJobSplitArgs["lumis_per_job"] = self.lumisPerJob
        elif self.procJobSplitAlgo == "FileBased":
            self.procJobSplitArgs["files_per_job"] = self.filesPerJob
        self.skimJobSplitArgs = {}
        if self.skimJobSplitAlgo == "EventBased" or self.skimJobSplitAlgo == "EventAwareLumiBased":
            if self.eventsPerJob is None:
                self.eventsPerJob = int((8.0 * 3600.0) / self.timePerEvent)
            self.skimJobSplitArgs["events_per_job"] = self.eventsPerJob
            if self.skimJobSplitAlgo == "EventAwareLumiBased":
                self.skimJobSplitArgs["max_events_per_lumi"] = 20000
        elif self.skimJobSplitAlgo == "LumiBased":
            self.skimJobSplitArgs["lumis_per_job"] = self.lumisPerJob
        elif self.skimJobSplitAlgo == "FileBased":
            self.skimJobSplitArgs["files_per_job"] = self.filesPerJob
        self.skimJobSplitArgs = arguments.get("SkimJobSplitArgs",
                                              {"files_per_job": 1,
                                               "include_parents": True})

        return self.buildWorkload()

    @staticmethod
    def getWorkloadArguments():
        baseArgs = StdBase.getWorkloadArguments()
        specArgs = {"Scenario" : {"default" : "pp", "type" : str,
                                  "optional" : False, "validate" : None,
                                  "attr" : "procScenario", "null" : False},
                    "GlobalTag" : {"default" : "GR_P_V29::All", "type" : str,
                                   "optional" : False, "validate" : None,
                                   "attr" : "globalTag", "null" : False},
                    "WriteTiers" : {"default" : ["RECO", "AOD", "DQM", "ALCARECO"],
                                    "type" : makeList, "optional" : False,
                                    "validate" : None,
                                    "attr" : "writeTiers", "null" : False},
                    "AlcaSkims" : {"default" : ["TkAlCosmics0T","MuAlGlobalCosmics","HcalCalHOCosmics"],
                                   "type" : makeList, "optional" : False,
                                   "validate" : None,
                                   "attr" : "alcaSkims", "null" : False},
                    "InputDataset" : {"default" : "/Cosmics/Run2012A-v1/RAW", "type" : str,
                                      "optional" : False, "validate" : dataset,
                                      "attr" : "inputDataset", "null" : False},
                    "PromptSkims" : {"default" : [], "type" : makeList,
                                     "optional" : True, "validate" : None,
                                     "attr" : "promptSkims", "null" : False},
                    "CouchURL" : {"default" : None, "type" : str,
                                  "optional" : False, "validate" : couchurl,
                                  "attr" : "couchURL", "null" : False},
                    "CouchDBName" : {"default" : "promptreco_t", "type" : str,
                                     "optional" : False, "validate" : identifier,
                                     "attr" : "couchDBName", "null" : False},
                    "ConfigCacheUrl" : {"default" : None, "type" : str,
                                        "optional" : True, "validate" : couchurl,
                                        "attr" : "configCacheUrl", "null" : False},
                    "InitCommand" : {"default" : None, "type" : str,
                                     "optional" : True, "validate" : None,
                                     "attr" : "initCommand", "null" : False},
                    "EnvPath" : {"default" : None, "type" : str,
                                 "optional" : True, "validate" : None,
                                 "attr" : "envPath", "null" : True},
                    "BinPath" : {"default" : None, "type" : str,
                                 "optional" : True, "validate" : None,
                                 "attr" : "binPath", "null" : True},
                    "DoLogCollect" : {"default" : True, "type" : strToBool,
                                      "optional" : True, "validate" : None,
                                      "attr" : "doLogCollect", "null" : False},
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
                    "SplittingAlgo" : {"default" : "EventBased", "type" : str,
                                       "optional" : True, "validate" : lambda x: x in ["EventBased", "LumiBased",
                                                                                       "EventAwareLumiBased", "FileBased"],
                                       "attr" : "procJobSplitAlgo", "null" : False},
                    "EventsPerJob" : {"default" : 500, "type" : int,
                                      "optional" : True, "validate" : lambda x : x > 0,
                                      "attr" : "eventsPerJob", "null" : False},
                    "LumisPerJob" : {"default" : 8, "type" : int,
                                     "optional" : True, "validate" : lambda x : x > 0,
                                     "attr" : "lumisPerJob", "null" : False},
                    "FilesPerJob" : {"default" : 1, "type" : int,
                                     "optional" : True, "validate" : lambda x : x > 0,
                                     "attr" : "filesPerJob", "null" : False},
                    "SkimSplittingAlgo" : {"default" : "FileBased", "type" : str,
                                           "optional" : True, "validate" : lambda x: x in ["EventBased", "LumiBased",
                                                                                           "EventAwareLumiBased", "FileBased"],
                                           "attr" : "skimJobSplitAlgo", "null" : False},
                    "SkimEventsPerJob" : {"default" : 500, "type" : int,
                                          "optional" : True, "validate" : lambda x : x > 0,
                                          "attr" : "skimEventsPerJob", "null" : False},
                    "SkimLumisPerJob" : {"default" : 8, "type" : int,
                                         "optional" : True, "validate" : lambda x : x > 0,
                                         "attr" : "skimLumisPerJob", "null" : False},
                    "SkimFilesPerJob" : {"default" : 1, "type" : int,
                                         "optional" : True, "validate" : lambda x : x > 0,
                                         "attr" : "skimFilesPerJob", "null" : False}
                    }
        baseArgs.update(specArgs)
        return baseArgs
