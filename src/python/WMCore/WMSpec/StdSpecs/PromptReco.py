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

import WMCore.Lexicon

from WMCore.Cache.WMConfigCache import ConfigCache
from WMCore.WMInit import getWMBASE
from WMCore.WMRuntime.Tools.Scram import Scram
from WMCore.WMSpec.StdSpecs.StdBase import StdBase

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

def getTestArguments():
    """
    _getTestArguments_

    This should be where the default REQUIRED arguments go
    This serves as documentation for what is currently required
    by the standard PromptReco workload in importable format.

    NOTE: These are test values.  If used in real workflows they
    will cause everything to crash/die/break, and we will be forced
    to hunt you down and kill you.
    """
    arguments = {
        "Requestor": "Dirk.Hufnagel@cern.ch",

        "ScramArch" : "slc5_amd64_gcc462",

        # these must be overridden
        "AcquisitionEra": "WMAgentCommissioning12",
        "CMSSWVersion" : "CMSSW_5_2_1",

        "ProcessingVersion" : 1,
        "ProcScenario" : "cosmics",
        "GlobalTag" : "GR_P_V29::All",

        "InputDataset" : "/Cosmics/Run2012A-v1/RAW",
        "WriteTiers" : ["RECO", "AOD", "DQM", "ALCARECO"],
        "AlcaSkims" : ["TkAlCosmics0T","MuAlGlobalCosmics","HcalCalHOCosmics"],
        "DqmSequences" : [ "@common", "@jetmet" ],

        "CouchURL": None,
        "CouchDBName": "promptreco_t",
        # or alternatively CouchURL part can be replaced by ConfigCacheUrl,
        # then ConfigCacheUrl + CouchDBName + ConfigCacheID
        "ConfigCacheUrl": None,
        
        "InitCommand": os.environ.get("INIT_COMMAND", None),
        "RunNumber": 195360,

        #PromptSkims should be a list of ConfigSection objects with the
        #following attributes
        #DataTier: "RECO"
        #SkimName: "CosmicsSkim1"
        #TwoFileRead: True
        #ConfigURL: http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/Configuration/Skimming/test/tier1/skim_Cosmics.py?revision=1.2&pathrev=SkimsFor426
        #ProcessingVersion: PromptSkim-v1
        "PromptSkims": [],

        "DashboardHost": "127.0.0.1",
        "DashboardPort": 8884,
        }

    return arguments

class PromptRecoWorkloadFactory(StdBase):
    """
    _PromptRecoWorkloadFactory_

    Stamp out PromptReco workflows.
    """
    def __init__(self):
        StdBase.__init__(self)
        self.multicore = False
        self.multicoreNCores = 1
        return

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

        # Required parameters that must be specified by the Requestor.
        self.frameworkVersion = arguments['CMSSWVersion']
        self.globalTag = arguments['GlobalTag']
        self.writeTiers = arguments['WriteTiers']
        self.alcaSkims = arguments['AlcaSkims']
        self.inputDataset = arguments['InputDataset']
        self.promptSkims = arguments['PromptSkims']
        self.couchURL = arguments['CouchURL']
        self.couchDBName = arguments['CouchDBName']
        self.configCacheUrl = arguments.get("ConfigCacheUrl", None)
        self.initCommand = arguments['InitCommand']

        #Optional parameters
        self.envPath = arguments.get('EnvPath', None)
        self.binPath = arguments.get('BinPath', None)

        if arguments.has_key('Multicore'):
            numCores = arguments.get('Multicore')
            if numCores == None or numCores == "":
                self.multicore = False
            elif numCores == "auto":
                self.multicore = True
                self.multicoreNCores = "auto"
            else:
                self.multicore = True
                self.multicoreNCores = numCores

        # Do we run log collect ? (Tier0 does not support it yet)
        self.doLogCollect = arguments.get("DoLogCollect", True)

        # Optional arguments that default to something reasonable.
        self.dbsUrl = arguments.get("DbsUrl", "http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet")
        self.blockBlacklist = arguments.get("BlockBlacklist", [])
        self.blockWhitelist = arguments.get("BlockWhitelist", [])
        self.runBlacklist = arguments.get("RunBlacklist", [])
        self.runWhitelist = arguments.get("RunWhitelist", [])
        self.emulation = arguments.get("Emulation", False)

        # These are mostly place holders because the job splitting algo and
        # parameters will be updated after the workflow has been created.
        self.procJobSplitAlgo = arguments.get("StdJobSplitAlgo", "EventBased")
        self.procJobSplitArgs = arguments.get("StdJobSplitArgs",
                                              {"events_per_job": 500})
        self.skimJobSplitAlgo = arguments.get("SkimJobSplitAlgo", "FileBased")
        self.skimJobSplitArgs = arguments.get("SkimJobSplitArgs",
                                              {"files_per_job": 1,
                                               "include_parents": True})

        return self.buildWorkload()

    def validateSchema(self, schema):
        """
        _validateSchema_

        Check for required fields, and some skim facts
        """
        requiredFields = ["ScramArch", "CMSSWVersion", "ProcessingVersion",
                          "ProcScenario", "GlobalTag", "InputDataset",
                          "WriteTiers", "AlcaSkims"]
        self.requireValidateFields(fields = requiredFields, schema = schema,
                                   validate = False)

        try:
            WMCore.Lexicon.dataset(schema.get('InputDataset', ''))
        except AssertionError:
            self.raiseValidationException(msg = "Invalid input dataset!")



def promptrecoWorkload(workloadName, arguments):
    """
    _promptrecoWorkload_

    Instantiate the PromptRecoWorkflowFactory and have it generate
    a workload for the given parameters.
    """
    myPromptRecoFactory = PromptRecoWorkloadFactory()
    return myPromptRecoFactory(workloadName, arguments)