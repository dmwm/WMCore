#!/usr/bin/env python
# encoding: utf-8
"""
_TaskChain_

Created by Dave Evans on 2011-06-15.
Copyright (c) 2011 Fermilab. All rights reserved.

Provide support for building arbitrary chains of WMTasks based on a nested dictionary structure
starting with either a generation (create new MC events) or processing (use an existing input dataset) step, followed
by a chain of dependent WMTasks that process the subsequent output.

The request is formed as a dictionary where some global parameters are provided as normal, but the
processing tasks are specified as sub dictionaries.

The top level dict should contain the parameter TaskChain and the value is the number of processing tasks to be run.
For each count in the chain, a dictionary entry named Task1...N should be made with a value being another dictionary.

Any parameters in the Main request will be used throughout the different task unless they are overriden, exceptions are
CouchDB parameters the main request parameters are:

{
    "CMSSWVersion": "CMSSW_3_5_8",                    CMSSW Version
    "ScramArch": "slc5_ia32_gcc434",                  Scram Arch
    "Requestor": "sfoulkes@fnal.gov",                 Person responsible
    "GlobalTag": "GR10_P_v4::All",                    Global Tag
    "CouchURL": "http://couchserver.cern.ch",         URL of CouchDB containing ConfigCache (Used for all sub-tasks)
    "ConfigCacheUrl": https://cmsweb-testbed.cern.ch/couchdb URL of an alternative CouchDB server containing Config documents
    "CouchDBName": "config_cache",                    Name of Couch Database containing config cache (Used for all sub-tasks)
    "TaskChain" : 4,                                  Define number of tasks in chain.
}


Task1 will be either a generation or processing task:

Example initial generation task:

"Task1" :{
    "TaskName"           : "GenSim",                 Task Name
    "ConfigCacheID"      : generatorDoc,             Generator Config id
    "SplittingAlgorithm" : "EventBased",             Splitting Algorithm
    "SplittingArguments" : {"events_per_job" : 250}, Size of jobs in terms of splitting algorithm
    "RequestNumEvents"   : 10000,                    Total number of events to generate
    "Seeding"            : "AutomaticSeeding",       Random seeding method
    "PrimaryDataset"     : "RelValTTBar",            Primary Dataset to be created
    "ScramArch"          : "slc5_amd64_gcc462",      Particular scramArch for this task
    "CMSSWVersion"       : "CMSSW_5_3_5",            Particular CMSSW version for this task
},

Example initial processing task

"Task1" :{
     "TaskName"           : "DigiHLT",                                 Task Name
     "ConfigCacheID"      : someHash,                                  Processing Config id
     "InputDataset"       : "/MinimumBias/Commissioning10-v4/GEN-SIM", Input Dataset to be processed
     "SplittingAlgorithm" : "FileBased",                               Splitting Algorithm
     "SplittingArguments" : {"files_per_job" : 1},                     Size of jobs in terms of splitting algorithm
     "MCPileup"           : "/MinBias/Summer12-v1/GEN-SIM-DIGI-RECO",  Pileup MC dataset for the task
     "DataPileup"         : "/MinimumBias/Run2011A-v1/RAW"             Pileup data dataset for the task
     "GlobalTag"          : "GR_P_V42::All"                            Global tag for  this task
     "KeepOutput"         : False                                      Indicates if the output data from this dataset should be kept in merged area
 },

 All subsequent Task entries will process the output of one of the preceeding steps, the primary dataset can be changed from the input.

 Example:

 "Task2" : {
     "TaskName"              : "Reco",                        Task Name
     "InputTask"             : "DigiHLT",                     Input Task Name (Task Name field of a previous Task entry)
     "InputFromOutputModule" : "writeRAWDIGI",                OutputModule name in the input task that will provide files to process
     "ConfigCacheID"         : "17612875182763812763812",     Processing Config id
     "SplittingAlgorithm"    : "FileBased",                   Splitting Algorithm
     "SplittingArguments"    : {"files_per_job" : 1 },        Size of jobs in terms of splitting algorithm
     "DataPileup"            : "/MinimumBias/Run2011A-v1/RAW" Pileup data dataset for the task
 },

 "Task3" : {
     "TaskName"              : "ALCAReco",             Task Name
     "InputTask"             : "Reco",                 Input Task Name (Task Name field of a previous Task entry)
     "InputFromOutputModule" : "writeALCA",            OutputModule name in the input task that will provide files to process
     "ConfigCacheID"         : "12871372323918187281", Processing Config id
     "SplittingAlgorithm"    : "FileBased",            Splitting Algorithm
     "SplittingArguments"    : {"files_per_job" : 1 }, Size of jobs in terms of splitting algorithm
 },
"""

from WMCore.Lexicon import identifier, couchurl, block, primdataset, dataset
from WMCore.WMSpec.StdSpecs.StdBase import StdBase
from WMCore.WMSpec.WMWorkloadTools import makeList, strToBool, \
    validateArgumentsCreate, validateArgumentsNoOptionalCheck, parsePileupConfig

#
# simple utils for data mining the request dictionary
#
isGenerator = lambda args: not args["Task1"].get("InputDataset", None)
parentTaskModule = lambda args: args.get("InputFromOutputModule", None)


class ParameterStorage(object):
    """
    _ParameterStorage_

    Decorator class which storages global parameters,
    sets them to local values before executing the passed function
    and restores them afterwards. This is only suited to decorate the
    setupTask and setupGeneratorTask in TaskChainWorkloadFactory.
    """

    def __init__(self, func):
        """
        __init__

        Stores the function and valid parameters to save/restore.

        Supported parameters are:

        Global tag, CMSSW version, Scram arch
        Primary Dataset, Processing Version, Processing String, Acquisition Era

        The validParameters dictionary contains a mapping from the name of the attribute
        in StdBase to the argument key in the task dictionaries
        """
        self.func = func
        self.validParameters = {'globalTag': 'GlobalTag',
                                'frameworkVersion': 'CMSSWVersion',
                                'scramArch': 'ScramArch',
                                'processingVersion': 'ProcessingVersion',
                                'processingString': 'ProcessingString',
                                'acquisitionEra': 'AcquisitionEra',
                                'timePerEvent': 'TimePerEvent',
                                'sizePerEvent': 'SizePerEvent',
                                'memory': 'Memory',
                                'dqmConfigCacheID': 'DQMConfigCacheID'
                               }
        return

    def __get__(self, instance, owner):
        """
        __get__

        Get method for the class, store the calling instance for latter use
        """
        self.obj = instance
        self.cls = owner
        return self.__call__

    def __call__(self, task, taskConf):
        """
        __call__

        Store the global parameters, alters the parameters
        using the taskConf argument. Executes the stored
        method, then restores the parameters and resets the local instance.
        """
        self.storeParameters()
        self.alterParameters(taskConf)
        self.func(self.obj, task, taskConf)
        self.restoreParameters()
        self.resetParameters()
        return

    def storeParameters(self):
        """
        _storeParameters_

        Store the original parameters in the decorator
        """
        for param in self.validParameters:
            globalValue = getattr(self.obj, param, None)
            setattr(self, param, globalValue)
        return

    def alterParameters(self, taskConf):
        """
        _alterParameters_

        Alter the parameters with the specific task configuration
        """
        for param in self.validParameters:
            if self.validParameters[param] in taskConf:
                taskValue = taskConf[self.validParameters[param]]
                setattr(self.obj, param, taskValue)
        return

    def restoreParameters(self):
        """
        _restoreParameters_

        Restore the parameters to the global values
        """
        for param in self.validParameters:
            globalValue = getattr(self, param)
            setattr(self.obj, param, globalValue)
        return

    def resetParameters(self):
        """
        _resetParameters_

        Reset parameters to None
        """
        for param in self.validParameters:
            setattr(self, param, None)
        return


class TaskChainWorkloadFactory(StdBase):
    def __init__(self):
        StdBase.__init__(self)
        self.mergeMapping = {}
        self.taskMapping = {}

    def __call__(self, workloadName, arguments):
        """
        _call_

        Create a ReReco workload with the given parameters.
        """
        StdBase.__call__(self, workloadName, arguments)
        self.workload = self.createWorkload()

        # Detect blow-up factor from first task in chain.
        blowupFactor = 0
        if (self.taskChain > 1) and 'TimePerEvent' in arguments["Task1"]:
            origTpe = arguments["Task1"]['TimePerEvent']
            if origTpe <= 0: origTpe = 1.0
            sumTpe = 0
            tpeCount = 0
            for i in range(1, self.taskChain + 1):
                if 'TimePerEvent' in arguments["Task%d" % i]:
                    sumTpe += arguments["Task%d" % i]['TimePerEvent']
                    tpeCount += 1
            if tpeCount > 0:
                blowupFactor = sumTpe / float(origTpe)

        for i in range(1, self.taskChain + 1):

            originalTaskConf = arguments["Task%d" % i]
            taskConf = {}
            # Make a shallow copy of the taskConf
            for k, v in originalTaskConf.items():
                taskConf[k] = v
            parent = taskConf.get("InputTask", None)

            self.modifyTaskConfiguration(taskConf, i == 1, i == 1 and 'InputDataset' not in taskConf)

            # Set task-specific global parameters
            self.blockBlacklist = taskConf["BlockBlacklist"]
            self.blockWhitelist = taskConf["BlockWhitelist"]
            self.runBlacklist = taskConf["RunBlacklist"]
            self.runWhitelist = taskConf["RunWhitelist"]

            if taskConf['Multicore'] and taskConf['Multicore'] != 'None':
                self.multicoreNCores = int(taskConf['Multicore'])

            parentTask = None
            if parent in self.mergeMapping:
                parentTask = self.mergeMapping[parent][parentTaskModule(taskConf)]

            task = self.makeTask(taskConf, parentTask)

            if i == 1:
                # First task will either be generator or processing
                self.workload.setDashboardActivity("relval")
                if isGenerator(arguments):
                    # generate mc events
                    self.workload.setWorkQueueSplitPolicy("MonteCarlo", taskConf['SplittingAlgo'],
                                                          taskConf['SplittingArguments'],
                                                          BlowupFactor=blowupFactor)
                    self.workload.setEndPolicy("SingleShot")
                    self.setupGeneratorTask(task, taskConf)
                else:
                    # process an existing dataset
                    self.workload.setWorkQueueSplitPolicy("Block", taskConf['SplittingAlgo'],
                                                          taskConf['SplittingArguments'],
                                                          BlowupFactor=blowupFactor)
                    self.setupTask(task, taskConf)
                self.reportWorkflowToDashboard(self.workload.getDashboardActivity())
            else:
                # all subsequent tasks have to be processing tasks
                self.setupTask(task, taskConf)
            self.taskMapping[task.name()] = taskConf

        self.workload.ignoreOutputModules(self.ignoredOutputModules)

        return self.workload

    def makeTask(self, taskConf, parentTask=None):
        """
        _makeTask_

        create new Task and populate it with basic required parameters from the
        taskConfig provided, if parentTask is None, the task will be created in
        the workload, else the task will be created as a child of the parent Task

        """
        if parentTask == None:
            task = self.workload.newTask(taskConf['TaskName'])
        else:
            task = parentTask.addTask(taskConf['TaskName'])
        return task

    def _updateCommonParams(self, task, taskConf):
        # sets the prepID  all the properties need to be set by
        # self.workload.setTaskPropertiesFromWorkload manually for the task
        task.setPrepID(taskConf.get("PrepID", self.workload.getPrepID()))
        task.setAcquisitionEra(taskConf.get("AcquisitionEra", self.workload.acquisitionEra))
        task.setProcessingString(taskConf.get("ProcessingString", self.workload.processingString))
        task.setProcessingVersion(taskConf.get("ProcessingVersion", self.workload.processingVersion))
        lumiMask = taskConf.get("LumiList", self.workload.lumiList)
        if lumiMask:
            task.setLumiMask(lumiMask)

        if taskConf["PileupConfig"]:
            self.setupPileup(task, taskConf['PileupConfig'])

    @ParameterStorage
    def setupGeneratorTask(self, task, taskConf):
        """
        _setupGeneratorTask_

        Set up an initial generation task
        """
        cmsswStepType = "CMSSW"
        configCacheID = taskConf['ConfigCacheID']
        splitAlgorithm = taskConf["SplittingAlgo"]
        splitArguments = taskConf["SplittingArguments"]
        keepOutput = taskConf["KeepOutput"]
        transientModules = taskConf["TransientOutputModules"]
        forceUnmerged = (not keepOutput) or (len(transientModules) > 0)

        self.inputPrimaryDataset = taskConf['PrimaryDataset']
        outputMods = self.setupProcessingTask(task, "Production",
                                              couchURL=self.couchURL, couchDBName=self.couchDBName,
                                              configDoc=configCacheID, splitAlgo=splitAlgorithm,
                                              configCacheUrl=self.configCacheUrl,
                                              splitArgs=splitArguments, stepType=cmsswStepType,
                                              seeding=taskConf['Seeding'], totalEvents=taskConf['RequestNumEvents'],
                                              forceUnmerged=forceUnmerged,
                                              timePerEvent=taskConf.get('TimePerEvent', None),
                                              sizePerEvent=taskConf.get('SizePerEvent', None),
                                              memoryReq=taskConf.get('Memory', None),
                                              taskConf=taskConf)

        # this need to be called after setpuProcessingTask since it will overwrite some values
        self._updateCommonParams(task, taskConf)

        self.addLogCollectTask(task, 'LogCollectFor%s' % task.name())

        # Do the output module merged/unmerged association
        self.setUpMergeTasks(task, outputMods, splitAlgorithm,
                             keepOutput, transientModules)

        return

    @ParameterStorage
    def setupTask(self, task, taskConf):
        """
        _setupTask_

        Build the task using the setupProcessingTask from StdBase
        and set the parents appropriately to handle a processing task
        """

        cmsswStepType = "CMSSW"
        configCacheID = taskConf["ConfigCacheID"]
        splitAlgorithm = taskConf["SplittingAlgo"]
        splitArguments = taskConf["SplittingArguments"]
        keepOutput = taskConf["KeepOutput"]
        transientModules = taskConf["TransientOutputModules"]
        forceUnmerged = (not keepOutput) or (len(transientModules) > 0)

        # in case the initial task is a processing task, we have an input dataset, otherwise
        # we look up the parent task and step
        inputDataset = taskConf["InputDataset"]
        if inputDataset is not None:
            self.inputDataset = inputDataset
            (self.inputPrimaryDataset, self.inputProcessedDataset,
             self.inputDataTier) = self.inputDataset[1:].split("/")
            inpStep = None
            inpMod = None
        else:
            self.inputDataset = None
            inputTask = taskConf["InputTask"]
            inputTaskConf = self.taskMapping[inputTask]
            parentTaskForMod = self.mergeMapping[inputTask][taskConf['InputFromOutputModule']]
            inpStep = parentTaskForMod.getStep("cmsRun1")
            if not inputTaskConf["KeepOutput"] or len(inputTaskConf["TransientOutputModules"]) > 0:
                inpMod = taskConf["InputFromOutputModule"]
                # Check if the splitting has to be changed
                if inputTaskConf["SplittingAlgo"] == 'EventBased' \
                        and (inputTaskConf["InputDataset"] or inputTaskConf["InputTask"]):
                    splitAlgorithm = 'WMBSMergeBySize'
                    splitArguments = {'max_merge_size': self.maxMergeSize,
                                      'min_merge_size': self.minMergeSize,
                                      'max_merge_events': self.maxMergeEvents,
                                      'max_wait_time': self.maxWaitTime}
            else:
                inpMod = "Merged"

        currentPrimaryDataset = self.inputPrimaryDataset
        if taskConf["PrimaryDataset"] is not None:
            self.inputPrimaryDataset = taskConf.get("PrimaryDataset")

        couchUrl = self.couchURL
        couchDB = self.couchDBName
        outputMods = self.setupProcessingTask(task, "Processing",
                                              inputDataset,
                                              inputStep=inpStep,
                                              inputModule=inpMod,
                                              couchURL=couchUrl,
                                              couchDBName=couchDB,
                                              configCacheUrl=self.configCacheUrl,
                                              configDoc=configCacheID,
                                              splitAlgo=taskConf["SplittingAlgo"],
                                              splitArgs=splitArguments,
                                              stepType=cmsswStepType,
                                              forceUnmerged=forceUnmerged,
                                              timePerEvent=taskConf.get('TimePerEvent', None),
                                              sizePerEvent=taskConf.get('SizePerEvent', None),
                                              memoryReq=taskConf.get("Memory", None),
                                              taskConf=taskConf)

        # this need to be called after setpuProcessingTask since it will overwrite some values
        self._updateCommonParams(task, taskConf)

        self.addLogCollectTask(task, 'LogCollectFor%s' % task.name())
        self.setUpMergeTasks(task, outputMods, splitAlgorithm,
                             keepOutput, transientModules)

        self.inputPrimaryDataset = currentPrimaryDataset

        return

    def setUpMergeTasks(self, parentTask, outputModules, splittingAlgo,
                        keepOutput, transientOutputModules):
        """
        _setUpMergeTasks_

        Set up the required merged tasks according to the following parameters:
        - KeepOutput : All output modules not in the transient list are merged.
        - TransientOutputModules : These output modules won't be merged.
        If not merged then only a cleanup task is created.
        """
        modulesToMerge = []
        unmergedModules = outputModules.keys()
        if keepOutput:
            unmergedModules = filter(lambda x: x in transientOutputModules, outputModules.keys())
            modulesToMerge = filter(lambda x: x not in transientOutputModules, outputModules.keys())

        procMergeTasks = {}
        for outputModuleName in modulesToMerge:
            mergeTask = self.addMergeTask(parentTask, splittingAlgo,
                                          outputModuleName)
            procMergeTasks[str(outputModuleName)] = mergeTask
        self.mergeMapping[parentTask.name()] = procMergeTasks

        procTasks = {}
        for outputModuleName in unmergedModules:
            self.addCleanupTask(parentTask, outputModuleName)
            procTasks[outputModuleName] = parentTask
        self.mergeMapping[parentTask.name()].update(procTasks)

        return

    def modifyTaskConfiguration(self, taskConf,
                                firstTask=False, generator=False):
        """
        _modifyTaskConfiguration_

        Modify the TaskConfiguration according to the specifications
        in getWorkloadArguments and getTaskArguments. It does
        type casting and assigns default values.
        """
        taskArguments = self.getTaskArguments(firstTask, generator)
        for argument in taskArguments:
            if argument not in taskConf:
                taskConf[argument] = taskArguments[argument]["default"]
            else:
                taskConf[argument] = taskArguments[argument]["type"](taskConf[argument])
        baseArguments = self.getWorkloadArguments()
        for argument in baseArguments:
            if argument in taskConf:
                taskConf[argument] = baseArguments[argument]["type"](taskConf[argument])

        if generator:
            taskConf["SplittingAlgo"] = "EventBased"
            # Adjust totalEvents according to the filter efficiency
            taskConf["RequestNumEvents"] = int(taskConf.get("RequestNumEvents", 0) / \
                                               taskConf.get("FilterEfficiency"))
            taskConf["SizePerEvent"] = taskConf.get("SizePerEvent", self.sizePerEvent) * \
                                       taskConf.get("FilterEfficiency")

        if taskConf["EventsPerJob"] is None:
            taskConf["EventsPerJob"] = int((8.0 * 3600.0) / (taskConf.get("TimePerEvent", self.timePerEvent)))
        if taskConf["EventsPerLumi"] is None:
            taskConf["EventsPerLumi"] = taskConf["EventsPerJob"]

        taskConf["SplittingArguments"] = {}
        if taskConf["SplittingAlgo"] == "EventBased" or taskConf["SplittingAlgo"] == "EventAwareLumiBased":
            taskConf["SplittingArguments"]["events_per_job"] = taskConf["EventsPerJob"]
            if taskConf["SplittingAlgo"] == "EventAwareLumiBased":
                taskConf["SplittingArguments"]["max_events_per_lumi"] = 20000
            else:
                taskConf["SplittingArguments"]["events_per_lumi"] = taskConf["EventsPerLumi"]
            taskConf["SplittingArguments"]["lheInputFiles"] = taskConf["LheInputFiles"]
        elif taskConf["SplittingAlgo"] == "LumiBased":
            taskConf["SplittingArguments"]["lumis_per_job"] = taskConf["LumisPerJob"]
        elif taskConf["SplittingAlgo"] == "FileBased":
            taskConf["SplittingArguments"]["files_per_job"] = taskConf["FilesPerJob"]

        taskConf["PileupConfig"] = parsePileupConfig(taskConf["MCPileup"], taskConf["DataPileup"])
        # Adjust the pileup splitting
        taskConf["SplittingArguments"].setdefault("deterministicPileup", taskConf['DeterministicPileup'])

        return

    @staticmethod
    def getWorkloadArguments():
        baseArgs = StdBase.getWorkloadArguments()
        reqMgrArgs = StdBase.getWorkloadArgumentsWithReqMgr()
        baseArgs.update(reqMgrArgs)
        specArgs = {"RequestType": {"default": "TaskChain", "optional": False,
                                    "attr": "requestType"},
                    "GlobalTag": {"default": "GT_TC_V1", "type": str,
                                  "optional": False, "validate": None,
                                  "attr": "globalTag", "null": False},
                    "CouchURL": {"default": "http://localhost:5984", "type": str,
                                 "optional": False, "validate": couchurl,
                                 "attr": "couchURL", "null": False},
                    "CouchDBName": {"default": "dp_configcache", "type": str,
                                    "optional": False, "validate": identifier,
                                    "attr": "couchDBName", "null": False},
                    "ConfigCacheUrl": {"default": None, "type": str,
                                       "optional": True, "validate": None,
                                       "attr": "configCacheUrl", "null": True},
                    "IgnoredOutputModules": {"default": [], "type": makeList,
                                             "optional": True, "validate": None,
                                             "attr": "ignoredOutputModules", "null": False},
                    "TaskChain": {"default": 1, "type": int,
                                  "optional": False, "validate": lambda x: x > 0,
                                  "attr": "taskChain", "null": False},
                    "FirstEvent": {"default": 1, "type": int,
                                   "optional": True, "validate": lambda x: x > 0,
                                   "attr": "firstEvent", "null": False},
                    "FirstLumi": {"default": 1, "type": int,
                                  "optional": True, "validate": lambda x: x > 0,
                                  "attr": "firstLumi", "null": False}
                   }
        baseArgs.update(specArgs)
        StdBase.setDefaultArgumentsProperty(baseArgs)
        return baseArgs

    @staticmethod
    def getTaskArguments(firstTask=False, generator=False):
        """
        _getTaskArguments_

        Each task dictionary specifies its own set of arguments
        that need to be validated as well, most of them are already
        defined in StdBase.getWorkloadArguments and those do not appear here
        since they are all optional. Here only new arguments are listed.
        """
        specArgs = {"TaskName": {"default": None, "type": str,
                                 "optional": False, "validate": None,
                                 "null": False},
                    "ConfigCacheUrl": {"default": "https://cmsweb.cern.ch/couchdb", "type": str,
                                       "optional": False, "validate": None,
                                       "attr": "configCacheUrl", "null": False},
                    "ConfigCacheID": {"default": None, "type": str,
                                      "optional": False, "validate": None,
                                      "null": False},
                    "KeepOutput": {"default": True, "type": strToBool,
                                   "optional": True, "validate": None,
                                   "null": False},
                    "TransientOutputModules": {"default": [], "type": makeList,
                                               "optional": True, "validate": None,
                                               "null": False},
                    "PrimaryDataset": {"default": None, "type": str,
                                       "optional": not generator, "validate": primdataset,
                                       "null": False},
                    "Seeding": {"default": "AutomaticSeeding", "type": str,
                                "optional": True,
                                "validate": lambda x: x in ["ReproducibleSeeding", "AutomaticSeeding"],
                                "null": False},
                    "RequestNumEvents": {"default": 1000, "type": int,
                                         "optional": not generator, "validate": lambda x: x > 0,
                                         "null": False},
                    "MCPileup": {"default": None, "type": str,
                                 "optional": True, "validate": dataset,
                                 "null": False},
                    "DataPileup": {"default": None, "type": str,
                                   "optional": True, "validate": dataset,
                                   "null": False},
                    "DeterministicPileup": {"default": False, "type": strToBool,
                                            "optional": True, "validate": None,
                                            "attr": "deterministicPileup", "null": False},
                    "InputDataset": {"default": None, "type": str,
                                     "optional": generator or not firstTask, "validate": dataset,
                                     "null": False},
                    "InputTask": {"default": None, "type": str,
                                  "optional": firstTask, "validate": None,
                                  "null": False},
                    "InputFromOutputModule": {"default": None, "type": str,
                                              "optional": firstTask, "validate": None,
                                              "null": False},
                    "BlockBlacklist": {"default": [], "type": makeList,
                                       "optional": True, "validate": lambda x: all([block(y) for y in x]),
                                       "null": False},
                    "BlockWhitelist": {"default": [], "type": makeList,
                                       "optional": True, "validate": lambda x: all([block(y) for y in x]),
                                       "null": False},
                    "RunBlacklist": {"default": [], "type": makeList,
                                     "optional": True, "validate": lambda x: all([int(y) > 0 for y in x]),
                                     "null": False},
                    "RunWhitelist": {"default": [], "type": makeList,
                                     "optional": True, "validate": lambda x: all([int(y) > 0 for y in x]),
                                     "null": False},
                    "SplittingAlgo": {"default": "EventAwareLumiBased", "type": str,
                                      "optional": True, "validate": lambda x: x in ["EventBased", "LumiBased",
                                                                                    "EventAwareLumiBased", "FileBased"],
                                      "null": False},
                    "EventsPerJob": {"default": None, "type": int,
                                     "optional": True, "validate": lambda x: x > 0,
                                     "null": False},
                    "LumisPerJob": {"default": 8, "type": int,
                                    "optional": True, "validate": lambda x: x > 0,
                                    "null": False},
                    "FilesPerJob": {"default": 1, "type": int,
                                    "optional": True, "validate": lambda x: x > 0,
                                    "null": False},
                    "EventsPerLumi": {"default": None, "type": int,
                                      "optional": True, "validate": lambda x: x > 0,
                                      "attr": "eventsPerLumi", "null": True},
                    "FilterEfficiency": {"default": 1.0, "type": float,
                                         "optional": True, "validate": lambda x: x > 0.0,
                                         "attr": "filterEfficiency", "null": False},
                    "LheInputFiles": {"default": False, "type": strToBool,
                                      "optional": True, "validate": None,
                                      "attr": "lheInputFiles", "null": False},
                    "PrepID": {"default": None, "type": str,
                               "optional": True, "validate": None,
                               "attr": "prepID", "null": True},
                    "Multicore": {"default": None, "type": int,
                                  "optional": True, "validate": lambda x: x > 0,
                                  "null": False},
                   }
        StdBase.setDefaultArgumentsProperty(specArgs)
        return specArgs

    def validateSchema(self, schema):
        """
        _validateSchema_

        Go over each task and make sure it matches validation
        parameters derived from Dave's requirements.
        """
        numTasks = schema['TaskChain']
        transientMapping = {}
        for i in range(1, numTasks + 1):
            taskName = "Task%s" % i
            if taskName not in schema:
                msg = "No Task%s entry present in request" % i
                self.raiseValidationException(msg=msg)

            task = schema[taskName]
            # We can't handle non-dictionary tasks
            if not isinstance(task, dict):
                msg = "Non-dictionary input for task in TaskChain.\n"
                msg += "Could be an indicator of JSON error.\n"
                self.raiseValidationException(msg=msg)

            # Generic task parameter validation
            self.validateTask(task, self.getTaskArguments(i == 1, i == 1 and 'InputDataset' not in task))

            # Validate the existence of the configCache
            if task["ConfigCacheID"]:
                configCacheUrl = schema.get("ConfigCacheUrl", None) or schema["CouchURL"]
                self.validateConfigCacheExists(configID=task['ConfigCacheID'],
                                               couchURL=configCacheUrl,
                                               couchDBName=schema["CouchDBName"],
                                               getOutputModules=True)

            # Validate the chaining of transient output modules, need to make a copy of the lists
            transientMapping[task['TaskName']] = [x for x in task.get('TransientOutputModules', [])]

            if i > 1:
                inputTransientModules = transientMapping[task['InputTask']]
                if task['InputFromOutputModule'] in inputTransientModules:
                    inputTransientModules.remove(task['InputFromOutputModule'])

        for task in transientMapping:
            if transientMapping[task]:
                msg = "A transient module is not processed by a subsequent task.\n"
                msg += "This is a malformed task chain workload"
                self.raiseValidationException(msg)

    def validateTask(self, taskConf, taskArgumentDefinition):
        """
        _validateTask_

        Validate the task information against the given
        argument description
        """
        msg = validateArgumentsCreate(taskConf, taskArgumentDefinition)
        if msg is not None:
            self.raiseValidationException(msg)

        # Also retrieve the "main" arguments which may be overriden in the task
        # Change them all to optional for validation
        baseArgs = self.getWorkloadArguments()
        validateArgumentsNoOptionalCheck(taskConf, baseArgs)

        for arg in baseArgs:
            baseArgs[arg]["optional"] = True
        msg = validateArgumentsCreate(taskConf, baseArgs)
        if msg is not None:
            self.raiseValidationException(msg)
        return
