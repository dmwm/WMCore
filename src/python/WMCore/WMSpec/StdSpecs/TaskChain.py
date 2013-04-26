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

from WMCore.WMSpec.StdSpecs.StdBase import StdBase, WMSpecFactoryException

# Validate functions
def validateSubTask(task, firstTask = False):
    """
    _validateSubTask_
    
    Check required fields for a sub task
    """
    reqKeys = ["TaskName", "SplittingAlgorithm", "SplittingArguments"]
    for rK in reqKeys:
        if not task.has_key(rK):
            msg = "Sub Task missing Required Key: %s\n" % rK
            msg += str(task)
            raise WMSpecFactoryException(msg)
    # 
    # input definition checks
    #
    if not firstTask:
        if not task.has_key("InputTask"):
            msg = "Task %s has no InputTask setting" % task['TaskName']
            raise WMSpecFactoryException(msg)
        if not task.has_key("InputFromOutputModule"):
            msg = "Task %s has no InputFromOutputModule setting" % task['TaskName']
            raise WMSpecFactoryException(msg)
        
    # configuration checks
    check = task.has_key("ProcScenario") or task.has_key("ConfigCacheID")
    if not check:
        msg = "Task %s has no Scenario or ConfigCacheID, one of these must be provided" % task['TaskName']
        raise WMSpecFactoryException(msg)
    if task.has_key("ProcScenario"):
        if not task.has_key("ScenarioMethod"):
            msg = "Scenario Specified for Task %s but no ScenarioMethod provided" % task['TaskName']
            raise WMSpecFactoryException(msg)
        scenArgs = task.get("ScenarioArgs", {})
        if not scenArgs.has_key("writeTiers"):
            msg = "task %s ScenarioArgs does not contain writeTiers argument" % task['TaskName']
            raise WMSpecFactoryException, msg

def validateGenFirstTask(task):
    """
    _validateGenFirstTask_
    
    Validate first task contains all stuff required for generation
    """
    if not task.has_key("RequestNumEvents"):
        msg = "RequestNumEvents is required for first Generator task"
        raise WMSpecFactoryException(msg)

    if not task.has_key("PrimaryDataset"):
        msg = "PrimaryDataset is required for first Generator task"
        raise WMSpecFactoryException(msg)

def validateProcFirstTask(task):
    """
    _validateProcFirstTask_
    
    Validate that Processing First task contains required params
    """
    if task['InputDataset'].count('/') != 3:
        raise WMSpecFactoryException("Need three slashes in InputDataset %s " % task['InputDataset'])

def parsePileupConfig(taskConfig):
    """
    _parsePileupConfig_

    If the pileup config is defined as MCPileup and DataPileup
    then make sure we get the usual dictionary as
    PileupConfig : {'mc' : '/mc/procds/tier', 'data': '/minbias/procds/tier'}

    This overrides any existing PileupConfig attribute in the task dict,
    the requestor shouldn't have put both forms of the pileup config anyway.
    """
    pileUpConfig = {}
    if taskConfig.get('MCPileup', None):
        pileUpConfig['mc'] = [taskConfig['MCPileup']]
    if taskConfig.get('DataPileup', None):
        pileUpConfig['data'] = [taskConfig['DataPileup']]
    taskConfig['PileupConfig'] = pileUpConfig

#
# simple utils for data mining the request dictionary
# 
getTaskN = lambda args, tasknum: args.get("Task%s" % tasknum, None)
isGenerator = lambda args: not args["Task1"].has_key("InputDataset")
parentTaskName = lambda args: args.get("InputTask", None)
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
        self.validParameters = {'globalTag' : 'GlobalTag',
                                'frameworkVersion' : 'CMSSWVersion',
                                'scramArch' : 'ScramArch',
                                'processingVersion' : 'ProcessingVersion',
                                'processingString' : 'ProcessingString',
                                'acquisitionEra' : 'AcquisitionEra',
                                'timePerEvent' : 'TimePerEvent',
                                'sizePerEvent' : 'SizePerEvent',
                                'memory' : 'Memory'
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
        self.restoreParameters(taskConf)
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

    def restoreParameters(self, taskConf):
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
        self.arguments = {}
        self.multicore = False
        self.multicoreNCores = 1
        self.ignoredOutputModules = []

    def __call__(self, workloadName, arguments):
        """
        _call_

        Create a ReReco workload with the given parameters.
        """
        StdBase.__call__(self, workloadName, arguments)
        self.workload = self.createWorkload()
        self.arguments = arguments
        self.couchURL = arguments['CouchURL']
        self.couchDBName = arguments['CouchDBName']
        self.configCacheUrl = arguments.get("ConfigCacheUrl", None)
        self.frameworkVersion = arguments["CMSSWVersion"]
        self.globalTag = arguments.get("GlobalTag", None)
        self.ignoredOutputModules = arguments.get("IgnoredOutputModules", [])

        # Optional arguments that default to something reasonable.
        self.dbsUrl = arguments.get("DbsUrl", "http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet")
        self.emulation = arguments.get("Emulation", False)
        
        numTasks = arguments['TaskChain']
        for i in range(1, numTasks+1):
            #consistency check that there are numTasks defined in the request:
            if not arguments.has_key("Task%s" % i):
                msg = "Specified number of tasks: %s does not match defined task dictionary for Task%s" % (i, i)
                raise RuntimeError, msg
                
            taskConf = getTaskN(arguments, i)
            parent = parentTaskName(taskConf)

            # Set task-specific global parameters
            self.blockBlacklist = taskConf.get("BlockBlacklist", [])
            self.blockWhitelist = taskConf.get("BlockWhitelist", [])
            self.runBlacklist   = taskConf.get("RunBlacklist", [])
            self.runWhitelist   = taskConf.get("RunWhitelist", [])

            parentTask = None
            if parent in self.mergeMapping:
                parentTask = self.mergeMapping[parent][parentTaskModule(taskConf)]
                
            task = self.makeTask(taskConf, parentTask)
            if i == 1:
                # First task will either be generator or processing
                self.workload.setDashboardActivity("relval")
                if isGenerator(arguments):
                    # generate mc events
                    self.workload.setWorkQueueSplitPolicy("MonteCarlo", taskConf['SplittingAlgorithm'], 
                                                          taskConf['SplittingArguments'])
                    self.workload.setEndPolicy("SingleShot")
                    self.setupGeneratorTask(task, taskConf)
                else:
                    # process an existing dataset
                    self.workload.setWorkQueueSplitPolicy("Block", taskConf['SplittingAlgorithm'],
                                                     taskConf['SplittingArguments'])
                    self.setupTask(task, taskConf)
                self.reportWorkflowToDashboard(self.workload.getDashboardActivity())
            else:
                # all subsequent tasks have to be processing tasks
                self.setupTask(task, taskConf)
            self.taskMapping[task.name()] = taskConf

        self.workload.ignoreOutputModules(self.ignoredOutputModules)
        return self.workload  

            
    def makeTask(self, taskConf, parentTask = None):
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

    @ParameterStorage
    def setupGeneratorTask(self, task, taskConf):
        """
        _setupGeneratorTask_
        
        Set up an initial generation task
        """
        cmsswStepType = "CMSSW"
        configCacheID = taskConf['ConfigCacheID']
        splitAlgorithm = taskConf.get('SplittingAlgorithm', 'EventBased')
        splitArguments = taskConf.get('SplittingArguments', {'events_per_job': int((24*3600)/float(self.timePerEvent))})
        keepOutput = taskConf.get('KeepOutput', True)
        transientModules = taskConf.get('TransientOutputModules', [])
        forceUnmerged = (not keepOutput) or (len(transientModules) > 0)

        self.inputPrimaryDataset = taskConf['PrimaryDataset']
        outputMods = self.setupProcessingTask(task, "Production",
                                              couchURL = self.couchURL, couchDBName = self.couchDBName,
                                              configDoc = configCacheID, splitAlgo = splitAlgorithm,
                                              configCacheUrl = self.configCacheUrl,
                                              splitArgs = splitArguments, stepType = cmsswStepType,
                                              seeding = taskConf['Seeding'], totalEvents = taskConf['RequestNumEvents'],
                                              forceUnmerged = forceUnmerged, timePerEvent = self.timePerEvent,
                                              memoryReq = self.memory, sizePerEvent = self.sizePerEvent)

        # Set up any pileup
        if 'MCPileup' in taskConf or 'DataPileup' in taskConf:
            parsePileupConfig(taskConf)
        if taskConf.get('PileupConfig', None):
            self.setupPileup(task, taskConf['PileupConfig'])

        self.addLogCollectTask(task, 'LogCollectFor%s' % task.name())

        # Do the output module merged/unmerged association
        self.setUpMergeTasks(task, outputMods, splitAlgorithm,
                             keepOutput, transientModules)

        return

    @ParameterStorage
    def setupTask(self, task, taskConf):
        """
        _setupTask_
        
        Build the task using the setupProcessingTask from StdBase and set the parents appropriately to handle
        a processing task
        """
       
        cmsswStepType  = "CMSSW"
        configCacheID  = taskConf.get('ConfigCacheID', None)
        splitAlgorithm = taskConf.get('SplittingAlgorithm', 'LumiBased')
        splitArguments = taskConf.get('SplittingArguments', {'lumis_per_job': 8})
        keepOutput     = taskConf.get('KeepOutput', True)
        transientModules = taskConf.get('TransientOutputModules', [])
        forceUnmerged = (not keepOutput) or (len(transientModules) > 0)

        # in case the initial task is a processing task, we have an input dataset, otherwise
        # we look up the parent task and step
        inputDataset = taskConf.get("InputDataset", None)
        if inputDataset != None:
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
            if not inputTaskConf.get('KeepOutput', True) or len(inputTaskConf.get('TransientOutputModules', [])) > 0:
                inpMod = taskConf['InputFromOutputModule']
                # Check if the splitting has to be changed
                if inputTaskConf.get('SplittingAlgorithm', 'LumiBased') == 'EventBased' \
                   and (('InputDataset' in inputTaskConf) or ('InputTask' in inputTaskConf)):
                    splitAlgorithm = 'WMBSMergeBySize'
                    splitArguments = {'max_merge_size'   : self.maxMergeSize,
                                      'min_merge_size'   : self.minMergeSize,
                                      'max_merge_events' : self.maxMergeEvents,
                                      'max_wait_time'    : self.maxWaitTime}
            else:
                inpMod = "Merged"

        currentPrimaryDataset = self.inputPrimaryDataset
        if taskConf.get("PrimaryDataset") is not None:
            self.inputPrimaryDataset = taskConf.get("PrimaryDataset")

        scenarioFunc = None
        scenarioArgs = {}
        couchUrl = self.couchURL
        couchDB = self.couchDBName
        if taskConf.get("ProcScenario", None) != None:
            self.procScenario = taskConf['ProcScenario']
            scenarioFunc = taskConf['ScenarioMethod']
            scenarioArgs = taskConf['ScenarioArguments']

        outputMods = self.setupProcessingTask(task, "Processing", inputDataset, inputStep = inpStep, inputModule = inpMod,
                                              scenarioName = self.procScenario, scenarioFunc = scenarioFunc, scenarioArgs = scenarioArgs,
                                              couchURL = couchUrl, couchDBName = couchDB,
                                              configCacheUrl = self.configCacheUrl,
                                              configDoc = configCacheID, splitAlgo = splitAlgorithm,
                                              splitArgs = splitArguments, stepType = cmsswStepType,
                                              forceUnmerged = forceUnmerged, timePerEvent = self.timePerEvent,
                                              memoryReq = self.memory, sizePerEvent = self.sizePerEvent)


        if 'MCPileup' in taskConf or 'DataPileup' in taskConf:
            parsePileupConfig(taskConf)
        if taskConf.get('PileupConfig', None):
            self.setupPileup(task, taskConf['PileupConfig'])

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
            unmergedModules = filter(lambda x : x in transientOutputModules, outputModules.keys())
            modulesToMerge = filter(lambda x : x not in transientOutputModules, outputModules.keys())

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

    def validateSchema(self, schema):
        """
        _validateSchema_
        
        Go over each task and make sure it matches validation
        parameters derived from Dave's requirements.
        """
        try:
            numTasks = int(schema['TaskChain'])
        except ValueError:
            msg = "TaskChain parameter is not an Integer"
            self.raiseValidationException(msg = msg)

        if numTasks == 0:
            msg = "No tasks present in taskChain!"
            self.raiseValidationException(msg = msg)

        transientMapping = {}
        for i in range(1, numTasks+1):
            taskName = "Task%s" % i
            if not schema.has_key(taskName):
                msg = "No Task%s entry present in request" % i
                self.raiseValidationException(msg = msg)

            task = schema[taskName]
            # We can't handle non-dictionary tasks
            if type(task) != dict:
                    msg =  "Non-dictionary input for task in TaskChain.\n"
                    msg += "Could be an indicator of JSON error.\n"
                    self.raiseValidationException(msg = msg)

            if i == 1:
                if task.has_key('InputDataset'):
                    validateProcFirstTask(task)
                else:
                    validateGenFirstTask(task)
                validateSubTask(task, firstTask = True)
            else:
                validateSubTask(task)

            # Validate the existence of the configCache
            if task.has_key("ConfigCacheID"):
                configCacheUrl = schema.get("ConfigCacheUrl", None) or schema["CouchURL"]
                self.validateConfigCacheExists(configID = task['ConfigCacheID'],
                                               couchURL = configCacheUrl,
                                               couchDBName = schema["CouchDBName"],
                                               getOutputModules = True)

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

def taskChainWorkload(workloadName, arguments):
    """
    _taskChainWorkload_
    
    Helper to generate a TaskChain workload given the name & arguments provided

    """
    f = TaskChainWorkloadFactory()
    return f(workloadName, arguments)