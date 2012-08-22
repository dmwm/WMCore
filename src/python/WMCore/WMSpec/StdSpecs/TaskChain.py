#!/usr/bin/env python
# encoding: utf-8
"""
TaskChain.py

Created by Dave Evans on 2011-06-15.
Copyright (c) 2011 Fermilab. All rights reserved.

Provide support for building arbitrary chains of WMTasks based on a nested dictionary structure
starting with either a generation (create new MC events) or processing (use an existing input dataset) step, followed
by a chain of dependent WMTasks that process the subsequent output.

The request is formed as a dictionary where some global parameters are provided as normal, but the 
processing tasks are specified as sub dictionaries.

The top level dict should contain the parameter TaskChain and the value is the number of processing tasks to be run.
For each count in the chain, a dictionary entry named Task1...N should be made with a value being another dictionary.

The main request parameters required are:

{
    "AcquisitionEra": "ReleaseValidation",            Acq Era
    "Requestor": "sfoulkes@fnal.gov",                 Person responsible
    "CMSSWVersion": "CMSSW_3_5_8",                    CMSSW Version (used for all tasks in chain)
    "ScramArch": "slc5_ia32_gcc434",                  Scram Arch (used for all tasks in chain)
    "ProcessingVersion": "1",                        Processing Version (used for all tasks in chain)
    "GlobalTag": "GR10_P_v4::All",                    Global Tag (used for all tasks)
    "CouchURL": "http://couchserver.cern.ch",         URL of CouchDB containing Config Cache
    "CouchDBName": "config_cache",                    Name of Couch Database containing config cache 
                                                       - Will contain all configs for all Tasks
    "SiteWhitelist" : ["T1_CH_CERN", "T1_US_FNAL"],   Site whitelist 
    "TaskChain" : 4,                                  Define number of tasks in chain.
}


Task1 will be either a generation or processing task:

Example initial generation task:

"Task1" :{
    "TaskName" : "GenSim",                            Task Name
    "ConfigCacheID" : generatorDoc,                   Generator Config id
    "SplittingAlgorithm"  : "EventBased",             Splitting Algorithm
    "SplittingArguments" : {"events_per_job" : 250},  Size of jobs in terms of splitting algorithm
    "RequestNumEvents" : 10000,                       Total number of events to generate
    "Seeding" : "AutomaticSeeding",                   Random seeding method
    "PrimaryDataset" : "RelValTTBar",                 Primary Dataset to be created
},

Example initial processing task

"Task1" :{
     "TaskName" : "DigiHLT",                                     Task Name
     "ConfigCacheID" : processorDocs['DigiHLT'],                 Processing Config id
     "InputDataset" : "/MinimumBias/Commissioning10-v4/GEN-SIM", Input Dataset to be processed
     "SplittingAlgorithm"  : "FileBased",                        Splitting Algorithm
     "SplittingArguments" : {"files_per_job" : 1},               Size of jobs in terms of splitting algorithm
 },
 
 All subsequent Task entries will process the output of one of the preceeding steps:
 Example:
 
 "Task2" : {
     "TaskName" : "Reco",                               Task Name
     "InputTask" : "DigiHLT",                           Input Task Name (Task Name field of a previous Task entry)
     "InputFromOutputModule" : "writeRAWDIGI",          OutputModule name in the input task that will provide files to process
     "ConfigCacheID" : "17612875182763812763812",       Processing Config id
     "SplittingAlgorithm" : "FileBased",                Splitting Algorithm
     "SplittingArguments" : {"files_per_job" : 1 },     Size of jobs in terms of splitting algorithm
 },
 "Task3" : {
     "TaskName" : "ALCAReco",                           Task Name
     "InputTask" : "Reco",                              Input Task Name (Task Name field of a previous Task entry)
     "InputFromOutputModule" : "writeALCA",             OutputModule name in the input task that will provide files to process
     "ConfigCacheID" : "12871372323918187281",          Processing Config id    
     "SplittingAlgorithm" : "FileBased",                Splitting Algorithm
     "SplittingArguments" : {"files_per_job" : 1 },     Size of jobs in terms of splitting algorithm
 
 },


"""

import sys
import os
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


#
# simple utils for data mining the request dictionary
# 
getTaskN = lambda args, tasknum: args.get("Task%s" % tasknum, None)
isGenerator = lambda args: not args["Task1"].has_key("InputDataset")
parentTaskName = lambda args: args.get("InputTask", None)
parentTaskModule = lambda args: args.get("InputFromOutputModule", None)

class TaskChainWorkloadFactory(StdBase):
    def __init__(self):
        StdBase.__init__(self)
        self.taskMapping = {}
        self.mergeMapping = {}
        self.arguments = {}
        self.multicore = False
        self.multicoreNCores = 1

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
        self.frameworkVersion = arguments["CMSSWVersion"]
        self.globalTag = arguments.get("GlobalTag", None)
        # Optional arguments that default to something reasonable.
        self.dbsUrl = arguments.get("DbsUrl", "http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet")
        self.emulation = arguments.get("Emulation", False)

        #Check for pileup configuration
        self.pileupConfig = arguments.get("PileupConfig", None)
        
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
            if self.mergeMapping.has_key(parent):
                parentTask = self.mergeMapping[parent][parentTaskModule(taskConf)]
                
            task = self.makeTask(taskConf, parentTask)
            if i == 1:
                #  //
                # // First task will either be generator or processing
                #//
                if isGenerator(arguments):
                    # generate mc events
                    self.workload.setDashboardActivity("production")
                    self.workload.setWorkQueueSplitPolicy("MonteCarlo", taskConf['SplittingAlgorithm'], 
                                                          taskConf['SplittingArguments'])
                    self.workload.setEndPolicy("SingleShot")
                    self.setupGeneratorTask(task, taskConf)
                else:
                    # process an existing dataset
                    self.workload.setDashboardActivity("reprocessing")
                    self.workload.setWorkQueueSplitPolicy("Block", taskConf['SplittingAlgorithm'],
                                                     taskConf['SplittingArguments'])
                    self.setupTask(task, taskConf)
                self.reportWorkflowToDashboard(self.workload.getDashboardActivity())
            else:
                #  //
                # // all subsequent tasks have to be processing tasks
                #//
                self.setupTask(task, taskConf)
            # keep a lookup table of tasks for setting parentage later
            self.taskMapping[task.name()] = task
            
        return self.workload  

            
    def makeTask(self, taskConf, parentTask = None ):
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
        
    def setupGeneratorTask(self, task, taskConf):
        """
        _setupGeneratorTask_
        
        Set up an initial generation task
        """
        cmsswStepType = "CMSSW"
        configCacheID = taskConf['ConfigCacheID']
        splitAlgorithm = taskConf['SplittingAlgorithm']
        splitArguments = taskConf['SplittingArguments']
        
        globalGlobalTag = self.globalTag
        if taskConf.has_key('GlobalTag'):
            self.globalTag = taskConf['GlobalTag']

        self.inputPrimaryDataset = taskConf['PrimaryDataset']
        outputMods = self.setupProcessingTask(task, "Production", None,
                                            scenarioName = None, scenarioFunc = None, scenarioArgs = {},
                                            couchURL = self.couchURL, couchDBName = self.couchDBName,
                                            configDoc = configCacheID, splitAlgo = splitAlgorithm,
                                            splitArgs = splitArguments, stepType = cmsswStepType, 
                                            seeding = taskConf['Seeding'], totalEvents = taskConf['RequestNumEvents']
                                            )

        if self.pileupConfig:
            self.setupPileup(task, self.pileupConfig)

        self.addLogCollectTask(task, 'LogCollectFor%s' % task.name())
        procMergeTasks = {}
        for outputModuleName in outputMods.keys():
            outputModuleInfo = outputMods[outputModuleName]
            mergeTask = self.addMergeTask(task, taskConf['SplittingAlgorithm'],
                                          outputModuleName)
            procMergeTasks[str(outputModuleName)] = mergeTask
        self.mergeMapping[task.name()] = procMergeTasks
        self.globalTag = globalGlobalTag
        
    def setupTask(self, task, taskConf):
        """
        _setupTask_
        
        Build the task using the setupProcessingTask from StdBase and set the parents appropriately to handle
        a processing task
        """
       
        cmsswStepType = "CMSSW"
        configCacheID = taskConf.get('ConfigCacheID', None)
        splitAlgorithm = taskConf['SplittingAlgorithm']
        splitArguments = taskConf['SplittingArguments']
        globalGlobalTag = self.globalTag
        if taskConf.has_key('GlobalTag'):
            self.globalTag = taskConf['GlobalTag']

        #  //
        # //  in case the initial task is a processing task, we have an input dataset, otherwise
        #//   we look up the parent task and step
        inputDataset = taskConf.get("InputDataset", None)
        if inputDataset != None:
            self.inputDataset = inputDataset
            (self.inputPrimaryDataset, self.inputProcessedDataset,
             self.inputDataTier) = self.inputDataset[1:].split("/")
            inpStep = None
            inpMod = None
        else:
            self.inputDataset = None
            inputTask = taskConf.get("InputTask", None)
            #  ToDo: if None, need to throw here
            inputTaskRef = self.taskMapping[inputTask]
            # ToDo: key check in self.taskMapping for inputTask & throw if missing
            mergeTaskForMod = self.mergeMapping[inputTask][taskConf['InputFromOutputModule']]
            inpStep = mergeTaskForMod.getStep("cmsRun1")
            inpMod = "Merged"            

        scenario = None
        scenarioFunc = None
        scenarioArgs = {}
        couchUrl = self.couchURL
        couchDB = self.couchDBName
        if taskConf.get("ProcScenario", None) != None:
            self.procScenario = taskConf['ProcScenario']
            scenarioFunc = taskConf['ScenarioMethod']
            scenarioArgs = taskConf['ScenarioArguments']
            
        outputMods = self.setupProcessingTask(task, "Processing", inputDataset, inputStep = inpStep, inputModule=inpMod,
                                            scenarioName = self.procScenario, scenarioFunc = scenarioFunc, scenarioArgs = scenarioArgs,
                                            couchURL = couchUrl, couchDBName = couchDB,
                                            configDoc = configCacheID, splitAlgo = splitAlgorithm,
                                            splitArgs = splitArguments, stepType = cmsswStepType)
                                        
        
        self.addLogCollectTask(task, 'LogCollectFor%s' % task.name())
        procMergeTasks = {}
        for outputModuleName in outputMods.keys():
            outputModuleInfo = outputMods[outputModuleName]
            mergeTask = self.addMergeTask(task, taskConf['SplittingAlgorithm'],
                                          outputModuleName)
            procMergeTasks[str(outputModuleName)] = mergeTask
        self.mergeMapping[task.name()] = procMergeTasks
        self.globalTag = globalGlobalTag
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
                outMod = self.validateConfigCacheExists(configID = task['ConfigCacheID'],
                                                        couchURL = schema["CouchURL"],
                                                        couchDBName = schema["CouchDBName"],
                                                        getOutputModules = True)
        return


def taskChainWorkload(workloadName, arguments):
    """
    _taskChainWorkload_
    
    Helper to generate a TaskChain workload given the name & arguments provided

    """
    f = TaskChainWorkloadFactory()
    return f(workloadName, arguments)


