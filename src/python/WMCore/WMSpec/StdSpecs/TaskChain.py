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
    "ProcessingVersion": "v1",                        Processing Version (used for all tasks in chain)
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
    "RequestSizeEvents" : 10000,                      Total number of events to generate
    "Seeding" : "Automatic",                          Random seeding method
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
from WMCore.WMSpec.StdSpecs.StdBase import StdBase


#
# simple utils for data mining the request dictionary
# 
getTaskN = lambda args, tasknum: args.get("Task%s" % tasknum, None)
isGenerator = lambda args: not args["Task1"].has_key("InputDataset")
parentTaskName = lambda args: args.get("InputTask", None)

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
        self.globalTag = arguments["GlobalTag"]
        # Optional arguments that default to something reasonable.
        self.dbsUrl = arguments.get("DbsUrl", "http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet")
        self.blockBlacklist = arguments.get("BlockBlacklist", [])
        self.blockWhitelist = arguments.get("BlockWhitelist", [])
        self.runBlacklist = arguments.get("RunBlacklist", [])
        self.runWhitelist = arguments.get("RunWhitelist", [])
        self.emulation = arguments.get("Emulation", False)

        
        numTasks = arguments['TaskChain']
        for i in range(1, numTasks+1):
            #consistency check that there are numTasks defined in the request:
            if not arguments.has_key("Task%s" % i):
                msg = "Specified number of tasks: %s does not match defined task dictionary for Task%s" % (i, i)
                raise RuntimeError, msg
                
            taskConf = getTaskN(arguments, i)
            parent = parentTaskName(taskConf)
            
            task = self.makeTask(taskConf, self.taskMapping.get(parent, None) )
            if i == 1:
                #  //
                # // First task will either be generator or processing
                #//
                if isGenerator(arguments):
                    # generate mc events
                    self.workload.setDashboardActivity("production")
                    self.workload.setWorkQueueSplitPolicy("MonteCarlo", taskConf['SplittingAlgorithm'], 
                                                          taskConf['SplittingArguments'])
                    self.workload.setEndPolicy("SingleShot", SuccessThreshold = 0.9)
                    self.setupGeneratorTask(task, taskConf)
                else:
                    # process an existing dataset
                    self.workload.setDashboardActivity("reprocessing")
                    self.workload.setWorkQueueSplitPolicy("Block", taskConf['SplittingAlgorithm'],
                                                     taskConf['SplittingArguments'])
                    self.setupTask(task, taskConf)
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


        self.inputPrimaryDataset = taskConf['PrimaryDataset']
        outputMods = self.setupProcessingTask(task, "Production", None,
                                            scenarioName = None, scenarioFunc = None, scenarioArgs = {},
                                            couchURL = self.couchURL, couchDBName = self.couchDBName,
                                            configDoc = configCacheID, splitAlgo = splitAlgorithm,
                                            splitArgs = splitArguments, stepType = cmsswStepType, 
                                            seeding = taskConf['Seeding'], totalEvents = taskConf['RequestSizeEvents']
                                            )
        self.addLogCollectTask(task, 'LogCollectFor%s' % task.name())
        procMergeTasks = {}
        for outputModuleName in outputMods.keys():
            outputModuleInfo = outputMods[outputModuleName]
            mergeTask = self.addMergeTask(task, taskConf['SplittingAlgorithm'],
                                          outputModuleName,
                                          outputModuleInfo["dataTier"],
                                          outputModuleInfo["filterName"],
                                          outputModuleInfo["processedDataset"])
            procMergeTasks[str(outputModuleName)] = mergeTask
        self.mergeMapping[task.name()] = procMergeTasks
        
        
    def setupTask(self, task, taskConf):
        """
        _setupTask_
        
        Build the task using the setupProcessingTask from StdBase and set the parents appropriately to handle
        a processing task
        """
       
        cmsswStepType = "CMSSW"
        configCacheID = taskConf['ConfigCacheID']
        splitAlgorithm = taskConf['SplittingAlgorithm']
        splitArguments = taskConf['SplittingArguments']


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
            inpMod = taskConf['InputFromOutputModule']
            mergeTaskForMod = self.mergeMapping[inputTask][inpMod]
            inpStep = mergeTaskForMod.getStep("cmsRun1")
            
        outputMods = self.setupProcessingTask(task, "Processing", inputDataset, inputStep = inpStep, inputModule=inpMod,
                                            scenarioName = None, scenarioFunc = None, scenarioArgs = {},
                                            couchURL = self.couchURL, couchDBName = self.couchDBName,
                                            configDoc = configCacheID, splitAlgo = splitAlgorithm,
                                            splitArgs = splitArguments, stepType = cmsswStepType)
                                        
        
        self.addLogCollectTask(task, 'LogCollectFor%s' % task.name())
        procMergeTasks = {}
        for outputModuleName in outputMods.keys():
            outputModuleInfo = outputMods[outputModuleName]
            mergeTask = self.addMergeTask(task, taskConf['SplittingAlgorithm'],
                                          outputModuleName,
                                          outputModuleInfo["dataTier"],
                                          outputModuleInfo["filterName"],
                                          outputModuleInfo["processedDataset"])
            procMergeTasks[str(outputModuleName)] = mergeTask
        self.mergeMapping[task.name()] = procMergeTasks

