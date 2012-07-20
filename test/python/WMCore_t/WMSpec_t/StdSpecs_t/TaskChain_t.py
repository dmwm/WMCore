#!/usr/bin/env python
# encoding: utf-8
"""
TaskChain_t.py

Created by Dave Evans on 2011-06-21.
Copyright (c) 2011 Fermilab. All rights reserved.
"""

import sys
import os
import unittest

from WMCore.WMSpec.StdSpecs.TaskChain import TaskChainWorkloadFactory
from WMQuality.TestInitCouchApp import TestInitCouchApp
from WMCore.Database.CMSCouch import CouchServer, Document
from WMCore.WorkQueue.WMBSHelper import WMBSHelper


from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Workflow import Workflow


def makeGeneratorConfig(couchDatabase):
    """
    _makeGeneratorConfig_

    Create a bogus config cache document for the montecarlo generation and
    inject it into couch.  Return the ID of the document.

    """
    newConfig = Document()
    newConfig["info"] = None
    newConfig["config"] = None
    newConfig["md5hash"] = "eb1c38cf50e14cf9fc31278a5c8e580f"
    newConfig["pset_hash"] = "7c856ad35f9f544839d8525ca10259a7"
    newConfig["owner"] = {"group": "cmsdataops", "user": "sfoulkes"}
    newConfig["pset_tweak_details"] ={"process": {"outputModules_": ["writeGENSIM"],
                                                  "writeGENSIM": {"dataset": {"filterName": "GenSimFilter",
                                                                          "dataTier": "GEN-SIM"}}}}
    result = couchDatabase.commitOne(newConfig)
    return result[0]["id"]
    

    
def makeProcessingConfigs(couchDatabase):
    """
    _makeProcessingConfigs_
    
    Make a bunch of processing configs in couch for a processing chain consisting of
    
    DigiHLT - Reco - ALCAReco - Skims
    
    returns a map of config names to IDs
    
    """
    rawConfig = Document()
    rawConfig["info"] = None
    rawConfig["config"] = None
    rawConfig["md5hash"] = "eb1c38cf50e14cf9fc31278a5c8e234f"
    rawConfig["pset_hash"] = "7c856ad35f9f544839d8525ca10876a7"
    rawConfig["owner"] = {"group": "cmsdataops", "user": "sfoulkes"}
    rawConfig["pset_tweak_details"] ={"process": {"outputModules_": ["writeRAWDIGI"],
                                                  "writeRAWDIGI": {"dataset": {"filterName": "RawDigiFilter",
                                                                          "dataTier": "RAW-DIGI"}}}}
    recoConfig = Document()
    recoConfig["info"] = None
    recoConfig["config"] = None
    recoConfig["md5hash"] = "eb1c38cf50e14cf9fc31278a5c8e736f"
    recoConfig["pset_hash"] = "7c856ad35f9f544839d8525ca11765a7"
    recoConfig["owner"] = {"group": "cmsdataops", "user": "sfoulkes"}
    recoConfig["pset_tweak_details"] = {
       "process": {"outputModules_": ["writeRECO", "writeAOD", "writeALCA"],
                   "writeRECO": {"dataset": {"dataTier": "RECO", "filterName" : "reco"}},
                   "writeAOD":  {"dataset": {"dataTier": "AOD", "filterName" : "AOD"}},
                   "writeALCA":  {"dataset": {"dataTier": "ALCARECO", "filterName" : "alca"}},                                                  
                }
        }                        
    alcaConfig = Document()
    alcaConfig["info"] = None
    alcaConfig["config"] = None
    alcaConfig["md5hash"] = "eb1c38cf50e14cf9fc31278a5c8e826a"
    alcaConfig["pset_hash"] = "7c856ad35f9f544839d8525ca53628a7"
    alcaConfig["owner"] = {"group": "cmsdataops", "user": "sfoulkes"}
    alcaConfig["pset_tweak_details"] = {
       "process": {"outputModules_": ["writeALCA1", "writeALCA2", "writeALCA3", "writeALCA4"],
                   "writeALCA1": {"dataset": {"dataTier": "ALCARECO", "filterName": "alca1"}},
                   "writeALCA2":  {"dataset": {"dataTier": "ALCARECO", "filterName": "alca2"}},
                   "writeALCA3":  {"dataset": {"dataTier": "ALCARECO", "filterName": "alca3"}},
                   "writeALCA4":  {"dataset": {"dataTier": "ALCARECO", "filterName": "alca4"}},
                }
        }                        
        
    skimsConfig = Document()
    skimsConfig["info"] = None
    skimsConfig["config"] = None
    skimsConfig["md5hash"] = "eb1c38cf50e14cf9fc31278a5cab278a"
    skimsConfig["pset_hash"] = "7c856ad35f9f544839d8524ca53728a6"
    skimsConfig["owner"] = {"group": "cmsdataops", "user": "sfoulkes"}
    skimsConfig["pset_tweak_details"] = {
       "process": {"outputModules_": ["writeSkim1", "writeSkim2", "writeSkim3", "writeSkim4", "writeSkim5"],
                   "writeSkim1": {"dataset": {"dataTier":  "RECO-AOD", "filterName": "skim1"}},
                   "writeSkim2":  {"dataset": {"dataTier": "RECO-AOD", "filterName": "skim2"}},
                   "writeSkim3":  {"dataset": {"dataTier": "RECO-AOD", "filterName": "skim3"}},
                   "writeSkim4":  {"dataset": {"dataTier": "RECO-AOD", "filterName": "skim4"}},
                   "writeSkim5":  {"dataset": {"dataTier": "RECO-AOD", "filterName": "skim5"}},                   
                }
        }
    couchDatabase.queue(rawConfig)          
    couchDatabase.queue(recoConfig)   
    couchDatabase.queue(alcaConfig)   
    couchDatabase.queue(skimsConfig)   
    result = couchDatabase.commit()
    
    docMap = {
        "DigiHLT" : result[0][u'id'],
        "Reco"    : result[1][u'id'],
        "ALCAReco": result[2][u'id'],
        "Skims"   : result[3][u'id'],
    }                               
    return docMap


def makePromptSkimConfigs(couchDatabase):
    """
    Fake a prompt skim config in ConfigCache for Tier0 test
    """
    skimsConfig = Document()
    skimsConfig["info"] = None
    skimsConfig["config"] = None
    skimsConfig["md5hash"] = "eb1c38cf50e14cf9fc31278a5cab2755"
    skimsConfig["pset_hash"] = "7c856ad35f9f544839d8524ca5372888"
    skimsConfig["owner"] = {"group": "cmsdataops", "user": "gutsche"}
    skimsConfig["pset_tweak_details"] = {
       "process": {"outputModules_": ["writeSkim1", "writeSkim2", "writeSkim3", "writeSkim4", "writeSkim5"],
                   "writeSkim1": {"dataset": {"dataTier":  "RECO-AOD", "filterName": "skim1"}},
                   "writeSkim2":  {"dataset": {"dataTier": "RECO-AOD", "filterName": "skim2"}},
                   "writeSkim3":  {"dataset": {"dataTier": "RECO-AOD", "filterName": "skim3"}},
                   "writeSkim4":  {"dataset": {"dataTier": "RECO-AOD", "filterName": "skim4"}},
                   "writeSkim5":  {"dataset": {"dataTier": "RECO-AOD", "filterName": "skim5"}},                   
                }
        }
    couchDatabase.queue(skimsConfig)   
    result = couchDatabase.commit()
    docMap = {
        "Skims" :result[0][u'id'] 
    }
    return docMap

def outputModuleList(task):
    """
    _outputModuleList_
    
    util to return list of output module names
    
    """
    result = []
    for om in task.getOutputModulesForTask():
        mods = om.listSections_()
        result.extend([str(x) for x in mods])
    return result

class TaskChainTests(unittest.TestCase):
    def setUp(self):
        """
        _setUp_

        Initialize the database and couch.
        
        """
        self.testInit = TestInitCouchApp(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setupCouch("taskchain_t", "ConfigCache")        
        self.testInit.setSchema(customModules = ["WMCore.WMBS"],
                                useDefault = False)

        couchServer = CouchServer(os.environ["COUCHURL"])
        self.configDatabase = couchServer.connectDatabase("taskchain_t")  
        self.workload = None      
        return


    def tearDown(self):
        """
        _tearDown_

        Clear out the database.
        
        """
        del self.workload
        self.testInit.tearDownCouch()
        self.testInit.clearDatabase()
        return


    def testA(self):
        """
        test creating workload with generator config
        """
        generatorDoc = makeGeneratorConfig(self.configDatabase)
        processorDocs = makeProcessingConfigs(self.configDatabase)
        
        
        arguments = {
            "AcquisitionEra": "ReleaseValidation",
            "Requestor": "sfoulkes@fnal.gov",
            "CMSSWVersion": "CMSSW_3_5_8",
            "ScramArch": "slc5_ia32_gcc434",
            "ProcessingVersion": 1,
            "GlobalTag": "GR10_P_v4::All",
            "CouchURL": self.testInit.couchUrl,
            "CouchDBName": self.testInit.couchDbName,
            "SiteWhitelist" : ["T1_CH_CERN", "T1_US_FNAL"],
            "DashboardHost": "127.0.0.1",
            "DashboardPort": 8884,
            "TaskChain" : 5,
            "Task1" :{
                "TaskName" : "GenSim",
                "ConfigCacheID" : generatorDoc, 
                "SplittingAlgorithm"  : "EventBased",
                "SplittingArguments" : {"events_per_job" : 250},
                "RequestNumEvents" : 10000,
                "Seeding" : "Automatic",
                "PrimaryDataset" : "RelValTTBar",
            },
            "Task2" : {
                "TaskName" : "DigiHLT",
                "InputTask" : "GenSim",
                "InputFromOutputModule" : "writeGENSIM",
                "ConfigCacheID" : processorDocs['DigiHLT'],
                "SplittingAlgorithm" : "FileBased",
                "SplittingArguments" : {"files_per_job" : 1 },
            },
            "Task3" : {
                "TaskName" : "Reco",
                "InputTask" : "DigiHLT",
                "InputFromOutputModule" : "writeRAWDIGI",
                "ConfigCacheID" : processorDocs['Reco'],
                "SplittingAlgorithm" : "FileBased",
                "SplittingArguments" : {"files_per_job" : 1 },
            },
            "Task4" : {
                "TaskName" : "ALCAReco",
                "InputTask" : "Reco",
                "InputFromOutputModule" : "writeALCA",
                "ConfigCacheID" : processorDocs['ALCAReco'],
                "SplittingAlgorithm" : "FileBased",
                "SplittingArguments" : {"files_per_job" : 1 },
            
            },
            "Task5" : {
                "TaskName" : "Skims",
                "InputTask" : "Reco",
                "InputFromOutputModule" : "writeRECO",
                "ConfigCacheID" : processorDocs['Skims'],
                "SplittingAlgorithm" : "FileBased",
                "SplittingArguments" : {"files_per_job" : 10 },            
            }
            
        }

        factory = TaskChainWorkloadFactory()
        
        try:
            self.workload = factory("PullingTheChain", arguments)
        except Exception, ex:
            msg = "Error invoking TaskChainWorkloadFactory:\n%s" % str(ex)
            self.fail(msg)
        
        
        self.workload.setSpecUrl("somespec")
        self.workload.setOwnerDetails("evansde@fnal.gov", "DMWM")


        testWMBSHelper = WMBSHelper(self.workload, "GenSim", "SomeBlock")
        testWMBSHelper.createTopLevelFileset()
        testWMBSHelper.createSubscription(testWMBSHelper.topLevelTask, testWMBSHelper.topLevelFileset)

        firstTask = self.workload.getTaskByPath("/PullingTheChain/GenSim")

        self._checkTask(firstTask, arguments['Task1'])
        self._checkTask(self.workload.getTaskByPath("/PullingTheChain/GenSim/GenSimMergewriteGENSIM/DigiHLT"), arguments['Task2'])
        self._checkTask(self.workload.getTaskByPath("/PullingTheChain/GenSim/GenSimMergewriteGENSIM/DigiHLT/DigiHLTMergewriteRAWDIGI/Reco"),
                        arguments['Task3'])
        self._checkTask(self.workload.getTaskByPath("/PullingTheChain/GenSim/GenSimMergewriteGENSIM/DigiHLT/DigiHLTMergewriteRAWDIGI/Reco/RecoMergewriteALCA/ALCAReco"),
                        arguments['Task4'])
        self._checkTask(self.workload.getTaskByPath("/PullingTheChain/GenSim/GenSimMergewriteGENSIM/DigiHLT/DigiHLTMergewriteRAWDIGI/Reco/RecoMergewriteRECO/Skims"),
                        arguments['Task5'])        
        
        
        

    def _checkTask(self, task, taskConf):
        """
        _checkTask_
        
        Give the provided task instance the once over, make sure parentage is set correctly etc
        
        """
        if taskConf.has_key("InputTask"):
            inpTask = taskConf['InputTask']
            inpMod = taskConf['InputFromOutputModule']
            inpTaskPath = task.getPathName()
            inpTaskPath = inpTaskPath.replace(task.name(), "")
            inpTaskPath += "cmsRun1"
            self.assertEqual(task.data.input.inputStep, inpTaskPath)

        
        workflow = Workflow(name = self.workload.name(),
                            task = task.getPathName())
        workflow.load()
        mods = outputModuleList(task)

        for mod in mods:
            filesets = workflow.outputMap[mod][0]
            merged = filesets['merged_output_fileset']
            unmerged = filesets['output_fileset']
            
            merged.loadData()
            unmerged.loadData()
            mergedset = task.getPathName() + "/" + task.name() + "Merge" + mod + "/merged-Merged"
            if mod == "logArchive":
                mergedset = task.getPathName() + "/unmerged-" + mod
            unmergedset = task.getPathName() + "/unmerged-" + mod
            
            self.failUnless(mergedset == merged.name)
            self.failUnless(unmergedset == unmerged.name)

            if mod == "logArchive": continue
            
            mergeTask = task.getPathName() + "/" + task.name() + "Merge" + mod
            
            mergeWorkflow = Workflow(name = self.workload.name(),
                                     task = mergeTask)
            mergeWorkflow.load()
            if not mergeWorkflow.outputMap.has_key("Merged"):
                msg = "Merge workflow does not contain a Merged output key"
                self.fail(msg)
            mrgFileset = mergeWorkflow.outputMap['Merged'][0]
            mergedFS = mrgFileset['merged_output_fileset']
            unmergedFS = mrgFileset['output_fileset']
            mergedFS.loadData()
            unmergedFS.loadData()
            self.assertEqual(mergedFS.name, mergedset)
            self.assertEqual(unmergedFS.name, mergedset)
            
            mrgLogArch = mergeWorkflow.outputMap['logArchive'][0]['merged_output_fileset']
            umrgLogArch = mergeWorkflow.outputMap['logArchive'][0]['output_fileset']
            mrgLogArch.loadData()
            umrgLogArch.loadData()
            
            archName = task.getPathName() + "/" + task.name() + "Merge" + mod + "/merged-logArchive"
            
            self.assertEqual(mrgLogArch.name, archName)
            self.assertEqual(umrgLogArch.name,archName)
            
            
        
        #
        # test subscriptions made by this task if it is the first task
        #
        if taskConf.has_key("InputDataset") or taskConf.has_key("PrimaryDataset"):
            taskFileset = Fileset(name = "%s-%s-SomeBlock" % (self.workload.name(), task.name() ))
            taskFileset.loadData()

            taskSubscription = Subscription(fileset = taskFileset, workflow = workflow)
            taskSubscription.loadData()
        
            if taskConf.has_key("PrimaryDataset"):
                # generator type
                self.assertEqual(taskSubscription["type"], "Production",
                                 "Error: Wrong subscription type.")
                self.assertEqual(taskSubscription["split_algo"], taskConf["SplittingAlgorithm"],
                                 "Error: Wrong split algo.")
            if taskConf.has_key("InputDataset"):
                # processor type
                self.assertEqual(taskSubscription["type"], "Processing", "Wrong subscription type for processing first task")
                self.assertEqual(taskSubscription["split_algo"], taskConf['SplittingAlgorithm'], "Splitting algo mismatch")
                                 

    def testB(self):
        """
        _testB_
        
        test creating an all processor workload with an input dataset
        
        """
        processorDocs = makeProcessingConfigs(self.configDatabase)
        arguments = {
            "AcquisitionEra": "ReleaseValidation",
            "Requestor": "sfoulkes@fnal.gov",
            "CMSSWVersion": "CMSSW_3_5_8",
            "ScramArch": "slc5_ia32_gcc434",
            "ProcessingVersion": 1,
            "GlobalTag": "DefaultGlobalTag",
            "CouchURL": self.testInit.couchUrl,
            "CouchDBName": self.testInit.couchDbName,
            "SiteWhitelist" : ["T1_CH_CERN", "T1_US_FNAL"],
            "DashboardHost": "127.0.0.1",
            "DashboardPort": 8884,
            "TaskChain" : 4,
            "Task1" :{
                "TaskName" : "DigiHLT",
                "ConfigCacheID" : processorDocs['DigiHLT'],
                "InputDataset" : "/MinimumBias/Commissioning10-v4/GEN-SIM", 
                "SplittingAlgorithm"  : "FileBased",
                "SplittingArguments" : {"files_per_job" : 1}
            },
            "Task2" : {
                "TaskName" : "Reco",
                "InputTask" : "DigiHLT",
                "InputFromOutputModule" : "writeRAWDIGI",
                "ConfigCacheID" : processorDocs['Reco'],
                "SplittingAlgorithm" : "FileBased",
                "SplittingArguments" : {"files_per_job" : 1 },
                "GlobalTag" : "GlobalTagForReco",
            },
            "Task3" : {
                "TaskName" : "ALCAReco",
                "InputTask" : "Reco",
                "InputFromOutputModule" : "writeALCA",
                "ConfigCacheID" : processorDocs['ALCAReco'],
                "SplittingAlgorithm" : "FileBased",
                "SplittingArguments" : {"files_per_job" : 1 },
                "GlobalTag" : "GlobalTagForALCAReco",   
            },
            "Task4" : {
                "TaskName" : "Skims",
                "InputTask" : "Reco",
                "InputFromOutputModule" : "writeRECO",
                "ConfigCacheID" : processorDocs['Skims'],
                "SplittingAlgorithm" : "FileBased",
                "SplittingArguments" : {"files_per_job" : 10 }, 
                "GlobalTag" : "GlobalTagForSkims"           
            }
        }
    
        factory = TaskChainWorkloadFactory()        
        try:
            self.workload = factory("YankingTheChain", arguments)
        except Exception, ex:
            msg = "Error invoking TaskChainWorkloadFactory:\n%s" % str(ex)
            self.fail(msg)


        self.workload.setSpecUrl("somespec")
        self.workload.setOwnerDetails("evansde@fnal.gov", "DMWM")


        testWMBSHelper = WMBSHelper(self.workload, "DigiHLT", "SomeBlock")
        testWMBSHelper.createTopLevelFileset()
        testWMBSHelper.createSubscription(testWMBSHelper.topLevelTask, testWMBSHelper.topLevelFileset)


        self._checkTask(self.workload.getTaskByPath("/YankingTheChain/DigiHLT"), arguments['Task1'])
        self._checkTask(self.workload.getTaskByPath("/YankingTheChain/DigiHLT/DigiHLTMergewriteRAWDIGI/Reco"), arguments['Task2'])
        self._checkTask(self.workload.getTaskByPath("/YankingTheChain/DigiHLT/DigiHLTMergewriteRAWDIGI/Reco/RecoMergewriteALCA/ALCAReco"),
                        arguments['Task3'])
        self._checkTask(self.workload.getTaskByPath("/YankingTheChain/DigiHLT/DigiHLTMergewriteRAWDIGI/Reco/RecoMergewriteRECO/Skims"),
                        arguments['Task4'])
 
        digi = self.workload.getTaskByPath("/YankingTheChain/DigiHLT")
        digiStep = digi.getStepHelper("cmsRun1")
        self.assertEqual(digiStep.getGlobalTag(), arguments['GlobalTag'])
 
        reco = self.workload.getTaskByPath("/YankingTheChain/DigiHLT/DigiHLTMergewriteRAWDIGI/Reco")
        recoStep = reco.getStepHelper("cmsRun1")
        self.assertEqual(recoStep.getGlobalTag(), arguments['Task2']['GlobalTag'])
 
        alca = self.workload.getTaskByPath("/YankingTheChain/DigiHLT/DigiHLTMergewriteRAWDIGI/Reco/RecoMergewriteALCA/ALCAReco")
        alcaStep = alca.getStepHelper("cmsRun1")
        self.assertEqual(alcaStep.getGlobalTag(), arguments['Task3']['GlobalTag'])

        skim = self.workload.getTaskByPath("/YankingTheChain/DigiHLT/DigiHLTMergewriteRAWDIGI/Reco/RecoMergewriteRECO/Skims")
        skimStep = skim.getStepHelper("cmsRun1")
        self.assertEqual(skimStep.getGlobalTag(), arguments['Task4']['GlobalTag'])
        
 
        
 
    def testC(self):
        """
        _testC_

        test creating an all processor workload with an input dataset and uses scenarios instead
        of config cache for a couple of the configs

        """
        processorDocs = makeProcessingConfigs(self.configDatabase)
        arguments = {
            "AcquisitionEra": "ReleaseValidation",
            "Requestor": "sfoulkes@fnal.gov",
            "CMSSWVersion": "CMSSW_3_5_8",
            "ScramArch": "slc5_ia32_gcc434",
            "ProcessingVersion": 1,
            "GlobalTag": "GR10_P_v4::All",
            "CouchURL": self.testInit.couchUrl,
            "CouchDBName": self.testInit.couchDbName,
            "SiteWhitelist" : ["T1_CH_CERN", "T1_US_FNAL"],
            "DashboardHost": "127.0.0.1",
            "DashboardPort": 8884,
            "TaskChain" : 4,
            "Task1" :{
                "TaskName" : "DigiHLT",
                "ConfigCacheID" : processorDocs['DigiHLT'],
                "InputDataset" : "/MinimumBias/Commissioning10-v4/GEN-SIM", 
                "SplittingAlgorithm"  : "FileBased",
                "SplittingArguments" : {"files_per_job" : 1},
            },
            "Task2" : {
                "TaskName" : "Reco",
                "InputTask" : "DigiHLT",
                "InputFromOutputModule" : "writeRAWDIGI",
                "ProcScenario" : "pp",
                "ScenarioMethod" : "promptReco",
                "ScenarioArguments" : { 'outputs' : [ { 'dataTier' : "RECO",
                                                        'moduleLabel' : "RECOoutput" },
                                                      { 'dataTier' : "AOD",
                                                        'moduleLabel' : "AODoutput" },
                                                      { 'dataTier' : "ALCARECO",
                                                        'moduleLabel' : "ALCARECOoutput" } ] },
                "SplittingAlgorithm" : "FileBased",
                "SplittingArguments" : {"files_per_job" : 1 },
            },
            "Task3" : {
                "TaskName" : "ALCAReco",
                "InputTask" : "Reco",
                "InputFromOutputModule" : "ALCARECOoutput",
                "ProcScenario" : "pp",
                "ScenarioMethod" : "alcaReco",
                "ScenarioArguments" : { 'outputs' : [ { 'dataTier' : "ALCARECO",
                                                        'moduleLabel' : "ALCARECOoutput" } ] },
                "SplittingAlgorithm" : "FileBased",
                "SplittingArguments" : {"files_per_job" : 1 },

            },
            "Task4" : {
                "TaskName" : "Skims",
                "InputTask" : "Reco",
                "InputFromOutputModule" : "RECOoutput",
                "ConfigCacheID" : processorDocs['Skims'],
                "SplittingAlgorithm" : "FileBased",
                "SplittingArguments" : {"files_per_job" : 10 },            
            }
        }

        factory = TaskChainWorkloadFactory()
        try:
            self.workload = factory("YankingTheChain", arguments)
        except Exception, ex:
            msg = "Error invoking TaskChainWorkloadFactory:\n%s" % str(ex)
            self.fail(msg)


        self.workload.setSpecUrl("somespec")
        self.workload.setOwnerDetails("evansde@fnal.gov", "DMWM")


        testWMBSHelper = WMBSHelper(self.workload, "DigiHLT", "SomeBlock")
        testWMBSHelper.createTopLevelFileset()
        testWMBSHelper.createSubscription(testWMBSHelper.topLevelTask, testWMBSHelper.topLevelFileset)


        self._checkTask(self.workload.getTaskByPath("/YankingTheChain/DigiHLT"), arguments['Task1'])
        self._checkTask(self.workload.getTaskByPath("/YankingTheChain/DigiHLT/DigiHLTMergewriteRAWDIGI/Reco"), arguments['Task2'])
        self._checkTask(self.workload.getTaskByPath("/YankingTheChain/DigiHLT/DigiHLTMergewriteRAWDIGI/Reco/RecoMergeALCARECOoutput/ALCAReco"),
                        arguments['Task3'])
        self._checkTask(self.workload.getTaskByPath("/YankingTheChain/DigiHLT/DigiHLTMergewriteRAWDIGI/Reco/RecoMergeRECOoutput/Skims"),
                        arguments['Task4'])
        
        
        
        reco = self.workload.getTaskByPath("/YankingTheChain/DigiHLT/DigiHLTMergewriteRAWDIGI/Reco")
        recoStep = reco.getStepHelper("cmsRun1")
        recoAppConf = recoStep.data.application.configuration
        self.assertEqual(recoAppConf.scenario, arguments['Task2']['ProcScenario'])
        self.assertEqual(recoAppConf.function, arguments['Task2']['ScenarioMethod'])
        
        alca = self.workload.getTaskByPath("/YankingTheChain/DigiHLT/DigiHLTMergewriteRAWDIGI/Reco/RecoMergeALCARECOoutput/ALCAReco")
        alcaStep = alca.getStepHelper("cmsRun1")
        alcaAppConf = alcaStep.data.application.configuration
        self.assertEqual(alcaAppConf.scenario, arguments['Task3']['ProcScenario'])
        self.assertEqual(alcaAppConf.function, arguments['Task3']['ScenarioMethod'])
        
    def testD(self):
        """
        Tier 0 style workload that incorporates some scenarios & some configs. 
        
        To use for real, replace the Skim ConfigCache URL and ID with a real
        skim config in config cache, Oli suggested this as a source:
        http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/Configuration/Skimming/test/tier1/skim_DoubleElectron.py?revision=1.3&pathrev=SkimsFor426
        
        """

        cfgs = makePromptSkimConfigs(self.configDatabase)
        arguments = {
            "AcquisitionEra": "ReleaseValidation",
            "Requestor": "gutsche@fnal.gov",
            "CMSSWVersion": "CMSSW_3_5_8",
            "ScramArch": "slc5_ia32_gcc434",
            "ProcessingVersion": 1,
            "GlobalTag": "NOTSET",
            "CouchURL": self.testInit.couchUrl,
            "CouchDBName": self.testInit.couchDbName,
            "SiteWhitelist" : ["T1_CH_CERN", "T1_US_FNAL"],
            "RunWhitelist" : [171050],
            "DashboardHost": "127.0.0.1",
            "DashboardPort": 8884,
            "TaskChain" : 3,
            "Task1" :{
                "TaskName" : "PromptReco",
                "InputDataset" : "/DoubleElectron/Run2011A-v1/RAW", 
                "SplittingAlgorithm"  : "LumiBased",
                "SplittingArguments" : {"lumis_per_job" : 1},
                "ProcScenario" : "pp",
                "ScenarioMethod" : "promptReco",
                "ScenarioArguments" : { 'outputs' : [ { 'dataTier' : "RECO",
                                                        'moduleLabel' : "RECOoutput" },
                                                      { 'dataTier' : "AOD",
                                                        'moduleLabel' : "AODoutput" },
                                                      { 'dataTier' : "DQM",
                                                        'moduleLabel' : "DQMoutput" },
                                                      { 'dataTier' : "ALCARECO",
                                                        'moduleLabel' : "ALCARECOoutput" } ] },
                "GlobalTag" : "NOTSET"    
            },
            "Task2" : {
                "TaskName" : "AlcaSkimming",
                "InputTask" : "PromptReco",
                "InputFromOutputModule" : "ALCARECOoutput",
                "ProcScenario" : "pp",
                "ScenarioMethod" : "alcaSkimming",
                "ScenarioArguments" : { 'outputs' : [ { 'dataTier' : "ALCARECO",
                                                        'moduleLabel' : "ALCARECOoutput" } ],
                                        'skims' : ["EcalCalElectron"] },
                "SplittingAlgorithm" : "FileBased",
                "SplittingArguments" : {"files_per_job" : 1 },
                "GlobalTag" : "NOTSET"    
            },
            "Task3" : {
                "TaskName" : "PromptSkim",
                "InputTask" : "PromptReco",
                "InputFromOutputModule" : "RECOoutput",
                "ConfigCacheID" : cfgs['Skims'],
                "GlobalTag" : "NOTSET",
                "SplittingAlgorithm" : "FileBased",
                "SplittingArguments" : {"files_per_job" : 10 }, 
            }
        }
        factory = TaskChainWorkloadFactory()        
        try:
            self.workload = factory("Tier0Test", arguments)
        except Exception, ex:
            msg = "Error invoking TaskChainWorkloadFactory:\n%s" % str(ex)
            self.fail(msg)


        self.workload.setSpecUrl("somespec")
        self.workload.setOwnerDetails("evansde@fnal.gov", "DMWM")


        testWMBSHelper = WMBSHelper(self.workload, "PromptReco", "SomeBlock")
        testWMBSHelper.createTopLevelFileset()
        testWMBSHelper.createSubscription(testWMBSHelper.topLevelTask, testWMBSHelper.topLevelFileset)
        
        
if __name__ == '__main__':
	unittest.main()
