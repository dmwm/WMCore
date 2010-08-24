#!/usr/bin/env python
# encoding: utf-8
# pylint: disable-msg=C0301,W0142
"""
MonteCarlo.py

Created by Dave Evans on 2010-08-17.
Copyright (c) 2010 Fermilab. All rights reserved.
"""


import os


from WMCore.WMSpec.WMWorkload import newWorkload
from WMCore.Cache.WMConfigCache import ConfigCache
from WMCore.Configuration import ConfigSection


class MoveToStdBase:
    """
    Placeholder for stuff that should move to StdBase once we finish meddling with them
    """
    def __init__(self):
        self.workloadName = None
        self.owner = None
        self.acquisitionEra = None
        self.maxMergeSize = None
        self.unmergedLFNBase = None
        self.globalTag = None
        self.minMergeSize = None
        self.primaryDataset = None
        self.mergedLFNBase = None
        self.maxMergeEvents = None
        self.couchConfigDoc = None
        self.dbsUrl = None
        self.processingVersion = None
        self.scramArch = None
        self.siteWhitelist = None
        self.siteBlacklist = None
        self.frameworkVersion = None
        self.couchUrl = None
        self.couchDBName = None
        self.emulation = None
        
        
        
        
    def newWorkload(self):
        """
        Create a new workload instance
        """
        workload = newWorkload(self.workloadName)
        workload.setOwner(self.owner)
        workload.data.properties.acquisitionEra = self.acquisitionEra
        return workload
        
    def addOutputModule(self, parentTask,  outputModuleName, primaryDataset,
                        dataTier, filterName):
        """
        _addOutputModule_

        Add an output module to the geven processing task.  This will also
        create merge and cleanup tasks for the output of the output module.
        A handle to the merge task is returned to make it easy to use the merged
        output of the output module as input to another task.
        """
        if filterName != None and filterName != "":
            processedDatasetName = "%s-%s-%s" % (self.acquisitionEra, filterName,
                                                 self.processingVersion)
        else:
            processedDatasetName = "%s-%s" % (self.acquisitionEra,
                                              self.processingVersion)

        unmergedLFN = "%s/%s/%s" % (self.unmergedLFNBase, dataTier,
                                    processedDatasetName)
        mergedLFN = "%s/%s/%s" % (self.mergedLFNBase, dataTier,
                                  processedDatasetName)
        cmsswStep = parentTask.getStep("cmsRun1")
        cmsswStepHelper = cmsswStep.getTypeHelper()
        cmsswStepHelper.addOutputModule(outputModuleName,
                                        primaryDataset = primaryDataset,
                                        processedDataset = processedDatasetName,
                                        dataTier = dataTier,
                                        lfnBase = unmergedLFN,
                                        mergedLFNBase = mergedLFN)
        return self.addMergeTask(parentTask,
                                 outputModuleName, primaryDataset, dataTier, processedDatasetName)

    def addMergeTask(self, parentTask, parentOutputModule, primaryDataset,
                    dataTier, processedDatasetName):
        """
        _addMergeTask_

        Create a merge task for files produced by the parent task.
        """
        mergeTask = parentTask.addTask("%sMerge%s" % (parentTask.name(), parentOutputModule))
        #self.addDashboardMonitoring(mergeTask)
        mergeTaskCmssw = mergeTask.makeStep("cmsRun1")
        mergeTaskCmssw.setStepType("CMSSW")

        mergeTaskStageOut = mergeTaskCmssw.addStep("stageOut1")
        mergeTaskStageOut.setStepType("StageOut")
        mergeTaskLogArch = mergeTaskCmssw.addStep("logArch1")
        mergeTaskLogArch.setStepType("LogArchive")

        mergeTask.setTaskLogBaseLFN(self.unmergedLFNBase)        
        #self.addLogCollectTask(mergeTask, taskName = "%s%sMergeLogCollect" % (parentTask.name(), parentOutputModule))

        mergeTask.addGenerator("BasicNaming")
        mergeTask.addGenerator("BasicCounter")
        mergeTask.setTaskType("Merge")  
        mergeTask.applyTemplates()

        
        splitAlgo = "WMBSMergeBySize"
        
        mergeTask.setSplittingAlgorithm(splitAlgo,
                                        max_merge_size = self.maxMergeSize,
                                        min_merge_size = self.minMergeSize,
                                        max_merge_events = self.maxMergeEvents,
                                        siteWhitelist = self.siteWhitelist,
                                        siteBlacklist = self.siteBlacklist)

        mergeTaskCmsswHelper = mergeTaskCmssw.getTypeHelper()
        mergeTaskCmsswHelper.cmsswSetup(self.frameworkVersion, softwareEnvironment = "",
                                        scramArch = self.scramArch)
        mergeTaskCmsswHelper.setDataProcessingConfig("cosmics", "merge")

        mergedLFN = "%s/%s/%s" % (self.mergedLFNBase, dataTier, processedDatasetName)    
        mergeTaskCmsswHelper.addOutputModule("Merged",
                                             primaryDataset = primaryDataset,
                                             processedDataset = processedDatasetName,
                                             dataTier = dataTier,
                                             lfnBase = mergedLFN)

        parentTaskCmssw = parentTask.getStep("cmsRun1")
        mergeTask.setInputReference(parentTaskCmssw, outputModule = parentOutputModule)
        #self.addCleanupTask(parentTask, parentOutputModule)
        return mergeTask
    

class MonteCarloWorkloadFactory(MoveToStdBase):
    """
    MonteCarlo request type workload.
    Notable features
    - Config comes from ConfigCache, grabbing output modules from the PSetTweak info
    - Does not in ANY WAY SHAPE OR FORM DO ANYTHING WITH SCRAM. EVAR. DERP.
    
    """
    def __init__(self):
        MoveToStdBase.__init__(self)

        
    def __call__(self, workloadName, arguments):
        """
        Create a workload instance for a MonteCarlo request
        
        """
        # Required parameters.
        self.workloadName = workloadName
        self.acquisitionEra = arguments["AcquisitionEra"]
        self.owner = arguments["Requestor"]
        self.frameworkVersion = arguments["CMSSWVersion"]
        self.scramArch = arguments["ScramArch"]
        self.processingVersion = arguments["ProcessingVersion"]
        self.globalTag = arguments["GlobalTag"]        
        self.primaryDataset = arguments['PrimaryDataset']
        jobSplittingAlgo = arguments.get("JobSplittingAlgorithm", "EventBased")
        jobSplittingParams = arguments.get("JobSplittingArgs", {"events_per_job": 1000})
        

        
        self.couchUrl = arguments.get("CouchUrl", "http://derpderp:derpityderp@cmssrv52.derp.gov:5984")
        self.couchDBName = arguments.get("CouchDBName", "wmagent_config_cache")        
        self.couchConfigDoc = arguments.get("ConfigCacheDoc", None)
        
        # for publication
        self.dbsUrl = arguments.get("DbsUrl", "http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet")

        self.siteBlacklist = arguments.get("SiteBlacklist", [])
        self.siteWhitelist = arguments.get("SiteWhitelist", [])

        self.unmergedLFNBase = arguments.get("UnmergedLFNBase", "/store/temp/WMAgent/unmerged")
        self.mergedLFNBase = arguments.get("MergedLFNBase", "/store/temp/WMAgent/merged")

        self.minMergeSize = arguments.get("MinMergeSize", 500000000)
        self.maxMergeSize = arguments.get("MaxMergeSize", 4294967296)
        self.maxMergeEvents = arguments.get("MaxMergeEvents", 100000)
        self.emulation = arguments.get("Emulation", False)
        
        workload = self.newWorkload()
        
    
        production = workload.newTask("Production")
        productionCmssw = production.makeStep("cmsRun1")
        productionCmssw.setStepType("CMSSW")
        productionStageOut = productionCmssw.addStep("stageOut1")
        productionStageOut.setStepType("StageOut")
        productionLogArch = productionCmssw.addStep("logArch1")
        productionLogArch.setStepType("LogArchive")
        production.applyTemplates()
        production.setSplittingAlgorithm(jobSplittingAlgo, **jobSplittingParams)
        production.addGenerator("BasicNaming")
        production.addGenerator("BasicCounter")
        #TODO: Seed Generator
        production.setTaskType("Production")
        production.addProduction(**{"ProductionArgs": "GoHere"})
    
        prodTaskCmsswHelper = productionCmssw.getTypeHelper()
        prodTaskCmsswHelper.setGlobalTag(self.globalTag)
        prodTaskCmsswHelper.cmsswSetup(self.frameworkVersion, softwareEnvironment = "",
                                           scramArch = self.scramArch)
        prodTaskCmsswHelper.setConfigCache(self.couchUrl, self.couchConfigDoc, dbName = self.couchDBName)
    
        for omod in self.getOutputModules():
            self.addOutputModule( production,  omod['moduleName'], self.primaryDataset,
                                omod['dataTier'], omod['filterName'])

        return workload
    
    def getOutputModules(self):
        """
        _getOutputModules_
        
        Use the config cache URL to pull the PSet Tweak and grab the output modules it defines
        
        TODO: Move to ConfigCache API, this will be general for anything dealing with the config cache
        
        """
        config = ConfigSection("ConfigCache")
        config.section_("CoreDatabase")
        config.CoreDatabase.couchurl = self.couchUrl
        confCache = ConfigCache(config = config, couchDBName= self.couchDBName, id = self.couchConfigDoc)
        confCache.load()
        outMods = confCache.document[u'pset_tweak_details'][u'process'][u'outputModules_']
        
        for outMod in outMods:
            outModule = confCache.document[u'pset_tweak_details'][u'process'][outMod]
            result = {"moduleName" : str(outMod)}
            for datasetP in outModule[u'dataset'][u'parameters_']:
                result[str(datasetP)] = str(outModule[u'dataset'][datasetP])
            yield result

        

def getTestArguments():
    """generate some test data"""
    args = {}
    args['AcquisitionEra'] = "CSA2010"
    args['Requestor'] = "evansde77"
    args['CMSSWVersion'] = "CMSSW_3_7_1"
    args["ScramArch"] =  "slc5_ia32_gcc434"
    args["ProcessingVersion"] = "v2scf"
    args["SkimInput"] = "output"
    args["GlobalTag"] = "GR10_P_v4::All"
    
    args["ProcessingConfig"] = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/Configuration/GlobalRuns/python/rereco_FirstCollisions_MinimumBias_35X.py?revision=1.8"
    args["SkimConfig"] = "http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/CMSSW/Configuration/DataOps/python/prescaleskimmer.py?revision=1.1"
    
    args["CouchUrl"] = "http://dmwmwriter:PASSWORD@cmssrv52.fnal.gov:5984"
    args["CouchDBName"] = "config_cache1"
    args["ConfigCacheDoc"] = "f6676adf792b73cd24f3b9b3c260f575"
    
    args['PrimaryDataset'] = "Derp"
    
    return args

def main():
    """main functionf for testing"""
    #from WMCore.DataStructs.Job import Job
    
    factory = MonteCarloWorkloadFactory()
    workload = factory("derp", getTestArguments())


    task = workload.getTask('Production')

    task.build(os.getcwd())
    #task.execute(Job("job1"))

if __name__ == '__main__':
    main()

