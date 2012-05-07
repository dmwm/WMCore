#!/usr/bin/env python
"""
_Harvesting_

Standard Harvesting workflow.
"""
import os

from WMCore.WMSpec.WMWorkload import newWorkload
from WMCore.WMSpec.WMStep import makeWMStep
from WMCore.WMSpec.Steps.StepFactory import getStepTypeHelper

from WMCore.Cache.ConfigCache import WMConfigCache
from WMCore.WMSpec.StdSpecs import SplitAlgoStartPolicyMap
from WMCore.WMSpec.StdSpecs.StdBase import StdBase

def getTestArguments():
    """
    _getTestArguments_

    This should be where the default REQUIRED arguments go
    This serves as documentation for what is currently required 
    by the standard Harvesting workload in importable format.

    NOTE: These are test values.  If used in real workflows they
    will cause everything to crash/die/break, and we will be forced
    to hunt you down and kill you.
    """

    arguments = {
        "CmsPath": "/afs/cern.ch/cms/sw",
        "Requestor": "direyes@cern.ch",
        "InputDataset": "/RelValMinBias/CMSSW_3_8_2-MC_38Y_V9-v1/GEN-SIM-RECO",
        "CMSSWVersion": "CMSSW_3_8_2",
        "ScramArch": "slc5_ia32_gcc434",
        "ProcessingVersion": "v1",
        "GlobalTag": "MC_38Y_V9::All",
        "Scenario": "relvalmc",
        "Proxy": "/afs/cern.ch/user/r/relval/.globus/direyes/myproxy",
        "DqmGuiUrl": "https://cmsweb.cern.ch/dqm/dev",
        "CouchUrl": None,
        "DoStageOut": True,
        "DoDqmUpload": True,
        "DqmBaseLFN": '/store/temp/WMAgent/dqm',
        "RefHistogram": '/store/unmerged/dqm/wmagent/CMSSW_3_8_1/RelValMinBias/GEN-SIM-RECO/MC_38Y_V8-v1/0000/DQM_V0001_R000000001__RelValMinBias__CMSSW_3_8_1-MC_38Y_V8-v1__GEN-SIM-RECO.root',
        "DashboardHost": "127.0.0.1",
        "DashboardPort": 8884
        }

    return arguments

class HarvestingWorkloadFactory(StdBase):
    """
    _HarvestingWorkloadFactory_

    Stamp out Harvesting workflows.
    """
    def __init__(self):
        StdBase.__init__(self)
        return

    def addDashboardMonitoring(self, task):
        """
        _addDashboardMonitoring_
        
        Add dashboard monitoring for the given task.
        """
        monitoring = task.data.section_("watchdog")
        monitoring.interval = 600
        monitoring.monitors = ["DashboardMonitor"]
        monitoring.section_("DashboardMonitor")
        monitoring.DashboardMonitor.softTimeOut = 300000
        monitoring.DashboardMonitor.hardTimeOut = 600000
        monitoring.DashboardMonitor.destinationHost = "cms-pamon.cern.ch"
        monitoring.DashboardMonitor.destinationPort = 8884
        return task

    def newWorkload(self):
        """
        _newWorkload_

        Create a new workload.
        """
        workload = newWorkload(self.workloadName)
        workload.setOwner(self.owner)
        #workload.data.properties.acquisitionEra = self.acquisitionEra        
        return workload
    
    def setupHarvestingTask(self, harvTask, taskType, inputDataset = None, inputStep = None,
                            inputModule = None, scenarioName = None,
                            scenarioFunc = None, scenarioArgs = None, couchUrl = None,
                            couchDBName = None, configDoc = None, splitAlgo = "RunBased",
                            splitArgs = {'files_per_job': 1000}):
        """
        _setupHarvestingTask_

        Given an empty task add cmsRun, stagOut and logArch steps.  Configure
        the input depending on the method parameters:
          inputDataset not empty: This is a top level processing task where the
            input will be fed in by DBS.  Setup the whitelists and blacklists.
          inputDataset empty: This processing task will be fed from the output
            of another task.  The inputStep and name of the output module from
            that step (inputModule) must be specified.

        Processing config will be setup as follows:
          configDoc not empty - Use a ConfigCache config, couchUrl and
            couchDBName must not be empty.
          configDoc empty - Use a Configuration.DataProcessing config.  The
            scenarioName, scenarioFunc and scenarioArgs parameters must not be
            empty.
        """
        #self.addDashboardMonitoring(harvTask)
        harvTaskCmssw = harvTask.makeStep("cmsRun1")
        harvTaskCmssw.setStepType("CMSSW")
        harvTaskDQMUpload = harvTaskCmssw.addStep("dqmUpload1")
        harvTaskDQMUpload.setStepType("DQMUpload")
        harvTaskLogArch = harvTaskCmssw.addStep("logArch1")
        harvTaskLogArch.setStepType("LogArchive")
        harvTask.applyTemplates()

        # Adding proxy to input sandbox
        harvTaskDQMUploadHelper = harvTaskDQMUpload.getTypeHelper()
        harvTaskDQMUploadHelper.setProxyFile(os.path.basename(self.proxy))
        harvTaskDQMUploadHelper.data.sandbox.section_("proxy")
        harvTaskDQMUploadHelper.data.sandbox.proxy.src = self.proxy
        harvTaskDQMUploadHelper.data.sandbox.proxy.injob = self.proxy

        # Adding DQM Server URL
        harvTaskDQMUploadHelper.setServerURL(self.dqmGuiUrl)

        # Deactivating DQM upload and Stage Out
        if not self.doStageOut:
            harvTaskDQMUploadHelper.disableStageOut()
        if not self.doDqmUpload:
            harvTaskDQMUploadHelper.disableUpload()

        # TODO: Might be good to add a set method the Task Helper
        harvTask.data.dqmBaseLFN = self.dqmBaseLFN

        harvTask.setSiteWhitelist(self.siteWhitelist)
        harvTask.setSiteBlacklist(self.siteBlacklist)

        newSplitArgs = {}
        for argName in splitArgs.keys():
            newSplitArgs[str(argName)] = splitArgs[argName]
        
        harvTask.setSplittingAlgorithm(splitAlgo, **newSplitArgs)
        harvTask.setTaskType(taskType)
        
        (primary, processed, tier) = self.inputDataset[1:].split("/")
        harvTask.addInputDataset(primary = primary, processed = processed,
                                 tier = tier, dbsurl = self.dbsUrl,
                                 block_blacklist = self.blockBlacklist,
                                 block_whitelist = self.blockWhitelist,
                                 run_blacklist = self.runBlacklist,
                                 run_whitelist = self.runWhitelist)

        harvTaskCmsswHelper = harvTaskCmssw.getTypeHelper()
        harvTaskCmsswHelper.setGlobalTag(self.globalTag)
        harvTaskCmsswHelper.cmsswSetup(self.frameworkVersion, softwareEnvironment = "",
                                       scramArch = self.scramArch)
        if configDoc != None:
            harvTaskCmsswHelper.setConfigCache(couchUrl, configDoc, couchDBName)
        else:
            harvTaskCmsswHelper.setDataProcessingConfig(scenarioName, scenarioFunc,
                                                        **scenarioArgs)
        
        return harvTask

    def addLogCollectTask(self, parentTask, taskName = "LogCollect"):
        """
        _addLogCollectTask_
        
        Create a LogCollect task for log archives that are produced by the
        parent task.
        """
        logCollectTask = parentTask.addTask(taskName)
        #self.addDashboardMonitoring(logCollectTask)        
        logCollectStep = logCollectTask.makeStep("logCollect1")
        logCollectStep.setStepType("LogCollect")
        logCollectTask.applyTemplates()
        logCollectTask.setSplittingAlgorithm("EndOfRun", files_per_job = 500)
        logCollectTask.setTaskType("LogCollect")
    
        parentTaskLogArch = parentTask.getStep("logArch1")
        logCollectTask.setInputReference(parentTaskLogArch, outputModule = "logArchive")
        return

    def __call__(self, workloadName, arguments):
        """
        _call_

        Create a Harvesting workload with the given parameters.
        """
        # Required parameters.
        self.workloadName = workloadName
        self.owner = arguments["Requestor"]
        self.inputDataset = arguments["InputDataset"]
        self.frameworkVersion = arguments["CMSSWVersion"]
        self.scramArch = arguments["ScramArch"]
        self.processingVersion = arguments["ProcessingVersion"]
        self.globalTag = arguments["GlobalTag"]        
        self.cmsPath = arguments["CmsPath"]
        self.couchUrl = arguments["CouchUrl"]
        self.proxy = arguments['Proxy']
        self.dqmGuiUrl = arguments['DqmGuiUrl']

        # Required parameters that can be empty.
        # Add an extra input argument to
        # Configuration.DataProcessing.Impl.scenario.dqmHarvesting()
        # and let this method insert the reference file.
        self.refHistogram = arguments.get("RefHistogram", None)
        self.doStageOut = arguments.get("DoStageOut", True)
        self.doDqmUpload = arguments.get("DoDqmUpload", True)
        self.scenario = arguments["Scenario"]
        self.harvestingConfig = arguments.get("HarvestingConfig", None)
        self.couchDBName = arguments.get("CouchDBName", "wmagent_config_cache")        
        
        # Optional arguments.
        self.dbsUrl = arguments.get("DbsUrl", "http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet")
        self.blockBlacklist = arguments.get("BlockBlacklist", [])
        self.blockWhitelist = arguments.get("BlockWhitelist", [])
        self.runBlacklist = arguments.get("RunBlacklist", [])
        self.runWhitelist = arguments.get("RunWhitelist", [])
        self.siteBlacklist = arguments.get("SiteBlacklist", [])
        self.siteWhitelist = arguments.get("SiteWhitelist", [])
        self.dqmBaseLFN = arguments.get("DqmBaseLFN", "/store/temp/WMAgent/dqm")
        self.emulation = arguments.get("Emulation", False)
        self.jobSplitAlgo  = arguments.get("JobSplitAlgo", 'RunBased')
        # TODO: The splitting algorithm should be unbounded. I mean,
        # if files_per_job is empty then it should use all the files from
        # given run.
        self.jobSplitArgs  = arguments.get("JobSplitArgs", {'files_per_job': 1000,
                                                            'require_run_closed': False})

        (self.inputPrimaryDataset, self.inputProcessedDataset, self.inputDataTier) = \
                                   self.inputDataset[1:].split("/")

        procConfigDoc = None
        if self.couchUrl != None and self.couchDBName != None:
            myConfigCache = WMConfigCache(dbname2 = self.couchDBName, dburl = self.couchUrl)
            if self.harvestingConfig != "":
                (procConfigDoc, rev) = myConfigCache.addConfig(self.harvestingConfig)

        workload = self.newWorkload()
        harvTask = workload.newTask("Harvesting")

        # Filling up information for Configuration/DataProcessing scenarios
        scenarioArgs = {"globalTag": self.globalTag,
                        "datasetName": self.inputDataset,
                        "runNumber": self.runWhitelist} # runNumber is not really used by dqmHarvesting(), putting a dummy value.
        # Is there a reference histogram?
        if self.refHistogram is not None:
            scenarioArgs["referenceFile"] = self.refHistogram

        self.setupHarvestingTask(harvTask, "Harvesting", self.inputDataset,
                                 scenarioName = self.scenario, scenarioFunc = "dqmHarvesting",
                                 scenarioArgs = scenarioArgs,
                                 couchUrl = self.couchUrl, couchDBName = self.couchDBName,
                                 configDoc = procConfigDoc, splitAlgo = self.jobSplitAlgo,
                                 splitArgs = self.jobSplitArgs) 

        workload.setStartPolicy("Dataset",
                                SliceType = SplitAlgoStartPolicyMap.getSliceType(self.jobSplitAlgo), 
                                SliceSize = SplitAlgoStartPolicyMap.getSliceSize(self.jobSplitAlgo, self.jobSplitArgs))
        
        workload.setEndPolicy("SingleShot")


        self.addLogCollectTask(harvTask)

        workload.setDashboardActivity("Harvesting")
        self.reportWorkflowToDashboard(workload.getDashboardActivity())

        return workload

def harvestingWorkload(workloadName, arguments):
    """
    _harvestingWorkload_

    Instantiate the HarvestingWorkflowFactory and have it generate a workload for
    the given parameters.
    """
    harvestingFactory = HarvestingWorkloadFactory()
    return harvestingFactory(workloadName, arguments)

def main():
    """main functionf for testing"""
    from WMCore.DataStructs.Job import Job
    from WMCore.DataStructs.File import File
    from WMCore.DataStructs.Run import Run
    from WMCore.DataStructs.JobPackage import JobPackage
    from WMCore.Services.UUID import makeUUID
    from WMCore.WMSpec.Makers.TaskMaker import TaskMaker

    factory = HarvestingWorkloadFactory()
    workload = factory("derp", getTestArguments())

    task = workload.getTask('Harvesting')

    job = Job("SampleJob")
    job["id"] = makeUUID()
    job["task"] = task.getPathName()
    job["workflow"] = workload.name()

    file = File(lfn="/store/relval/CMSSW_3_8_2/RelValMinBias/GEN-SIM-RECO/MC_38Y_V9-v1/0019/FEC5BB4D-BFAF-DF11-A52A-001A92810AD2.root")
    job.addFile(file)

    jpackage = JobPackage()
    jpackage[1] = job

    import pickle
    
    handle = open("%s/JobPackage.pkl" % os.getcwd(), 'w')
    pickle.dump(jpackage, handle)
    handle.close()

    taskMaker = TaskMaker(workload, os.getcwd())
    taskMaker.skipSubscription = True
    taskMaker.processWorkload()
    task.build(os.getcwd())

#    task.build(os.getcwd())
#    task.execute(Job("job1"))


