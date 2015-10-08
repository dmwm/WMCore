from WMCore.Lexicon import dataset, block
from WMCore.WMSpec.StdSpecs.StdBase import StdBase
from WMCore.WMSpec.WMWorkloadTools import makeList
from WMCore.WMSpec.WMSpecErrors import WMSpecFactoryException

class DQMHarvestWorkloadFactory(StdBase):
    """
    _DQMHarvestWorkloadFactory_

    """

    def __call__(self, workloadName, arguments):
        """
        _call_

        Create a DQMHarvest workload with the given parameters.
        """
        StdBase.__call__(self, workloadName, arguments)

        self.workload = self.createWorkload()
        
        self.workload.setDashboardActivity("harvesting")
        self.reportWorkflowToDashboard(self.workload.getDashboardActivity())

        self.workload.setWorkQueueSplitPolicy("Dataset", "FileBased", {"files_per_job": 99999} )

        # also creates the logCollect job by default
        self.addDQMHarvestTask(uploadProxy = self.dqmUploadProxy,
                               periodic_harvest_interval= self.periodicHarvestInterval,
                               dqmHarvestUnit = self.dqmHarvestUnit)
        
        # setting the parameters which need to be set for all the tasks
        # sets acquisitionEra, processingVersion, processingString
        self.workload.setTaskPropertiesFromWorkload()
        
        return self.workload

    def validateSchema(self, schema):
        """
        _validateSchema_

        Standard StdBase schema validation, plus verification
        of the DQMConfigCacheID.
        """
        StdBase.validateSchema(self, schema)

        if not schema.get("DQMUploadUrl", None):
            msg = "DQMUploadUrl parameter has not been provided in the request"
            raise WMSpecFactoryException(message = msg)
        if not schema.get("DQMConfigCacheID", None):
            msg = "DQMConfigCacheID parameter has not been provided in the request"
            raise WMSpecFactoryException(message = msg)

        couchUrl = schema.get("ConfigCacheUrl", None)
        self.validateConfigCacheExists(configID = schema["DQMConfigCacheID"],
                                       couchURL = couchUrl,
                                       couchDBName = schema["CouchDBName"],
                                       getOutputModules = False)

    @staticmethod
    def getWorkloadArguments():
        baseArgs = StdBase.getWorkloadArguments()
        reqMgrArgs = StdBase.getWorkloadArgumentsWithReqMgr()
        baseArgs.update(reqMgrArgs)
        specArgs = {"RequestType" : {"default" : "DQMHarvest"},
                    "InputDataset" : {"default" : None, "optional" : False,
                                      "validate" : dataset},
                    "UnmergedLFNBase" : {"default" : "/store/unmerged"},
                    "MergedLFNBase" : {"default" : "/store/data"},
                    "MinMergeSize" : {"default" : 2 * 1024 * 1024 * 1024, "type" : int,
                                      "validate" : lambda x : x > 0},
                    "MaxMergeSize" : {"default" : 4 * 1024 * 1024 * 1024, "type" : int,
                                      "validate" : lambda x : x > 0},
                    "MaxMergeEvents" : {"default" : 100000, "type" : int,
                                        "validate" : lambda x : x > 0},
                    "BlockBlacklist" : {"default" : [], "type" : makeList,
                                        "validate" : lambda x: all([block(y) for y in x])},
                    "BlockWhitelist" : {"default" : [], "type" : makeList,
                                        "validate" : lambda x: all([block(y) for y in x])},
                    "RunBlacklist" : {"default" : [], "type" : makeList, 
                                      "validate" : lambda x: all([int(y) > 0 for y in x])},
                    "RunWhitelist" : {"default" : [], "type" : makeList,
                                      "validate" : lambda x: all([int(y) > 0 for y in x])}
                    }
        baseArgs.update(specArgs)
        StdBase.setDefaultArgumentsProperty(baseArgs)
        return baseArgs


    def addDQMHarvestTask(self, uploadProxy = None, periodic_harvest_interval = 0,
                          periodic_harvest_sibling = False, parentStepName = "cmsRun1",
                          doLogCollect = True, dqmHarvestUnit = "byRun"):
        """
        _addDQMHarvestTask_

        Create a DQM harvest task that does not depend on previous tasks.
        """
        if periodic_harvest_interval:
            harvestType = "Periodic"
        else:
            harvestType = "EndOfRun"

        harvestTask = self.workload.newTask("%sDQMHarvest" % harvestType)

        self.addDashboardMonitoring(harvestTask)
        harvestTaskCmssw = harvestTask.makeStep("cmsRun1")
        harvestTaskCmssw.setStepType("CMSSW")

        harvestTaskUpload = harvestTaskCmssw.addStep("upload1")
        harvestTaskUpload.setStepType("DQMUpload")
        harvestTaskLogArch = harvestTaskCmssw.addStep("logArch1")
        harvestTaskLogArch.setStepType("LogArchive")

        harvestTask.setTaskLogBaseLFN(self.unmergedLFNBase)
        if doLogCollect:
            self.addLogCollectTask(harvestTask, taskName = "%sDQMHarvestLogCollect" % harvestType)

        harvestTask.setTaskType("Harvesting")
        harvestTask.applyTemplates()

        harvestTaskCmsswHelper = harvestTaskCmssw.getTypeHelper()
        harvestTaskCmsswHelper.cmsswSetup(self.frameworkVersion, softwareEnvironment = "",
                                          scramArch = self.scramArch)

        harvestTaskCmsswHelper.setErrorDestinationStep(stepName = harvestTaskLogArch.name())
        harvestTaskCmsswHelper.setGlobalTag(self.globalTag)
        harvestTaskCmsswHelper.setOverrideCatalog(self.overrideCatalog)

        harvestTaskCmsswHelper.setUserLFNBase("/")

        (self.inputPrimaryDataset, self.inputProcessedDataset, self.inputDataTier) = self.inputDataset[1:].split("/")
        harvestTask.addInputDataset(primary = self.inputPrimaryDataset,
                                    processed = self.inputProcessedDataset,
                                    tier = self.inputDataTier,
                                    dbsurl = self.dbsUrl,
                                    block_whitelist = self.blockWhitelist,
                                    black_blacklist = self.blockBlacklist,
                                    run_whitelist =self.runWhitelist,
                                    run_blacklist = self.runBlacklist)

        harvestTask.setSplittingAlgorithm("Harvest",
                                          periodic_harvest_interval = periodic_harvest_interval,
                                          periodic_harvest_sibling = periodic_harvest_sibling,
                                          dqmHarvestUnit = dqmHarvestUnit)

        if self.dqmConfigCacheID is not None:
            if getattr(self, "configCacheUrl", None) is not None:
                harvestTaskCmsswHelper.setConfigCache(self.configCacheUrl, self.dqmConfigCacheID, self.couchDBName)
            else:
                harvestTaskCmsswHelper.setConfigCache(self.couchURL, self.dqmConfigCacheID, self.couchDBName)
            harvestTaskCmsswHelper.setDatasetName(self.inputDataset)
        else:
            scenarioArgs = { 'globalTag' : self.globalTag,
                             'datasetName' : self.inputDataset,
                             'runNumber' : self.runNumber,
                             'dqmSeq' : self.dqmSequences }
            if self.globalTagConnect:
                scenarioArgs['globalTagConnect'] = self.globalTagConnect
            if self.inputDataTier == "DQMIO":
                scenarioArgs['newDQMIO'] = True
            harvestTaskCmsswHelper.setDataProcessingConfig(self.procScenario,
                                                           "dqmHarvesting",
                                                           **scenarioArgs)

        harvestTaskUploadHelper = harvestTaskUpload.getTypeHelper()
        harvestTaskUploadHelper.setProxyFile(uploadProxy)
        harvestTaskUploadHelper.setServerURL(self.dqmUploadUrl)

        # if this was a Periodic harvesting add another for EndOfRun
        if periodic_harvest_interval:
            self.addDQMHarvestTask(uploadProxy = uploadProxy, periodic_harvest_interval = 0,
                                   periodic_harvest_sibling = True, parentStepName = parentStepName,
                                   doLogCollect = doLogCollect, dqmHarvestUnit = dqmHarvestUnit)

        return
