#!/usr/bin/env python
"""
_DQMHarvest_

Workflow for harvest an input dataset.
"""
from WMCore.WMSpec.StdSpecs.DataProcessing import DataProcessing


class DQMHarvestWorkloadFactory(DataProcessing):
    """
    _DQMHarvestWorkloadFactory_

    """

    def __call__(self, workloadName, arguments):
        """
        _call_

        Create a DQMHarvest workload with the given parameters.
        """
        DataProcessing.__call__(self, workloadName, arguments)

        self.workload = self.createWorkload()

        self.workload.setDashboardActivity("harvesting")

        splitArgs = {"runs_per_job": 1}
        if self.dqmHarvestUnit == "multiRun":
            # then it should result in a single job in the end, very high number of runs
            splitArgs['runs_per_job'] = 999999
        self.workload.setWorkQueueSplitPolicy("Dataset", "Harvest", splitArgs)

        # also creates the logCollect job by default
        self.addDQMHarvestTask(uploadProxy=self.dqmUploadProxy,
                               periodic_harvest_interval=self.periodicHarvestInterval,
                               dqmHarvestUnit=self.dqmHarvestUnit)

        # setting the parameters which need to be set for all the tasks
        # sets acquisitionEra, processingVersion, processingString
        self.workload.setTaskPropertiesFromWorkload()

        return self.workload

    def validateSchema(self, schema):
        """
        _validateSchema_

        Standard DataProcessing schema validation.
        """
        DataProcessing.validateSchema(self, schema)

        self.validateConfigCacheExists(configID=schema["DQMConfigCacheID"],
                                       configCacheUrl=schema['ConfigCacheUrl'],
                                       couchDBName=schema["CouchDBName"],
                                       getOutputModules=False)

    @staticmethod
    def getWorkloadCreateArgs():
        baseArgs = DataProcessing.getWorkloadCreateArgs()
        specArgs = {"RequestType": {"default": "DQMHarvest", "optional": True},
                    "ConfigCacheID": {"optional": True, "null": True},
                    "DQMConfigCacheID": {"optional": False, "attr": "dqmConfigCacheID"},
                    "DQMUploadUrl": {"optional": False, "attr": "dqmUploadUrl"},
                   }
        baseArgs.update(specArgs)
        DataProcessing.setDefaultArgumentsProperty(baseArgs)
        return baseArgs

    def addDQMHarvestTask(self, parentTask=None, parentOutputModuleName=None, uploadProxy=None,
                          periodic_harvest_interval=0, periodic_harvest_sibling=False,
                          parentStepName="cmsRun1", doLogCollect=True, dqmHarvestUnit="byRun",
                          cmsswVersion=None, scramArch=None):
        """
        _addDQMHarvestTask_

        Create a DQM harvest task that does not depend on previous tasks.
        """
        if periodic_harvest_interval:
            harvestType = "Periodic"
        else:
            harvestType = "EndOfRun"

        harvestTask = self.workload.newTask("%sDQMHarvest" % harvestType)

        self.addRuntimeMonitors(harvestTask)
        harvestTaskCmssw = harvestTask.makeStep("cmsRun1")
        harvestTaskCmssw.setStepType("CMSSW")

        harvestTaskUpload = harvestTaskCmssw.addStep("upload1")
        harvestTaskUpload.setStepType("DQMUpload")
        harvestTaskLogArch = harvestTaskCmssw.addStep("logArch1")
        harvestTaskLogArch.setStepType("LogArchive")

        harvestTask.setTaskLogBaseLFN(self.unmergedLFNBase)
        if doLogCollect:
            self.addLogCollectTask(harvestTask, taskName="%sDQMHarvestLogCollect" % harvestType)

        harvestTask.setTaskType("Harvesting")
        harvestTask.applyTemplates()

        harvestTaskCmsswHelper = harvestTaskCmssw.getTypeHelper()
        harvestTaskCmsswHelper.cmsswSetup(self.frameworkVersion, softwareEnvironment="",
                                          scramArch=self.scramArch)

        harvestTaskCmsswHelper.setErrorDestinationStep(stepName=harvestTaskLogArch.name())
        harvestTaskCmsswHelper.setGlobalTag(self.globalTag)
        harvestTaskCmsswHelper.setOverrideCatalog(self.overrideCatalog)

        harvestTaskCmsswHelper.setUserLFNBase("/")

        (inputPrimaryDataset, inputProcessedDataset, inputDataTier) = self.inputDataset[1:].split("/")
        harvestTask.addInputDataset(name=self.inputDataset,
                                    primary=inputPrimaryDataset,
                                    processed=inputProcessedDataset,
                                    tier=inputDataTier,
                                    dbsurl=self.dbsUrl,
                                    block_whitelist=self.blockWhitelist,
                                    block_blacklist=self.blockBlacklist,
                                    run_whitelist=self.runWhitelist,
                                    run_blacklist=self.runBlacklist)

        harvestTask.setSplittingAlgorithm("Harvest",
                                          periodic_harvest_interval=periodic_harvest_interval,
                                          periodic_harvest_sibling=periodic_harvest_sibling,
                                          dqmHarvestUnit=dqmHarvestUnit)

        harvestTask.setJobResourceInformation(memoryReq=self.memory)

        if self.dqmConfigCacheID is not None:
            harvestTaskCmsswHelper.setConfigCache(self.configCacheUrl, self.dqmConfigCacheID, self.couchDBName)
            harvestTaskCmsswHelper.setDatasetName(self.inputDataset)
        else:
            scenarioArgs = {'globalTag': self.globalTag,
                            'datasetName': self.inputDataset,
                            'runNumber': self.runNumber,
                            'dqmSeq': self.dqmSequences}
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
            self.addDQMHarvestTask(uploadProxy=uploadProxy, periodic_harvest_interval=0,
                                   periodic_harvest_sibling=True, parentStepName=parentStepName,
                                   doLogCollect=doLogCollect, dqmHarvestUnit=dqmHarvestUnit)

        return
