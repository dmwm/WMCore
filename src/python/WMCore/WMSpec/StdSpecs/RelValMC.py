#!/usr/bin/env python
"""
RelValMC - CMSSW release validation workflow.

"""

import os

from WMCore.WMSpec.StdSpecs.StdBase import StdBase

def getTestArguments():
    """
    _getTestArguments_

    Get example arguments for running this workflow.
    """
    args = {}
    args["AcquisitionEra"] = "CSA2010"
    args["Requestor"] = "sfoulkes@fnal.gov"
    args["ScramArch"] =  "slc5_ia32_gcc434"
    args["PrimaryDataset"] = "MonteCarloData"
    args["ProcessingVersion"] = 2
    args["GlobalTag"] = None
    args["RequestNumEvents"] = 10
    args["CouchURL"] = os.environ.get("COUCHURL", None)
    args["CouchDBName"] = "scf_wmagent_configcache"
    # or alternatively CouchURL part can be replaced by ConfigCacheUrl,
    # then ConfigCacheUrl + CouchDBName + ConfigCacheID
    args["ConfigCacheUrl"] = None
    
    args["CMSSWVersion"] = "CMSSW_3_8_1"

    args["GenConfigCacheID"] = "nonsense_id_gen"
    args["StepOneConfigCacheID"] = "nonsense_id_step_one"
    args["StepTwoConfigCacheID"] = "nonsense_id_step_two"
    args["GenDataOutputModuleName"] = "GenSimOutput"
    args["StepOneOutputModuleName"] = "GenSimRawOutput"
    args['DashboardHost'] = "127.0.0.1"
    args['DashboardPort'] = 8884
    return args

class RelValMCWorkloadFactory(StdBase):
    def __init__(self):
        StdBase.__init__(self)

    def buildWorkload(self):
        """
        Build a workflow for a RelValMC request.
        This means a production config and merge tasks for each output module.

        All tasknames hierarchy do automatically derive from each other
        and output -> input files names are sorted out automatically by this
        dependent running.

        (The chaining implemented in WMCore/WMSpec/Steps/Templates/CMSSW.py
        setupChainedProcessingby is not used here.)

        """
        workload = self.createWorkload()
        workload.setDashboardActivity("relval")
        self.reportWorkflowToDashboard(workload.getDashboardActivity())
        workload.setWorkQueueSplitPolicy("MonteCarlo", self.genJobSplitAlgo,
                                         self.genJobSplitArgs)

        genTask = workload.newTask("Generation")
        genOutputMods = self.setupProcessingTask(genTask, "Production",
                                                 inputDataset = None,
                                                 couchURL = self.couchURL,
                                                 couchDBName = self.couchDBName,
                                                 configDoc = self.genConfigCacheID,
                                                 configCacheUrl = self.configCacheUrl,
                                                 splitAlgo = self.genJobSplitAlgo,
                                                 splitArgs = self.genJobSplitArgs,
                                                 seeding = self.seeding,
                                                 totalEvents = self.totalEvents)
        self.addLogCollectTask(genTask, "GenLogCollect")
        if self.pileupConfig:
            self.setupPileup(genTask, self.pileupConfig)

        genMergeTask = None
        for outputModuleName in genOutputMods.keys():
            outputModuleInfo = genOutputMods[outputModuleName]
            task = self.addMergeTask(genTask, self.genJobSplitAlgo,
                                     outputModuleName)
            if outputModuleName == self.genOutputModuleName:
                genMergeTask = task

        stepOneTask = genMergeTask.addTask("StepOne")
        parentCmsswStep = genMergeTask.getStep("cmsRun1")
        stepOneOutputMods = self.setupProcessingTask(stepOneTask, "Processing",
                                                     inputStep = parentCmsswStep,
                                                     inputModule = "Merged",
                                                     couchURL = self.couchURL,
                                                     configCacheUrl = self.configCacheUrl,
                                                     couchDBName = self.couchDBName,
                                                     configDoc = self.stepOneConfigCacheID,
                                                     splitAlgo = self.procJobSplitAlgo,
                                                     splitArgs = self.procJobSplitArgs)
        self.addLogCollectTask(stepOneTask, "ProcLogCollect")

        stepOneMergeTask = None
        for outputModuleName in stepOneOutputMods.keys():
            outputModuleInfo = stepOneOutputMods[outputModuleName]
            task = self.addMergeTask(stepOneTask, self.procJobSplitAlgo,
                                     outputModuleName)
            if outputModuleName == self.stepOneOutputModuleName:
                stepOneMergeTask = task

        stepTwoTask = stepOneMergeTask.addTask("StepTwo")
        parentCmsswStep = stepOneMergeTask.getStep("cmsRun1")
        stepTwoOutputMods = self.setupProcessingTask(stepTwoTask, "Processing",
                                                     inputStep = parentCmsswStep,
                                                     inputModule = "Merged",
                                                     couchURL = self.couchURL,
                                                     couchDBName = self.couchDBName,
                                                     configDoc = self.stepTwoConfigCacheID,
                                                     configCacheUrl = self.configCacheUrl,
                                                     splitAlgo = self.procJobSplitAlgo,
                                                     splitArgs = self.procJobSplitArgs)
        self.addLogCollectTask(stepTwoTask, "StepTwoLogCollect")

        for outputModuleName in stepTwoOutputMods.keys():
            outputModuleInfo = stepTwoOutputMods[outputModuleName]
            self.addMergeTask(stepTwoTask, self.procJobSplitAlgo,
                              outputModuleName)
        return workload


    def __call__(self, workloadName, arguments):
        """
        __call__

        Create a RelValMC workload with the given parametrs.
        """
        StdBase.__call__(self, workloadName, arguments)

        self.frameworkVersion = arguments["CMSSWVersion"]
        self.globalTag = arguments["GlobalTag"]

        # Required parameters relevant to the MC generation.
        self.genConfigCacheID = arguments["GenConfigCacheID"]
        self.inputPrimaryDataset = arguments["PrimaryDataset"]
        self.totalEvents = arguments["RequestNumEvents"]
        self.seeding = arguments.get("Seeding", "AutomaticSeeding")
        self.pileupConfig = arguments.get("PileupConfig", None)

        # The CouchURL and name of the ConfigCache database must be passed in
        # by the ReqMgr or whatever is creating this workflow.
        self.couchURL = arguments["CouchURL"]
        self.couchDBName = arguments["CouchDBName"]
        self.configCacheUrl = arguments.get("ConfigCacheUrl", None)

        # Generation step parameters
        self.genJobSplitAlgo = arguments.get("GenJobSplitAlgo", "EventBased")
        self.genJobSplitArgs = arguments.get("GenJobSplitArgs",
                                             {"events_per_job": 1000})

        # Processing step parameteras
        self.procJobSplitAlgo = arguments.get("ProcJobSplitAlgo", "FileBased")
        self.procJobSplitArgs = arguments.get("ProcJobSplitArgs",
                                              {"files_per_job": 1})

        self.genOutputModuleName = arguments.get("GenOutputModuleName", None)
        self.stepOneOutputModuleName = arguments.get("StepOneOutputModuleName", None)
        self.stepOneConfigCacheID = arguments["StepOneConfigCacheID"]
        self.stepTwoConfigCacheID = arguments["StepTwoConfigCacheID"]
        return self.buildWorkload()

    def validateSchema(self, schema):
        """
        _validateSchema_

        Check for required fields, and some skim facts
        """
        requiredFields = ["CMSSWVersion", "Requestor", "ScramArch",
                          "PrimaryDataset", "GlobalTag", "RequestNumEvents",
                          "GenConfigCacheID", "StepOneConfigCacheID", "StepTwoConfigCacheID",
                          "GenOutputModuleName", "StepOneOutputModuleName", "CouchURL", "CouchDBName"]
        self.requireValidateFields(fields = requiredFields, schema = schema,
                                   validate = False)
        couchCacheUrl = schema.get("ConfigCacheUrl", None) or schema["CouchURL"]
        outMod = self.validateConfigCacheExists(configID = schema["GenConfigCacheID"],
                                                couchURL = couchCacheUrl,
                                                couchDBName = schema["CouchDBName"],
                                                getOutputModules = True)
        if not schema["GenOutputModuleName"] in outMod.keys():
            self.raiseValidationException(msg = "GenOutputMod Name not found in configCache")
        outMod = self.validateConfigCacheExists(configID = schema["StepOneConfigCacheID"],
                                                couchURL = couchCacheUrl,
                                                couchDBName = schema["CouchDBName"],
                                                getOutputModules = True)
        if not schema["StepOneOutputModuleName"] in outMod.keys():
            self.raiseValidationException(msg = "StepOneOutputMod Name not found in configCache")
        outMod = self.validateConfigCacheExists(configID = schema["StepTwoConfigCacheID"],
                                                couchURL = couchCacheUrl,
                                                couchDBName = schema["CouchDBName"],
                                                getOutputModules = True)        



def relValMCWorkload(workloadName, arguments):
    """
    Instantiate the RelValMCWorkloadFactory and have it generate
    a workload for the given parameters.
    """
    myRelValMCFactory = RelValMCWorkloadFactory()
    instance = myRelValMCFactory(workloadName, arguments)
    return instance