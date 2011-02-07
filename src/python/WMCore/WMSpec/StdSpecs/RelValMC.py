"""

RelValMC - CMSSW release validation workflow.

This workload is dependent running of the following tasks:
    1) generation Monte Carlo
    2) reconstruction
    3) ALCARECO
    
This is implementation of page 1 workload as specified
in a document attached here
https://svnweb.cern.ch/trac/CMSDMWM/ticket/655

Detailed instructions given on the "RelValMC Support in WM System" wave.

Support for pile up:
normally, the generation task has no input. However, if there is a 
pile up section defined in the configuration, the generation task
fetches from DBS the information about pileup input.

"""


import os

from WMCore.WMSpec.StdSpecs.StdBase import StdBase



def getTestArguments():
    args = {}
    args["AcquisitionEra"] = "CSA2010"
    args["Requestor"] = "sfoulkes@fnal.gov"
    args["ScramArch"] =  "slc5_ia32_gcc434"
    args["PrimaryDataset"] = "MonteCarloData"
    args["ProcessingVersion"] = "v2scf"
    args["GlobalTag"] = None
    args["RequestSizeEvents"] = 10
    args["CouchURL"] = os.environ.get("COUCHURL", None)
    args["CouchDBName"] = "scf_wmagent_configcache"
    args["CMSSWVersion"] = "CMSSW_3_8_1"
    args["ProcScenario"] = "cosmics"
    args["Multicore"] = 4
    # three additional arguments
    # will be correctly set after the configuration is injected into couchdb
    # (see the test)
    args["GenConfigCacheID"] = "nonsence_id_gen"
    args["RecoConfigCacheID"] = "nonsence_id_reco"
    args["AlcaRecoConfigCacheID"] = "nonsence_id_alcareco"
    # another additional argument - name of the datatier of the generatio task
    args["GenDataTier"] = None
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
        
        # tell the WorkQueue how to estimate how much work is in the request
        # will be done just once for the first task
        workload.setWorkQueueSplitPolicy("MonteCarlo", self.genJobSplitAlgo, self.genJobSplitArgs)
        
        # setting up 6 layers of tasks (based on MonteCarlo, ReReco, PromptSkim workloads)
        # 6 layers of tasks, GEN -> Merge -> RECO -> Merge -> ALCARECO -> Merge
        
        # 1 - generation task
        # name of the task - important (e.g. when searching WMBS)
        genTask = workload.newTask("Generation")
        
        # def setupProcessingTask(self, procTask, taskType, inputDataset = None, inputStep = None,
        # method is defined in StdBase
        # second attribute (taskType), if it's != "Production", issues with inputDataset
        # value which needs to be set accordingly
        # configDoc is in fact a config document ID in the couch
        genOutputMods = self.setupProcessingTask(genTask, "Production",
                                                 inputDataset = None,
                                                 couchURL = self.couchURL,
                                                 couchDBName = self.couchDBName,
                                                 configDoc = self.genConfig,
                                                 splitAlgo = self.genJobSplitAlgo,
                                                 splitArgs = self.genJobSplitArgs,
                                                 seeding = self.seeding,
                                                 totalEvents = self.totalEvents)
        self.addLogCollectTask(genTask, "GenLogCollect")

        # pile up support implementation
        # generation task will still take inputDataset = None but if
        # there is pileup configuration supplied, it will be added
        # to the first generation task (configuration of the generation task
        # step helper)
        if self.pileupConfig:
            self.setupPileup(genTask, self.pileupConfig)

        # 2 - merge task for the data produced by the generation task
        # the subsequent rereco task will runs either
        #    1) over 'generationDataTier' name of it's specified by the user or
        #    2) the first output module if generationDataTier is not specified
        # can't no longer assume that the datatier will always be "GEN-SIM-RAW" 
        # dataTier - it specifies that content/format of the data
        genSimRawMergeTask = None
        for outputModuleName in genOutputMods.keys():
            outputModuleInfo = genOutputMods[outputModuleName]
            task = self.addMergeTask(genTask, self.genJobSplitAlgo,
                                     outputModuleName,        
                                     outputModuleInfo["dataTier"],
                                     outputModuleInfo["filterName"],
                                     outputModuleInfo["processedDataset"])
            if not self.genDataTier and not genSimRawMergeTask:
                # generation task data tier has not been user-specified
                # and genSimRawMergeTask has not been set yet - we're at first output module
                genSimRawMergeTask = task
            else:
                if outputModuleInfo["dataTier"] == self.genDataTier:
                    genSimRawMergeTask = task
                
        # 3 - ReReco task to run reco on data produced by the MC generation task
        recoTask = genSimRawMergeTask.addTask("Reconstruction")
        parentCmsswStep = genSimRawMergeTask.getStep("cmsRun1")
        recoOutputMods = self.setupProcessingTask(recoTask, "Processing",
                                                  inputStep = parentCmsswStep,
                                                  inputModule = "Merged",
                                                  couchURL = self.couchURL,
                                                  couchDBName = self.couchDBName,
                                                  configDoc = self.recoConfig,
                                                  splitAlgo = self.recoJobSplitAlgo,
                                                  splitArgs = self.recoJobSplitArgs)
        self.addLogCollectTask(recoTask, "RecoLogCollect")
        
        # 4 merge task for reconstruction task
        # loop through all of the output modules and create merge tasks, 
        # catch the merged "GEN-SIM-RECO" merge task and create and add an
        # alcareco (skim) task to it.
        genSimRecoMergeTask = None
        for outputModuleName in recoOutputMods.keys():
            outputModuleInfo = recoOutputMods[outputModuleName]
            task = self.addMergeTask(recoTask, self.recoJobSplitAlgo,
                                     outputModuleName,
                                     outputModuleInfo["dataTier"],
                                     outputModuleInfo["filterName"],
                                     outputModuleInfo["processedDataset"])
            if outputModuleInfo["dataTier"] == "GEN-SIM-RECO":
                genSimRecoMergeTask = task
                
        # 5 ALCARECO task (alcareco/skim) run on output of reco (ReReco) task
        # how to taken from PromptSkim workload implementation
        alcaRecoTask = genSimRecoMergeTask.addTask("ALCARECO")
        # "cmsRun1" is correct
        # genSimRecoMergeTask.listAllStepNames() -> ['cmsRun1', 'stageOut1', 'logArch1']
        parentCmsswStep = genSimRecoMergeTask.getStep("cmsRun1")
        alcaRecoOutputMods = self.setupProcessingTask(alcaRecoTask, "Processing",
                                                  inputStep = parentCmsswStep,
                                                  inputModule = "Merged",
                                                  couchURL = self.couchURL,
                                                  couchDBName = self.couchDBName,
                                                  configDoc = self.alcaRecoConfig,
                                                  splitAlgo = self.alcaRecoJobSplitAlgo,
                                                  splitArgs = self.alcaRecoJobSplitArgs)
        self.addLogCollectTask(recoTask, "AlcaRecoLogCollect")
                
        # 6 merge task for alcareco (skim) task
        for outputModuleName in alcaRecoOutputMods.keys():
            outputModuleInfo = alcaRecoOutputMods[outputModuleName]
            task = self.addMergeTask(alcaRecoTask, self.alcaRecoJobSplitAlgo,
                                     outputModuleName,
                                     outputModuleInfo["dataTier"],
                                     outputModuleInfo["filterName"],
                                     outputModuleInfo["processedDataset"])
        
        return workload
        
    
    def __call__(self, workloadName, arguments):
        StdBase.__call__(self, workloadName, arguments)
    
        # required parameters that must be specified by the
        # Requestor (ReqMgr)
        
        # configuration values related to Generation (step / task)        
        self.genConfig = arguments["GenConfigCacheID"]        
        # from MonteCarlo implementation
        self.inputPrimaryDataset = arguments["PrimaryDataset"]
        self.frameworkVersion = arguments["CMSSWVersion"]
        self.globalTag = arguments["GlobalTag"]
        self.totalEvents = arguments["RequestSizeEvents"]
        self.seeding = arguments.get("Seeding", "AutomaticSeeding")
        # The CouchURL and name of the ConfigCache database must be passed in
        # by the ReqMgr or whatever is creating this workflow.
        self.couchURL = arguments["CouchURL"]
        self.couchDBName = arguments["CouchDBName"]
        # Optional arguments that default to something reasonable.
        self.dbsUrl = arguments.get("DbsUrl", "http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet")
        self.emulation = arguments.get("Emulation", False)
        # job splitting parameters (information for WorkQueue) for the
        # production / generation task
        # These are mostly place holders because the job splitting algo and
        # parameters will be updated after the workflow has been created.
        self.genJobSplitAlgo = arguments.get("GenJobSplitAlgo", "EventBased")
        self.genJobSplitArgs = arguments.get("GenJobSplitArgs",
                                               {"events_per_job": 1000})
        # generation task data tier name is configurable and may be set by the user
        self.genDataTier = arguments.get("GenDataTier", None)
        # pileup configuration for the first generation task
        self.pileupConfig = arguments.get("PileupConfig", None)
        
        # reconstruction task
        self.recoConfig = arguments["RecoConfigCacheID"]        
        # job splitting parameters (information for WorkQueue) for the reco task
        self.recoJobSplitAlgo = arguments.get("RecoJobSplitAlgo", "FileBased")
        self.recoJobSplitArgs = arguments.get("RecoJobSplitArgs",
                                              {"files_per_job": 1})
                
        # alcareco (skim) task
        self.alcaRecoConfig = arguments["AlcaRecoConfigCacheID"]
        # job splitting parameters (information for WorkQueue) for the
        # alcareco task
        self.alcaRecoJobSplitAlgo = arguments.get("AlcaRecoJobSplitAlgo", "FileBased")
        self.alcaRecoJobSplitArgs = arguments.get("AlcaRecoJobSplitArgs",
                                                  {"files_per_job": 1})        
        
        return self.buildWorkload()
    
    

def relValMCWorkload(workloadName, arguments):
    """
    Instantiate the RelValMCWorkloadFactory and have it generate
    a workload for the given parameters.
    
    """
    myRelValMCFactory = RelValMCWorkloadFactory()
    instance = myRelValMCFactory(workloadName, arguments)
    return instance