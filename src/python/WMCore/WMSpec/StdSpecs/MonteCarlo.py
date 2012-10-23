#!/usr/bin/env python
"""
_MonteCarlo_

Created by Dave Evans on 2010-08-17.
Copyright (c) 2010 Fermilab. All rights reserved.

Support for pile up:
normally, the generation task has no input. However, if there is a
pile up section defined in the configuration, the generation task
fetches from DBS the information about pileup input.

"""

import os
import math

from WMCore.WMSpec.StdSpecs.StdBase import StdBase

def getTestArguments():
    """
    _getTestArguments_

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

    args["FirstLumi"] = 1
    args["FirstEvent"] = 1

    args["CMSSWVersion"] = "CMSSW_3_8_1"
    args["ProcConfigCacheID"] = "f90fc973b731a37c531f6e60e6c57955"
    args["TimePerEvent"] = 60
    args["FilterEfficiency"] = 1.0
    args["TotalTime"] = 9 * 3600
    args['DashboardHost'] = "127.0.0.1"
    args['DashboardPort'] = 8884
    return args


class MonteCarloWorkloadFactory(StdBase):
    """
    _MonteCarloWorkloadFactory_

    Stamp out Monte Carlo workflows.

    """

    def __init__(self):
        StdBase.__init__(self)


    def buildWorkload(self):
        """
        _buildWorkload_

        Build a workflow for a MonteCarlo request.  This means a production
        config and merge tasks for each output module.

        """
        workload = self.createWorkload()
        workload.setDashboardActivity("production")
        self.reportWorkflowToDashboard(workload.getDashboardActivity())
        workload.setWorkQueueSplitPolicy("MonteCarlo", self.prodJobSplitAlgo, self.prodJobSplitArgs)
        workload.setEndPolicy("SingleShot")
        prodTask = workload.newTask("Production")

        outputMods = self.setupProcessingTask(prodTask, "Production", None,
                                              couchURL = self.couchURL, couchDBName = self.couchDBName,
                                              configDoc = self.prodConfigCacheID, splitAlgo = self.prodJobSplitAlgo,
                                              splitArgs = self.prodJobSplitArgs,
                                              seeding = self.seeding, totalEvents = self.totalEvents,
                                              eventsPerLumi = self.eventsPerLumi)
        self.addLogCollectTask(prodTask)

        # pile up support
        if self.pileupConfig:
            self.setupPileup(prodTask, self.pileupConfig)

        prodMergeTasks = {}
        for outputModuleName in outputMods.keys():
            outputModuleInfo = outputMods[outputModuleName]
            self.addMergeTask(prodTask, self.prodJobSplitAlgo,
                              outputModuleName, lfn_counter = self.previousJobCount)

        return workload


    def __call__(self, workloadName, arguments):
        """
        Create a workload instance for a MonteCarlo request

        """
        StdBase.__call__(self, workloadName, arguments)

        # Required parameters that must be specified by the Requestor.
        self.inputPrimaryDataset = arguments["PrimaryDataset"]
        self.frameworkVersion    = arguments["CMSSWVersion"]
        self.globalTag           = arguments["GlobalTag"]
        self.seeding             = arguments.get("Seeding", "AutomaticSeeding")
        self.prodConfigCacheID   = arguments["ProcConfigCacheID"]

        # Splitting arguments
        timePerEvent     = int(arguments.get("TimePerEvent", 60))
        filterEfficiency = float(arguments.get("FilterEfficiency", 1.0))
        totalTime        = int(arguments.get("TotalTime", 9 * 3600))
        self.totalEvents = int(int(arguments["RequestNumEvents"]) / filterEfficiency)
        self.firstEvent  = int(arguments.get("FirstEvent", 1))
        self.firstLumi   = int(arguments.get("FirstLumi", 1))

        # pileup configuration for the first generation task
        self.pileupConfig = arguments.get("PileupConfig", None)

        #Events per lumi configuration (Allow others to inherit)
        self.eventsPerLumi = arguments.get("EventsPerLumi", None)
        if self.eventsPerLumi != None:
            self.eventsPerLumi = int(self.eventsPerLumi)

        # The CouchURL and name of the ConfigCache database must be passed in
        # by the ReqMgr or whatever is creating this workflow.
        self.couchURL = arguments["CouchURL"]
        self.couchDBName = arguments["CouchDBName"]

        # Optional arguments that default to something reasonable.
        self.dbsUrl = arguments.get("DbsUrl", "http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet")
        self.emulation = arguments.get("Emulation", False)

        # These are mostly place holders because the job splitting algo and
        # parameters will be updated after the workflow has been created.
        eventsPerJob = int(totalTime/timePerEvent/filterEfficiency)
        self.prodJobSplitAlgo  = arguments.get("ProdJobSplitAlgo", "EventBased")
        self.prodJobSplitArgs  = arguments.get("ProdJobSplitArgs",
                                               {"events_per_job": eventsPerJob})
        self.previousJobCount  = 0
        if self.firstEvent > 1 or self.firstLumi > 1:
            self.previousJobCount = int(math.ceil(self.firstEvent/float(self.prodJobSplitArgs["events_per_job"])))
            self.prodJobSplitArgs["initial_lfn_counter"] = self.previousJobCount
        
        return self.buildWorkload()

    def validateSchema(self, schema):
        """
        _validateSchema_

        Check for required fields, and some skim facts
        """
        arguments = getTestArguments()
        requiredFields = ["CMSSWVersion", "ProcConfigCacheID",
                          "PrimaryDataset", "CouchURL",
                          "CouchDBName", "RequestNumEvents",
                          "GlobalTag", "ScramArch",
                          "FirstEvent", "FirstLumi"]
        self.requireValidateFields(fields = requiredFields, schema = schema,
                                   validate = False)
        outMod = self.validateConfigCacheExists(configID = schema["ProcConfigCacheID"],
                                                couchURL = schema["CouchURL"],
                                                couchDBName = schema["CouchDBName"],
                                                getOutputModules = True)

        if schema.get("ProdJobSplitAlgo", "EventBased") == "EventBased":
            self.validateEventBasedParameters(schema = schema)

        return

    def validateEventBasedParameters(self, schema):
        """
        _validateEventBasedParameters_

        Validate the EventBased splitting job parameters
        """
        # First, see if they passed stuff in
        if schema.get("ProdJobSplitArgs", None):
            if not schema["ProdJobSplitArgs"].has_key("events_per_job"):
                msg = "Workflow submitted with invalid ProdJobSplitArgs to match SplitAlgo"
                self.raiseValidationException(msg = msg)
            if not int(schema["ProdJobSplitArgs"]['events_per_job']) > 0:
                msg = "Invalid number of events_per_job entered by user"
                self.raiseValidationException(msg = msg)
        else:
            # Get the default arguments
            timePerEvent     = int(schema.get("TimePerEvent", 60))
            filterEfficiency = float(schema.get("FilterEfficiency", 1.0))
            totalTime        = int(schema.get("TotalTime", 9 * 3600))

            if not totalTime > 0:
                self.raiseValidationException(msg = "Negative total time for MC workflow")
            if not filterEfficiency > 0.0:
                self.raiseValidationException(msg = "Negative filter efficiency for MC workflow")
            if not timePerEvent > 0:
                self.raiseValidationException(msg = "Negative time per event for MC workflow")
            if not int(totalTime/timePerEvent/filterEfficiency) > 0:
                self.raiseValidationException(msg = "No events created in MC workflow")

        return



def monteCarloWorkload(workloadName, arguments):
    """
    _monteCarloWorkload_

    Instantiate the MonteCarloWorkflowFactory and have it generate a workload for
    the given parameters.

    """
    myMonteCarloFactory = MonteCarloWorkloadFactory()
    return myMonteCarloFactory(workloadName, arguments)
