#!/usr/bin/env python
"""
_ReReco_

Standard ReReco workflow.
"""

import os
import WMCore.Lexicon

from WMCore.WMSpec.StdSpecs.DataProcessing import DataProcessingWorkloadFactory
from WMCore.WMSpec.StdSpecs.StdBase        import WMSpecFactoryException

def getTestArguments():
    """
    _getTestArguments_

    This should be where the default REQUIRED arguments go
    This serves as documentation for what is currently required
    by the standard ReReco workload in importable format.

    NOTE: These are test values.  If used in real workflows they
    will cause everything to crash/die/break, and we will be forced
    to hunt you down and kill you.
    """
    arguments = {
        "AcquisitionEra": "WMAgentCommissioning10",
        "Requestor": "sfoulkes@fnal.gov",
        "InputDataset": "/MinimumBias/Commissioning10-v4/RAW",
        "CMSSWVersion": "CMSSW_3_9_7",
        "ScramArch": "slc5_ia32_gcc434",
        "ProcessingVersion": "2",
        "SkimInput": "output",
        "GlobalTag": "GR10_P_v4::All",

        "CouchURL": os.environ.get("COUCHURL", None),
        "CouchDBName": "scf_wmagent_configcache",

        "ProcScenario": "cosmics",
        "DashboardHost": "127.0.0.1",
        "DashboardPort": 8884,
        "TimePerEvent" : 1,
        "Memory"       : 1,
        "SizePerEvent" : 1,
        }

    return arguments

class ReRecoWorkloadFactory(DataProcessingWorkloadFactory):
    """
    _ReRecoWorkloadFactory_

    Stamp out ReReco workflows.
    """
    def __init__(self):
        DataProcessingWorkloadFactory.__init__(self)
        return

    def addSkims(self, workload):
        """
        _addSkims_

        Add skims to the standard dataprocessing workload that was given.

        Note that there will be LogCollect tasks created for each processing
        task and Cleanup tasks created for each merge task.
        """
        procMergeTasks = {}
        procTask = workload.getTopLevelTask()[0]
        for mergeTask in procTask.childTaskIterator():
            if mergeTask.taskType() == "Merge":
                procMergeTasks[mergeTask.data.input.outputModule] = mergeTask

        for skimConfig in self.skimConfigs:
            if not procMergeTasks.has_key(skimConfig["SkimInput"]):
                # This is an extremely rare case - we have to wait until the entire system is built to get to this point
                # But if we do get here we need to raise a Validation exception, which is normally only raised in the validate
                # steps.  This is a once in a lifetime thing - don't go raising validationExceptions in the rest of the code.
                error = "Processing config does not have the following output module: %s.  " % skimConfig["SkimInput"]
                error += "Please change your skim input to be one of the following: %s" % procMergeTasks.keys()
                self.raiseValidationException(msg = error)


            mergeTask = procMergeTasks[skimConfig["SkimInput"]]
            skimTask = mergeTask.addTask(skimConfig["SkimName"])
            parentCmsswStep = mergeTask.getStep("cmsRun1")
            outputMods = self.setupProcessingTask(skimTask, "Skim", inputStep = parentCmsswStep, inputModule = "Merged",
                                                  couchURL = self.couchURL, couchDBName = self.couchDBName,
                                                  configDoc = skimConfig["ConfigCacheID"], splitAlgo = self.skimJobSplitAlgo,
                                                  splitArgs = self.skimJobSplitArgs)
            self.addLogCollectTask(skimTask, taskName = "%sLogCollect" % skimConfig["SkimName"])

            for outputModuleName in outputMods.keys():
                outputModuleInfo = outputMods[outputModuleName]
                self.addMergeTask(skimTask, self.skimJobSplitAlgo,
                                  outputModuleName)

        return workload

    def __call__(self, workloadName, arguments):
        """
        _call_

        Create a ReReco workload with the given parameters.
        """
        workload = DataProcessingWorkloadFactory.__call__(self, workloadName, arguments)

        # The SkimConfig parameter must be a list of dictionaries where each
        # dictionary will have the following keys:
        #  SkimName
        #  SkimInput
        #  SkimSplitAlgo - Optional at workflow creation time
        #  SkimSplitParams - Optional at workflow creation time
        #  ConfigCacheID
        #  Scenario
        #
        # The ConfigCacheID and Scenaio are mutually exclusive, only one can be
        # set.  The SkimSplitAlgo and SkimSplitParams don't have to be set when
        # the workflow is created but must be set when the workflow is approved.
        # The SkimInput is the name of the output module in the  processing step
        # that will be used as the input for the skim.
        self.skimConfigs = arguments.get("SkimConfigs", [])

        # These are mostly place holders because the job splitting algo and
        # parameters will be updated after the workflow has been created.
        self.skimJobSplitAlgo = arguments.get("SkimJobSplitAlgo", "FileBased")
        self.skimJobSplitArgs = arguments.get("SkimJobSplitArgs",
                                              {"files_per_job": 1,
                                               "include_parents": True})

        return self.addSkims(workload)

    def validateSchema(self, schema):
        """
        _validateSchema_

        Check for required fields, and some skim facts
        """
        requiredFields = ["CMSSWVersion", "ScramArch",
                          "GlobalTag", "InputDataset"]
        self.requireValidateFields(fields = requiredFields, schema = schema,
                                   validate = False)

        if schema.get('ConfigCacheID', None) and schema.get('CouchURL', None) and schema.get('CouchDBName', None):
            outMod = self.validateConfigCacheExists(configID = schema['ConfigCacheID'],
                                                    couchURL = schema["CouchURL"],
                                                    couchDBName = schema["CouchDBName"],
                                                    getOutputModules = True)
        elif not schema.get('ProcScenario', None):
            self.raiseValidationException(msg = "No Scenario or Config in Processing Request!")

        try:
            WMCore.Lexicon.dataset(schema.get('InputDataset', ''))
        except AssertionError:
            self.raiseValidationException(msg = "Invalid input dataset!")

        return


def rerecoWorkload(workloadName, arguments):
    """
    _rerecoWorkload_

    Instantiate the ReRecoWorkflowFactory and have it generate a workload for
    the given parameters.
    """
    myReRecoFactory = ReRecoWorkloadFactory()
    return myReRecoFactory(workloadName, arguments)
