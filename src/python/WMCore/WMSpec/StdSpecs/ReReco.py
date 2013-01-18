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
        # or alternatively CouchURL part can be replaced by ConfigCacheUrl,
        # then ConfigCacheUrl + CouchDBName + ConfigCacheID
        "ConfigCacheUrl": None,

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
        skimmableTasks = {}
        procTask = workload.getTopLevelTask()[0]
        for skimmableTask in procTask.childTaskIterator():
            if skimmableTask.taskType() == "Merge":
                skimmableTasks[skimmableTask.data.input.outputModule] = skimmableTask
        # Now add the output modules that are not merged but may be skimmed
        for outputModule in self.transientModules:
            skimmableTasks[outputModule] = procTask


        for skimConfig in self.skimConfigs:
            if skimConfig["SkimInput"] not in skimmableTasks:
                # This is an extremely rare case - we have to wait until the entire system is built to get to this point
                # But if we do get here we need to raise a Validation exception, which is normally only raised in the validate
                # steps.  This is a once in a lifetime thing - don't go raising validationExceptions in the rest of the code.
                error = "Processing config does not have the following output module: %s.  " % skimConfig["SkimInput"]
                error += "Please change your skim input to be one of the following: %s" % skimmableTasks.keys()
                self.raiseValidationException(msg = error)


            skimmableTask = skimmableTasks[skimConfig["SkimInput"]]
            skimTask = skimmableTask.addTask(skimConfig["SkimName"])
            parentCmsswStep = skimmableTask.getStep("cmsRun1")

            # Check that the splitting agrees, if the parent is event based then we must do WMBSMergeBySize
            # With reasonable defaults
            skimJobSplitAlgo = self.skimJobSplitAlgo
            skimJobSplitArgs = self.skimJobSplitArgs
            if skimmableTask.jobSplittingAlgorithm == "EventBased":
                skimJobSplitAlgo = "WMBSMergeBySize"
                skimJobSplitArgs = {"max_merge_size"   : self.maxMergeSize,
                                    "min_merge_size"   : self.minMergeSize,
                                    "max_merge_events" : self.maxMergeEvents,
                                    "max_wait_time"    : self.maxWaitTime}
            # Define the input module
            inputModule = "Merged"
            if skimConfig["SkimInput"] in self.transientModules:
                inputModule = skimConfig["SkimInput"]

            outputMods = self.setupProcessingTask(skimTask, "Skim", inputStep = parentCmsswStep, inputModule = inputModule,
                                                  couchURL = self.couchURL, couchDBName = self.couchDBName,
                                                  configCacheUrl = self.configCacheUrl,
                                                  configDoc = skimConfig["ConfigCacheID"], splitAlgo = skimJobSplitAlgo,
                                                  splitArgs = skimJobSplitArgs)
            self.addLogCollectTask(skimTask, taskName = "%sLogCollect" % skimConfig["SkimName"])

            for outputModuleName in outputMods.keys():
                self.addMergeTask(skimTask, skimJobSplitAlgo,
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
        # TODO
        # this list of required arguments is most likely incomplete, obsolete ...
        requiredFields = ["CMSSWVersion", "ScramArch",
                          "GlobalTag", "InputDataset"]
        self.requireValidateFields(fields = requiredFields, schema = schema,
                                   validate = False)

        if schema.get('ConfigCacheID', None) and schema.get('CouchURL', None) and schema.get('CouchDBName', None):
            couchUrl = schema.get("ConfigCacheUrl", None) or schema["CouchURL"]
            outMod = self.validateConfigCacheExists(configID = schema['ConfigCacheID'],
                                                    couchURL = couchUrl,
                                                    couchDBName = schema["CouchDBName"],
                                                    getOutputModules = True)
        # TODO
        # ProcScenario is now in request arguments Scenario, however this change
        # didn't quite make it in the workflows implementation (#4280) 
        elif not schema.get('ProcScenario', None):
            self.raiseValidationException(msg = "No Scenario or Config in Processing Request!")

        try:
            WMCore.Lexicon.dataset(schema.get('InputDataset', ''))
        except AssertionError:
            self.raiseValidationException(msg = "Invalid input dataset!")

        # Validate that the transient output modules are used in a skim task
        if 'TransientOutputModules' in schema:
            for outMod in schema['TransientOutputModules']:
                for skimConfig in schema.get('SkimConfigs', []):
                    if outMod == skimConfig['SkimInput']:
                        break
                else:
                    self.raiseValidationException(msg = 'A transient output module was specified but no skim was defined for it')

def rerecoWorkload(workloadName, arguments):
    """
    _rerecoWorkload_

    Instantiate the ReRecoWorkflowFactory and have it generate a workload for
    the given parameters.
    """
    myReRecoFactory = ReRecoWorkloadFactory()
    return myReRecoFactory(workloadName, arguments)
