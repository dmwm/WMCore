#!/usr/bin/env python
"""
_ReReco_

Standard ReReco workflow.
"""
from __future__ import division

import json

from future.utils import viewitems

from Utils.Utilities import makeList
from WMCore.WMSpec.StdSpecs.DataProcessing import DataProcessing
from WMCore.WMSpec.WMWorkloadTools import validateArgumentsCreate


class ReRecoWorkloadFactory(DataProcessing):
    """
    _ReRecoWorkloadFactory_

    Stamp out ReReco workflows.
    """

    def buildWorkload(self):
        """
        _buildWorkload_

        Build the workload given all of the input parameters.  At the very least
        this will create a processing task and merge tasks for all the outputs
        of the processing task.
        Note that there will be LogCollect tasks created for each processing
        task and Cleanup tasks created for each merge task.
        """
        (self.inputPrimaryDataset, self.inputProcessedDataset,
         self.inputDataTier) = self.inputDataset[1:].split("/")

        workload = self.createWorkload()
        workload.setDashboardActivity("reprocessing")
        workload.setWorkQueueSplitPolicy("Block", self.procJobSplitAlgo,
                                         self.procJobSplitArgs,
                                         OpenRunningTimeout=self.openRunningTimeout)
        procTask = workload.newTask("DataProcessing")

        cmsswStepType = "CMSSW"
        taskType = "Processing"

        forceUnmerged = False
        if self.transientModules:
            # If we have at least one output module not being merged,
            # we must force all the processing task to be unmerged
            forceUnmerged = True

        outputMods = self.setupProcessingTask(procTask, taskType,
                                              self.inputDataset,
                                              couchDBName=self.couchDBName,
                                              configCacheUrl=self.configCacheUrl,
                                              forceUnmerged=forceUnmerged,
                                              configDoc=self.configCacheID,
                                              splitAlgo=self.procJobSplitAlgo,
                                              splitArgs=self.procJobSplitArgs,
                                              stepType=cmsswStepType)
        self.addLogCollectTask(procTask)

        # no real need to sort it, but we better have the same order between Py2/Py3
        for outputModuleName in sorted(list(outputMods)):
            # Only merge the desired outputs
            if outputModuleName not in self.transientModules:
                self.addMergeTask(procTask, self.procJobSplitAlgo, outputModuleName)
            else:
                self.addCleanupTask(procTask, outputModuleName)

        self.addSkims(workload)
        # setting the parameters which need to be set for all the tasks
        # sets acquisitionEra, processingVersion, processingString
        workload.setTaskPropertiesFromWorkload()

        # set the LFN bases (normally done by request manager)
        # also pass runNumber (workload evaluates it)
        workload.setLFNBase(self.mergedLFNBase, self.unmergedLFNBase,
                            runNumber=self.runNumber)

        return workload

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
            skimmableTask = skimmableTasks[skimConfig["SkimInput"]]
            skimTask = skimmableTask.addTask(skimConfig["SkimName"])
            parentCmsswStep = skimmableTask.getStep("cmsRun1")

            skimSizePerEvent = skimConfig["SizePerEvent"]
            skimTimePerEvent = skimConfig["TimePerEvent"]
            skimMemory = skimConfig["Memory"]

            # Check that the splitting agrees, if the parent is event based then we must do WMBSMergeBySize
            # With reasonable defaults
            skimJobSplitAlgo = skimConfig["SkimJobSplitAlgo"]
            skimJobSplitArgs = skimConfig["SkimJobSplitArgs"]
            if skimmableTask.jobSplittingAlgorithm == "EventBased":
                skimJobSplitAlgo = "WMBSMergeBySize"
                skimJobSplitArgs = {"max_merge_size": self.maxMergeSize,
                                    "min_merge_size": self.minMergeSize,
                                    "max_merge_events": self.maxMergeEvents,
                                    "max_wait_time": self.maxWaitTime}

            # Define the input module
            inputModule = "Merged"
            if skimConfig["SkimInput"] in self.transientModules:
                inputModule = skimConfig["SkimInput"]

            outputMods = self.setupProcessingTask(skimTask, "Skim",
                                                  inputStep=parentCmsswStep,
                                                  inputModule=inputModule,
                                                  couchDBName=self.couchDBName,
                                                  configCacheUrl=self.configCacheUrl,
                                                  configDoc=skimConfig["ConfigCacheID"],
                                                  splitAlgo=skimJobSplitAlgo,
                                                  splitArgs=skimJobSplitArgs,
                                                  timePerEvent=skimTimePerEvent,
                                                  sizePerEvent=skimSizePerEvent,
                                                  memoryReq=skimMemory)

            self.addLogCollectTask(skimTask, taskName="%sLogCollect" % skimConfig["SkimName"])

            for outputModuleName in outputMods:
                self.addMergeTask(skimTask, skimJobSplitAlgo, outputModuleName)

        return

    def __call__(self, workloadName, arguments):
        """
        _call_

        Create a ReReco workload with the given parameters.
        """
        DataProcessing.__call__(self, workloadName, arguments)

        # Arrange the skims in a skimConfig object (i.e. a list of skim configurations)
        self.skimConfigs = []
        skimIndex = 1
        while "SkimName%s" % skimIndex in arguments:
            skimConfig = {}
            skimConfig["SkimName"] = arguments["SkimName%s" % skimIndex]
            skimConfig["SkimInput"] = arguments["SkimInput%s" % skimIndex]
            skimConfig["ConfigCacheID"] = arguments["Skim%sConfigCacheID" % skimIndex]
            skimConfig["TimePerEvent"] = float(arguments.get("SkimTimePerEvent%s" % skimIndex, self.timePerEvent))
            skimConfig["SizePerEvent"] = float(arguments.get("SkimSizePerEvent%s" % skimIndex, self.sizePerEvent))
            skimConfig["Memory"] = float(arguments.get("SkimMemory%s" % skimIndex, self.memory))
            skimConfig["SkimJobSplitAlgo"] = arguments.get("SkimSplittingAlgo%s" % skimIndex, "FileBased")
            skimConfig["SkimJobSplitArgs"] = {"include_parents": True}
            if skimConfig["SkimJobSplitAlgo"] == "FileBased":
                skimConfig["SkimJobSplitArgs"]["files_per_job"] = int(arguments.get("SkimFilesPerJob%s" % skimIndex, 1))
            elif skimConfig["SkimJobSplitAlgo"] in ["EventBased", "EventAwareLumiBased"]:
                standardSkim = int((8.0 * 3600.0) / skimConfig["TimePerEvent"])
                skimConfig["SkimJobSplitArgs"]["events_per_job"] = int(arguments.get("SkimEventsPerJob%s" % skimIndex, standardSkim))
                if skimConfig["SkimJobSplitAlgo"] == "EventAwareLumiBased":
                    skimConfig["SkimJobSplitAlgo"]["job_time_limit"] = 48 * 3600  # 2 days
            elif skimConfig["SkimJobSplitAlgo"] == "LumiBased":
                skimConfig["SkimJobSplitArgs"]["lumis_per_job"] = int(arguments.get("SkimLumisPerJob%s" % skimIndex, 8))
            self.skimConfigs.append(skimConfig)
            skimIndex += 1

        return self.buildWorkload()

    @staticmethod
    def getWorkloadCreateArgs():

        baseArgs = DataProcessing.getWorkloadCreateArgs()
        specArgs = {"RequestType": {"default": "ReReco", "optional": False},
                    "TransientOutputModules": {"default": [], "type": makeList,
                                               "attr": "transientModules", "null": False}
                    }
        baseArgs.update(specArgs)
        DataProcessing.setDefaultArgumentsProperty(baseArgs)
        return baseArgs

    @staticmethod
    def getSkimArguments():
        """
        _getSkimArguments_

        Skim arguments can be many of the same, it depends on the number
        of defined skims. However we need to keep the same definition of its arguments
        in a generic form. This method follows the same definition of getWorkloadCreateArgs in StdBase.
        """
        skimArgs = {
            "SkimName#N": {"default": None, "type": str,
                           "optional": False, "validate": None,
                           "null": False},
            "SkimInput#N": {"default": None, "type": str,
                            "optional": False, "validate": None,
                            "null": False},
            "Skim#NConfigCacheID": {"default": None, "type": str,
                                    "optional": False, "validate": None,
                                    "null": False},
            "SkimTimePerEvent#N": {"default": None, "type": float,
                                   "optional": True, "validate": lambda x: x > 0,
                                   "null": False},
            "SkimSizePerEvent#N": {"default": None, "type": float,
                                   "optional": True, "validate": lambda x: x > 0,
                                   "null": False},
            "SkimMemory#N": {"default": None, "type": float,
                             "optional": True, "validate": lambda x: x > 0,
                             "null": False},
            "SkimSplittingAlgo#N": {"default": None, "type": str,
                                    "optional": True, "validate": None,
                                    "null": False},
            "SkimEventsPerJob#N": {"default": None, "type": int,
                                   "optional": True, "validate": lambda x: x > 0,
                                   "null": False},
            "SkimLumisPerJob#N": {"default": 8, "type": int,
                                  "optional": True, "validate": lambda x: x > 0,
                                  "null": False},
            "SkimFilesPerJob#N": {"default": 1, "type": int,
                                  "optional": True, "validate": lambda x: x > 0,
                                  "null": False}}
        return skimArgs

    def validateSchema(self, schema):
        """
        _validateSchema_

        Check for required fields, and some skim facts
        """
        DataProcessing.validateSchema(self, schema)
        mainOutputModules = list(self.validateConfigCacheExists(configID=schema["ConfigCacheID"],
                                                           configCacheUrl=schema['ConfigCacheUrl'],
                                                           couchDBName=schema["CouchDBName"],
                                                           getOutputModules=True))

        # Skim facts have to be validated outside the usual master validation
        skimSchema = {k: v for (k, v) in viewitems(schema) if k.startswith("Skim")}
        skimArguments = self.getSkimArguments()
        skimIndex = 1
        skimInputs = set()
        while "SkimName%s" % skimIndex in schema:
            instanceArguments = {}
            for argument in skimArguments:
                realArg = argument.replace("#N", str(skimIndex))
                instanceArguments[realArg] = skimArguments[argument]
            try:
                validateArgumentsCreate(skimSchema, instanceArguments)
                # Validate GPU-related spec parameters
                DataProcessing.validateGPUSettings(schema)
            except Exception as ex:
                self.raiseValidationException(str(ex))

            self.validateConfigCacheExists(configID=schema["Skim%sConfigCacheID" % skimIndex],
                                           configCacheUrl=schema['ConfigCacheUrl'],
                                           couchDBName=schema["CouchDBName"],
                                           getOutputModules=False)
            if schema["SkimInput%s" % skimIndex] not in mainOutputModules:
                error = "Processing config does not have the following output module: %s." % schema[
                    "SkimInput%s" % skimIndex]
                self.raiseValidationException(msg=error)
            skimInputs.add(schema["SkimInput%s" % skimIndex])
            skimIndex += 1

        # Validate that the transient output modules are used in a skim task
        if "TransientOutputModules" in schema:
            diffSet = set(schema["TransientOutputModules"]) - skimInputs
            if diffSet:
                self.raiseValidationException(
                    msg="A transient output module was specified but no skim was defined for it")
