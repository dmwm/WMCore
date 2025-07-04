"""
_Express_

Express workflow

express processing -> FEVT/RAW/RECO/whatever -> express merge
                      (supports multiple output with different primary datasets)
                   -> ALCARECO -> alca skimming / merging -> ALCARECO
                                                          -> ALCAPROMPT -> end of run alca harvesting -> sqlite -> dropbox upload
                   -> DQM -> merge -> DQM -> periodic dqm harvesting
                                          -> end of run dqm harvesting
"""

from __future__ import division

from future.utils import viewitems

import WMCore.WMSpec.Steps.StepFactory as StepFactory
from Utils.Utilities import makeList, makeNonEmptyList
from WMCore.Lexicon import procstringT0
from WMCore.ReqMgr.Tools.cms import releases, architectures
from WMCore.WMSpec.StdSpecs.StdBase import StdBase


class ExpressWorkloadFactory(StdBase):
    """
    _ExpressWorkloadFactory_

    Stamp out Express workflows.
    """

    def __init__(self):
        StdBase.__init__(self)

        self.inputPrimaryDataset = None
        self.inputProcessedDataset = None

        return

    def buildWorkload(self):
        """
        _buildWorkload_

        Build the workload given all of the input parameters.

        Not that there will be LogCollect tasks created for each processing
        task and Cleanup tasks created for each merge task.

        """
        workload = self.createWorkload()
        workload.setDashboardActivity("t0")

        cmsswStepType = "CMSSW"
        taskType = "Processing"

        # complete output configuration
        for output in self.outputs:
            output['moduleLabel'] = "write_%s_%s" % (output['primaryDataset'],
                                                     output['dataTier'])

        # finalize splitting parameters
        mySplitArgs = self.expressSplitArgs.copy()
        mySplitArgs['algo_package'] = "T0.JobSplitting"

        expressTask = workload.newTask("Express")

        #
        # need to split this up into two separate code paths
        # one is direct reco from the streamer files
        # the other is conversion and then reco
        #
        if self.recoFrameworkVersion is None or self.recoFrameworkVersion == self.frameworkVersion:

            expressRecoStepName = "cmsRun1"

            scenarioArgs = {'globalTag': self.globalTag,
                            'skims': self.alcaSkims,
                            'dqmSeq': self.dqmSequences,
                            'outputs': self.outputs,
                            'inputSource': "DAT"}

            if self.globalTagConnect:
                scenarioArgs['globalTagConnect'] = self.globalTagConnect

            expressOutMods = self.setupProcessingTask(expressTask, taskType,
                                                      scenarioName=self.procScenario,
                                                      scenarioFunc="expressProcessing",
                                                      scenarioArgs=scenarioArgs,
                                                      splitAlgo="Express",
                                                      splitArgs=mySplitArgs,
                                                      stepType=cmsswStepType,
                                                      forceUnmerged=True)
        else:

            expressRecoStepName = "cmsRun2"

            conversionOutMods = self.setupProcessingTask(expressTask, taskType,
                                                         scenarioName=self.procScenario,
                                                         scenarioFunc="repack",
                                                         scenarioArgs={'outputs': [{'dataTier': "RAW",
                                                                                    'eventContent': "ALL",
                                                                                    'primaryDataset': self.specialDataset,
                                                                                    'moduleLabel': "write_RAW"}]},
                                                         splitAlgo="Express",
                                                         splitArgs=mySplitArgs,
                                                         stepType=cmsswStepType,
                                                         forceUnmerged=True)

            # there is only one
            conversionOutLabel = next(iter(conversionOutMods))

            # everything coming after should use the reco CMSSW version and Scram Arch
            self.frameworkVersion = self.recoFrameworkVersion
            self.scramArch = self.recoScramArch

            # add a second step doing the reconstruction
            parentCmsswStep = expressTask.getStep("cmsRun1")
            parentCmsswStepHelper = parentCmsswStep.getTypeHelper()
            parentCmsswStepHelper.keepOutput(False)
            stepTwoCmssw = parentCmsswStep.addTopStep("cmsRun2")
            stepTwoCmssw.setStepType(cmsswStepType)

            template = StepFactory.getStepTemplate(cmsswStepType)
            template(stepTwoCmssw.data)

            stepTwoCmsswHelper = stepTwoCmssw.getTypeHelper()

            stepTwoCmsswHelper.setNumberOfCores(self.multicore)

            stepTwoCmsswHelper.setGlobalTag(self.globalTag)
            stepTwoCmsswHelper.setupChainedProcessing("cmsRun1", conversionOutLabel)
            stepTwoCmsswHelper.cmsswSetup(self.frameworkVersion, softwareEnvironment="",
                                          scramArch=self.scramArch)

            scenarioFunc = "expressProcessing"
            scenarioArgs = {'globalTag': self.globalTag,
                            'skims': self.alcaSkims,
                            'dqmSeq': self.dqmSequences,
                            'outputs': self.outputs,
                            'inputSource': "EDM"}

            if self.globalTagConnect:
                scenarioArgs['globalTagConnect'] = self.globalTagConnect

            configOutput = self.determineOutputModules(scenarioFunc, scenarioArgs)

            expressOutMods = {}
            for outputModuleName in configOutput:
                outputModule = self.addOutputModule(expressTask,
                                                    outputModuleName,
                                                    configOutput[outputModuleName]['primaryDataset'],
                                                    configOutput[outputModuleName]['dataTier'],
                                                    configOutput[outputModuleName].get('filterName', None),
                                                    stepName="cmsRun2", forceUnmerged=True)
                expressOutMods[outputModuleName] = outputModule

            if 'outputs' in scenarioArgs:
                for output in scenarioArgs['outputs']:
                    if 'primaryDataset' in output:
                        del output['primaryDataset']
            if 'primaryDataset' in scenarioArgs:
                del scenarioArgs['primaryDataset']

            stepTwoCmsswHelper.setDataProcessingConfig(self.procScenario,
                                                       scenarioFunc,
                                                       **scenarioArgs)

        expressTask.setTaskType("Express")

        self.addLogCollectTask(expressTask, taskName="ExpressLogCollect")

        for expressOutLabel, expressOutInfo in viewitems(expressOutMods):

            if expressOutInfo['dataTier'] == "ALCARECO":

                # finalize splitting parameters
                mySplitArgs = self.expressMergeSplitArgs.copy()
                mySplitArgs['algo_package'] = "T0.JobSplitting"

                alcaSkimTask = expressTask.addTask("%sAlcaSkim%s" % (expressTask.name(), expressOutLabel))

                alcaSkimTask.setInputReference(expressTask.getStep(expressRecoStepName),
                                               outputModule=expressOutLabel,
                                               dataTier="ALCARECO")

                alcaTaskConf = {'Multicore': 1,
                                'EventStreams': 0}

                scenarioArgs = {'globalTag': self.globalTag,
                                'skims': self.alcaSkims,
                                'primaryDataset': self.specialDataset}

                if self.globalTagConnect:
                    scenarioArgs['globalTagConnect'] = self.globalTagConnect

                alcaSkimOutMods = self.setupProcessingTask(alcaSkimTask, taskType,
                                                           scenarioName=self.procScenario,
                                                           scenarioFunc="alcaSkim",
                                                           scenarioArgs=scenarioArgs,
                                                           splitAlgo="ExpressMerge",
                                                           splitArgs=mySplitArgs,
                                                           stepType=cmsswStepType,
                                                           forceMerged=True,
                                                           taskConf=alcaTaskConf)

                alcaSkimTask.setTaskType("Express")

                self.addLogCollectTask(alcaSkimTask, taskName="AlcaSkimLogCollect")
                self.addCleanupTask(expressTask, expressOutLabel, dataTier=expressOutInfo['dataTier'])

                for alcaSkimOutLabel, alcaSkimOutInfo in viewitems(alcaSkimOutMods):

                    if alcaSkimOutInfo['dataTier'] == "ALCAPROMPT" and \
                            (self.alcaHarvestCondLFNBase is not None or self.alcaHarvestLumiURL is not None):
                        harvestTask = self.addAlcaHarvestTask(alcaSkimTask, alcaSkimOutLabel,
                                                              alcapromptdataset=alcaSkimOutInfo['filterName'],
                                                              condOutLabel=self.alcaHarvestOutLabel,
                                                              condLFNBase=self.alcaHarvestCondLFNBase,
                                                              lumiURL=self.alcaHarvestLumiURL,
                                                              uploadProxy=self.dqmUploadProxy,
                                                              doLogCollect=True)

                        self.addConditionTask(harvestTask, self.alcaHarvestOutLabel)

            else:

                mergeTask = self.addExpressMergeTask(expressTask, expressRecoStepName, expressOutLabel)

                if expressOutInfo['dataTier'] in ["DQM", "DQMIO"]:
                    self.addDQMHarvestTask(mergeTask, "Merged",
                                           uploadProxy=self.dqmUploadProxy,
                                           periodic_harvest_interval=self.periodicHarvestInterval,
                                           doLogCollect=True)

        # setting the parameters which need to be set for all the tasks
        # sets acquisitionEra, processingVersion, processingString
        workload.setTaskPropertiesFromWorkload()

        # set the LFN bases (normally done by request manager)
        # also pass run number to add run based directories
        workload.setLFNBase(self.mergedLFNBase, self.unmergedLFNBase,
                            runNumber=self.runNumber)

        return workload

    def addExpressMergeTask(self, parentTask, parentStepName, parentOutputModuleName):
        """
        _addExpressMergeTask_

        Create an expressmerge task for files produced by the parent task

        """
        # finalize splitting parameters
        mySplitArgs = self.expressMergeSplitArgs.copy()
        mySplitArgs['algo_package'] = "T0.JobSplitting"

        parentTaskCmssw = parentTask.getStep(parentStepName)
        parentOutputModule = parentTaskCmssw.getOutputModule(parentOutputModuleName)

        mergeTask = parentTask.addTask("%sMerge%s" % (parentTask.name(), parentOutputModuleName))

        dataTier = getattr(parentOutputModule, "dataTier")
        mergeTask.setInputReference(parentTaskCmssw, outputModule=parentOutputModuleName, dataTier=dataTier)

        self.addRuntimeMonitors(mergeTask)
        mergeTaskCmssw = mergeTask.makeStep("cmsRun1")
        mergeTaskCmssw.setStepType("CMSSW")

        mergeTaskStageOut = mergeTaskCmssw.addStep("stageOut1")
        mergeTaskStageOut.setStepType("StageOut")
        mergeTaskLogArch = mergeTaskCmssw.addStep("logArch1")
        mergeTaskLogArch.setStepType("LogArchive")

        mergeTask.setTaskLogBaseLFN(self.unmergedLFNBase)

        self.addLogCollectTask(mergeTask, taskName="%s%sMergeLogCollect" % (parentTask.name(), parentOutputModuleName))

        mergeTask.applyTemplates()

        mergeTaskCmsswHelper = mergeTaskCmssw.getTypeHelper()

        mergeTaskCmsswHelper.cmsswSetup(self.frameworkVersion, softwareEnvironment="",
                                        scramArch=self.scramArch)

        mergeTaskCmsswHelper.setErrorDestinationStep(stepName=mergeTaskLogArch.name())
        mergeTaskCmsswHelper.setGlobalTag(self.globalTag)
        mergeTaskCmsswHelper.setOverrideCatalog(self.overrideCatalog)

        # mergeTaskStageHelper = mergeTaskStageOut.getTypeHelper()
        # mergeTaskStageHelper.setMinMergeSize(0, 0)

        mergeTask.setTaskType("Merge")

        # DQM is handled differently
        #  merging does not increase size
        #                => disable size limits
        #  only harvest every 15 min
        #                => higher limits for latency (disabled for now)
        dataTier = getattr(parentOutputModule, "dataTier")
        if dataTier in ["DQM", "DQMIO"]:
            mySplitArgs['maxInputSize'] *= 100

        mergeTask.setSplittingAlgorithm("ExpressMerge",
                                        **mySplitArgs)
        mergeTaskCmsswHelper.setDataProcessingConfig(self.procScenario, "merge",
                                                     newDQMIO=(dataTier == "DQMIO"))

        self.addOutputModule(mergeTask, "Merged",
                             primaryDataset=getattr(parentOutputModule, "primaryDataset"),
                             dataTier=getattr(parentOutputModule, "dataTier"),
                             filterName=getattr(parentOutputModule, "filterName"),
                             forceMerged=True)

        self.addCleanupTask(parentTask, parentOutputModuleName, dataTier=getattr(parentOutputModule, "dataTier"))

        return mergeTask

    def addAlcaHarvestTask(self, parentTask, parentOutputModuleName,
                           alcapromptdataset, condOutLabel, condLFNBase, lumiURL,
                           uploadProxy, parentStepName="cmsRun1", doLogCollect=True):
        """
        _addAlcaHarvestTask_

        Create an Alca harvest task to harvest the files produces by the parent task.
        """
        # finalize splitting parameters
        mySplitArgs = {}
        mySplitArgs['algo_package'] = "T0.JobSplitting"
        mySplitArgs['runNumber'] = self.runNumber
        mySplitArgs['alcapromptdataset'] = alcapromptdataset
        mySplitArgs['timeout'] = self.alcaHarvestTimeout

        harvestTask = parentTask.addTask("%sAlcaHarvest%s" % (parentTask.name(), parentOutputModuleName))
        self.addRuntimeMonitors(harvestTask)
        harvestTaskCmssw = harvestTask.makeStep("cmsRun1")
        harvestTaskCmssw.setStepType("CMSSW")

        harvestTaskCondition = harvestTaskCmssw.addStep("condition1")
        harvestTaskCondition.setStepType("AlcaHarvest")
        harvestTaskUpload = harvestTaskCmssw.addStep("upload1")
        harvestTaskUpload.setStepType("DQMUpload")
        harvestTaskLogArch = harvestTaskCmssw.addStep("logArch1")
        harvestTaskLogArch.setStepType("LogArchive")

        harvestTask.setTaskLogBaseLFN(self.unmergedLFNBase)
        if doLogCollect:
            self.addLogCollectTask(harvestTask,
                                   taskName="%s%sAlcaHarvestLogCollect" % (parentTask.name(), parentOutputModuleName))

        harvestTask.setTaskType("Harvesting")
        harvestTask.applyTemplates()

        harvestTaskCmsswHelper = harvestTaskCmssw.getTypeHelper()
        harvestTaskCmsswHelper.cmsswSetup(self.frameworkVersion, softwareEnvironment="",
                                          scramArch=self.scramArch)

        harvestTaskCmsswHelper.setErrorDestinationStep(stepName=harvestTaskLogArch.name())
        harvestTaskCmsswHelper.setGlobalTag(self.globalTag)
        harvestTaskCmsswHelper.setOverrideCatalog(self.overrideCatalog)

        harvestTaskCmsswHelper.setUserLFNBase("/")

        parentTaskCmssw = parentTask.getStep(parentStepName)
        parentOutputModule = parentTaskCmssw.getOutputModule(parentOutputModuleName)

        dataTier = getattr(parentOutputModule, "dataTier")
        harvestTask.setInputReference(parentTaskCmssw, outputModule=parentOutputModuleName, dataTier=dataTier)

        harvestTask.setSplittingAlgorithm("AlcaHarvest",
                                          **mySplitArgs)

        scenarioArgs = {'globalTag': self.globalTag,
                        'datasetName': "/%s/%s/%s" % (getattr(parentOutputModule, "primaryDataset"),
                                                      getattr(parentOutputModule, "processedDataset"),
                                                      dataTier),
                        'runNumber': self.runNumber,
                        'alcapromptdataset': alcapromptdataset}

        if self.globalTagConnect:
            scenarioArgs['globalTagConnect'] = self.globalTagConnect

        harvestTaskCmsswHelper.setDataProcessingConfig(self.procScenario,
                                                       "alcaHarvesting",
                                                       **scenarioArgs)

        harvestTaskConditionHelper = harvestTaskCondition.getTypeHelper()
        harvestTaskConditionHelper.setRunNumber(self.runNumber)
        harvestTaskConditionHelper.setConditionOutputLabel(condOutLabel + "ALCAPROMPT")
        harvestTaskConditionHelper.setConditionLFNBase(condLFNBase)
        harvestTaskConditionHelper.setLuminosityURL(lumiURL)

        self.addOutputModule(harvestTask, condOutLabel,
                             primaryDataset=getattr(parentOutputModule, "primaryDataset"),
                             dataTier=getattr(parentOutputModule, "dataTier"),
                             filterName=getattr(parentOutputModule, "filterName"))

        harvestTaskUploadHelper = harvestTaskUpload.getTypeHelper()
        harvestTaskUploadHelper.setProxyFile(uploadProxy)
        harvestTaskUploadHelper.setServerURL(self.dqmUploadUrl)

        return harvestTask

    def addConditionTask(self, parentTask, parentOutputModuleName):
        """
        _addConditionTask_

        Does not actually produce any jobs
        The job splitter is custom and just forwards information
        into T0AST specific data structures, the actual upload
        of the conditions to the DropBox is handled in a separate
        Tier0 component.

        """
        # finalize splitting parameters
        mySplitArgs = {}
        mySplitArgs['algo_package'] = "T0.JobSplitting"
        mySplitArgs['runNumber'] = self.runNumber
        mySplitArgs['streamName'] = self.streamName

        parentTaskCmssw = parentTask.getStep("cmsRun1")
        parentOutputModule = parentTaskCmssw.getOutputModule(parentOutputModuleName)

        conditionTask = parentTask.addTask("%sCondition%s" % (parentTask.name(), parentOutputModuleName))

        # this is complete bogus, but other code can't deal with a task with no steps
        conditionTaskBogus = conditionTask.makeStep("bogus")
        conditionTaskBogus.setStepType("DQMUpload")

        dataTier = getattr(parentOutputModule, "dataTier")
        conditionTask.setInputReference(parentTaskCmssw, outputModule=parentOutputModuleName, dataTier=dataTier)

        conditionTask.applyTemplates()

        conditionTask.setTaskType("Harvesting")

        conditionTask.setSplittingAlgorithm("Condition",
                                            **mySplitArgs)

        return

    def __call__(self, workloadName, arguments):
        """
        _call_

        Create a Express workload with the given parameters.
        """
        StdBase.__call__(self, workloadName, arguments)

        # Required parameters that must be specified by the Requestor.
        self.outputs = arguments['Outputs']

        # job splitting parameters (also required parameters)
        self.expressSplitArgs = {}
        self.expressSplitArgs['maxInputRate'] = arguments['MaxInputRate']
        self.expressSplitArgs['maxInputEvents'] = arguments['MaxInputEvents']
        self.expressMergeSplitArgs = {}
        self.expressMergeSplitArgs['maxInputSize'] = arguments['MaxInputSize']
        self.expressMergeSplitArgs['maxInputFiles'] = arguments['MaxInputFiles']
        self.expressMergeSplitArgs['maxLatency'] = arguments['MaxLatency']

        # fixed parameters that are used in various places
        self.alcaHarvestOutLabel = "Sqlite"

        return self.buildWorkload()

    @staticmethod
    def getWorkloadCreateArgs():
        baseArgs = StdBase.getWorkloadCreateArgs()
        specArgs = {"RequestType": {"default": "Express"},
                    "ConfigCacheID": {"optional": True, "null": True},
                    "Scenario": {"optional": False, "attr": "procScenario"},
                    "RecoCMSSWVersion": {"validate": lambda x: x in releases(),
                                         "optional": False, "attr": "recoFrameworkVersion"},
                    "RecoScramArch": {"validate": lambda x: all([y in architectures() for y in x]),
                                      "optional": False, "type": makeNonEmptyList},
                    "GlobalTag": {"optional": False},
                    "ProcessingString": {"default": "", "validate": procstringT0},
                    "StreamName": {"optional": False},
                    "SpecialDataset": {"optional": False},
                    "AlcaHarvestTimeout": {"type": int, "optional": False},
                    "AlcaHarvestCondLFNBase": {"optional": False, "null": True},
                    "AlcaHarvestLumiURL": {"optional": False, "null": True},
                    "AlcaSkims": {"type": makeList, "optional": False},
                    "DQMSequences": {"type": makeList, "attr": "dqmSequences", "optional": False},
                    "Outputs": {"type": makeList, "optional": False},
                    "MaxInputRate": {"type": int, "optional": False},
                    "MaxInputEvents": {"type": int, "optional": False},
                    "MaxInputSize": {"type": int, "optional": False},
                    "MaxInputFiles": {"type": int, "optional": False},
                    "MaxLatency": {"type": int, "optional": False},

                    }
        baseArgs.update(specArgs)
        StdBase.setDefaultArgumentsProperty(baseArgs)
        return baseArgs

    @staticmethod
    def getWorkloadAssignArgs():
        baseArgs = StdBase.getWorkloadAssignArgs()
        specArgs = {
            "Override": {"default": {"eos-lfn-prefix": "root://eoscms.cern.ch//eos/cms/store/logs/prod/recent/Express"},
                         "type": dict},
            }
        baseArgs.update(specArgs)
        StdBase.setDefaultArgumentsProperty(baseArgs)
        return baseArgs
