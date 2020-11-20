#!/usr/bin/env python
"""
_WMWorkload_

Request level processing specification, acts as a container of a set
of related tasks.
"""
from __future__ import print_function

from Utils.Utilities import strToBool
from WMCore.Configuration import ConfigSection
from WMCore.Lexicon import sanitizeURL
from WMCore.WMException import WMException
from WMCore.WMSpec.ConfigSectionTree import findTop
from WMCore.WMSpec.Persistency import PersistencyHelper
from WMCore.WMSpec.WMTask import WMTask, WMTaskHelper
from WMCore.WMSpec.WMWorkloadTools import (validateArgumentsUpdate, loadSpecClassByType,
                                           setAssignArgumentsWithDefault)

parseTaskPath = lambda p: [x for x in p.split('/') if x.strip() != '']


def getWorkloadFromTask(taskRef):
    """
    _getWorkloadFromTask_

    Util to retrieve a Workload wrapped in a WorkloadHelper
    from a WMTask.
    """
    nodeData = taskRef
    if isinstance(taskRef, WMTaskHelper):
        nodeData = taskRef.data

    topNode = findTop(nodeData)
    if not hasattr(topNode, "objectType"):
        msg = "Top Node is not a WM definition object:\n"
        msg += "Object has no objectType attribute"
        # TODO: Replace with real exception class
        raise RuntimeError(msg)

    objType = getattr(topNode, "objectType")
    if objType != "WMWorkload":
        msg = "Top level object is not a WMWorkload: %s" % objType
        # TODO: Replace with real exception class
        raise RuntimeError(msg)

    return WMWorkloadHelper(topNode)


class WMWorkloadException(WMException):
    """
    _WMWorkloadException_

    Exceptions raised by the Workload during filling
    """
    pass


class WMWorkloadHelper(PersistencyHelper):
    """
    _WMWorkloadHelper_

    Methods & Utils for working with a WMWorkload instance.
    """

    def __init__(self, wmWorkload=None):
        self.data = wmWorkload

    def setSpecUrl(self, url):
        self.data.persistency.specUrl = sanitizeURL(url)["url"]

    def specUrl(self):
        """
        _specUrl_

        return url location of workload
        """
        return self.data.persistency.specUrl

    def name(self):
        """
        _name_

        return name of the workload
        """
        return self.data._internal_name

    def setName(self, workloadName):
        """
        _setName_

        Set the workload name.
        """
        self.data._internal_name = workloadName
        return

    def setRequestType(self, requestType):
        self.data.requestType = requestType

    def setStepProperties(self, assignArgs):
        """
        _setStepProperties_

        Used for properly setting AcqEra/ProcStr/ProcVer for each step in a StepChain request
        during assignment. Only used if one of those parameters is a dictionary.
        """
        if "AcquisitionEra" in assignArgs and isinstance(assignArgs["AcquisitionEra"], dict):
            pass
        elif "ProcessingString" in assignArgs and isinstance(assignArgs["ProcessingString"], dict):
            pass
        elif "ProcessingVersion" in assignArgs and isinstance(assignArgs["ProcessingVersion"], dict):
            pass
        else:
            return

        stepNameMapping = self.getStepMapping()
        # it has only one top level task
        for task in self.taskIterator():
            # Merge task has cmsRun1 step, so it gets messy on Merge ACDC of StepChain
            if task.taskType() == "Merge":
                continue
            task.updateLFNsAndDatasets(dictValues=assignArgs, stepMapping=stepNameMapping)

        return

    def setStepMapping(self, mapping):
        """
        _setStepMapping_

        Mostly used for StepChains. It creates a mapping between the StepName and the step
        number and the cmsRun number. E.g.:
          {'GENSIM': ('Step1', 'cmsRun1'), 'DIGI': ('Step2', 'cmsRun2'), 'RECO': ('Step3', 'cmsRun3')}
        """
        self.data.properties.stepMapping = mapping

    def getStepMapping(self):
        """
        _getStepMapping_

        Only important for StepChains. Map from step name to step number
        and cmsRun number.
        """
        return getattr(self.data.properties, "stepMapping", None)

    def setStepParentageMapping(self, mapping):
        """
        _setStepParentageMapping_

        Used for StepChains. Set a wider dictionary structure with a mapping between
        parent and child steps as well as dataset parentage
        """
        self.data.properties.stepParentageMapping = mapping

    def getStepParentageMapping(self):
        """
        _getStepParentageMapping_

        Only important for StepChains. Map from step name to step and parent
        step properties, including a map of output datasets to the parent dataset.
        """
        return getattr(self.data.properties, "stepParentageMapping", {})

    def getStepParentDataset(self, childDataset):
        """
        :param childDataset: child dataset which is looking for parent dataset
        :return: str parent dataset if exist, otherwise None

        Correct parentage mapping is set when workflow is assigned, Shouldn't call this method before workflow is assigned
        Assumes there is only one parent dataset given childDataset
        """
        ### FIXME: Seangchan, I don't think we need this method, since we'll add the
        # map to the dbsbuffer_dataset table and then use it from there. So,
        # wmbsHelper should actually fetch the simple map data and insert that into db
        stepParentageMap = self.getStepParentageMapping()
        if stepParentageMap:
            for stepName in stepParentageMap:
                stepItem = stepParentageMap[stepName]
                outDSMap = stepItem["OutputDatasetMap"]
                for outmodule in outDSMap:
                    if childDataset in outDSMap[outmodule] and stepItem['ParentDataset']:
                        return stepItem['ParentDataset']
        else:
            return None

    def setTaskParentageMapping(self, mapping):
        """
        _setTaskParentageMapping_

        Used for TaskChains. Sets a dictionary with the task / parent task /
        parent dataset / and output datasets relationship.
        """
        self.data.properties.taskParentageMapping = mapping

    def getTaskParentageMapping(self):
        """
        _getTaskParentageMapping_

        Only important for TaskChains. Returns a map of task name to
        parent dataset and output datasets.
        """
        return getattr(self.data.properties, "taskParentageMapping", {})

    def getChainParentageSimpleMapping(self):
        """
        Creates a simple map of task or step to parent and output datasets
        such that it can be friendly stored in the reqmgr workload cache doc.
        :return:  {'Step1': {'ParentDset': 'blah1', 'ChildDsets': ['blah2']},
                   'Step2': {'ParentDset': 'blah2', 'ChildDsets': ['blah3', 'blah4],
                   ...} if stepParentageMapping exist otherwise None
        """
        if self.getRequestType() == "TaskChain":
            chainMap = self.getTaskParentageMapping()
        elif self.getRequestType() == "StepChain":
            chainMap = self.getStepParentageMapping()
        else:
            return {}

        newMap = {}
        if chainMap:
            for _, cData in chainMap.items():
                cNum = cData.get('TaskNumber', cData.get('StepNumber'))
                newMap[cNum] = {'ParentDset': cData['ParentDataset'],
                                'ChildDsets': []}
                for outMod in cData['OutputDatasetMap']:
                    newMap[cNum]['ChildDsets'].append(cData['OutputDatasetMap'][outMod])
        return newMap

    def updateStepParentageMap(self):
        """
        _updateStepParentageMap
        Used to update the step parentage mapping of StepChain requests at the
        end of the assignment process, given that we might have new output
        dataset names
        :return: just updates the workload property: stepParentageMapping
        """
        topLevelTask = next(self.taskIterator())
        if topLevelTask.taskType() == "Merge":
            # handle ACDC for merge jobs, see #9051. Nothing to do here
            return

        parentMap = self.getStepParentageMapping()
        listOfStepNames = parentMap.keys()
        for stepName in listOfStepNames:
            if parentMap[stepName]['OutputDatasetMap']:
                # then there is output dataset, let's update it
                cmsRunNumber = parentMap[stepName]['StepCmsRun']
                stepHelper = topLevelTask.getStepHelper(cmsRunNumber)
                for outputModuleName in stepHelper.listOutputModules():
                    outputModule = stepHelper.getOutputModule(outputModuleName)
                    outputDataset = "/%s/%s/%s" % (outputModule.primaryDataset,
                                                   outputModule.processedDataset,
                                                   outputModule.dataTier)

                    # now find and replace the old dataset by the new dataset name
                    oldOutputDset = parentMap[stepName]['OutputDatasetMap'][outputModuleName]
                    for s in listOfStepNames:
                        if parentMap[s]['ParentDataset'] == oldOutputDset:
                            parentMap[s]['ParentDataset'] = outputDataset
                        if oldOutputDset == parentMap[s]['OutputDatasetMap'].get(outputModuleName, ""):
                            parentMap[s]['OutputDatasetMap'][outputModuleName] = outputDataset

        self.setStepParentageMapping(parentMap)

        return

    def updateTaskParentageMap(self):
        """
        _updateTaskParentageMap_
        Used to update the task dataset parentage mapping of TaskChain requests
        at the end of the assignment process, given that we might have new output
        dataset names
        :return: just updates the workload property: taskParentageMapping
        """
        taskMap = self.getTaskParentageMapping()

        for tName in taskMap.keys():
            if not taskMap[tName]['OutputDatasetMap']:
                continue

            taskO = self.getTaskByName(tName)
            if taskO is None:
                # Resubmission requests might not have certain tasks
                continue

            for outInfo in taskO.listOutputDatasetsAndModules():
                # Check whether it's a transient output module
                if outInfo['outputModule'] not in taskMap[tName]['OutputDatasetMap']:
                    continue
                oldOutputDset = taskMap[tName]['OutputDatasetMap'][outInfo['outputModule']]
                taskMap[tName]['OutputDatasetMap'][outInfo['outputModule']] = outInfo['outputDataset']
                for tt in taskMap.keys():
                    if taskMap[tt]['ParentDataset'] == oldOutputDset:
                        taskMap[tt]['ParentDataset'] = outInfo['outputDataset']

        self.setTaskParentageMapping(taskMap)

        return

    def getInitialJobCount(self):
        """
        _getInitialJobCount_

        Get the initial job count, this is incremented everytime the workflow
        is resubmitted with ACDC.
        """
        return self.data.initialJobCount

    def setInitialJobCount(self, jobCount):
        """
        _setInitialJobCount_

        Set the initial job count.
        """
        self.data.initialJobCount = jobCount
        return

    def getDashboardActivity(self):
        """
        _getDashboardActivity_

        Retrieve the dashboard activity.
        """
        return self.data.properties.dashboardActivity

    def setDashboardActivity(self, dashboardActivity):
        """
        _setDashboardActivity_

        Set the dashboard activity for the workflow.
        """
        self.data.properties.dashboardActivity = dashboardActivity
        return

    def getTopLevelTask(self):
        """
        _getTopLevelTask_

        Retrieve the top level task.
        """
        topLevelTasks = []
        for task in self.taskIterator():
            if task.isTopOfTree():
                topLevelTasks.append(task)

        return topLevelTasks

    def getOwner(self):
        """
        _getOwner_

        Retrieve the owner information.
        """
        return self.data.owner.dictionary_()

    def setOwner(self, name, ownerProperties=None):
        """
        _setOwner_
        sets the owner of wmspec.
        Takes a name as a mandatory argument, and then a dictionary of properties
        """
        ownerProperties = ownerProperties or {'dn': 'DEFAULT'}

        self.data.owner.name = name
        self.data.owner.group = "undefined"

        if not isinstance(ownerProperties, dict):
            raise Exception("Someone is trying to setOwner without a dictionary")

        for key in ownerProperties.keys():
            setattr(self.data.owner, key, ownerProperties[key])

        return

    def setOwnerDetails(self, name, group, ownerProperties=None):
        """
        _setOwnerDetails_

        Set the owner, explicitly requiring the group and user arguments
        """
        ownerProperties = ownerProperties or {'dn': 'DEFAULT'}

        self.data.owner.name = name
        self.data.owner.group = group

        if not isinstance(ownerProperties, dict):
            raise Exception("Someone is trying to setOwnerDetails without a dictionary")
        for key in ownerProperties.keys():
            setattr(self.data.owner, key, ownerProperties[key])
        return

    def sandbox(self):
        """
        _sandbox_
        """
        return self.data.sandbox

    def setSandbox(self, sandboxPath):
        """
        _sandbox_
        """
        self.data.sandbox = sandboxPath

    def setPriority(self, priority):
        """
        _setPriority_

        Set the priority for the workload
        """
        self.data.request.priority = int(priority)

    def priority(self):
        """
        _priority_
        return priority of workload
        """
        return self.data.request.priority

    def setStartPolicy(self, policyName, **params):
        """
        _setStartPolicy_

        Set the Start policy and its parameters
        """
        self.data.policies.start.policyName = policyName
        for key, val in params.iteritems():
            setattr(self.data.policies.start, key, val)

    def startPolicy(self):
        """
        _startPolicy_

        Get Start Policy name
        """
        return getattr(self.data.policies.start, "policyName", None)

    def startPolicyParameters(self):
        """
        _startPolicyParameters_

        Get Start Policy parameters
        """
        datadict = getattr(self.data.policies, "start")
        return datadict.dictionary_()

    def setEndPolicy(self, policyName, **params):
        """
        _setEndPolicy_

        Set the End policy and its parameters
        """
        self.data.policies.end.policyName = policyName
        for key, val in params.iteritems():
            setattr(self.data.policies.end, key, val)

    def endPolicy(self):
        """
        _endPolicy_

        Get End Policy name
        """
        return getattr(self.data.policies.end, "policyName", None)

    def endPolicyParameters(self):
        """
        _startPolicyParameters_

        Get Start Policy parameters
        """
        datadict = getattr(self.data.policies, "end")
        return datadict.dictionary_()

    def getTask(self, taskName):
        """
        _getTask_

        Retrieve a - top level task - with the given name.
        """
        task = getattr(self.data.tasks, taskName, None)
        if task is None:
            return None
        return WMTaskHelper(task)

    def getTaskByName(self, taskName):
        """
        _getTaskByName_

        Retrieve a task with the given name in the whole workflow tree.
        """
        for t in self.taskIterator():
            if t.name() == taskName:
                return t
            for x in t.taskIterator():
                if x.name() == taskName:
                    return x
        return None

    def getTaskByPath(self, taskPath):
        """
        _getTask_

        Get a task instance based on the path name

        """
        mapping = {}
        for t in self.taskIterator():
            for x in t.taskIterator():
                mapping.__setitem__(x.getPathName, x.name())

        taskList = parseTaskPath(taskPath)

        if taskList[0] != self.name():  # should always be workload name first
            msg = "Workload name does not match:\n"
            msg += "requested name %s from workload %s " % (taskList[0],
                                                            self.name())
            raise RuntimeError(msg)
        if len(taskList) < 2:
            # path should include workload and one task
            msg = "Task Path does not contain a top level task:\n"
            msg += taskPath
            raise RuntimeError(msg)

        topTask = self.getTask(taskList[1])
        if topTask is None:
            msg = "Task /%s/%s Not Found in Workload" % (taskList[0],
                                                         taskList[1])
            raise RuntimeError(msg)
        for x in topTask.taskIterator():
            if x.getPathName() == taskPath:
                return x
        return None

    def taskIterator(self):
        """
        generator to traverse top level tasks

        """
        for i in self.data.tasks.tasklist:
            yield self.getTask(i)

    def listAllTaskNodes(self):
        """
        """
        result = []
        for t in self.taskIterator():
            if t != None:
                result.extend(t.listNodes())
        return result

    def listAllTaskPathNames(self):
        """
        _listAllTaskPathNames_

        Generate a list of all known task path names including
        tasks that are part of the top level tasks
        """
        result = []
        for t in self.taskIterator():
            result.extend(t.listPathNames())
        return result

    def listAllTaskNames(self):
        """
        _listAllTaskNames_

        Generate a list of all known task names including
        tasks that are part of the top level tasks
        """
        result = []
        for t in self.taskIterator():
            result.extend(t.listNames())
        return result

    def listTasksOfType(self, ttype):
        """
        _listTasksOfType_

        Get tasks matching the type provided
        """
        return [t for t in self.taskIterator() if t.taskType() == ttype]

    def getAllTasks(self, cpuOnly=False):
        """
        _getAllTasks_

        Get all tasks from a workload.
        If cpuOnly flag is set to True, then don't return utilitarian tasks.
        """
        tasks = []
        for n in self.listAllTaskPathNames():
            task = self.getTaskByPath(taskPath=n)
            if cpuOnly and task.taskType() in ["Cleanup", "LogCollect"]:
                continue
            tasks.append(task)

        return tasks

    def addTask(self, wmTask):
        """
        _addTask_

        Add a Task instance either naked or wrapped in a helper

        """
        task = wmTask
        if isinstance(wmTask, WMTaskHelper):
            task = wmTask.data
            helper = wmTask
        else:
            helper = WMTaskHelper(wmTask)
        taskName = helper.name()
        pathName = "/%s/%s" % (self.name(), taskName)
        helper.setPathName(pathName)
        if taskName in self.listAllTaskNodes():
            msg = "Duplicate task name: %s\n" % taskName
            msg += "Known tasks: %s\n" % self.listAllTaskNodes()
            raise RuntimeError(msg)
        self.data.tasks.tasklist.append(taskName)
        setattr(self.data.tasks, taskName, task)
        return

    def newTask(self, taskName):
        """
        _newTask_

        Factory like interface for adding a toplevel task to this
        workload

        """
        if taskName in self.listAllTaskNodes():
            msg = "Duplicate task name: %s\n" % taskName
            msg += "Known tasks: %s\n" % self.listAllTaskNodes()
            raise RuntimeError(msg)
        task = WMTask(taskName)
        helper = WMTaskHelper(task)
        helper.setTopOfTree()
        self.addTask(helper)
        return helper

    def removeTask(self, taskName):
        """
        _removeTask_

        Remove given task with given name

        """
        self.data.tasks.__delattr__(taskName)
        self.data.tasks.tasklist.remove(taskName)
        return

    def setSiteWhitelist(self, siteWhitelist):
        """
        _setSiteWhitelist_

        Set the site white list for the top level tasks in the workload.
        """
        if not isinstance(siteWhitelist, list):
            siteWhitelist = [siteWhitelist]

        taskIterator = self.taskIterator()

        for task in taskIterator:
            task.setSiteWhitelist(siteWhitelist)

        return

    def setSiteBlacklist(self, siteBlacklist):
        """
        _setSiteBlacklist_

        Set the site black list for the top level tasks in the workload.
        """
        if not isinstance(siteBlacklist, type([])):
            siteBlacklist = [siteBlacklist]

        taskIterator = self.taskIterator()

        for task in taskIterator:
            task.setSiteBlacklist(siteBlacklist)

        return

    def setBlockWhitelist(self, blockWhitelist, initialTask=None):
        """
        _setBlockWhitelist_

        Set the block white list for all tasks that have an input dataset
        defined.
        """
        if not isinstance(blockWhitelist, type([])):
            blockWhitelist = [blockWhitelist]

        if initialTask:
            taskIterator = initialTask.childTaskIterator()
        else:
            taskIterator = self.taskIterator()

        for task in taskIterator:
            if task.getInputDatasetPath():
                task.setInputBlockWhitelist(blockWhitelist)
            self.setBlockWhitelist(blockWhitelist, task)

        return

    def setBlockBlacklist(self, blockBlacklist, initialTask=None):
        """
        _setBlockBlacklist_

        Set the block black list for all tasks that have an input dataset
        defined.
        """
        if not isinstance(blockBlacklist, type([])):
            blockBlacklist = [blockBlacklist]

        if initialTask:
            taskIterator = initialTask.childTaskIterator()
        else:
            taskIterator = self.taskIterator()

        for task in taskIterator:
            if task.getInputDatasetPath():
                task.setInputBlockBlacklist(blockBlacklist)
            self.setBlockBlacklist(blockBlacklist, task)

        return

    def setRunWhitelist(self, runWhitelist, initialTask=None):
        """
        _setRunWhitelist_

        Set the run white list for all tasks that have an input dataset defined.
        """
        if not isinstance(runWhitelist, type([])):
            runWhitelist = [runWhitelist]

        if initialTask:
            taskIterator = initialTask.childTaskIterator()
        else:
            taskIterator = self.taskIterator()

        for task in taskIterator:
            if task.getInputDatasetPath():
                task.setInputRunWhitelist(runWhitelist)
                task.setSplittingParameters(runWhitelist=runWhitelist)
            self.setRunWhitelist(runWhitelist, task)

        return

    def setRunBlacklist(self, runBlacklist, initialTask=None):
        """
        _setRunBlacklist_

        Set the run black list for all tasks that have an input dataset defined.
        """
        if not isinstance(runBlacklist, type([])):
            runBlacklist = [runBlacklist]

        if initialTask:
            taskIterator = initialTask.childTaskIterator()
        else:
            taskIterator = self.taskIterator()

        for task in taskIterator:
            if task.getInputDatasetPath():
                task.setInputRunBlacklist(runBlacklist)
            self.setRunBlacklist(runBlacklist, task)

        return

    def updateLFNsAndDatasets(self, runNumber=None):
        """
        _updateLFNsAndDatasets_

        Update all the output LFNs and data names for all tasks in the workflow.
        This needs to be called after updating the acquisition era, processing
        version or merged/unmerged lfn base.
        """
        taskIterator = self.taskIterator()

        for task in taskIterator:
            task.updateLFNsAndDatasets(runNumber=runNumber)
        return

    def updateDatasetName(self, mergeTask, datasetName):
        """
        _updateDatasetName_

        Updates the dataset name argument of the mergeTask's harvesting
        children tasks
        """
        for task in mergeTask.childTaskIterator():
            if task.taskType() == "Harvesting":
                for stepName in task.listAllStepNames():
                    stepHelper = task.getStepHelper(stepName)

                    if stepHelper.stepType() == "CMSSW":
                        cmsswHelper = stepHelper.getTypeHelper()
                        cmsswHelper.setDatasetName(datasetName)

        return

    def setCoresAndStreams(self, cores, nStreams, initialTask=None):
        """
        _setCoresAndStreams_

        Update the number of cores and event streams for each task in the spec.

        One can update only the number of cores, which will set the number of streams to 0 (default).
        However, updating only the number of streams is not allowed, it's coupled to # of cores.

        :param cores: number of cores. Can be either an integer or a dict key'ed by taskname
        :param nStreams: number of streams. Can be either an integer or a dict key'ed by taskname
        :param initialTask: parent task object
        """
        if not cores:
            return

        if initialTask:
            taskIterator = initialTask.childTaskIterator()
        else:
            taskIterator = self.taskIterator()

        for task in taskIterator:
            task.setNumberOfCores(cores, nStreams)
            self.setCoresAndStreams(cores, nStreams, task)

        return

    def setMemory(self, memory, initialTask=None):
        """
        _setMemory_

        Update memory requirements for each task in the spec, thus it
        can be either an integer or a dictionary key'ed by the task name.
        """
        if not memory:
            return

        if initialTask:
            taskIterator = initialTask.childTaskIterator()
        else:
            taskIterator = self.taskIterator()

        for task in taskIterator:
            if isinstance(memory, dict):
                mem = memory.get(task.name())
            else:
                mem = memory
            task.setJobResourceInformation(memoryReq=mem)
            self.setMemory(memory, task)

        return

    def setTimePerEvent(self, timePerEvent, initialTask=None):
        """
        _setTimePerEvent_

        Update TimePerEvent requirements for each task in the spec, thus it
        can be either an integer or a dictionary key'ed by the task name.
        """
        # don't set it for utilitarian/merge tasks
        if not timePerEvent:
            return

        if initialTask:
            taskIterator = initialTask.childTaskIterator()
        else:
            taskIterator = self.taskIterator()

        for task in taskIterator:
            if isinstance(timePerEvent, dict):
                timePE = timePerEvent.get(task.name())
            else:
                timePE = timePerEvent
            task.setJobResourceInformation(timePerEvent=timePE)
            self.setTimePerEvent(timePerEvent, task)

        return

    def setAcquisitionEra(self, acquisitionEras):
        """
        _setAcquistionEra_

        Change the acquisition era for all tasks in the spec and then update
        all of the output LFNs and datasets to use the new acquisition era.
        """
        stepNameMapping = self.getStepMapping()
        for task in self.taskIterator():
            task.setAcquisitionEra(acquisitionEras, stepChainMap=stepNameMapping)

        self.updateLFNsAndDatasets()
        # set acquistionEra for workload (need to refactor)
        self.data.properties.acquisitionEra = acquisitionEras
        return

    def setProcessingVersion(self, processingVersions):
        """
        _setProcessingVersion_

        Change the processing version for all tasks in the spec and then update
        all of the output LFNs and datasets to use the new processing version.
        """
        stepNameMapping = self.getStepMapping()

        for task in self.taskIterator():
            task.setProcessingVersion(processingVersions, stepChainMap=stepNameMapping)

        self.updateLFNsAndDatasets()
        self.data.properties.processingVersion = processingVersions
        return

    def setProcessingString(self, processingStrings):
        """
        _setProcessingString_

        Change the processing string for all tasks in the spec and then update
        all of the output LFNs and datasets to use the new processing version.
        """
        stepNameMapping = self.getStepMapping()

        for task in self.taskIterator():
            task.setProcessingString(processingStrings, stepChainMap=stepNameMapping)

        self.updateLFNsAndDatasets()
        self.data.properties.processingString = processingStrings
        return

    def setLumiList(self, lumiLists):
        """
        _setLumiList_

        Change the lumi mask for all tasks in the spec
        """

        for task in self.taskIterator():
            task.setLumiMask(lumiLists, override=False)

        # set lumiList for workload (need to refactor)
        self.data.properties.lumiList = lumiLists
        return

    def setTaskProperties(self, requestArgs):
        # FIXME (Alan): I don't think it works, given that the assignment
        # parameters never have the TaskChain parameter...
        if not 'TaskChain' in requestArgs:
            return
        numTasks = requestArgs['TaskChain']
        taskArgs = []
        for i in range(numTasks):
            taskArgs.append(requestArgs["Task%s" % (i + 1)])

        for prop in taskArgs:
            taskName = prop['TaskName']
            for task in self.getAllTasks():
                if task.name() == taskName:
                    del prop['TaskName']
                    task.setProperties(prop)
                    break
        return

    def getAcquisitionEra(self, taskName=None):
        """
        _getAcquisitionEra_

        Get the acquisition era
        """
        if taskName and isinstance(self.data.properties.acquisitionEra, dict):
            return self.data.properties.acquisitionEra.get(taskName, None)
        return self.data.properties.acquisitionEra

    def getRequestType(self):
        """
        _getRequestType_

        Get the Request type (ReReco, TaskChain, etc)
        """
        if getattr(self.data, 'requestType', None):
            return getattr(self.data, "requestType")

        if hasattr(self.data, "request"):
            if hasattr(self.data.request, "schema"):
                return getattr(self.data.request.schema, "RequestType", None)
        return None

    def getProcessingVersion(self, taskName=None):
        """
        _getProcessingVersion_

        Get the processingVersion
        """

        if taskName and isinstance(self.data.properties.processingVersion, dict):
            return self.data.properties.processingVersion.get(taskName, 0)
        return self.data.properties.processingVersion

    def getProcessingString(self, taskName=None):
        """
        _getProcessingString_

        Get the processingString
        """

        if taskName and isinstance(self.data.properties.processingString, dict):
            return self.data.properties.processingString.get(taskName, None)
        return self.data.properties.processingString

    def getLumiList(self):
        """
        _getLumiList_

        Get the LumitList from workload (task level should have the same lumiList)
        """
        return self.data.properties.lumiList

    def setValidStatus(self, validStatus):
        """
        _setValidStatus_

        Sets the status that will be reported to the processed dataset
        in DBS
        """

        self.data.properties.validStatus = validStatus
        return

    def getValidStatus(self):
        """
        _getValidStatus_

        Get the valid status for DBS
        """

        return getattr(self.data.properties, 'validStatus', None)

    def setAllowOpportunistic(self, allowOpport):
        """
        _setAllowOpportunistic_

        Set a flag which enables the workflow to run in cloud resources.
        """
        self.data.properties.allowOpportunistic = allowOpport
        return

    def getAllowOpportunistic(self):
        """
        _getAllowOpportunistic_

        Retrieve AllowOpportunitisc flag for the workflow
        """
        return getattr(self.data.properties, 'allowOpportunistic', None)

    def setPrepID(self, prepID):
        """
        _setPrepID_

        Set the prepID to for all the tasks below
        """

        taskIterator = self.taskIterator()
        for task in taskIterator:
            task.setPrepID(prepID)
        self.data.properties.prepID = prepID
        return

    def getPrepID(self):
        """
        _getPrepID_

        Get the prepID for the workflow
        """
        return getattr(self.data.properties, 'prepID', None)

    def setDbsUrl(self, dbsUrl):
        """
        _setDbsUrl_

        Set the workload level DbsUrl.
        """
        self.data.dbsUrl = dbsUrl

    def getDbsUrl(self):
        """
        _getDbsUrl_

        Get the DbsUrl specified for the input dataset.
        """
        if getattr(self.data, 'dbsUrl', None):
            return getattr(self.data, "dbsUrl")

        if hasattr(self.data, "request"):
            if hasattr(self.data.request, "schema"):
                if not getattr(self.data.request.schema, "DbsUrl", None):
                    return "https://cmsweb.cern.ch/dbs/prod/global/DBSReader"

        return getattr(self.data.request.schema, "DbsUrl")

    def setCampaign(self, campaign):
        """
        _setCampaign_

        Set the campaign to which this workflow belongs
        Optional
        """
        self.data.properties.campaign = campaign
        return

    def getCampaign(self):
        """
        _getCampaign_

        Get the campaign for the workflow
        """
        return getattr(self.data.properties, 'campaign', None)

    def setLFNBase(self, mergedLFNBase, unmergedLFNBase, runNumber=None):
        """
        _setLFNBase_

        Set the merged and unmerged base LFNs for all tasks.  Update all of the
        output LFNs to use them.
        """
        self.data.properties.mergedLFNBase = mergedLFNBase
        self.data.properties.unmergedLFNBase = unmergedLFNBase

        # set all child tasks lfn base.
        for task in self.taskIterator():
            task.setLFNBase(mergedLFNBase, unmergedLFNBase)
        self.updateLFNsAndDatasets(runNumber=runNumber)
        return

    def setMergeParameters(self, minSize, maxSize, maxEvents,
                           initialTask=None):
        """
        _setMergeParameters_

        Set the parameters for every merge task in the workload.  Also update
        the min merge size of every CMSSW step.
        """
        if initialTask:
            taskIterator = initialTask.childTaskIterator()
        else:
            taskIterator = self.taskIterator()

        for task in taskIterator:
            for stepName in task.listAllStepNames():
                stepHelper = task.getStepHelper(stepName)
                if stepHelper.stepType() == "StageOut" and stepHelper.minMergeSize() > 0:
                    stepHelper.setMinMergeSize(minSize, maxEvents)

            if task.taskType() == "Merge":
                task.setSplittingParameters(min_merge_size=minSize,
                                            max_merge_size=maxSize,
                                            max_merge_events=maxEvents)

            self.setMergeParameters(minSize, maxSize, maxEvents, task)

        return

    def setWorkQueueSplitPolicy(self, policyName, splitAlgo, splitArgs, **kwargs):
        """
        _setWorkQueueSplitPolicy_

        Set the WorkQueue split policy.
        policyName should be either 'DatasetBlock', 'Dataset', 'MonteCarlo' 'Block'
        different policy could be added in the workqueue plug in.
        Additionally general parameters can be specified, these are not mapped and passed directly to the startPolicyArgs,
        also record the splitting algorithm in case the WorkQUeue policy needs it.
        """
        SplitAlgoToStartPolicy = {"FileBased": ["NumberOfFiles"],
                                  "EventBased": ["NumberOfEvents",
                                                 "NumberOfEventsPerLumi"],
                                  "LumiBased": ["NumberOfLumis"],
                                  "Harvest": ["NumberOfRuns"],
                                  "EventAwareLumiBased": ["NumberOfEvents"]}
        SplitAlgoToArgMap = {"NumberOfFiles": "files_per_job",
                             "NumberOfEvents": "events_per_job",
                             "NumberOfLumis": "lumis_per_job",
                             "NumberOfRuns": "runs_per_job",
                             "NumberOfEventsPerLumi": "events_per_lumi"}
        startPolicyArgs = {'SplittingAlgo': splitAlgo}
        startPolicyArgs.update(kwargs)

        sliceTypes = SplitAlgoToStartPolicy.get(splitAlgo, ["NumberOfFiles"])
        sliceType = sliceTypes[0]
        sliceSize = splitArgs.get(SplitAlgoToArgMap[sliceType], 1)
        startPolicyArgs["SliceType"] = sliceType
        startPolicyArgs["SliceSize"] = sliceSize

        if len(sliceTypes) > 1:
            subSliceType = sliceTypes[1]
            subSliceSize = splitArgs.get(SplitAlgoToArgMap[subSliceType],
                                         sliceSize)
            startPolicyArgs["SubSliceType"] = subSliceType
            startPolicyArgs["SubSliceSize"] = subSliceSize

        self.setStartPolicy(policyName, **startPolicyArgs)
        self.setEndPolicy("SingleShot")
        return

    def setJobSplittingParameters(self, taskPath, splitAlgo, splitArgs, updateOnly=False):
        """
        _setJobSplittingParameters_

        Update the job splitting algorithm and arguments for the given task.
        """
        taskHelper = self.getTaskByPath(taskPath)
        if taskHelper == None:
            return

        if taskHelper.isTopOfTree():
            self.setWorkQueueSplitPolicy(self.startPolicy(), splitAlgo, splitArgs)

        # There are currently two merge algorithms in WMBS.  WMBSMergeBySize
        # will reassemble the parent file.  This is only necessary for
        # EventBased processing where we break up lumi sections.  Everything
        # else can use ParentlessMergeBySize which won't reassemble parents.
        # Everything defaults to ParentlessMergeBySize as it is much less load
        # on the database.
        minMergeSize = None
        maxMergeEvents = None
        for childTask in taskHelper.childTaskIterator():
            if childTask.taskType() == "Merge":
                if splitAlgo == "EventBased" and taskHelper.taskType() != "Production":
                    mergeAlgo = "WMBSMergeBySize"
                    for stepName in childTask.listAllStepNames():
                        stepHelper = childTask.getStepHelper(stepName)
                        if stepHelper.stepType() == "CMSSW":
                            stepCmsswHelper = stepHelper.getTypeHelper()
                            stepCmsswHelper.setSkipBadFiles(False)
                else:
                    mergeAlgo = "ParentlessMergeBySize"

                childSplitParams = childTask.jobSplittingParameters()
                minMergeSize = childSplitParams["min_merge_size"]
                maxMergeEvents = childSplitParams["max_merge_events"]
                if not updateOnly:
                    del childSplitParams["algorithm"]
                    del childSplitParams["siteWhitelist"]
                    del childSplitParams["siteBlacklist"]
                    childTask.setSplittingAlgorithm(mergeAlgo, **childSplitParams)
                else:
                    childTask.updateSplittingParameters(mergeAlgo, **childSplitParams)
        # Set the splitting algorithm for the task.  If the split algo is
        # EventBased, we need to disable straight to merge.  If this isn't an
        # EventBased algo we need to enable straight to merge. If straight
        # to merge is disabled then keep it that way.
        if not updateOnly:
            taskHelper.setSplittingAlgorithm(splitAlgo, **splitArgs)
        else:
            taskHelper.updateSplittingParameters(splitAlgo, **splitArgs)
        for stepName in taskHelper.listAllStepNames():
            stepHelper = taskHelper.getStepHelper(stepName)
            if stepHelper.stepType() == "StageOut":
                if splitAlgo != "EventBased" and stepHelper.minMergeSize() != -1 and minMergeSize:
                    stepHelper.setMinMergeSize(minMergeSize, maxMergeEvents)
                else:
                    stepHelper.disableStraightToMerge()
            if stepHelper.stepType() == "CMSSW" and splitAlgo == "WMBSMergeBySize" \
                    and stepHelper.getSkipBadFiles():
                stepHelper.setSkipBadFiles(False)

            if taskHelper.isTopOfTree() and taskHelper.taskType() == "Production" and stepName == "cmsRun1":
                # set it only for the first cmsRun in multi-steps tasks
                stepHelper.setEventsPerLumi(splitArgs.get("events_per_lumi", None))
        return

    def listJobSplittingParametersByTask(self, initialTask=None, performance=True):
        """
        _listJobSplittingParametersByTask_

        Create a dictionary that maps task names to job splitting parameters.
        """
        output = {}

        if initialTask:
            taskIterator = initialTask.childTaskIterator()
        else:
            taskIterator = self.taskIterator()

        for task in taskIterator:
            taskName = task.getPathName()
            taskParams = task.jobSplittingParameters(performance)
            del taskParams["siteWhitelist"]
            del taskParams["siteBlacklist"]
            output[taskName] = taskParams
            output[taskName]["type"] = task.taskType()
            output.update(self.listJobSplittingParametersByTask(task, performance))

        return output

    def listInputDatasets(self):
        """
        _listInputDatasets_

        List all the input datasets in the workload
        """
        inputDatasets = []

        taskIterator = self.taskIterator()
        for task in taskIterator:
            path = task.getInputDatasetPath()
            if path:
                inputDatasets.append(path)

        return inputDatasets

    def listOutputDatasets(self, initialTask=None):
        """
        _listOutputDatasets_

        List the names of all the datasets produced by this workflow.
        """
        outputDatasets = []

        if initialTask:
            taskIterator = initialTask.childTaskIterator()
        else:
            taskIterator = self.taskIterator()

        for task in taskIterator:
            for stepName in task.listAllStepNames():
                stepHelper = task.getStepHelper(stepName)

                if not getattr(stepHelper.data.output, "keep", True):
                    continue

                if stepHelper.stepType() == "CMSSW":
                    for outputModuleName in stepHelper.listOutputModules():
                        # Only consider non-transient output
                        outputModule = stepHelper.getOutputModule(outputModuleName)
                        if getattr(outputModule, "transient", False):
                            continue
                        outputDataset = "/%s/%s/%s" % (outputModule.primaryDataset,
                                                       outputModule.processedDataset,
                                                       outputModule.dataTier)
                        outputDatasets.append(outputDataset)

            moreDatasets = self.listOutputDatasets(task)
            outputDatasets.extend(moreDatasets)

        return outputDatasets

    def listAllOutputModulesLFNBases(self, initialTask=None, onlyUnmerged=True):
        """
        _listAllOutputModulesLFNBases_

        List all output LFN bases defined in this workload object.
        """
        listLFNBases = set()
        if initialTask:
            taskIterator = initialTask.childTaskIterator()
        else:
            taskIterator = self.taskIterator()

        for task in taskIterator:
            for stepName in task.listAllStepNames():
                outModule = task.getOutputModulesForStep(stepName)
                for module in outModule.dictionary_().values():
                    lfnBase = getattr(module, "lfnBase", "")
                    if not onlyUnmerged and lfnBase:
                        listLFNBases.add(lfnBase)
                    elif lfnBase.startswith('/store/unmerged'):
                        listLFNBases.add(lfnBase)
            # recursively go through all the tasks
            listLFNBases.update(self.listAllOutputModulesLFNBases(task, onlyUnmerged))

        return list(listLFNBases)

    def listPileupDatasets(self, initialTask=None):
        """
        _listPileUpDataset_

        Returns a dictionary with all the required pile-up datasets
        in this workload and their associated dbs url as the key
        """
        pileupDatasets = {}

        if initialTask:
            taskIterator = initialTask.childTaskIterator()
        else:
            taskIterator = self.taskIterator()

        for task in taskIterator:
            for stepName in task.listAllStepNames():
                stepHelper = task.getStepHelper(stepName)
                if stepHelper.stepType() == "CMSSW":
                    pileupSection = stepHelper.getPileup()
                    if pileupSection is None:
                        continue
                    dbsUrl = stepHelper.data.dbsUrl
                    if dbsUrl not in pileupDatasets:
                        pileupDatasets[dbsUrl] = set()
                    for pileupType in pileupSection.listSections_():
                        datasets = getattr(getattr(stepHelper.data.pileup, pileupType), "dataset")
                        pileupDatasets[dbsUrl].update(datasets)

            pileupDatasets.update(self.listPileupDatasets(task))

        return pileupDatasets

    def listOutputProducingTasks(self, initialTask=None):
        """
        _listOutputProducingTasks_

        List the paths to any task capable of producing merged output
        """
        taskList = []

        if initialTask:
            taskIterator = initialTask.childTaskIterator()
        else:
            taskIterator = self.taskIterator()

        for task in taskIterator:
            for stepName in task.listAllStepNames():
                stepHelper = task.getStepHelper(stepName)
                if not getattr(stepHelper.data.output, "keep", True):
                    continue

                if stepHelper.stepType() == "CMSSW":
                    if stepHelper.listOutputModules():
                        taskList.append(task.getPathName())
                        break

            taskList.extend(self.listOutputProducingTasks(task))

        return taskList

    def setSubscriptionInformation(self, initialTask=None, custodialSites=None,
                                   nonCustodialSites=None, autoApproveSites=None,
                                   custodialSubType="Replica", nonCustodialSubType="Replica",
                                   custodialGroup="DataOps", nonCustodialGroup="DataOps",
                                   priority="Low", primaryDataset=None,
                                   useSkim=False, isSkim=False,
                                   dataTier=None, deleteFromSource=False):
        """
        _setSubscriptionInformation_

        Set the given subscription information for all datasets
        in the workload that match the given primaryDataset (if any)
        """

        if custodialSites and not isinstance(custodialSites, list):
            custodialSites = [custodialSites]
        if nonCustodialSites and not isinstance(nonCustodialSites, list):
            nonCustodialSites = [nonCustodialSites]
        if autoApproveSites and not isinstance(autoApproveSites, list):
            autoApproveSites = [autoApproveSites]

        if initialTask:
            taskIterator = initialTask.childTaskIterator()
        else:
            taskIterator = self.taskIterator()

        for task in taskIterator:
            task.setSubscriptionInformation(custodialSites, nonCustodialSites,
                                            autoApproveSites,
                                            custodialSubType, nonCustodialSubType,
                                            custodialGroup, nonCustodialGroup,
                                            priority, primaryDataset,
                                            useSkim, isSkim,
                                            dataTier, deleteFromSource)
            self.setSubscriptionInformation(task,
                                            custodialSites, nonCustodialSites,
                                            autoApproveSites,
                                            custodialSubType, nonCustodialSubType,
                                            custodialGroup, nonCustodialGroup,
                                            priority, primaryDataset,
                                            useSkim, isSkim,
                                            dataTier, deleteFromSource)

        return

    def getSubscriptionInformation(self, initialTask=None):
        """
        _getSubscriptionInformation_

        Get the subscription information for the whole workload, this is given by
        dataset and aggregated according to the information from each individual task
        See WMTask.WMTaskHelper.getSubscriptionInformation for the output structure
        """
        subInfo = {}

        # Add site lists without duplicates
        extendWithoutDups = lambda x, y: x + list(set(y) - set(x))
        # Choose the lowest priority
        solvePrioConflicts = lambda x, y: y if x == "High" or y == "Low" else x
        # Choose replica over move
        solveTypeConflicts = lambda x, y: y if x == "Move" else x
        # Choose the 'smallest' group (based on string comparison)
        solveGroupConflicts = lambda x, y: y if x > y else x
        # Always choose a logical AND
        solveDelConflicts = lambda x, y: x and y

        if initialTask:
            taskIterator = initialTask.childTaskIterator()
            subInfo = initialTask.getSubscriptionInformation()
        else:
            taskIterator = self.taskIterator()

        for task in taskIterator:
            taskSubInfo = self.getSubscriptionInformation(task)
            for dataset in taskSubInfo:
                if dataset in subInfo:
                    subInfo[dataset]["CustodialSites"] = extendWithoutDups(taskSubInfo[dataset]["CustodialSites"],
                                                                           subInfo[dataset]["CustodialSites"])
                    subInfo[dataset]["NonCustodialSites"] = extendWithoutDups(taskSubInfo[dataset]["NonCustodialSites"],
                                                                              subInfo[dataset]["NonCustodialSites"])
                    subInfo[dataset]["AutoApproveSites"] = extendWithoutDups(taskSubInfo[dataset]["AutoApproveSites"],
                                                                             subInfo[dataset]["AutoApproveSites"])
                    subInfo[dataset]["Priority"] = solvePrioConflicts(taskSubInfo[dataset]["Priority"],
                                                                      subInfo[dataset]["Priority"])
                    subInfo[dataset]["DeleteFromSource"] = solveDelConflicts(taskSubInfo[dataset]["DeleteFromSource"],
                                                                             subInfo[dataset]["DeleteFromSource"])
                    subInfo[dataset]["CustodialSubType"] = solveTypeConflicts(taskSubInfo[dataset]["CustodialSubType"],
                                                                              subInfo[dataset]["CustodialSubType"])
                    subInfo[dataset]["NonCustodialSubType"] = solveTypeConflicts(
                        taskSubInfo[dataset]["NonCustodialSubType"],
                        subInfo[dataset]["NonCustodialSubType"])
                    subInfo[dataset]["CustodialGroup"] = solveGroupConflicts(taskSubInfo[dataset]["CustodialGroup"],
                                                                             subInfo[dataset]["CustodialGroup"])
                    subInfo[dataset]["NonCustodialGroup"] = solveGroupConflicts(
                        taskSubInfo[dataset]["NonCustodialGroup"],
                        subInfo[dataset]["NonCustodialGroup"])
                else:
                    subInfo[dataset] = taskSubInfo[dataset]
                subInfo[dataset]["CustodialSites"] = list(
                    set(subInfo[dataset]["CustodialSites"]) - set(subInfo[dataset]["NonCustodialSites"]))

        return subInfo

    def getWorkloadOverrides(self):
        """
        _getWorkloadOverrides_

        Get the overrides config section
        of this workload, creates it if it doesn't exist
        """
        return self.data.section_('overrides')

    def setWorkloadOverrides(self, overrides):
        """
        _setWorkloadOverrides_

        Set the override parameters for all logArch steps
        in all tasks.
        """
        if overrides:
            for task in self.getAllTasks():
                for stepName in task.listAllStepNames():
                    stepHelper = task.getStepHelper(stepName)
                    if stepHelper.stepType() == "LogArchive":
                        for key, value in overrides.items():
                            stepHelper.addOverride(key, value)
            # save it at workload level as well
            for key, value in overrides.items():
                setattr(self.data.overrides, key, value)

        return

    def setBlockCloseSettings(self, blockCloseMaxWaitTime,
                              blockCloseMaxFiles, blockCloseMaxEvents,
                              blockCloseMaxSize):
        """
        _setBlockCloseSettings_

        Set the parameters that define when a block should be closed
        for this workload, they should all be defined so it is a single call
        """
        self.data.properties.blockCloseMaxWaitTime = blockCloseMaxWaitTime
        self.data.properties.blockCloseMaxFiles = blockCloseMaxFiles
        self.data.properties.blockCloseMaxEvents = blockCloseMaxEvents
        self.data.properties.blockCloseMaxSize = blockCloseMaxSize

    def getBlockCloseMaxWaitTime(self):
        """
        _getBlockCloseMaxWaitTime_

        Return the amount of time that a block should stay open
        for this workload before closing it in DBS
        """

        return getattr(self.data.properties, 'blockCloseMaxWaitTime', 66400)

    def getBlockCloseMaxSize(self):
        """
        _getBlockCloseMaxSize_

        Return the maximum size that a block from this workload should have
        """

        return getattr(self.data.properties, 'blockCloseMaxSize', 5000000000000)

    def getBlockCloseMaxEvents(self):
        """
        _blockCloseMaxEvents_

        Return the maximum number of events that a block from this workload
        should have
        """

        return getattr(self.data.properties, 'blockCloseMaxEvents', 25000000)

    def getBlockCloseMaxFiles(self):
        """
        _getBlockCloseMaxFiles_

        Return the maximum amount of files that a block from this workload
        should have
        """

        return getattr(self.data.properties, 'blockCloseMaxFiles', 500)

    def getUnmergedLFNBase(self):
        """
        _getUnmergedLFNBase_

        Get the unmerged LFN Base from properties
        """

        return getattr(self.data.properties, 'unmergedLFNBase', None)

    def getMergedLFNBase(self):
        """
        _getMergedLFNBase_

        Get the merged LFN Base from properties
        """

        return getattr(self.data.properties, 'mergedLFNBase', None)

    def getLFNBases(self):
        """
        _getLFNBases_

        Retrieve the LFN bases.  They are returned as a tuple with the merged
        LFN base first, followed by the unmerged LFN base.
        """
        return self.getMergedLFNBase(), self.getUnmergedLFNBase()

    def setRetryPolicy(self):
        """
        _setRetryPolicy_

        """
        pass

    def truncate(self, newWorkloadName, initialTaskPath, serverUrl,
                 databaseName, collectionName=None):
        """
        _truncate_

        Truncate a workflow so that it can be used for resubmission.  This will
        rename the workflow and set the task in the intitialTaskPath parameter
        to be the top level task.  This modifies the workflow in place.
        The input collection name can be specified otherwise it will default to
        the old workload name.
        """
        if not collectionName:
            collectionName = self.name()

        allTaskPaths = self.listAllTaskPathNames()
        newTopLevelTask = self.getTaskByPath(initialTaskPath)
        newTopLevelTask.addInputACDC(serverUrl, databaseName, collectionName,
                                     initialTaskPath)
        newTopLevelTask.setInputStep(None)
        workloadOwner = self.getOwner()
        self.setInitialJobCount(self.getInitialJobCount() + 10000000)
        newTopLevelTask.setSplittingParameters(collectionName=collectionName,
                                               filesetName=initialTaskPath,
                                               couchURL=serverUrl,
                                               couchDB=databaseName,
                                               owner=workloadOwner["name"],
                                               group=workloadOwner["group"],
                                               initial_lfn_counter=self.getInitialJobCount())

        for taskPath in allTaskPaths:
            if not taskPath.startswith(initialTaskPath) or taskPath == initialTaskPath:
                taskName = taskPath.split("/")[-1]
                if hasattr(self.data.tasks, taskName):
                    delattr(self.data.tasks, taskName)
                if taskName in self.data.tasks.tasklist:
                    self.data.tasks.tasklist.remove(taskName)

        self.setName(newWorkloadName)
        self.addTask(newTopLevelTask)
        newTopLevelTask.setTopOfTree()

        self.setWorkQueueSplitPolicy("ResubmitBlock",
                                     newTopLevelTask.jobSplittingAlgorithm(),
                                     newTopLevelTask.jobSplittingParameters())

        def adjustPathsForTask(initialTask, parentPath):
            """
            _adjustPathsForTask_

            Given an initial task and the path for that task set the path
            correctly for all of the children tasks.
            """
            for childTask in initialTask.childTaskIterator():
                childTask.setPathName("%s/%s" % (parentPath, childTask.name()))
                inputStep = childTask.getInputStep()
                if inputStep != None:
                    inputStep = inputStep.replace(parentPath, "/" + newWorkloadName)
                    childTask.setInputStep(inputStep)

                adjustPathsForTask(childTask, childTask.getPathName())

            return

        adjustPathsForTask(newTopLevelTask, "/%s/%s" % (newWorkloadName,
                                                        newTopLevelTask.name()))
        return

    def ignoreOutputModules(self, badModules, initialTask=None):
        """
        _ignoreOutputModules_

        If there is a list of ignored output modules the following must be done:
        - Trim the workload tree so that no task that depends on the merged output of the ignored modules
          exists in the tree, also eliminate the merge task for such modules
        - Add flags to make the runtime code ignore the files from this module so they are not
          staged out
        """

        if not badModules:
            return

        if initialTask:
            taskIterator = initialTask.childTaskIterator()
        else:
            taskIterator = self.taskIterator()

        for task in taskIterator:
            # Find the children tasks that have a bad output module as
            # input, disown them. Can't delete them on the spot, save the names in a list
            childTasksToDelete = []
            for childTask in task.childTaskIterator():
                taskInput = childTask.inputReference()
                inputOutputModule = getattr(taskInput, "outputModule", None)
                if inputOutputModule in badModules:
                    childTasksToDelete.append(childTask.name())

            # Now delete
            for childTaskName in childTasksToDelete:
                task.deleteChild(childTaskName)

            if childTasksToDelete:
                # Tell any CMSSW step to ignore the output modules
                for stepName in task.listAllStepNames():
                    stepHelper = task.getStepHelper(stepName)
                    if stepHelper.stepType() == "CMSSW":
                        stepHelper.setIgnoredOutputModules(badModules)
            # Go deeper in the tree
            self.ignoreOutputModules(badModules, task)

        return

    def setCMSSWVersions(self, cmsswVersion=None, globalTag=None,
                         scramArch=None, initialTask=None):
        """
        _setCMSSWVersions_

        Set the CMSSW version and the global tag for all CMSSW steps in the
        workload.
        """
        if initialTask:
            taskIterator = initialTask.childTaskIterator()
        else:
            taskIterator = self.taskIterator()

        for task in taskIterator:
            for stepName in task.listAllStepNames():
                stepHelper = task.getStepHelper(stepName)

                if stepHelper.stepType() == "CMSSW":
                    if cmsswVersion != None:
                        if scramArch != None:
                            stepHelper.cmsswSetup(cmsswVersion=cmsswVersion,
                                                  scramArch=scramArch)
                        else:
                            stepHelper.cmsswSetup(cmsswVersion=cmsswVersion)

                    if globalTag != None:
                        stepHelper.setGlobalTag(globalTag)

            self.setCMSSWVersions(cmsswVersion, globalTag, scramArch, task)

        return

    def getCMSSWVersions(self, initialTask=None):
        """
        _getCMSSWVersions_

        Return a list of all CMSSW releases being used in this workload.
        """
        versions = set()
        if initialTask:
            taskIterator = initialTask.childTaskIterator()
        else:
            taskIterator = self.taskIterator()

        for task in taskIterator:
            for stepName in task.listAllStepNames():
                stepHelper = task.getStepHelper(stepName)
                if stepHelper.stepType() == "CMSSW":
                    versions.add(stepHelper.getCMSSWVersion())
            versions.update(self.getCMSSWVersions(task))

        return list(versions)

    def generateWorkloadSummary(self):
        """
        _generateWorkloadSummary_

        Generates a dictionary with the following information:
        task paths
        ACDC
        input datasets
        output datasets

        Intended for use in putting WMSpec info into couch
        """
        summary = {'tasks': [],
                   'ACDC': {"collection": None, "filesets": {}},
                   'input': [],
                   'output': [],
                   'owner': {},
                   }

        summary['tasks'] = self.listAllTaskPathNames()
        summary['output'] = self.listOutputDatasets()
        summary['input'] = self.listInputDatasets()
        summary['owner'] = self.data.owner.dictionary_()
        summary['performance'] = {}
        for t in summary['tasks']:
            summary['performance'][t] = {}

        return summary

    def setupPerformanceMonitoring(self, softTimeout, gracePeriod):
        """
        _setupPerformanceMonitoring_

        Setups performance monitors for all tasks in the workflow
        """
        for task in self.getAllTasks():
            task.setPerformanceMonitor(softTimeout=softTimeout, gracePeriod=gracePeriod)

        return

    def listAllCMSSWConfigCacheIDs(self):
        """
        _listAllCMSSWConfigCacheIDs_

        Go through each task and check to see if we have a configCacheID
        """
        result = []
        for t in self.taskIterator():
            result.extend(t.getConfigCacheIDs())
        return result

    def setTrustLocationFlag(self, inputFlag=False, pileupFlag=False):
        """
        Set the input and the pileup flags in the top level tasks
        indicating that site lists should be used as location data

        The input data flag has to be set only for top level tasks, otherwise
        it affects where secondary jobs are meant to run.
        The pileup flag has to be set for all the tasks in the workload.

        Validate these parameters to make sure they are only set for workflows
        that require those type of input datasets (ACDCs are not validated)
        """
        isACDCWorkflow = False
        for task in self.taskIterator():
            if task.getInputACDC():
                isACDCWorkflow = True

        if inputFlag is True and isACDCWorkflow is False and not self.listInputDatasets():
            msg = "Setting TrustSitelists=True for workflows without input dataset is forbidden!"
            raise RuntimeError(msg)
        if pileupFlag is True and isACDCWorkflow is False and not self.listPileupDatasets():
            msg = "Setting TrustPUSitelists=True for workflows without pileup dataset is forbidden!"
            raise RuntimeError(msg)
        for task in self.getAllTasks(cpuOnly=True):
            if task.isTopOfTree():
                task.setTrustSitelists(inputFlag, pileupFlag)
            else:
                task.setTrustSitelists(False, pileupFlag)

        return

    def getTrustLocationFlag(self):
        """
        _getTrustLocationFlag_

        Get a tuple with the inputFlag and the pileupFlag values from
        the *top level* tasks that indicates whether the site lists should
        be trusted as the location for input and/or for the pileup data.
        """
        for task in self.getTopLevelTask():
            return task.getTrustSitelists()
        return {'trustlists': False, 'trustPUlists': False}

    def validateArgumentForAssignment(self, schema):
        specClass = loadSpecClassByType(self.getRequestType())
        argumentDefinition = specClass.getWorkloadAssignArgs()
        validateArgumentsUpdate(schema, argumentDefinition)
        return

    def updateArguments(self, kwargs):
        """
        set up all the argument related to assigning request.
        args are validated before update.
        assignment is common for all different types spec.

        Input data should have been validated already using
        validateArgumentForAssignment.
        """
        specClass = loadSpecClassByType(self.getRequestType())
        argumentDefinition = specClass.getWorkloadAssignArgs()
        setAssignArgumentsWithDefault(kwargs, argumentDefinition)

        if kwargs.get('RequestPriority') is not None and kwargs['RequestPriority'] != self.priority():
            self.setPriority(kwargs['RequestPriority'])
        else:
            # if it's the same, pop it out to avoid priority transition update
            kwargs.pop("RequestPriority", None)

        self.setWorkloadOverrides(kwargs["Override"])
        self.setSiteWhitelist(kwargs["SiteWhitelist"])
        self.setSiteBlacklist(kwargs["SiteBlacklist"])
        self.setTrustLocationFlag(inputFlag=strToBool(kwargs["TrustSitelists"]),
                                  pileupFlag=strToBool(kwargs["TrustPUSitelists"]))

        self.setMergeParameters(int(kwargs["MinMergeSize"]),
                                int(kwargs["MaxMergeSize"]),
                                int(kwargs["MaxMergeEvents"]))

        # FIXME not validated
        if kwargs.get("MergedLFNBase") and kwargs.get("UnmergedLFNBase"):
            self.setLFNBase(kwargs["MergedLFNBase"], kwargs["UnmergedLFNBase"])
        # Set ProcessingVersion and AcquisitionEra, which could be json encoded dicts
        # it should be processed once LFNBase are set
        if kwargs.get("AcquisitionEra") is not None:
            self.setAcquisitionEra(kwargs["AcquisitionEra"])
        if kwargs.get("ProcessingString") is not None:
            self.setProcessingString(kwargs["ProcessingString"])
        if kwargs.get("ProcessingVersion") is not None:
            self.setProcessingVersion(kwargs["ProcessingVersion"])

        self.setupPerformanceMonitoring(softTimeout=kwargs["SoftTimeout"],
                                        gracePeriod=kwargs["GracePeriod"])

        # Check whether we should check location for the data
        self.setAllowOpportunistic(allowOpport=strToBool(kwargs["AllowOpportunistic"]))

        # Block closing information
        self.setBlockCloseSettings(kwargs["BlockCloseMaxWaitTime"],
                                   kwargs["BlockCloseMaxFiles"],
                                   kwargs["BlockCloseMaxEvents"],
                                   kwargs["BlockCloseMaxSize"])

        self.setDashboardActivity(kwargs["Dashboard"])

        if kwargs.get("Memory") is not None:
            self.setMemory(kwargs.get("Memory"))
        if kwargs.get("Multicore") is not None:
            self.setCoresAndStreams(kwargs.get("Multicore"), kwargs.get("EventStreams"))

        # MUST be set after AcqEra/ProcStr/ProcVer
        if self.getRequestType() == "StepChain":
            self.setStepProperties(kwargs)
            self.updateStepParentageMap()
        elif self.getRequestType() == "TaskChain":
            # TODO: need to define proper task form maybe kwargs['Tasks']?
            self.setTaskProperties(kwargs)
            self.updateTaskParentageMap()

        # Since it lists the output datasets, it has to be done in the very end
        # Set phedex subscription information
        if kwargs.get("CustodialSites") or kwargs.get("NonCustodialSites"):
            self.setSubscriptionInformation(custodialSites=kwargs["CustodialSites"],
                                            nonCustodialSites=kwargs["NonCustodialSites"],
                                            autoApproveSites=kwargs["AutoApproveSubscriptionSites"],
                                            custodialSubType=kwargs["CustodialSubType"],
                                            nonCustodialSubType=kwargs["NonCustodialSubType"],
                                            custodialGroup=kwargs["CustodialGroup"],
                                            nonCustodialGroup=kwargs["NonCustodialGroup"],
                                            priority=kwargs["SubscriptionPriority"],
                                            deleteFromSource=kwargs["DeleteFromSource"])

        return

    def loadSpecFromCouch(self, couchurl, requestName):
        """
        This depends on PersitencyHelper.py saveCouch (That method better be decomposed)
        """
        return self.load("%s/%s/spec" % (couchurl, requestName))

    def setTaskPropertiesFromWorkload(self):
        """
        set task properties inherits from workload properties
        This is need to be called at the end of the buildWorkload function
        after all the tasks are added.
        It sets acquisitionEra, processingVersion, processingString,
        since those values are needed to be set for all the tasks in the workload
        TODO: need to force to call this function after task is added instead of
              rely on coder's won't forget to call this at the end of
              self.buildWorkload()
        """
        self.setAcquisitionEra(self.getAcquisitionEra())
        self.setProcessingVersion(self.getProcessingVersion())
        self.setProcessingString(self.getProcessingString())
        self.setLumiList(self.getLumiList())
        self.setPrepID(self.getPrepID())
        return


class WMWorkload(ConfigSection):
    """
    _WMWorkload_

    Request container

    """

    def __init__(self, name="test"):
        ConfigSection.__init__(self, name)
        self.objectType = self.__class__.__name__
        #  //persistent data
        # //
        # //
        self.section_("persistency")
        self.persistency.specUrl = None
        #  //
        # // request related information
        # //
        self.section_("request")
        self.request.priority = None  # what should be the default value
        #  //
        # // owner related information
        # //
        self.section_("owner")

        #  //
        # // Policies applied to this workload by the processing system
        # //
        self.section_("policies")
        self.policies.section_("start")
        self.policies.section_("end")
        self.policies.start.policyName = None
        self.policies.end.policyName = None

        #  //
        # // properties of the Workload and all tasks there-in
        # //
        self.section_("properties")
        self.properties.unmergedLFNBase = "/store/unmerged"
        self.properties.mergedLFNBase = "/store/data"
        self.properties.dashboardActivity = None
        self.properties.blockCloseMaxWaitTime = 66400
        self.properties.blockCloseMaxSize = 5000000000000
        self.properties.blockCloseMaxFiles = 500
        self.properties.blockCloseMaxEvents = 250000000
        self.properties.prepID = None

        # Overrides for this workload
        self.section_("overrides")

        #  //
        # // tasks
        # //
        self.section_("tasks")
        self.tasks.tasklist = []

        #  workload spec type
        self.requestType = ""
        self.dbsUrl = None

        self.sandbox = None
        self.initialJobCount = 0


def newWorkload(workloadName):
    """
    _newWorkload_

    Util method to create a new WMWorkload and wrap it in a helper

    """
    return WMWorkloadHelper(WMWorkload(workloadName))
