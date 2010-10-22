#!/usr/bin/env python
"""
_WMWorkload_

Request level processing specification, acts as a container of a set
of related tasks.
"""

import logging
import os

from WMCore.Configuration import ConfigSection
from WMCore.WMSpec.ConfigSectionTree import findTop
from WMCore.WMSpec.Persistency import PersistencyHelper
from WMCore.WMSpec.WMTask import WMTask, WMTaskHelper

parseTaskPath = lambda p: [ x for x in p.split('/') if x.strip() != '' ]

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
        #TODO: Replace with real exception class
        raise RuntimeError, msg

    objType = getattr(topNode, "objectType")
    if objType != "WMWorkload":
        msg = "Top level object is not a WMWorkload: %s" % objType
        #TODO: Replace with real exception class
        raise RuntimeError, msg

    return WMWorkloadHelper(topNode)


class WMWorkloadHelper(PersistencyHelper):
    """
    _WMWorkloadHelper_

    Methods & Utils for working with a WMWorkload instance.
    """
    def __init__(self, wmWorkload = None):
        self.data = wmWorkload

    def setSpecUrl(self, url):
        self.data.persistency.specUrl = url

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

    def getOwner(self):
        """
        _getOwner_
        return owner information
        """
        return self.data.owner.dictionary_()

    def setOwner(self, name, ownerProperties = {}):
        """
        _setOwner_
        sets the owner of wmspec.
        Takes a name as a mandatory argument, and then a dictionary of properties
        """
        self.data.owner.name = name

        if not type(ownerProperties) == dict:
            raise Exception("Someone is trying to setOwner without a dictionary")

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
        
    
    def priority(self):
        """
        _priority_
        return priorty of workload
        """
        return self.data.request.priority

    def setStartPolicy(self, policyName, **params):
        """
        _setStartPolicy_

        Set the Start policy and its parameters
        """
        self.data.policies.start.policyName = policyName
        [ setattr(self.data.policies.start, key, val)
          for key, val in params.items() ]

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
        [ setattr(self.data.policies.end, key, val)
          for key, val in params.items() ]

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

        Get Toplevel task by name

        """
        task = getattr(self.data.tasks, taskName, None)
        if task == None:
            return None
        return WMTaskHelper(task)

    def getTaskByPath(self, taskPath):
        """
        _getTask_

        Get a task instance based on the path name

        """
        mapping = {}
        for t in self.taskIterator():
            [mapping.__setitem__(x.getPathName, x.name())
             for x in t.taskIterator()]

        taskList = parseTaskPath(taskPath)

        if taskList[0] != self.name(): # should always be workload name first
            msg = "Workload name does not match:\n"
            msg += "requested name %s from workload %s " % (taskList[0],
                                                            self.name())
            raise RuntimeError, msg
        if len(taskList) < 2:
            # path should include workload and one task
            msg = "Task Path does not contain a top level task:\n"
            msg += taskPath
            raise RuntimeError, msg


        topTask = self.getTask(taskList[1])
        if topTask == None:
            msg = "Task /%s/%s Not Found in Workload" % (taskList[0],
                                                         taskList[1])
            raise RuntimeError, msg
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
        for t in self.taskIterator():
			print t
			print t.taskType()
        return [ t for t in self.taskIterator() if t.taskType() == ttype ]
        

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
            raise RuntimeError, msg
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
            raise RuntimeError, msg
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

    def setSiteWhitelist(self, siteWhitelist, initialTask = None):
        """
        _setSiteWhitelist_

        Set the site white list for all tasks in the workload.
        """
        if type(siteWhitelist) != type([]):
            siteWhitelist = [siteWhitelist]
            
        if initialTask:
            taskIterator = initialTask.childTaskIterator()
        else:
            taskIterator = self.taskIterator()
        
        for task in taskIterator:
            task.setSiteWhitelist(siteWhitelist)
            self.setSiteWhitelist(siteWhitelist, task)

        return

    def setSiteBlacklist(self, siteBlacklist, initialTask = None):
        """
        _setSiteBlacklist_

        Set the site black list for all tasks in the workload.
        """
        if type(siteBlacklist) != type([]):
            siteBlacklist = [siteBlacklist]
            
        if initialTask:
            taskIterator = initialTask.childTaskIterator()
        else:
            taskIterator = self.taskIterator()
        
        for task in taskIterator:
            task.setSiteBlacklist(siteBlacklist)
            self.setSiteBlacklist(siteBlacklist, task)
            
        return

    def setBlockWhitelist(self, blockWhitelist, initialTask = None):
        """
        _setBlockWhitelist_

        Set the block white list for all tasks that have an input dataset
        defined.
        """
        if type(blockWhitelist) != type([]):
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

    def setBlockBlacklist(self, blockBlacklist, initialTask = None):
        """
        _setBlockBlacklist_

        Set the block black list for all tasks that have an input dataset
        defined.
        """
        if type(blockBlacklist) != type([]):
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

    def setRunWhitelist(self, runWhitelist, initialTask = None):
        """
        _setRunWhitelist_

        Set the run white list for all tasks that have an input dataset defined.
        """
        if type(runWhitelist) != type([]):
            runWhitelist = [runWhitelist]
            
        if initialTask:
            taskIterator = initialTask.childTaskIterator()
        else:
            taskIterator = self.taskIterator()
        
        for task in taskIterator:
            if task.getInputDatasetPath():
                task.setInputRunWhitelist(runWhitelist)
            self.setRunWhitelist(runWhitelist, task)

        return

    def setRunBlacklist(self, runBlacklist, initialTask = None):
        """
        _setRunBlacklist_

        Set the run black list for all tasks that have an input dataset defined.
        """
        if type(runBlacklist) != type([]):
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

    def updateLFNsAndDatasets(self, initialTask = None):
        """
        _updateLFNsAndDatasets_

        Update all the output LFNs and data names for all tasks in the workflow.
        This needs to be called after updating the acquisition era, processing
        version or merged/unmerged lfn base.
        """
        if initialTask:
            taskIterator = initialTask.childTaskIterator()
        else:
            taskIterator = self.taskIterator()
            
        for task in taskIterator:
            taskType = task.taskType()
            for stepName in task.listAllStepNames():
                stepHelper = task.getStepHelper(stepName)

                if stepHelper.stepType() == "CMSSW":
                    for outputModuleName in stepHelper.listOutputModules():
                        outputModule = stepHelper.getOutputModule(outputModuleName)
                        filterName = getattr(outputModule, "filterName", None)

                        if filterName:
                            processedDataset = "%s-%s-%s" % (self.data.properties.acquisitionEra,
                                                             filterName,
                                                             self.data.properties.processingVersion)
                        else:
                            processedDataset = "%s-%s" % (self.data.properties.acquisitionEra,
                                                          self.data.properties.processingVersion)

                        unmergedLFN = "%s/%s/%s" % (self.data.properties.unmergedLFNBase,
                                                    getattr(outputModule, "dataTier"),
                                                    processedDataset)
                        mergedLFN = "%s/%s/%s" % (self.data.properties.mergedLFNBase,
                                                  getattr(outputModule, "dataTier"),
                                                  processedDataset)                        
                        setattr(outputModule, "processedDataset", processedDataset)

                        # For merge tasks, we want all output to go to the merged LFN base.
                        if taskType == "Merge":
                            setattr(outputModule, "lfnBase", mergedLFN)
                            setattr(outputModule, "mergedLFNBase", mergedLFN)
                        else:
                            setattr(outputModule, "lfnBase", unmergedLFN)
                            setattr(outputModule, "mergedLFNBase", mergedLFN)
                            
            task.setTaskLogBaseLFN(self.data.properties.unmergedLFNBase)
            self.updateLFNsAndDatasets(task)

        return
    
    def setAcquisitionEra(self, acquisitionEra):
        """
        _setAcquistionEra_

        Change the acquisition era for all tasks in the spec and then update
        all of the output LFNs and datasets to use the new acquisition era.
        """
        self.data.properties.acquisitionEra = acquisitionEra
        self.updateLFNsAndDatasets()
        return

    def setProcessingVersion(self, processingVersion):
        """
        _setProcessingVersion_

        Change the processing version for all tasks in the spec and then update
        all of the output LFNs and datasets to use the new processing version.
        """
        self.data.properties.processingVersion = processingVersion
        self.updateLFNsAndDatasets()
        return

    def setLFNBase(self, mergedLFNBase, unmergedLFNBase):
        """
        _setLFNBase_

        Set the merged and unmerged base LFNs for all tasks.  Update all of the
        output LFNs to use them.
        """
        self.data.properties.mergedLFNBase = mergedLFNBase
        self.data.properties.unmergedLFNBase = unmergedLFNBase
        self.updateLFNsAndDatasets()
        return

    def setMergeParameters(self, minSize, maxSize, maxEvents,
                           initialTask = None):
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
            if task.taskType() == "Merge":
                task.setSplittingParameters(min_merge_size = minSize,
                                            max_merge_size = maxSize,
                                            max_merge_events = maxEvents)
            for stepName in task.listAllStepNames():
                stepHelper = task.getStepHelper(stepName)                
                if stepHelper.stepType() == "CMSSW":
                    stepHelper.setMinMergeSize(minSize)

            self.setMergeParameters(minSize, maxSize, maxEvents, task)

        return

    def setWorkQueueSplitPolicy(self, policyName, splitAlgo, splitArgs):
        """
        _setWorkQueueSplitPolicy_
        
        Set the WorkQueue split policy.
        policyName should be either 'DatasetBlock', 'Dataset', 'MonteCarlo' 'Block'
        different policy could be added in the workqueue plug in.
        """
        SplitAlgoToStartPolicy = {"FileBased": "NumberOfFiles",
                                  "EventBased": "NumberOfEvents",
                                  "LumiBased": "NumberOfLumis" }
        SplitAlgoToArgMap = {"NumberOfFiles": "files_per_job",
                             "NumberOfEvents": "events_per_job",
                             "NumberOfLumis": "lumis_per_job"}

        sliceType = SplitAlgoToStartPolicy.get(splitAlgo, "NumberOfFiles")
        sliceSize = splitArgs.get(SplitAlgoToArgMap[sliceType], 1)

        self.setStartPolicy(policyName, SliceType = sliceType, SliceSize = sliceSize)
        self.setEndPolicy("SingleShot")
        return
        
    def setJobSplittingParameters(self, taskPath, splitAlgo, splitArgs):
        """
        _setJobSplittingParameters_

        Update the job splitting algorithm and arguments for the given task.
        """
        taskHelper = self.getTaskByPath(taskPath)
        if taskHelper == None:
            return

        if taskHelper.isTopOfTree():
            if taskHelper.taskType() == "Production":
                self.setWorkQueueSplitPolicy("MonteCarlo", splitAlgo, splitArgs)
            else:
                self.setWorkQueueSplitPolicy("Block", splitAlgo, splitArgs)                

        # There are currently two merge algorithms in WMBS.  WMBSMergeBySize
        # will reassemble the parent file.  This is only necessary for
        # EventBased processing where we break up lumi sections.  Everything
        # else can use ParentlessMergeBySize which won't reassemble parents.
        # Everything defaults to ParentlessMergeBySize as it is much less load
        # on the database.
        for childTask in taskHelper.childTaskIterator():
            if childTask.taskType() == "Merge":
                if splitAlgo == "EventBased" and taskHelper.taskType() != "Production":
                    mergeAlgo = "WMBSMergeBySize"
                else:
                    mergeAlgo = "ParentlessMergeBySize"

                childSplitParams = childTask.jobSplittingParameters()
                del childSplitParams["algorithm"]
                del childSplitParams["siteWhitelist"]
                del childSplitParams["siteBlacklist"]
                childTask.setSplittingAlgorithm(mergeAlgo, **childSplitParams)

        taskHelper.setSplittingAlgorithm(splitAlgo, **splitArgs)
        return

    def setTaskTimeOut(self, taskPath, taskTimeOut):
        """
        _setTaskTimeOut_

        Set the timeout value for the given tasks in the workload.
        """
        taskHelper = self.getTaskByPath(taskPath)
        if taskHelper == None:
            return
        
        taskHelper.setTaskTimeOut(taskTimeOut)
        return

    def listTimeOutsByTask(self, initialTask = None):
        """
        _listTimeOutsByTask_

        Create a dictionary that maps task names to timeouts.
        """
        output = {}
        
        if initialTask:
            taskIterator = initialTask.childTaskIterator()
        else:
            taskIterator = self.taskIterator()
            
        for task in taskIterator:        
            output[task.getPathName()] = task.getTaskTimeOut()
            output.update(self.listTimeOutsByTask(task))

        return output

    def listJobSplittingParametersByTask(self, initialTask = None):
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
            taskParams = task.jobSplittingParameters()
            del taskParams["siteWhitelist"]
            del taskParams["siteBlacklist"]
            output[taskName] = taskParams
            output[taskName]["type"] = task.taskType()
            output.update(self.listJobSplittingParametersByTask(task))

        return output
            
    def listOutputDatasets(self, initialTask = None):
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

                if stepHelper.stepType() == "CMSSW":
                    for outputModuleName in stepHelper.listOutputModules():
                        outputModule = stepHelper.getOutputModule(outputModuleName)
                        outputDataset = "/%s/%s/%s" % (outputModule.primaryDataset,
                                                       outputModule.processedDataset,
                                                       outputModule.dataTier)
                        if outputDataset not in outputDatasets:
                            outputDatasets.append(outputDataset)

            moreDatasets = self.listOutputDatasets(task)
            for anotherDataset in moreDatasets:
                if anotherDataset not in outputDatasets:
                    outputDatasets.append(anotherDataset)

        return outputDatasets

    def getLFNBases(self):
        """
        _getLFNBases_

        Retrieve the LFN bases.  They are returned as a tuple with the merged
        LFN base first, followed by the unmerged LFN base.
        """
        return (self.data.properties.mergedLFNBase,
                self.data.properties.unmergedLFNBase)

    def setRetryPolicy(self):
        """
        _setRetryPolicy_

        """
        pass

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
        #//
        self.section_("persistency")
        self.persistency.specUrl = None
        #  //
        # // request related information
        #//
        self.section_("request")
        self.request.priority = None # what should be the default value
        #  //
        # // owner related information
        #//
        self.section_("owner")

        #  //
        # // Policies applied to this workload by the processing system
        #//
        self.section_("policies")
        self.policies.section_("start")
        self.policies.section_("end")
        self.policies.start.policyName = None
        self.policies.end.policyName = None

        #  //
        # // properties of the Workload and all tasks there-in
        #//
        self.section_("properties")
        self.properties.acquisitionEra = None
        self.properties.processingVersion = None
        self.properties.unmergedLFNBase = "/store/unmerged"
        self.properties.mergedLFNBase = "/store/data"

        #  //
        # // tasks
        #//
        self.section_("tasks")
        self.tasks.tasklist = []
        
        self.sandbox = None

def newWorkload(workloadName):
    """
    _newWorkload_

    Util method to create a new WMWorkload and wrap it in a helper

    """
    return WMWorkloadHelper(WMWorkload(workloadName))
