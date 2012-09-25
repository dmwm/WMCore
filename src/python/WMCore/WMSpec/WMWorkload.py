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
from WMCore.Lexicon import lfnBase, sanitizeURL
from WMCore.WMException import WMException

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
    def __init__(self, wmWorkload = None):
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

    def setOwner(self, name, ownerProperties = {'dn': 'DEFAULT'}):
        """
        _setOwner_
        sets the owner of wmspec.
        Takes a name as a mandatory argument, and then a dictionary of properties
        """
        self.data.owner.name = name
        self.data.owner.group = "undefined"

        if not type(ownerProperties) == dict:
            raise Exception("Someone is trying to setOwner without a dictionary")

        for key in ownerProperties.keys():
            setattr(self.data.owner, key, ownerProperties[key])

        return

    def setOwnerDetails(self, name, group, ownerProperties = {'dn': 'DEFAULT'}):
        """
        _setOwnerDetails_

        Set the owner, explicitly requiring the group and user arguments

        """
        self.data.owner.name = name
        self.data.owner.group = group
        if not type(ownerProperties) == dict:
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

        Retrieve a task with the given name.
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
        return [ t for t in self.taskIterator() if t.taskType() == ttype ]

    def getAllTasks(self):
        """
        _getAllTasks_

        Get all tasks from a workload
        """
        tasks = []
        pathNames = self.listAllTaskPathNames()
        for n in pathNames:
            tasks.append(self.getTaskByPath(taskPath = n))

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


    def setSiteWildcardsLists(self, siteWhitelist, siteBlacklist, wildcardDict):
        """
        _setSiteWildcardsLists_

        Given a whitelist and a blacklist that contain wildcards, and a
        list of wildcards, assign the whitelist and the blacklist so that
        they have the proper sites and not the wildcard symbols.

        Expects a wildcardDict containing all available wildcard symbols as keys
        and the list of sites corresponding to those keys as values.

        e.g.
        {'T2*': ['T2_US_UCSD', 'T2_US_UNL', 'T2_US_CIT'],
        'US*': ['T1_US_FNAL', 'T2_US_UCSD', 'T2_US_UNL', 'T2_US_CIT'],
        'T1*': ['T1_US_FNAL', 'T1_CH_CERN', 'T1_UK_RAL']}
        """
        newWhiteList = self.removeWildcardsFromList(siteList = siteWhitelist, wildcardDict = wildcardDict)
        newBlackList = self.removeWildcardsFromList(siteList = siteBlacklist, wildcardDict = wildcardDict)

        for site in newWhiteList:
            if '*' in site:
                msg = "Invalid wildcard site %s in site whitelist!" % site
                #logging.error(msg)
                raise WMWorkloadException(msg)
        for site in newBlackList:
            if '*' in site:
                msg = "Invalid wildcard site %s in site blacklist!" % site
                #logging.error(msg)
                raise WMWorkloadException(msg)

        self.setSiteWhitelist(siteWhitelist = newWhiteList)
        self.setSiteBlacklist(siteBlacklist = newBlackList)

        return

    def removeWildcardsFromList(self, siteList, wildcardDict = {}):
        """
        _removeWildcardsFromList_

        Given a list of sites, remove any of the wildcards
        that are in site.wildcards and replace them with the
        sites that you picked out of SiteDB.
        """

        deleteKeys = []
        for s in siteList:
            if s in wildcardDict.keys():
                deleteKeys.append(s)

        for s in deleteKeys:
            keyList = wildcardDict[s]
            for site in keyList:
                if site not in siteList:
                    siteList.append(site)
            siteList.remove(s)

        return siteList

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
                task.setSplittingParameters(runWhitelist = runWhitelist)
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

                if stepHelper.stepType() == "CMSSW" or \
                       stepHelper.stepType() == "MulticoreCMSSW":
                    for outputModuleName in stepHelper.listOutputModules():
                        outputModule = stepHelper.getOutputModule(outputModuleName)
                        filterName = getattr(outputModule, "filterName", None)

                        if filterName:
                            processedDataset = "%s-%s-%s" % (task.getAcquisitionEra(),
                                                             filterName,
                                                             task.getProcessingVersion())
                            processingString = "%s-%s" % (filterName,
                                                          task.getProcessingVersion())
                        else:
                            processedDataset = "%s-%s" % (task.getAcquisitionEra(),
                                                          task.getProcessingVersion())
                            processingString = task.getProcessingVersion()

                        unmergedLFN = "%s/%s/%s/%s/%s" % (self.data.properties.unmergedLFNBase,
                                                          task.getAcquisitionEra(),
                                                          getattr(outputModule, "primaryDataset"),
                                                          getattr(outputModule, "dataTier"),
                                                          processingString)
                        mergedLFN = "%s/%s/%s/%s/%s" % (self.data.properties.mergedLFNBase,
                                                        task.getAcquisitionEra(),
                                                        getattr(outputModule, "primaryDataset"),
                                                        getattr(outputModule, "dataTier"),
                                                        processingString)
                        lfnBase(unmergedLFN)
                        lfnBase(mergedLFN)
                        setattr(outputModule, "processedDataset", processedDataset)

                        # For merge tasks, we want all output to go to the merged LFN base.
                        if taskType == "Merge":
                            setattr(outputModule, "lfnBase", mergedLFN)
                            setattr(outputModule, "mergedLFNBase", mergedLFN)

                            if getattr(outputModule, "dataTier") in ["DQM", "DQMROOT"]:
                                datasetName = "/%s/%s/%s" % (getattr(outputModule, "primaryDataset"),
                                                             processedDataset,
                                                             getattr(outputModule, "dataTier"))
                                self.updateDatasetName(task, datasetName)
                        else:
                            setattr(outputModule, "lfnBase", unmergedLFN)
                            setattr(outputModule, "mergedLFNBase", mergedLFN)

            task.setTaskLogBaseLFN(self.data.properties.unmergedLFNBase)
            self.updateLFNsAndDatasets(task)

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

                    if stepHelper.stepType() == "CMSSW" or \
                           stepHelper.stepType() == "MulticoreCMSSW":
                        cmsswHelper = stepHelper.getTypeHelper()
                        cmsswHelper.setDatasetName(datasetName)

        return

    def setAcquisitionEra(self, acquisitionEras, initialTask = None,
                          parentAcquisitionEra = None):
        """
        _setAcquistionEra_

        Change the acquisition era for all tasks in the spec and then update
        all of the output LFNs and datasets to use the new acquisition era.
        """
        if initialTask:
            taskIterator = initialTask.childTaskIterator()
        else:
            taskIterator = self.taskIterator()

        for task in taskIterator:
            if type(acquisitionEras) == dict:
                task.setAcquisitionEra(acquisitionEras.get(task.name(),
                                       parentAcquisitionEra))
                self.setAcquisitionEra(acquisitionEras, task,
                                       acquisitionEras.get(task.name(),
                                       parentAcquisitionEra))
            else:
                task.setAcquisitionEra(acquisitionEras)
                self.setAcquisitionEra(acquisitionEras, task)

        if not initialTask:
            self.updateLFNsAndDatasets()
        return

    def setProcessingVersion(self, processingVersions, initialTask = None,
                             parentProcessingVersion = None):
        """
        _setProcessingVersion_

        Change the processing version for all tasks in the spec and then update
        all of the output LFNs and datasets to use the new processing version.
        """
        if initialTask:
            taskIterator = initialTask.childTaskIterator()
        else:
            taskIterator = self.taskIterator()

        for task in taskIterator:
            if type(processingVersions) == dict:
                task.setProcessingVersion(processingVersions.get(task.name(),
                                          parentProcessingVersion))
                self.setProcessingVersion(processingVersions, task,
                                          processingVersions.get(task.name(),
                                          parentProcessingVersion))
            else:
                task.setProcessingVersion(processingVersions)
                self.setProcessingVersion(processingVersions, task)

        if not initialTask:
            self.updateLFNsAndDatasets()
        return

    def getAcquisitionEra(self):
        """
        _getAcquisitionEra_

        Get the acquisition era
        """

        topTasks = self.getTopLevelTask()

        if len(topTasks):
            return topTasks[0].getAcquisitionEra()

    def getProcessingVersion(self):
        """
        _getProcessingVersion_

        Get the processingVersion
        """

        topTasks = self.getTopLevelTask()

        if len(topTasks):
            return topTasks[0].getProcessingVersion()

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

    def setCustodialSite(self, siteName):
        """
        _setCustodialSite_

        Set the custody site for all datasets produced
        by this workflow
        """
        self.data.properties.custodialSite = siteName
        return

    def getCustodialSite(self):
        """
        _getCustodialSite_

        Get the custodial site for this workflow
        """
        return getattr(self.data.properties, 'custodialSite', None)

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
            for stepName in task.listAllStepNames():
                stepHelper = task.getStepHelper(stepName)
                if stepHelper.stepType() == "StageOut" and stepHelper.minMergeSize() != -1:
                    stepHelper.setMinMergeSize(minSize, maxEvents)
                
            if task.taskType() == "Merge":
                task.setSplittingParameters(min_merge_size = minSize,
                                            max_merge_size = maxSize,
                                            max_merge_events = maxEvents)

            self.setMergeParameters(minSize, maxSize, maxEvents, task)

        return

    def setWorkQueueSplitPolicy(self, policyName, splitAlgo, splitArgs):
        """
        _setWorkQueueSplitPolicy_

        Set the WorkQueue split policy.
        policyName should be either 'DatasetBlock', 'Dataset', 'MonteCarlo' 'Block'
        different policy could be added in the workqueue plug in.
        """
        SplitAlgoToStartPolicy = {"FileBased": ["NumberOfFiles"],
                                  "EventBased": ["NumberOfEvents",
                                                 "NumberOfEventsPerLumi"],
                                  "LumiBased": ["NumberOfLumis"]}
        SplitAlgoToArgMap = {"NumberOfFiles": "files_per_job",
                             "NumberOfEvents": "events_per_job",
                             "NumberOfLumis": "lumis_per_job",
                             "NumberOfEventsPerLumi": "events_per_lumi"}
        startPolicyArgs = {}

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

    def setJobSplittingParameters(self, taskPath, splitAlgo, splitArgs):
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
                else:
                    mergeAlgo = "ParentlessMergeBySize"

                childSplitParams = childTask.jobSplittingParameters()
                minMergeSize = childSplitParams["min_merge_size"]
                maxMergeEvents = childSplitParams["max_merge_events"]
                del childSplitParams["algorithm"]
                del childSplitParams["siteWhitelist"]
                del childSplitParams["siteBlacklist"]
                childTask.setSplittingAlgorithm(mergeAlgo, **childSplitParams)

        # Set the splitting algorithm for the task.  If the split algo is
        # EventBased, we need to disable straight to merge.  If this isn't an
        # EventBased algo we need to enable straight to merge.
        taskHelper.setSplittingAlgorithm(splitAlgo, **splitArgs)
        for stepName in taskHelper.listAllStepNames():
            stepHelper = taskHelper.getStepHelper(stepName)
            if stepHelper.stepType() == "StageOut":
                if splitAlgo != "EventBased" and minMergeSize:
                    stepHelper.setMinMergeSize(minMergeSize, maxMergeEvents)
                else:
                    stepHelper.disableStraightToMerge()
            if taskHelper.isTopOfTree() and stepHelper.stepType() == "CMSSW" \
                and taskHelper.taskType() == "Production":
                stepHelper.setEventsPerLumi(splitArgs.get("events_per_lumi",
                                                          None))
        return

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

                if not getattr(stepHelper.data.output, "keep", True):
                    continue

                if stepHelper.stepType() == "CMSSW" or \
                       stepHelper.stepType() == "MulticoreCMSSW":
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
                 databaseName):
        """
        _truncate_

        Truncate a workflow so that it can be used for resubmission.  This will
        rename the workflow and set the task in the intitialTaskPath parameter
        to be the top level task.  This modifies the workflow in place.
        """
        allTaskPaths = self.listAllTaskPathNames()
        newTopLevelTask = self.getTaskByPath(initialTaskPath)
        newTopLevelTask.addInputACDC(serverUrl, databaseName, self.name(),
                                     initialTaskPath)
        newTopLevelTask.setInputStep(None)
        workloadOwner = self.getOwner()
        self.setInitialJobCount(self.getInitialJobCount() + 10000000)
        newTopLevelTask.setSplittingParameters(collectionName = self.name(),
                                               filesetName = initialTaskPath,
                                               couchURL = serverUrl,
                                               couchDB = databaseName,
                                               owner = workloadOwner["name"],
                                               group = workloadOwner["group"],
                                               initial_lfn_counter = self.getInitialJobCount())

        cleanupTask = None
        parentTaskPath = "/".join(initialTaskPath.split("/")[:-1])
        if not newTopLevelTask.isTopOfTree():
            parentTask = self.getTaskByPath(parentTaskPath)
            for childTask in parentTask.childTaskIterator():
                if childTask.taskType() == "Cleanup" and \
                       childTask.data.input.outputModule == newTopLevelTask.data.input.outputModule:
                    cleanupTask = childTask
                    break

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
        if cleanupTask:
            self.addTask(cleanupTask)
            cleanupTask.setTopOfTree()

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
                    inputStep = inputStep.replace(parentTaskPath, "/" + newWorkloadName)
                    childTask.setInputStep(inputStep)
                    
                adjustPathsForTask(childTask, childTask.getPathName())

            return

        adjustPathsForTask(newTopLevelTask, "/%s/%s" % (newWorkloadName,
                                                        newTopLevelTask.name()))
        return

    def setCMSSWParams(self, cmsswVersion = None, globalTag = None,
                       scramArch = None, initialTask = None):
        """
        _setCMSSWVersion_

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

                if stepHelper.stepType() == "CMSSW" or \
                       stepHelper.stepType() == "MulticoreCMSSW":
                    if cmsswVersion != None:
                        if scramArch != None:
                            stepHelper.cmsswSetup(cmsswVersion = cmsswVersion,
                                                  scramArch = scramArch)
                        else:
                            stepHelper.cmsswSetup(cmsswVersion = cmsswVersion)
                        
                    if globalTag != None:
                        stepHelper.setGlobalTag(globalTag)

            self.setCMSSWParams(cmsswVersion, globalTag, scramArch, task)

        return

    def getCMSSWVersions(self):
        """
        _getCMSSWVersions_

        Pull out any CMSSW Versions we might be looking for.
        """
        versions = []

        for task in self.taskIterator():
            for stepName in task.listAllStepNames():
                
                stepHelper = task.getStepHelper(stepName)
                if stepHelper.stepType() != "CMSSW" and stepHelper.stepType() != "MulticoreCMSSW":
                    continue
                version = stepHelper.getCMSSWVersion()
                if not version in versions:
                    versions.append(version)
        return versions
        

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
                   'ACDC': { "collection" : None, "filesets": {} },
                   'input': [],
                   'output': [],
                   'owner' : {},
                   }
    
        summary['tasks']  = self.listAllTaskPathNames()
        summary['output'] = self.listOutputDatasets()
        summary['input']  = self.listInputDatasets()
        summary['owner'] = self.data.owner.dictionary_()
        summary['performance'] = {}
        for t in summary['tasks']:
            summary['performance'][t] = {}
        
        return summary

    def setupPerformanceMonitoring(self, maxRSS, maxVSize, softTimeout,
                                         gracePeriod):
        """
        _setupPerformanceMonitoring_
        
        Setups performance monitors for all tasks in the workflow
        """
        for task in self.getAllTasks():
            task.setPerformanceMonitor(maxRSS = maxRSS, maxVSize = maxVSize,
                                       softTimeout = softTimeout,
                                       gracePeriod = gracePeriod)

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
        self.properties.unmergedLFNBase = "/store/unmerged"
        self.properties.mergedLFNBase = "/store/data"
        self.properties.dashboardActivity = None

        #  //
        # // tasks
        #//
        self.section_("tasks")
        self.tasks.tasklist = []

        self.sandbox = None
        self.initialJobCount = 0

def newWorkload(workloadName):
    """
    _newWorkload_

    Util method to create a new WMWorkload and wrap it in a helper

    """
    return WMWorkloadHelper(WMWorkload(workloadName))
