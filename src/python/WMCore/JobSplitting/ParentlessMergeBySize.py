#!/usr/bin/env python
"""
_ParentlessMergeBySize_

WMBS merging that ignores file parents.
"""




import threading

from WMCore.WMBS.File import File
from WMCore.DataStructs.Run import Run

from WMCore.DAOFactory import DAOFactory
from WMCore.JobSplitting.JobFactory import JobFactory

def fileCompare(a, b):
    """
    _fileCompare_

    Compare two files based on their "file_first_event" attribute.
    """
    if a["file_run"] > b["file_run"]:
        return 1
    elif a["file_run"] == b["file_run"]:
        if a["file_lumi"] > b["file_lumi"]:
            return 1
        elif a["file_lumi"] == b["file_lumi"]:
            if a["file_first_event"] > b["file_first_event"]:
                return 1
            if a["file_first_event"] == b["file_first_event"]:
                return 0

    return -1

class ParentlessMergeBySize(JobFactory):
    def defineFileGroups(self, mergeableFiles):
        """
        _defineFileGroups_

        Group mergeable files by their SE name so that we don't try to merge
        together files on different SEs.
        """
        fileGroups = {}

        for mergeableFile in mergeableFiles:
            if not fileGroups.has_key(mergeableFile["se_name"]):
                fileGroups[mergeableFile["se_name"]] = []

            fileGroups[mergeableFile["se_name"]].append(mergeableFile)

        return fileGroups

    def createMergeJob(self, mergeableFiles):
        """
        _createMergeJob_

        Create a merge job for the given merge units.  All the files contained
        in the merge units will be associated to the job.
        """
        self.newJob(name = self.getJobName())
        mergeableFiles.sort(fileCompare)

        for file in mergeableFiles:
            newFile = File(id = file["file_id"], lfn = file["file_lfn"],
                           events = file["file_events"])

            # The WMBS data structure puts locations that are passed in through
            # the constructor in the "newlocations" attribute.  We want these to
            # be in the "locations" attribute so that they get picked up by the
            # job submitter.
            newFile["locations"] = set([file["se_name"]])
            newFile.addRun(Run(file["file_run"], file["file_lumi"]))                                                                                    
            self.currentJob.addFile(newFile)
    
    def defineMergeJobs(self, mergeableFiles):
        """
        _defineMergeJobs_

        Try to define merge jobs that meet the size requirement.
        """
        mergeJobFileSize = 0
        mergeJobEvents = 0
        mergeJobFiles = []

        mergeableFiles.sort(fileCompare)

        for mergeableFile in mergeableFiles:
            if mergeableFile["file_size"] > self.maxMergeSize or \
                   mergeableFile["file_events"] > self.maxMergeEvents:
                self.createMergeJob([mergeableFile])
                continue
            elif mergeableFile["file_size"] + mergeJobFileSize > self.maxMergeSize or \
                     mergeableFile["file_events"] + mergeJobEvents > self.maxMergeEvents:
                if mergeJobFileSize > self.minMergeSize or \
                       self.forceMerge == True:
                    self.createMergeJob(mergeJobFiles)
                    mergeJobFileSize = 0
                    mergeJobEvents = 0
                    mergeJobFiles = []
                else:
                    continue
                    
            mergeJobFiles.append(mergeableFile)
            mergeJobFileSize += mergeableFile["file_size"]
            mergeJobEvents += mergeableFile["file_events"]
                        
        if mergeJobFileSize > self.minMergeSize or self.forceMerge == True:
            if len(mergeJobFiles) > 0:
                self.createMergeJob(mergeJobFiles)
                                
        return
    
    def algorithm(self, *args, **kwargs):
        """
        _algorithm_

        Try to merge any available files for the subscription provided.  This
        accepts the following keyword arguments:
          max_merge_size - The maximum size of merged files
          min_merge_size - The minimum size of merged files
          max_merge_events - The maximum number of events in a merge file
        """
        self.maxMergeSize = int(kwargs.get("max_merge_size", 1000000000))
        self.minMergeSize = int(kwargs.get("min_merge_size", 1048576))
        self.maxMergeEvents = int(kwargs.get("max_merge_events", 50000))

        self.subscription["fileset"].load()

        if self.subscription["fileset"].open == True:
            self.forceMerge = False
        else:
            self.forceMerge = True

        myThread = threading.currentThread()
        daoFactory = DAOFactory(package = "WMCore.WMBS",
                                logger = myThread.logger,
                                dbinterface = myThread.dbi)
        
        mergeDAO = daoFactory(classname = "Subscriptions.GetFilesForParentlessMerge")
        mergeableFiles = mergeDAO.execute(self.subscription["id"])

        groupedFiles = self.defineFileGroups(mergeableFiles)

        self.newGroup()
        for seName in groupedFiles.keys():
            self.defineMergeJobs(groupedFiles[seName])

        return
