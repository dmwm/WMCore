#!/usr/bin/env python
"""
_WMBSMergeBySize_

Generic merging for WMBS.  This will correctly handle merging files that have
been split up honoring the original file boundaries.
"""

import threading
from functools import cmp_to_key

from WMCore.DAOFactory import DAOFactory
from WMCore.DataStructs.Run import Run
from WMCore.JobSplitting.JobFactory import JobFactory
from WMCore.WMBS.File import File


def mergeUnitCompare(a, b):
    """
    _mergeUnitCompare_

    Compare two merge units.  They will be sorted first by run ID and then by
    lumi ID.
    """
    if a["run"] > b["run"]:
        return 1
    elif a["run"] == b["run"]:
        if a["lumi"] > b["lumi"]:
            return 1
        elif a["lumi"] == b["lumi"]:
            return 0
        else:
            return -1
    else:
        return -1


def fileCompare(a, b):
    """
    _fileCompare_

    Compare two files based on their "file_first_event" attribute.
    """
    if a["file_first_event"] > b["file_first_event"]:
        return 1
    if a["file_first_event"] == b["file_first_event"]:
        return 0
    else:
        return -1


def sortedFilesFromMergeUnits(mergeUnits):
    """
    _sortedFilesFromMergeUnits_

    Given a list of merge units sort them and the files that they contain.
    Return a list of sorted WMBS File structures.
    """
    mergeUnits.sort(key=cmp_to_key(mergeUnitCompare))

    sortedFiles = []
    for mergeUnit in mergeUnits:
        mergeUnit["files"].sort(key=cmp_to_key(fileCompare))

        for file in mergeUnit["files"]:
            newFile = File(id=file["file_id"], lfn=file["file_lfn"],
                           events=file["file_events"], size=file["file_size"])

            # The WMBS data structure puts locations that are passed in through
            # the constructor in the "newlocations" attribute.  We want these to
            # be in the "locations" attribute so that they get picked up by the
            # job submitter.
            newFile["locations"] = set(file["pnn"])
            newFile.addRun(Run(file["file_run"], file["file_lumi"]))
            sortedFiles.append(newFile)

    return sortedFiles


class WMBSMergeBySize(JobFactory):
    """
    _WMBSMergeBySize_

    Generic merging for WMBS.  This will correctly handle merging files that
    have been split up honoring the original file boundaries merging the files
    in the correct order.
    """

    def defineMergeUnits(self, mergeableFiles):
        """
        _defineMergeUnits_

        Split all the mergeable files into merge units.  A merge unit is a group
        of files that must be merged together.  For example, the files that
        result from event based splitting jobs need to be merged back together.
        This method will return a list of merge units.  A merge unit is a
        dictionary with the following keys: file_parent, total_events, total_size,
        run, lumi, and files.  The files in the merge group are stored in a list
        under the files key.
        """
        mergeUnits = {}
        newMergeUnit = {}

        for mergeableFile in mergeableFiles:
            newMergeFile = {}

            for key in mergeableFile:
                newMergeFile[key] = mergeableFile[key]

            if newMergeFile["pnn"] not in mergeUnits:
                mergeUnits[newMergeFile["pnn"]] = {}

            if newMergeFile["file_run"] not in mergeUnits[newMergeFile["pnn"]]:
                mergeUnits[newMergeFile["pnn"]][newMergeFile["file_run"]] = []

            for mergeUnit in mergeUnits[newMergeFile["pnn"]][newMergeFile["file_run"]]:
                if mergeUnit["file_parent"] == mergeableFile["file_parent"]:
                    mergeUnit["files"].append(newMergeFile)
                    mergeUnit["total_size"] += newMergeFile["file_size"]
                    mergeUnit["total_events"] += newMergeFile["file_events"]

                    if mergeableFile["file_run"] < mergeUnit["run"] or \
                            (mergeableFile["file_run"] == mergeUnit["run"] and \
                                         mergeableFile["file_lumi"] < mergeUnit["lumi"]):
                        newMergeUnit["run"] = newMergeFile["file_run"]
                        newMergeUnit["lumi"] = newMergeFile["file_lumi"]

                    break
            else:
                newMergeUnit = {}
                newMergeUnit["file_parent"] = newMergeFile["file_parent"]
                newMergeUnit["total_events"] = newMergeFile["file_events"]
                newMergeUnit["total_size"] = newMergeFile["file_size"]
                newMergeUnit["run"] = newMergeFile["file_run"]
                newMergeUnit["lumi"] = newMergeFile["file_lumi"]
                newMergeUnit["files"] = []
                newMergeUnit["files"].append(newMergeFile)
                mergeUnits[newMergeFile["pnn"]][newMergeFile["file_run"]].append(newMergeUnit)

        return mergeUnits

    def createMergeJob(self, mergeUnits):
        """
        _createMergeJob_

        Create a merge job for the given merge units.  All the files contained
        in the merge units will be associated to the job.
        """
        if self.currentGroup == None:
            self.newGroup()

        self.newJob(name=self.getJobName())
        sortedFiles = sortedFilesFromMergeUnits(mergeUnits)

        for file in sortedFiles:
            self.currentJob.addResourceEstimates(disk=float(file["size"]) / 1024)
            self.currentJob.addFile(file)

    def defineMergeJobs(self, mergeUnits):
        """
        _defineMergeJobs_

        Go through the list of merge units and try to combine them together into
        merge jobs that fit within the min/max filesizes and under the maximum
        number of events.
        """
        mergeJobFileSize = 0
        mergeJobEvents = 0
        mergeJobFiles = []

        for mergeUnit in mergeUnits:
            if mergeUnit["total_size"] > self.maxMergeSize or \
                            mergeUnit["total_events"] > self.maxMergeEvents:
                self.createMergeJob([mergeUnit])
                continue
            elif mergeUnit["total_size"] + mergeJobFileSize > self.maxMergeSize or \
                                    mergeUnit["total_events"] + mergeJobEvents > self.maxMergeEvents:
                if mergeJobFileSize > self.minMergeSize or \
                                self.forceMerge == True:
                    self.createMergeJob(mergeJobFiles)
                    mergeJobFileSize = 0
                    mergeJobEvents = 0
                    mergeJobFiles = []
                else:
                    continue

            mergeJobFiles.append(mergeUnit)
            mergeJobFileSize += mergeUnit["total_size"]
            mergeJobEvents += mergeUnit["total_events"]

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
        # This doesn't use a proxy
        self.grabByProxy = False

        self.maxMergeSize = int(kwargs.get("max_merge_size", 1000000000))
        self.minMergeSize = int(kwargs.get("min_merge_size", 1048576))
        self.maxMergeEvents = int(kwargs.get("max_merge_events", 50000))

        myThread = threading.currentThread()
        daoFactory = DAOFactory(package="WMCore.WMBS",
                                logger=myThread.logger,
                                dbinterface=myThread.dbi)

        self.subscription["fileset"].load()

        myThread = threading.currentThread()
        if self.subscription["fileset"].open == True:
            self.forceMerge = False
        else:
            orphanDAO = daoFactory(classname="Subscriptions.FailOrphanFiles")
            orphanDAO.execute(self.subscription["id"],
                              self.subscription["fileset"].id,
                              conn=myThread.transaction.conn,
                              transaction=True)

            getAction = daoFactory(classname="Workflow.CheckInjectedWorkflow")
            injected = getAction.execute(name=self.subscription["workflow"].name,
                                         conn=myThread.transaction.conn,
                                         transaction=True)
            if injected:
                self.forceMerge = True
            else:
                self.forceMerge = False

        mergeDAO = daoFactory(classname="Subscriptions.GetFilesForMerge")
        mergeableFiles = mergeDAO.execute(self.subscription["id"],
                                          conn=myThread.transaction.conn,
                                          transaction=True)

        mergeUnits = self.defineMergeUnits(mergeableFiles)

        for pnn in mergeUnits:
            for runNumber in mergeUnits[pnn]:
                self.defineMergeJobs(mergeUnits[pnn][runNumber])

        return
