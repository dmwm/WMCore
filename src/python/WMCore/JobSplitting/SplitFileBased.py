#!/usr/bin/env python
"""
_SplitFileBased_

JobSplitting algorithm for creating jobs that run over the results of split
processing jobs.  This borrows heavily from the generic WMBS merging algorithm
but instead of combining multiple merge units together this will create a
single job for each merge unit.
"""

import threading
from functools import cmp_to_key

from WMCore.DAOFactory import DAOFactory
from WMCore.DataStructs.Run import Run
from WMCore.JobSplitting.JobFactory import JobFactory
from WMCore.Services.UUIDLib import makeUUID
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
                           events=file["file_events"])
            newFile.addRun(Run(file["file_run"], file["file_lumi"]))

            # The WMBS data structure puts locations that are passed in through
            # the constructor in the "newlocations" attribute.  We want these to
            # be in the "locations" attribute so that they get picked up by the
            # job submitter.
            newFile["locations"] = set([file["pnn"]])
            newFile.addRun(Run(file["file_run"], file["file_lumi"]))
            sortedFiles.append(newFile)

    return sortedFiles


class SplitFileBased(JobFactory):
    """
    _SplitFileBased_

    JobSplitting algorithm for creating jobs that run over the results of split
    processing jobs.
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

            if newMergeFile["file_run"] not in mergeUnits:
                mergeUnits[newMergeFile["file_run"]] = []

            for mergeUnit in mergeUnits[newMergeFile["file_run"]]:
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
                mergeUnits[newMergeFile["file_run"]].append(newMergeUnit)

        return mergeUnits

    def createProcJobs(self, mergeUnits):
        """
        _createProcJob_

        Given a list of merge units, create a job that processes all the files
        in the merge unit.  Each job will be placed in its own JobGroup so that
        merging will work correctly.
        """
        for mergeUnit in mergeUnits:
            self.newGroup()
            self.newJob(name=makeUUID())
            sortedFiles = sortedFilesFromMergeUnits([mergeUnit])

            for file in sortedFiles:
                file.load()
                self.currentJob.addFile(file)

    def algorithm(self, *args, **kwargs):
        """
        _algorithm_

        Use the generic WMBS merging DAO to get a list of files in our fileset
        that correspond to completed job groups.  Create jobs for these files.
        """
        myThread = threading.currentThread()
        daoFactory = DAOFactory(package="WMCore.WMBS",
                                logger=myThread.logger,
                                dbinterface=myThread.dbi)

        mergeDAO = daoFactory(classname="Subscriptions.GetFilesForMerge")
        mergeableFiles = mergeDAO.execute(self.subscription["id"])

        mergeUnits = self.defineMergeUnits(mergeableFiles)
        for runNumber in mergeUnits:
            mergeUnits[runNumber].sort(key=cmp_to_key(mergeUnitCompare))
            self.createProcJobs(mergeUnits[runNumber])

        return self.jobGroups
