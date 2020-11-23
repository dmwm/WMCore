#!/usr/bin/env python
"""
_Job_

Data object that describes a job

"""

from builtins import map, range

__all__ = []


from WMCore.DataStructs.Mask import Mask
from WMCore.DataStructs.WMObject import WMObject
from WMCore.Configuration import ConfigSection

import time


class Job(WMObject, dict):
    def __init__(self, name = None, files = None):
        """
        A job has a jobgroup which gives it its subscription and workflow.
        inputFiles is a list containing files associated to a job
        last_update is the time the job last changed
        """
        dict.__init__(self)
        self.baggage = ConfigSection("baggage")
        if files == None:
            self["input_files"] = []
        else:
            self["input_files"] = files

        self["id"] = None
        self["jobgroup"] = None
        self["name"] = name
        self["state"] = 'new'
        self["state_time"] = int(time.time())
        self["outcome"] = "failure"
        self["retry_count"] = 0
        self["location"] = None
        self["mask"] = Mask()
        self["task"] = None
        self["fwjr"] = None
        self["fwjr_path"] = None
        self["workflow"] = None
        self["owner"] = None
        self["estimatedJobTime"] = None
        self["estimatedMemoryUsage"] = None
        self["estimatedDiskUsage"] = None
        return

    #  //
    # // Use property to define getter/setter API for task
    #//  makes job.task and job.tast = "value" possible
    task = property(lambda self: self['task'],
                    lambda self, x: self.__setitem__('task',x))

    def save(self):
        """
        _save_

        Bogus method to make this look more like the WMBS JOB.
        """
        return

    def getFiles(self, type = "list"):
        """
        _getFiles_

        Retrieve information about the input files for the job.  The type
        parameter can be set to one of the following:
          list - A list of File objects will be returned
          set - A set of File objects will be returned
          lfn - A list of LFNs will be returned
          id - A list if File IDs will be returned
        """
        if type == "list":
            return self["input_files"]
        elif type == "set":
            return self.makeset(self["input_files"])
        elif type == "lfn":
            def getLFN(file):
                return file["lfn"]

            lfns = list(map(getLFN, self["input_files"]))
            return lfns
        elif type == "id":
            def getID(file):
                return file["id"]

            ids = list(map(getID, self["input_files"]))
            return ids

    def addFile(self, file):
        """
        _addFile_

        Add a file or list of files to the job's input.
        """
        if isinstance(file, list):
            self["input_files"].extend(file)
        else:
            self["input_files"].append(file)

        return

    def changeState(self, newState):
        """
        _changeState_

        Change the state of the job.
        """
        self["state"] = newState
        self["state_time"] = int(time.time())

        return

    def changeOutcome(self, jobOutcome):
        """
        _changeOutcome_

        Change the final outcome of the job, can be either success or fail.
        """
        self["outcome"] = jobOutcome
        return

    def addResourceEstimates(self, jobTime = None, memory = None, disk = None):
        """
        _addResourceEstimates_

        Add to the current resource estimates, if None then initialize them
        to the given value. Each value can be set independently.
        """
        if not self["estimatedJobTime"]:
            self["estimatedJobTime"] = jobTime
        elif jobTime:
            self["estimatedJobTime"] += jobTime

        if not self["estimatedMemoryUsage"]:
            self["estimatedMemoryUsage"] = memory
        elif memory:
            self["estimatedMemoryUsage"] += memory

        if not self["estimatedDiskUsage"]:
            self["estimatedDiskUsage"] = disk
        elif disk:
            self["estimatedDiskUsage"] += disk

        return

    def getBaggage(self):
        """
        _getBaggage_

        Get a reference to the embedded ConfigSection that is
        used to store per job arguments needed to be propagated at
        submission time

        """
        return self.baggage


    def addBaggageParameter(self, attrName, value):
        """
        _addBaggageParameter_

        Add an attribute as process.pset1.pset2.param = value
        Value should be the appropriate python type

        """
        currentPSet = self.baggage
        paramList = attrName.split(".")
        for i in range(0, len(paramList)):
            param = paramList.pop(0)
            if len(paramList) > 0:
                if not hasattr(currentPSet, param):
                    currentPSet.section_(param)
                currentPSet = getattr(currentPSet, param)
            else:
                setattr(currentPSet, param, value)
