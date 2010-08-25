#!/usr/bin/env python
"""
_Job_

Data object that describes a job

"""

__all__ = []
__revision__ = "$Id: Job.py,v 1.26 2009/08/26 16:34:18 mnorman Exp $"
__version__ = "$Revision: 1.26 $"

from WMCore.DataStructs.Fileset import Fileset
from WMCore.DataStructs.JobGroup import JobGroup
from WMCore.DataStructs.Mask import Mask
from WMCore.DataStructs.WMObject import WMObject
from WMCore.Configuration import ConfigSection

from WMCore.Services.UUID import makeUUID

from sets import Set
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
        self["outcome"] = 'fail'
        self["retry_count"] = 0
        self["location"] = None
        self["mask"] = Mask()

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

            lfns = map(getLFN, self["input_files"])
            return lfns
        elif type == "id":
            def getID(file):
                return file["id"]

            ids = map(getID, self["input_files"])
            return ids

    def addFile(self, file):
        """
        _addFile_

        Add a file or list of files to the job's input.
        """
        if type(file) == list:
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


