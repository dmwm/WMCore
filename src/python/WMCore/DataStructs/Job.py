#!/usr/bin/env python
"""
_Job_

Data object that describes a job

Jobs know their status (active, failed, complete) and know the files they run on
but don't know the subscription or workflow. They are kept together by a 
JobGroup which knows the subscription and corresponding workflow. A Job is not a 
job in a batch system, it's more abstract - it's the piece of 
work that needs to get done.
"""

__all__ = []
__revision__ = "$Id: Job.py,v 1.20 2009/05/01 15:40:53 sryu Exp $"
__version__ = "$Revision: 1.20 $"

from WMCore.DataStructs.Pickleable import Pickleable
from WMCore.DataStructs.Fileset import Fileset
from WMCore.DataStructs.JobGroup import JobGroup
from WMCore.DataStructs.Mask import Mask

from sets import Set
import datetime

class Job(Pickleable):
    def __init__(self, name = None, files = None):
        """
        A job has a jobgroup which gives it its subscription and workflow.
        inputFiles is a list containing files associated to a job
        last_update is the time the job last changed
        """
        if files == None:
            self.inputFiles = []
        else:
            self.inputFiles = files

        self.last_update = datetime.datetime.now()
        self.job_group = -1
        self.status = 'QUEUED'
        self.name = name
        
        self.output = Fileset(name = 'output')
        self.report = None
        self.mask = Mask()

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
            return self.inputFiles
        elif type == "set":
            return self.makeset(self.inputFiles)
        elif type == "lfn":
            def getLFN(file):
                return file["lfn"]

            lfns = map(getLFN, self.inputFiles)
            return lfns
        elif type == "id":
            def getID(file):
                return file["id"]

            ids = map(getID, self.inputFiles)
            return ids

    def addFile(self, file):
        """
        _addFile_

        Add a to the job's input.
        """
        self.inputFiles.append(file)
        return

    def addOutput(self, file):
        """
        add files to self.output
        """
        self.output.addFile(file)
        self.output.commit()

    def changeStatus(self, status):
        self.last_update = datetime.datetime.now()
        self.status = status

    def submit(self, name = None):
        """
        Once submitted to a batch queue set status to active and set the job's
        name to some id from the batch system. Calling this method means the job
        has been submitted to the batch queue.
        """
        if name != self.name and name != None:
            self.name = name
        self.changeStatus('ACTIVE')

    def resubmit(self, name = None):
        """
        Reset the file status to acquired for files associated to this job
        """
        self.submit(name)

    def fail(self, report = None):
        """
        Job has failed, mark all files associated with it as failed, and store
        the job report
        """
        self.changeStatus('FAILED')
        self.report = report

    def complete(self, report = None):
        """
        Job has completed successfully, mark all files associated
        with it as complete, and store the job report
        """
        self.changeStatus('COMPLETE')
        self.report = report
    
    def getStatus(self):
        return self.status
