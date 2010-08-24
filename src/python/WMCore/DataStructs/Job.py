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
__revision__ = "$Id: Job.py,v 1.14 2008/10/01 22:02:30 metson Exp $"
__version__ = "$Revision: 1.14 $"

from WMCore.DataStructs.Pickleable import Pickleable
from WMCore.DataStructs.Fileset import Fileset
from WMCore.DataStructs.JobGroup import JobGroup
from WMCore.DataStructs.Mask import Mask
from WMCore.Services.UUID import makeUUID
from sets import Set
import datetime

class Job(Pickleable):
    def __init__(self, name=None, files=None, logger=None, dbfactory=None):
        """
        A job has a jobgroup which gives it its subscription and workflow.
        file_set is a Fileset containing files associated to a job
        last_update is the time the job last changed
        """
        if files == None:
            self.file_set = Fileset()
        else:
            self.file_set = files
        self.last_update = datetime.datetime.now()
        self.status = 'QUEUED'
        if name == None:
            # Job's need to be uniquely named, so generate a GUID
            self.name = makeUUID()
        else:
            self.name = name
        
        self.output = Fileset(name = 'output', logger = self.file_set.logger)
        self.report = None
        self.mask = Mask()

    def getFiles(self, type='list'):
        if type == 'list':
            return self.file_set.getFiles(type='list')
        elif type == 'set':
            return self.file_set.getFiles(type='set')
        elif type == 'lfn':
            return self.file_set.getFiles(type='lfn')
        elif type == 'id':
            return self.file_set.getFiles(type='id')

    def listLFNs(self):
        """
        To be deprecated
        """
        return self.getFiles(type='lfn')
    
    def listFiles(self):
        """
        To be deprecated
        """
        return self.getFiles()
    
    def addFile(self, file):
        """
        add a file to self.file_set
        """
        self.file_set.addFile(file)

    def addOutput(self, file):
        """
        add a file to self.output
        """
        self.output.addFile(file)

    def changeStatus(self, status):
        self.last_update = datetime.datetime.now()
        self.status = status

    def submit(self, name=None):
        """
        Once submitted to a batch queue set status to active and set the job's
        name to some id from the batch system. Calling this method means the job
        has been submitted to the batch queue.
        """
        if name != self.name and name != None:
            self.name = name
        self.changeStatus('ACTIVE')

    def resubmit(self, name=None):
        """
        Reset the file status to acquired for files associated to this job
        """
        self.submit(name)

    def fail(self, report=None):
        """
        Job has failed, mark all files associated with it as failed, and store
        the job report
        """
        self.changeStatus('FAILED')
        self.report = report

    def complete(self, report=None):
        """
        Job has completed successfully, mark all files associated
        with it as complete, and store the job report
        """
        self.changeStatus('COMPLETE')
        self.report = report
