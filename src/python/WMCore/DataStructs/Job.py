#!/usr/bin/env python
"""
_Job_

Data object that describes a job

Jobs know their status (active, failed, complete) and know the files they run on 
but don't know the subscription or group. They do know their workflow.
"""
__all__ = []
__revision__ = "$Id: Job.py,v 1.7 2008/09/19 17:26:19 metson Exp $"
__version__ = "$Revision: 1.7 $"

from WMCore.DataStructs.Pickleable import Pickleable 
from WMCore.DataStructs.Fileset import Fileset
from WMCore.DataStructs.Subscription import Subscription
from sets import Set
import datetime

class Job(Pickleable):
    def __init__(self, subscription=None, files=None):
        """
        A job has a subscription which gives it its workflow.
        file_set is a Fileset containing files associated to a job
        last_update is the time the job last changed
        """
        self.subscription = subscription
        self.workflow = subscription.workflow
        if files == None: 
            self.file_set = Fileset()
        else:
            self.file_set = files
        self.last_update = datetime.datetime.now()
        self.status = 'QUEUED'
        self.name = None
        self.output = Fileset(name='output', logger=files.logger)
        self.report = None

    def listFiles(self):
        """
        return the list of files associated with the job
        """
        return self.file_set.listFiles()
    
    def listLFNs(self):
        """
        return the list of lfns associated with the job
        """
        return self.file_set.listLFNs()
    
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
        
    def submit(self, name):
        """
        Once submitted to a batch queue set status to active and set the job's 
        name to some id from the batch system
        """
        self.name = name
        self.changeStatus('ACTIVE')
        
    def resubmit(self, name):
        """
        Reset the file status to acquired for files associated to this job
        """
        self.submit(name)        
    
    def fail(self, report):
        """
        Job has failed, mark all files associated with it as failed, and store 
        the job report
        """
        self.changeStatus('FAILED')
        self.report = report
    
    def complete(self, report):
        """
        Job has completed successfully, mark all files associated 
        with it as complete, and store the job report
        """
        self.changeStatus('COMPLETE')
        self.report = report