#!/usr/bin/env python
"""
_Job_

Data object that describes a job

"""
__all__ = []
__revision__ = "$Id: Job.py,v 1.2 2008/08/05 17:56:04 metson Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.DataStructs.Pickleable import Pickleable 
from WMCore.DataStructs.Fileset import Fileset
from sets import Set
import datetime

class Job(Pickleable):
    def __init__(self, subscription=None, files=Fileset()):
        """
        subscription is the subscription object the jobs are associated with
        file_set is a Fileset containing files associated to a job
        last_update is the time the job last changed
        """
        self.subscription = subscription
        self.file_set = files
        self.last_update = datetime.datetime.now()

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
        add a file to       self.file_set
        """
        self.file_set.addFile(file) 