#!/usr/bin/env python
"""
_Subscription_

workflow + fileset = subscription

TODO: Add some kind of tracking for state of files - though if too much is 
added becomes counter productive
"""
__all__ = []
__revision__ = "$Id: Subscription.py,v 1.5 2008/07/07 09:39:54 metson Exp $"
__version__ = "$Revision: 1.5 $"
import copy
from WMCore.DataStructs.Pickleable import Pickleable
from WMCore.DataStructs.Fileset import Fileset 
class Subscription(Pickleable):
    def __init__(self, fileset = Fileset(), workflow = None, 
               split_algo = 'FileBased', type = "Processing"):
        self.fileset = fileset
        self.workflow = workflow
        self.type = type
        self.split_algo = split_algo
        self.available = copy.copy(fileset)
        self.acquired = Fileset()
        self.completed = Fileset()
        self.failed = Fileset()
        
    def getWorkflow(self):
        return self.workflow
    
    def getFileset(self):
        return self.fileset

    def availableFiles(self, parents=0):
        """
        Return a Set of files that are available for processing 
        (e.g. not already in use)
        """
        return self.available.listFiles() - self.acquiredFiles.listFiles() - \
            self.completed.listFiles() - self.failed.listFiles() 
    
    def acquireFiles(self, size=1):
        self.acquired.commit()
        if len(self.available.files) < size or size == 0:
            size = len(self.available.files)        
        for i in range(size):
            self.acquired.addFile(self.available.files.pop())            
        retval = self.acquired.listNewFiles() 
        return retval 
    
    def filesOfStatus(self, status=None):
        if status == 'AvailableFiles':
            return self.available.listFiles() - \
            (self.acquiredFiles() | self.completedFiles() | self.failedFiles())
        elif status == 'AcquiredFiles':
            return self.acquired.listFiles()
        elif status == 'CompletedFiles':
            return self.completed.listFiles()
        elif status == 'FailedFiles':
            return self.failed.listFiles()
        
    def availableFiles(self):
        """
        Return a Set of files that are available for processing 
        (e.g. not already in use)
        """
        return self.filesOfStatus(status='AvailableFiles')
            
    def acquiredFiles(self):
        """
        Set of files marked as acquired.
        """
        return self.filesOfStatus(status='AcquiredFiles')
        
    def completedFiles(self):
        """
        Set of files marked as completed.
        """
        return self.filesOfStatus(status='CompletedFiles')
    
    def failedFiles(self):
        """
        Set of files marked as failed. 
        """
        return self.filesOfStatus(status='FailedFiles')
