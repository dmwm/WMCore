#!/usr/bin/env python
"""
_Subscription_

workflow + fileset = subscription

TODO: Add some kind of tracking for state of files - though if too much is 
added becomes counter productive
"""
__all__ = []
__revision__ = "$Id: Subscription.py,v 1.3 2008/07/04 16:47:08 metson Exp $"
__version__ = "$Revision: 1.3 $"
from WMCore.DataStructs.Pickleable import Pickleable
from WMCore.DataStructs.Fileset import Fileset 
class Subscription(Pickleable):
    def __init__(self, fileset = None, workflow = None, 
               split_algo = 'FileBased', type = "Processing"):
        self.fileset = fileset
        self.workflow = workflow
        self.type = type
        self.split_algo = split_algo
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
        return self.fileset.listFiles() - self.acquiredFiles
    
    def acquireFiles(self, size=1):
        print "DataStruct Subscription acquireFiles"
        files = self.availableFiles()
        self.acquiredFiles.commit()
        if len(files) < size or size == 0:
            size = len(files)
        for i in range(size):
            self.acquiredFiles.addFile(files.pop())
            i = i + 1
        return self.acquiredFiles.listNewFiles()  
    
    def filesOfStatus(self, status=None):
        if status == 'AvailableFiles':
            return self.fileset - (self.acquired | self.completed | self.failed)
        elif status == 'AcquiredFiles':
            return self.acquired
        elif status == 'CompletedFiles':
            return self.completed
        elif status == 'FailedFiles':
            return self.failed
        
    def availableFiles(self):
        """
        Return a Set of files that are available for processing 
        (e.g. not already in use)
        """
        return self.filestatus(status='AvailableFiles')
            
    def acquiredFiles(self):
        """
        Set of files marked as acquired.
        """
        return self.filestatus(status='AcquiredFiles')
        
    def completedFiles(self):
        """
        Set of files marked as completed.
        """
        return self.filestatus(status='CompletedFiles')
    
    def failedFiles(self):
        """
        Set of files marked as failed. 
        """
        return self.filestatus(status='FailedFiles')