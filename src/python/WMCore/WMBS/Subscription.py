#!/usr/bin/env python
"""
_Subscription_

A simple object representing a Subscription in WMBS.

A subscription is just a way to link many sets of jobs to a 
fileset and track the process of the associated jobs. It is 
associated to a single fileset and a single workflow.

workflow + fileset = subscription

subscription + application logic = jobs

"""

__revision__ = "$Id: Subscription.py,v 1.1 2008/05/02 13:50:44 metson Exp $"
__version__ = "$Revision: 1.1 $"

from sets import Set
class Subscription(object):
    def __init__(self, fileset = None, workflow = None, id = -1, type = "processing", wmbs = None):
        self.wmbs = wmbs
        self.fileset = fileset
        self.workflow = workflow
        self.type = type
        self.id = id
        
    def create(self):
        self.wmbs.newSubscription(self.fileset.name, self.workflow.spec, 
                                  self.workflow.owner, self.type)
        
        for i in self.wmbs.subscriptionID(self.fileset.name, self.workflow.spec, 
                                  self.workflow.owner, self.type):
            self.id = i.fetchone().id
        print 'ID of subscripion is'
        print self.id
             
    def availableFiles(self):
        """
        Return a Set of file ids that are available for processing 
        (e.g. not already in use)
        """
        files = Set()
        for f in self.wmbs.listAvailableFiles(self.id):
            for i in f.fetchall():
                files.add(i.file)
        return files
            
    def acquiredFiles(self):
        """
        Return a Set of file ids that are available for processing 
        (e.g. not already in use)
        """
        files = Set()
        for f in self.wmbs.listAcquiredFiles(self.id):
            for i in f.fetchall():
                files.add(i.file)
        return files
                     
    def acquireFiles(self, size = 0):
        """
        Acquire size files as active for the subscription. If size = 0 
        acquire all files (default behaviour).
        """
        files = self.availableFiles()
        if size == 0:
            self.wmbs.acquireNewFiles(self.id, list(files))
        else:
            l = []
            for i in range(size):
                l.append(files.pop())
                i = i + 1
            self.wmbs.acquireNewFiles(self.id, l)
    
    def completeFiles(self, files):
        """
        Mark a (set of) file(s) as completed.
        """
        self.wmbs.completeFiles(self.id, files)
    
    def failFiles(self, files):
        """
        Mark a (set of) file(s) as failed. 
        """
        self.wmbs.failFiles(self.id, files)

    
    def completedFiles(self):
        """
        Mark a (set of) file(s) as completed.
        """
        files = Set()
        for f in self.wmbs.listCompletedFiles(self.id):
            for i in f.fetchall():
                files.add(i.file)
        return files
    
    def failedFiles(self):
        """
        Mark a (set of) file(s) as failed. 
        """
        files = Set()
        for f in self.wmbs.listFailedFiles(self.id):
            for i in f.fetchall():
                files.add(i.file)
        return files