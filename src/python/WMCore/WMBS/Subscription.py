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

__revision__ = "$Id: Subscription.py,v 1.2 2008/05/12 11:58:06 swakef Exp $"
__version__ = "$Revision: 1.2 $"

from sets import Set
from sqlalchemy.exceptions import IntegrityError
from WMCore.WMBS.File import File

class Subscription(object):
    def __init__(self, fileset = None, workflow = None, id = -1,
                  type = "Processing", parentage=0, wmbs = None):
        self.wmbs = wmbs
        self.fileset = fileset
        self.workflow = workflow
        self.type = type
        self.id = id
        self.parentage = parentage
        
    def create(self):
        try:
            self.wmbs.newSubscription(self.fileset.name, self.workflow.spec, 
                                  self.workflow.owner, self.type, self.parentage)
        except IntegrityError:
            self.wmbs.logger.exception('Subcription %s:%s exists' % (self.fileset, self.workflow))
        
        for i in self.wmbs.subscriptionID(self.fileset.name, self.workflow.spec, 
                                  self.workflow.owner, self.type):
            self.id = i[0]
        return self
                                  
        
#    def load(self, fileset, workflow, type='Processing'):
#        self.id = self.wmbs.subscriptionID(self.fileset.name, self.workflow.spec, 
#                                  self.workflow.owner, self.type)[0][0]
#        return self
    def load(self):
        
        result = self.wmbs.subscriptionID(self.fileset.name, self.workflow.spec, 
                                  self.workflow.owner, self.type)
        if not result:
            raise RuntimeError, "Subscription for %s:%s unknown" % \
                                    (self.fileset.name, self.workflow.spec)
        self.id = result[0][0]
        return self
             
    def availableFiles(self):
        """
        Return a Set of file ids that are available for processing 
        (e.g. not already in use)
        """
        files = []
        for f in self.wmbs.listAvailableFiles(self.id):
            #files.add(f.file)
            # files.add(f[0])
            files.append(File(lfn=f[0], wmbs=self.wmbs).load(parentage=self.parentage))
            #for i in f.fetchall():
            #    files.add(i.file)
        return files
            
    def acquiredFiles(self):
        """
        Return a Set of file ids that have been processed
        """
        files = Set()
        for f in self.wmbs.listAcquiredFiles(self.id):
            #files.add(f.file)
            files.add(File(lfn=f[0], wmbs=self.wmbs).load(parentage=self.parentage))
#           for i in f.fetchall():
#                files.add(i.file)
        return files
                     
    def acquireFiles(self, files = [], size = 0):
        """
        Acquire size files as active for the subscription. If size = 0 
        acquire all files (default behaviour).
        """
        if len(files):
            self.wmbs.acquireNewFiles(self.id, [x.id for x in files])
        elif size == 0:
            files = self.availableFiles()
            self.wmbs.acquireNewFiles(self.id, [x.id for x in files])
        else:
            files = self.availableFiles()
            l = []
            for i in range(size):
                l.append(files.pop())
                i = i + 1
            self.wmbs.acquireNewFiles(self.id, [x.id for x in l])
    
    def completeFiles(self, files):
        """
        Mark a (set of) file(s) as completed.
        """
        if not isinstance(files, list) and not isinstance(files, set):
            files=[files]
        self.wmbs.completeFiles(self.id, [x.id for x in files])
    
    def failFiles(self, files):
        """
        Mark a (set of) file(s) as failed. 
        """
        if not isinstance(files, list) and not isinstance(files, set):
            files=[files]
        self.wmbs.failFiles(self.id, [x.id for x in files])

    
    def completedFiles(self):
        """
        Mark a (set of) file(s) as completed.
        """
        files = Set()
        for f in self.wmbs.listCompletedFiles(self.id):
            files.add(File(lfn=f[0], wmbs=self.wmbs).load(parentage=self.parentage))
            #files.add(f.file)
#            for i in f.fetchall():
#                files.add(i.file)
        return list(files)
    
    def failedFiles(self):
        """
        Mark a (set of) file(s) as failed. 
        """
        files = Set()
        for f in self.wmbs.listFailedFiles(self.id):
            files.add(File(lfn=f[0], wmbs=self.wmbs).load(parentage=self.parentage))
            #files.add(f.file)
#            for i in f.fetchall():
#                files.add(i.file)
        return list(files)