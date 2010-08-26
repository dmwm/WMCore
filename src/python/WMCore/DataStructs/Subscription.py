#!/usr/bin/env python
"""
_Subscription_

workflow + fileset = subscription

TODO: Add some kind of tracking for state of files - though if too much is
added becomes counter productive
"""
__all__ = []
__revision__ = "$Id: Subscription.py,v 1.30 2010/06/28 19:01:22 sfoulkes Exp $"
__version__ = "$Revision: 1.30 $"

import copy
from WMCore.DataStructs.Pickleable import Pickleable
from WMCore.DataStructs.Fileset import Fileset

class Subscription(Pickleable, dict):
    def __init__(self, fileset = None, workflow = None,
                 split_algo = "FileBased", type = "Processing"):
        if fileset == None:
            fileset = Fileset()

        self.setdefault('fileset', fileset)
        self.setdefault('workflow', workflow)
        self.setdefault('type', type)

        self.setdefault('split_algo', split_algo)

        self.available = Fileset(name=fileset.name,
                                 files = fileset.getFiles())

        self.acquired = Fileset(name='acquired')
        self.completed = Fileset(name='completed')
        self.failed = Fileset(name='failed')

    def name(self):
        return self.getWorkflow().name.replace(' ', '') + '_' + \
                    self.getFileset().name.replace(' ', '')

    def getWorkflow(self):
        return self["workflow"]

    def workflowName(self):
        if self["workflow"] == None:
            return "Unknown"
        return self["workflow"].name

    def taskName(self):
        if self['workflow'] == None:
            return "Unknown"
        return self['workflow'].task

    def owner(self):
        if self['workflow'] == None:
            return 'Unknown'
        return self['workflow'].owner

    def getFileset(self):
        return self['fileset']

    def acquireFiles(self, files = [], size=1):
        """
        Return the files acquired
        """
        self.acquired.commit()
        self.available.commit()
        self.failed.commit()
        self.completed.commit()
        retval = []
        if len(files):
            for i in files:
                # Check each set, instead of elif, just in case something has
                # got out of synch
                if i in self.available.files:
                    self.available.files.remove(i)
                if i in self.failed.files:
                    self.failed.files.remove(i)
                if i in self.completed.files:
                    self.completed.files.remove(i)
                self.acquired.addFile(i)
        else:
            if len(self.available.files) < size or size == 0:
                size = len(self.available.files)
            for i in range(size):
                self.acquired.addFile(self.available.files.pop())

        return self.acquired.listNewFiles()

    def completeFiles(self, files):
        """
        Return the number of files complete
        """
        self.acquired.commit()
        self.available.commit()
        self.failed.commit()
        self.completed.commit()
        for i in files:
            # Check each set, instead of elif, just in case something has
            # got out of synch
            if i in self.available.files:
                self.available.files.remove(i)
            if i in self.failed.files:
                self.failed.files.remove(i)
            if i in self.acquired.files:
                self.acquired.files.remove(i)
            self.completed.addFile(i)

    def failFiles(self, files):
        """
        Return the number of files failed
        """
        self.acquired.commit()
        self.available.commit()
        self.failed.commit()
        self.completed.commit()
        for i in files:
            # Check each set, instead of elif, just in case something has
            # got out of synch
            if i in self.available.files:
                self.available.files.remove(i)
            if i in self.completed.files:
                self.completed.files.remove(i)
            if i in self.acquired.files:
                self.acquired.files.remove(i)
            self.failed.addFile(i)

    def filesOfStatus(self, status=None):
        """
        _filesOfStatus_

        Return a Set of File objects that are associated with the subscription
        and have a particular status.
        """
        status = status.title()
        if status == 'Available':
            return self.available.getFiles(type='set') - \
            (self.acquiredFiles() | self.completedFiles() | self.failedFiles())
        elif status == 'Acquired':
            return self.acquired.getFiles(type='set')
        elif status == 'Completed':
            return self.completed.getFiles(type='set')
        elif status == 'Failed':
            return self.failed.getFiles(type='set')

    def availableFiles(self):
        """
        _availableFiles_

        Return a Set of files that are available for processing
        (e.g. not already in use)
        """
        return self.filesOfStatus(status = "Available")

    def acquiredFiles(self):
        """
        Set of files marked as acquired.
        """
        return self.filesOfStatus(status = "Acquired")

    def completedFiles(self):
        """
        Set of files marked as completed.
        """
        return self.filesOfStatus(status = "Completed")

    def failedFiles(self):
        """
        Set of files marked as failed.
        """
        return self.filesOfStatus(status = "Failed")
