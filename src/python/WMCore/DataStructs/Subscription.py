#!/usr/bin/env python
"""
_Subscription_

workflow + fileset = subscription

TODO: Add some kind of tracking for state of files - though if too much is 
added becomes counter productive
"""
__all__ = []
__revision__ = "$Id: Subscription.py,v 1.17 2008/10/28 18:55:40 metson Exp $"
__version__ = "$Revision: 1.17 $"
import copy
from sets import Set
from WMCore.DataStructs.Pickleable import Pickleable
from WMCore.DataStructs.Fileset import Fileset 

class Subscription(Pickleable, dict):
    def __init__(self, fileset = Fileset(), workflow = None, 
               whitelist = Set(), blacklist = Set(),
               split_algo = 'FileBased', type = "Processing"):
        self.setdefault('fileset', fileset)
        self.setdefault('workflow', workflow)
        self.setdefault('type', type)
        
        self.setdefault('split_algo', split_algo)
        self.setdefault('whitelist', whitelist)
        self.setdefault('blacklist', blacklist)
        
        self.available = Fileset(name=fileset.name, 
                                 files = fileset.listFiles(), 
                                 logger = fileset.logger)  
        
        self.acquired = Fileset(name='acquired', logger = fileset.logger)
        self.completed = Fileset(name='completed', logger = fileset.logger)
        self.failed = Fileset(name='failed', logger = fileset.logger)
    
    def name(self):
        return self.getWorkflow().name.replace(' ', '') + '_' + \
                    self.getFileset().name.replace(' ', '')
        
    def getWorkflow(self):
        return self['workflow']
    
    def getFileset(self):
        return self['fileset']
#
#    def availableFiles(self, parents=0):
#        """
#        Return a Set of files that are available for processing 
#        (e.g. not already in use)
#        """
#        return self.available.listFiles() - self.acquiredFiles.listFiles() - \
#            self.completed.listFiles() - self.failed.listFiles() 
    
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
        if status == 'AvailableFiles':
            return self.available.getFiles(type='set') - \
            (self.acquiredFiles() | self.completedFiles() | self.failedFiles())
            #return [12,11]
        elif status == 'AcquiredFiles':
            return self.acquired.getFiles(type='set')
        elif status == 'CompletedFiles':
            return self.completed.getFiles(type='set')
        elif status == 'FailedFiles':
            return self.failed.getFiles(type='set')
    
    
    def markLocation(self, location, whitelist = True):
        """
        Add a location to the subscriptions white or black list
        """
        if whitelist:
            self['whitelist'].add(location)
        else:
            self['blacklist'].add(location)
        
    def availableFiles(self):
        """
        Return a Set of files that are available for processing 
        (e.g. not already in use) and at sites that are white listed 
        or not black listed
        """
        def locationMagic(files, locations):
            """
            files and locations are sets. method returns the subset of files 
            that are at the locations - this is a lot simpler with the database
            """
            magicfiles = Set()
            for f in files:
                if len(f['locations'] & locations) > 0:
                    magicfiles.add(f)
            return magicfiles
            
        files = self.filesOfStatus(status='AvailableFiles')
        if len(self['whitelist']) > 0:
            # Return files at white listed sites
            return locationMagic(files, self['whitelist'])
        elif len(self['blacklist']) > 0:
            # Return files not at black listed sites
            return files - locationMagic(files, self['blacklist'])
        #Return all files, because you're crazy and just don't care
        return files 
            
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
