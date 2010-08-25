#!/usr/bin/env python


__revision__ = "$Id: JobFactory.py,v 1.14 2009/09/10 18:58:58 mnorman Exp $"
__version__  = "$Revision: 1.14 $"


import logging
import threading

from sets import Set
from sets import ImmutableSet
from WMCore.DataStructs.WMObject import WMObject
from WMCore.DataStructs.Fileset  import Fileset
from WMCore.DataStructs.File     import File

class JobFactory(WMObject):
    """
    A JobFactory is created with a subscription (which has a fileset). It is a
    base class of an object instance of an object representing some job 
    splitting algorithm. It is called with a job type (at least) to return a 
    JobGroup object. The JobFactory should be subclassed by real splitting 
    algorithm implementations.
    """
    def __init__(self, package='WMCore.DataStructs', subscription=None):
        self.package = package
        self.subscription = subscription
        self.timing = {'jobInstance': 0, 'sortByLocation': 0, 'acquireFiles': 0, 'jobGroup': 0}

    def __call__(self, jobtype='Job', grouptype='JobGroup', *args, **kwargs):
        """
        The default behaviour of JobFactory.__call__ is to return a single
        Job associated with all the files in the subscription's fileset
        """
        module = "%s.%s" % (self.package, jobtype)
        module = __import__(module, globals(), locals(), [jobtype])#, -1)
        jobInstance = getattr(module, jobtype.split('.')[-1])
        
        module = "%s.%s" % (self.package, grouptype)
        module = __import__(module, globals(), locals(), [grouptype])
        groupInstance = getattr(module, grouptype.split('.')[-1])

        jobGroups = self.algorithm(groupInstance = groupInstance,
                                   jobInstance = jobInstance,
                                   *args, **kwargs)

        return jobGroups
    
    def algorithm(self, groupInstance = None, jobInstance = None, *args,
                  **kwargs):
        """
        _algorithm_

        A splitting algorithm that takes all available files from the
        subscription and splits them into jobs and inserts them into job groups.
        The algorithm must return a list of job groups.
        """
        pass


    def sortByLocation(self):
        """
        _sortByLocation_
        
        Sorts the files in the job by location and passes back a dictionary of files, with each key corresponding
        to a set of locations
        """

        fileDict = {}

        fileset = self.subscription.availableFiles()

        for file in fileset:
            locSet = ImmutableSet(file['locations'])
                    
            if locSet in fileDict.keys():
                fileDict[locSet].append(file)
            else:
                fileDict[locSet] = [file]

        return fileDict
            
