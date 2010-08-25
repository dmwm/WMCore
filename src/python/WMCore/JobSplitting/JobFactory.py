#!/usr/bin/env python


__revision__ = "$Id: JobFactory.py,v 1.18 2009/10/29 13:31:13 sfoulkes Exp $"
__version__  = "$Revision: 1.18 $"


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
    def __init__(self,
                 package='WMCore.DataStructs',
                 subscription=None,
                 generators=[]):
        self.package = package
        self.subscription = subscription
        self.generators = generators
        self.jobInstance = None
        self.groupInstance = None
        self.jobGroups = []
        self.currentGroup = None
        self.currentJob = None
        self.timing = {'jobInstance': 0, 'sortByLocation': 0, 'acquireFiles': 0, 'jobGroup': 0}


    def __call__(self, jobtype='Job', grouptype='JobGroup', *args, **kwargs):
        """
        The default behaviour of JobFactory.__call__ is to return a single
        Job associated with all the files in the subscription's fileset
        """

        #Need to reset the internal data for multiple calls to the factory
        self.jobGroups = []
        self.currentGroup = None
        self.currentJob = None


        module = "%s.%s" % (self.package, jobtype)
        module = __import__(module, globals(), locals(), [jobtype])#, -1)
        self.jobInstance = getattr(module, jobtype.split('.')[-1])

        module = "%s.%s" % (self.package, grouptype)
        module = __import__(module, globals(), locals(), [grouptype])
        self.groupInstance = getattr(module, grouptype.split('.')[-1])

        map(lambda x: x.start(), self.generators)

        self.algorithm(*args, **kwargs)

        self.commit()

        map(lambda x: x.finish(), self.generators)
        return self.jobGroups

    def algorithm(self, *args, **kwargs):
        """
        _algorithm_

        A splitting algorithm that takes all available files from the
        subscription and splits them into jobs and inserts them into job groups.
        The algorithm must return a list of job groups.
        """
        self.newGroup(args, kwargs)
        self.newJob(name='myJob')

    def newGroup(self):
        """
        Return and new JobGroup
        """
        self.commit()
        self.currentGroup = self.groupInstance(subscription=self.subscription)
        map(lambda x: x.startGroup(self.currentGroup), self.generators)

    def newJob(self, name=None, files=None):
        """
        Instantiate a new Job onject, apply all the generators to it
        """
        self.currentJob = self.jobInstance(name, files)
        for gen in self.generators:
            gen(self.currentJob)
        self.currentGroup.add(self.currentJob)

    def commit(self):
        """
        Bulk commit the JobGroup
        """
        if self.currentGroup:
            map(lambda x: x.finishGroup(self.currentGroup), self.generators)
        if self.currentGroup \
                and (self.currentGroup.jobs + self.currentGroup.newjobs) > 0:
            self.currentGroup.commitBulk()
            self.jobGroups.append(self.currentGroup)
            logging.debug('I have committed a jobGroup with id %i' %
                                (self.currentGroup.id))
            self.currentGroup = None

        for jobGroup in self.jobGroups:
            for job in jobGroup.jobs:
                self.subscription.acquireFiles(job["input_files"])
                job.save()

        return

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

