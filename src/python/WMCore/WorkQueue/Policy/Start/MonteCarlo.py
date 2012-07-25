#!/usr/bin/env python
"""
WorkQueue splitting by block

"""
__all__ = []



from WMCore.WorkQueue.Policy.Start.StartPolicyInterface import StartPolicyInterface
from WMCore.WorkQueue.WorkQueueExceptions import WorkQueueWMSpecError, WorkQueueNoWorkError
from WMCore.DataStructs.Mask import Mask
from copy import copy
from math import ceil

class MonteCarlo(StartPolicyInterface):
    """Split elements into blocks"""
    def __init__(self, **args):
        StartPolicyInterface.__init__(self, **args)
        self.args.setdefault('SliceType', 'NumberOfEvents')
        self.args.setdefault('SliceSize', 1000)         # events per job
        self.args.setdefault('SubSliceType', 'NumberOfEventsPerLumi')
        self.args.setdefault('SubSliceSize', self.args['SliceSize']) # events per lumi
        self.args.setdefault('MaxJobsPerElement', 250)  # jobs per WQE


    def split(self):
        """Apply policy to spec"""
        # if not specified take standard defaults
        if not self.mask:
            self.mask = Mask(FirstRun = 1,
                             FirstLumi = self.initialTask.getFirstLumi(1),
                             FirstEvent = self.initialTask.getFirstEvent(1),
                             LastRun = 1,
                             LastEvent = self.initialTask.getFirstEvent(1) +
                                             self.initialTask.totalEvents() - 1)
        mask = Mask(**self.mask)

        #First let's initialize some parameters
        stepSize = int(self.args['SliceSize']) * int(self.args['MaxJobsPerElement'])
        total = mask['LastEvent'] - mask['FirstEvent'] + 1
        lastAllowedEvent = mask['LastEvent']
        eventsAccounted = 0

        while eventsAccounted < total:
            current = mask['FirstEvent'] + stepSize - 1 # inclusive range
            if current > lastAllowedEvent:
                current = lastAllowedEvent

            nEvents = current - mask['FirstEvent'] + 1 # inclusive range

            # loop around at 32 bit signed int
            if current > (2**32 - 1):
                current = current % (2**32 - 1)

            # note LastEvent may be smaller than FirstEvent when 2**32 reached
            mask['LastEvent'] = current

            #Calculate the job splitting without actually doing it
            nLumis = int(ceil(nEvents / float(self.args['SubSliceSize'])))
            mask['LastLumi'] = mask['FirstLumi'] + nLumis - 1 # inclusive range
            jobs = int(ceil(nEvents / float(self.args['SliceSize'])))

            self.newQueueElement(WMSpec = self.wmspec,
                                 NumberOfLumis = nLumis,
                                 NumberOfEvents = nEvents,
                                 Jobs = jobs,
                                 Mask = copy(mask))

            # Increment beginning fields for next iteration
            mask['FirstEvent'] = mask['LastEvent'] + 1
            mask['FirstLumi'] = mask['LastLumi'] + 1
            eventsAccounted += stepSize


    def validate(self):
        """Check args and spec work with block splitting"""
        StartPolicyInterface.validateCommon(self)

        if self.initialTask.totalEvents() < 1:
            raise WorkQueueNoWorkError(self.wmspec, 'Invalid total events selection: %s' % str(self.initialTask.totalEvents()))

        if self.mask and self.mask['LastEvent'] < self.mask['FirstEvent']:
            raise WorkQueueWMSpecError(self.wmspec, "Invalid start & end events")

        if self.mask and self.mask['LastLumi'] < self.mask['FirstLumi']:
            raise WorkQueueWMSpecError(self.wmspec, "Invalid start & end lumis")
