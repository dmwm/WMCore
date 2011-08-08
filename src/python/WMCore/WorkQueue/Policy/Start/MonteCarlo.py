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
        self.args.setdefault('MaxJobsPerElement', 250)  # jobs per WQE


    def split(self):
        """Apply policy to spec"""
        # if not specified take standard defaults
        if not self.mask:
            self.mask = Mask(FirstRun = 1, FirstLumi = 1, FirstEvent = 1,
                             LastRun = 1,
                             LastEvent = self.initialTask.totalEvents())
        mask = Mask(**self.mask)
        stepSize = self.args['SliceSize'] * self.args['MaxJobsPerElement']
        total = mask['LastEvent']
        assert(total > mask['FirstEvent'])
        while mask['FirstEvent'] < total:
            current = mask['FirstEvent'] + stepSize - 1 # inclusive range
            if current > total:
                current = total
            mask['LastEvent'] = current
            jobs = ceil((mask['LastEvent'] - mask['FirstEvent']) /
                        float(self.args['SliceSize']))
            mask['LastLumi'] = mask['FirstLumi'] + int(jobs) - 1 # inclusive range
            self.newQueueElement(WMSpec = self.wmspec,
                                 Jobs = jobs,
                                 Mask = copy(mask))
            mask['FirstEvent'] = mask['LastEvent'] + 1
            mask['FirstLumi'] = mask['LastLumi'] + 1



    def validate(self):
        """Check args and spec work with block splitting"""
        StartPolicyInterface.validateCommon(self)

        if self.initialTask.totalEvents() < 1:
            raise WorkQueueNoWorkError(self.wmspec, 'Invalid total events selection: %s' % str(self.initialTask.totalEvents()))

        if self.mask and self.mask['LastEvent'] < self.mask['FirstEvent']:
            raise WorkQueueWMSpecError(self.wmspec, "Invalid start & end events")

        if self.mask and self.mask['LastLumi'] < self.mask['FirstLumi']:
            raise WorkQueueWMSpecError(self.wmspec, "Invalid start & end lumis")
