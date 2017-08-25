#!/usr/bin/env python
"""
WorkQueue splitting by block

"""
from __future__ import division

import os
from WMCore.WorkQueue.Policy.Start.StartPolicyInterface import StartPolicyInterface
from WMCore.WorkQueue.WorkQueueExceptions import WorkQueueWMSpecError, WorkQueueNoWorkError
from WMCore.DataStructs.Mask import Mask
from copy import copy
from math import ceil, floor

__all__ = []


class MonteCarlo(StartPolicyInterface):
    """Split elements into blocks"""
    def __init__(self, **args):
        StartPolicyInterface.__init__(self, **args)


    def split(self):
        """Apply policy to spec"""
        # if not specified take standard defaults
        self.args.setdefault('SliceType', 'NumberOfEvents')
        self.args.setdefault('SliceSize', 1000)  # events per job
        self.args.setdefault('SubSliceType', 'NumberOfEventsPerLumi')
        self.args.setdefault('SubSliceSize', self.args['SliceSize'])  # events per lumi
        self.args.setdefault('MaxJobsPerElement', 1000)  # jobs per WQE
        self.args.setdefault('MaxLumisPerElement', os.environ.get('MAX_LUMIS_PER_WQE'))
        self.args.setdefault('blowupFactor', 1.0)  # Estimate of additional jobs following tasks.
                                                  # Total WQE tasks will be Jobs*(1+blowupFactor)
        noInputUpdate = self.initialTask.getTrustSitelists().get('trustlists')
        noPileupUpdate = self.initialTask.getTrustSitelists().get('trustPUlists')

        if not self.mask:
            self.mask = Mask(FirstRun=1,
                             FirstLumi=self.initialTask.getFirstLumi(),
                             FirstEvent=self.initialTask.getFirstEvent(),
                             LastRun=1,
                             LastEvent=self.initialTask.getFirstEvent() +
                                             self.initialTask.totalEvents() - 1)
        mask = Mask(**self.mask)

        # First let's initialize some parameters
        lumis_per_job = ceil(self.args['SliceSize'] / self.args['SubSliceSize'])
        totalLumisPerElement = int(self.args['MaxJobsPerElement']) * lumis_per_job

        if self.args['MaxLumisPerElement'] and totalLumisPerElement > int(self.args['MaxLumisPerElement']):
            # If there are too many lumis in the WQ element. reduce the number of jobs per element
            self.args['MaxJobsPerElement'] = floor(int(self.args['MaxLumisPerElement']) / lumis_per_job)
            if self.args['MaxJobsPerElement'] == 0:
                raise WorkQueueWMSpecError(self.wmspec, """Too many lumis (%s) in a job:
                                           Change 'SliceSize' / 'SubSliceSize'""" % lumis_per_job)

        stepSize = int(self.args['SliceSize']) * int(self.args['MaxJobsPerElement'])
        total = mask['LastEvent'] - mask['FirstEvent'] + 1
        lastAllowedEvent = mask['LastEvent']
        eventsAccounted = 0

        while eventsAccounted < total:
            current = mask['FirstEvent'] + stepSize - 1  # inclusive range
            if current > lastAllowedEvent:
                current = lastAllowedEvent
            mask['LastEvent'] = current

            # Calculate the job splitting without actually doing it
            # number of lumis is calculated by events number and SubSliceSize which is events per lumi
            # So if there no exact division between events per job and events per lumi
            # it takes the ceiling of the value.
            # Therefore total lumis can't be calculated from total events / SubSliceSize
            # It has to be caluated by adding the lumis_per_job * number of jobs
            nEvents = mask['LastEvent'] - mask['FirstEvent'] + 1
            nLumis = floor(nEvents / self.args['SliceSize']) * lumis_per_job
            remainingLumis = ceil(nEvents % self.args['SliceSize'] / self.args['SubSliceSize'])
            nLumis += remainingLumis
            jobs = ceil(nEvents / self.args['SliceSize'])

            mask['LastLumi'] = mask['FirstLumi'] + int(nLumis) - 1  # inclusive range
            self.newQueueElement(WMSpec=self.wmspec,
                                 NumberOfLumis=nLumis,
                                 NumberOfEvents=nEvents,
                                 Jobs=jobs,
                                 Mask=copy(mask),
                                 NoInputUpdate=noInputUpdate,
                                 NoPileupUpdate=noPileupUpdate,
                                 blowupFactor=self.args['blowupFactor'])


            if mask['LastEvent'] > (2 ** 32 - 1):
                # This is getting tricky, to ensure consecutive
                # events numbers we must calculate where the jobSplitter
                # will restart the firstEvent to 1 for the last time
                # in the newly created unit
                internalEvents = mask['FirstEvent']
                accumulatedEvents = internalEvents
                breakPoint = internalEvents

                while accumulatedEvents < mask['LastEvent']:
                    if (internalEvents + self.args['SliceSize'] - 1) > (2 ** 32 - 1):
                        internalEvents = 1
                        breakPoint = accumulatedEvents
                    else:
                        internalEvents += self.args['SliceSize']
                        accumulatedEvents += self.args['SliceSize']

                leftoverEvents = mask['LastEvent'] - breakPoint + 1
                mask['FirstEvent'] = leftoverEvents + 1

            else:
                mask['FirstEvent'] = mask['LastEvent'] + 1

            mask['FirstLumi'] = mask['LastLumi'] + 1
            eventsAccounted += stepSize
            lastAllowedEvent = (total - eventsAccounted) + mask['FirstEvent'] - 1


    def validate(self):
        """Check args and spec work with block splitting"""
        StartPolicyInterface.validateCommon(self)

        if self.initialTask.totalEvents() < 1:
            raise WorkQueueNoWorkError(self.wmspec, 'Invalid total events selection: %s' % str(self.initialTask.totalEvents()))

        if self.mask and self.mask['LastEvent'] < self.mask['FirstEvent']:
            raise WorkQueueWMSpecError(self.wmspec, "Invalid start & end events")

        if self.mask and self.mask['LastLumi'] < self.mask['FirstLumi']:
            raise WorkQueueWMSpecError(self.wmspec, "Invalid start & end lumis")
