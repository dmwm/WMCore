#!/usr/bin/env python
"""
_EventAwareLumiBased_

Lumi based splitting algorithm that will chop a fileset into
a set of jobs based on lumi sections, failing jobs with too many
events in a lumi and adapting the number of lumis per job
according to the average number of events per lumi in the files.

Created on Sep 25, 2012

@author: dballest
"""

from __future__ import division
from builtins import int

import logging
import math
import operator

from WMCore.DataStructs.Run import Run
from WMCore.JobSplitting.JobFactory import JobFactory
from WMCore.JobSplitting.LumiBased import isGoodLumi, isGoodRun, LumiChecker
from WMCore.WMBS.File import File
from WMCore.WMSpec.WMTask import buildLumiMask


class EventAwareLumiBased(JobFactory):
    """
    Split jobs by lumis taking into account events per lumi
    """

    locations = []

    def __init__(self, package='WMCore.DataStructs', subscription=None, generators=None, limit=0):
        super(EventAwareLumiBased, self).__init__(package, subscription, generators, limit)

        self.loadRunLumi = None  # Placeholder for DAO factory if needed
        self.collectionName = None  # Placeholder for ACDC Collection Name, if needed
        # TODO this might need to be configurable instead of being hardcoded
        self.defaultJobTimeLimit = 48 * 3600  # 48 hours
        self.lumiChecker = None

    def algorithm(self, *args, **kwargs):
        """
        _algorithm_

        Split files into a number of lumis per job
        Allow a flag to determine if we split files between jobs
        """

        avgEventsPerJob = int(kwargs.get('events_per_job', 5000))
        jobLimit = int(kwargs.get('job_limit', 0))
        jobTimeLimit = int(kwargs.get('job_time_limit', self.defaultJobTimeLimit))
        totalEvents = int(kwargs.get('total_events', 0))
        splitOnFile = bool(kwargs.get('halt_job_on_file_boundaries', False))
        self.collectionName = kwargs.get('collectionName', None)
        splitOnRun = kwargs.get('splitOnRun', True)
        getParents = kwargs.get('include_parents', False)
        runWhitelist = kwargs.get('runWhitelist', [])
        runs = kwargs.get('runs', None)
        lumis = kwargs.get('lumis', None)
        applyLumiCorrection = bool(kwargs.get('applyLumiCorrection', False))
        deterministicPileup = kwargs.get('deterministicPileup', False)
        allowCreationFailure = kwargs.get('allowCreationFailure', True)

        timePerEvent, sizePerEvent, memoryRequirement = \
            self.getPerformanceParameters(kwargs.get('performance', {}))

        eventsPerLumiInDataset = 0

        if avgEventsPerJob <= 0:
            msg = "events_per_job parameter must be positive. Its value is: %d" % avgEventsPerJob
            raise RuntimeError(msg)

        if self.package == 'WMCore.WMBS':
            self.loadRunLumi = self.daoFactory(classname="Files.GetBulkRunLumi")
            if deterministicPileup:
                getJobNumber = self.daoFactory(classname="Jobs.GetNumberOfJobsPerWorkflow")
                self.nJobs = getJobNumber.execute(workflow=self.subscription.getWorkflow().id)
                logging.info('Creating jobs in DeterministicPileup mode for %s',
                             self.subscription.workflowName())

        goodRunList = {}
        if runs and lumis:
            goodRunList = buildLumiMask(runs, lumis)

        # If we have runLumi info, we need to load it from couch
        if self.collectionName:
            try:
                from WMCore.ACDC.DataCollectionService import DataCollectionService
                couchURL = kwargs.get('couchURL')
                couchDB = kwargs.get('couchDB')
                filesetName = kwargs.get('filesetName')

                logging.info('Creating jobs for ACDC fileset %s', filesetName)
                dcs = DataCollectionService(couchURL, couchDB)
                goodRunList = dcs.getLumiWhitelist(self.collectionName, filesetName)
            except Exception as ex:
                msg = "Exception while trying to load goodRunList. "
                msg += "Refusing to create any jobs.\nDetails: %s" % str(ex)
                logging.exception(msg)
                return

        lDict = self.getFilesSortedByLocation(avgEventsPerJob)
        if not lDict:
            logging.info("There are not enough events/files to be splitted. Trying again next cycle")
            return

        locationDict = {}
        for key in lDict:
            newlist = []
            # First we need to load the data
            if self.loadRunLumi:
                fileLumis = self.loadRunLumi.execute(files=lDict[key])
                if not fileLumis:
                    logging.warning("Empty fileLumis dict for workflow %s, subs %s.",
                                    self.subscription.workflowName(), self.subscription['id'])
                for f in lDict[key]:
                    lumiDict = fileLumis.get(f['id'], {})
                    for run in lumiDict:
                        f.addRun(run=Run(run, *lumiDict[run]))

            for f in lDict[key]:
                if len(f['runs']) == 0:
                    continue
                f['runs'] = sorted(f['runs'])
                f['lumiCount'] = 0
                for run in f['runs']:
                    run.lumis.sort()
                    f['lumiCount'] += len(run.lumis)
                f['lowestRun'] = f['runs'][0]

                # Do average event per lumi calculation
                if f['lumiCount']:
                    f['avgEvtsPerLumi'] = int(round(f['events'] / f['lumiCount']))
                    if deterministicPileup:
                        # We assume that all lumis are equal in the dataset
                        eventsPerLumiInDataset = f['avgEvtsPerLumi']
                else:
                    # No lumis in the file, ignore it
                    continue
                newlist.append(f)

            locationDict[key] = sorted(newlist, key=operator.itemgetter('lowestRun'))

        totalJobs = 0
        lastLumi = None
        firstLumi = None
        lastRun = None
        lumisInJob = 0
        totalAvgEventCount = 0
        currentJobAvgEventCount = 0
        stopTask = False
        self.lumiChecker = LumiChecker(applyLumiCorrection)
        for location in locationDict:

            # For each location, we need a new jobGroup
            self.newGroup()
            stopJob = True
            for f in locationDict[location]:

                if getParents:
                    parentLFNs = self.findParent(lfn=f['lfn'])
                    for lfn in parentLFNs:
                        parent = File(lfn=lfn)
                        f['parents'].add(parent)

                lumisInJobInFile = 0
                updateSplitOnJobStop = False
                failNextJob = False
                # If estimated job time is higher the job time limit (condor limit)
                # and it's only one lumi then ditch that lumi
                timePerLumi = f['avgEvtsPerLumi'] * timePerEvent
                if timePerLumi > jobTimeLimit and f['lumiCount'] == 1:
                    lumisPerJob = 1
                    stopJob = True
                    if allowCreationFailure:
                        failNextJob = True
                elif splitOnFile:
                    # Then we have to split on every boundary
                    stopJob = True
                    # Check the average number of events per lumi in this file
                    # Adapt the lumis per job to match the target conditions
                    if f['avgEvtsPerLumi']:
                        # If there are events in the file
                        ratio = float(avgEventsPerJob) / f['avgEvtsPerLumi']
                        lumisPerJob = max(int(math.floor(ratio)), 1)
                    else:
                        # Zero event file, then the ratio goes to infinity. Computers don't like that
                        lumisPerJob = f['lumiCount']
                else:
                    # Analyze how many events does this job already has
                    # Check how many we want as target, include as many lumi sections as possible
                    updateSplitOnJobStop = True
                    eventsRemaining = max(avgEventsPerJob - currentJobAvgEventCount, 0)
                    if f['avgEvtsPerLumi']:
                        lumisAllowed = int(math.floor(float(eventsRemaining) / f['avgEvtsPerLumi']))
                    else:
                        lumisAllowed = f['lumiCount']
                    lumisPerJob = max(lumisInJob + lumisAllowed, 1)

                for run in f['runs']:
                    if not isGoodRun(goodRunList=goodRunList, run=run.run):
                        # Then skip this one
                        continue
                    if len(runWhitelist) > 0 and not run.run in runWhitelist:
                        # Skip due to run whitelist
                        continue
                    firstLumi = None

                    if splitOnRun and run.run != lastRun:
                        # Then we need to kill this job and get a new one
                        stopJob = True

                    # Now loop over the lumis
                    for lumi in run:
                        if (not isGoodLumi(goodRunList, run=run.run, lumi=lumi) or
                                self.lumiChecker.isSplitLumi(run.run, lumi, f)):
                            # Kill the chain of good lumis
                            # Skip this lumi
                            if firstLumi != None and firstLumi != lumi:
                                self.currentJob['mask'].addRunAndLumis(run=run.run,
                                                                       lumis=[firstLumi, lastLumi])
                                eventsAdded = ((lastLumi - firstLumi + 1) * f['avgEvtsPerLumi'])
                                runAddedTime = eventsAdded * timePerEvent
                                runAddedSize = eventsAdded * sizePerEvent
                                self.currentJob.addResourceEstimates(jobTime=runAddedTime, disk=runAddedSize)
                                firstLumi = None
                                lastLumi = None
                            continue

                        # You have to kill the lumi chain if they're not continuous
                        if lastLumi and not lumi == lastLumi + 1:
                            self.currentJob['mask'].addRunAndLumis(run=run.run,
                                                                   lumis=[firstLumi, lastLumi])
                            eventsAdded = ((lastLumi - firstLumi + 1) * f['avgEvtsPerLumi'])
                            runAddedTime = eventsAdded * timePerEvent
                            runAddedSize = eventsAdded * sizePerEvent
                            self.currentJob.addResourceEstimates(jobTime=runAddedTime, disk=runAddedSize)
                            firstLumi = None
                            lastLumi = None

                        if firstLumi is None:
                            # Set the first lumi in the run
                            firstLumi = lumi

                        # If we're full, end the job
                        if lumisInJob == lumisPerJob:
                            stopJob = True
                        # Actually do the new job creation
                        if stopJob:
                            if firstLumi != None and lastLumi != None and lastRun != None:
                                self.currentJob['mask'].addRunAndLumis(run=lastRun,
                                                                       lumis=[firstLumi, lastLumi])
                                eventsAdded = ((lastLumi - firstLumi + 1) * f['avgEvtsPerLumi'])
                                runAddedTime = eventsAdded * timePerEvent
                                runAddedSize = eventsAdded * sizePerEvent
                                self.currentJob.addResourceEstimates(jobTime=runAddedTime, disk=runAddedSize)
                            msg = None
                            if failNextJob:
                                msg = "File %s has a single lumi %s, in run %s " % (f['lfn'], lumi, run.run)
                                msg += "with too many events %d and it woud take %d sec to run" \
                                       % (f['events'], timePerLumi)
                            self.lumiChecker.closeJob(self.currentJob)
                            self.newJob(name=self.getJobName(), failedJob=failNextJob, failedReason=msg)
                            if deterministicPileup:
                                skipEvents = (self.nJobs - 1) * lumisPerJob * eventsPerLumiInDataset
                                self.currentJob.addBaggageParameter("skipPileupEvents", skipEvents)
                            self.currentJob.addResourceEstimates(memory=memoryRequirement)
                            failNextJob = False
                            firstLumi = lumi
                            lumisInJob = 0
                            lumisInJobInFile = 0
                            currentJobAvgEventCount = 0
                            totalJobs += 1
                            if jobLimit and totalJobs > jobLimit:
                                msg = "Job limit of {0} jobs exceeded.".format(jobLimit)
                                raise RuntimeError(msg)

                            # Add the file to new jobs
                            self.currentJob.addFile(f)

                            if updateSplitOnJobStop:
                                # Then we were carrying from a previous file
                                # Reset calculations for this file
                                updateSplitOnJobStop = False
                                if f['avgEvtsPerLumi']:
                                    ratio = float(avgEventsPerJob) / f['avgEvtsPerLumi']
                                    lumisPerJob = max(int(math.floor(ratio)), 1)
                                else:
                                    lumisPerJob = f['lumiCount']

                        lumisInJob += 1
                        lumisInJobInFile += 1
                        lastLumi = lumi
                        stopJob = False
                        lastRun = run.run
                        totalAvgEventCount += f['avgEvtsPerLumi']

                        if self.currentJob and not f in self.currentJob['input_files']:
                            self.currentJob.addFile(f)

                        # We stop here if there are more total events than requested.
                        if totalEvents > 0 and totalAvgEventCount >= totalEvents:
                            stopTask = True
                            break

                    if firstLumi != None and lastLumi != None:
                        # Add this run to the mask
                        self.currentJob['mask'].addRunAndLumis(run=run.run,
                                                               lumis=[firstLumi, lastLumi])
                        eventsAdded = ((lastLumi - firstLumi + 1) * f['avgEvtsPerLumi'])
                        runAddedTime = eventsAdded * timePerEvent
                        runAddedSize = eventsAdded * sizePerEvent
                        self.currentJob.addResourceEstimates(jobTime=runAddedTime, disk=runAddedSize)
                        firstLumi = None
                        lastLumi = None

                    if stopTask:
                        break

                if not splitOnFile:
                    currentJobAvgEventCount += f['avgEvtsPerLumi'] * lumisInJobInFile

                if stopTask:
                    break

            if stopTask:
                break

        self.lumiChecker.closeJob(self.currentJob)
        self.lumiChecker.fixInputFiles()
        return
